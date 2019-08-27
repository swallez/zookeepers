

use std::io::Read;
use std::collections::HashMap;

use serde::de::{self, DeserializeSeed, MapAccess, VariantAccess, EnumAccess, SeqAccess, Visitor, IntoDeserializer};

use byteorder::{BigEndian, ReadBytesExt};

use super::error::{Error, Result};
use super::MAX_LENGTH;

use num_traits::ToPrimitive;
use strum::IntoEnumIterator;

pub struct Deserializer<'de, R: Read> {
    reader: &'de mut R,

    /// Struct enum type -> (enum variant discriminant -> enum variant name)
    enum_mappings: HashMap<&'static str, HashMap<i32, &'static str>>,
}

pub fn from_reader<'de, R: Read>(reader: &'de mut R) -> Deserializer<'de, R> {
    Deserializer{
        reader,
        enum_mappings: HashMap::new(),
    }
}

impl<'de, 'a, R:Read> Deserializer<'de, R> {

    /// Add a discriminant mapping for struct enum types.
    pub fn add_enum_mapping<E, I>(&mut self, enum_struct: &'static str) where
        E: IntoEnumIterator<Iterator=I>,
        I: Iterator<Item=E>,
        E: ToPrimitive,
        E: Into<&'static str>,
    {
        self.enum_mappings.insert(
            enum_struct,
            E::iter().map(|v| (v.to_i32().expect(""), v.into())).collect()
        );
    }
}

impl<'de, 'a, R: Read> de::Deserializer<'de> for &'a mut Deserializer<'de, R> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.reader.read_u8()? != 0)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.reader.read_i8()?)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        // Jute only supports 8, 32 & 64 bits integers. We make a deliberate choice to fail
        // hard as it's not a runtime failure, but an error in the struct definition.
        // Same for other unsupported types.
        unimplemented!()
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.reader.read_i32::<BigEndian>()?)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.reader.read_i64::<BigEndian>()?)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u16<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_u64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(self.reader.read_f32::<BigEndian>()?)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(self.reader.read_f64::<BigEndian>()?)
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let len = self.reader.read_u32::<BigEndian>()? as usize;

        if len > MAX_LENGTH {
            return Err(Error::TooLarge(len));
        }

        let mut chars = vec![0; len];
        let buffer = chars.as_mut_slice();
        self.reader.read_exact(buffer)?;

        visitor.visit_str(std::str::from_utf8(buffer)?)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let len = self.reader.read_u32::<BigEndian>()? as usize;

        if len > MAX_LENGTH {
            return Err(Error::TooLarge(len));
        }

        let mut chars = vec![0; len];
        let buffer = chars.as_mut_slice();
        self.reader.read_exact(buffer)?;

        visitor.visit_string(String::from_utf8(chars)?)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_option<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(self, _name: &'static str, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(mut self, visitor: V) -> Result<V::Value> {
        let read_size = self.reader.read_i32::<BigEndian>()?;

        // The java encoding distinguishes null vectors (length -1) from empty vectors (length 0)
        // We don't find such a distinction though in the C/C++ code and sampling the ZK server
        // code shows that a number of places expect non-null vectors.
        let size = if read_size < 0 {
            0
        } else {
            read_size.to_usize().ok_or(Error::Message("Size value too large".to_owned()))?
        };

        visitor.visit_seq(JuteAccess {size, de: &mut self})
    }

    fn deserialize_tuple<V: Visitor<'de>>(mut self, len: usize, visitor: V) -> Result<V::Value> {
        // A tuple is just a sequence of values
        visitor.visit_seq(JuteAccess {
            size: len,
            de: &mut self
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(self, _name: &'static str, len: usize, visitor: V) -> Result<V::Value> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(mut self, visitor: V) -> Result<V::Value> {
        let read_size = self.reader.read_i32::<BigEndian>()?;

        let size = if read_size < 0 {
            0
        } else {
            read_size.to_usize().ok_or(Error::Message("Size value too large".to_owned()))?
        };

        visitor.visit_map(JuteAccess {size: size, de: &mut self})
    }

    fn deserialize_struct<V: Visitor<'de>>(self, _name: &'static str, fields: &'static [&'static str], visitor: V) -> Result<V::Value> {
        // Field names are not stored, so just consider it as a tuple (where fields are ordered)
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        mut self,
        name: &'static str,
        _variants: &'static [&'static str],
        visitor: V
    ) -> Result<V::Value> {
        visitor.visit_enum(JuteEnumAccess {enum_type: name, de: &mut self})
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        unimplemented!()
    }
}

struct JuteAccess<'a, 'de: 'a, R: Read> {
    de: &'a mut Deserializer<'de, R>,
    size: usize,
}

impl <'a, 'de: 'a, R: Read> SeqAccess<'de> for JuteAccess<'a, 'de, R> {
    type Error = super::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.size <= 0 {
            Ok(None)
        } else {
            self.size -= 1;
            seed.deserialize(&mut *self.de).map(Some)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.size)
    }
}

impl <'a, 'de: 'a, R: Read> MapAccess<'de> for JuteAccess<'a, 'de, R> {
    type Error = super::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>>  {
        if self.size <= 0 {
            Ok(None)
        } else {
            self.size -= 1;
            seed.deserialize(&mut *self.de).map(Some)
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        seed.deserialize(&mut *self.de)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.size)
    }
}
struct JuteEnumAccess<'a, 'de: 'a, R: Read> {
    de: &'a mut Deserializer<'de, R>,
    enum_type: &'static str,
}

impl <'a, 'de: 'a, R: Read> EnumAccess<'de> for JuteEnumAccess<'a, 'de, R> {
    type Error = super::Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
        where
            V: serde::de::DeserializeSeed<'de>,
    {
        let mappings = self.de.enum_mappings
            .get(self.enum_type)
            .ok_or_else(||Error::Message(format!("Cannot find mapping for type {}", self.enum_type)))?;

        let d = self.de.reader.read_i32::<BigEndian>()?;
        let idx = mappings
            .get(&d)
            .ok_or_else(||Error::Message(format!("Wrong discriminant for {}: {}", self.enum_type, d)))?;

        let val: Result<_> = seed.deserialize(idx.into_deserializer());
        Ok((val?, self))
    }
}

impl <'a, 'de: 'a, R: Read> VariantAccess<'de> for JuteEnumAccess<'a, 'de, R> {
    type Error = super::Error;

    fn unit_variant(self) -> Result<()> {
        unimplemented!()
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        serde::de::DeserializeSeed::deserialize(seed, self.de)
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        serde::de::Deserializer::deserialize_tuple(self.de, len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value> {
        serde::de::Deserializer::deserialize_tuple(self.de, fields.len(), visitor)
    }
}


#[cfg(test)]
pub mod test {

    use serde_derive::Deserialize;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Deserialize)]
    struct NewType(i32);

    #[derive(Deserialize)]
    struct Foo {
        a: NewType,
        x: i32,
        y: String,
        z: std::collections::HashMap<i8, String>
    }

    #[derive(Deserialize)]
    struct Bar {
        _x: i32,
    }


    #[test]
    fn test_deser() {
        let data: Vec<u8> = vec![
            0x01, 0x02, 0x03, 0x04,
            0x05, 0x06, 0x07, 0x08,
            0x00, 0x00, 0x00, 0x04, // string length
            0x61, 0x62, 0x63, 0x64, // "abcd"
            0x00, 0x00, 0x00, 0x01, // map length
            0x0F,                   // i8
            0x00, 0x00, 0x00, 0x04, // string length
            0x61, 0x62, 0x63, 0x64  // string
        ];
        let mut bytes = data.as_slice();

        let mut deser = crate::proto::de::from_reader(&mut bytes);

        let foo = Foo::deserialize(&mut deser).expect("Failed to deserialize");

        assert_eq!(foo.a, NewType(0x01020304));
        assert_eq!(foo.x, 0x05060708);
        assert_eq!(&foo.y, "abcd");

        assert_eq!(foo.z.len(), 1);
        assert_eq!(foo.z.get(&0xF), Some(&("abcd".to_owned())));
    }

    #[derive(Debug, PartialEq)]
    #[derive(FromPrimitive, ToPrimitive)]
    #[derive(EnumString, IntoStaticStr, EnumIter)]
    enum FooBarCode {
        Foo = 3,
        Bar = 4,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    enum FooBar {
        Foo(i32),
        Bar(String)
    }

    #[test]
    fn test_enum() {

        let data: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x03, // Foo discriminant
            0x01, 0x02, 0x03, 0x04  // i32
        ];
        let mut bytes = data.as_slice();

        let mut deser = crate::proto::de::from_reader(&mut bytes);
        deser.add_enum_mapping::<FooBarCode, _>("FooBar");

        let foobar = FooBar::deserialize(&mut deser).expect("fail");
        println!("FooBar = {:?}", foobar);

        assert_eq!(foobar, FooBar::Foo(0x01020304));

        let data: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x04, // Bar discriminant
            0x00, 0x00, 0x00, 0x04, // string length
            0x61, 0x62, 0x63, 0x64, // "abcd"
        ];
        let mut bytes = data.as_slice();

        let mut deser = crate::proto::de::from_reader(&mut bytes);
        deser.add_enum_mapping::<FooBarCode, _>("FooBar");
        let foobar = FooBar::deserialize(&mut deser).expect("fail");
        println!("FooBar = {:?}", foobar);

        assert_eq!(foobar, FooBar::Bar("abcd".to_owned()));
    }
}
