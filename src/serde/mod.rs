//! [Serde] serializer and deserializer for the ZooKeeper protocol.
//!
//! [Serde]:https://serde.rs/

pub mod de;
pub mod error;
pub mod ser;

pub use de::Deserializer;
pub use de::OpCodeEnum;

const MAX_LENGTH: usize = 1024 * 1024; // FIXME: make configurable

/// Order of type and length in the encoding format for enumerations.
///
/// ZooKeeper doesn't encode enumerations in a consistent way:
/// - in multi operations, it's type, length, data.
/// - everywhere else it's length, type, data.
/// - in some places though we need to read the length beforehand, so we need to instruct the
///   serializer/deserializer to only handle the type.
///
pub enum EnumEncoding {
    TypeThenLength,
    LengthThenType,
    Type,
}
