pub mod de;
pub mod error;
pub mod ser;

pub use de::Deserializer;
pub use de::OpCodeEnum;

const MAX_LENGTH: usize = 1024 * 1024;

/// Order type and length in the encoding format for enumerations.
///
/// ZooKeeper doesn't encode enumerations in a consistent way:
/// - in multi operations, it's type, length, data
/// - everywhere else it's length, type, data
///
pub enum EnumEncoding {
    TypeThenLength,
    LengthThenType,
    Type,
}
