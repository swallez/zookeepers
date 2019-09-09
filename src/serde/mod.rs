pub mod de;
pub mod error;
pub mod ser;

pub use de::Deserializer;
pub use de::OpCodeEnum;

const MAX_LENGTH: usize = 1024 * 1024;
