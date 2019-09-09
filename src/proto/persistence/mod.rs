use serde_derive::Deserialize;
use serde_derive::Serialize;

pub mod snapshot;
pub mod txnlog;

#[derive(Deserialize, Serialize)]
pub struct FileHeader {
    pub magic: i32,   // Should be TXNLOG_MAGIC or SNAP_MAGIC
    pub version: i32, // Should be 2
    pub dbid: i64,
}

pub const TXNLOG_MAGIC: i32 = 0x5a4b4c47; // "ZKLG"
pub const SNAP_MAGIC: i32 = 0x5a4b534e; // ZKSN
