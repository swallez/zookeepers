use serde_derive::Deserialize;
use serde_derive::Serialize;

use std::path::Path;

pub mod snapshot;
pub mod txnlog;

use super::Zxid;

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
pub struct FileHeader {
    pub magic: i32,   // Should be TXNLOG_MAGIC or SNAP_MAGIC
    pub version: i32, // Should be 2
    pub dbid: i64,
}

pub const TXNLOG_MAGIC: i32 = 0x5a4b_4c47; // "ZKLG"
pub const SNAP_MAGIC: i32 = 0x5a4b_534e; // ZKSN

pub fn zxid_from_path(path: impl AsRef<Path>) -> Option<Zxid> {
    let path = path.as_ref();

    let ext = path.extension()?.to_str()?;
    let value = i64::from_str_radix(ext, 16).ok()?;

    Some(Zxid(value))
}
