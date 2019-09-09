use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::proto::{ErrorCode, OpCode};
use super::Duration;
use super::SessionId;
use super::Timestamp;
use super::Version;
use super::Xid;
use super::Zxid;
use super::ACL;

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct TxnHeader {
    pub client_id: SessionId,
    pub cxid: Xid,
    pub zxid: Zxid,
    pub time: Timestamp,
    #[serde(rename = "type")]
    pub typ: OpCode,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CreateTxnV0 {
    pub path: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub acl: Vec<ACL>,
    pub ephemeral: bool,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CreateTxn {
    pub path: String,
    pub data: Vec<u8>,
    pub acl: Vec<ACL>,
    pub ephemeral: bool,
    pub parent_c_version: Version,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CreateContainerTxn {
    pub path: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub acl: Vec<ACL>,
    pub parent_c_version: Version,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CreateTTLTxn {
    pub path: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub acl: Vec<ACL>,
    pub parent_c_version: Version,
    pub ttl: i64,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct DeleteTxn {
    pub path: String,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetDataTxn {
    pub path: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub version: Version,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CheckVersionTxn {
    pub path: String,
    pub version: Version,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetACLTxn {
    pub path: String,
    pub acl: Vec<ACL>,
    pub version: Version,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetMaxChildrenTxn {
    pub path: String,
    pub max: i32,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CreateSessionTxn {
    pub time_out: Duration,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct ErrorTxn {
    pub err: ErrorCode,
}

/// Use `Transaction` for typed structs
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct Txn {
    #[serde(rename = "type")]
    pub typ: OpCode,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct MultiTxn {
    pub txns: Vec<Transaction>,
}

// See SerializeUtils.java
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub enum Transaction {
    CreateSession(CreateSessionTxn),
    Create(CreateTxn),
    Create2(CreateTxn),
    CreateTTL(CreateTTLTxn),
    CreateContainer(CreateContainerTxn),
    Delete(DeleteTxn),
    DeleteContainer(DeleteTxn),
    Reconfig,
    SetData(SetDataTxn),
    SetACL(SetACLTxn),
    Error(ErrorTxn),
    Multi(MultiTxn),
    //Check(CheckVersionTxn), -- not persisted
}
