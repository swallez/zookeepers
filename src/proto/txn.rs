use named_type::NamedType;
use named_type_derive::NamedType;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::proto::ErrorCode;
use super::Duration;
use super::SessionId;
use super::Timestamp;
use super::Version;
use super::Xid;
use super::Zxid;
use super::ACL;

/// Transaction header.
///
/// Compared to `ZooKeeper.jute` it doesn't contain the operation type, which is handled in a
/// type-safe way in `TxnOperation`.
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct TxnHeader {
    pub client_id: SessionId,
    pub cxid: Xid,
    pub zxid: Zxid,
    pub time: Timestamp,
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

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct MultiTxn {
    pub txns: Vec<MultiTxnOperation>,
}

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
#[derive(NamedType)]
pub enum MultiTxnOperation {
    Create(CreateTxn),
    Create2(CreateTxn),
    CreateTTL(CreateTTLTxn),
    CreateContainer(CreateContainerTxn),
    Delete(DeleteTxn),
    DeleteContainer(DeleteTxn),
    SetData(SetDataTxn),
    Error(ErrorTxn),
    Check(CheckVersionTxn),
}

/// A transaction, composed of its header and operation
#[derive(Debug)]
#[derive(Deserialize, Serialize)]
pub struct Txn {
    pub header: TxnHeader,
    pub op: TxnOperation,
}

/// A transaction operation.
///
/// There's a hack in SerializeUtils.deserializeTxn for CreateV0 transactions that don't contain
/// a version id. We assume the files we process are not ancient enough to have those.
#[derive(Debug)]
#[derive(Deserialize, Serialize)]
#[derive(NamedType)]
pub enum TxnOperation {
    CreateSession(CreateSessionTxn),
    CloseSession,
    Create(CreateTxn),
    Create2(CreateTxn),
    CreateTTL(CreateTTLTxn),
    CreateContainer(CreateContainerTxn),
    Delete(DeleteTxn),
    DeleteContainer(DeleteTxn),
    Reconfig(SetDataTxn),
    SetData(SetDataTxn),
    SetACL(SetACLTxn),
    Error(ErrorTxn),
    Multi(MultiTxn),
}
