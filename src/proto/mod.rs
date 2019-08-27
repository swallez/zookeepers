mod de;
mod error;
mod ser;

// Inspired by https://github.com/servo/bincode

pub use de::{Deserializer};
pub use error::{Error, Result};

use serde_derive::Deserialize;
use serde_derive::Serialize;

const MAX_LENGTH: usize = 1024 * 1024;


// See https://github.com/apache/zookeeper/blob/trunk/src/zookeeper.jute

/// ZooKeeper transaction id
#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Zxid(pub i64);

#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Timestamp(pub i64);

#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Version(pub i32);

#[derive(Serialize, Deserialize, PartialEq)]
pub struct SessionId(pub i64);

/// Exchange id, a correlation id sent by a request and returned in its response.
/// It starts at 1, but can be negative (for server-generated notifications? see
/// FinalRequestProcessor in ZK server)
#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Xid(pub i32);

// See ZooDefs.java
#[derive(Debug, PartialEq)]
#[derive(FromPrimitive, ToPrimitive)]
#[derive(EnumString, IntoStaticStr, EnumIter)]
pub enum OpCode {
    Notification = 0,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetData = 4,
    SetData = 5,
    GetACL = 6,
    SetACL = 7,
    GetChildren = 8,
    Sync = 9,
    // 10 not used
    Ping = 11,
    GetChildren2 = 12,
    Check = 13,
    Multi = 14,
    Create2 = 15,
    Reconfig = 16,
    CheckWatches = 17,
    RemoveWatches = 18,
    CreateContainer = 19,
    DeleteContainer = 20,
    CreateTTL = 21,
    Auth = 100,
    SetWatches = 101,
    Sasl = 102,
    CreateSession = -10,
    CloseSession = -11,
    Error = -1
}

//----- Data

#[derive(Serialize, Deserialize, PartialEq)]
pub struct Id {
    pub scheme: String,
    pub id: String,
}

#[derive(Serialize, Deserialize)]
pub struct ACL {
    // FIXME: newtype
    pub perms: u32,
    pub id: Id,
}

/// Information shared with the client
#[derive(Serialize, Deserialize)]
pub struct Stat {
    /// Created zxid
    pub czxid: Zxid,
    /// Last modified zxid
    pub mzxid: Zxid,
    /// Created time
    pub ctime: Timestamp,
    /// Last modified time
    pub mtime: Timestamp,
    /// Version
    pub version: Version,
    /// Child version
    pub cversion: Version,
    /// ACL version
    pub aversion: Version,
    /// Owner id if ephemeral, 0 otherwise
    pub ephemeral_owner: SessionId,
    /// Length of the data in the node
    pub data_length: i32,
    /// Number of children of this node
    pub num_children: i32,
    /// Last modified children
    pub pzxid: Zxid,
}

/// Information explicitly stored by the server persistently
#[derive(Serialize, Deserialize)]
pub struct StatPersisted {
    /// created zxid
    pub czxid: Zxid,
    /// last modified zxid
    pub mzxid: Zxid,
    /// created
    pub ctime: Timestamp,
    /// last modified
    pub mtime: Timestamp,
    /// version
    pub version: Version,
    /// child version
    pub cversion: Version,
    /// acl version
    pub aversion: Version,
    /// owner id if ephemeral, 0 otw
    pub ephemeral_owner: SessionId,
    /// last modified children
    pub pzxid: Zxid,
}

//----- Protocol

/// The `Request` trait holds the response type, so that we can implement strongly typed RPC
pub trait Request {
    type Response;
}

#[derive(Serialize, Deserialize)]
pub struct ConnectRequest {
    pub protocol_version: i32,
    pub last_zxid_seen: Zxid,
    pub time_out: i32, // FIXME: duration
    pub session_id: SessionId,
    pub passwd: Vec<u8>,
}

impl Request for ConnectRequest {
    type Response = ConnectResponse;
}

#[derive(Serialize, Deserialize)]
pub struct ConnectResponse {
    pub protocol_version: i32,
    pub time_out: i32,
    pub session_id: SessionId,
    pub passwd: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct SetWatches {
    pub relative_zxid: Zxid,
    pub data_watches: Vec<String>,
    pub exist_watches: Vec<String>,
    pub child_watches: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct RequestHeader {
    pub xid: Xid,
    pub r#type: i32,
}

#[derive(Serialize, Deserialize)]
pub struct MultiHeader {
    pub r#type: i32,
    pub done: bool,
    pub err: i32,
}

#[cfg(test)]
pub mod test {

    /// Test that the additional derives on enums behave as expected
    #[test]
    pub fn test_opcode_derives() {
        use super::OpCode;
        use std::str::FromStr;
        use num_traits::cast::ToPrimitive;
        use num_traits::cast::FromPrimitive;
        use strum::IntoEnumIterator;

        // Use CloseSession as its value is different from its position in the variants

        // EnumString
        let x = OpCode::from_str("CloseSession").expect("Cannot resolve enum");
        assert_eq!(x, OpCode::CloseSession);

        // ToPrimitive
        assert_eq!(x.to_i32(), Some(-11));

        // FromPrimitive
        assert_eq!(OpCode::from_i32(-11), Some(OpCode::CloseSession));

        // IntoStaticStr
        let x: &'static str = OpCode::Create.into();
        assert_eq!(x, "Create");

        // EnumIter
        let v = OpCode::iter().collect::<Vec<_>>();
        assert_eq!(&v[0..3], &[OpCode::Notification, OpCode::Create, OpCode::Delete]);

        let _v = OpCode::iter().map(|v| (v, 0)).collect::<Vec<_>>();
    }
}
