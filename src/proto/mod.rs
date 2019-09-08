
pub mod proto;
pub mod persistence;
pub mod txn;

use serde_derive::Deserialize;
use serde_derive::Serialize;

// See https://github.com/apache/zookeeper/blob/trunk/src/zookeeper.jute

/// ZooKeeper transaction id
#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Zxid(pub i64);

#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Timestamp(pub u64);

#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Duration(pub i32);

#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Version(pub i32);
pub const ANY_VERSION: Version = Version(-1);

#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct OptionalVersion(pub i32);

#[derive(Serialize, Deserialize, PartialEq)]
pub struct SessionId(pub i64);

/// Exchange id, a correlation id sent by a request and returned in its response.
/// It starts at 1, but can be negative for server-generated notifications (see
/// `FinalRequestProcessor` in ZK server)
#[derive(Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Xid(pub i32);

/// Permissions associated to an ACL
#[derive(Serialize, Deserialize)]
pub struct Perms(u32);

impl Perms {
    /// Checks that `self` grants all permissions granted by `perm`.
    pub fn has(&self, perm: Perms) -> bool {
        (self.0 & perm.0) ^ perm.0 == 0
    }
}

impl std::ops::BitOr for Perms {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Perms(self.0 | rhs.0)
    }
}

pub const PERM_READ: Perms = Perms(1 << 0);
pub const PERM_WRITE: Perms = Perms(1 << 1);
pub const PERM_CREATE: Perms = Perms(1 << 2);
pub const PERM_DELETE: Perms = Perms(1 << 3);
pub const PERM_ADMIN: Perms = Perms(1 << 4);
pub const PERM_ALL: Perms = Perms(PERM_READ.0 | PERM_WRITE.0 | PERM_CREATE.0 | PERM_DELETE.0 | PERM_ADMIN.0);


// See CreateMode.java
#[derive(Serialize, Deserialize)]
pub enum CreateMode {
    Persistent = 0,
    Ephemeral = 1,
    PersistentSequential = 2,
    EphemeralSequential = 3,
    Container = 4,
    PersistentWithTTL = 5,
    PersistentSequentialWithTTL = 6,
}

use CreateMode::*;
impl CreateMode {
    pub fn is_ephemeral(&self) -> bool {
        match self {
            Ephemeral | EphemeralSequential => true,
            _ => false,
        }
    }

    pub fn is_sequential(&self) -> bool {
        match self {
            PersistentSequential | EphemeralSequential => true,
            _ => false,
        }
    }

    pub fn is_container(&self) -> bool {
        match self {
            Container => true,
            _ => false,
        }
    }

    pub fn is_ttl(&self) -> bool {
        match self {
            PersistentWithTTL | PersistentSequentialWithTTL => true,
            _ => false,
        }
    }
}

//----- Data

#[derive(Serialize, Deserialize, PartialEq)]
pub struct Id {
    pub scheme: String,
    pub id: String,
}

#[derive(Serialize, Deserialize)]
pub struct ACL {
    pub perms: Perms,
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

#[cfg(test)]
pub mod test {

    /// Test that the additional derives on enums behave as expected
    #[test]
    pub fn test_opcode_derives() {
        use super::proto::OpCode;
        use num_traits::cast::ToPrimitive;
        use strum::IntoEnumIterator;

        // Use CloseSession as its value is different from its position in the variants

        let x = OpCode::CloseSession;

//        // EnumString
//        let x = OpCode::from_str("CloseSession").expect("Cannot resolve enum");
//        assert_eq!(x, OpCode::CloseSession);

        // ToPrimitive
        assert_eq!(x.to_i32(), Some(-11));

//        // FromPrimitive
//        assert_eq!(OpCode::from_i32(-11), Some(OpCode::CloseSession));

        // IntoStaticStr
        let x: &'static str = OpCode::Create.into();
        assert_eq!(x, "Create");

        // EnumIter
        let v = OpCode::iter().collect::<Vec<_>>();
        assert_eq!(&v[0..3], &[OpCode::Notification, OpCode::Create, OpCode::Delete]);

        let _v = OpCode::iter().map(|v| (v, 0)).collect::<Vec<_>>();
    }
}
