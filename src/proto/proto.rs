use named_type::NamedType;
use named_type_derive::NamedType;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::CreateMode;
use super::Duration;
use super::OptionalVersion;
use super::SessionId;
use super::Stat;
use super::Version;
use super::Xid;
use super::Zxid;
use super::ACL;

/// The `Request` trait holds the response type, so that we can implement strongly typed RPC
pub trait Request {
    type Response;
}

// See ZooDefs.java

#[derive(Debug, PartialEq)]
#[derive(Serialize, Deserialize)]
#[derive(ToPrimitive)]
#[derive(IntoStaticStr, EnumIter)]
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
    Error = -1,
}

#[derive(Debug, PartialEq, PartialOrd)]
#[derive(Serialize, Deserialize)]
#[derive(ToPrimitive)]
#[derive(IntoStaticStr, EnumIter)]
#[derive(NamedType)]
pub enum ErrorCode {
    /// Everything is OK
    Ok = 0,

    /// System and server-side errors.
    /// This is never thrown by the server, it shouldn't be used other than
    /// to indicate a range. Specifically error codes greater than this
    /// value, but lesser than `APIError`, are system errors.
    SystemError = -1,

    /// A runtime inconsistency was found
    RuntimeInconsistency = -2,
    /// A data inconsistency was found
    DataInconsistency = -3,
    /// Connection to the server has been lost
    ConnectionLoss = -4,
    /// Error while marshalling or unmarshalling data
    MarshallingError = -5,
    /// Operation is unimplemented
    Unimplemented = -6,
    /// Operation timeout
    OperationTimeout = -7,
    /// Invalid arguments
    BadArguments = -8,
    /// No quorum of new config is connected and up-to-date with the leader of last commmitted config - try
    /// invoking reconfiguration after new servers are connected and synced
    NewConfigNoQuorum = -13,
    /// Another reconfiguration is in progress -- concurrent reconfigs not supported (yet)
    ReconfigInProgress = -14,
    /// Unknown session (internal server use only)
    UnknownSession = -12,

    /// API errors.
    /// This is never thrown by the server, it shouldn't be used other than
    /// to indicate a range. Specifically error codes greater than this
    /// value are API errors (while values less than this indicate a `SystemError`).
    APIError = -100,

    /// Node does not exist
    NoNode = -101,
    /// Not authenticated
    NoAuth = -102,
    /// Version conflict
    /// In case of reconfiguration: reconfig requested from config version X but last seen config
    /// has a different version Y.
    BadVersion = -103,
    /// Ephemeral nodes may not have children
    NoChildrenForEphemerals = -108,
    /// The node already exists
    NodeExists = -110,
    /// The node has children
    NotEmpty = -111,
    /// The session has been expired by the server
    SessionExpired = -112,
    /// Invalid callback specified
    InvalidCallback = -113,
    /// Invalid ACL specified
    InvalidACL = -114,
    /// Client authentication failed
    AuthFailed = -115,
    /// Session moved to another server, so operation is ignored
    SessionMoved = -118,
    /// State-changing request is passed to read-only server
    NotReadOnly = -119,
    /// Attempt to create ephemeral node on a local session
    EphemeralOnLocalSession = -120,
    /// Attempts to remove a non-existing watcher
    NoWatcher = -121,
    /// Attempts to perform a reconfiguration operation when reconfiguration feature is disabled.
    ReconfigDisabled = -123,
}

impl ErrorCode {
    pub fn is_system_error(&self) -> bool {
        self < &ErrorCode::SystemError && self > &ErrorCode::APIError
    }

    pub fn is_api_error(&self) -> bool {
        self < &ErrorCode::APIError
    }
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct ConnectRequest {
    pub protocol_version: i32,
    pub last_zxid_seen: Zxid,
    pub time_out: Duration,
    pub session_id: SessionId,
    #[serde(with = "serde_bytes")]
    pub passwd: Vec<u8>,
}

impl Request for ConnectRequest {
    type Response = ConnectResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct ConnectResponse {
    pub protocol_version: i32,
    pub time_out: Duration,
    pub session_id: SessionId,
    #[serde(with = "serde_bytes")]
    pub passwd: Vec<u8>,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetWatches {
    pub relative_zxid: Zxid,
    pub data_watches: Vec<String>,
    pub exist_watches: Vec<String>,
    pub child_watches: Vec<String>,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct RequestHeader {
    pub xid: Xid,
    #[serde(rename = "type")]
    pub typ: i32,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct MultiHeader {
    #[serde(rename = "type")]
    pub typ: i32,
    pub done: bool,
    pub err: i32,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct AuthPacket {
    #[serde(rename = "type")]
    pub typ: i32,
    pub scheme: String,
    #[serde(with = "serde_bytes")]
    pub buffer: Vec<u8>,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct ReplyHeader {
    pub xid: Xid,
    pub zxid: Zxid,
    pub err: i32,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetDataRequest {
    pub path: String,
    pub watch: bool,
}

impl Request for GetDataRequest {
    type Response = GetDataResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetDataRequest {
    pub path: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub version: Version,
}

impl Request for SetDataRequest {
    type Response = SetDataResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct ReconfigRequest {
    pub joining_servers: String,
    pub leaving_servers: String,
    pub new_members: String,
    pub cur_config_id: i64,
}

impl Request for ReconfigRequest {
    type Response = GetDataResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetDataResponse {
    pub stat: Stat,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetSASLRequest {
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

impl Request for GetSASLRequest {
    type Response = SetSASLResponse; // Same response type as SetSASL
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetSASLRequest {
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

impl Request for SetSASLRequest {
    type Response = SetSASLResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetSASLResponse {
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CreateRequest {
    pub path: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub acl: Vec<ACL>,
    pub flags: CreateMode,
}

impl Request for CreateRequest {
    type Response = CreateResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct DeleteRequest {
    pub path: String,
    pub version: OptionalVersion,
}

impl Request for DeleteRequest {
    type Response = ();
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetChildrenRequest {
    pub path: String,
    pub watch: bool,
}

impl Request for GetChildrenRequest {
    type Response = GetChildrenResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetChildren2Request {
    pub path: String,
    pub watch: bool,
}

impl Request for GetChildren2Request {
    type Response = GetChildren2Response;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CheckVersionRequest {
    pub path: String,
    pub version: Version,
}

impl Request for CheckVersionRequest {
    type Response = ();
}

/// Doesn't seem to be used in the ZK server code base
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetMaxChildrenRequest {
    pub path: String,
}

impl Request for GetMaxChildrenRequest {
    type Response = GetMaxChildrenResponse;
}

/// Doesn't seem to be used in the ZK server code base
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetMaxChildrenResponse {
    pub max: i32,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetMaxChildrenRequest {
    pub path: String,
    pub max: i32,
}

impl Request for SetMaxChildrenRequest {
    type Response = ();
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SyncRequest {
    pub path: String,
}

impl Request for SyncRequest {
    type Response = SyncResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SyncResponse {
    pub path: String,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetACLRequest {
    pub path: String,
}

impl Request for GetACLRequest {
    type Response = GetACLResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetACLRequest {
    pub path: String,
    pub acl: Vec<ACL>,
    pub version: OptionalVersion,
}

impl Request for SetACLRequest {
    type Response = SetACLResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct SetACLResponse {
    pub stat: Stat,
}

// See Watcher.java
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub enum WatcherEventType {
    None = -1,
    NodeCreated = 1,
    NodeDeleted = 2,
    NodeDataChanged = 3,
    NodeChildrenChanged = 4,
    DataWatchRemoved = 5,
    ChildWatchRemoved = 6,
}

// See Watcher.java
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub enum KeeperState {
    /// The client is in the disconnected state - it is not connected
    /// to any server in the ensemble.
    Disconnected = 0,

    /// The client is in the connected state - it is connected
    /// to a server in the ensemble (one of the servers specified
    /// in the host connection parameter during ZooKeeper client
    /// creation).
    SyncConnected = 3,

    /// Auth failed state
    AuthFailed = 4,

    /// The client is connected to a read-only server, that is the
    /// server which is not currently connected to the majority.
    /// The only operations allowed after receiving this state is
    /// read operations.
    /// This state is generated for read-only clients only since
    /// read/write clients aren't allowed to connect to r/o servers.
    ConnectedReadOnly = 5,

    /// SaslAuthenticated: used to notify clients that they are SASL-authenticated,
    /// so that they can perform Zookeeper actions with their SASL-authorized permissions.
    SaslAuthenticated = 6,

    /// The serving cluster has expired this session. The ZooKeeper
    /// client connection (the session) is no longer valid. You must
    /// create a new client connection (instantiate a new ZooKeeper
    /// instance) if you with to access the ensemble. */
    Expired = -112,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct WatcherEvent {
    #[serde(rename = "type")]
    pub typ: WatcherEventType,
    /// State of the Keeper client runtime
    pub state: KeeperState,
    pub path: String,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    pub err: ErrorCode,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CreateResponse {
    pub path: String,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct Create2Response {
    pub path: String,
    pub stat: Stat,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct ExistsRequest {
    pub path: String,
    pub watch: bool,
}

impl Request for ExistsRequest {
    type Response = ExistsResponse;
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct ExistsResponse {
    pub stat: Stat,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetDataResponse {
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub stat: Stat,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetChildrenResponse {
    /// Name of children (not the full path)
    pub children: Vec<String>,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetChildren2Response {
    pub children: Vec<String>,
    pub stat: Stat,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct GetACLResponse {
    pub acl: Vec<ACL>,
    pub stat: Stat,
}

// See Watcher.java
#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub enum WatcherType {
    Children = 1,
    Data = 2,
    Any = 3,
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct CheckWatchesRequest {
    pub path: String,
    #[serde(rename = "type")]
    pub typ: WatcherType,
}

impl Request for CheckWatchesRequest {
    type Response = ();
}

#[derive(Debug)]
#[derive(Serialize, Deserialize)]
pub struct RemoveWatchesRequest {
    pub path: String,
    #[serde(rename = "type")]
    pub typ: WatcherType,
}

impl Request for RemoveWatchesRequest {
    type Response = ();
}
