use serde::Deserialize;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::proto::Duration;
use crate::proto::SessionId;
use crate::proto::StatPersisted;
use crate::proto::Zxid;
use crate::proto::ACL;

use failure::Error;
use std::fs::File;
use std::io::BufReader;
use std::iter::Iterator;
use std::path::Path;

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
pub struct ACLRef(i64);

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
pub struct Session {
    pub id: SessionId,
    pub timeout: Duration,
}

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
pub struct ACLCacheEntry {
    pub entry_id: ACLRef,
    pub acl: Vec<ACL>,
}

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
pub struct DataNode {
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
    acl: ACLRef,
    stat: StatPersisted,
}

/// A ZooKeeper snapshot file. After the initial header, it is composed of 3 sections:
/// - information about sessions
/// - acl cache, used in data nodes
/// - data nodes
///
/// Each section is implemented as type state implementing iterator for the type related to that
/// section (sessions, acls, data nodes).
///
/// See [`SnapshotFormatter.java`] and [`SerializeUtils.java`] for details.
///
/// [`SnapshotFormatter.java`]: https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/server/SnapshotFormatter.java
/// [`SerializeUtils.java`]: https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/server/util/SerializeUtils.java
///
pub struct SnapshotFile<S> {
    deser: crate::serde::Deserializer<BufReader<File>>,
    count: usize,
    errored: bool,
    state: S,
}

//--------------------------------------------------------------------------------------------------
// Part 1: header

pub struct InitState {
    zxid: Zxid,
}

impl SnapshotFile<InitState> {
    /// Find the most recent snapshot in a directory
    pub fn most_recent_snapshot(dir: impl AsRef<Path>) -> Result<Option<SnapshotFile<InitState>>, Error> {
        let mut snapshot_paths = std::fs::read_dir(dir)?
            .filter_map(|r| r.ok())
            .map(|entry| entry.path())
            .filter(|path| path.starts_with("snapshot."))
            .collect::<Vec<_>>();

        snapshot_paths.sort();

        snapshot_paths.pop().map(|path| Self::new(path)).transpose()
    }

    pub fn new(path: impl AsRef<Path>) -> Result<SnapshotFile<InitState>, Error> {
        let path = path.as_ref();

        let zxid =
            super::zxid_from_path(path).ok_or_else(|| format_err!("Can't parse version in path {}", path.display()))?;

        let file = BufReader::new(File::open(path)?);

        let mut deser = crate::serde::de::from_reader(file);
        let header = super::FileHeader::deserialize(&mut deser)?;

        if header.magic != super::SNAP_MAGIC {
            return Err(failure::err_msg("Wrong magic number"));
        }

        if header.version != 2 {
            return Err(failure::err_msg("Wrong version number"));
        }

        Ok(SnapshotFile {
            deser,
            count: 0,
            errored: false,
            state: InitState { zxid },
        })
    }

    /// The transaction id for this snapshot
    pub fn zxid(&self) -> Zxid {
        self.state.zxid
    }

    /// Transition to session information
    pub fn sessions(self) -> Result<SnapshotFile<SessionsState>, Error> {
        SnapshotFile::new_sessions(self)
    }
}

/// Generic implementation of Iterator::next
fn next_item<'de, T: Deserialize<'de>, S>(snap: &mut SnapshotFile<S>) -> Option<Result<T, Error>> {
    if snap.count == 0 || snap.errored {
        return None;
    }
    snap.count -= 1;

    let r = T::deserialize(&mut snap.deser);
    if r.is_err() {
        snap.errored = true;
    }

    Some(r.map_err(|e| e.into()))
}

//--------------------------------------------------------------------------------------------------
// Part 2: sessions

pub struct SessionsState {}

impl SnapshotFile<SessionsState> {
    fn new_sessions<T>(mut prev: SnapshotFile<T>) -> Result<Self, Error> {
        let count = <i32>::deserialize(&mut prev.deser)? as usize;
        Ok(SnapshotFile {
            deser: prev.deser,
            count,
            errored: false,
            state: SessionsState {},
        })
    }

    /// Transition to ACL cache entries. It will skip any session states that have not been
    /// read yet.
    pub fn acls(mut self) -> Result<SnapshotFile<ACLCacheState>, Error> {
        // drain iterator
        self.last();

        if self.errored {
            return Err(failure::err_msg("Stream already errored out"));
        }

        SnapshotFile::<ACLCacheState>::new_acl_cache(self)
    }
}

/// Iterate on the sessions contained in this snapshot
///
/// Note: implemented on `&mut SnapshotFile` so that we can use functions that consume the iterator
/// while still being able to use the object to move to the next state.
///
impl Iterator for &mut SnapshotFile<SessionsState> {
    type Item = Result<Session, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        next_item(self)
    }
}

//--------------------------------------------------------------------------------------------------
// Part 3: ACL cache

pub struct ACLCacheState {}

impl SnapshotFile<ACLCacheState> {
    fn new_acl_cache<T>(mut prev: SnapshotFile<T>) -> Result<SnapshotFile<ACLCacheState>, Error> {
        let count = <i32>::deserialize(&mut prev.deser)? as usize;
        Ok(SnapshotFile {
            deser: prev.deser,
            count,
            errored: false,
            state: ACLCacheState {},
        })
    }

    /// Transition to data nodes. It will skip any ACL cache entries that have not been read yet.
    pub fn data_nodes(mut self) -> Result<SnapshotFile<DataNodesState>, Error> {
        // drain iterator
        self.last();

        if self.errored {
            return Err(failure::err_msg("Stream already errored out"));
        }

        SnapshotFile::<DataNodesState>::new_data_nodes(self)
    }
}

impl Iterator for &mut SnapshotFile<ACLCacheState> {
    type Item = Result<ACLCacheEntry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        next_item(self)
    }
}

//--------------------------------------------------------------------------------------------------
// Part 4: data nodes

pub struct DataNodesState {}

impl SnapshotFile<DataNodesState> {
    fn new_data_nodes<T>(prev: SnapshotFile<T>) -> Result<SnapshotFile<DataNodesState>, Error> {
        // We don't have a count of entries for this section. This is a series of (path, data) and
        // the section ends when we see a "/" path.

        Ok(SnapshotFile {
            deser: prev.deser,
            count: 1,
            errored: false,
            state: DataNodesState {},
        })
    }
}

impl Iterator for SnapshotFile<DataNodesState> {
    type Item = Result<(String, DataNode), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 || self.errored {
            return None;
        }

        let path = match <String>::deserialize(&mut self.deser) {
            Ok(p) => p,
            Err(e) => {
                self.errored = true;
                return Some(Err(e.into()));
            }
        };

        if &path == "/" {
            self.count = 0;
            return None;
        }

        let data = match <DataNode>::deserialize(&mut self.deser) {
            Ok(d) => d,
            Err(e) => {
                self.errored = true;
                return Some(Err(e.into()));
            }
        };

        Some(Ok((path, data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_snapshot() {
        let snap = SnapshotFile::new("data/version-2/snapshot.1000005d0").unwrap();
        let zxid = snap.zxid();

        println!("{:?}", zxid);

        let mut snap = snap.sessions().unwrap();

        // println!("sessions: {}", snap.count);
        &snap.for_each(|x| {
            let session = x.unwrap();
            //println!("{:?}", session);
        });

        let mut snap = snap.acls().unwrap();

        // println!("acls: {}", snap.count);
        &snap.for_each(|x| {
            let acl = x.unwrap();
            //println!("{:?}", acl);
        });

        let snap = snap.data_nodes().unwrap();

        // println!("data nodes:");
        let mut max_zxid = Zxid(0);

        &snap.for_each(|x| {
            let (path, mut node) = x.unwrap();
            let len = node.data.len();
            node.data = Vec::new();

            max_zxid = std::cmp::max(max_zxid, node.stat.czxid);
            max_zxid = std::cmp::max(max_zxid, node.stat.mzxid);

            // println!("{} - {} bytes", path, len);
            //println!("{:?}", node);
        });

        assert_eq!(zxid, max_zxid);
    }
}
