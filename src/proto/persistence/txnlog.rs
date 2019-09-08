use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde::Deserialize;
use named_type_derive::NamedType;
use named_type::NamedType;

use crate::proto::proto::ErrorCode;
use crate::proto::proto::OpCode;
use crate::proto::SessionId;
use crate::proto::Timestamp;
use crate::proto::Xid;
use crate::proto::Zxid;
use crate::proto::txn::*;

use std::path::Path;
use std::io::BufReader;
use std::fs::File;
use failure::Error;
use std::iter::Iterator;

#[derive(Deserialize, Serialize)]
pub struct Txn {
    pub client_id: SessionId,
    pub cxid: Xid,
    pub zxid: Zxid,
    pub time: Timestamp,
    pub op: TxnOperation,
}

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

/// A ZooKeeper transaction log file. After the initial header, it is composed of
///
/// See [`LogFormatter.java`] and [`SerializeUtils.java`] for details.
///
/// [`LogFormatter.java`]: https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/server/LogFormatter.java
/// [`SerializeUtils.java`]: https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/server/util/SerializeUtils.java
///
pub struct TxnlogFile {
    deser: crate::serde::Deserializer<BufReader<File>>,
    done: bool,
}

impl TxnlogFile {

    pub fn new(path: impl AsRef<Path>) -> Result<TxnlogFile, Error> {
        let file = BufReader::new(File::open(path)?);

        let mut deser = crate::serde::de::from_reader(file);
        deser.add_enum_mapping::<OpCode, TxnOperation>();
        deser.add_enum_mapping::<ErrorCode, ErrorCode>();

        let header = super::FileHeader::deserialize(&mut deser)?;

        if header.magic != super::TXNLOG_MAGIC {
            return Err(failure::err_msg("Wrong magic number"));
        }

        if header.version != 2 {
            return Err(failure::err_msg("Wrong version number"));
        }

        Ok(TxnlogFile{
            deser,
            done: false,
        })
    }
}

impl Iterator for TxnlogFile {
    type Item = Result<Txn, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // Evaluate an expression returning a result and end the iterator if it's an error.
        macro_rules! try_or_end {
            ($e:expr) => (
                match $e {
                    Ok(v) => v,
                    Err(e) => { self.done = true; return Some(Err(e.into())); }
                }
            )
        }

        if self.done {
            return None;
        }

        // An Adler-32 CRC of the bytes that represent the txn (without the length)
        let _crc = try_or_end!(<u64>::deserialize(&mut self.deser));

        let length = try_or_end!(<u32>::deserialize(&mut self.deser));
        if length == 0 {
            self.done = true;
            return None;
        }

        let txn = try_or_end!(Txn::deserialize(&mut self.deser));

        // Next byte must be 'B' (0x42) (see LogFormatter.java & o.a.z.s.persistence.Util.java)
        let b = try_or_end!(<u8>::deserialize(&mut self.deser));
        if b != 0x42 {
            self.done = true;
            return Some(Err(failure::err_msg("Last transaction was partial.")));
        }

        Some(Ok(txn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_tnxlog() {
        let tnxlog = TxnlogFile::new("data/version-2/log.200000001").unwrap();

        tnxlog.for_each(|x| {
            let txn = x.unwrap();
            println!("{}", serde_json::to_string(&txn).unwrap())
        });
    }
}
