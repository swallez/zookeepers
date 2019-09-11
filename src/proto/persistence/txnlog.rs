use serde::Deserialize;

use crate::proto::proto::ErrorCode;
use crate::proto::proto::OpCode;
use crate::proto::txn::*;
use crate::proto::Zxid;
use crate::serde::EnumEncoding;
use failure::Error;
use std::fs::File;
use std::io::BufReader;
use std::iter::Iterator;
use std::path::Path;
use std::path::PathBuf;

use itertools::Itertools;

/// A ZooKeeper transaction log file. After the initial header, it is a sequence of transactions.
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
    /// Find transactions in the logs that include or are after `snapshot_zxid`.
    ///
    /// If txnlog files are found and open successfully, returns an iterator on the transactions.
    ///
    pub fn find_txnlog(
        dir: impl AsRef<Path>,
        snapshot_zxid: Zxid,
    ) -> Result<impl Iterator<Item = Result<Txn, Error>>, Error> {
        let paths = Self::find_txnlog_paths(dir, snapshot_zxid)?;

        // Open all txnfiles, failing if one can't be opened
        let files =
            paths
                .into_iter()
                .map(|path| TxnlogFile::new(path))
                .fold_results(Vec::new(), |mut vec, txnlog| {
                    vec.push(txnlog);
                    vec
                })?;

        // Flatmap all files, keeping only transactions >= snapshot_zxid
        let txns = files.into_iter().flat_map(|v| v).filter(move |r| match r {
            Ok(txn) if txn.header.zxid < snapshot_zxid => false,
            _ => true,
        });

        Ok(txns)
    }

    /// Find transaction log files that include or are after `snapshot_zxid`.
    ///
    pub fn find_txnlog_paths(dir: impl AsRef<Path>, snapshot_zxid: Zxid) -> Result<Vec<PathBuf>, Error> {
        //
        // Collect log files as (zxid, path) pairs
        let mut zxid_paths = std::fs::read_dir(dir)?
            .filter_map(|r| r.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
                    .starts_with("log.")
            })
            .filter_map(|path| super::zxid_from_path(&path).map(|zxid| (zxid, path)))
            .collect::<Vec<_>>();

        zxid_paths.sort_by(|(zxid1, _), (zxid2, _)| zxid1.cmp(&zxid2));

        // Find the highest zxid that is <= snapshot_zxid
        let max_zxid = zxid_paths
            .iter()
            .map(|(zxid, _)| *zxid)
            .filter(|zxid| zxid <= &snapshot_zxid)
            .max()
            .ok_or_else(|| format_err!("No txnlogs found before zxid {:x}", snapshot_zxid.0))?;

        let result = zxid_paths
            .into_iter()
            .filter_map(|(zxid, path)| if zxid < max_zxid { None } else { Some(path) })
            .collect();

        Ok(result)
    }

    pub fn new(path: impl AsRef<Path>) -> Result<TxnlogFile, Error> {
        let file = BufReader::new(File::open(path)?);
        let mut deser = crate::serde::de::from_reader(file);

        // We read length separately for TxnOperations as zero indicates EOF
        deser.add_enum_mapping::<OpCode, TxnOperation>(EnumEncoding::Type);
        deser.add_enum_mapping::<OpCode, MultiTxnOperation>(EnumEncoding::TypeThenLength);
        deser.add_enum::<ErrorCode>();

        let header = super::FileHeader::deserialize(&mut deser)?;

        if header.magic != super::TXNLOG_MAGIC {
            return Err(failure::err_msg("Wrong magic number"));
        }

        if header.version != 2 {
            return Err(failure::err_msg("Wrong version number"));
        }

        Ok(TxnlogFile { deser, done: false })
    }
}

impl Iterator for TxnlogFile {
    type Item = Result<Txn, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        fn read_next(this: &mut TxnlogFile) -> Result<Option<Txn>, Error> {
            // An Adler-32 CRC of the bytes that represent the txn (without the length)
            let _crc = <u64>::deserialize(&mut this.deser)?;

            let length = <u32>::deserialize(&mut this.deser)?;
            if length == 0 {
                // Txnlog files are 64MB pre-allocated files, and zero length indicates end of log
                return Ok(None);
            }

            let txn = Txn::deserialize(&mut this.deser)?;

            // Next byte must be 'B' (0x42) (see LogFormatter.java & o.a.z.s.persistence.Util.java)
            let b = <u8>::deserialize(&mut this.deser)?;
            if b != 0x42 {
                return Err(failure::err_msg("Last transaction was partial."));
            }

            Ok(Some(txn))
        }

        if self.done {
            None
        } else {
            let result = read_next(self).transpose();
            self.done = result.is_none();
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::persistence::zxid_from_path;

    #[test]
    fn read_tnxlog() {
        let tnxlog = TxnlogFile::new("data/version-2/log.200000001").unwrap();
        //let tnxlog = TxnlogFile::new("data/version-2/log.100000001").unwrap();

        let mut count = 0;
        tnxlog.for_each(|x| {
            let txn = x.unwrap();
            //println!("{:?}", txn);
            count += 1;
        });

        println!("{} transactions", count);
    }

    #[test]
    fn read_tnxs() {
        let tnxlog = TxnlogFile::find_txnlog("data/version-2", zxid_from_path("log.200000001").unwrap()).unwrap();

        let mut count = 0;
        tnxlog.for_each(|x| {
            let txn = x.unwrap();
            //println!("{:?}", txn);
            count += 1;
        });

        println!("{} transactions", count);
    }
}
