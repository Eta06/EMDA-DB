//! Snapshot verification and bounded replay into an unpublished target.
use super::OnlineCompactionOptions;
use crate::log::ValueLocation;
use crate::store::{Snapshot, Store};
use crate::{BatchOperation, MAX_ATOMIC_BATCH_BYTES};
use std::collections::BTreeMap;
use std::io;

const MAX_GROUP_RECORDS: usize = 64;
const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    InitialCopy,
    BeforeFinalBarrier,
    BeforeExchange,
    AfterExchange,
}

type OwnedOperation = (Vec<u8>, Option<Vec<u8>>);

pub(super) fn boundary_locked(source: &Store) -> io::Result<u64> {
    if source.commits.failed() {
        return Err(io::Error::other(
            "source write outcome is uncertain; reopen required",
        ));
    }
    source.log.verify_witness()?;
    source.log.len()
}
pub(super) fn boundary(source: &Store) -> io::Result<u64> {
    let _writer = source
        .writes
        .write()
        .map_err(|_| io::Error::other("write barrier poisoned"))?;
    boundary_locked(source)
}
pub(super) fn capture<'a>(
    source: &'a Store,
    options: &OnlineCompactionOptions,
) -> io::Result<(Snapshot<'a>, u64)> {
    let _writer = source
        .writes
        .write()
        .map_err(|_| io::Error::other("write barrier poisoned"))?;
    let snapshot = source.snapshot(options.max_snapshot_keys, options.max_snapshot_key_bytes)?;
    let end = boundary_locked(source)?;
    Ok((snapshot, end))
}

pub(super) fn verify_initial(
    source: &Store,
    snapshot: &Snapshot<'_>,
    end: u64,
    options: &OnlineCompactionOptions,
) -> io::Result<()> {
    let mut index = BTreeMap::<Vec<u8>, ValueLocation>::new();
    let mut bytes = 0_usize;
    source
        .log
        .visit_synchronized_range(0, end, |key, location| {
            match location {
                Some(location) => {
                    if !index.contains_key(&key) {
                        if index.len() >= options.max_snapshot_keys {
                            return Err(io::Error::other(
                                "historical key count exceeds compaction limit",
                            ));
                        }
                        bytes = bytes
                            .checked_add(key.len())
                            .filter(|bytes| *bytes <= options.max_snapshot_key_bytes)
                            .ok_or_else(|| {
                                io::Error::other("historical key bytes exceed compaction limit")
                            })?;
                    }
                    index.insert(key, location);
                }
                None => {
                    if index.remove(&key).is_some() {
                        bytes -= key.len();
                    }
                }
            }
            Ok(())
        })?;
    if index != snapshot.read_index()?.index {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot does not match verified log prefix",
        ));
    }
    Ok(())
}

struct Writer<'a> {
    target: &'a Store,
    operations: Vec<OwnedOperation>,
    bytes: usize,
}
impl<'a> Writer<'a> {
    fn new(target: &'a Store) -> Self {
        Self {
            target,
            operations: Vec::new(),
            bytes: 4,
        }
    }
    fn push(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> io::Result<()> {
        let length = crate::record::HEADER_SIZE
            + key.len()
            + value.as_ref().map_or(0, Vec::len)
            + crate::record::CHECKSUM_SIZE;
        if !self.operations.is_empty()
            && (self.operations.len() >= MAX_GROUP_RECORDS || self.bytes + length > MAX_GROUP_BYTES)
        {
            self.flush()?;
        }
        if length + 4 > MAX_ATOMIC_BATCH_BYTES {
            match value {
                Some(value) => self.target.put(&key, &value)?,
                None => self.target.delete(&key)?,
            }
        } else {
            self.bytes += length;
            self.operations.push((key, value));
        }
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.operations.is_empty() {
            return Ok(());
        }
        let operations = self
            .operations
            .iter()
            .map(|(key, value)| match value {
                Some(value) => BatchOperation::Put { key, value },
                None => BatchOperation::Delete { key },
            })
            .collect::<Vec<_>>();
        self.target.write_batch(&operations)?;
        self.operations.clear();
        self.bytes = 4;
        Ok(())
    }
}

pub(super) fn copy_snapshot(
    source: &Store,
    snapshot: &Snapshot<'_>,
    target: &Store,
) -> io::Result<()> {
    let index = snapshot.read_index()?;
    let mut writer = Writer::new(target);
    for (key, location) in &index.index {
        writer.push(key.clone(), Some(source.log.read(*location)?))?;
    }
    writer.flush()
}
pub(super) fn replay(source: &Store, target: &Store, start: u64, end: u64) -> io::Result<()> {
    let mut writer = Writer::new(target);
    source
        .log
        .visit_synchronized_range(start, end, |key, location| {
            let value = location
                .map(|location| source.log.read(location))
                .transpose()?;
            writer.push(key, value)
        })?;
    writer.flush()
}
pub(super) fn verify_target(source: &Store, target: &Store) -> io::Result<()> {
    let source_index = source.read()?;
    let target_index = target.read()?;
    target.log.verify_index(&target_index.index)?;
    if source_index.index.len() != target_index.index.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "compacted key count mismatch",
        ));
    }
    for (key, location) in &source_index.index {
        let target_location = target_index
            .index
            .get(key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "compacted key missing"))?;
        if source.log.read(*location)? != target.log.read(*target_location)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted value mismatch",
            ));
        }
    }
    Ok(())
}
