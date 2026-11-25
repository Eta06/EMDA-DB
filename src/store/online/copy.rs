//! Snapshot verification and bounded replay into an unpublished target.
use super::OnlineCompactionOptions;
use crate::log::ValueLocation;
use crate::store::compact::RestoreSize;
use crate::store::{Snapshot, Store};
use crate::{BatchOperation, MAX_ATOMIC_BATCH_BYTES};
use sha2::Sha256;
use std::borrow::Cow;
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

pub(super) use crate::store::source_read_budget::SourceReadBudget;

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
    snapshot: &Snapshot<'_>,
    end: u64,
    options: &OnlineCompactionOptions,
    digest: Option<&mut Sha256>,
    budget: &mut SourceReadBudget,
) -> io::Result<()> {
    budget.charge(end)?;
    let mut index = BTreeMap::<Vec<u8>, ValueLocation>::new();
    let mut bytes = 0_usize;
    let data = snapshot.read_index()?;
    let log = data.log.as_ref().expect("active snapshot retains its log");
    let visit = |key: Cow<'_, [u8]>, location| {
        match location {
            Some(location) => {
                if let Some(existing) = index.get_mut(key.as_ref()) {
                    *existing = location;
                } else {
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
                    index.insert(key.into_owned(), location);
                }
            }
            None => {
                if index.remove(key.as_ref()).is_some() {
                    bytes -= key.len();
                }
            }
        }
        Ok(())
    };
    if let Some(digest) = digest {
        log.visit_records_with_digest(0, end, end, digest, visit)?;
    } else {
        log.visit_synchronized_range(0, end, visit)?;
    }
    if &index != data.index.as_ref() {
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
    snapshot: &Snapshot<'_>,
    target: Store,
    budget: &mut SourceReadBudget,
) -> io::Result<u64> {
    let index = snapshot.read_index()?;
    let log = index.log.as_ref().expect("active snapshot retains its log");
    let mut size = RestoreSize {
        entries: index.index.len() as u64,
        keys: 0,
        payload: 0,
    };
    for (key, location) in index.index.iter() {
        size.keys = size
            .keys
            .checked_add(key.len() as u64)
            .ok_or_else(copy_size_overflow)?;
        size.payload = size
            .payload
            .checked_add((key.len() + location.length) as u64)
            .ok_or_else(copy_size_overflow)?;
    }
    budget.charge(size.payload - size.keys)?;
    let (end, ()) = target.write_private_copy(size, |copy| {
        for (key, &location) in index.index.iter() {
            copy.put_log_value(key, log, location, |_| {})?;
        }
        Ok(())
    })?;
    Ok(end)
}

fn copy_size_overflow() -> io::Error {
    io::Error::other("online compaction snapshot size overflow")
}
pub(super) fn replay(
    source: &Store,
    target: &Store,
    start: u64,
    end: u64,
    digest: &mut Sha256,
    budget: &mut SourceReadBudget,
) -> io::Result<()> {
    let bytes = end
        .checked_sub(start)
        .ok_or_else(|| io::Error::other("source log boundary moved backward"))?;
    budget.charge(bytes)?;
    let mut writer = Writer::new(target);
    source
        .log
        .visit_records_with_digest(start, end, end, digest, |key, location| {
            let value = location
                .map(|location| {
                    budget.charge(location.length as u64)?;
                    source.log.read(location)
                })
                .transpose()?;
            writer.push(key.into_owned(), value)
        })?;
    writer.flush()
}
pub(super) fn verify_target(
    source: &Store,
    target: &Store,
    budget: &mut SourceReadBudget,
) -> io::Result<()> {
    let source_index = source.read()?;
    let target_index = target.read()?;
    target.log.verify_index(&target_index.index)?;
    if source_index.index.len() != target_index.index.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "compacted key count mismatch",
        ));
    }
    for (key, location) in source_index.index.iter() {
        let target_location = target_index
            .index
            .get(key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "compacted key missing"))?;
        budget.charge(location.length as u64)?;
        if source.log.read(*location)? != target.log.read(*target_location)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted value mismatch",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_reads_use_captured_log_and_reject_cancelled_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let source = Store::open(root.path().join("source")).unwrap();
        let captured = Store::open(root.path().join("captured")).unwrap();
        let target_path = root.path().join("target");
        let target = Store::open(&target_path).unwrap();
        // Identical record layouts give the two logs overlapping locations.
        source.put(b"key", b"source!!").unwrap();
        captured.put(b"key", b"captured").unwrap();
        let options = OnlineCompactionOptions::default();
        let (mut snapshot, end) = capture(&captured, &options).unwrap();
        let mut budget = SourceReadBudget(Some(end + 8));
        verify_initial(&snapshot, end, &options, None, &mut budget).unwrap();
        let end_copied = copy_snapshot(&snapshot, target, &mut budget).unwrap();
        let target = Store::open(&target_path).unwrap();
        assert_eq!(target.log.len().unwrap(), end_copied);
        assert_eq!(budget.0, Some(0));
        assert_eq!(target.get(b"key").unwrap(), Some(b"captured".to_vec()));
        assert_eq!(source.get(b"key").unwrap(), Some(b"source!!".to_vec()));
        assert!(verify_initial(&snapshot, 0, &options, None, &mut budget).is_err());

        let rejected_path = root.path().join("rejected");
        let rejected = Store::open(&rejected_path).unwrap();
        let initial = rejected.log.len().unwrap();
        let error = copy_snapshot(&snapshot, rejected, &mut SourceReadBudget(Some(7))).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let rejected = Store::open(&rejected_path).unwrap();
        assert_eq!(rejected.log.len().unwrap(), initial);
        assert_eq!(rejected.get(b"key").unwrap(), None);

        snapshot.cancel();
        let mut unrestricted = SourceReadBudget(None);
        assert!(verify_initial(&snapshot, end, &options, None, &mut unrestricted).is_err());
        let cancelled_target = Store::open(root.path().join("cancelled")).unwrap();
        assert!(copy_snapshot(&snapshot, cancelled_target, &mut unrestricted).is_err());
    }
}
