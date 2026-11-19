//! Verified, non-overwriting compaction to a separate store.
use super::*;
use crate::log::AppendRecord;
use crate::retained_base_v2::index::OverlayValue;

pub(super) const MAX_COPY_RECORDS: usize = 64;
pub(super) const MAX_COPY_BYTES: usize = 4 * 1024 * 1024;
const LARGE_RESTORE_BUFFER_BYTES: usize = 1024 * 1024;

pub(super) struct CopyRecord {
    pub(super) encoded: Vec<u8>,
    pub(super) key_len: usize,
    pub(super) value_len: usize,
    pub(super) checksum: u32,
}

pub(super) fn flush_copy(target: &Log, records: &mut Vec<CopyRecord>) -> io::Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    target.append_batch_unsynced(records.iter().map(|entry| AppendRecord {
        encoded: &entry.encoded,
        key_len: entry.key_len,
        value_len: entry.value_len,
        checksum: entry.checksum,
    }))?;
    target.sync_data()?;
    records.clear();
    Ok(())
}

fn copy_live_records(
    source: &Log,
    index: &BTreeMap<Vec<u8>, ValueLocation>,
    target: &Log,
) -> io::Result<()> {
    let mut copy = GroupedCopy::new(target, None);
    for (key, &location) in index {
        copy.put(key, location.length, || source.read(location))?;
    }
    copy.finish()
}

#[derive(Clone, Copy)]
pub(crate) struct RestoreSize {
    pub(crate) entries: u64,
    pub(crate) keys: u64,
    pub(crate) payload: u64,
}

impl RestoreSize {
    fn encoded_bytes(self) -> io::Result<u64> {
        self.entries
            .checked_mul((record::HEADER_SIZE + record::CHECKSUM_SIZE) as u64)
            .and_then(|overhead| overhead.checked_add(self.payload))
            .ok_or_else(|| io::Error::other("restored log size overflow"))
    }

    fn consume(&mut self, key_len: usize, value_len: usize) -> io::Result<()> {
        let exceeded = || record::invalid_data("restored output exceeded admitted size");
        self.entries = self.entries.checked_sub(1).ok_or_else(exceeded)?;
        self.keys = self.keys.checked_sub(key_len as u64).ok_or_else(exceeded)?;
        self.payload = self
            .payload
            .checked_sub(key_len as u64)
            .and_then(|n| n.checked_sub(value_len as u64))
            .ok_or_else(exceeded)?;
        Ok(())
    }
}

pub(crate) struct GroupedCopy<'a> {
    target: &'a Log,
    records: Vec<CopyRecord>,
    bytes: usize,
    remaining: Option<RestoreSize>,
}

impl<'a> GroupedCopy<'a> {
    fn new(target: &'a Log, remaining: Option<RestoreSize>) -> Self {
        Self {
            target,
            records: Vec::with_capacity(MAX_COPY_RECORDS),
            bytes: 0,
            remaining,
        }
    }

    fn put(
        &mut self,
        key: &[u8],
        value_len: usize,
        read: impl FnOnce() -> io::Result<Vec<u8>>,
    ) -> io::Result<()> {
        if let Some(remaining) = &mut self.remaining {
            remaining.consume(key.len(), value_len)?;
        }
        let length = record::HEADER_SIZE + key.len() + value_len + record::CHECKSUM_SIZE;
        if !self.records.is_empty()
            && (self.records.len() == MAX_COPY_RECORDS || self.bytes + length > MAX_COPY_BYTES)
        {
            flush_copy(self.target, &mut self.records)?;
            self.bytes = 0;
        }
        let value = read()?;
        if value.len() != value_len {
            return Err(record::invalid_data("copy source value length changed"));
        }
        let checksum = crc32fast::hash(&value);
        let encoded = record::encode(Operation::Put, key, &value)?;
        self.bytes += encoded.len();
        self.records.push(CopyRecord {
            encoded,
            key_len: key.len(),
            value_len: value.len(),
            checksum,
        });
        Ok(())
    }

    /// Small values share grouped synchronization. A record that cannot fit
    /// in a group uses the fixed-size authenticated reader and its own sync.
    pub(crate) fn put_retained(
        &mut self,
        key: &[u8],
        value: OverlayValue<'_>,
        before_value: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        let value_len = value.len();
        if record::HEADER_SIZE + key.len() + value_len + record::CHECKSUM_SIZE <= MAX_COPY_BYTES {
            return self.put(key, value_len, || {
                before_value()?;
                value.to_owned_bytes()
            });
        }
        if let Some(remaining) = &mut self.remaining {
            remaining.consume(key.len(), value_len)?;
        }
        flush_copy(self.target, &mut self.records)?;
        self.bytes = 0;
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(LARGE_RESTORE_BUFFER_BYTES)
            .map_err(io::Error::other)?;
        scratch.resize(LARGE_RESTORE_BUFFER_BYTES, 0);
        before_value()?;
        self.target.append_private_put(key, value_len, |append| {
            value.visit_chunks_with(&mut scratch, append)
        })
    }

    fn finish(mut self) -> io::Result<()> {
        if self
            .remaining
            .is_some_and(|size| size.entries != 0 || size.keys != 0 || size.payload != 0)
        {
            return Err(record::invalid_data(
                "restored output shorter than admitted size",
            ));
        }
        flush_copy(self.target, &mut self.records)
    }
}

impl Store {
    // The retained restore owns this unpublished, newly initialized store.
    // Admit the final stream before appending, and check the count and exact
    // bytes while writing. The stage is reopened and verified before publication.
    pub(crate) fn write_private_restore<T>(
        self,
        size: RestoreSize,
        write: impl FnOnce(&mut GroupedCopy<'_>) -> io::Result<T>,
    ) -> io::Result<(u64, T)> {
        if self
            .max_live_keys
            .is_some_and(|max| size.entries > max as u64)
        {
            return Err(io::Error::other(
                "configured live key limit would be exceeded",
            ));
        }
        if self
            .max_live_key_bytes
            .is_some_and(|max| size.keys > max as u64)
        {
            return Err(io::Error::other(
                "configured live key byte limit would be exceeded",
            ));
        }
        let bytes = size.encoded_bytes()?;
        let end = self
            .log
            .len()?
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("restored log size overflow"))?;
        let result = {
            let _reservation = self
                .log_quota
                .as_ref()
                .map(|quota| quota.reserve(bytes))
                .transpose()?;
            let mut copy = GroupedCopy::new(&self.log, Some(size));
            let result = write(&mut copy)?;
            copy.finish()?;
            result
        };
        self.close_private_store()?;
        Ok((end, result))
    }

    /// Copies current live keys into a verified new store with no old records.
    ///
    /// Mutations wait for the copy and verification; existing readers can
    /// continue. The source remains open and unchanged, and callers control any
    /// later switchover. The destination must not exist. An error after rename
    /// can leave a complete destination whose directory sync was unsuccessful.
    /// This operation currently requires memory for additional full indexes and
    /// is not a bounded-memory or online in-place compaction mechanism.
    pub fn compact_to(&self, destination: impl AsRef<Path>) -> io::Result<()> {
        self.compact_to_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    /// Creates a compacted copy with structured destination-publication
    /// failures. A pre-existing destination or staging artifacts may remain;
    /// an uncertain outcome needs reconciliation before retrying.
    pub fn compact_to_detailed(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<(), crate::PublicationFailure> {
        self.compact_to_detailed_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    /// Compacts after admitting the historical verification replay under caller limits.
    ///
    /// The limits bound encoded log length and logical live key count and key
    /// bytes during replay. They do not bound peak allocations or process memory:
    /// replay buffers, map overhead, existing indexes, values, and copy buffers
    /// also consume memory. Rejection returns `NotPublished` with `InvalidInput`;
    /// it does not certify that the log is valid. Successful admission still
    /// performs full verification before publication.
    pub fn compact_to_with_verification_limits(
        &self,
        destination: impl AsRef<Path>,
        limits: crate::PeakVerificationLimits,
    ) -> Result<(), crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.compact_to_using_tracked(
                destination.as_ref(),
                crate::directory::sync_existing,
                copy_live_records,
                Some(tracker),
                Some(limits),
            )
        })
    }

    fn compact_to_detailed_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<(), crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.compact_to_using_tracked(
                destination,
                sync_parent,
                copy_live_records,
                Some(tracker),
                None,
            )
        })
    }

    fn compact_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        self.compact_to_using(destination, sync_parent, copy_live_records)
    }

    fn compact_to_using(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        copy: impl FnOnce(&Log, &BTreeMap<Vec<u8>, ValueLocation>, &Log) -> io::Result<()>,
    ) -> io::Result<()> {
        self.compact_to_using_tracked(destination, sync_parent, copy, None, None)
    }

    fn compact_to_using_tracked(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        copy: impl FnOnce(&Log, &BTreeMap<Vec<u8>, ValueLocation>, &Log) -> io::Result<()>,
        tracker: Option<&crate::publication_outcome::Tracker>,
        verification_limits: Option<crate::log::PeakVerificationLimits>,
    ) -> io::Result<()> {
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        // Revalidate all log records, including obsolete ones and keys: value
        // reads alone would not detect a damaged historical record or key.
        if let Some(limits) = verification_limits {
            self.log
                .verify_index_peaks_with_limits(&inner.index, Some(limits))?;
        } else {
            self.log.verify_index(&inner.index)?;
        }
        let parent = backup::parent_directory(destination)?;
        let mut staging = crate::staging::Stage::directory(parent)?;
        let target = Store::open(staging.path())?;
        // This store is private to the staging directory. Copy the existing
        // DB01 records in bounded groups and synchronize each group; the target
        // index is rebuilt and checked by reopening before publication.
        copy(&self.log, &inner.index, &target.log)?;
        target.close_private_store()?;
        // Reopen to exercise durable recovery, validate every record, and
        // compare actual values and key count before publishing the directory.
        let verified = Store::open(staging.path())?;
        if verified.read()?.index.len() != inner.index.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted key count mismatch",
            ));
        }
        for (key, &location) in inner.index.iter() {
            if verified.get(key)?.as_deref() != Some(self.log.read(location)?.as_slice()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compacted value mismatch",
                ));
            }
        }
        verified.close_private_store()?;
        File::open(staging.path())?.sync_all()?;
        if let Some(tracker) = tracker {
            tracker.begin_publication();
        }
        staging.publish(destination)?;
        sync_parent(parent).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("compacted store may exist but directory synchronization failed: {error}"),
            )
        })?;
        staging.publication_synchronized();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_io::LogIo;
    use std::io::Write;
    use std::os::unix::fs::FileExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn detailed_compaction_reports_unknown_after_parent_sync_failure() {
        let root = tempfile::tempdir().unwrap();
        let source = Store::open(root.path().join("source")).unwrap();
        source.put(b"key", b"value").unwrap();
        let destination = root.path().join("copy");
        let result = source.compact_to_detailed_with_sync(&destination, |_| {
            Err(io::Error::other("injected parent sync failure"))
        });
        assert!(matches!(
            result,
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert_eq!(source.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        let copy = crate::test_support::retry_lock(|| Store::open(&destination)).unwrap();
        assert_eq!(copy.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
    }

    #[test]
    fn private_store_lock_release_is_not_delayed_by_a_duplicate_descriptor() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let inherited = store._lock_file.duplicate_descriptor().unwrap();
        assert!(Store::open(directory.path()).is_err());
        store.close_private_store().unwrap();
        let reopened = Store::open(directory.path()).unwrap();
        drop(inherited);
        assert!(Store::open(directory.path()).is_err());
        reopened.close_private_store().unwrap();
        assert!(Store::open(directory.path()).is_ok());
    }

    #[derive(Clone, Copy)]
    enum Fault {
        None,
        Append(usize),
        Sync(usize),
    }

    struct CopyIo {
        file: File,
        fault: Fault,
        appends: Arc<AtomicUsize>,
        syncs: Arc<AtomicUsize>,
    }

    impl LogIo for CopyIo {
        fn len(&self) -> io::Result<u64> {
            Ok(self.file.metadata()?.len())
        }
        fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
            FileExt::read_exact_at(&self.file, buffer, offset)
        }
        fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
            let count = self.appends.fetch_add(1, Ordering::SeqCst) + 1;
            let mut file = &self.file;
            if matches!(self.fault, Fault::Append(fail) if count == fail) {
                file.write_all(&bytes[..bytes.len().min(3)])?;
                return Err(io::Error::other("injected partial append"));
            }
            file.write_all(bytes)
        }
        fn sync_data(&self) -> io::Result<()> {
            let count = self.syncs.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(self.fault, Fault::Sync(fail) if count == fail) {
                return Err(io::Error::other("injected sync failure"));
            }
            self.file.sync_data()
        }
    }

    fn target_log(path: &Path, fault: Fault) -> (Log, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(path)
            .unwrap();
        let appends = Arc::new(AtomicUsize::new(0));
        let syncs = Arc::new(AtomicUsize::new(0));
        let (log, _) = Log::from_io(Box::new(CopyIo {
            file,
            fault,
            appends: Arc::clone(&appends),
            syncs: Arc::clone(&syncs),
        }))
        .unwrap();
        (log, appends, syncs)
    }

    #[test]
    fn grouped_copy_syncs_bounded_db01_records_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let source = Store::open(dir.path().join("source")).unwrap();
        let keys = (0..130_u16).map(u16::to_be_bytes).collect::<Vec<_>>();
        let operations = keys
            .iter()
            .map(|key| crate::BatchOperation::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        source.write_batch(&operations).unwrap();
        let path = dir.path().join("target.log");
        let (target, appends, syncs) = target_log(&path, Fault::None);
        {
            let inner = source.read().unwrap();
            copy_live_records(&source.log, &inner.index, &target).unwrap();
        }
        assert_eq!(appends.load(Ordering::SeqCst), 130);
        assert_eq!(syncs.load(Ordering::SeqCst), 3);
        drop(target);
        let bytes = std::fs::read(&path).unwrap();
        let mut offset = 0;
        for _ in 0..130 {
            assert_eq!(&bytes[offset..offset + 4], b"DB01");
            let header: [u8; record::HEADER_SIZE] = bytes[offset..offset + record::HEADER_SIZE]
                .try_into()
                .unwrap();
            offset += record::Header::parse(&header).unwrap().record_len() as usize;
        }
        assert_eq!(offset, bytes.len());
        let (_, recovered) = Log::from_io(Box::new(File::open(&path).unwrap())).unwrap();
        assert_eq!(recovered.len(), 130);
    }

    #[test]
    fn failed_large_restore_value_leaves_its_record_unwitnessed() {
        use sha2::{Digest, Sha256};

        let root = tempfile::tempdir().unwrap();
        let length = MAX_COPY_BYTES + 1;
        let digest = Sha256::digest(vec![0; length]).into();
        for truncate in [false, true] {
            let path = root.path().join(format!("source-{truncate}"));
            let source = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)
                .unwrap();
            source.set_len(length as u64).unwrap();
            if truncate {
                source.set_len(length as u64 - 1).unwrap();
            } else {
                source.write_all_at(&[1], length as u64 - 1).unwrap();
            }
            let stage = crate::staging::Stage::directory(root.path()).unwrap();
            let target = Store::open(stage.path()).unwrap();
            let initial = target.log.len().unwrap();
            let mut copy = GroupedCopy::new(&target.log, None);
            copy.put(b"a", 1, || Ok(vec![42])).unwrap();
            let witnessed =
                initial + record::encode(Operation::Put, b"a", &[42]).unwrap().len() as u64;
            let error = copy
                .put_retained(
                    b"b",
                    OverlayValue::Disk {
                        file: &source,
                        offset: 0,
                        len: length,
                        digest: &digest,
                    },
                    || Ok(()),
                )
                .unwrap_err();
            assert_eq!(
                error.kind(),
                if truncate {
                    io::ErrorKind::UnexpectedEof
                } else {
                    io::ErrorKind::InvalidData
                }
            );
            let (log, boundary) = Log::open_checkpoint_view(stage.path()).unwrap();
            assert_eq!(boundary, witnessed);
            assert!(log.len().unwrap() > witnessed + MAX_COPY_BYTES as u64 / 2);
            assert!(log.visit_records(0, Some(boundary), |_, _| Ok(())).is_err());
            let stage_path = stage.path().to_owned();
            drop(log);
            drop(copy);
            drop(target);
            drop(stage);
            assert!(!stage_path.exists());
        }
    }

    #[test]
    fn private_restore_admits_all_output_before_reading_and_flushes_bounded_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let length = record::encode(Operation::Put, b"00", b"v").unwrap().len() as u64;
        let entries = (0..130_u16)
            .map(|key| (key.to_be_bytes().to_vec(), b"v"))
            .collect::<Vec<_>>();
        let header = {
            let store = Store::open(&path).unwrap();
            let header = store.log.len().unwrap();
            store.close_private_store().unwrap();
            header
        };
        let end = header + length * entries.len() as u64;
        let key_bytes = 2 * entries.len();
        let size = RestoreSize {
            entries: entries.len() as u64,
            keys: key_bytes as u64,
            payload: (key_bytes + entries.len()) as u64,
        };
        for (log_bytes, keys, key_bytes) in [
            (end - 1, entries.len(), key_bytes),
            (end, entries.len() - 1, key_bytes),
            (end, entries.len(), key_bytes - 1),
        ] {
            let store = Store::open_with_resource_limits(
                &path,
                Some(log_bytes),
                Some(keys),
                Some(key_bytes),
            )
            .unwrap();
            assert!(
                store
                    .write_private_restore(size, |_| -> io::Result<()> {
                        panic!("restore read before output admission")
                    })
                    .is_err()
            );
            assert_eq!(
                std::fs::metadata(path.join("records.log")).unwrap().len(),
                header
            );
        }
        let store = Store::open_with_resource_limits(
            &path,
            Some(end),
            Some(entries.len()),
            Some(key_bytes),
        )
        .unwrap();
        let mut read = 0;
        store
            .write_private_restore(size, |copy| {
                for (key, value) in &entries {
                    copy.put(key, value.len(), || {
                        let flushed = read / MAX_COPY_RECORDS * MAX_COPY_RECORDS;
                        assert_eq!(
                            std::fs::metadata(path.join("records.log"))?.len(),
                            header + length * flushed as u64
                        );
                        read += 1;
                        Ok(value.to_vec())
                    })?;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(read, entries.len());
        let store = Store::open_with_resource_limits(
            &path,
            Some(end),
            Some(entries.len()),
            Some(key_bytes),
        )
        .unwrap();
        assert_eq!(store.log.len().unwrap(), end);
        store.log.verify_witness().unwrap();
        assert_eq!(
            store.scan(None, None, entries.len()).unwrap(),
            entries
                .into_iter()
                .map(|(key, value)| (key, value.to_vec()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn copy_flushes_before_combined_values_exceed_byte_limit() {
        let dir = tempfile::tempdir().unwrap();
        let source = Store::open(dir.path().join("source")).unwrap();
        let value = vec![3; 3 * 1024 * 1024];
        source.put(b"a", &value).unwrap();
        source.put(b"b", &value).unwrap();
        let (target, _, syncs) = target_log(&dir.path().join("target.log"), Fault::None);
        let inner = source.read().unwrap();
        copy_live_records(&source.log, &inner.index, &target).unwrap();
        assert_eq!(syncs.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn copy_propagates_partial_append_and_sync_failure_without_touching_source() {
        for (index, fault) in [Fault::Append(2), Fault::Sync(1)].into_iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source");
            let source = Store::open(&path).unwrap();
            source.put(b"a", b"one").unwrap();
            source.put(b"b", b"two").unwrap();
            let before = std::fs::read(path.join("records.log")).unwrap();
            let (target, _, _) = target_log(&dir.path().join(format!("target-{index}.log")), fault);
            {
                let inner = source.read().unwrap();
                assert!(copy_live_records(&source.log, &inner.index, &target).is_err());
            }
            assert_eq!(std::fs::read(path.join("records.log")).unwrap(), before);
            assert_eq!(source.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn copy_propagates_each_metadata_witness_failure_without_touching_source() {
        use crate::witness::{Witness, WitnessIo};
        use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};

        #[derive(Clone, Copy)]
        enum MetadataFault {
            WriteBefore,
            WriteAfter,
            SyncBefore,
            SyncAfter,
        }
        struct FaultWitness {
            file: File,
            fault: MetadataFault,
        }
        impl WitnessIo for FaultWitness {
            fn len(&self) -> io::Result<u64> {
                Ok(36)
            }
            fn read_exact_at(&self, bytes: &mut [u8], _: u64) -> io::Result<()> {
                let expected = bytes.len();
                if fgetxattr(&self.file, "user.db.commit.v3", bytes)? != expected {
                    return Err(io::Error::other("unexpected metadata length"));
                }
                Ok(())
            }
            fn write_all_at(&self, bytes: &[u8], _: u64) -> io::Result<()> {
                if matches!(self.fault, MetadataFault::WriteBefore) {
                    return Err(io::Error::other("injected metadata write failure"));
                }
                fsetxattr(&self.file, "user.db.commit.v3", bytes, XattrFlags::REPLACE)?;
                if matches!(self.fault, MetadataFault::WriteAfter) {
                    return Err(io::Error::other("injected post-write failure"));
                }
                Ok(())
            }
            fn sync_data(&self) -> io::Result<()> {
                if matches!(self.fault, MetadataFault::SyncBefore) {
                    return Err(io::Error::other("injected metadata sync failure"));
                }
                self.file.sync_all()?;
                if matches!(self.fault, MetadataFault::SyncAfter) {
                    return Err(io::Error::other("injected post-sync failure"));
                }
                Ok(())
            }
        }

        for fault in [
            MetadataFault::WriteBefore,
            MetadataFault::WriteAfter,
            MetadataFault::SyncBefore,
            MetadataFault::SyncAfter,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let source_path = directory.path().join("source");
            let target_path = directory.path().join("target");
            let source = Store::open(&source_path).unwrap();
            source.put(b"a", b"one").unwrap();
            let before = std::fs::read(source_path.join("records.log")).unwrap();
            let mut target = Store::open(&target_path).unwrap();
            let bytes = std::fs::read(target_path.join("records.log")).unwrap();
            assert_eq!(&bytes[..8], b"EMDALOG3");
            let id = bytes[8..24].try_into().unwrap();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(target_path.join("records.log"))
                .unwrap();
            let (witness, _) =
                Witness::from_io(Box::new(FaultWitness { file, fault }), id).unwrap();
            target.instrument_log_for_test(|log| log.attach_test_witness(witness));
            {
                let inner = source.read().unwrap();
                assert!(copy_live_records(&source.log, &inner.index, &target.log).is_err());
            }
            assert_eq!(
                std::fs::read(source_path.join("records.log")).unwrap(),
                before
            );
            assert_eq!(source.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
        }
    }

    #[test]
    fn interrupted_staged_copy_never_publishes_a_destination() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source");
        let source = Store::open(&source_path).unwrap();
        source.put(b"a", b"one").unwrap();
        source.put(b"b", b"two").unwrap();
        let before = std::fs::read(source_path.join("records.log")).unwrap();
        let destination = dir.path().join("destination");
        let result = source.compact_to_using(
            &destination,
            |_| Ok(()),
            |log, index, target| {
                let one = index
                    .iter()
                    .take(1)
                    .map(|(key, value)| (key.clone(), *value))
                    .collect();
                copy_live_records(log, &one, target)?;
                Err(io::Error::other("injected interruption"))
            },
        );
        assert!(result.is_err());
        assert!(!destination.exists());
        assert_eq!(
            std::fs::read(source_path.join("records.log")).unwrap(),
            before
        );
        assert!(std::fs::read_dir(dir.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".emda-stage-v1-")
        }));
    }

    #[test]
    fn compaction_crash_child() {
        let Ok(parent) = std::env::var("EMDA_COMPACTION_CRASH_PATH") else {
            return;
        };
        let parent = Path::new(&parent);
        let source = Store::open(parent.join("source")).unwrap();
        if std::env::var("EMDA_COMPACTION_CRASH_STAGE").as_deref() == Ok("published") {
            let _: io::Result<()> =
                source.compact_to_with_sync(&parent.join("destination"), |_| {
                    println!("COMPACTION_CRASH_BOUNDARY_REACHED");
                    std::io::stdout().flush()?;
                    loop {
                        std::thread::park();
                    }
                });
            return;
        }
        let _: io::Result<()> = source.compact_to_using(
            &parent.join("destination"),
            |_| Ok(()),
            |log, index, target| {
                let one = index
                    .iter()
                    .take(1)
                    .map(|(key, value)| (key.clone(), *value))
                    .collect();
                copy_live_records(log, &one, target)?;
                println!("COMPACTION_CRASH_BOUNDARY_REACHED");
                std::io::stdout().flush()?;
                loop {
                    std::thread::park();
                }
            },
        );
    }

    fn kill_child_at_compaction_boundary(parent: &Path, stage: &str) {
        use std::io::BufRead;
        use std::process::{Command, Stdio};
        use std::sync::mpsc;
        use std::time::Duration;
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::compact::tests::compaction_crash_child",
                "--nocapture",
            ])
            .env("EMDA_COMPACTION_CRASH_PATH", parent)
            .env("EMDA_COMPACTION_CRASH_STAGE", stage)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(output)
                .lines()
                .map_while(Result::ok)
            {
                if line.contains("COMPACTION_CRASH_BOUNDARY_REACHED") {
                    let _ = sender.send(());
                    break;
                }
            }
        });
        let copied = receiver.recv_timeout(Duration::from_secs(20));
        let busy = crate::cleanup_abandoned_staging(parent);
        child.kill().unwrap();
        child.wait().unwrap();
        copied.expect("compaction process did not reach the requested boundary");
        assert_eq!(busy.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn process_killed_during_compaction_keeps_source_and_does_not_publish_copy() {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source");
        let source = Store::open(&source_path).unwrap();
        source.put(b"a", b"one").unwrap();
        source.put(b"b", b"two").unwrap();
        let before = std::fs::read(source_path.join("records.log")).unwrap();
        drop(source);
        kill_child_at_compaction_boundary(directory.path(), "staged");
        assert!(!directory.path().join("destination").exists());
        assert_eq!(
            std::fs::read(source_path.join("records.log")).unwrap(),
            before
        );
        let recovered = Store::open(&source_path).unwrap();
        assert_eq!(
            recovered.get_many(&[b"a", b"b"]).unwrap(),
            vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
        );
        recovered.put(b"after", b"ok").unwrap();
        assert!(std::fs::read_dir(directory.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".emda-stage-v1-d-")
        }));
        let cleaned = crate::cleanup_abandoned_staging(directory.path()).unwrap();
        assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (1, 1));
        assert_eq!(
            recovered.get(b"after").unwrap().as_deref(),
            Some(&b"ok"[..])
        );
    }

    #[test]
    fn process_killed_after_publication_sees_complete_copy_on_running_os() {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source");
        let source = Store::open(&source_path).unwrap();
        source.put(b"a", b"old").unwrap();
        source.put(b"a", b"current").unwrap();
        source.put(b"b", b"present").unwrap();
        let before = std::fs::read(source_path.join("records.log")).unwrap();
        drop(source);
        kill_child_at_compaction_boundary(directory.path(), "published");
        let cleaned = crate::cleanup_abandoned_staging(directory.path()).unwrap();
        assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (0, 1));
        assert_eq!(
            std::fs::read(source_path.join("records.log")).unwrap(),
            before
        );
        let source = Store::open(&source_path).unwrap();
        let copy = Store::open(directory.path().join("destination")).unwrap();
        let expected = vec![Some(b"current".to_vec()), Some(b"present".to_vec())];
        assert_eq!(source.get_many(&[b"a", b"b"]).unwrap(), expected);
        assert_eq!(copy.get_many(&[b"a", b"b"]).unwrap(), expected);
    }

    #[test]
    fn failed_parent_sync_reports_uncertain_publication_and_preserves_source() {
        let dir = tempfile::tempdir().unwrap();
        let source = Store::open(dir.path().join("source")).unwrap();
        source.put(b"key", b"value").unwrap();
        let destination = dir.path().join("destination");
        let error = source
            .compact_to_with_sync(&destination, |_| {
                Err(io::Error::other("injected sync error"))
            })
            .unwrap_err();
        assert!(error.to_string().contains("may exist"));
        assert_eq!(source.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        assert_eq!(
            Store::open(&destination)
                .unwrap()
                .get(b"key")
                .unwrap()
                .as_deref(),
            Some(&b"value"[..])
        );
        assert!(
            matches!(source.compact_to(&destination), Err(e) if e.kind() == io::ErrorKind::AlreadyExists)
        );
    }
    #[test]
    fn every_compaction_ancestor_sync_failure_reports_uncertain_copy() {
        let probe = tempfile::tempdir().unwrap();
        let count = std::fs::canonicalize(probe.path())
            .unwrap()
            .ancestors()
            .count();
        drop(probe);
        for fail_at in 0..count {
            let dir = tempfile::tempdir().unwrap();
            let source = Store::open(dir.path().join("source")).unwrap();
            source.put(b"key", b"value").unwrap();
            let destination = dir.path().join("copy");
            let mut seen = 0;
            let result = source.compact_to_with_sync(&destination, |parent| {
                crate::directory::sync_existing_with(parent, |directory| {
                    let index = seen;
                    seen += 1;
                    if index == fail_at {
                        Err(io::Error::other("injected ancestor sync"))
                    } else {
                        File::open(directory)?.sync_all()
                    }
                })
            });
            assert!(result.unwrap_err().to_string().contains("may exist"));
            assert_eq!(seen, fail_at + 1);
            assert_eq!(source.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
            let copy = Store::open(&destination).unwrap();
            assert_eq!(copy.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        }
    }
}

#[cfg(test)]
mod retained_peak_tests {
    use super::*;
    use crate::record::batch::BatchOperation;

    #[test]
    fn bounded_verification_rejects_before_publication() {
        use crate::log::PeakVerificationLimits;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        let store = Store::open(&source).unwrap();
        store.put(b"aa", b"old").unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"long-key",
                    value: b"",
                },
                BatchOperation::Delete { key: b"long-key" },
            ])
            .unwrap();
        let length = std::fs::metadata(source.join("records.log")).unwrap().len();
        let limits = PeakVerificationLimits {
            max_log_bytes: length,
            max_live_keys: 2,
            max_live_key_bytes: 10,
        };
        for cap in [
            PeakVerificationLimits {
                max_log_bytes: length - 1,
                ..limits
            },
            PeakVerificationLimits {
                max_live_keys: 1,
                ..limits
            },
            PeakVerificationLimits {
                max_live_key_bytes: 9,
                ..limits
            },
        ] {
            let result = store.compact_to_with_verification_limits(&destination, cap);
            match result {
                Err(crate::PublicationFailure::NotPublished(error)) => {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidInput)
                }
                other => panic!("unexpected result: {other:?}"),
            }
            assert!(!destination.exists());
            assert_eq!(store.get(b"aa").unwrap().as_deref(), Some(&b"old"[..]));
        }
        store
            .compact_to_with_verification_limits(&destination, limits)
            .unwrap();
        assert_eq!(
            Store::open(&destination)
                .unwrap()
                .get(b"aa")
                .unwrap()
                .as_deref(),
            Some(&b"old"[..])
        );
    }

    #[test]
    fn verified_peaks_reject_corrupt_superseded_record() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put(b"key", b"first-value").unwrap();
        store.put(b"key", b"second-value").unwrap();
        let path = dir.path().join("records.log");
        let bytes = std::fs::read(&path).unwrap();
        let offset = bytes.windows(11).position(|b| b == b"first-value").unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.write_all_at(b"X", offset as u64).unwrap();
        let destination = dir.path().join("corrupt-destination");
        let failure = store
            .compact_to_with_verification_limits(
                &destination,
                crate::PeakVerificationLimits {
                    max_log_bytes: u64::MAX,
                    max_live_keys: 10,
                    max_live_key_bytes: 100,
                },
            )
            .unwrap_err();
        match failure {
            crate::PublicationFailure::NotPublished(error) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidData)
            }
            other => panic!("unexpected failure: {other:?}"),
        }
        assert!(!destination.exists());
        let inner = store.read().unwrap();
        assert!(store.log.verify_index_peaks(&inner.index).is_err());
        assert!(
            store
                .log
                .verify_index_peaks_with_limits(
                    &inner.index,
                    Some(crate::log::PeakVerificationLimits {
                        max_log_bytes: u64::MAX,
                        max_live_keys: 10,
                        max_live_key_bytes: 100,
                    })
                )
                .is_err()
        );
    }

    #[test]
    fn verified_peaks_include_intermediate_batch_states() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put(b"aa", b"old").unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"long-key",
                    value: b"",
                },
                BatchOperation::Put {
                    key: b"aa",
                    value: b"replacement",
                },
                BatchOperation::Delete { key: b"long-key" },
                BatchOperation::Put {
                    key: b"b",
                    value: b"v",
                },
            ])
            .unwrap();
        let inner = store.read().unwrap();
        assert_eq!(store.log.verify_index_peaks(&inner.index).unwrap(), (2, 10));
        let mut wrong = (*inner.index).clone();
        wrong.clear();
        assert!(store.log.verify_index_peaks(&wrong).is_err());
    }
}
