//! Verified, non-overwriting compaction to a separate store.
use super::*;
use crate::log::AppendRecord;

pub(super) const MAX_COPY_RECORDS: usize = 64;
pub(super) const MAX_COPY_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct CopyRecord {
    pub(super) encoded: Vec<u8>,
    pub(super) key_len: usize,
    pub(super) value_len: usize,
    pub(super) checksum: u32,
}

// These stores are owned solely by the private staging operation. Explicitly
// unlock before dropping them so a child forked by another thread cannot keep
// the staging lock alive through an inherited descriptor until exec.
pub(super) fn close_private_store(store: Store) -> io::Result<()> {
    fs2::FileExt::unlock(&store._lock_file)?;
    drop(store);
    Ok(())
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
    let mut records = Vec::with_capacity(MAX_COPY_RECORDS);
    let mut bytes = 0_usize;
    for (key, &location) in index {
        let length = record::HEADER_SIZE + key.len() + location.length + record::CHECKSUM_SIZE;
        if !records.is_empty()
            && (records.len() == MAX_COPY_RECORDS || bytes + length > MAX_COPY_BYTES)
        {
            flush_copy(target, &mut records)?;
            bytes = 0;
        }
        let value = source.read(location)?;
        let checksum = crc32fast::hash(&value);
        let encoded = record::encode(Operation::Put, key, &value)?;
        bytes += encoded.len();
        records.push(CopyRecord {
            encoded,
            key_len: key.len(),
            value_len: value.len(),
            checksum,
        });
    }
    flush_copy(target, &mut records)
}

impl Store {
    /// Copies current live keys into a verified new store with no old records.
    ///
    /// Mutations wait for the copy and verification; existing readers can
    /// continue. The source remains open and unchanged, and callers control any
    /// later switchover. The destination must not exist. An error after rename
    /// can leave a complete destination whose directory sync was unsuccessful.
    /// This operation currently requires memory for additional full indexes and
    /// is not a bounded-memory or online in-place compaction mechanism.
    pub fn compact_to(&self, destination: impl AsRef<Path>) -> io::Result<()> {
        self.compact_to_with_sync(destination.as_ref(), |parent| {
            File::open(parent)?.sync_all()
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
        self.log.verify_index(&inner.index)?;
        let parent = backup::parent_directory(destination)?;
        let mut staging = crate::staging::Stage::directory(parent)?;
        let target = Store::open(staging.path())?;
        // This store is private to the staging directory. Copy the existing
        // DB01 records in bounded groups and synchronize each group; the target
        // index is rebuilt and checked by reopening before publication.
        copy(&self.log, &inner.index, &target.log)?;
        close_private_store(target)?;
        // Reopen to exercise durable recovery, validate every record, and
        // compare actual values and key count before publishing the directory.
        let verified = Store::open(staging.path())?;
        if verified.read()?.index.len() != inner.index.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted key count mismatch",
            ));
        }
        for (key, &location) in &inner.index {
            if verified.get(key)?.as_deref() != Some(self.log.read(location)?.as_slice()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compacted value mismatch",
                ));
            }
        }
        close_private_store(verified)?;
        File::open(staging.path())?.sync_all()?;
        staging.publish(destination)?;
        sync_parent(parent).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("compacted store may exist but directory synchronization failed: {error}"),
            )
        })
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
    fn private_store_lock_release_is_not_delayed_by_a_duplicate_descriptor() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let inherited = store._lock_file.try_clone().unwrap();
        assert!(Store::open(directory.path()).is_err());
        close_private_store(store).unwrap();
        let reopened = Store::open(directory.path()).unwrap();
        drop(inherited);
        assert!(Store::open(directory.path()).is_err());
        close_private_store(reopened).unwrap();
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
            target.log.attach_test_witness(witness);
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
}
