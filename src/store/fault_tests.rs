//! Deterministic I/O faults against the store's real log operations.

use super::*;
use crate::log_io::LogIo;
use std::fs;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn duplicated_store_lock_retains_ownership_until_the_duplicate_closes() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let inherited = store._lock_file.try_clone().unwrap();
    drop(store);
    assert_eq!(
        Store::open(directory.path()).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(inherited);
    assert!(Store::open(directory.path()).is_ok());
}

#[derive(Clone, Copy)]
enum Fault {
    PartialAppend(usize),
    PanicAfterAppend(usize),
    SyncBefore,
    SyncAfter,
    ShortRead(usize),
    ShortReadAfter(usize, usize),
}

struct FaultIo {
    file: File,
    fault: Arc<Mutex<Option<Fault>>>,
}

impl FaultIo {
    fn take_matching(&self, predicate: impl FnOnce(Fault) -> bool) -> Option<Fault> {
        let mut fault = self.fault.lock().unwrap();
        if fault.is_some_and(predicate) {
            fault.take()
        } else {
            None
        }
    }
}

impl LogIo for FaultIo {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        let failure = {
            let mut fault = self.fault.lock().unwrap();
            match *fault {
                Some(Fault::ShortRead(bytes) | Fault::ShortReadAfter(0, bytes)) => {
                    *fault = None;
                    Some(bytes)
                }
                Some(Fault::ShortReadAfter(remaining, bytes)) => {
                    *fault = Some(Fault::ShortReadAfter(remaining - 1, bytes));
                    None
                }
                _ => None,
            }
        };
        if let Some(bytes) = failure {
            let count = bytes.min(buffer.len());
            FileExt::read_exact_at(&self.file, &mut buffer[..count], offset)?;
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "injected short read",
            ));
        }
        FileExt::read_exact_at(&self.file, buffer, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        let mut file = &self.file;
        match self
            .take_matching(|f| matches!(f, Fault::PartialAppend(_) | Fault::PanicAfterAppend(_)))
        {
            Some(Fault::PartialAppend(count)) => {
                file.write_all(&bytes[..count.min(bytes.len())])?;
                // POSIX ENOSPC: emulate a disk-full error following a partial append.
                return Err(io::Error::from_raw_os_error(28));
            }
            Some(Fault::PanicAfterAppend(count)) => {
                file.write_all(&bytes[..count.min(bytes.len())])?;
                panic!("injected panic after partial write");
            }
            _ => {}
        }
        file.write_all(bytes)
    }

    fn sync_data(&self) -> io::Result<()> {
        match self.take_matching(|f| matches!(f, Fault::SyncBefore | Fault::SyncAfter)) {
            Some(Fault::SyncBefore) => Err(io::Error::other(
                "injected sync failure before synchronization",
            )),
            Some(Fault::SyncAfter) => {
                self.file.sync_data()?;
                Err(io::Error::other(
                    "injected sync failure after synchronization",
                ))
            }
            _ => self.file.sync_data(),
        }
    }
}

fn open_fault_store(dir: &TempDir, fault: Arc<Mutex<Option<Fault>>>) -> io::Result<Store> {
    Store::open_using(dir.path(), |path, _may_initialize| {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join("records.log"))?;
        File::open(path)?.sync_all()?;
        Log::from_io(Box::new(FaultIo { file, fault }))
    })
}

#[test]
fn disk_full_at_every_record_offset_fails_closed_and_preserves_bytes() {
    let record_len = record::encode(Operation::Put, b"failed", b"value")
        .unwrap()
        .len();
    for offset in 0..record_len {
        let dir = tempfile::tempdir().unwrap();
        let fault = Arc::new(Mutex::new(None));
        let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
        store.put(b"safe", b"committed").unwrap();
        let path = dir.path().join("records.log");
        *fault.lock().unwrap() = Some(Fault::PartialAppend(offset));
        let error = store.put(b"failed", b"value").unwrap_err();
        assert!(error.to_string().contains("outcome is uncertain"));
        assert!(store.get(b"safe").is_err());
        assert!(store.put(b"next", b"value").is_err());
        drop(store);
        let bytes_before = fs::read(&path).unwrap();
        let reopened = Store::open(dir.path());
        if offset == 0 {
            assert_eq!(
                reopened.unwrap().get(b"safe").unwrap().as_deref(),
                Some(&b"committed"[..])
            );
        } else {
            assert!(
                matches!(reopened, Err(ref e) if e.kind() == io::ErrorKind::InvalidData),
                "offset {offset}"
            );
        }
        assert_eq!(fs::read(&path).unwrap(), bytes_before);
    }
}

#[test]
fn failed_sync_has_unknown_outcome_and_blocks_further_operations() {
    for failure in [Fault::SyncBefore, Fault::SyncAfter] {
        let dir = tempfile::tempdir().unwrap();
        let fault = Arc::new(Mutex::new(None));
        let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
        store.put(b"safe", b"committed").unwrap();
        *fault.lock().unwrap() = Some(failure);
        assert!(
            store
                .put(b"unknown", b"value")
                .unwrap_err()
                .to_string()
                .contains("outcome is uncertain")
        );
        assert!(store.get(b"safe").is_err());
        assert!(store.delete(b"safe").is_err());
        drop(store);
        // The complete failed write may still appear on reopen; callers must not
        // assume it was absent. This is not a simulated power failure.
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"safe").unwrap().as_deref(),
            Some(&b"committed"[..])
        );
        assert_eq!(
            store.get(b"unknown").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}

#[test]
fn conditional_mismatch_does_not_write_and_failed_match_is_uncertain() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    store.put(b"key", b"old").unwrap();
    *fault.lock().unwrap() = Some(Fault::SyncBefore);
    assert!(
        !store
            .compare_exchange(b"key", Some(b"wrong"), Some(b"new"))
            .unwrap()
    );
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"old"[..]));
    let error = store
        .compare_exchange(b"key", Some(b"old"), Some(b"new"))
        .unwrap_err();
    assert!(error.to_string().contains("uncertain"));
    assert!(store.get(b"key").is_err());
    assert!(store.compare_exchange(b"key", Some(b"old"), None).is_err());
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn increment_sync_failure_is_uncertain_and_requires_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    assert_eq!(store.increment_i64(b"counter", 3).unwrap(), 3);
    *fault.lock().unwrap() = Some(Fault::SyncBefore);
    assert!(
        store
            .increment_i64(b"counter", 4)
            .unwrap_err()
            .to_string()
            .contains("uncertain")
    );
    assert!(store.increment_i64(b"counter", 1).is_err());
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get(b"counter")
            .unwrap()
            .as_deref(),
        Some(&7_i64.to_le_bytes()[..])
    );
}

#[test]
fn limited_store_preserves_unknown_outcome_after_sync_error() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let mut store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    let record_len = record::encode(Operation::Put, b"key", b"value")
        .unwrap()
        .len() as u64;
    let length = store.log.len().unwrap();
    store.log_quota = Some(LogQuota::new(length + record_len, length).unwrap());
    *fault.lock().unwrap() = Some(Fault::SyncBefore);
    assert!(
        store
            .put(b"key", b"value")
            .unwrap_err()
            .to_string()
            .contains("uncertain")
    );
    // The failed append used the entire allowance, but the original unknown
    // outcome must take precedence over reporting an ordinary quota rejection.
    assert!(
        store
            .put(b"key", b"next")
            .unwrap_err()
            .to_string()
            .contains("uncertain")
    );
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn failed_preparation_releases_log_capacity_for_a_later_write() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let length = store.log.len().unwrap();
    let record_len = record::encode(Operation::Put, b"key", b"value")
        .unwrap()
        .len() as u64;
    let mut store = Store::set_open_limits(store, Some(length + record_len), None, None).unwrap();
    store.commits = Coordinator::for_test(1, 1);
    let before = std::fs::read(directory.path().join("records.log")).unwrap();
    assert!(store.put(b"key", b"value").is_err());
    assert_eq!(
        std::fs::read(directory.path().join("records.log")).unwrap(),
        before
    );
    store.commits = Coordinator::new();
    store.put(b"key", b"value").unwrap();
    assert!(store.put(b"x", b"value").is_err());
    drop(store);
    let recovered = Store::open_with_log_limit(directory.path(), length + record_len).unwrap();
    assert_eq!(
        recovered.get(b"key").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn generation_exhaustion_fails_closed_instead_of_wrapping() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, fault).unwrap();
    store.put(b"safe", b"value").unwrap();
    store.inner.write().unwrap().generation = u64::MAX;
    let error = store.put(b"uncertain", b"value").unwrap_err();
    assert!(error.to_string().contains("uncertain"));
    assert!(store.get(b"safe").is_err());
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"safe").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    // The record was synchronized before the generation check failed, so the
    // failed mutation can be recovered; its original outcome was unknown.
    assert_eq!(
        reopened.get(b"uncertain").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn a_write_failure_clears_in_progress_preparation_reservations() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    store.put(b"safe", b"committed").unwrap();
    let pending = store.commits.reserve_for_test(1024).unwrap();
    *fault.lock().unwrap() = Some(Fault::PartialAppend(5));
    assert!(store.put(b"failed", b"value").is_err());
    drop(pending);
    assert_eq!(store.commits.buffered(), (0, 0));
    assert!(store.get(b"safe").is_err());
    assert!(store.put(b"later", b"value").is_err());
}

#[derive(Clone, Copy)]
enum WitnessFault {
    WriteBefore,
    WriteAfter,
    Sync,
}

struct InjectedWitness {
    bytes: Mutex<Vec<u8>>,
    fault: WitnessFault,
}

impl crate::witness::WitnessIo for InjectedWitness {
    fn len(&self) -> io::Result<u64> {
        Ok(self.bytes.lock().unwrap().len() as u64)
    }
    fn read_exact_at(&self, buffer: &mut [u8], _offset: u64) -> io::Result<()> {
        buffer.copy_from_slice(&self.bytes.lock().unwrap());
        Ok(())
    }
    fn write_all_at(&self, buffer: &[u8], _offset: u64) -> io::Result<()> {
        if matches!(self.fault, WitnessFault::WriteBefore) {
            return Err(io::Error::other("injected witness write failure"));
        }
        self.bytes.lock().unwrap().copy_from_slice(buffer);
        if matches!(self.fault, WitnessFault::WriteAfter) {
            return Err(io::Error::other("injected witness write outcome unknown"));
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        if matches!(self.fault, WitnessFault::Sync) {
            return Err(io::Error::other("injected witness sync failure"));
        }
        Ok(())
    }
}

#[test]
fn witness_write_and_sync_failures_never_acknowledge_and_poison_the_store() {
    for fault in [
        WitnessFault::WriteBefore,
        WitnessFault::WriteAfter,
        WitnessFault::Sync,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let id = [4; 16];
        let store = Store::open_using(dir.path(), |path, _| {
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.join("records.log"))?;
            let (mut log, index) = Log::from_io(Box::new(file))?;
            let marker = InjectedWitness {
                bytes: Mutex::new(crate::witness::encode_for_test(&id, 0)),
                fault,
            };
            log.attach_test_witness(crate::witness::Witness::for_test(Box::new(marker), id, 0));
            Ok((log, index))
        })
        .unwrap();
        let error = store.put(b"uncertain", b"value").unwrap_err();
        assert!(error.to_string().contains("uncertain"));
        assert!(store.get(b"uncertain").is_err());
        assert!(store.put(b"later", b"value").is_err());
        drop(store);
        // The append completed before the injected witness failure; its
        // unacknowledged outcome may be visible after recovery.
        let reopened = Store::open(dir.path()).unwrap();
        assert_eq!(
            reopened.get(b"uncertain").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}

#[test]
fn short_read_is_reported_without_returning_partial_data() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    store.put(b"key", b"value").unwrap();
    *fault.lock().unwrap() = Some(Fault::ShortRead(2));
    assert_eq!(
        store.get(b"key").unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
}

#[test]
fn bulk_read_returns_no_partial_results_after_a_later_short_read() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    store.put(b"a", b"first").unwrap();
    store.put(b"b", b"second").unwrap();
    *fault.lock().unwrap() = Some(Fault::ShortReadAfter(1, 1));
    assert_eq!(
        store.get_many(&[b"a", b"b"]).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        store.get_many(&[b"a", b"b"]).unwrap(),
        vec![Some(b"first".to_vec()), Some(b"second".to_vec())]
    );
}

#[test]
fn snapshot_refuses_reads_after_uncertain_write_and_propagates_read_errors() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    store.put(b"a", b"first").unwrap();
    store.put(b"b", b"second").unwrap();
    let snapshot = store.snapshot(2, 2).unwrap();
    *fault.lock().unwrap() = Some(Fault::ShortReadAfter(1, 1));
    assert_eq!(
        snapshot.scan(None, None, 2).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    *fault.lock().unwrap() = Some(Fault::SyncBefore);
    assert!(store.put(b"c", b"uncertain").is_err());
    assert!(
        snapshot
            .get(b"a")
            .unwrap_err()
            .to_string()
            .contains("uncertain")
    );
    assert!(
        snapshot
            .scan(None, None, 2)
            .unwrap_err()
            .to_string()
            .contains("uncertain")
    );
}

#[test]
fn short_read_during_recovery_preserves_the_log() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"key", b"value").unwrap();
    }
    let path = dir.path().join("records.log");
    let before = fs::read(&path).unwrap();
    let fault = Arc::new(Mutex::new(Some(Fault::ShortRead(3))));
    let result = open_fault_store(&dir, fault);
    assert!(matches!(result, Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof));
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn backup_source_short_read_does_not_publish_a_partial_backup() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    store.put(b"key", b"value").unwrap();
    let destination = dir.path().join("backup");
    *fault.lock().unwrap() = Some(Fault::ShortRead(2));
    assert_eq!(
        store.backup(&destination).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert!(!destination.exists());
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
}

#[test]
fn writer_panic_blocks_subsequent_reads_and_preserves_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = open_fault_store(&dir, Arc::clone(&fault)).unwrap();
    store.put(b"safe", b"committed").unwrap();
    *fault.lock().unwrap() = Some(Fault::PanicAfterAppend(5));
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.put(b"bad", b"value")));
    assert!(panic.is_err());
    assert!(
        store
            .get(b"safe")
            .unwrap_err()
            .to_string()
            .contains("uncertain")
    );
    assert!(store.put(b"next", b"value").is_err());
    drop(store);
    let path = dir.path().join("records.log");
    let before = fs::read(&path).unwrap();
    assert!(Store::open(dir.path()).is_err());
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn concurrent_writers_cannot_continue_after_an_uncertain_write() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Mutex::new(None));
    let store = Arc::new(open_fault_store(&dir, Arc::clone(&fault)).unwrap());
    store.put(b"safe", b"committed").unwrap();
    *fault.lock().unwrap() = Some(Fault::PartialAppend(5));
    let barrier = Arc::new(Barrier::new(6));
    let mut threads = Vec::new();
    for id in 0..6 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            barrier.wait();
            store.put(format!("key-{id}").as_bytes(), b"value").is_err()
        }));
    }
    for thread in threads {
        assert!(thread.join().unwrap());
    }
    drop(store);
    let path = dir.path().join("records.log");
    let before = fs::read(&path).unwrap();
    assert!(Store::open(dir.path()).is_err());
    assert_eq!(fs::read(path).unwrap(), before);
}

pub(super) struct BlockingSyncIo {
    pub(super) file: File,
    pub(super) armed: Arc<AtomicBool>,
    pub(super) started: mpsc::Sender<()>,
    pub(super) resume: Mutex<mpsc::Receiver<()>>,
}

impl LogIo for BlockingSyncIo {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, buffer, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        let mut file = &self.file;
        file.write_all(bytes)
    }

    fn sync_data(&self) -> io::Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.started.send(()).map_err(io::Error::other)?;
            self.resume
                .lock()
                .unwrap()
                .recv()
                .map_err(io::Error::other)?;
        }
        self.file.sync_data()
    }
}

#[test]
fn reads_observe_previous_value_while_a_write_is_syncing() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(
        Store::open_using(dir.path(), |path, _| {
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.join("records.log"))?;
            File::open(path)?.sync_all()?;
            Log::from_io(Box::new(BlockingSyncIo {
                file,
                armed: Arc::clone(&armed),
                started: started_tx,
                resume: Mutex::new(resume_rx),
            }))
        })
        .unwrap(),
    );
    store.put(b"key", b"old").unwrap();
    armed.store(true, Ordering::SeqCst);
    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || writer_store.put(b"key", b"new"));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let (read_tx, read_rx) = mpsc::channel();
    let reader_store = Arc::clone(&store);
    let reader = thread::spawn(move || {
        let _ = read_tx.send(reader_store.get(b"key"));
    });
    let read = read_rx.recv_timeout(Duration::from_secs(3));
    resume_tx.send(()).unwrap();
    writer.join().unwrap().unwrap();
    reader.join().unwrap();
    assert_eq!(
        read.expect("read blocked on an in-flight sync")
            .unwrap()
            .as_deref(),
        Some(&b"old"[..])
    );
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn short_read_after_buffer_refill_fails_recovery_without_changing_log() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        for i in 0_u32..1500 {
            store.put(&i.to_be_bytes(), &[i as u8; 64]).unwrap();
        }
    }
    let path = dir.path().join("records.log");
    let before = fs::read(&path).unwrap();
    assert!(before.len() > 64 * 1024);
    let fault = Arc::new(Mutex::new(Some(Fault::ShortReadAfter(2, 11))));
    let result = open_fault_store(&dir, fault);
    assert!(matches!(result, Err(ref error) if error.kind() == io::ErrorKind::UnexpectedEof));
    assert_eq!(fs::read(&path).unwrap(), before);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(&1499_u32.to_be_bytes()).unwrap().as_deref(),
        Some(&[1499_u32 as u8; 64][..])
    );
}

#[test]
fn short_read_in_large_direct_payload_preserves_log() {
    let dir = tempfile::tempdir().unwrap();
    let value = vec![0xa7; 256 * 1024];
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"large", &value).unwrap();
    }
    let path = dir.path().join("records.log");
    let before = fs::read(&path).unwrap();
    let fault = Arc::new(Mutex::new(Some(Fault::ShortReadAfter(2, 11))));
    let result = open_fault_store(&dir, fault);
    assert!(matches!(result, Err(ref error) if error.kind() == io::ErrorKind::UnexpectedEof));
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        Store::open(dir.path()).unwrap().get(b"large").unwrap(),
        Some(value)
    );
}

#[test]
fn checkpoint_parent_sync_failure_reports_uncertain_publication() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let store = Store::open(&source).unwrap();
    store.put(b"key", b"value").unwrap();
    let checkpoint = dir.path().join("index");
    let inner = store.read().unwrap();
    let error = crate::checkpoint::create_with_sync(&store.log, &inner.index, &checkpoint, |_| {
        Err(io::Error::other("injected directory sync failure"))
    })
    .unwrap_err();
    assert!(error.to_string().contains("may exist"));
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
    drop(inner);
    drop(store);
    let bytes = std::fs::read(&checkpoint).unwrap();
    let token: [u8; 32] = bytes[bytes.len() - 32..].try_into().unwrap();
    assert_eq!(
        Store::open_with_checkpoint(&source, &checkpoint, &token)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn checkpoint_reports_uncertain_publication_for_each_ancestor_sync_failure() {
    let probe = tempfile::tempdir().unwrap();
    let count = std::fs::canonicalize(probe.path())
        .unwrap()
        .ancestors()
        .count();
    drop(probe);
    for fail_at in 0..count {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let store = Store::open(&source).unwrap();
        store.put(b"key", b"value").unwrap();
        let checkpoint = dir.path().join("index");
        let mut seen = 0;
        let result = store.checkpoint_to_with_sync(&checkpoint, |parent| {
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
        assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        drop(store);
        let bytes = fs::read(&checkpoint).unwrap();
        let token = bytes[bytes.len() - 32..].try_into().unwrap();
        let reopened = Store::open_with_checkpoint(&source, &checkpoint, &token).unwrap();
        assert_eq!(
            reopened.get(b"key").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}
