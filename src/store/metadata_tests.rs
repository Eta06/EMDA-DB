//! Failure and publication checks through the real V3 coordinator path.
use super::*;
use crate::witness::{Witness, WitnessIo};
use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

const NAME: &str = "user.db.commit.v3";

#[derive(Clone, Copy)]
enum Fault {
    Read,
    WriteBefore,
    WriteAfter,
    SyncBefore,
    SyncTimeout,
    SyncAfter,
    Panic,
    Block,
    BlockThenSyncError,
    BlockThenPanic,
    BlockThenGroupError,
    BlockThenGroupPanic,
}

struct Controlled {
    file: File,
    armed: Arc<AtomicBool>,
    fault: Fault,
    entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
    armed_syncs: AtomicUsize,
}

impl WitnessIo for Controlled {
    fn len(&self) -> io::Result<u64> {
        Ok(36)
    }
    fn read_exact_at(&self, bytes: &mut [u8], _: u64) -> io::Result<()> {
        if self.armed.load(Ordering::Acquire) && matches!(self.fault, Fault::Read) {
            return Err(io::Error::other("injected xattr read error"));
        }
        assert_eq!(fgetxattr(&self.file, NAME, bytes)?, 36);
        Ok(())
    }
    fn write_all_at(&self, bytes: &[u8], _: u64) -> io::Result<()> {
        let armed = self.armed.load(Ordering::Acquire);
        if armed && matches!(self.fault, Fault::WriteBefore) {
            return Err(io::Error::other("injected xattr write error"));
        }
        fsetxattr(&self.file, NAME, bytes, XattrFlags::REPLACE)?;
        if armed && matches!(self.fault, Fault::WriteAfter) {
            return Err(io::Error::other("injected error after xattr write"));
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        let armed = self.armed.load(Ordering::Acquire);
        if armed {
            match self.fault {
                Fault::SyncBefore => return Err(io::Error::other("injected fsync error")),
                Fault::SyncTimeout => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "injected fsync timeout",
                    ));
                }
                Fault::Panic => panic!("injected sync panic"),
                Fault::Block => {
                    self.entered.send(()).unwrap();
                    self.resume.lock().unwrap().recv().unwrap();
                }
                Fault::BlockThenSyncError => {
                    self.entered.send(()).unwrap();
                    self.resume.lock().unwrap().recv().unwrap();
                    return Err(io::Error::other("injected delayed fsync error"));
                }
                Fault::BlockThenPanic => {
                    self.entered.send(()).unwrap();
                    self.resume.lock().unwrap().recv().unwrap();
                    panic!("injected delayed sync panic");
                }
                Fault::BlockThenGroupError | Fault::BlockThenGroupPanic => {
                    let count = self.armed_syncs.fetch_add(1, Ordering::SeqCst);
                    self.entered.send(()).unwrap();
                    self.resume.lock().unwrap().recv().unwrap();
                    if count == 1 {
                        if matches!(self.fault, Fault::BlockThenGroupPanic) {
                            panic!("injected grouped sync panic");
                        }
                        return Err(io::Error::other("injected grouped sync error"));
                    }
                }
                _ => {}
            }
        }
        self.file.sync_all()?;
        if armed && matches!(self.fault, Fault::SyncAfter) {
            return Err(io::Error::other("injected error after fsync"));
        }
        Ok(())
    }
}

fn store(
    path: &Path,
    fault: Fault,
    armed: Arc<AtomicBool>,
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
) -> Store {
    Store::open_using(path, |path, initialize| {
        let (mut log, index) = Log::open(path, initialize)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("records.log"))?;
        let bytes = std::fs::read(path.join("records.log"))?;
        assert_eq!(&bytes[..8], b"EMDALOG3");
        let id = bytes[8..24].try_into().unwrap();
        let (witness, _) = Witness::from_io(
            Box::new(Controlled {
                file,
                armed,
                fault,
                entered,
                resume: Mutex::new(resume),
                armed_syncs: AtomicUsize::new(0),
            }),
            id,
        )?;
        log.attach_test_witness(witness);
        Ok((log, index))
    })
    .unwrap()
}

#[test]
fn metadata_failures_and_panics_never_acknowledge_and_poison_the_store() {
    for fault in [
        Fault::Read,
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncAfter,
        Fault::Panic,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered, _) = mpsc::channel();
        let (_, resume) = mpsc::channel();
        let store = store(dir.path(), fault, Arc::clone(&armed), entered, resume);
        store.put(b"key", b"old").unwrap();
        armed.store(true, Ordering::Release);
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.put(b"key", b"new")));
        assert!(!matches!(outcome, Ok(Ok(()))));
        assert!(store.get(b"key").is_err());
        assert!(store.delete(b"key").is_err());
        drop(store);
        let reopened = Store::open(dir.path()).unwrap();
        let value = reopened.get(b"key").unwrap().unwrap();
        assert!(value == b"old" || value == b"new");
    }
}

#[test]
fn metadata_fsync_must_complete_before_publication_or_acknowledgement() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(store(
        dir.path(),
        Fault::Block,
        Arc::clone(&armed),
        entered_tx,
        resume_rx,
    ));
    store.put(b"key", b"old").unwrap();
    armed.store(true, Ordering::Release);
    let writer_store = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || done_tx.send(writer_store.put(b"key", b"new")).unwrap());
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let value_before_sync = store.get(b"key");
    let completion_before_sync = done_rx.try_recv();
    resume_tx.send(()).unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer.join().unwrap();
    assert_eq!(value_before_sync.unwrap().as_deref(), Some(&b"old"[..]));
    assert!(matches!(
        completion_before_sync,
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn transaction_deadline_during_fsync_does_not_report_an_aborted_commit() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(store(
        dir.path(),
        Fault::Block,
        Arc::clone(&armed),
        entered_tx,
        resume_rx,
    ));
    store.put(b"key", b"old").unwrap();
    armed.store(true, Ordering::Release);
    let writing = Arc::clone(&store);
    let deadline = Instant::now() + Duration::from_secs(2);
    let worker = thread::spawn(move || {
        writing.transaction_until(deadline, TransactionMode::ReadSet, |tx| {
            tx.put(b"key", b"new")
        })
    });
    let entered = entered_rx.recv_timeout(Duration::from_secs(5));
    if entered.is_ok() {
        thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
        );
    }
    let _ = resume_tx.send(());
    let result = worker.join().unwrap();
    entered.expect("transaction did not enter metadata synchronization");
    result.unwrap();
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
}

#[test]
fn transaction_fsync_timeout_remains_an_unknown_operation_error() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, _) = mpsc::channel();
    let (_, resume_rx) = mpsc::channel();
    let store = store(
        dir.path(),
        Fault::SyncTimeout,
        Arc::clone(&armed),
        entered_tx,
        resume_rx,
    );
    store.put(b"a", b"old").unwrap();
    store.put(b"b", b"old").unwrap();
    armed.store(true, Ordering::Release);
    let result = store.transaction_until(
        Instant::now() + Duration::from_secs(30),
        TransactionMode::ReadSet,
        |tx| {
            tx.put(b"a", b"new")?;
            tx.put(b"b", b"new")
        },
    );
    match result {
        Err(TimedTransactionError::Operation(error)) => {
            assert_eq!(error.kind(), io::ErrorKind::TimedOut)
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(store.get(b"a").is_err());
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    let values = reopened.get_many(&[b"a", b"b"]).unwrap();
    assert!(
        values == vec![Some(b"old".to_vec()), Some(b"old".to_vec())]
            || values == vec![Some(b"new".to_vec()), Some(b"new".to_vec())]
    );
}

#[test]
fn limited_key_byte_accounting_is_rebuilt_after_uncertain_single_and_batch_writes() {
    use crate::BatchOperation as Op;
    for fault in [Fault::SyncBefore, Fault::SyncAfter, Fault::Panic] {
        for batch in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let armed = Arc::new(AtomicBool::new(false));
            let (entered_tx, _) = mpsc::channel();
            let (_, resume_rx) = mpsc::channel();
            let store = store(dir.path(), fault, Arc::clone(&armed), entered_tx, resume_rx);
            store.put(b"a", b"old").unwrap();
            let store = Store::set_open_limits(store, None, None, Some(5)).unwrap();
            armed.store(true, Ordering::Release);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if batch {
                    store.write_batch(&[
                        Op::Put {
                            key: b"bb",
                            value: b"new",
                        },
                        Op::Put {
                            key: b"cc",
                            value: b"new",
                        },
                    ])
                } else {
                    store.put(b"long", b"new")
                }
            }));
            assert!(!matches!(outcome, Ok(Ok(()))));
            assert!(store.put(b"x", b"must-not-commit").is_err());
            drop(store);
            let recovered =
                Store::open_with_resource_limits(dir.path(), None, None, Some(5)).unwrap();
            let b = recovered.get(b"bb").unwrap();
            let c = recovered.get(b"cc").unwrap();
            assert_eq!(b.is_some(), c.is_some());
            let original_write_survived = if batch {
                b.is_some()
            } else {
                recovered.get(b"long").unwrap().is_some()
            };
            assert_eq!(
                recovered.put(b"z", b"later").is_err(),
                original_write_survived
            );
        }
    }
}

#[test]
fn capped_existing_key_updates_share_a_witness_sync_and_block_deletion() {
    use std::time::Instant;
    for (max_keys, max_bytes, log_capped) in [
        (Some(4), None, false),
        (None, Some(4), false),
        (Some(4), Some(4), false),
        (Some(4), Some(4), true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let store = store(
            dir.path(),
            Fault::Block,
            Arc::clone(&armed),
            entered_tx,
            resume_rx,
        );
        for key in [b"a", b"b", b"c", b"d"] {
            store.put(key, b"old").unwrap();
        }
        let max_log = log_capped.then(|| store.log.len().unwrap() + 1024);
        let store = Arc::new(Store::set_open_limits(store, max_log, max_keys, max_bytes).unwrap());
        armed.store(true, Ordering::Release);
        let first_store = Arc::clone(&store);
        let first = thread::spawn(move || first_store.put(b"a", b"new"));
        let first_entered = entered_rx.recv_timeout(Duration::from_secs(10));
        let (done_tx, done_rx) = mpsc::channel();
        let mut writers = Vec::new();
        for key in [b"b", b"c", b"d"] {
            let store = Arc::clone(&store);
            let done = done_tx.clone();
            writers.push(thread::spawn(move || {
                let result = store.put(key, b"new");
                let _ = done.send(());
                result
            }));
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.commits.queued_len() < 3 && Instant::now() < deadline {
            thread::yield_now();
        }
        let queued = store.commits.queued_len();
        let deleting = Arc::clone(&store);
        let (deleted_tx, deleted_rx) = mpsc::channel();
        let delete = thread::spawn(move || {
            let result = deleting.delete(b"d");
            let _ = deleted_tx.send(());
            result
        });
        let _ = resume_tx.send(());
        let group_entered = entered_rx.recv_timeout(Duration::from_secs(10));
        let completed_early = done_rx.try_recv().is_ok();
        let deleted_early = deleted_rx.try_recv().is_ok();
        let before_group = store.get_many(&[b"b", b"c", b"d"]);
        for _ in 0..8 {
            let _ = resume_tx.send(());
        }
        let first_result = first.join().unwrap();
        let results = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect::<Vec<_>>();
        let delete_result = delete.join().unwrap();
        first_entered.expect("first update did not start synchronization");
        group_entered.expect("queued updates did not start synchronization");
        assert_eq!(queued, 3, "existing-key updates did not share admission");
        assert!(!completed_early);
        assert!(!deleted_early);
        assert_eq!(before_group.unwrap(), vec![Some(b"old".to_vec()); 3]);
        first_result.unwrap();
        for result in results {
            result.unwrap();
        }
        delete_result.unwrap();
        assert!(store.get(b"d").unwrap().is_none());
        assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"new"[..]));
        // One extra synchronization belongs to the deletion; updates shared one.
        assert_eq!(entered_rx.try_iter().count(), 1);
        drop(store);
        let recovered =
            Store::open_with_resource_limits(dir.path(), max_log, max_keys, max_bytes).unwrap();
        recovered.put(b"z", b"replacement").unwrap();
        assert!(recovered.put(b"extra", b"over quota").is_err());
    }
}

#[test]
fn log_length_cap_reserves_inflight_bytes_before_other_updates() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = store(
        dir.path(),
        Fault::Block,
        Arc::clone(&armed),
        entered_tx,
        resume_rx,
    );
    store.put(b"a", b"old").unwrap();
    store.put(b"b", b"old").unwrap();
    let cap = store.log.len().unwrap()
        + record::encode(Operation::Put, b"a", b"new").unwrap().len() as u64;
    let store = Arc::new(Store::set_open_limits(store, Some(cap), Some(2), Some(2)).unwrap());
    armed.store(true, Ordering::Release);
    let first_store = Arc::clone(&store);
    let first = thread::spawn(move || first_store.put(b"a", b"new"));
    let entered = entered_rx.recv_timeout(Duration::from_secs(10));
    let second_store = Arc::clone(&store);
    let (attempted_tx, attempted_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let second = thread::spawn(move || {
        let _ = attempted_tx.send(());
        let _ = done_tx.send(second_store.put(b"b", b"new"));
    });
    let attempted = attempted_rx.recv_timeout(Duration::from_secs(10));
    let early = done_rx.recv_timeout(Duration::from_secs(3));
    let arrived_early = early.is_ok();
    for _ in 0..4 {
        let _ = resume_tx.send(());
    }
    let first_result = first.join().unwrap();
    let second_result =
        early.unwrap_or_else(|_| done_rx.recv_timeout(Duration::from_secs(10)).unwrap());
    second.join().unwrap();
    entered.expect("first update did not start synchronization");
    attempted.expect("second update did not start");
    assert!(
        arrived_early,
        "capacity should be reserved before the first sync completes"
    );
    first_result.unwrap();
    assert!(second_result.is_err());
    assert_eq!(store.log.len().unwrap(), cap);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"old"[..]));
}

#[test]
fn failed_capped_group_leader_wakes_existing_key_waiters_and_requires_recovery() {
    use std::time::Instant;
    for fault in [Fault::BlockThenSyncError, Fault::BlockThenPanic] {
        let dir = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let store = store(dir.path(), fault, Arc::clone(&armed), entered_tx, resume_rx);
        for key in [b"a", b"b", b"c", b"d"] {
            store.put(key, b"old").unwrap();
        }
        let store = Arc::new(Store::set_open_limits(store, None, Some(4), Some(4)).unwrap());
        armed.store(true, Ordering::Release);
        let first_store = Arc::clone(&store);
        let first = thread::spawn(move || first_store.put(b"a", b"new"));
        let entered = entered_rx.recv_timeout(Duration::from_secs(10));
        let mut writers = Vec::new();
        for key in [b"b", b"c", b"d"] {
            let store = Arc::clone(&store);
            writers.push(thread::spawn(move || store.put(key, b"new")));
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.commits.queued_len() < 3 && Instant::now() < deadline {
            thread::yield_now();
        }
        let queued = store.commits.queued_len();
        let _ = resume_tx.send(());
        let first_result = first.join();
        let results = writers
            .into_iter()
            .map(|writer| writer.join())
            .collect::<Vec<_>>();
        entered.expect("group leader did not reach synchronization");
        assert_eq!(queued, 3);
        assert!(!matches!(first_result, Ok(Ok(()))));
        assert!(
            results
                .into_iter()
                .all(|result| matches!(result, Ok(Err(_))))
        );
        assert!(store.get(b"a").is_err());
        drop(store);
        let recovered =
            Store::open_with_resource_limits(dir.path(), None, Some(4), Some(4)).unwrap();
        for key in [b"b", b"c", b"d"] {
            assert_eq!(recovered.get(key).unwrap().as_deref(), Some(&b"old"[..]));
        }
        assert!(recovered.put(b"z", b"too-many").is_err());
    }
}

#[test]
fn failed_capped_group_sync_never_acknowledges_waiters_or_oversubscribes() {
    use std::time::Instant;
    for fault in [Fault::BlockThenGroupError, Fault::BlockThenGroupPanic] {
        let dir = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let store = store(dir.path(), fault, Arc::clone(&armed), entered_tx, resume_rx);
        for key in [b"a", b"b", b"c", b"d"] {
            store.put(key, b"old").unwrap();
        }
        let max_log = Some(store.log.len().unwrap() + 1024);
        let store = Arc::new(Store::set_open_limits(store, max_log, Some(4), Some(4)).unwrap());
        armed.store(true, Ordering::Release);
        let first_store = Arc::clone(&store);
        let first = thread::spawn(move || first_store.put(b"a", b"new"));
        let first_entered = entered_rx.recv_timeout(Duration::from_secs(10));
        let (done_tx, done_rx) = mpsc::channel();
        let mut writers = Vec::new();
        for key in [b"b", b"c", b"d"] {
            let store = Arc::clone(&store);
            let done = done_tx.clone();
            writers.push(thread::spawn(move || {
                let result = store.put(key, b"new");
                let _ = done.send(());
                result
            }));
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.commits.queued_len() < 3 && Instant::now() < deadline {
            thread::yield_now();
        }
        let queued = store.commits.queued_len();
        let _ = resume_tx.send(());
        let group_entered = entered_rx.recv_timeout(Duration::from_secs(10));
        let completed_early = done_rx.try_recv().is_ok();
        let before_group = store.get_many(&[b"b", b"c", b"d"]);
        for _ in 0..8 {
            let _ = resume_tx.send(());
        }
        let first_result = first.join();
        let results = writers
            .into_iter()
            .map(|writer| writer.join())
            .collect::<Vec<_>>();
        first_entered.expect("first update did not reach synchronization");
        group_entered.expect("grouped updates did not reach synchronization");
        assert_eq!(queued, 3);
        assert!(!completed_early);
        assert_eq!(before_group.unwrap(), vec![Some(b"old".to_vec()); 3]);
        first_result.unwrap().unwrap();
        assert!(
            results
                .into_iter()
                .all(|result| !matches!(result, Ok(Ok(()))))
        );
        assert!(store.get(b"a").is_err());
        drop(store);
        let recovered =
            Store::open_with_resource_limits(dir.path(), max_log, Some(4), Some(4)).unwrap();
        assert_eq!(recovered.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
        for key in [b"b", b"c", b"d"] {
            let value = recovered.get(key).unwrap().unwrap();
            assert!(value == b"old" || value == b"new");
        }
        assert!(recovered.put(b"z", b"too-many").is_err());
    }
}

#[test]
fn atomic_batch_is_not_published_until_metadata_fsync_completes() {
    use crate::BatchOperation as Op;
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(store(
        dir.path(),
        Fault::Block,
        Arc::clone(&armed),
        entered_tx,
        resume_rx,
    ));
    store.put(b"a", b"old-a").unwrap();
    store.put(b"b", b"old-b").unwrap();
    armed.store(true, Ordering::Release);
    let writing = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let result = writing.write_batch(&[
            Op::Put {
                key: b"a",
                value: b"new-a",
            },
            Op::Put {
                key: b"b",
                value: b"new-b",
            },
        ]);
        done_tx.send(result).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let before = store.get_many(&[b"a", b"b"]);
    let ack = done_rx.try_recv();
    resume_tx.send(()).unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer.join().unwrap();
    assert_eq!(
        before.unwrap(),
        vec![Some(b"old-a".to_vec()), Some(b"old-b".to_vec())]
    );
    assert!(matches!(ack, Err(mpsc::TryRecvError::Empty)));
    assert_eq!(
        store.get_many(&[b"a", b"b"]).unwrap(),
        vec![Some(b"new-a".to_vec()), Some(b"new-b".to_vec())]
    );
}

#[test]
fn atomic_batch_metadata_failures_never_publish_a_partial_batch() {
    use crate::BatchOperation as Op;
    for fault in [
        Fault::Read,
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncAfter,
        Fault::Panic,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered, _) = mpsc::channel();
        let (_, resume) = mpsc::channel();
        let store = store(dir.path(), fault, Arc::clone(&armed), entered, resume);
        store.put(b"a", b"old").unwrap();
        store.put(b"b", b"old").unwrap();
        armed.store(true, Ordering::Release);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.write_batch(&[
                Op::Put {
                    key: b"a",
                    value: b"new",
                },
                Op::Put {
                    key: b"b",
                    value: b"new",
                },
            ])
        }));
        assert!(!matches!(outcome, Ok(Ok(()))));
        assert!(store.get_many(&[b"a", b"b"]).is_err());
        drop(store);
        let reopened = Store::open(dir.path()).unwrap();
        let pair = reopened.get_many(&[b"a", b"b"]).unwrap();
        assert!(
            pair == vec![Some(b"old".to_vec()), Some(b"old".to_vec())]
                || pair == vec![Some(b"new".to_vec()), Some(b"new".to_vec())],
            "partial batch: {pair:?}"
        );
    }
}

#[test]
fn transaction_conflicts_with_writer_that_was_syncing_during_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(store(
        dir.path(),
        Fault::Block,
        Arc::clone(&armed),
        entered_tx,
        resume_rx,
    ));
    store.put(b"key", b"old").unwrap();
    armed.store(true, Ordering::Release);
    let writing = Arc::clone(&store);
    let writer = thread::spawn(move || writing.put(b"key", b"new"));
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let transactional = Arc::clone(&store);
    let (observed_tx, observed_rx) = mpsc::channel();
    let transaction = thread::spawn(move || {
        transactional.transaction(|tx| {
            assert_eq!(tx.get(b"key")?.as_deref(), Some(&b"old"[..]));
            tx.put(b"other", b"staged")?;
            observed_tx.send(()).unwrap();
            Ok(())
        })
    });
    let observed = observed_rx.recv_timeout(Duration::from_secs(5));
    resume_tx.send(()).unwrap();
    writer.join().unwrap().unwrap();
    observed.unwrap();
    assert_eq!(
        transaction.join().unwrap().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(store.get(b"other").unwrap(), None);
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn transaction_commit_metadata_failures_never_recover_partial_writes() {
    for fault in [
        Fault::Read,
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncAfter,
        Fault::Panic,
    ] {
        for read_set in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let armed = Arc::new(AtomicBool::new(false));
            let (entered, _) = mpsc::channel();
            let (_, resume) = mpsc::channel();
            let store = store(dir.path(), fault, Arc::clone(&armed), entered, resume);
            store.put(b"a", b"old").unwrap();
            store.put(b"b", b"old").unwrap();
            armed.store(true, Ordering::Release);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let action = |tx: &mut Transaction<'_>| -> io::Result<()> {
                    assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
                    tx.put(b"a", b"new")?;
                    tx.put(b"b", b"new")?;
                    Ok(())
                };
                if read_set {
                    store.transaction_with_read_set(action)
                } else {
                    store.transaction(action)
                }
            }));
            assert!(!matches!(outcome, Ok(Ok(()))));
            assert!(store.get_many(&[b"a", b"b"]).is_err());
            drop(store);
            let pair = Store::open(dir.path())
                .unwrap()
                .get_many(&[b"a", b"b"])
                .unwrap();
            assert!(
                pair == vec![Some(b"old".to_vec()), Some(b"old".to_vec())]
                    || pair == vec![Some(b"new".to_vec()), Some(b"new".to_vec())],
                "partial transaction: {pair:?}"
            );
        }
    }
}

#[test]
fn failed_fifo_transaction_wakes_waiter_and_preserves_atomic_recovery() {
    use std::time::Instant;
    for panic_on_sync in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let store = Arc::new(store(
            directory.path(),
            if panic_on_sync {
                Fault::BlockThenPanic
            } else {
                Fault::BlockThenSyncError
            },
            Arc::clone(&armed),
            entered_tx,
            resume_rx,
        ));
        store.put(b"a", b"old").unwrap();
        store.put(b"b", b"old").unwrap();
        armed.store(true, Ordering::Release);
        let first_store = Arc::clone(&store);
        let first = thread::spawn(move || {
            first_store.transaction_with_read_set_fifo(|tx| {
                tx.put(b"a", b"new")?;
                tx.put(b"b", b"new")
            })
        });
        if entered_rx.recv_timeout(Duration::from_secs(10)).is_err() {
            let _ = resume_tx.send(());
            let _ = first.join();
            panic!("first transaction did not reach the sync boundary");
        }
        let (staged_tx, staged_rx) = mpsc::channel();
        let second_store = Arc::clone(&store);
        let second = thread::spawn(move || {
            second_store.transaction_with_read_set_fifo(|tx| {
                tx.put(b"c", b"should-not-commit")?;
                staged_tx.send(()).map_err(io::Error::other)
            })
        });
        let staged = staged_rx.recv_timeout(Duration::from_secs(10));
        let deadline = Instant::now() + Duration::from_secs(10);
        let queued = loop {
            if store.transaction_queue.queued_for_test() == 2 {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            thread::yield_now();
        };
        resume_tx.send(()).unwrap();
        let first_result = first.join();
        let second_result = second.join().unwrap();
        staged.expect("second transaction did not finish staging");
        assert!(queued, "second transaction did not enter FIFO admission");
        if panic_on_sync {
            assert!(first_result.is_err());
        } else {
            assert!(first_result.unwrap().is_err());
        }
        assert!(second_result.is_err());
        assert!(store.get(b"a").is_err());
        drop(store);
        let reopened = Store::open(directory.path()).unwrap();
        let values = reopened.get_many(&[b"a", b"b", b"c"]).unwrap();
        assert!(
            values == vec![Some(b"old".to_vec()), Some(b"old".to_vec()), None]
                || values == vec![Some(b"new".to_vec()), Some(b"new".to_vec()), None]
        );
    }
}
