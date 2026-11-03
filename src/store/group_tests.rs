//! Group-commit synchronization, failure, admission, and backup tests.
use super::fault_tests::BlockingSyncIo;
use super::*;
use crate::log_io::LogIo;
use std::fs;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy)]
enum GroupFault {
    None,
    PartialAppend,
    SyncError,
    SyncErrorAfter,
    SyncPanic,
}

struct GroupIo {
    file: File,
    appends: Arc<std::sync::atomic::AtomicUsize>,
    syncs: Arc<std::sync::atomic::AtomicUsize>,
    started: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
    group_started: mpsc::Sender<()>,
    group_resume: Mutex<mpsc::Receiver<()>>,
    fault: GroupFault,
}

impl LogIo for GroupIo {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, buffer, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        let count = self.appends.fetch_add(1, Ordering::SeqCst) + 1;
        let mut file = &self.file;
        if count == 4 && matches!(self.fault, GroupFault::PartialAppend) {
            file.write_all(&bytes[..5])?;
            return Err(io::Error::from_raw_os_error(28));
        }
        file.write_all(bytes)
    }

    fn sync_data(&self) -> io::Result<()> {
        let count = self.syncs.fetch_add(1, Ordering::SeqCst) + 1;
        if count == 2 {
            self.started.send(()).map_err(io::Error::other)?;
            self.resume
                .lock()
                .unwrap()
                .recv()
                .map_err(io::Error::other)?;
        }
        if count == 3 {
            self.group_started.send(()).map_err(io::Error::other)?;
            self.group_resume
                .lock()
                .unwrap()
                .recv()
                .map_err(io::Error::other)?;
            match self.fault {
                GroupFault::SyncError => {
                    return Err(io::Error::other("injected group sync failure"));
                }
                GroupFault::SyncErrorAfter => {
                    self.file.sync_data()?;
                    return Err(io::Error::other(
                        "injected failure after group synchronization",
                    ));
                }
                GroupFault::SyncPanic => panic!("injected group sync panic"),
                _ => {}
            }
        }
        self.file.sync_data()
    }
}

fn group_fault_case(fault: GroupFault) {
    let dir = tempfile::tempdir().unwrap();
    let appends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let syncs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (group_started_tx, group_started_rx) = mpsc::channel();
    let (group_resume_tx, group_resume_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let store = Arc::new(
        Store::open_using(dir.path(), |path, _| {
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.join("records.log"))?;
            File::open(path)?.sync_all()?;
            Log::from_io(Box::new(GroupIo {
                file,
                appends: Arc::clone(&appends),
                syncs: Arc::clone(&syncs),
                started: started_tx,
                resume: Mutex::new(resume_rx),
                group_started: group_started_tx,
                group_resume: Mutex::new(group_resume_rx),
                fault,
            }))
        })
        .unwrap(),
    );
    store.put(b"safe", b"committed").unwrap();
    let first_store = Arc::clone(&store);
    let first = thread::spawn(move || first_store.put(b"first", b"committed"));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let mut writers = Vec::new();
    for id in 0..3 {
        let store = Arc::clone(&store);
        let done_tx = done_tx.clone();
        writers.push(thread::spawn(move || {
            let result = store.put(format!("group-{id}").as_bytes(), b"value");
            let _ = done_tx.send(());
            result
        }));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store.commits.queued_len() < 3 && std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    let queued = store.commits.queued_len();
    resume_tx.send(()).unwrap();
    assert_eq!(
        queued, 3,
        "all grouped writers should be queued before release"
    );
    first.join().unwrap().unwrap();
    if !matches!(fault, GroupFault::PartialAppend) {
        group_started_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        let completed_early = done_rx.try_recv().is_ok();
        let visible_early = (0..3).any(|id| {
            store
                .get(format!("group-{id}").as_bytes())
                .unwrap()
                .is_some()
        });
        group_resume_tx.send(()).unwrap();
        assert!(
            !completed_early,
            "a grouped write returned before its sync completed"
        );
        assert!(
            !visible_early,
            "a grouped write was visible before its sync completed"
        );
    }
    let mut successes = 0;
    let mut errors = 0;
    let mut panics = 0;
    for writer in writers {
        match writer.join() {
            Ok(Ok(())) => successes += 1,
            Ok(Err(_)) => errors += 1,
            Err(_) => panics += 1,
        }
    }
    match fault {
        GroupFault::None => {
            assert_eq!((successes, errors, panics), (3, 0, 0));
            assert_eq!(
                syncs.load(Ordering::SeqCst),
                3,
                "three concurrent writes should share a sync"
            );
            for id in 0..3 {
                assert_eq!(
                    store
                        .get(format!("group-{id}").as_bytes())
                        .unwrap()
                        .as_deref(),
                    Some(&b"value"[..])
                );
            }
        }
        GroupFault::PartialAppend | GroupFault::SyncError | GroupFault::SyncErrorAfter => {
            assert_eq!((successes, errors, panics), (0, 3, 0));
            assert!(store.get(b"safe").is_err());
            assert!(store.put(b"later", b"value").is_err());
        }
        GroupFault::SyncPanic => {
            assert_eq!((successes, errors, panics), (0, 2, 1));
            assert!(store.get(b"safe").is_err());
            assert!(store.put(b"later", b"value").is_err());
        }
    }
    drop(store);
    let path = dir.path().join("records.log");
    let before = fs::read(&path).unwrap();
    let reopened = Store::open(dir.path());
    if matches!(fault, GroupFault::PartialAppend) {
        assert!(matches!(reopened, Err(ref e) if e.kind() == io::ErrorKind::InvalidData));
        assert_eq!(fs::read(path).unwrap(), before);
    } else {
        let reopened = reopened.unwrap();
        assert_eq!(
            reopened.get(b"safe").unwrap().as_deref(),
            Some(&b"committed"[..])
        );
        assert_eq!(
            reopened.get(b"first").unwrap().as_deref(),
            Some(&b"committed"[..])
        );
    }
}

#[test]
fn concurrent_writes_share_a_sync_before_success() {
    group_fault_case(GroupFault::None);
}
#[test]
fn partial_append_in_a_group_fails_all_and_preserves_the_log() {
    group_fault_case(GroupFault::PartialAppend);
}
#[test]
fn failed_group_sync_fails_all_waiters() {
    group_fault_case(GroupFault::SyncError);
}
#[test]
fn error_after_group_sync_keeps_outcomes_uncertain() {
    group_fault_case(GroupFault::SyncErrorAfter);
}
#[test]
fn panic_in_group_sync_wakes_waiters_and_blocks_new_work() {
    group_fault_case(GroupFault::SyncPanic);
}

fn bounded_queue_case(max_bytes: usize, max_records: usize, expected_buffered: usize) {
    let dir = tempfile::tempdir().unwrap();
    let appends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let syncs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (group_started_tx, _group_started_rx) = mpsc::channel();
    let (group_resume_tx, group_resume_rx) = mpsc::channel();
    let mut store = Store::open_using(dir.path(), |path, _| {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join("records.log"))?;
        File::open(path)?.sync_all()?;
        Log::from_io(Box::new(GroupIo {
            file,
            appends,
            syncs,
            started: started_tx,
            resume: Mutex::new(resume_rx),
            group_started: group_started_tx,
            group_resume: Mutex::new(group_resume_rx),
            fault: GroupFault::None,
        }))
    })
    .unwrap();
    store.commits = Coordinator::for_test(max_bytes, max_records);
    let store = Arc::new(store);
    store.put(b"safe", b"initial").unwrap();
    let first_store = Arc::clone(&store);
    let first = thread::spawn(move || first_store.put(b"first", &[7; 512]));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let mut writers = Vec::new();
    for id in 0..3 {
        let store = Arc::clone(&store);
        let attempt_tx = attempt_tx.clone();
        writers.push(thread::spawn(move || {
            attempt_tx.send(()).unwrap();
            store.put(format!("writer-{id}").as_bytes(), &[8; 512])
        }));
    }
    for _ in 0..3 {
        attempt_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store.commits.buffered().1 < expected_buffered && std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    let (bytes, records) = store.commits.buffered();
    group_resume_tx.send(()).unwrap();
    resume_tx.send(()).unwrap();
    assert_eq!(records, expected_buffered);
    assert!(bytes <= max_bytes);
    first.join().unwrap().unwrap();
    for writer in writers {
        writer.join().unwrap().unwrap();
    }
    assert_eq!(store.commits.buffered(), (0, 0));
}

#[test]
fn buffered_group_writes_respect_byte_and_record_limits() {
    bounded_queue_case(4096, 2, 2);
    bounded_queue_case(1024, 10, 1);
}

#[test]
fn backup_waits_for_synchronized_writes_before_taking_snapshot() {
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
    let writing = Arc::clone(&store);
    let writer = thread::spawn(move || writing.put(b"key", b"new"));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let target = dir.path().join("backup");
    let target_thread = target.clone();
    let backup_store = Arc::clone(&store);
    let (begun_tx, begun_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let backup = thread::spawn(move || {
        begun_tx.send(()).unwrap();
        let result = backup_store.backup(target_thread);
        let _ = done_tx.send(());
        result
    });
    begun_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let premature = done_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    resume_tx.send(()).unwrap();
    writer.join().unwrap().unwrap();
    backup.join().unwrap().unwrap();
    assert!(
        !premature,
        "backup completed while a write was synchronizing"
    );
    let restored = dir.path().join("restored");
    Store::restore(&target, &restored).unwrap();
    let restored_store = Store::open(restored).unwrap();
    assert_eq!(
        restored_store.get(b"key").unwrap().as_deref(),
        Some(&b"new"[..])
    );
}
