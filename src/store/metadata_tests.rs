//! Failure and publication checks through the real V3 coordinator path.
use super::*;
use crate::witness::{Witness, WitnessIo};
use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};
use std::sync::atomic::{AtomicBool, Ordering};
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
    SyncAfter,
    Panic,
    Block,
    BlockThenSyncError,
}

struct Controlled {
    file: File,
    armed: Arc<AtomicBool>,
    fault: Fault,
    entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
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
    let directory = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Arc::new(store(
        directory.path(),
        Fault::BlockThenSyncError,
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
    let first_result = first.join().unwrap();
    let second_result = second.join().unwrap();
    staged.expect("second transaction did not finish staging");
    assert!(queued, "second transaction did not enter FIFO admission");
    assert!(first_result.is_err());
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
