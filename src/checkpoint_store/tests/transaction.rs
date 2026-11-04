use super::*;

#[test]
fn staged_budget_is_shared_and_released_on_abort_and_panic() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let value = [7; 48];
    let cost = HEADER_SIZE + 1 + value.len() + CHECKSUM_SIZE;
    store.transaction_budget.lock().unwrap().limit = cost * 2 - 1;
    let result: io::Result<()> = store.transaction(|outer| {
        outer.put(b"a", &value)?;
        outer.put(b"a", b"x")?;
        assert_eq!(
            store.transaction_budget.lock().unwrap().used,
            HEADER_SIZE + 2 + CHECKSUM_SIZE
        );
        outer.put(b"a", &value)?;
        let rejected: io::Result<()> = store.transaction(|inner| inner.put(b"b", &value));
        assert!(rejected.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, cost);
        let aborted: io::Result<()> = store.transaction(|inner| {
            inner.put(b"b", b"x")?;
            Err(io::Error::other("abort nested transaction"))
        });
        assert!(aborted.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, cost);
        Err(io::Error::other("abort outer transaction"))
    });
    assert!(result.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    assert_eq!(store.get(b"a").unwrap(), None);
    assert_eq!(store.get(b"b").unwrap(), None);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: io::Result<()> = store.transaction(|tx| {
            tx.put(b"a", &value)?;
            panic!("abort transaction callback")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    store.transaction(|tx| tx.put(b"a", b"ok")).unwrap();
}

#[test]
fn generation_exhaustion_rejects_a_write_before_append() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.write().unwrap().generation = u64::MAX;
    let before = fs::read(path.join("records.log")).unwrap();
    assert!(store.put(b"new", b"value").is_err());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(store.get(b"new").unwrap(), None);
}

fn wait_for_queue(store: &CheckpointStore, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while store.transaction_queue.queued_for_test() != expected {
        assert!(
            std::time::Instant::now() < deadline,
            "transaction did not enter queue"
        );
        std::thread::yield_now();
    }
}

#[test]
fn cancellation_removes_a_checkpoint_fifo_waiter_before_callback() {
    use crate::{CancellationToken, CheckpointTransactionMode, ControlledTransactionError};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::time::Duration;
    let (_dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let first = store.transaction_queue.enter().unwrap();
    let token = CancellationToken::new();
    let called = Arc::new(AtomicBool::new(false));
    let (done_tx, done_rx) = mpsc::channel();
    let writer = Arc::clone(&store);
    let worker_token = token.clone();
    let worker_called = Arc::clone(&called);
    let worker = std::thread::spawn(move || {
        let result = writer.transaction_controlled(
            &worker_token,
            None,
            CheckpointTransactionMode::Fifo,
            |tx| {
                worker_called.store(true, Ordering::Relaxed);
                tx.put(b"key", b"value")
            },
        );
        done_tx.send(result).unwrap();
    });
    wait_for_queue(&store, 2);
    let follower_store = Arc::clone(&store);
    let follower = std::thread::spawn(move || {
        follower_store.transaction_with_fifo(|tx| tx.put(b"later", b"ok"))
    });
    wait_for_queue(&store, 3);
    token.cancel();
    let early = done_rx.recv_timeout(Duration::from_secs(5));
    drop(first);
    let early_result = early.is_ok();
    let result = early.unwrap_or_else(|_| done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
    worker.join().unwrap();
    follower.join().unwrap().unwrap();
    assert!(
        early_result,
        "cancelled FIFO admission did not finish until the prior turn ended"
    );
    assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
    assert!(!called.load(Ordering::Relaxed));
    assert_eq!(store.transaction_queue.queued_for_test(), 0);
    assert_eq!(store.get(b"key").unwrap(), None);
    assert_eq!(store.get(b"later").unwrap().as_deref(), Some(&b"ok"[..]));
}

#[test]
fn cancellation_after_callback_with_checkpoint_state_lock_held_aborts_before_append() {
    use crate::{CancellationToken, CheckpointTransactionMode, ControlledTransactionError};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    let (_dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let token = CancellationToken::new();
    let (staged_tx, staged_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer = Arc::clone(&store);
    let worker_token = token.clone();
    let worker = std::thread::spawn(move || {
        writer.transaction_controlled(
            &worker_token,
            None,
            CheckpointTransactionMode::Conservative,
            |tx| {
                tx.put(b"key", b"value")?;
                staged_tx.send(()).map_err(io::Error::other)?;
                resume_rx.recv().map_err(io::Error::other)?;
                finished_tx.send(()).map_err(io::Error::other)
            },
        )
    });
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let guard = store.write().unwrap();
    resume_tx.send(()).unwrap();
    finished_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    token.cancel();
    drop(guard);
    assert!(matches!(
        worker.join().unwrap(),
        Err(ControlledTransactionError::Cancelled)
    ));
    assert_eq!(store.get(b"key").unwrap(), None);
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
}

#[test]
fn fifo_runs_callbacks_and_commits_in_admission_order() {
    use std::sync::{Arc, mpsc};
    let (_dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let first_turn = store.transaction_queue.enter().unwrap();
    let (send, receive) = mpsc::channel();
    let mut handles = Vec::new();
    for index in 0..3_u8 {
        let waiting = Arc::clone(&store);
        let send = send.clone();
        handles.push(std::thread::spawn(move || {
            waiting.transaction_with_fifo(|tx| {
                send.send(index).unwrap();
                tx.put(b"winner", &[index])
            })
        }));
        wait_for_queue(&store, usize::from(index) + 2);
    }
    drop(first_turn);
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    for result in results {
        result.unwrap();
    }
    assert_eq!(receive.try_iter().collect::<Vec<_>>(), vec![0, 1, 2]);
    assert_eq!(store.get(b"winner").unwrap().as_deref(), Some(&[2][..]));
    assert_eq!(store.transaction_queue.queued_for_test(), 0);
    // Same-thread nested FIFO admission is rejected before it can deadlock.
    store
        .transaction_with_fifo(|outer| {
            let nested = store.transaction_with_fifo(|_| Ok(()));
            assert_eq!(nested.unwrap_err().kind(), io::ErrorKind::Other);
            outer.put(b"outer", b"ok")
        })
        .unwrap();
}

#[test]
fn fifo_waiters_wake_after_failed_or_panicking_commit() {
    use std::sync::Arc;
    for fault in [LogFault::SyncBefore, LogFault::Panic(1)] {
        let (_dir, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let store = Arc::new(store);
        let first_turn = store.transaction_queue.enter().unwrap();
        let first_store = Arc::clone(&store);
        let first = std::thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                first_store.transaction_with_fifo(|tx| {
                    tx.put(b"left", b"one")?;
                    tx.put(b"right", b"two")
                })
            }))
        });
        wait_for_queue(&store, 2);
        let second_store = Arc::clone(&store);
        let second = std::thread::spawn(move || {
            second_store.transaction_with_fifo(|tx| tx.put(b"other", b"must-not-commit"))
        });
        wait_for_queue(&store, 3);
        drop(first_turn);
        let first_result = first.join().unwrap();
        assert!(first_result.is_err() || first_result.unwrap().is_err());
        assert!(second.join().unwrap().is_err());
        assert_eq!(store.transaction_queue.queued_for_test(), 0);
        failed_instance_rejects_all_operations(&store, &path);
        drop(store);
        let recovered = retry_lock(|| Store::open(&path));
        if matches!(fault, LogFault::Panic(_)) {
            assert!(matches!(
                recovered.err().unwrap().kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
        } else {
            let recovered = recovered.unwrap();
            assert_eq!(
                recovered.get_many(&[b"left", b"right"]).unwrap(),
                vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
            );
            assert_eq!(recovered.get(b"other").unwrap(), None);
        }
    }
}

#[test]
fn fifo_other_writes_still_conflict_and_callback_panic_releases_turn() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let error = store
        .transaction_with_fifo(|tx| {
            tx.put(b"staged", b"discarded")?;
            store.put(b"independent", b"committed")?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(store.get(b"staged").unwrap(), None);
    assert_eq!(
        store.get(b"independent").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: io::Result<()> = store.transaction_with_fifo(|tx| {
            tx.put(b"staged", b"discarded")?;
            panic!("injected callback panic")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(store.transaction_queue.queued_for_test(), 0);
    assert_eq!(store.get(b"staged").unwrap(), None);
    store
        .transaction_with_fifo(|tx| tx.put(b"later", b"committed"))
        .unwrap();
}

#[test]
fn fifo_deadline_expires_in_queue_without_running_callback_or_stranding_followers() {
    use crate::{CheckpointTransactionMode, TimedTransactionError};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    let (_dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let turn = store.transaction_queue.enter().unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let waiting = Arc::clone(&store);
    let was_called = Arc::clone(&called);
    let deadline = Instant::now() + Duration::from_secs(2);
    let worker = std::thread::spawn(move || {
        waiting.transaction_until(deadline, CheckpointTransactionMode::Fifo, |_| {
            was_called.store(true, Ordering::SeqCst);
            Ok(())
        })
    });
    wait_for_queue(&store, 2);
    assert!(matches!(
        worker.join().unwrap(),
        Err(TimedTransactionError::DeadlineExceeded)
    ));
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(store.transaction_queue.queued_for_test(), 1);
    drop(turn);
    store
        .transaction_with_fifo(|tx| tx.put(b"later", b"ok"))
        .unwrap();
}

#[test]
fn expiry_waiting_for_state_lock_aborts_before_commit() {
    use crate::{CheckpointTransactionMode, TimedTransactionError};
    use std::sync::{Arc, mpsc};
    use std::time::{Duration, Instant};
    for mode in [
        CheckpointTransactionMode::Conservative,
        CheckpointTransactionMode::Fifo,
    ] {
        let (_dir, path, checkpoint, digest) = fixture();
        let store =
            Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
        let (staged_send, staged_receive) = mpsc::channel();
        let (resume_send, resume_receive) = mpsc::channel();
        let waiting = Arc::clone(&store);
        let deadline = Instant::now() + Duration::from_secs(2);
        let worker = std::thread::spawn(move || {
            waiting.transaction_until(deadline, mode, |tx| {
                tx.put(b"staged", b"discarded")?;
                staged_send.send(()).unwrap();
                resume_receive.recv().unwrap();
                Ok(())
            })
        });
        staged_receive
            .recv_timeout(Duration::from_secs(5))
            .expect("callback did not stage");
        let state = store.write().unwrap();
        resume_send.send(()).unwrap();
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
        );
        drop(state);
        assert!(matches!(
            worker.join().unwrap(),
            Err(TimedTransactionError::DeadlineExceeded)
        ));
        assert_eq!(store.get(b"staged").unwrap(), None);
    }
}

struct BlockingSyncLog {
    file: File,
    entered: std::sync::mpsc::Sender<()>,
    resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    failure: Option<io::ErrorKind>,
}

impl LogIo for BlockingSyncLog {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, bytes, offset)
    }
    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        (&self.file).write_all(bytes)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.entered.send(()).map_err(io::Error::other)?;
        self.resume
            .lock()
            .unwrap()
            .recv()
            .map_err(io::Error::other)?;
        if let Some(kind) = self.failure {
            return Err(io::Error::new(kind, "injected delayed sync failure"));
        }
        self.file.sync_data()
    }
}

#[test]
fn deadline_passed_during_sync_does_not_report_a_successful_commit_as_aborted() {
    use crate::CheckpointTransactionMode;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};
    let (_dir, path, checkpoint, digest) = fixture();
    let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    let (entered_send, entered_receive) = mpsc::channel();
    let (resume_send, resume_receive) = mpsc::channel();
    store.log.replace_io_for_test(Box::new(BlockingSyncLog {
        file,
        entered: entered_send,
        resume: Mutex::new(resume_receive),
        failure: None,
    }));
    let store = Arc::new(store);
    let writing = Arc::clone(&store);
    let deadline = Instant::now() + Duration::from_secs(2);
    let worker = std::thread::spawn(move || {
        writing.transaction_until(deadline, CheckpointTransactionMode::Fifo, |tx| {
            tx.put(b"left", b"one")?;
            tx.put(b"right", b"two")
        })
    });
    let entered = entered_receive.recv_timeout(Duration::from_secs(5));
    if entered.is_ok() {
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
        );
    }
    let _ = resume_send.send(());
    let result = worker.join().unwrap();
    entered.expect("transaction did not reach log synchronization");
    result.unwrap();
    assert_eq!(
        store.get_many(&[b"left", b"right"]).unwrap(),
        vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
    );
    drop(store);
    let recovered = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        recovered.get_many(&[b"left", b"right"]).unwrap(),
        vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
    );
}

#[test]
fn cancellation_during_checkpoint_sync_preserves_success_or_unknown_io_outcome() {
    use crate::{CancellationToken, CheckpointTransactionMode, ControlledTransactionError};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;
    for failure in [
        None,
        Some(io::ErrorKind::Other),
        Some(io::ErrorKind::Interrupted),
    ] {
        let (_dir, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        let (entered_send, entered_receive) = mpsc::channel();
        let (resume_send, resume_receive) = mpsc::channel();
        store.log.replace_io_for_test(Box::new(BlockingSyncLog {
            file,
            entered: entered_send,
            resume: Mutex::new(resume_receive),
            failure,
        }));
        let store = Arc::new(store);
        let writing = Arc::clone(&store);
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let worker = std::thread::spawn(move || {
            writing.transaction_controlled(
                &worker_token,
                None,
                CheckpointTransactionMode::Fifo,
                |tx| {
                    tx.put(b"left", b"one")?;
                    tx.put(b"right", b"two")
                },
            )
        });
        let entered = entered_receive.recv_timeout(Duration::from_secs(10));
        token.cancel();
        let _ = resume_send.send(());
        let result = worker.join().unwrap();
        entered.expect("transaction did not enter synchronization");
        if let Some(kind) = failure {
            assert!(
                matches!(result, Err(ControlledTransactionError::Operation(error)) if error.kind() == kind)
            );
            failed_instance_rejects_all_operations(&store, &path);
        } else {
            result.unwrap();
            assert_eq!(
                store.get_many(&[b"left", b"right"]).unwrap(),
                vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
            );
        }
        drop(store);
        let recovered = retry_lock(|| Store::open(&path)).unwrap();
        let values = recovered.get_many(&[b"left", b"right"]).unwrap();
        assert!(
            values == vec![None, None]
                || values == vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
        );
        if failure.is_none() {
            assert_eq!(values, vec![Some(b"one".to_vec()), Some(b"two".to_vec())]);
        }
    }
}

#[test]
fn sync_timeout_is_an_unknown_operation_error_not_confirmed_expiry() {
    use crate::{CheckpointTransactionMode, TimedTransactionError};
    use std::time::{Duration, Instant};
    let (_dir, path, checkpoint, digest) = fixture();
    let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    store.log.replace_io_for_test(Box::new(FaultLog {
        file,
        fault: LogFault::SyncTimeout,
    }));
    let result = store.transaction_until(
        Instant::now() + Duration::from_secs(30),
        CheckpointTransactionMode::Fifo,
        |tx| {
            tx.put(b"left", b"one")?;
            tx.put(b"right", b"two")
        },
    );
    match result {
        Err(TimedTransactionError::Operation(error)) => {
            assert_eq!(error.kind(), io::ErrorKind::TimedOut)
        }
        other => panic!("unexpected result: {other:?}"),
    }
    failed_instance_rejects_all_operations(&store, &path);
    drop(store);
    let recovered = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        recovered.get_many(&[b"left", b"right"]).unwrap(),
        vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
    );
}

#[test]
fn point_read_reservations_share_the_staged_budget_and_release_on_abort_and_panic() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let charge = b"missing-a".len() + std::mem::size_of::<Option<crate::log::ValueLocation>>();
    store.transaction_budget.lock().unwrap().limit = charge * 2 - 1;
    let result: io::Result<()> = store.transaction_with_read_set(|outer| {
        assert_eq!(outer.get(b"missing-a")?, None);
        assert_eq!(store.transaction_budget.lock().unwrap().used, charge);
        let rejected = store.transaction_with_read_set(|inner| inner.get(b"missing-b"));
        assert!(rejected.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, charge);
        Err(io::Error::other("abort outer transaction"))
    });
    assert!(result.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: io::Result<()> = store.transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"missing-a")?, None);
            panic!("abort transaction callback")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"missing-a")?, None);
            Ok(())
        })
        .unwrap();
}

#[test]
fn range_reservations_are_deduplicated_and_released_after_failed_capture() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"other", b"value").unwrap();
    let per_entry = std::mem::size_of::<crate::checkpoint_store::read_set::RangeEntry>();
    let charge = b"base".len() + b"other".len() + per_entry * 2;
    store.transaction_budget.lock().unwrap().limit = charge - 1;
    let failed = store.transaction_with_read_set(|tx| tx.scan(None, None, 10));
    assert!(failed.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    store.transaction_budget.lock().unwrap().limit = charge;
    let aborted: io::Result<()> = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.scan(None, None, 10)?.len(), 2);
        assert_eq!(store.transaction_budget.lock().unwrap().used, charge);
        assert_eq!(tx.scan(None, None, 1)?.len(), 1);
        assert_eq!(store.transaction_budget.lock().unwrap().used, charge);
        Err(io::Error::other("abort after scan"))
    });
    assert!(aborted.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    store.transaction_budget.lock().unwrap().limit = charge + 8192;
    let aborted: io::Result<()> = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.scan(None, None, 10)?.len(), 2);
        // Unused reservation quantum is returned before the page is exposed.
        assert_eq!(store.transaction_budget.lock().unwrap().used, charge);
        Err(io::Error::other("abort after scan"))
    });
    assert!(aborted.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    store.put(b"later", b"ok").unwrap();
}

#[test]
fn failed_range_capture_releases_partially_used_reservation_quanta() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    for index in 0..6_u8 {
        let mut key = vec![b'x'; 1024];
        key[0] = b'a' + index;
        store.put(&key, b"value").unwrap();
    }
    store.transaction_budget.lock().unwrap().limit = 4096 + 100;
    let result = store.transaction_with_read_set(|tx| tx.scan(None, None, 10));
    assert!(result.is_err());
    assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    store.put(b"later", b"ok").unwrap();
}

#[test]
fn checkpoint_snapshot_rejects_reads_after_an_uncertain_commit() {
    let (_dir, path, checkpoint, digest) = fixture();
    let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    store.log.replace_io_for_test(Box::new(FaultLog {
        file,
        fault: LogFault::SyncBefore,
    }));
    let snapshot = store.snapshot().unwrap();
    assert_eq!(
        snapshot.get(b"base").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert!(store.put(b"new", b"unknown").is_err());
    assert!(snapshot.get(b"base").is_err());
    assert!(snapshot.scan(None, None, 1).is_err());
    drop(snapshot);
    drop(store);
    let recovered = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        recovered.get(b"new").unwrap().as_deref(),
        Some(&b"unknown"[..])
    );
}
