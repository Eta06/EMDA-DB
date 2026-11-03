//! Writer handoff, queue admission, and failure wake-up tests.
use super::fault_tests::BlockingSyncIo;
use super::group_tests::{GroupFault, GroupIo};
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

fn many_waiters_case(fault: GroupFault) {
    const WRITERS: usize = 70; // More than the 64-record group limit.
    let dir = tempfile::tempdir().unwrap();
    let appends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let syncs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (group_started_tx, group_started_rx) = mpsc::channel();
    let (group_resume_tx, group_resume_rx) = mpsc::channel();
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
                appends,
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
    let (done_tx, done_rx) = mpsc::channel();
    let mut writers = Vec::new();
    for id in 0..WRITERS {
        let store = Arc::clone(&store);
        let done_tx = done_tx.clone();
        writers.push(
            thread::Builder::new()
                .stack_size(256 * 1024)
                .spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        store.put(format!("many-{id}").as_bytes(), b"value")
                    }));
                    done_tx.send(outcome).unwrap();
                })
                .unwrap(),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store.commits.queued_len() < WRITERS && std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    let queued = store.commits.queued_len();
    resume_tx.send(()).unwrap();
    assert_eq!(queued, WRITERS, "all writers must enter the queue");
    group_started_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    assert!(
        done_rx.try_recv().is_err(),
        "a writer completed before the group's sync"
    );
    group_resume_tx.send(()).unwrap();
    first.join().unwrap().unwrap();
    let mut success = 0;
    let mut error = 0;
    let mut panic = 0;
    for _ in 0..WRITERS {
        match done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("a writer was not woken")
        {
            Ok(Ok(())) => success += 1,
            Ok(Err(_)) => error += 1,
            Err(_) => panic += 1,
        }
    }
    for writer in writers {
        writer.join().unwrap();
    }
    match fault {
        GroupFault::None => {
            assert_eq!((success, error, panic), (WRITERS, 0, 0));
            assert_eq!(syncs.load(Ordering::SeqCst), 4);
            for id in 0..WRITERS {
                assert_eq!(
                    store
                        .get(format!("many-{id}").as_bytes())
                        .unwrap()
                        .as_deref(),
                    Some(&b"value"[..])
                );
            }
        }
        GroupFault::SyncError | GroupFault::SyncErrorAfter => {
            assert_eq!((success, error, panic), (0, WRITERS, 0));
            assert!(store.get(b"safe").is_err());
        }
        GroupFault::SyncPanic => {
            assert_eq!((success, error, panic), (0, WRITERS - 1, 1));
            assert!(store.get(b"safe").is_err());
        }
        GroupFault::PartialAppend(_) => unreachable!(),
    }
}

#[test]
fn writers_beyond_a_single_batch_all_complete() {
    many_waiters_case(GroupFault::None);
}

#[test]
fn group_sync_error_wakes_active_and_future_batches() {
    many_waiters_case(GroupFault::SyncError);
}

#[test]
fn group_sync_panic_wakes_active_and_future_batches() {
    many_waiters_case(GroupFault::SyncPanic);
}

#[test]
fn group_failure_wakes_capacity_waiters_and_queued_writers() {
    let dir = tempfile::tempdir().unwrap();
    let appends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let syncs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (group_started_tx, group_started_rx) = mpsc::channel();
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
            fault: GroupFault::SyncPanic,
        }))
    })
    .unwrap();
    store.commits = Coordinator::for_test(4096, 2);
    let store = Arc::new(store);
    store.put(b"safe", b"initial").unwrap();
    let first_store = Arc::clone(&store);
    let first = thread::spawn(move || first_store.put(b"first", b"committed"));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let mut writers = Vec::new();
    for id in 0..6 {
        let store = Arc::clone(&store);
        let done_tx = done_tx.clone();
        writers.push(thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store.put(format!("limited-{id}").as_bytes(), b"value")
            }));
            done_tx.send(result).unwrap();
        }));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store.commits.buffered().1 < 2 && std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    let buffered = store.commits.buffered().1;
    resume_tx.send(()).unwrap();
    assert_eq!(buffered, 2, "one write should be active and another queued");
    group_started_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store.commits.admission_waiters() == 0 && std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    let admission_waiters = store.commits.admission_waiters();
    group_resume_tx.send(()).unwrap();
    assert!(
        admission_waiters > 0,
        "at least one writer must be waiting for capacity"
    );
    first.join().unwrap().unwrap();
    let mut errors = 0;
    let mut panics = 0;
    for _ in 0..6 {
        match done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("capacity waiter was not woken")
        {
            Ok(Err(_)) => errors += 1,
            Err(_) => panics += 1,
            Ok(Ok(())) => panic!("unacknowledged write succeeded after failed sync"),
        }
    }
    assert_eq!((errors, panics), (5, 1));
    for writer in writers {
        writer.join().unwrap();
    }
    assert!(store.get(b"safe").is_err());
}

#[test]
fn poisoned_coordinator_wakes_queued_and_capacity_waiters() {
    let dir = tempfile::tempdir().unwrap();
    let appends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let syncs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (group_started_tx, _group_started_rx) = mpsc::channel();
    let (_group_resume_tx, group_resume_rx) = mpsc::channel();
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
    store.commits = Coordinator::for_test(4096, 2);
    let store = Arc::new(store);
    store.put(b"safe", b"initial").unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let first_store = Arc::clone(&store);
    let first_tx = done_tx.clone();
    let first = thread::spawn(move || {
        first_tx.send(first_store.put(b"first", b"value")).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let mut writers = Vec::new();
    for id in 0..4 {
        let store = Arc::clone(&store);
        let done_tx = done_tx.clone();
        writers.push(thread::spawn(move || {
            done_tx
                .send(store.put(format!("poison-{id}").as_bytes(), b"value"))
                .unwrap();
        }));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while (store.commits.queued_len() == 0 || store.commits.admission_waiters() == 0)
        && std::time::Instant::now() < deadline
    {
        thread::yield_now();
    }
    let queued = store.commits.queued_len();
    let capacity = store.commits.admission_waiters();
    let poisoning = Arc::clone(&store);
    assert!(
        thread::spawn(move || poisoning.commits.poison_for_test())
            .join()
            .is_err()
    );
    resume_tx.send(()).unwrap();
    assert!(
        queued > 0 && capacity > 0,
        "test requires queued and capacity waiters"
    );
    for _ in 0..5 {
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("writer remained blocked")
                .is_err()
        );
    }
    first.join().unwrap();
    for writer in writers {
        writer.join().unwrap();
    }
    assert!(store.get(b"safe").is_err());
}

#[test]
fn unavailable_head_does_not_strand_queued_writers() {
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
    store.put(b"safe", b"initial").unwrap();
    armed.store(true, Ordering::SeqCst);
    let first_store = Arc::clone(&store);
    let first = thread::spawn(move || first_store.put(b"first", b"value"));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    // Model an already-enqueued caller that is unavailable when the next
    // leader is selected. Its record has not been acknowledged.
    for id in 0..5 {
        store
            .commits
            .enqueue_unowned_for_test(format!("unowned-{id}").as_bytes(), b"value");
    }
    let writing = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let _ = done_tx.send(writing.put(b"live", b"value"));
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store.commits.queued_len() < 6 && std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    let queued = store.commits.queued_len();
    resume_tx.send(()).unwrap();
    assert_eq!(queued, 6);
    first.join().unwrap().unwrap();
    let initial = done_rx.recv_timeout(Duration::from_secs(3));
    let progressed = initial.is_ok();
    let result = match initial {
        Ok(result) => result,
        Err(_) => {
            // Release a stranded worker before asserting so a failure cannot
            // leave a thread alive in the test process.
            store.commits.wake_queued_for_test();
            done_rx.recv_timeout(Duration::from_secs(10)).unwrap()
        }
    };
    writer.join().unwrap();
    assert!(progressed, "unavailable queue heads blocked another writer");
    result.unwrap();
    assert_eq!(store.get(b"live").unwrap().as_deref(), Some(&b"value"[..]));
    for id in 0..5 {
        assert_eq!(
            store
                .get(format!("unowned-{id}").as_bytes())
                .unwrap()
                .as_deref(),
            Some(&b"value"[..])
        );
    }
}
