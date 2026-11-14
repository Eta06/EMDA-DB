//! Distinguish queued writes from capacity waiters after a group sync fault.
use super::{
    group_tests::{GroupFault, GroupIo},
    *,
};
use crate::{MutationFailure, test_support::retry_lock};
use std::fs;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn group_failure_rejects_unsubmitted_waiters_but_keeps_queued_outcomes_unknown() {
    let root = tempfile::tempdir().unwrap();
    fs::write(
        root.path().join("records.log"),
        include_bytes!("../../tests/fixtures/v2-empty-records.log"),
    )
    .unwrap();
    fs::write(
        root.path().join("commit.state"),
        include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
    )
    .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (group_started_tx, group_started_rx) = mpsc::channel();
    let (group_resume_tx, group_resume_rx) = mpsc::channel();
    let mut store = Store::open_using(root.path(), |path, initialize| {
        let (mut log, index) = Log::open(path, initialize)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))?;
        log.replace_io_for_test(Box::new(GroupIo {
            file,
            appends: Arc::new(AtomicUsize::new(0)),
            syncs: Arc::new(AtomicUsize::new(0)),
            started: started_tx,
            resume: Mutex::new(resume_rx),
            group_started: group_started_tx,
            group_resume: Mutex::new(group_resume_rx),
            fault: GroupFault::SyncError,
        }));
        Ok((log, index))
    })
    .unwrap();
    store.commits = Coordinator::for_test(4096, 2);
    store.put(b"safe", b"yes").unwrap();
    let store = Arc::new(store);
    let first_store = Arc::clone(&store);
    let first = thread::spawn(move || first_store.put_detailed(b"first", b"yes"));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let queued_store = Arc::clone(&store);
    let queued = thread::spawn(move || queued_store.put_detailed(b"queued", b"yes"));
    let deadline = Instant::now() + Duration::from_secs(10);
    while store.commits.queued_len() != 1 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(store.commits.queued_len(), 1);
    let mut waiters = Vec::new();
    for id in 0..4 {
        let store = Arc::clone(&store);
        waiters.push(thread::spawn(move || {
            let key = format!("waiter-{id}");
            let result = store.put_detailed(key.as_bytes(), b"yes");
            (key, result)
        }));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while store.commits.admission_waiters() < 4 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(store.commits.admission_waiters(), 4);
    resume_tx.send(()).unwrap();
    group_started_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while (store.commits.queued_or_active_len() < 2 || store.commits.admission_waiters() < 1)
        && Instant::now() < deadline
    {
        thread::yield_now();
    }
    assert_eq!(store.commits.queued_or_active_len(), 2);
    assert!(store.commits.admission_waiters() >= 1);
    group_resume_tx.send(()).unwrap();
    assert!(first.join().unwrap().is_ok());
    assert!(matches!(
        queued.join().unwrap(),
        Err(MutationFailure::OutcomeUnknown(_))
    ));
    let mut rejected = Vec::new();
    let mut unknown = 0;
    for worker in waiters {
        let (key, result) = worker.join().unwrap();
        match result {
            Err(MutationFailure::Rejected(_)) => rejected.push(key),
            Err(MutationFailure::OutcomeUnknown(_)) => unknown += 1,
            other => panic!("unexpected waiter outcome: {other:?}"),
        }
    }
    assert!(!rejected.is_empty());
    assert!(unknown >= 1);
    assert!(store.get(b"safe").is_err());
    drop(store);
    let reopened = retry_lock(|| Store::open(root.path())).unwrap();
    assert_eq!(reopened.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(
        reopened.get(b"first").unwrap().as_deref(),
        Some(&b"yes"[..])
    );
    for key in rejected {
        assert_eq!(reopened.get(key.as_bytes()).unwrap(), None);
    }
}
