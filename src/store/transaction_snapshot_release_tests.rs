//! Validated transactions release only their own snapshot before commit.
use super::{commit::SnapshotCopyPoint, fault_tests::BlockingSyncIo, *};
use crate::test_support::retry_lock;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

#[test]
fn validated_transaction_avoids_own_index_copy_but_preserves_external_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.put(b"key", b"old").unwrap();
    let copies = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&copies);
    store
        .commits
        .set_snapshot_copy_hook(Some(Arc::new(move |point| {
            if point == SnapshotCopyPoint::BeforeCopy {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        })));
    store.transaction(|tx| tx.put(b"key", b"middle")).unwrap();
    assert_eq!(copies.load(Ordering::SeqCst), 0);
    let external = store.snapshot(10, 100).unwrap();
    store.transaction(|tx| tx.put(b"key", b"new")).unwrap();
    assert_eq!(copies.load(Ordering::SeqCst), 1);
    assert_eq!(
        external.get(b"key").unwrap().as_deref(),
        Some(&b"middle"[..])
    );
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn snapshot_created_during_sync_remains_stable_and_transaction_stays_admitted() {
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
    let armed = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let store = Store::open_using(root.path(), |path, initialize| {
        let (mut log, index) = Log::open(path, initialize)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))?;
        log.replace_io_for_test(Box::new(BlockingSyncIo {
            file,
            armed: Arc::clone(&armed),
            started: started_tx,
            resume: Mutex::new(resume_rx),
        }));
        Ok((log, index))
    })
    .unwrap();
    store.put(b"key", b"old").unwrap();
    let store = Arc::new(store);
    armed.store(true, Ordering::SeqCst);
    let writing = Arc::clone(&store);
    let writer = thread::spawn(move || writing.transaction(|tx| tx.put(b"key", b"new")));
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(store.snapshot_budget_status().unwrap().active, 0);
    assert_eq!(
        store
            .transaction_budget_status()
            .unwrap()
            .active_transactions,
        1
    );
    let snapshot = store.snapshot(10, 100).unwrap();
    assert_eq!(snapshot.get(b"key").unwrap().as_deref(), Some(&b"old"[..]));
    resume_tx.send(()).unwrap();
    writer.join().unwrap().unwrap();
    assert_eq!(snapshot.get(b"key").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
    drop(snapshot);
    assert_eq!(store.snapshot_budget_status().unwrap().active, 0);
    assert_eq!(
        store
            .transaction_budget_status()
            .unwrap()
            .active_transactions,
        0
    );
    drop(store);
    let reopened = retry_lock(|| Store::open(root.path())).unwrap();
    assert_eq!(reopened.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
}
