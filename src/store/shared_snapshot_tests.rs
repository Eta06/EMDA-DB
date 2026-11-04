//! User-visible snapshot behavior and key-byte accounting across mutations.
use super::*;
use crate::BatchOperation;
use crate::store::commit::SnapshotCopyPoint;
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

fn assert_model(store: &Store, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let bytes = model.keys().map(Vec::len).sum::<usize>();
    let state = store.read().unwrap();
    assert_eq!(state.index.len(), model.len());
    assert_eq!(state.index_key_bytes, bytes);
    drop(state);
    if store.max_live_key_bytes.is_some() {
        assert_eq!(store.live_key_bytes.load(Ordering::Relaxed), bytes);
    }
    let snapshot = store.snapshot(model.len(), bytes).unwrap();
    let entries = snapshot.scan(None, None, 1024).unwrap();
    assert_eq!(
        entries,
        model
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    );
    if bytes > 0 {
        assert!(store.snapshot(model.len(), bytes - 1).is_err());
    }
    if !model.is_empty() {
        assert!(store.snapshot(model.len() - 1, bytes).is_err());
    }
}

fn exercise_snapshot_model(limited: bool) {
    let root = tempfile::tempdir().unwrap();
    let open = || {
        Store::open_with_resource_limits(
            root.path(),
            None,
            limited.then_some(200),
            limited.then_some(4096),
        )
        .unwrap()
    };
    let store = open();
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for step in 0..240_usize {
        let key = format!("key-{:02}-{}", step % 17, "x".repeat(step % 8)).into_bytes();
        let value = (step as u64).to_le_bytes().to_vec();
        match step % 4 {
            0 | 1 => {
                store.put(&key, &value).unwrap();
                model.insert(key, value);
            }
            2 => {
                store.delete(&key).unwrap();
                model.remove(&key);
            }
            _ => {
                let other = format!("other-{}", step % 5).into_bytes();
                store
                    .write_batch(&[
                        BatchOperation::Put {
                            key: &key,
                            value: &value,
                        },
                        BatchOperation::Delete { key: &other },
                        BatchOperation::Put {
                            key: &other,
                            value: b"new",
                        },
                        BatchOperation::Delete { key: &key },
                    ])
                    .unwrap();
                model.insert(other, b"new".to_vec());
                model.remove(&key);
            }
        }
        assert_model(&store, &model);
    }
    drop(store);
    let reopened = open();
    assert_model(&reopened, &model);
}

#[test]
fn snapshots_and_key_byte_limits_match_a_mutation_model_across_reopen() {
    exercise_snapshot_model(false);
    exercise_snapshot_model(true);
}

#[test]
fn old_and_new_snapshots_remain_stable_during_concurrent_writes() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    store.put(b"key", b"before").unwrap();
    let old = store.snapshot(10, 100).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let writer_store = Arc::clone(&store);
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        for number in 0..200 {
            writer_store
                .put(b"key", number.to_string().as_bytes())
                .unwrap();
            writer_store
                .put(format!("k-{number}").as_bytes(), b"value")
                .unwrap();
        }
    });
    barrier.wait();
    for _ in 0..200 {
        assert_eq!(old.get(b"key").unwrap(), Some(b"before".to_vec()));
        thread::yield_now();
    }
    writer.join().unwrap();
    let current = store.snapshot(1000, 10000).unwrap();
    assert_eq!(current.get(b"key").unwrap(), Some(b"199".to_vec()));
    assert_eq!(old.get(b"key").unwrap(), Some(b"before".to_vec()));
    assert_eq!(old.get(b"k-0").unwrap(), None);
    store.delete(b"key").unwrap();
    assert_eq!(current.get(b"key").unwrap(), Some(b"199".to_vec()));
}

#[test]
fn accounting_failure_after_sync_poison_reads_before_partial_index_is_visible() {
    for overflow in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        store.put(b"base", b"old").unwrap();
        store.inner.write().unwrap().index_key_bytes = if overflow { usize::MAX } else { 0 };
        let operations = if overflow {
            vec![
                BatchOperation::Put {
                    key: b"base",
                    value: b"new",
                },
                BatchOperation::Put {
                    key: b"added",
                    value: b"value",
                },
            ]
        } else {
            vec![
                BatchOperation::Put {
                    key: b"base",
                    value: b"new",
                },
                BatchOperation::Delete { key: b"base" },
            ]
        };
        assert!(store.write_batch(&operations).is_err());
        assert!(store.get(b"base").is_err());
        assert!(store.snapshot(10, 100).is_err());
        drop(store);
        let reopened = Store::open(root.path()).unwrap();
        if overflow {
            assert_eq!(reopened.get(b"base").unwrap(), Some(b"new".to_vec()));
            assert_eq!(reopened.get(b"added").unwrap(), Some(b"value".to_vec()));
        } else {
            assert_eq!(reopened.get(b"base").unwrap(), None);
        }
        let state = reopened.read().unwrap();
        assert_eq!(
            state.index_key_bytes,
            state.index.keys().map(Vec::len).sum::<usize>()
        );
    }
}

#[test]
fn copying_allows_readers_and_keeps_new_snapshots_on_the_old_root() {
    use std::sync::mpsc;
    use std::time::Duration;
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    store.put(b"key", b"old").unwrap();
    let old = store.snapshot(10, 100).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume = Mutex::new(resume_rx);
    store
        .commits
        .set_snapshot_copy_hook(Some(Arc::new(move |point| {
            if point == SnapshotCopyPoint::BeforeCopy {
                ready_tx.send(()).unwrap();
                resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
        })));
    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || writer_store.put(b"key", b"new"));
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    store.commits.set_snapshot_copy_hook(None);
    let (read_tx, read_rx) = mpsc::channel();
    let reader_store = Arc::clone(&store);
    let reader = thread::spawn(move || read_tx.send(reader_store.get(b"key")).unwrap());
    assert_eq!(
        read_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap(),
        Some(b"old".to_vec())
    );
    let during = store.snapshot(10, 100).unwrap();
    assert_eq!(during.get(b"key").unwrap(), Some(b"old".to_vec()));
    resume_tx.send(()).unwrap();
    writer.join().unwrap().unwrap();
    reader.join().unwrap();
    assert_eq!(old.get(b"key").unwrap(), Some(b"old".to_vec()));
    assert_eq!(during.get(b"key").unwrap(), Some(b"old".to_vec()));
    assert_eq!(store.get(b"key").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn unexpected_index_change_during_preparation_fails_closed_and_recovers() {
    use std::sync::mpsc;
    use std::time::Duration;
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    store.put(b"key", b"old").unwrap();
    let snapshot = store.snapshot(10, 100).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume = Mutex::new(resume_rx);
    store
        .commits
        .set_snapshot_copy_hook(Some(Arc::new(move |point| {
            if point == SnapshotCopyPoint::AfterCopy {
                ready_tx.send(()).unwrap();
                resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
        })));
    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || writer_store.put(b"key", b"new"));
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    store.inner.write().unwrap().generation += 1;
    resume_tx.send(()).unwrap();
    assert!(writer.join().unwrap().is_err());
    assert!(store.get(b"key").is_err());
    assert!(snapshot.get(b"key").is_err());
    drop(snapshot);
    drop(store);
    let reopened = crate::test_support::retry_lock(|| Store::open(root.path())).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn panic_after_snapshot_copy_poison_store_and_does_not_acknowledge() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.put(b"key", b"old").unwrap();
    let snapshot = store.snapshot(10, 100).unwrap();
    store.commits.set_snapshot_copy_hook(Some(Arc::new(|point| {
        if point == SnapshotCopyPoint::AfterCopy {
            panic!("injected copy panic");
        }
    })));
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.put(b"key", b"new")));
    assert!(!matches!(result, Ok(Ok(()))));
    assert!(store.get(b"key").is_err());
    assert!(snapshot.get(b"key").is_err());
    drop(snapshot);
    drop(store);
    let reopened = crate::test_support::retry_lock(|| Store::open(root.path())).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"new".to_vec()));
}
