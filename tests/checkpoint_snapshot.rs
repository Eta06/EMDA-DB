use database_engine::{
    BatchOperation, CheckpointStore, MAX_CHECKPOINT_ACTIVE_SNAPSHOTS, MAX_CHECKPOINT_VIEW_BLOCKS,
    MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS, Store,
};
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf, CheckpointStore) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"a", b"old").unwrap();
    ordinary.put(b"c", b"base").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store.put(b"b", b"suffix").unwrap();
    store.delete(b"c").unwrap();
    (dir, path, checkpoint, store)
}

#[test]
fn snapshot_stays_stable_across_writes_batches_and_checkpoint_rollover() {
    let (dir, _path, _checkpoint, store) = fixture();
    let snapshot = store.snapshot().unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"new",
            },
            BatchOperation::Delete { key: b"b" },
            BatchOperation::Put {
                key: b"c",
                value: b"restored",
            },
        ])
        .unwrap();
    store
        .rollover_to(dir.path().join("new-checkpoint"))
        .unwrap();
    store.put(b"d", b"later").unwrap();
    assert_eq!(
        snapshot.get_many(&[b"a", b"b", b"c", b"a"]).unwrap(),
        vec![
            Some(b"old".to_vec()),
            Some(b"suffix".to_vec()),
            None,
            Some(b"old".to_vec()),
        ]
    );
    let first = snapshot.scan(None, None, 1).unwrap();
    let second = snapshot.scan(Some(&first[0].0), None, 1).unwrap();
    assert_eq!(first, vec![(b"a".to_vec(), b"old".to_vec())]);
    assert_eq!(second, vec![(b"b".to_vec(), b"suffix".to_vec())]);
    assert!(
        snapshot
            .scan(Some(&second[0].0), None, 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn multiple_snapshots_keep_distinct_historical_states() {
    let (dir, _path, _checkpoint, store) = fixture();
    let first = store.snapshot().unwrap();
    store.put(b"a", b"middle").unwrap();
    let second = store.snapshot().unwrap();
    store
        .rollover_to(dir.path().join("middle-checkpoint"))
        .unwrap();
    store.put(b"a", b"latest").unwrap();
    let third = store.snapshot().unwrap();
    store.delete(b"a").unwrap();
    assert_eq!(first.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(second.get(b"a").unwrap().as_deref(), Some(&b"middle"[..]));
    assert_eq!(third.get(b"a").unwrap().as_deref(), Some(&b"latest"[..]));
    assert_eq!(store.get(b"a").unwrap(), None);
}

#[test]
fn per_view_and_aggregate_limits_release_on_cancel_and_drop() {
    let (_dir, _path, _checkpoint, store) = fixture();
    assert!(
        store
            .snapshot_with_limits(
                0,
                MAX_RECOVERED_VIEW_TAIL_KEYS,
                MAX_RECOVERED_VIEW_TAIL_KEY_BYTES
            )
            .is_err()
    );
    assert!(
        store
            .snapshot_with_limits(
                MAX_CHECKPOINT_VIEW_BLOCKS,
                1,
                MAX_RECOVERED_VIEW_TAIL_KEY_BYTES
            )
            .is_err()
    );
    assert!(
        store
            .snapshot_with_limits(MAX_CHECKPOINT_VIEW_BLOCKS + 1, 0, 0)
            .is_err()
    );
    assert!(
        store
            .snapshot_with_limits(MAX_CHECKPOINT_VIEW_BLOCKS, MAX_RECOVERED_VIEW_TAIL_KEYS, 1)
            .is_err()
    );
    let mut snapshots = (0..MAX_CHECKPOINT_ACTIVE_SNAPSHOTS)
        .map(|_| store.snapshot().unwrap())
        .collect::<Vec<_>>();
    assert!(store.snapshot().is_err());
    snapshots[0].cancel();
    snapshots[0].cancel();
    assert!(snapshots[0].get(b"a").is_err());
    let extra = store.snapshot().unwrap();
    drop(extra);
    drop(snapshots);
    store.snapshot().unwrap();
}

#[test]
fn concurrent_snapshot_admission_cannot_exceed_the_active_limit() {
    let (_dir, _path, _checkpoint, store) = fixture();
    let start = std::sync::Barrier::new(33);
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let mut releases = Vec::new();
        for _ in 0..32 {
            let send = send.clone();
            let start = &start;
            let store = &store;
            let (release, wait) = std::sync::mpsc::channel();
            releases.push(release);
            scope.spawn(move || {
                start.wait();
                let snapshot = store.snapshot();
                send.send(snapshot.is_ok()).unwrap();
                let _ = wait.recv_timeout(Duration::from_secs(10));
                drop(snapshot);
            });
        }
        start.wait();
        let mut admitted = 0;
        let mut complete = true;
        for _ in 0..32 {
            match receive.recv_timeout(Duration::from_secs(10)) {
                Ok(true) => admitted += 1,
                Ok(false) => {}
                Err(_) => {
                    complete = false;
                    break;
                }
            }
        }
        for release in releases {
            let _ = release.send(());
        }
        assert!(complete, "snapshot workers did not finish admission");
        assert_eq!(admitted, MAX_CHECKPOINT_ACTIVE_SNAPSHOTS);
    });
    store.snapshot().unwrap();
}

#[test]
fn expired_views_reject_new_reads_until_cancelled() {
    let (_dir, _path, _checkpoint, store) = fixture();
    assert_eq!(
        store.snapshot_until(Instant::now()).err().unwrap().kind(),
        io::ErrorKind::TimedOut
    );
    let deadline = Instant::now() + Duration::from_millis(50);
    let mut snapshot = store.snapshot_until(deadline).unwrap();
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
    std::thread::sleep(
        deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
    );
    assert_eq!(
        snapshot.get(b"a").unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        snapshot.scan(None, None, 1).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    snapshot.cancel();
    assert!(snapshot.get(b"a").is_err());
}

#[test]
fn new_snapshot_reclaims_expired_idle_views() {
    let (_dir, _path, _checkpoint, store) = fixture();
    let deadline = Instant::now() + Duration::from_millis(500);
    let snapshots = (0..MAX_CHECKPOINT_ACTIVE_SNAPSHOTS)
        .map(|_| store.snapshot_until(deadline).unwrap())
        .collect::<Vec<_>>();
    assert!(store.snapshot().is_err());
    std::thread::sleep(
        deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
    );
    let current = store.snapshot().unwrap();
    assert_eq!(current.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
    for snapshot in &snapshots {
        assert_eq!(
            snapshot.get(b"a").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }
    assert_eq!(store.reap_expired_snapshots().unwrap(), 0);
}

#[test]
fn snapshot_detects_corrupted_checkpoint_blocks() {
    let (_dir, _path, checkpoint, store) = fixture();
    let snapshot = store.snapshot().unwrap();
    let mut bytes = std::fs::read(&checkpoint).unwrap();
    bytes[70] ^= 1;
    std::fs::write(&checkpoint, bytes).unwrap();
    assert!(snapshot.get(b"a").is_err());
}

#[test]
fn forgetting_a_snapshot_does_not_leak_the_store_lock() {
    let (_dir, path, _checkpoint, store) = fixture();
    let snapshot = store.snapshot().unwrap();
    std::mem::forget(snapshot);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
}

#[test]
fn concurrent_batch_writes_do_not_change_snapshot_reads() {
    let (_dir, _path, _checkpoint, store) = fixture();
    let snapshot = store.snapshot().unwrap();
    let start = std::sync::Barrier::new(3);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            for index in 0..100_u8 {
                store
                    .write_batch(&[
                        BatchOperation::Put {
                            key: b"a",
                            value: &[index],
                        },
                        BatchOperation::Put {
                            key: b"b",
                            value: &[index],
                        },
                    ])
                    .unwrap();
            }
        });
        scope.spawn(|| {
            start.wait();
            for _ in 0..100 {
                assert_eq!(
                    snapshot.get_many(&[b"a", b"b"]).unwrap(),
                    vec![Some(b"old".to_vec()), Some(b"suffix".to_vec()),]
                );
            }
        });
        start.wait();
    });
}

#[test]
fn snapshots_reject_oversized_bulk_responses_before_reading_values() {
    use database_engine::MAX_SCAN_ENTRIES;
    let (_dir, _path, _checkpoint, store) = fixture();
    store.put(b"large", &vec![7; 16 * 1024 * 1024]).unwrap();
    let snapshot = store.snapshot().unwrap();
    let too_many = vec![b"a".as_slice(); MAX_SCAN_ENTRIES + 1];
    assert_eq!(
        snapshot.get_many(&too_many).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        snapshot.get_many(&[b"large", b"large"]).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
}
