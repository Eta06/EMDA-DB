//! Per-open snapshot budgets account for reservations and preserve handoffs.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, MAX_ACTIVE_SNAPSHOTS,
    MAX_CHECKPOINT_ACTIVE_SNAPSHOTS, MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS,
    MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES, MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
    MAX_SNAPSHOT_TOTAL_KEY_BYTES, MAX_SNAPSHOT_TOTAL_KEYS, Store,
};
#[cfg(target_os = "linux")]
use database_engine::{OnlineCompactionOptions, OnlineStore};
use serde_json::json;
use std::{
    io::ErrorKind,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

mod support;
use support::retry_lock;

#[test]
fn ordinary_snapshot_limits_follow_live_reservations_and_block_shared_users() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"bb", b"two").unwrap();
    let initial = store.snapshot_budget_status().unwrap();
    assert_eq!(
        (
            initial.active_limit,
            initial.key_limit,
            initial.key_byte_limit
        ),
        (
            MAX_ACTIVE_SNAPSHOTS,
            MAX_SNAPSHOT_TOTAL_KEYS,
            MAX_SNAPSHOT_TOTAL_KEY_BYTES
        )
    );
    store.set_snapshot_budget_limits(1, 2, 3).unwrap();
    let snapshot = store.snapshot(2, 3).unwrap();
    let held = store.snapshot_budget_status().unwrap();
    assert_eq!((held.active, held.keys, held.key_bytes), (1, 2, 3));
    for (active, keys, bytes) in [(0, 2, 3), (1, 1, 3), (1, 2, 2)] {
        assert_eq!(
            store
                .set_snapshot_budget_limits(active, keys, bytes)
                .unwrap_err()
                .kind(),
            ErrorKind::WouldBlock
        );
        assert_eq!(store.snapshot_budget_status().unwrap(), held);
    }
    for (active, keys, bytes) in [
        (MAX_ACTIVE_SNAPSHOTS + 1, 2, 3),
        (1, MAX_SNAPSHOT_TOTAL_KEYS + 1, 3),
        (1, 2, MAX_SNAPSHOT_TOTAL_KEY_BYTES + 1),
    ] {
        assert_eq!(
            store
                .set_snapshot_budget_limits(active, keys, bytes)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(store.snapshot_budget_status().unwrap(), held);
    }
    assert!(store.snapshot(2, 3).is_err());
    let called = AtomicBool::new(false);
    assert!(
        store
            .transaction(|_| {
                called.store(true, Ordering::SeqCst);
                Ok(())
            })
            .is_err()
    );
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    drop(snapshot);
    assert_eq!(store.snapshot_budget_status().unwrap().active, 0);
    store.set_snapshot_budget_limits(0, 0, 0).unwrap();
    assert!(store.snapshot(2, 3).is_err());
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    store.put(b"a", b"updated").unwrap();
    store.set_snapshot_budget_limits(1, 2, 3).unwrap();
    assert_eq!(
        store.snapshot(2, 3).unwrap().get(b"a").unwrap().as_deref(),
        Some(&b"updated"[..])
    );
    drop(store);
    let reopened = retry_lock(|| Store::open(root.path())).unwrap();
    let reset = reopened.snapshot_budget_status().unwrap();
    assert_eq!(
        (reset.active_limit, reset.key_limit, reset.key_byte_limit),
        (
            MAX_ACTIVE_SNAPSHOTS,
            MAX_SNAPSHOT_TOTAL_KEYS,
            MAX_SNAPSHOT_TOTAL_KEY_BYTES
        )
    );
}

#[test]
fn checkpoint_snapshot_limits_follow_all_live_reservations() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"one").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"bb", b"two").unwrap();
    let snapshot = store.snapshot().unwrap();
    let used = store.snapshot_budget_status().unwrap();
    assert_eq!((used.active, used.keys, used.key_bytes), (1, 1, 2));
    assert!(used.blocks > 0);
    store
        .set_snapshot_budget_limits(used.active, used.blocks, used.keys, used.key_bytes)
        .unwrap();
    let held = store.snapshot_budget_status().unwrap();
    for (active, blocks, keys, bytes) in [
        (0, used.blocks, 1, 2),
        (1, used.blocks - 1, 1, 2),
        (1, used.blocks, 0, 2),
        (1, used.blocks, 1, 1),
    ] {
        assert_eq!(
            store
                .set_snapshot_budget_limits(active, blocks, keys, bytes)
                .unwrap_err()
                .kind(),
            ErrorKind::WouldBlock
        );
        assert_eq!(store.snapshot_budget_status().unwrap(), held);
    }
    for (active, blocks, keys, bytes) in [
        (MAX_CHECKPOINT_ACTIVE_SNAPSHOTS + 1, used.blocks, 1, 2),
        (1, MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS + 1, 1, 2),
        (
            1,
            used.blocks,
            MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS + 1,
            2,
        ),
        (
            1,
            used.blocks,
            1,
            MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES + 1,
        ),
    ] {
        assert_eq!(
            store
                .set_snapshot_budget_limits(active, blocks, keys, bytes)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(store.snapshot_budget_status().unwrap(), held);
    }
    assert!(store.snapshot().is_err());
    assert_eq!(snapshot.get(b"bb").unwrap().as_deref(), Some(&b"two"[..]));
    drop(snapshot);
    store.set_snapshot_budget_limits(0, 0, 0, 0).unwrap();
    assert!(store.snapshot().is_err());
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(
        store.transaction(|tx| tx.get(b"bb")).unwrap().as_deref(),
        Some(&b"two"[..])
    );
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let reset = reopened.snapshot_budget_status().unwrap();
    assert_eq!(
        (
            reset.active_limit,
            reset.block_limit,
            reset.key_limit,
            reset.key_byte_limit
        ),
        (
            MAX_CHECKPOINT_ACTIVE_SNAPSHOTS,
            MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS,
            MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
            MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES
        )
    );
}

#[test]
fn expired_reservations_remain_counted_until_reaped() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let source = Store::open(&path).unwrap();
        source.put(b"key", b"value").unwrap();
        let digest = source.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(source);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let snapshot = store
                .snapshot_until(Instant::now() + Duration::from_millis(250))
                .unwrap();
            std::thread::sleep(Duration::from_millis(300));
            assert_eq!(store.snapshot_budget_status().unwrap().active, 1);
            let status = store.snapshot_budget_status().unwrap();
            assert_eq!(
                store
                    .set_snapshot_budget_limits(
                        0,
                        status.block_limit,
                        status.key_limit,
                        status.key_byte_limit
                    )
                    .unwrap_err()
                    .kind(),
                ErrorKind::WouldBlock
            );
            assert_eq!(store.reap_expired_snapshots().unwrap(), 1);
            assert_eq!(store.snapshot_budget_status().unwrap().active, 0);
            store.set_snapshot_budget_limits(0, 0, 0, 0).unwrap();
            assert!(snapshot.get(b"key").is_err());
        } else {
            let snapshot = source
                .snapshot_until(16, 64, Instant::now() + Duration::from_millis(250))
                .unwrap();
            std::thread::sleep(Duration::from_millis(300));
            assert_eq!(source.snapshot_budget_status().unwrap().active, 1);
            let status = source.snapshot_budget_status().unwrap();
            assert_eq!(
                source
                    .set_snapshot_budget_limits(0, status.key_limit, status.key_byte_limit)
                    .unwrap_err()
                    .kind(),
                ErrorKind::WouldBlock
            );
            assert_eq!(source.reap_expired_snapshots().unwrap(), 1);
            assert_eq!(source.snapshot_budget_status().unwrap().active, 0);
            source.set_snapshot_budget_limits(0, 0, 0).unwrap();
            assert!(snapshot.get(b"key").is_err());
        }
    }
}

#[test]
fn ordinary_document_point_reads_share_snapshot_admission() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "id", &json!({"n":1})).unwrap();
    store.set_snapshot_budget_limits(0, 0, 0).unwrap();
    assert!(db.get("c", "id").is_err());
    store
        .set_snapshot_budget_limits(1, MAX_SNAPSHOT_TOTAL_KEYS, MAX_SNAPSHOT_TOTAL_KEY_BYTES)
        .unwrap();
    assert_eq!(db.get("c", "id").unwrap(), Some(json!({"n":1})));
}

#[cfg(target_os = "linux")]
#[test]
fn blocking_handoffs_preserve_configured_limits_but_reopen_resets_them() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let previous = root.path().join("previous");
        let checkpoint = root.path().join("checkpoint");
        let source = Store::open(&path).unwrap();
        source.put(b"key", b"value").unwrap();
        if checkpoint_mode {
            let digest = source.checkpoint_to(&checkpoint).unwrap();
            drop(source);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            store.set_snapshot_budget_limits(0, 0, 0, 0).unwrap();
            let (store, digest) = store.compact_and_replace(&previous).unwrap();
            assert_eq!(store.snapshot_budget_status().unwrap().active_limit, 0);
            assert!(store.snapshot().is_err());
            drop(store);
            let reopened = retry_lock(|| {
                CheckpointStore::open(
                    &path,
                    path.join(database_engine::COMPACTED_CHECKPOINT_FILE),
                    &digest,
                )
            })
            .unwrap();
            assert_eq!(
                reopened.snapshot_budget_status().unwrap().active_limit,
                MAX_CHECKPOINT_ACTIVE_SNAPSHOTS
            );
            assert_eq!(
                reopened.get(b"key").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        } else {
            source.set_snapshot_budget_limits(0, 0, 0).unwrap();
            let store = source.compact_and_replace(&path, &previous).unwrap();
            assert_eq!(store.snapshot_budget_status().unwrap().active_limit, 0);
            assert!(store.snapshot(8, 32).is_err());
            drop(store);
            let reopened = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                reopened.snapshot_budget_status().unwrap().active_limit,
                MAX_ACTIVE_SNAPSHOTS
            );
            assert_eq!(
                reopened.get(b"key").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn online_compaction_rejects_a_zero_snapshot_budget_without_replacing_the_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let previous = root.path().join("previous");
    let store = OnlineStore::open(&path).unwrap();
    store
        .with_store(|db| {
            db.put(b"key", b"value")?;
            db.set_snapshot_budget_limits(0, 0, 0)
        })
        .unwrap();
    assert!(
        store
            .compact_and_replace(&previous, &OnlineCompactionOptions::default())
            .is_err()
    );
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|db| db.get(b"key")).unwrap().as_deref(),
        Some(&b"value"[..])
    );
    store
        .with_store(|db| {
            db.set_snapshot_budget_limits(1, MAX_SNAPSHOT_TOTAL_KEYS, MAX_SNAPSHOT_TOTAL_KEY_BYTES)
        })
        .unwrap();
    store
        .compact_and_replace(&previous, &OnlineCompactionOptions::default())
        .unwrap();
    assert_eq!(
        store
            .with_store(|db| Ok(db.snapshot_budget_status()?.active_limit))
            .unwrap(),
        1
    );
    assert_eq!(
        store.with_store(|db| db.get(b"key")).unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

fn exercise_concurrent_budget(
    read: impl Fn() + Sync,
    set: impl Fn(usize) -> std::io::Result<()> + Sync,
    valid: impl Fn() -> bool + Sync,
) {
    let barrier = std::sync::Barrier::new(6);
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..4 {
            let (read, valid, barrier) = (&read, &valid, &barrier);
            workers.push(scope.spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    read();
                    assert!(valid());
                    std::thread::yield_now();
                }
            }));
        }
        let (set, valid, barrier) = (&set, &valid, &barrier);
        let setter = scope.spawn(move || {
            barrier.wait();
            for i in 0..400 {
                if let Err(error) = set([0, 1, 2, 4][i % 4]) {
                    assert_eq!(error.kind(), ErrorKind::WouldBlock);
                }
                assert!(valid());
                std::thread::yield_now();
            }
        });
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        setter.join().unwrap();
    });
}

#[test]
fn concurrent_snapshot_admission_and_reconfiguration_never_oversubscribe() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let source = Store::open(&path).unwrap();
        source.put(b"key", b"value").unwrap();
        let digest = source.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(source);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let read = || match store.snapshot() {
                Ok(snapshot) => {
                    assert_eq!(
                        snapshot.get(b"key").unwrap().as_deref(),
                        Some(&b"value"[..])
                    );
                    std::thread::yield_now();
                }
                Err(error) => {
                    assert_eq!(error.kind(), ErrorKind::Other);
                    assert!(
                        error
                            .to_string()
                            .contains("aggregate checkpoint snapshot limit exceeded")
                    );
                }
            };
            let set = |n| {
                store.set_snapshot_budget_limits(
                    n,
                    MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS,
                    MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
                    MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES,
                )
            };
            let valid = || {
                let s = store.snapshot_budget_status().unwrap();
                s.active <= s.active_limit
                    && s.blocks <= s.block_limit
                    && s.keys <= s.key_limit
                    && s.key_bytes <= s.key_byte_limit
            };
            exercise_concurrent_budget(read, set, valid);
            assert_eq!(store.snapshot_budget_status().unwrap().active, 0);
        } else {
            let read = || match source.snapshot(16, 128) {
                Ok(snapshot) => {
                    assert_eq!(
                        snapshot.get(b"key").unwrap().as_deref(),
                        Some(&b"value"[..])
                    );
                    std::thread::yield_now();
                }
                Err(error) => {
                    assert_eq!(error.kind(), ErrorKind::Other);
                    assert!(
                        error
                            .to_string()
                            .contains("aggregate snapshot limit exceeded")
                    );
                }
            };
            let set = |n| {
                source.set_snapshot_budget_limits(
                    n,
                    MAX_SNAPSHOT_TOTAL_KEYS,
                    MAX_SNAPSHOT_TOTAL_KEY_BYTES,
                )
            };
            let valid = || {
                let s = source.snapshot_budget_status().unwrap();
                s.active <= s.active_limit
                    && s.keys <= s.key_limit
                    && s.key_bytes <= s.key_byte_limit
            };
            exercise_concurrent_budget(read, set, valid);
            assert_eq!(source.snapshot_budget_status().unwrap().active, 0);
            drop(source);
        }
        let recovered = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(
            recovered.get(b"key").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}
