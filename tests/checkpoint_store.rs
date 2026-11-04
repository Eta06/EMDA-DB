use database_engine::{
    BatchOperation, CheckpointStore, CheckpointView, RecoveredCheckpointView, Store,
};
use std::fs;
use std::io;
use std::sync::{Arc, Barrier};

fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"base-a").unwrap();
    store.put(b"c", b"base-c").unwrap();
    store.put(b"z", b"base-z").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    (dir, path, checkpoint, digest)
}

#[test]
fn writes_batches_and_recovery_share_one_ordered_state() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = Store::open(&path).unwrap();
    store.put(b"b", b"suffix-b").unwrap();
    store.delete(b"c").unwrap();
    drop(store);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    assert_eq!(store.overlay_keys().unwrap(), 2);
    assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"suffix-b"[..]));
    assert_eq!(store.get(b"c").unwrap(), None);
    store.put(b"a", b"updated-a").unwrap();
    store.delete(b"b").unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"d",
                value: b"first",
            },
            BatchOperation::Delete { key: b"z" },
            BatchOperation::Put {
                key: b"d",
                value: b"last",
            },
            BatchOperation::Put {
                key: b"e",
                value: b"value-e",
            },
        ])
        .unwrap();
    let expected = vec![
        (b"a".to_vec(), b"updated-a".to_vec()),
        (b"d".to_vec(), b"last".to_vec()),
        (b"e".to_vec(), b"value-e".to_vec()),
    ];
    assert_eq!(store.scan(None, None, 10).unwrap(), expected);
    assert_eq!(
        store.scan(Some(b"a"), Some(b"e"), 10).unwrap(),
        expected[1..2]
    );
    assert_eq!(store.overlay_keys().unwrap(), 6);
    let tail = store.tail_bytes().unwrap();
    assert!(tail > 0);
    drop(store);
    let reopened = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    assert_eq!(reopened.tail_bytes().unwrap(), tail);
    assert_eq!(reopened.scan(None, None, 10).unwrap(), expected);
    drop(reopened);
    let full_replay = Store::open(&path).unwrap();
    assert_eq!(full_replay.scan(None, None, 10).unwrap(), expected);
}

#[test]
fn all_overlay_limits_reject_before_append_and_count_tombstones() {
    let (_dir, path, checkpoint, digest) = fixture();
    let log = path.join("records.log");
    let store = CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 1000, 2, 3).unwrap();
    store.delete(b"a").unwrap();
    store.put(b"bb", b"x").unwrap();
    let length = fs::metadata(&log).unwrap().len();
    assert!(store.put(b"c", b"x").is_err());
    assert!(
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"bb",
                    value: b"new"
                },
                BatchOperation::Put {
                    key: b"cc",
                    value: b"new"
                },
            ])
            .is_err()
    );
    assert_eq!(fs::metadata(&log).unwrap().len(), length);
    assert_eq!(store.get(b"bb").unwrap().as_deref(), Some(&b"x"[..]));
    store.put(b"bb", b"updated").unwrap();
    assert_eq!(store.overlay_keys().unwrap(), 2);
    drop(store);
    let recovered =
        CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 1000, 2, 3).unwrap();
    assert_eq!(recovered.get(b"a").unwrap(), None);
    assert_eq!(
        recovered.get(b"bb").unwrap().as_deref(),
        Some(&b"updated"[..])
    );
    drop(recovered);

    let current_tail = fs::metadata(&log).unwrap().len();
    let store = CheckpointStore::open_with_limits(&path, &checkpoint, &digest, current_tail, 10, 4)
        .unwrap();
    assert!(store.put(b"cc", b"x").is_err());
    assert_eq!(fs::metadata(&log).unwrap().len(), current_tail);
    drop(store);

    let view = RecoveredCheckpointView::open(&path, &checkpoint, &digest).unwrap();
    let tail = view.tail_bytes();
    drop(view);
    let store =
        CheckpointStore::open_with_limits(&path, &checkpoint, &digest, tail, 10, 100).unwrap();
    assert!(store.put(b"a", b"cannot append").is_err());
    assert_eq!(fs::metadata(&log).unwrap().len(), current_tail);
}

#[test]
fn writable_checkpoint_excludes_all_cooperating_stores_and_views() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    assert_eq!(
        Store::open(&path).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        CheckpointView::open(&path, &checkpoint, &digest)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        RecoveredCheckpointView::open(&path, &checkpoint, &digest)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        CheckpointStore::open(&path, &checkpoint, &digest)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
    assert_eq!(
        CheckpointStore::open(&path, &checkpoint, &digest)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(view);
}

#[test]
fn concurrent_readers_never_observe_partial_batches() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = Arc::new(CheckpointStore::open(&path, &checkpoint, &digest).unwrap());
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"x1",
                value: &[0],
            },
            BatchOperation::Put {
                key: b"x2",
                value: &[0],
            },
        ])
        .unwrap();
    let start = Arc::new(Barrier::new(3));
    std::thread::scope(|scope| {
        let writer = Arc::clone(&store);
        let start_writer = Arc::clone(&start);
        scope.spawn(move || {
            start_writer.wait();
            for n in 1..=100_u8 {
                writer
                    .write_batch(&[
                        BatchOperation::Put {
                            key: b"x1",
                            value: &[n],
                        },
                        BatchOperation::Put {
                            key: b"x2",
                            value: &[n],
                        },
                    ])
                    .unwrap();
            }
        });
        let reader = Arc::clone(&store);
        let start_reader = Arc::clone(&start);
        scope.spawn(move || {
            start_reader.wait();
            for _ in 0..500 {
                let page = reader.scan(Some(b"x"), Some(b"y"), 10).unwrap();
                assert_eq!(page.len(), 2);
                assert_eq!(page[0].1, page[1].1);
            }
        });
        start.wait();
    });
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.get(b"x1").unwrap(), Some(vec![100]));
    assert_eq!(reopened.get(b"x2").unwrap(), Some(vec![100]));
}

#[test]
fn conditional_updates_and_concurrent_increments_are_atomic() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = Arc::new(CheckpointStore::open(&path, &checkpoint, &digest).unwrap());
    assert!(
        !store
            .compare_exchange(b"a", Some(b"wrong"), Some(b"new"))
            .unwrap()
    );
    assert!(store.compare_exchange(b"a", Some(b"base-a"), None).unwrap());
    assert!(
        store
            .compare_exchange(b"a", None, Some(b"restored!"))
            .unwrap()
    );
    assert!(store.increment_i64(b"a", 1).is_err());
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for _ in 0..50 {
                    store.increment_i64(b"counter", 1).unwrap();
                }
            });
        }
    });
    assert_eq!(
        store.get(b"counter").unwrap().unwrap(),
        200_i64.to_le_bytes()
    );
    store.put(b"counter", &i64::MAX.to_le_bytes()).unwrap();
    let tail = store.tail_bytes().unwrap();
    assert!(store.increment_i64(b"counter", 1).is_err());
    assert_eq!(store.tail_bytes().unwrap(), tail);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get(b"a").unwrap().as_deref(),
        Some(&b"restored!"[..])
    );
    assert_eq!(
        reopened.get(b"counter").unwrap().unwrap(),
        i64::MAX.to_le_bytes()
    );
}
