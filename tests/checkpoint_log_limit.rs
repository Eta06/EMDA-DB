use database_engine::{
    CheckpointStore, MAX_RECOVERED_VIEW_TAIL_BYTES, MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
    MAX_RECOVERED_VIEW_TAIL_KEYS, Store,
};
use std::fs;
use std::io;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
    u64,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let length = fs::metadata(path.join("records.log")).unwrap().len();
    drop(store);
    (dir, path, checkpoint, digest, length)
}

fn open(
    path: &std::path::Path,
    checkpoint: &std::path::Path,
    digest: &[u8; 32],
    limit: u64,
) -> io::Result<CheckpointStore> {
    CheckpointStore::open_with_resource_limits(
        path,
        checkpoint,
        digest,
        MAX_RECOVERED_VIEW_TAIL_BYTES,
        MAX_RECOVERED_VIEW_TAIL_KEYS,
        MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        Some(limit),
    )
}

#[test]
fn whole_log_limit_survives_rollover_and_rejects_before_append() {
    let (dir, path, checkpoint, digest, initial) = fixture();
    let record_len = 21 + 1 + 3;
    let limit = initial + 2 * record_len;
    let store = open(&path, &checkpoint, &digest, limit).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"b", b"two").unwrap();
    let log = path.join("records.log");
    let before = fs::read(&log).unwrap();
    assert_eq!(before.len() as u64, limit);
    assert!(store.delete(b"a").is_err());
    assert_eq!(fs::read(&log).unwrap(), before);
    let next = dir.path().join("next");
    let next_digest = store.rollover_to(&next).unwrap();
    assert!(store.put(b"a", b"new").is_err());
    assert_eq!(fs::read(&log).unwrap(), before);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    drop(store);
    assert_eq!(
        open(&path, &next, &next_digest, limit - 1)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let reopened = open(&path, &next, &next_digest, limit).unwrap();
    assert_eq!(reopened.get(b"b").unwrap().as_deref(), Some(&b"two"[..]));
}

#[test]
fn concurrent_writers_cannot_oversubscribe_the_log_limit() {
    let (_dir, path, checkpoint, digest, initial) = fixture();
    let limit = initial + 8 * (21 + 1 + 3);
    let store = Arc::new(open(&path, &checkpoint, &digest, limit).unwrap());
    let start = Arc::new(Barrier::new(17));
    let accepted = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            let accepted = &accepted;
            scope.spawn(move || {
                start.wait();
                if store.put(b"a", b"new").is_ok() {
                    accepted.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        start.wait();
    });
    assert_eq!(accepted.load(Ordering::Relaxed), 8);
    assert_eq!(fs::metadata(path.join("records.log")).unwrap().len(), limit);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
    drop(store);
    assert_eq!(
        open(&path, &checkpoint, &digest, limit)
            .unwrap()
            .get(b"a")
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
}

#[test]
fn conditional_batch_mismatch_needs_no_log_space() {
    use database_engine::BatchOperation;
    let (_dir, path, checkpoint, digest, initial) = fixture();
    let store = open(&path, &checkpoint, &digest, initial).unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    let operations = [BatchOperation::Put {
        key: b"a",
        value: b"new",
    }];
    assert!(
        !store
            .compare_exchange_batch(&[(b"a", Some(b"wrong"))], &operations)
            .unwrap()
    );
    assert!(
        store
            .compare_exchange_batch(&[(b"a", Some(b"old"))], &operations)
            .is_err()
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
}
