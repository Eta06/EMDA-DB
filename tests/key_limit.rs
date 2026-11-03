use database_engine::Store;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn limit_allows_existing_updates_and_reclaims_deleted_key_slots() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open_with_limits(dir.path(), None, Some(2)).unwrap();
    store.put(b"a", b"old").unwrap();
    store.put(b"b", b"value").unwrap();
    store.put(b"a", b"new").unwrap();
    assert!(
        store
            .put(b"c", b"value")
            .unwrap_err()
            .to_string()
            .contains("key limit")
    );
    assert!(store.compare_exchange(b"c", None, Some(b"value")).is_err());
    assert!(store.increment_i64(b"counter", 1).is_err());
    assert_eq!(store.get(b"c").unwrap(), None);
    store.delete(b"a").unwrap();
    assert_eq!(store.increment_i64(b"counter", 1).unwrap(), 1);
    assert!(store.put(b"a", b"again").is_err());
    drop(store);
    assert!(Store::open_with_limits(dir.path(), None, Some(1)).is_err());
    let store = Store::open_with_limits(dir.path(), None, Some(2)).unwrap();
    assert_eq!(
        store.get(b"counter").unwrap().as_deref(),
        Some(&1_i64.to_le_bytes()[..])
    );
}

#[test]
fn simultaneous_new_keys_cannot_exceed_key_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open_with_limits(dir.path(), None, Some(3)).unwrap());
    let barrier = Barrier::new(9);
    let winners = thread::scope(|scope| {
        let mut threads = Vec::new();
        for id in 0..8u8 {
            let (store, barrier) = (&store, &barrier);
            threads.push(scope.spawn(move || {
                barrier.wait();
                (id, store.put(&[id + 1], b"value").is_ok())
            }));
        }
        barrier.wait();
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter_map(|(id, won)| won.then_some(id))
            .collect::<Vec<_>>()
    });
    assert_eq!(winners.len(), 3);
    drop(store);
    let reopened = Store::open_with_limits(dir.path(), None, Some(3)).unwrap();
    assert_eq!(reopened.scan(None, None, 10).unwrap().len(), 3);
}

#[test]
fn both_limits_apply_without_modifying_rejected_records() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open_with_limits(dir.path(), Some(51), Some(1)).unwrap();
    store.put(b"a", b"1").unwrap();
    assert!(store.put(b"b", b"1").is_err());
    assert!(store.put(b"a", b"2").is_err());
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(store.get(b"b").unwrap(), None);
}

#[test]
fn replay_peak_limit_fails_without_modification_and_compaction_can_reduce_peak() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let store = Store::open(&source).unwrap();
    store.put(b"a", b"value").unwrap();
    store.put(b"b", b"value").unwrap();
    store.put(b"c", b"value").unwrap();
    store.delete(b"a").unwrap();
    store.delete(b"b").unwrap();
    drop(store);
    let before = std::fs::read(source.join("records.log")).unwrap();
    let error = match Store::open_with_limits(&source, None, Some(1)) {
        Ok(_) => panic!("replay must enforce the limit on historical prefixes"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("recovery live key limit"));
    assert_eq!(std::fs::read(source.join("records.log")).unwrap(), before);
    let store = Store::open(&source).unwrap();
    let compacted = dir.path().join("compacted");
    store.compact_to(&compacted).unwrap();
    let reopened = Store::open_with_limits(compacted, None, Some(1)).unwrap();
    assert_eq!(reopened.get(b"c").unwrap().as_deref(), Some(&b"value"[..]));
}
