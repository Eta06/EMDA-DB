use database_engine::Store;
use std::fs;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn limit_counts_header_overwrites_and_tombstones_and_does_not_poison_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open_with_log_limit(dir.path(), 51).unwrap();
    store.put(b"a", b"1").unwrap(); // 28-byte log header + 23-byte record
    assert!(
        store
            .put(b"a", b"2")
            .unwrap_err()
            .to_string()
            .contains("limit")
    );
    assert!(
        store
            .delete(b"a")
            .unwrap_err()
            .to_string()
            .contains("limit")
    );
    assert!(!store.compare_exchange(b"a", Some(b"wrong"), None).unwrap());
    assert!(
        store
            .compare_exchange(b"a", Some(b"1"), None)
            .unwrap_err()
            .to_string()
            .contains("limit")
    );
    assert!(
        store
            .increment_i64(b"b", 1)
            .unwrap_err()
            .to_string()
            .contains("limit")
    );
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(
        fs::metadata(dir.path().join("records.log")).unwrap().len(),
        51
    );
    drop(store);
    assert!(Store::open_with_log_limit(dir.path(), 50).is_err());
    let larger = Store::open_with_log_limit(dir.path(), 100).unwrap();
    larger.delete(b"a").unwrap();
    assert_eq!(larger.get(b"a").unwrap(), None);
}

#[test]
fn concurrent_mutations_cannot_oversubscribe_limit() {
    let dir = tempfile::tempdir().unwrap();
    let limit = 28 + 4 * (17 + 2 + 8 + 4);
    let store = Arc::new(Store::open_with_log_limit(dir.path(), limit).unwrap());
    let barrier = Barrier::new(9);
    let winners = thread::scope(|scope| {
        let mut workers = Vec::new();
        for id in 0..8u8 {
            let (store, barrier) = (&store, &barrier);
            workers.push(scope.spawn(move || {
                barrier.wait();
                (id, store.put(&[b'k', id], &[id; 8]).is_ok())
            }));
        }
        barrier.wait();
        workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .filter_map(|(id, ok)| ok.then_some(id))
            .collect::<Vec<_>>()
    });
    assert_eq!(winners.len(), 4);
    assert_eq!(
        fs::metadata(dir.path().join("records.log")).unwrap().len(),
        limit
    );
    drop(store);
    let reopened = Store::open_with_log_limit(dir.path(), limit).unwrap();
    for id in 0..8u8 {
        assert_eq!(
            reopened.get(&[b'k', id]).unwrap(),
            winners.contains(&id).then_some(vec![id; 8])
        );
    }
}
