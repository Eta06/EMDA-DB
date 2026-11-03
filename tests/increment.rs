use database_engine::Store;
use std::io::ErrorKind;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn signed_increment_checks_encoding_and_overflow_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.increment_i64(b"n", -3).unwrap(), -3);
    assert_eq!(store.increment_i64(b"n", 8).unwrap(), 5);
    store.put(b"bad", b"text").unwrap();
    assert_eq!(
        store.increment_i64(b"bad", 1).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
    assert_eq!(store.get(b"bad").unwrap().as_deref(), Some(&b"text"[..]));
    store.put(b"max", &i64::MAX.to_le_bytes()).unwrap();
    assert_eq!(
        store.increment_i64(b"max", 1).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store.get(b"max").unwrap().as_deref(),
        Some(&i64::MAX.to_le_bytes()[..])
    );
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get(b"n")
            .unwrap()
            .as_deref(),
        Some(&5_i64.to_le_bytes()[..])
    );
}

#[test]
fn simultaneous_increments_are_not_lost_and_recover() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let barrier = Barrier::new(9);
    let mut values = thread::scope(|scope| {
        let mut threads = Vec::new();
        for _ in 0..8 {
            let (store, barrier) = (&store, &barrier);
            threads.push(scope.spawn(move || {
                barrier.wait();
                (0..20)
                    .map(|_| store.increment_i64(b"counter", 1).unwrap())
                    .collect::<Vec<_>>()
            }));
        }
        barrier.wait();
        threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    values.sort_unstable();
    assert_eq!(values, (1..=160).collect::<Vec<_>>());
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get(b"counter")
            .unwrap()
            .as_deref(),
        Some(&160_i64.to_le_bytes()[..])
    );
}
