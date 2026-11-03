use database_engine::Store;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn conditional_put_delete_and_mismatch_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    assert!(store.compare_exchange(b"key", None, Some(b"one")).unwrap());
    assert!(
        !store
            .compare_exchange(b"key", None, Some(b"wrong"))
            .unwrap()
    );
    assert!(
        !store
            .compare_exchange(b"key", Some(b"wrong"), None)
            .unwrap()
    );
    assert!(
        store
            .compare_exchange(b"key", Some(b"one"), Some(b"two"))
            .unwrap()
    );
    assert!(store.compare_exchange(b"empty", None, Some(b"")).unwrap());
    assert!(!store.compare_exchange(b"empty", None, None).unwrap());
    assert!(store.compare_exchange(b"empty", Some(b""), None).unwrap());
    drop(store);
    let recovered = Store::open(dir.path()).unwrap();
    assert_eq!(recovered.get(b"key").unwrap().as_deref(), Some(&b"two"[..]));
    assert_eq!(recovered.get(b"empty").unwrap(), None);
}

#[test]
fn exactly_one_concurrent_claim_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let barrier = Barrier::new(9);
    let winners = thread::scope(|scope| {
        let mut workers = Vec::new();
        for candidate in 0..8u8 {
            let (barrier, store) = (&barrier, &store);
            workers.push(scope.spawn(move || {
                barrier.wait();
                (
                    candidate,
                    store
                        .compare_exchange(b"claim", None, Some(&[candidate]))
                        .unwrap(),
                )
            }));
        }
        barrier.wait();
        workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .filter_map(|(id, won)| won.then_some(id))
            .collect::<Vec<_>>()
    });
    assert_eq!(winners.len(), 1);
    drop(store);
    assert_eq!(
        Store::open(dir.path()).unwrap().get(b"claim").unwrap(),
        Some(vec![winners[0]])
    );
}

#[test]
fn concurrent_unconditional_put_cannot_be_overwritten_by_stale_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    for _ in 0..80 {
        store.put(b"key", b"initial").unwrap();
        let barrier = Barrier::new(3);
        thread::scope(|scope| {
            let (barrier1, barrier2) = (&barrier, &barrier);
            let (store1, store2) = (&store, &store);
            let conditional = scope.spawn(move || {
                barrier1.wait();
                store1
                    .compare_exchange(b"key", Some(b"initial"), Some(b"conditional"))
                    .unwrap()
            });
            let unconditional = scope.spawn(move || {
                barrier2.wait();
                store2.put(b"key", b"unconditional").unwrap();
            });
            barrier.wait();
            conditional.join().unwrap();
            unconditional.join().unwrap();
        });
        assert_eq!(
            store.get(b"key").unwrap().as_deref(),
            Some(&b"unconditional"[..])
        );
    }
}
