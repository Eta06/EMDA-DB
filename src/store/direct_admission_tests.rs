//! Direct-write admission includes writers waiting at the ordinary barrier.
use super::*;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn direct_limit_covers_writer_barrier_wait_and_releases_after_recovery() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    store.set_active_direct_write_limit(1).unwrap();
    let held = store.writes.write().unwrap();
    let writing = Arc::clone(&store);
    let worker = thread::spawn(move || writing.put(b"first", b"committed"));
    let timeout = Instant::now() + Duration::from_secs(10);
    while store.direct_write_status().active != 1 {
        assert!(Instant::now() < timeout);
        thread::yield_now();
    }
    assert_eq!(
        store.set_active_direct_write_limit(0).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let second_store = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let second = thread::spawn(move || {
        done_tx
            .send(second_store.put(b"second", b"rejected"))
            .unwrap();
    });
    let early = done_rx.recv_timeout(Duration::from_secs(5));
    drop(held);
    worker.join().unwrap().unwrap();
    second.join().unwrap();
    assert_eq!(
        early
            .expect("over-limit write blocked instead of rejecting")
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(store.direct_write_status().active, 0);
    assert_eq!(store.get(b"second").unwrap(), None);
    drop(store);
    let recovered = crate::test_support::retry_lock(|| Store::open(root.path())).unwrap();
    assert_eq!(
        recovered.get(b"first").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
    assert_eq!(recovered.get(b"second").unwrap(), None);
}
