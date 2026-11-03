//! Tests for bounded capacity while writes prepare outside the queue lock.
use super::*;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn a_delayed_preparation_does_not_block_other_writers() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    // Hold the capacity a large in-progress preparation would occupy.
    let pending = store.commits.reserve_for_test(16 * 1024 * 1024).unwrap();
    let writing = Arc::clone(&store);
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let _ = done_tx.send(writing.put(b"small", b"value"));
    });
    let completed = done_rx.recv_timeout(Duration::from_secs(3));
    drop(pending);
    writer.join().unwrap();
    completed
        .expect("a reserved preparation blocked the queue")
        .unwrap();
    assert_eq!(store.commits.buffered(), (0, 0));
    assert_eq!(store.get(b"small").unwrap().as_deref(), Some(&b"value"[..]));
}

#[test]
fn panicking_preparation_releases_capacity_and_wakes_waiters() {
    let coordinator = Arc::new(Coordinator::for_test(100, 1));
    let preparing = Arc::clone(&coordinator);
    let (reserved_tx, reserved_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let first = thread::spawn(move || {
        let _reservation = preparing.reserve_for_test(100).unwrap();
        reserved_tx.send(()).unwrap();
        resume_rx.recv().unwrap();
        panic!("injected preparation panic");
    });
    reserved_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let waiting = Arc::clone(&coordinator);
    let (done_tx, done_rx) = mpsc::channel();
    let second = thread::spawn(move || {
        let reservation = waiting.reserve_for_test(100).unwrap();
        drop(reservation);
        done_tx.send(()).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while coordinator.admission_waiters() == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    let waiting = coordinator.admission_waiters();
    resume_tx.send(()).unwrap();
    assert!(first.join().is_err());
    done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    second.join().unwrap();
    assert!(
        waiting > 0,
        "the second reservation did not wait for capacity"
    );
    assert_eq!(coordinator.buffered(), (0, 0));
}
