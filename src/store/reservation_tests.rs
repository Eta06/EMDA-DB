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

#[test]
fn reconfiguration_is_atomic_and_rechecks_waiting_record_size() {
    let coordinator = Arc::new(Coordinator::for_test(200, 1));
    let held = coordinator.reserve_for_test(60).unwrap();
    let initial = coordinator.status().unwrap();
    assert_eq!((initial.limit_bytes, initial.limit_records), (200, 1));
    assert_eq!((initial.reserved_bytes, initial.reserved_records), (60, 1));
    assert_eq!(
        coordinator.set_limits(59, 1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        coordinator.set_limits(200, 0).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(coordinator.status().unwrap(), initial);

    let waiting = Arc::clone(&coordinator);
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = waiting.reserve_for_test(150).map(drop);
        done_tx.send(result).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while coordinator.admission_waiters() != 1 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    // Existing reservation fits, but the waiting 150-byte record no longer
    // can. It must wake and reject before the held reservation is released.
    coordinator.set_limits(100, 1).unwrap();
    let early = done_rx.recv_timeout(Duration::from_secs(5));
    drop(held);
    let result = early.expect("waiting writer did not recheck reduced limit");
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    worker.join().unwrap();
    let status = coordinator.status().unwrap();
    assert_eq!((status.limit_bytes, status.limit_records), (100, 1));
    assert_eq!(
        (
            status.reserved_bytes,
            status.reserved_records,
            status.waiting_writers
        ),
        (0, 0, 0)
    );
}

#[test]
fn raising_record_limit_wakes_waiters_before_existing_reservations_finish() {
    let coordinator = Arc::new(Coordinator::for_test(100, 1));
    let held = coordinator.reserve_for_test(60).unwrap();
    let waiting = Arc::clone(&coordinator);
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = waiting.reserve_for_test(10).map(drop);
        done_tx.send(result).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while coordinator.admission_waiters() != 1 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    coordinator.set_limits(100, 2).unwrap();
    let early = done_rx.recv_timeout(Duration::from_secs(5));
    drop(held);
    early
        .expect("raising the limit did not wake a waiting writer")
        .unwrap();
    worker.join().unwrap();
    assert_eq!(coordinator.buffered(), (0, 0));
}
