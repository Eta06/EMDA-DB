//! Direct-write admission includes checkpoint state-lock waiters.
use super::*;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn conditional_value_checks_allow_reads_while_excluding_writers() {
    let previous = 9_i64.to_le_bytes();
    let next = 10_i64.to_le_bytes();
    for operation in ["compare_exchange", "increment", "batch", "mismatch"] {
        let (_dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.put(b"counter", &previous).unwrap();
        let log_path = path.join("records.log");
        let before = fs::read(&log_path).unwrap();
        let (entered, reading) = mpsc::channel();
        let (resume, release) = mpsc::channel();
        Arc::get_mut(&mut store.state.write().unwrap().view.base.log)
            .unwrap()
            .replace_io_for_test(Box::new(PausingLog {
                file: OpenOptions::new().read(true).open(&log_path).unwrap(),
                armed: AtomicBool::new(true),
                entered,
                resume: Mutex::new(release),
            }));
        let snapshot = thread::scope(|scope| {
            let store = &store;
            let writing = scope.spawn(|| match operation {
                "compare_exchange" => assert!(
                    store
                        .compare_exchange(b"counter", Some(&previous), Some(&next))
                        .unwrap()
                ),
                "increment" => assert_eq!(store.increment_i64(b"counter", 1).unwrap(), 10),
                "batch" => assert!(
                    store
                        .compare_exchange_batch(
                            &[(b"counter", Some(&previous))],
                            &[BatchOperation::Put {
                                key: b"counter",
                                value: &next
                            }],
                        )
                        .unwrap()
                ),
                "mismatch" => assert!(
                    !store
                        .compare_exchange(b"counter", Some(&next), None)
                        .unwrap()
                ),
                _ => unreachable!(),
            });
            reading.recv_timeout(Duration::from_secs(10)).unwrap();
            assert!(matches!(
                store.writers.try_read(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            assert_eq!(fs::read(&log_path).unwrap(), before);
            let (observed, committed) = mpsc::channel();
            scope.spawn(move || {
                let value = store.get(b"counter").unwrap();
                observed.send((value, store.snapshot().unwrap())).unwrap();
            });
            let observed = committed.recv_timeout(Duration::from_secs(3));
            resume.send(()).unwrap();
            writing.join().unwrap();
            let (value, snapshot) =
                observed.expect("committed reads stalled during conditional check");
            assert_eq!(value.as_deref(), Some(previous.as_slice()));
            snapshot
        });
        let expected = if operation == "mismatch" {
            previous
        } else {
            next
        };
        assert_eq!(
            snapshot.get(b"counter").unwrap().as_deref(),
            Some(previous.as_slice())
        );
        assert_eq!(
            store.get(b"counter").unwrap().as_deref(),
            Some(expected.as_slice())
        );
        if operation == "mismatch" {
            assert_eq!(fs::read(&log_path).unwrap(), before);
        }
        drop(snapshot);
        drop(store);
        let reopened = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        assert_eq!(
            reopened.get(b"counter").unwrap().as_deref(),
            Some(expected.as_slice())
        );
    }
}

#[test]
fn direct_limit_covers_state_lock_wait_and_releases_after_recovery() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    store.set_active_direct_write_limit(1).unwrap();
    let held = store.state.write().unwrap();
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
    let recovered = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        recovered.get(b"first").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
    assert_eq!(recovered.get(b"second").unwrap(), None);
}
