use database_engine::{CheckpointStore, CheckpointTransactionMode, Store, TimedTransactionError};
use std::cell::Cell;
use std::io;
use std::time::{Duration, Instant};

fn fixture() -> (tempfile::TempDir, CheckpointStore) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"base", b"old").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    (dir, store)
}

#[test]
fn expired_transactions_do_not_call_callbacks_and_unexpired_ones_commit() {
    let (_dir, store) = fixture();
    for mode in [
        CheckpointTransactionMode::Conservative,
        CheckpointTransactionMode::Fifo,
    ] {
        let called = Cell::new(false);
        let result = store.transaction_until(Instant::now(), mode, |_| {
            called.set(true);
            Ok(())
        });
        assert!(matches!(
            result,
            Err(TimedTransactionError::DeadlineExceeded)
        ));
        assert!(!called.get());
        store
            .transaction_until(Instant::now() + Duration::from_secs(30), mode, |tx| {
                assert_eq!(tx.get(b"base")?.as_deref(), Some(&b"old"[..]));
                tx.put(b"ok", b"committed")
            })
            .unwrap();
    }
    assert_eq!(
        store.get(b"ok").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
}

#[test]
fn expiry_in_callback_discards_staged_writes_and_forwarded_read_timeout_is_an_operation_error() {
    let (_dir, store) = fixture();
    for mode in [
        CheckpointTransactionMode::Conservative,
        CheckpointTransactionMode::Fifo,
    ] {
        let deadline = Instant::now() + Duration::from_millis(50);
        let result = store.transaction_until(deadline, mode, |tx| {
            tx.put(b"discarded", b"value")?;
            std::thread::sleep(
                deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
            );
            assert_eq!(tx.get(b"base").unwrap_err().kind(), io::ErrorKind::TimedOut);
            Ok(())
        });
        assert!(matches!(
            result,
            Err(TimedTransactionError::DeadlineExceeded)
        ));
        assert_eq!(store.get(b"discarded").unwrap(), None);

        let deadline = Instant::now() + Duration::from_millis(50);
        let result = store.transaction_until(deadline, mode, |tx| {
            std::thread::sleep(
                deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
            );
            tx.get(b"base")
        });
        match result {
            Err(TimedTransactionError::Operation(error)) => {
                assert_eq!(error.kind(), io::ErrorKind::TimedOut)
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
