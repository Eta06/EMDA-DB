use database_engine::{Store, TimedTransactionError, TransactionMode};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const MODES: [TransactionMode; 3] = [
    TransactionMode::Conservative,
    TransactionMode::ReadSet,
    TransactionMode::ReadSetFifo,
];

#[test]
fn expiry_before_or_after_callback_discards_staged_writes_in_all_modes() {
    for mode in MODES {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let called = AtomicBool::new(false);
        let expired = store.transaction_until(Instant::now(), mode, |_| {
            called.store(true, Ordering::Relaxed);
            Ok(())
        });
        assert!(matches!(
            expired,
            Err(TimedTransactionError::DeadlineExceeded)
        ));
        assert!(!called.load(Ordering::Relaxed));

        let original = std::fs::read(directory.path().join("records.log")).unwrap();
        let staged = AtomicBool::new(false);
        let deadline = Instant::now() + Duration::from_millis(250);
        let result = store.transaction_until(deadline, mode, |tx| {
            tx.put(b"first", b"value")?;
            tx.put(b"second", b"value")?;
            staged.store(true, Ordering::Relaxed);
            std::thread::sleep(
                deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
            );
            Ok(())
        });
        assert!(matches!(
            result,
            Err(TimedTransactionError::DeadlineExceeded)
        ));
        assert!(staged.load(Ordering::Relaxed));
        assert_eq!(
            std::fs::read(directory.path().join("records.log")).unwrap(),
            original
        );
        drop(store);
        let reopened = Store::open(directory.path()).unwrap();
        assert_eq!(
            reopened.get_many(&[b"first", b"second"]).unwrap(),
            vec![None, None]
        );
    }
}

#[test]
fn unexpired_transactions_commit_in_every_mode() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    for (index, mode) in MODES.into_iter().enumerate() {
        let key = [index as u8 + 1];
        store
            .transaction_until(Instant::now() + Duration::from_secs(10), mode, |tx| {
                assert!(tx.get(&key)?.is_none());
                tx.put(&key, b"value")
            })
            .unwrap();
        assert_eq!(store.get(&key).unwrap().as_deref(), Some(&b"value"[..]));
    }
    drop(store);
    let reopened = Store::open(directory.path()).unwrap();
    for key in [1_u8, 2, 3] {
        assert_eq!(
            reopened.get(&[key]).unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}

#[test]
fn a_callback_timeout_is_not_mistaken_for_engine_deadline_expiry() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let result: Result<(), _> = store.transaction_until(
        Instant::now() + Duration::from_secs(10),
        TransactionMode::ReadSet,
        |tx| {
            tx.put(b"key", b"value")?;
            Err(io::Error::new(io::ErrorKind::TimedOut, "callback timeout"))
        },
    );
    match result {
        Err(TimedTransactionError::Operation(error)) => {
            assert_eq!(error.kind(), io::ErrorKind::TimedOut)
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(store.get(b"key").unwrap().is_none());
}

#[test]
fn an_expired_snapshot_read_forwarded_by_the_callback_is_an_operation_error() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let deadline = Instant::now() + Duration::from_millis(250);
    let result = store.transaction_until(deadline, TransactionMode::ReadSet, |tx| {
        tx.put(b"key", b"value")?;
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
        );
        tx.get(b"key")
    });
    match result {
        Err(TimedTransactionError::Operation(error)) => {
            assert_eq!(error.kind(), io::ErrorKind::TimedOut)
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(store.get(b"key").unwrap().is_none());
}
