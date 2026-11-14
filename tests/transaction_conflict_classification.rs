//! A conflict marker must identify only a pre-commit transaction rejection.
use database_engine::{
    CancellationToken, CheckpointStore, CheckpointTransactionMode, ControlledTransactionError,
    DocumentDatabase, Store, TimedTransactionError, TransactionMode, is_transaction_conflict,
};
use std::io::{self, ErrorKind};
use std::time::{Duration, Instant};

#[test]
fn ordinary_conflicts_are_marked_but_callback_and_admission_errors_are_not() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    for mode in [
        TransactionMode::Conservative,
        TransactionMode::ReadSet,
        TransactionMode::ReadSetFifo,
    ] {
        store.put(b"observed", b"before").unwrap();
        let error = store
            .transaction_until(Instant::now() + Duration::from_secs(10), mode, |tx| {
                assert_eq!(tx.get(b"observed")?.as_deref(), Some(&b"before"[..]));
                tx.put(b"staged", b"must-not-commit")?;
                store.put(b"observed", b"changed")?;
                Ok(())
            })
            .unwrap_err();
        match error {
            TimedTransactionError::Operation(error) => {
                assert_eq!(error.kind(), ErrorKind::WouldBlock);
                assert!(is_transaction_conflict(&error));
            }
            other => panic!("expected conflict: {other:?}"),
        }
        assert_eq!(store.get(b"staged").unwrap(), None);
    }
    let callback_error = store
        .transaction(|_| -> io::Result<()> {
            Err(io::Error::new(
                ErrorKind::WouldBlock,
                "transaction conflict: observed state changed since snapshot",
            ))
        })
        .unwrap_err();
    assert!(!is_transaction_conflict(&callback_error));
    store.set_active_transaction_limit(0).unwrap();
    let admission = store.transaction(|_| Ok(())).unwrap_err();
    assert_eq!(admission.kind(), ErrorKind::WouldBlock);
    assert!(!is_transaction_conflict(&admission));
}

#[test]
fn checkpoint_conflicts_in_callback_and_final_validation_are_marked() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"observed", b"before").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let error = store
        .transaction(|tx| {
            tx.put(b"staged", b"must-not-commit")?;
            store.put(b"observed", b"changed")?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert!(is_transaction_conflict(&error));
    assert_eq!(store.get(b"staged").unwrap(), None);

    let error = store
        .transaction_with_read_set(|tx| {
            tx.get(b"observed")?;
            store.put(b"observed", b"changed-again")?;
            tx.get(b"observed")?;
            Ok(())
        })
        .unwrap_err();
    assert!(is_transaction_conflict(&error));
    let callback_error = store
        .transaction(|_| -> io::Result<()> { Err(io::Error::from(ErrorKind::WouldBlock)) })
        .unwrap_err();
    assert!(!is_transaction_conflict(&callback_error));

    let error = store
        .transaction_until(
            Instant::now() + Duration::from_secs(10),
            CheckpointTransactionMode::ReadSetFifo,
            |tx| {
                tx.get(b"observed")?;
                store.put(b"observed", b"timed-change")?;
                tx.put(b"timed-staged", b"must-not-commit")
            },
        )
        .unwrap_err();
    match error {
        TimedTransactionError::Operation(error) => assert!(is_transaction_conflict(&error)),
        other => panic!("expected timed conflict: {other:?}"),
    }
    assert_eq!(store.get(b"timed-staged").unwrap(), None);
}

#[test]
fn document_transaction_preserves_a_real_conflict_marker() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.put(b"observed", b"before").unwrap();
    let documents = DocumentDatabase::new(&store);
    let error = documents
        .transaction(|tx| {
            tx.get_raw(b"observed")?;
            tx.put_raw(b"staged", b"must-not-commit")?;
            store.put(b"observed", b"changed")?;
            Ok(())
        })
        .unwrap_err();
    assert!(is_transaction_conflict(&error));
    assert_eq!(store.get(b"staged").unwrap(), None);
}

#[test]
fn controlled_transactions_preserve_confirmed_conflicts_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"observed", b"before").unwrap();
    let token = CancellationToken::new();
    let error = ordinary
        .transaction_controlled(&token, None, TransactionMode::ReadSet, |tx| {
            tx.get(b"observed")?;
            ordinary.put(b"observed", b"changed")?;
            tx.put(b"staged", b"must-not-commit")
        })
        .unwrap_err();
    match error {
        ControlledTransactionError::Operation(error) => assert!(is_transaction_conflict(&error)),
        other => panic!("expected confirmed conflict: {other:?}"),
    }
    assert_eq!(ordinary.get(b"staged").unwrap(), None);
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);

    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let error = store
        .transaction_controlled(&token, None, CheckpointTransactionMode::ReadSet, |tx| {
            tx.get(b"observed")?;
            store.put(b"observed", b"changed-again")?;
            tx.put(b"staged", b"must-not-commit")
        })
        .unwrap_err();
    match error {
        ControlledTransactionError::Operation(error) => assert!(is_transaction_conflict(&error)),
        other => panic!("expected confirmed conflict: {other:?}"),
    }
    assert_eq!(store.get(b"staged").unwrap(), None);
}
