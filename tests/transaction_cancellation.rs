use database_engine::{
    CancellationToken, CheckpointStore, CheckpointTransactionMode, CollectionSchema,
    ControlledTransactionError, DocumentDatabase, DocumentTransactionMode, IndexDefinition,
    IndexQuery, IndexValue, Store, TransactionMode,
};
use serde_json::json;
use std::io::{self, ErrorKind};
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

mod support;
use support::retry_lock;

fn exercise(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "unique".into(),
            fields: vec![vec!["v".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    for mode in [
        DocumentTransactionMode::Conservative,
        DocumentTransactionMode::ReadSet,
        DocumentTransactionMode::ReadSetFifo,
    ] {
        let token = CancellationToken::new();
        let clone = token.clone();
        clone.cancel();
        assert!(token.is_cancelled());
        let called = AtomicBool::new(false);
        let result = db.transaction_controlled(&token, None, mode, |_tx| {
            called.store(true, Ordering::Relaxed);
            Ok(())
        });
        assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
        assert!(!called.load(Ordering::Relaxed));
    }
    let deadline_token = CancellationToken::new();
    let deadline = Instant::now() - Duration::from_millis(1);
    assert!(matches!(
        db.transaction_controlled(
            &deadline_token,
            Some(deadline),
            DocumentTransactionMode::Conservative,
            |_| Ok(())
        ),
        Err(ControlledTransactionError::DeadlineExceeded)
    ));

    let token = CancellationToken::new();
    let reached = Barrier::new(2);
    let released = Barrier::new(2);
    std::thread::scope(|scope| {
        let cancel = token.clone();
        let (writer_reached, writer_released) = (&reached, &released);
        let writer = scope.spawn(move || {
            writer_reached.wait();
            cancel.cancel();
            writer_released.wait();
        });
        let result =
            db.transaction_controlled(&token, None, DocumentTransactionMode::ReadSet, |tx| {
                tx.insert("c", "cancelled", &json!({"v":7}))?;
                tx.put_raw(b"cancelled-raw", b"no")?;
                reached.wait();
                released.wait();
                Ok(())
            });
        assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
        writer.join().unwrap();
    });
    assert_eq!(db.get("c", "cancelled").unwrap(), None);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"cancelled-raw")).unwrap(),
        None
    );
    let page = db
        .query_index(
            "c",
            "unique",
            &IndexQuery {
                equal: vec![IndexValue::Integer(7)],
                ..Default::default()
            },
        )
        .unwrap();
    assert!(page.documents.is_empty());
    db.insert("c", "kept", &json!({"v":7})).unwrap();

    let token = CancellationToken::new();
    let result =
        db.transaction_controlled(&token, None, DocumentTransactionMode::Conservative, |tx| {
            tx.insert("c", "cancelled-in-callback", &json!({"v":8}))?;
            token.cancel();
            tx.put_raw(b"never-staged", b"x")?;
            Ok(())
        });
    assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
    assert_eq!(db.get("c", "cancelled-in-callback").unwrap(), None);
    let token = CancellationToken::new();
    let result =
        db.transaction_controlled(&token, None, DocumentTransactionMode::Conservative, |_tx| {
            token.cancel();
            Err::<(), _>(io::Error::new(
                ErrorKind::Interrupted,
                "independent callback interruption",
            ))
        });
    assert!(
        matches!(result, Err(ControlledTransactionError::Operation(error)) if error.kind() == ErrorKind::Interrupted)
    );
}

#[test]
fn document_and_raw_changes_cancel_atomically_on_both_backends_and_recovery() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = Store::open(&path).unwrap();
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(initial);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            exercise(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            exercise(&DocumentDatabase::new(&initial));
            drop(initial);
        }
        let recovered = retry_lock(|| Store::open(&path)).unwrap();
        let db = DocumentDatabase::new(&recovered);
        assert_eq!(db.get("c", "cancelled").unwrap(), None);
        assert_eq!(db.get("c", "cancelled-in-callback").unwrap(), None);
        assert_eq!(db.get("c", "kept").unwrap(), Some(json!({"v":7})));
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"cancelled-raw")).unwrap(),
            None
        );
    }
}

#[test]
fn checkpoint_conservative_fifo_mode_also_honors_pre_cancelled_tokens() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let token = CancellationToken::new();
    token.cancel();
    assert!(matches!(
        store.transaction_controlled(&token, None, CheckpointTransactionMode::Fifo, |tx| tx
            .put(b"x", b"y")),
        Err(ControlledTransactionError::Cancelled)
    ));
    assert_eq!(store.get(b"x").unwrap(), None);
}

#[test]
fn ordinary_controlled_transaction_can_commit_when_not_cancelled() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let token = CancellationToken::new();
    store
        .transaction_controlled(
            &token,
            Some(Instant::now() + Duration::from_secs(30)),
            TransactionMode::ReadSetFifo,
            |tx| tx.put(b"x", b"y"),
        )
        .unwrap();
    assert_eq!(store.get(b"x").unwrap().as_deref(), Some(&b"y"[..]));
}
