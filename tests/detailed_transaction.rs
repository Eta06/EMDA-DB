use database_engine::{
    CancellationToken, CheckpointStore, CheckpointTransaction, CheckpointTransactionMode,
    DocumentDatabase, DocumentTransactionMode, Store, Transaction, TransactionFailure,
    TransactionMode, is_transaction_conflict,
};
use std::cell::Cell;
use std::io;
use std::time::{Duration, Instant};

trait TransactionOps {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()>;
}

impl TransactionOps for Transaction<'_> {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get(key)
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.put(key, value)
    }
}

impl TransactionOps for CheckpointTransaction<'_> {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get(key)
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.put(key, value)
    }
}

trait Backend {
    fn run<T>(
        &self,
        mode: usize,
        deadline: Option<Instant>,
        token: Option<&CancellationToken>,
        action: impl FnOnce(&mut dyn TransactionOps) -> io::Result<T>,
    ) -> Result<T, TransactionFailure>;
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn limit(&self, value: usize);
}

impl Backend for Store {
    fn run<T>(
        &self,
        mode: usize,
        deadline: Option<Instant>,
        token: Option<&CancellationToken>,
        action: impl FnOnce(&mut dyn TransactionOps) -> io::Result<T>,
    ) -> Result<T, TransactionFailure> {
        let mode = [
            TransactionMode::Conservative,
            TransactionMode::ReadSet,
            TransactionMode::ReadSetFifo,
        ][mode];
        self.transaction_detailed(mode, deadline, token, |tx| action(tx))
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get(key)
    }
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.put(key, value)
    }
    fn limit(&self, value: usize) {
        self.set_active_transaction_limit(value).unwrap();
    }
}

impl Backend for CheckpointStore {
    fn run<T>(
        &self,
        mode: usize,
        deadline: Option<Instant>,
        token: Option<&CancellationToken>,
        action: impl FnOnce(&mut dyn TransactionOps) -> io::Result<T>,
    ) -> Result<T, TransactionFailure> {
        let mode = [
            CheckpointTransactionMode::Conservative,
            CheckpointTransactionMode::ReadSet,
            CheckpointTransactionMode::ReadSetFifo,
        ][mode];
        self.transaction_detailed(mode, deadline, token, |tx| action(tx))
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get(key)
    }
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.put(key, value)
    }
    fn limit(&self, value: usize) {
        self.set_active_transaction_limit(value).unwrap();
    }
}

fn exercise_backend(store: &impl Backend) {
    for mode in 0..3 {
        store.put(b"observed", b"before").unwrap();
        let value = store
            .run(mode, None, None, |tx| {
                tx.put(b"success", b"yes")?;
                Ok(42)
            })
            .unwrap();
        assert_eq!(value, 42);
        assert_eq!(store.get(b"success").unwrap().as_deref(), Some(&b"yes"[..]));

        let error = store
            .run(mode, None, None, |tx| {
                tx.put(b"aborted", b"no")?;
                // An independent callback effect survives the aborted transaction.
                store.put(b"external", b"yes")?;
                Err::<(), _>(io::Error::from_raw_os_error(11))
            })
            .unwrap_err();
        assert!(
            matches!(error, TransactionFailure::Callback(ref error) if error.raw_os_error() == Some(11))
        );
        assert_eq!(store.get(b"aborted").unwrap(), None);
        assert_eq!(
            store.get(b"external").unwrap().as_deref(),
            Some(&b"yes"[..])
        );

        let error = store
            .run(mode, None, None, |tx| {
                tx.get(b"observed")?;
                tx.put(b"conflicted", b"no")?;
                store.put(b"observed", b"changed")?;
                Ok(())
            })
            .unwrap_err();
        assert!(
            matches!(error, TransactionFailure::Rejected(ref error) if is_transaction_conflict(error))
        );
        assert_eq!(store.get(b"conflicted").unwrap(), None);

        let token = CancellationToken::new();
        let error = store
            .run(mode, None, Some(&token), |tx| {
                tx.put(b"cancelled", b"no")?;
                token.cancel();
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(error, TransactionFailure::Cancelled));
        assert_eq!(store.get(b"cancelled").unwrap(), None);

        let token = CancellationToken::new();
        let error = store
            .run(mode, None, Some(&token), |tx| {
                tx.put(b"callback-cancelled", b"no")?;
                token.cancel();
                // Forwarding a cancellation marker from inside a callback is
                // still classified by its origin, not as an engine control result.
                tx.get(b"observed")?;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(error, TransactionFailure::Callback(_)));
        assert_eq!(store.get(b"callback-cancelled").unwrap(), None);

        let called = Cell::new(false);
        let error = store
            .run(
                mode,
                Some(Instant::now() - Duration::from_secs(1)),
                None,
                |_| {
                    called.set(true);
                    Ok(())
                },
            )
            .unwrap_err();
        assert!(!called.get());
        assert!(matches!(error, TransactionFailure::DeadlineExceeded));
    }

    // A forwarded conflict from an independently run transaction is a
    // callback failure for the outer transaction, not its validation error.
    let nested = store
        .run(0, None, None, |tx| {
            tx.put(b"outer", b"no")?;
            match store.run(0, None, None, |inner| {
                inner.get(b"observed")?;
                store.put(b"observed", b"nested-change")?;
                Ok(())
            }) {
                Err(TransactionFailure::Rejected(error)) => Err::<(), _>(error),
                other => panic!("expected nested conflict: {other:?}"),
            }
        })
        .unwrap_err();
    assert!(
        matches!(nested, TransactionFailure::Callback(ref error) if is_transaction_conflict(error))
    );
    assert_eq!(store.get(b"outer").unwrap(), None);

    store.limit(0);
    let called = Cell::new(false);
    let error = store
        .run(0, None, None, |_| {
            called.set(true);
            Ok(())
        })
        .unwrap_err();
    assert!(!called.get());
    assert!(matches!(error, TransactionFailure::Rejected(_)));
    store.limit(database_engine::MAX_ACTIVE_TRANSACTIONS);
}

#[test]
fn structured_outcomes_match_both_writable_backends_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    exercise_backend(&store);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    exercise_backend(&store);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get(b"success").unwrap().as_deref(),
        Some(&b"yes"[..])
    );
    for key in [
        b"aborted".as_slice(),
        b"conflicted",
        b"cancelled",
        b"callback-cancelled",
        b"outer",
    ] {
        assert_eq!(reopened.get(key).unwrap(), None);
    }
}

fn exercise_documents(documents: &DocumentDatabase<'_>) {
    for mode in [
        DocumentTransactionMode::Conservative,
        DocumentTransactionMode::ReadSet,
        DocumentTransactionMode::ReadSetFifo,
    ] {
        documents
            .transaction_detailed(mode, None, None, |tx| tx.put_raw(b"doc-success", b"yes"))
            .unwrap();
        let error = documents
            .transaction_detailed(mode, None, None, |tx| {
                tx.put_raw(b"doc-aborted", b"no")?;
                // A handled document mutation failure still aborts staging.
                assert!(tx.put_raw(b"", b"invalid").is_err());
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(error, TransactionFailure::Callback(_)));
        documents
            .transaction_detailed(mode, None, None, |tx| {
                assert_eq!(tx.get_raw(b"doc-aborted")?, None);
                assert_eq!(tx.get_raw(b"doc-success")?.as_deref(), Some(&b"yes"[..]));
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn document_staging_failures_are_classified_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    exercise_documents(&DocumentDatabase::new(&store));
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    exercise_documents(&DocumentDatabase::with_checkpoint_store(&store));
}

#[test]
fn commit_path_rejections_can_be_conservatively_unknown_without_poisoning() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"yes").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let length = std::fs::metadata(path.join("records.log")).unwrap().len();

    let store = Store::open_with_log_limit(&path, length).unwrap();
    let error = store
        .transaction_detailed(TransactionMode::Conservative, None, None, |tx| {
            tx.put(b"over-limit", b"no")
        })
        .unwrap_err();
    assert!(matches!(error, TransactionFailure::OutcomeUnknown(_)));
    assert_eq!(store.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(store.get(b"over-limit").unwrap(), None);
    drop(store);

    let store = CheckpointStore::open_with_resource_limits(
        &path,
        &checkpoint,
        &digest,
        database_engine::MAX_RECOVERED_VIEW_TAIL_BYTES,
        database_engine::MAX_RECOVERED_VIEW_TAIL_KEYS,
        database_engine::MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        Some(length),
    )
    .unwrap();
    let error = store
        .transaction_detailed(CheckpointTransactionMode::Conservative, None, None, |tx| {
            tx.put(b"over-limit", b"no")
        })
        .unwrap_err();
    assert!(matches!(error, TransactionFailure::OutcomeUnknown(_)));
    assert_eq!(store.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(store.get(b"over-limit").unwrap(), None);
    assert_eq!(
        std::fs::metadata(path.join("records.log")).unwrap().len(),
        length
    );
}
