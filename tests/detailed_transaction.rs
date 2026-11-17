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

    // A read that expires inside the callback is a callback failure, even
    // though engine-detected expiry after the callback has another variant.
    let deadline = Instant::now() + Duration::from_millis(250);
    let error = store
        .run(1, Some(deadline), None, |tx| {
            tx.put(b"callback-timeout", b"no")?;
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
            tx.get(b"observed")?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(error, TransactionFailure::Callback(ref error) if error.kind() == io::ErrorKind::TimedOut)
    );
    assert_eq!(store.get(b"callback-timeout").unwrap(), None);

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
fn log_quota_rejections_are_confirmed_before_submission() {
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
    assert!(matches!(error, TransactionFailure::Rejected(_)));
    assert_eq!(store.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(store.get(b"over-limit").unwrap(), None);
    let error = DocumentDatabase::new(&store)
        .transaction_detailed(DocumentTransactionMode::Conservative, None, None, |tx| {
            tx.put_raw(b"doc-over-limit", b"no")
        })
        .unwrap_err();
    assert!(matches!(error, TransactionFailure::Rejected(_)));
    assert_eq!(store.get(b"doc-over-limit").unwrap(), None);
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
    assert!(matches!(error, TransactionFailure::Rejected(_)));
    assert_eq!(store.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(store.get(b"over-limit").unwrap(), None);
    let error = DocumentDatabase::with_checkpoint_store(&store)
        .transaction_detailed(DocumentTransactionMode::Conservative, None, None, |tx| {
            tx.put_raw(b"doc-over-limit", b"no")
        })
        .unwrap_err();
    assert!(matches!(error, TransactionFailure::Rejected(_)));
    assert_eq!(store.get(b"doc-over-limit").unwrap(), None);
    assert_eq!(
        std::fs::metadata(path.join("records.log")).unwrap().len(),
        length
    );
}

#[test]
fn key_and_checkpoint_suffix_quota_reject_before_submission() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"yes").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let original = std::fs::read(path.join("records.log")).unwrap();

    let store = Store::open_with_limits(&path, None, Some(1)).unwrap();
    let error = store
        .transaction_detailed(TransactionMode::ReadSet, None, None, |tx| {
            tx.put(b"over-limit", b"no")
        })
        .unwrap_err();
    assert!(matches!(error, TransactionFailure::Rejected(_)));
    assert_eq!(store.get(b"over-limit").unwrap(), None);
    drop(store);

    let store = CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 0, 0, 0).unwrap();
    let error = store
        .transaction_detailed(CheckpointTransactionMode::ReadSet, None, None, |tx| {
            tx.put(b"over-limit", b"no")
        })
        .unwrap_err();
    assert!(matches!(error, TransactionFailure::Rejected(_)));
    assert_eq!(store.get(b"over-limit").unwrap(), None);
    drop(store);
    assert_eq!(std::fs::read(path.join("records.log")).unwrap(), original);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(reopened.get(b"over-limit").unwrap(), None);
}

#[test]
fn ordinary_group_admission_rejection_is_confirmed_before_submission() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.put(b"safe", b"yes").unwrap();
    store.set_group_commit_limits(0, 0).unwrap();
    let error = store
        .transaction_detailed(TransactionMode::Conservative, None, None, |tx| {
            tx.put(b"blocked", b"no")
        })
        .unwrap_err();
    assert!(matches!(error, TransactionFailure::Rejected(_)));
    assert_eq!(store.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
    assert_eq!(store.get(b"blocked").unwrap(), None);
}

fn race_for_one_remaining_key(store: &(impl Backend + Sync)) {
    let results = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let mut releases = Vec::new();
        let workers = [b"first".as_slice(), b"second".as_slice()].map(|key| {
            let ready = ready_tx.clone();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            releases.push(release_tx);
            scope.spawn(move || {
                store.run(1, None, None, |tx| {
                    let staged = tx.put(key, b"winner");
                    ready.send(()).map_err(io::Error::other)?;
                    release_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(io::Error::other)?;
                    staged
                })
            })
        });
        let ready = [
            ready_rx.recv_timeout(Duration::from_secs(10)),
            ready_rx.recv_timeout(Duration::from_secs(10)),
        ];
        for release in releases {
            let _ = release.send(());
        }
        let results = workers.map(|worker| worker.join().unwrap());
        assert!(
            ready.into_iter().all(|result| result.is_ok()),
            "transaction did not reach staging"
        );
        results
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(TransactionFailure::Rejected(_))))
            .count(),
        1
    );
    let present = [b"first".as_slice(), b"second".as_slice()]
        .iter()
        .filter(|key| store.get(key).unwrap().as_deref() == Some(&b"winner"[..]))
        .count();
    assert_eq!(present, 1);
}

#[test]
fn concurrent_logical_quota_races_reject_only_the_unsubmitted_transaction() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("ordinary");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"yes").unwrap();
    drop(store);
    let store = Store::open_with_limits(&path, None, Some(2)).unwrap();
    race_for_one_remaining_key(&store);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        [b"first".as_slice(), b"second".as_slice()]
            .iter()
            .filter(|key| reopened.get(key).unwrap().is_some())
            .count(),
        1
    );

    let path = root.path().join("checkpoint-store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"yes").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store =
        CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 1024, 1, 64).unwrap();
    race_for_one_remaining_key(&store);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        [b"first".as_slice(), b"second".as_slice()]
            .iter()
            .filter(|key| reopened.get(key).unwrap().is_some())
            .count(),
        1
    );
}
