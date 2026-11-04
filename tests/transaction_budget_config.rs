use database_engine::{
    COMPACTED_CHECKPOINT_FILE, CancellationToken, CheckpointStore, ControlledTransactionError,
    DocumentDatabase, DocumentTransactionMode, MAX_TRANSACTION_STAGED_TOTAL_BYTES, OnlineStore,
    Store, TransactionBudgetStatus,
};
use std::{
    cell::Cell,
    io::{self, ErrorKind},
    path::Path,
    sync::{Arc, Barrier, Condvar, Mutex, mpsc},
    thread,
    time::Duration,
};

mod support;
use support::retry_lock;

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn db(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }
    fn set(&self, limit: usize) -> io::Result<()> {
        match self {
            Self::Ordinary(store) => store.set_transaction_staging_limit(limit),
            Self::Checkpoint(store) => store.set_transaction_staging_limit(limit),
        }
    }
    fn status(&self) -> io::Result<TransactionBudgetStatus> {
        match self {
            Self::Ordinary(store) => store.transaction_budget_status(),
            Self::Checkpoint(store) => store.transaction_budget_status(),
        }
    }
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        match self {
            Self::Ordinary(store) => store.put(key, value),
            Self::Checkpoint(store) => store.put(key, value),
        }
    }
}
fn open(root: &Path, checkpoint_mode: bool) -> Backend {
    let source = root.join("store");
    let checkpoint = root.join("checkpoint");
    let store = Store::open(&source).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    if checkpoint_mode {
        drop(store);
        Backend::Checkpoint(
            retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap(),
        )
    } else {
        Backend::Ordinary(store)
    }
}

#[test]
fn configurable_limits_are_atomic_with_active_reservations_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(open(root.path(), checkpoint_mode));
        assert_eq!(
            backend.status().unwrap().limit_bytes,
            MAX_TRANSACTION_STAGED_TOTAL_BYTES
        );
        assert_eq!(
            backend
                .set(MAX_TRANSACTION_STAGED_TOTAL_BYTES + 1)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let other = Arc::clone(&backend);
        let writer = thread::spawn(move || {
            other.db().transaction(|tx| {
                tx.put_raw(b"held", &[7; 128])?;
                ready_tx.send(other.status()?.reserved_bytes).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
        });
        let reserved = ready_rx.recv().unwrap();
        assert!(reserved >= 128);
        assert_eq!(
            backend.set(reserved - 1).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
        assert_eq!(
            backend.status().unwrap().limit_bytes,
            MAX_TRANSACTION_STAGED_TOTAL_BYTES
        );
        backend.set(reserved).unwrap();
        assert!(
            backend
                .db()
                .transaction(|tx| tx.put_raw(b"extra", b"forbidden"))
                .is_err()
        );
        assert_eq!(backend.status().unwrap().reserved_bytes, reserved);
        release_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        assert_eq!(backend.status().unwrap().reserved_bytes, 0);
        backend.set(0).unwrap();
        assert!(
            backend
                .db()
                .transaction(|tx| tx.put_raw(b"blocked", b"value"))
                .is_err()
        );
        assert_eq!(
            backend.db().transaction(|tx| tx.get_raw(b"held")).unwrap(),
            Some(vec![7; 128])
        );
        assert_eq!(
            backend
                .db()
                .transaction(|tx| tx.get_raw(b"blocked"))
                .unwrap(),
            None
        );
        backend.put(b"direct", b"still allowed").unwrap();
        assert_eq!(
            backend
                .db()
                .transaction(|tx| tx.get_raw(b"direct"))
                .unwrap(),
            Some(b"still allowed".to_vec())
        );
        backend.set(MAX_TRANSACTION_STAGED_TOTAL_BYTES).unwrap();
    }
}

#[test]
fn many_concurrent_transactions_never_oversubscribe_the_limit() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(open(root.path(), checkpoint_mode));
        backend.set(256).unwrap();
        let start = Arc::new(Barrier::new(9));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (sender, receiver) = mpsc::channel();
        let mut threads = Vec::new();
        for id in 0..8_u8 {
            let backend = Arc::clone(&backend);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            let sender = sender.clone();
            threads.push(thread::spawn(move || {
                start.wait();
                let announced = Cell::new(false);
                let result = backend.db().transaction(|tx| {
                    if let Err(error) = tx.put_raw(&[b'k', id], &[id; 128]) {
                        announced.set(true);
                        sender.send(false).unwrap();
                        return Err(error);
                    }
                    announced.set(true);
                    sender.send(true).unwrap();
                    let (lock, wake) = &*release;
                    let mut ready = lock.lock().unwrap();
                    while !*ready {
                        ready = wake.wait(ready).unwrap();
                    }
                    Ok(())
                });
                if !announced.get() {
                    sender.send(false).unwrap();
                }
                (id, result.is_ok())
            }));
        }
        drop(sender);
        start.wait();
        let announcements = (0..8)
            .map(|_| receiver.recv_timeout(Duration::from_secs(10)))
            .collect::<Result<Vec<_>, _>>();
        let status = backend.status().unwrap();
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let outcomes = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        let accepted = announcements
            .unwrap()
            .into_iter()
            .filter(|accepted| *accepted)
            .count();
        assert!((1..8).contains(&accepted));
        assert!(status.reserved_bytes > 0 && status.reserved_bytes <= status.limit_bytes);
        assert_eq!(backend.status().unwrap().reserved_bytes, 0);
        assert!(outcomes.iter().any(|(_, committed)| *committed));
        for (id, committed) in outcomes {
            let actual = backend
                .db()
                .transaction(|tx| tx.get_raw(&[b'k', id]))
                .unwrap();
            assert_eq!(actual, committed.then(|| vec![id; 128]));
        }
    }
}

#[test]
fn reservations_release_after_a_transaction_unwinds() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let backend = open(root.path(), checkpoint_mode);
        backend.set(1024).unwrap();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: io::Result<()> = backend.db().transaction(|tx| {
                tx.put_raw(b"key", b"value")?;
                panic!("injected callback panic")
            });
        }));
        assert!(outcome.is_err());
        assert_eq!(backend.status().unwrap().reserved_bytes, 0);
        backend.set(0).unwrap();
        assert_eq!(
            backend.db().transaction(|tx| tx.get_raw(b"key")).unwrap(),
            None
        );
    }
}

#[test]
fn cancellation_releases_the_configured_budget_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let backend = open(root.path(), checkpoint_mode);
        backend.set(1024).unwrap();
        let token = CancellationToken::default();
        let result = backend.db().transaction_controlled(
            &token,
            None,
            DocumentTransactionMode::Conservative,
            |tx| {
                tx.put_raw(b"cancelled", b"value")?;
                assert!(backend.status()?.reserved_bytes > 0);
                token.cancel();
                Ok(())
            },
        );
        assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
        assert_eq!(backend.status().unwrap().reserved_bytes, 0);
        assert_eq!(
            backend
                .db()
                .transaction(|tx| tx.get_raw(b"cancelled"))
                .unwrap(),
            None
        );
    }
}

#[test]
fn checkpoint_read_set_reservations_obey_the_same_configured_budget() {
    let root = tempfile::tempdir().unwrap();
    let backend = open(root.path(), true);
    let Backend::Checkpoint(store) = backend else {
        unreachable!()
    };
    store.put(b"observed", b"value").unwrap();
    store.set_transaction_staging_limit(0).unwrap();
    assert!(
        store
            .transaction_with_read_set(|tx| tx.get(b"observed"))
            .is_err()
    );
    assert_eq!(store.transaction_budget_status().unwrap().reserved_bytes, 0);
    store.set_transaction_staging_limit(1024).unwrap();
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"observed")?, Some(b"value".to_vec()));
            let reserved = store.transaction_budget_status()?.reserved_bytes;
            assert!(reserved > 0);
            assert_eq!(tx.get(b"observed")?, Some(b"value".to_vec()));
            assert_eq!(store.transaction_budget_status()?.reserved_bytes, reserved);
            assert_eq!(
                store
                    .set_transaction_staging_limit(reserved - 1)
                    .unwrap_err()
                    .kind(),
                ErrorKind::WouldBlock
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(store.transaction_budget_status().unwrap().reserved_bytes, 0);
}

#[test]
#[cfg(target_os = "linux")]
fn limit_survives_ordinary_and_checkpoint_compaction_but_not_reopen() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let previous = root.path().join("previous");
        let store = Store::open(&source).unwrap();
        let checkpoint = root.path().join("checkpoint");
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            store.set_transaction_staging_limit(512).unwrap();
            store.put(b"key", b"value").unwrap();
            let (store, digest) = store.compact_and_replace(&previous).unwrap();
            assert_eq!(store.transaction_budget_status().unwrap().limit_bytes, 512);
            assert_eq!(store.get(b"key").unwrap(), Some(b"value".to_vec()));
            drop(store);
            let reopened = retry_lock(|| {
                CheckpointStore::open(&source, source.join(COMPACTED_CHECKPOINT_FILE), &digest)
            })
            .unwrap();
            assert_eq!(
                reopened.transaction_budget_status().unwrap().limit_bytes,
                MAX_TRANSACTION_STAGED_TOTAL_BYTES
            );
        } else {
            store.set_transaction_staging_limit(512).unwrap();
            store.put(b"key", b"value").unwrap();
            let store = store.compact_and_replace(&source, &previous).unwrap();
            assert_eq!(store.transaction_budget_status().unwrap().limit_bytes, 512);
            assert_eq!(store.get(b"key").unwrap(), Some(b"value".to_vec()));
            drop(store);
            let reopened = retry_lock(|| Store::open(&source)).unwrap();
            assert_eq!(
                reopened.transaction_budget_status().unwrap().limit_bytes,
                MAX_TRANSACTION_STAGED_TOTAL_BYTES
            );
        }
    }
}

#[test]
#[cfg(target_os = "linux")]
fn online_handoff_preserves_the_latest_transaction_limit() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let previous = root.path().join("previous");
    let store = OnlineStore::open(&source).unwrap();
    store
        .with_store(|db| {
            db.set_transaction_staging_limit(512)?;
            db.put(b"key", b"value")
        })
        .unwrap();
    store
        .compact_and_reclaim(&previous, &Default::default())
        .unwrap();
    store
        .with_store(|db| {
            assert_eq!(db.transaction_budget_status()?.limit_bytes, 512);
            assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
            Ok(())
        })
        .unwrap();
}
