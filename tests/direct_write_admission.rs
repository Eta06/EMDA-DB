//! Independent write admission is separate from transactional admission.
use database_engine::{
    BatchOperation, CheckpointStore, DirectWriteStatus, DocumentDatabase, MAX_ACTIVE_DIRECT_WRITES,
    MAX_GROUP_COMMIT_BUFFERED_BYTES, MAX_GROUP_COMMIT_BUFFERED_RECORDS, Store,
};
use std::io::{self, ErrorKind};

mod support;
use support::retry_lock;

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn status(&self) -> DirectWriteStatus {
        match self {
            Self::Ordinary(s) => s.direct_write_status(),
            Self::Checkpoint(s) => s.direct_write_status(),
        }
    }
    fn set(&self, limit: usize) -> io::Result<()> {
        match self {
            Self::Ordinary(s) => s.set_active_direct_write_limit(limit),
            Self::Checkpoint(s) => s.set_active_direct_write_limit(limit),
        }
    }
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        match self {
            Self::Ordinary(s) => s.put(key, value),
            Self::Checkpoint(s) => s.put(key, value),
        }
    }
    fn delete(&self, key: &[u8]) -> io::Result<()> {
        match self {
            Self::Ordinary(s) => s.delete(key),
            Self::Checkpoint(s) => s.delete(key),
        }
    }
    fn compare(&self, key: &[u8]) -> io::Result<bool> {
        match self {
            Self::Ordinary(s) => s.compare_exchange(key, None, Some(b"value")),
            Self::Checkpoint(s) => s.compare_exchange(key, None, Some(b"value")),
        }
    }
    fn increment(&self, key: &[u8]) -> io::Result<i64> {
        match self {
            Self::Ordinary(s) => s.increment_i64(key, 1),
            Self::Checkpoint(s) => s.increment_i64(key, 1),
        }
    }
    fn batch(&self) -> io::Result<()> {
        let batch = [BatchOperation::Put {
            key: b"batch",
            value: b"value",
        }];
        match self {
            Self::Ordinary(s) => s.write_batch(&batch),
            Self::Checkpoint(s) => s.write_batch(&batch),
        }
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(s) => s.get(key),
            Self::Checkpoint(s) => s.get(key),
        }
    }
    fn db(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(s) => DocumentDatabase::new(s),
            Self::Checkpoint(s) => DocumentDatabase::with_checkpoint_store(s),
        }
    }
}

#[test]
fn zero_limit_rejects_all_direct_writes_but_not_transactions_or_reads() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("index");
        let source = Store::open(&path).unwrap();
        source.put(b"original", b"durable").unwrap();
        let digest = source.checkpoint_to(&checkpoint).unwrap();
        let backend = if checkpoint_mode {
            drop(source);
            Backend::Checkpoint(
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap(),
            )
        } else {
            Backend::Ordinary(source)
        };
        assert_eq!(backend.status().limit, MAX_ACTIVE_DIRECT_WRITES);
        assert_eq!(
            backend
                .set(MAX_ACTIVE_DIRECT_WRITES + 1)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        backend.set(0).unwrap();
        assert_eq!(
            backend.put(b"", b"invalid").unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        for error in [
            backend.put(b"put", b"value").unwrap_err(),
            backend.delete(b"original").unwrap_err(),
            backend.compare(b"compare").unwrap_err(),
            backend.increment(b"increment").unwrap_err(),
            backend.batch().unwrap_err(),
        ] {
            assert_eq!(error.kind(), ErrorKind::WouldBlock);
        }
        if let Backend::Checkpoint(store) = &backend {
            assert_eq!(
                store
                    .compare_exchange_batch(&[], &[BatchOperation::Delete { key: b"original" }])
                    .unwrap_err()
                    .kind(),
                ErrorKind::WouldBlock
            );
        }
        assert_eq!(backend.status().active, 0);
        assert_eq!(
            backend.get(b"original").unwrap().as_deref(),
            Some(&b"durable"[..])
        );
        backend
            .db()
            .transaction(|tx| tx.put_raw(b"transaction", b"allowed"))
            .unwrap();
        assert_eq!(
            backend.get(b"transaction").unwrap().as_deref(),
            Some(&b"allowed"[..])
        );
        backend.set(1).unwrap();
        backend.put(b"after", b"allowed").unwrap();
        assert_eq!(backend.status().active, 0);
        drop(backend);
        let recovered = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(
            recovered.direct_write_status().limit,
            MAX_ACTIVE_DIRECT_WRITES
        );
        assert_eq!(
            recovered.get(b"original").unwrap().as_deref(),
            Some(&b"durable"[..])
        );
        assert_eq!(
            recovered.get(b"transaction").unwrap().as_deref(),
            Some(&b"allowed"[..])
        );
        assert_eq!(
            recovered.get(b"after").unwrap().as_deref(),
            Some(&b"allowed"[..])
        );
    }
}

#[test]
fn rejection_by_group_commit_releases_direct_write_admission() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.set_active_direct_write_limit(1).unwrap();
    store.set_group_commit_limits(0, 0).unwrap();
    assert_eq!(
        store.put(b"key", b"value").unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    assert_eq!(store.direct_write_status().active, 0);
    store
        .set_group_commit_limits(
            MAX_GROUP_COMMIT_BUFFERED_BYTES,
            MAX_GROUP_COMMIT_BUFFERED_RECORDS,
        )
        .unwrap();
    store.put(b"key", b"value").unwrap();
    assert_eq!(store.direct_write_status().active, 0);
}

#[cfg(target_os = "linux")]
#[test]
fn both_blocking_compaction_handoffs_preserve_limits_and_reopen_resets() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("live");
        let previous = root.path().join("previous");
        let checkpoint = root.path().join("index");
        let source = Store::open(&path).unwrap();
        source.put(b"key", b"durable").unwrap();
        if checkpoint_mode {
            let digest = source.checkpoint_to(&checkpoint).unwrap();
            drop(source);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            store.set_active_direct_write_limit(0).unwrap();
            let (store, digest) = store.compact_and_replace(&previous).unwrap();
            assert_eq!(store.direct_write_status().limit, 0);
            assert_eq!(
                store.put(b"rejected", b"value").unwrap_err().kind(),
                ErrorKind::WouldBlock
            );
            assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"durable"[..]));
            drop(store);
            let index = path.join(database_engine::COMPACTED_CHECKPOINT_FILE);
            let recovered = retry_lock(|| CheckpointStore::open(&path, &index, &digest)).unwrap();
            assert_eq!(
                recovered.direct_write_status().limit,
                MAX_ACTIVE_DIRECT_WRITES
            );
            recovered.put(b"after", b"allowed").unwrap();
        } else {
            source.set_active_direct_write_limit(0).unwrap();
            let store = source.compact_and_replace(&path, &previous).unwrap();
            assert_eq!(store.direct_write_status().limit, 0);
            assert_eq!(
                store.put(b"rejected", b"value").unwrap_err().kind(),
                ErrorKind::WouldBlock
            );
            assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"durable"[..]));
            drop(store);
            let recovered = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                recovered.direct_write_status().limit,
                MAX_ACTIVE_DIRECT_WRITES
            );
            recovered.put(b"after", b"allowed").unwrap();
        }
    }
}
