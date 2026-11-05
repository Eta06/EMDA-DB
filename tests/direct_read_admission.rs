//! Independent read admission does not constrain writes or transactions.
use database_engine::{
    CheckpointStore, CollectionSchema, DirectReadStatus, DocumentDatabase, MAX_ACTIVE_DIRECT_READS,
    Store,
};
use serde_json::json;
use std::io::{self, ErrorKind};

mod support;
use support::retry_lock;

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn enable(&mut self, n: usize) -> io::Result<()> {
        match self {
            Self::Ordinary(s) => s.enable_direct_read_limit(n),
            Self::Checkpoint(s) => s.enable_direct_read_limit(n),
        }
    }
    fn status(&self) -> DirectReadStatus {
        match self {
            Self::Ordinary(s) => s.direct_read_status(),
            Self::Checkpoint(s) => s.direct_read_status(),
        }
    }
    fn set(&self, n: usize) -> io::Result<()> {
        match self {
            Self::Ordinary(s) => s.set_active_direct_read_limit(n),
            Self::Checkpoint(s) => s.set_active_direct_read_limit(n),
        }
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(s) => s.get(key),
            Self::Checkpoint(s) => s.get(key),
        }
    }
    fn get_many(&self, keys: &[&[u8]]) -> io::Result<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Ordinary(s) => s.get_many(keys),
            Self::Checkpoint(s) => s.get_many(keys),
        }
    }
    fn scan(&self, limit: usize) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::Ordinary(s) => s.scan(None, None, limit),
            Self::Checkpoint(s) => s.scan(None, None, limit),
        }
    }
    fn compare(&self) -> io::Result<bool> {
        match self {
            Self::Ordinary(s) => s.compare_exchange(b"key", Some(b"old"), Some(b"new")),
            Self::Checkpoint(s) => s.compare_exchange(b"key", Some(b"old"), Some(b"new")),
        }
    }
    fn increment(&self) -> io::Result<i64> {
        match self {
            Self::Ordinary(s) => s.increment_i64(b"counter", 1),
            Self::Checkpoint(s) => s.increment_i64(b"counter", 1),
        }
    }
    fn db(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(s) => DocumentDatabase::new(s),
            Self::Checkpoint(s) => DocumentDatabase::with_checkpoint_store(s),
        }
    }
    fn snapshot_get(&self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(s) => s.snapshot(64, 4096)?.get(b"key"),
            Self::Checkpoint(s) => s.snapshot()?.get(b"key"),
        }
    }
}

#[test]
fn zero_blocks_direct_reads_but_not_writes_transactions_snapshots_or_documents() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let source = Store::open(&path).unwrap();
        source.put(b"key", b"old").unwrap();
        let db = DocumentDatabase::new(&source);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "doc", &json!({"n":1})).unwrap();
        let digest = source.checkpoint_to(&checkpoint).unwrap();
        let mut store = if checkpoint_mode {
            drop(source);
            Backend::Checkpoint(
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap(),
            )
        } else {
            Backend::Ordinary(source)
        };
        assert_eq!((store.status().limit, store.status().active), (None, None));
        assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"old"[..]));
        assert_eq!((store.status().limit, store.status().active), (None, None));
        assert_eq!(store.set(0).unwrap_err().kind(), ErrorKind::InvalidInput);
        assert_eq!(
            store
                .enable(MAX_ACTIVE_DIRECT_READS + 1)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!((store.status().limit, store.status().active), (None, None));
        store.enable(0).unwrap();
        assert_eq!(store.get(b"").unwrap_err().kind(), ErrorKind::InvalidInput);
        assert_eq!(
            store.get_many(&[b""]).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(store.scan(0).unwrap_err().kind(), ErrorKind::InvalidInput);
        assert_eq!(store.get(b"key").unwrap_err().kind(), ErrorKind::WouldBlock);
        assert_eq!(
            store.get_many(&[b"key"]).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
        assert_eq!(store.scan(1).unwrap_err().kind(), ErrorKind::WouldBlock);
        assert_eq!(store.snapshot_get().unwrap().as_deref(), Some(&b"old"[..]));
        assert_eq!(store.db().get("c", "doc").unwrap(), Some(json!({"n":1})));
        assert_eq!(
            store
                .db()
                .transaction(|tx| tx.get_raw(b"key"))
                .unwrap()
                .as_deref(),
            Some(&b"old"[..])
        );
        assert!(store.compare().unwrap());
        assert_eq!(store.increment().unwrap(), 1);
        assert_eq!(
            (store.status().limit, store.status().active),
            (Some(0), Some(0))
        );
        store.set(1).unwrap();
        assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(
            store.get_many(&[b"key", b"missing"]).unwrap(),
            vec![Some(b"new".to_vec()), None]
        );
        assert!(!store.scan(10).unwrap().is_empty());
        assert_eq!(store.status().active, Some(0));
        drop(store);
        let recovered = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(recovered.direct_read_status().limit, None);
        assert_eq!(recovered.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(
            recovered.get(b"counter").unwrap().as_deref(),
            Some(&1_i64.to_le_bytes()[..])
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn blocking_compaction_preserves_the_limit_and_reopen_resets_it() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("live");
        let previous = root.path().join("previous");
        let index = root.path().join("index");
        let mut source = Store::open(&path).unwrap();
        source.put(b"key", b"value").unwrap();
        if checkpoint_mode {
            let digest = source.checkpoint_to(&index).unwrap();
            drop(source);
            let mut store = retry_lock(|| CheckpointStore::open(&path, &index, &digest)).unwrap();
            store.enable_direct_read_limit(0).unwrap();
            let (store, digest) = store.compact_and_replace(&previous).unwrap();
            assert_eq!(store.direct_read_status().limit, Some(0));
            assert_eq!(store.get(b"key").unwrap_err().kind(), ErrorKind::WouldBlock);
            drop(store);
            let reopened = retry_lock(|| {
                CheckpointStore::open(
                    &path,
                    path.join(database_engine::COMPACTED_CHECKPOINT_FILE),
                    &digest,
                )
            })
            .unwrap();
            assert_eq!(reopened.direct_read_status().limit, None);
            assert_eq!(
                reopened.get(b"key").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        } else {
            source.enable_direct_read_limit(0).unwrap();
            let store = source.compact_and_replace(&path, &previous).unwrap();
            assert_eq!(store.direct_read_status().limit, Some(0));
            assert_eq!(store.get(b"key").unwrap_err().kind(), ErrorKind::WouldBlock);
            drop(store);
            let reopened = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(reopened.direct_read_status().limit, None);
            assert_eq!(
                reopened.get(b"key").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        }
    }
}
