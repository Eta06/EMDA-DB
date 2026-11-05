#![no_main]
use database_engine::{
    BatchOperation, CheckpointStore, DirectReadStatus, DirectWriteStatus, DocumentDatabase,
    MAX_ACTIVE_DIRECT_READS, MAX_ACTIVE_DIRECT_WRITES, Store,
};
use libfuzzer_sys::fuzz_target;
use std::{
    collections::BTreeMap,
    io::{self, ErrorKind},
    path::Path,
};

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn read_status(&self) -> DirectReadStatus {
        match self {
            Self::Ordinary(s) => s.direct_read_status(),
            Self::Checkpoint(s) => s.direct_read_status(),
        }
    }
    fn set_read(&self, limit: usize) -> io::Result<()> {
        match self {
            Self::Ordinary(s) => s.set_active_direct_read_limit(limit),
            Self::Checkpoint(s) => s.set_active_direct_read_limit(limit),
        }
    }
    fn get_many(&self, keys: &[&[u8]]) -> io::Result<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Ordinary(s) => s.get_many(keys),
            Self::Checkpoint(s) => s.get_many(keys),
        }
    }
    fn scan(&self) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::Ordinary(s) => s.scan(None, None, 16),
            Self::Checkpoint(s) => s.scan(None, None, 16),
        }
    }
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
    fn batch(&self, key: &[u8], value: &[u8], other: &[u8]) -> io::Result<()> {
        let batch = [
            BatchOperation::Put { key, value },
            BatchOperation::Delete { key: other },
        ];
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
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], checkpoint_mode: bool) -> Backend {
    if checkpoint_mode {
        Backend::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(path).unwrap())
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let source = Store::open(&path).unwrap();
    let digest = source.checkpoint_to(&checkpoint).unwrap();
    drop(source);
    let checkpoint_mode = data[0] & 1 != 0;
    let mut store = open(&path, &checkpoint, &digest, checkpoint_mode);
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut limit = MAX_ACTIVE_DIRECT_WRITES;
    let mut read_limit = MAX_ACTIVE_DIRECT_READS;
    for chunk in data[1..].chunks(3) {
        let flag = chunk[0];
        let key = vec![b'k', chunk.get(1).copied().unwrap_or(0) % 4];
        let other = vec![b'k', (key[1] + 1) % 4];
        let value = vec![flag; usize::from(chunk.get(2).copied().unwrap_or(0) % 32)];
        match flag % 10 {
            0 => {
                limit = if flag & 8 == 0 { 0 } else { 1 };
                store.set(limit).unwrap();
            }
            1 => {
                let before = store.status();
                assert_eq!(
                    store.set(MAX_ACTIVE_DIRECT_WRITES + 1).unwrap_err().kind(),
                    ErrorKind::InvalidInput
                );
                assert_eq!(store.status(), before);
            }
            2 => {
                let result = store.put(&key, &value);
                assert_eq!(result.is_ok(), limit > 0);
                if limit > 0 {
                    model.insert(key, value);
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            3 => {
                let result = store.delete(&key);
                assert_eq!(result.is_ok(), limit > 0);
                if limit > 0 {
                    model.remove(&key);
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            4 => {
                let result = store.batch(&key, &value, &other);
                assert_eq!(result.is_ok(), limit > 0);
                if limit > 0 {
                    model.insert(key, value);
                    model.remove(&other);
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            5 => {
                store
                    .db()
                    .transaction(|tx| {
                        tx.put_raw(&key, &value)?;
                        tx.delete_raw(&other)
                    })
                    .unwrap();
                model.insert(key, value);
                model.remove(&other);
            }
            6 => {
                drop(store);
                store = open(&path, &checkpoint, &digest, checkpoint_mode);
                limit = MAX_ACTIVE_DIRECT_WRITES;
                read_limit = MAX_ACTIVE_DIRECT_READS;
            }
            7 => {
                read_limit = if flag & 16 == 0 { 0 } else { 1 };
                store.set_read(read_limit).unwrap();
            }
            8 => {
                let before = store.read_status();
                assert_eq!(
                    store
                        .set_read(MAX_ACTIVE_DIRECT_READS + 1)
                        .unwrap_err()
                        .kind(),
                    ErrorKind::InvalidInput
                );
                assert_eq!(store.read_status(), before);
            }
            _ => {
                let many = store.get_many(&[&key, &other]);
                let scan = store.scan();
                if read_limit == 0 {
                    assert_eq!(many.unwrap_err().kind(), ErrorKind::WouldBlock);
                    assert_eq!(scan.unwrap_err().kind(), ErrorKind::WouldBlock);
                } else {
                    assert_eq!(
                        many.unwrap(),
                        vec![model.get(&key).cloned(), model.get(&other).cloned()]
                    );
                    assert_eq!(
                        scan.unwrap(),
                        model
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
        let status = store.status();
        assert_eq!((status.limit, status.active), (limit, 0));
        let status = store.read_status();
        assert_eq!((status.limit, status.active), (read_limit, 0));
        for n in 0..4 {
            let key = vec![b'k', n];
            if read_limit == 0 {
                assert_eq!(store.get(&key).unwrap_err().kind(), ErrorKind::WouldBlock);
            } else {
                assert_eq!(store.get(&key).unwrap(), model.get(&key).cloned());
            }
            assert_eq!(
                store.db().transaction(|tx| tx.get_raw(&key)).unwrap(),
                model.get(&key).cloned()
            );
        }
    }
    drop(store);
    let recovered = Store::open(&path).unwrap();
    for n in 0..4 {
        let key = vec![b'k', n];
        assert_eq!(recovered.get(&key).unwrap(), model.get(&key).cloned());
    }
});
