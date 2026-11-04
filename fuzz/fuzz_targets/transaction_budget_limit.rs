#![no_main]
use database_engine::{
    CancellationToken, CheckpointStore, ControlledTransactionError, DocumentDatabase,
    DocumentTransactionMode, MAX_ACTIVE_TRANSACTIONS, MAX_TRANSACTION_STAGED_TOTAL_BYTES, Store,
    TransactionBudgetStatus,
};
use libfuzzer_sys::fuzz_target;
use std::{collections::BTreeMap, io, path::Path};

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
    fn status(&self) -> TransactionBudgetStatus {
        match self {
            Self::Ordinary(store) => store.transaction_budget_status(),
            Self::Checkpoint(store) => store.transaction_budget_status(),
        }
        .unwrap()
    }
    fn set_active(&self, limit: usize) -> io::Result<()> {
        match self {
            Self::Ordinary(store) => store.set_active_transaction_limit(limit),
            Self::Checkpoint(store) => store.set_active_transaction_limit(limit),
        }
    }
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        match self {
            Self::Ordinary(store) => store.put(key, value),
            Self::Checkpoint(store) => store.put(key, value),
        }
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(store) => store.get(key),
            Self::Checkpoint(store) => store.get(key),
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
fn check(store: &Backend, model: &BTreeMap<u8, Vec<u8>>, limit: usize, active_limit: usize) {
    let status = store.status();
    assert_eq!((status.limit_bytes, status.reserved_bytes), (limit, 0));
    assert_eq!(
        (status.active_limit, status.active_transactions),
        (active_limit, 0)
    );
    for id in 0..8 {
        assert_eq!(store.get(&[id]).unwrap(), model.get(&id).cloned());
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let checkpoint_mode = data[0] & 1 != 0;
    let mut store = open(&path, &checkpoint, &digest, checkpoint_mode);
    let mut model = BTreeMap::<u8, Vec<u8>>::new();
    let mut limit = MAX_TRANSACTION_STAGED_TOTAL_BYTES;
    let mut active_limit = MAX_ACTIVE_TRANSACTIONS;
    for bytes in data[1..].chunks_exact(4) {
        let id = bytes[1] % 8;
        let value = vec![bytes[2]; usize::from(bytes[2] % 8)];
        match bytes[0] % 8 {
            0 => {
                let next = [
                    0,
                    16,
                    32,
                    64,
                    128,
                    512,
                    MAX_TRANSACTION_STAGED_TOTAL_BYTES,
                    MAX_TRANSACTION_STAGED_TOTAL_BYTES + 1,
                ][usize::from(bytes[3] % 8)];
                let result = store.set(next);
                if next > MAX_TRANSACTION_STAGED_TOTAL_BYTES {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
                } else {
                    result.unwrap();
                    limit = next;
                }
            }
            1 | 2 => {
                let result = store.db().transaction(|tx| {
                    assert_eq!(store.status().active_transactions, 1);
                    assert_eq!(
                        store.set_active(0).unwrap_err().kind(),
                        io::ErrorKind::WouldBlock
                    );
                    let nested = store.db().transaction(|_| {
                        assert_eq!(store.status().active_transactions, 2);
                        Ok(())
                    });
                    assert_eq!(nested.is_ok(), active_limit >= 2);
                    tx.put_raw(&[id], &value)?;
                    let state = store.status();
                    assert!(state.reserved_bytes <= state.limit_bytes);
                    Ok(())
                });
                if limit == 0 || active_limit == 0 {
                    assert!(result.is_err());
                }
                if limit >= 512 && active_limit > 0 {
                    assert!(result.is_ok());
                }
                if result.is_ok() {
                    model.insert(id, value);
                }
            }
            3 => {
                store.put(&[id], &value).unwrap();
                model.insert(id, value);
            }
            4 => {
                let result = store.db().transaction(|tx| tx.delete_raw(&[id]));
                if limit == 0 || active_limit == 0 {
                    assert!(result.is_err());
                }
                if limit >= 512 && active_limit > 0 {
                    assert!(result.is_ok());
                }
                if result.is_ok() {
                    model.remove(&id);
                }
            }
            5 => {
                let token = CancellationToken::default();
                let result = store.db().transaction_controlled(
                    &token,
                    None,
                    DocumentTransactionMode::Conservative,
                    |tx| {
                        tx.put_raw(&[id], &value)?;
                        token.cancel();
                        Ok(())
                    },
                );
                if limit >= 512 && active_limit > 0 {
                    assert!(matches!(result, Err(ControlledTransactionError::Cancelled)));
                } else {
                    assert!(result.is_err());
                }
            }
            6 => {
                drop(store);
                store = open(&path, &checkpoint, &digest, checkpoint_mode);
                limit = MAX_TRANSACTION_STAGED_TOTAL_BYTES;
                active_limit = MAX_ACTIVE_TRANSACTIONS;
            }
            _ => {
                let next = [
                    0,
                    1,
                    2,
                    MAX_ACTIVE_TRANSACTIONS,
                    MAX_ACTIVE_TRANSACTIONS + 1,
                ][usize::from(bytes[3] % 5)];
                let result = store.set_active(next);
                if next > MAX_ACTIVE_TRANSACTIONS {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
                } else {
                    result.unwrap();
                    active_limit = next;
                }
            }
        }
        check(&store, &model, limit, active_limit);
    }
});
