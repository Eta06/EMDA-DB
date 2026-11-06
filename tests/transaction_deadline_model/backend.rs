//! Adapter for testing both writable transaction implementations.
use super::{MARKERS, State, retry_lock};
use database_engine::{
    BatchOperation, CheckpointStore, CheckpointTransaction, CheckpointTransactionMode, Store,
    TimedTransactionError, Transaction, TransactionMode,
};
use std::io;
use std::path::Path;
use std::time::Instant;

pub(super) trait TransactionOps {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()>;
}

impl TransactionOps for Transaction<'_> {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        Transaction::get(self, key)
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        Transaction::put(self, key, value)
    }
}

impl TransactionOps for CheckpointTransaction<'_> {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        CheckpointTransaction::get(self, key)
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        CheckpointTransaction::put(self, key, value)
    }
}

pub(super) enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl Backend {
    pub(super) fn open(
        path: &Path,
        checkpoint: &Path,
        digest: &[u8; 32],
        capped: bool,
        cp: bool,
    ) -> Self {
        if cp {
            Self::Checkpoint(
                retry_lock(|| {
                    if capped {
                        CheckpointStore::open_with_limits(
                            path,
                            checkpoint,
                            digest,
                            16 * 1024 * 1024,
                            6,
                            10,
                        )
                    } else {
                        CheckpointStore::open(path, checkpoint, digest)
                    }
                })
                .unwrap(),
            )
        } else {
            Self::Ordinary(
                retry_lock(|| {
                    if capped {
                        Store::open_with_resource_limits(
                            path,
                            Some(16 * 1024 * 1024),
                            Some(6),
                            Some(10),
                        )
                    } else {
                        Store::open(path)
                    }
                })
                .unwrap(),
            )
        }
    }

    pub(super) fn timed(
        &self,
        deadline: Instant,
        mode: usize,
        action: impl FnOnce(&mut dyn TransactionOps) -> io::Result<[u8; 2]>,
    ) -> Result<[u8; 2], TimedTransactionError> {
        match self {
            Self::Ordinary(store) => store.transaction_until(
                deadline,
                [
                    TransactionMode::Conservative,
                    TransactionMode::ReadSet,
                    TransactionMode::ReadSetFifo,
                ][mode % 3],
                |tx| action(tx),
            ),
            Self::Checkpoint(store) => store.transaction_until(
                deadline,
                [
                    CheckpointTransactionMode::Conservative,
                    CheckpointTransactionMode::Fifo,
                    CheckpointTransactionMode::ReadSet,
                    CheckpointTransactionMode::ReadSetFifo,
                ][mode % 4],
                |tx| action(tx),
            ),
        }
    }

    pub(super) fn batch(&self, operations: &[BatchOperation<'_>]) {
        match self {
            Self::Ordinary(store) => store.write_batch(operations),
            Self::Checkpoint(store) => store.write_batch(operations),
        }
        .unwrap();
    }

    pub(super) fn put(&self, key: &[u8], value: &[u8]) {
        match self {
            Self::Ordinary(store) => store.put(key, value),
            Self::Checkpoint(store) => store.put(key, value),
        }
        .unwrap();
    }

    pub(super) fn state(&self) -> State {
        let keys = [
            b"a".as_slice(),
            b"b".as_slice(),
            MARKERS[0],
            MARKERS[1],
            MARKERS[2],
            MARKERS[3],
        ];
        let values = match self {
            Self::Ordinary(store) => store.get_many(&keys),
            Self::Checkpoint(store) => store.get_many(&keys),
        }
        .unwrap();
        let value = |index: usize| {
            let bytes = values[index].as_ref().unwrap();
            assert_eq!(bytes.len(), 1);
            bytes[0]
        };
        let mut marks = 0;
        for index in 0..4 {
            if let Some(marker) = &values[index + 2] {
                assert_eq!(marker, &[1]);
                marks |= 1 << index;
            }
        }
        State {
            pair: [value(0), value(1)],
            marks,
        }
    }
}
