//! Transaction adapters for both writable paths.
use super::*;
use database_engine::{CheckpointTransaction, Transaction};

trait TransactionOps {
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

fn stage_increment(tx: &mut dyn TransactionOps, key: &[u8]) -> io::Result<u64> {
    let next = number(tx.get(COUNTER)?)?
        .checked_add(1)
        .ok_or_else(|| io::Error::other("transaction counter overflow"))?;
    tx.put(COUNTER, &next.to_le_bytes())?;
    std::thread::yield_now();
    tx.put(key, &next.to_le_bytes())?;
    Ok(next)
}

pub(super) trait TransactionStore: TestStore {
    fn increment(&self, worker: usize, sequence: usize) -> io::Result<u64>;
    fn block_after_staging(&self, ready: mpsc::Sender<()>) -> io::Result<()>;
}

impl TransactionStore for Store {
    fn increment(&self, worker: usize, sequence: usize) -> io::Result<u64> {
        let key = marker_key(worker, sequence);
        match worker % 3 {
            0 => self.transaction(|tx| stage_increment(tx, &key)),
            1 => self.transaction_with_read_set(|tx| stage_increment(tx, &key)),
            _ => self.transaction_with_read_set_fifo(|tx| stage_increment(tx, &key)),
        }
    }

    fn block_after_staging(&self, ready: mpsc::Sender<()>) -> io::Result<()> {
        self.transaction_with_read_set(|tx| {
            stage_increment(tx, BLOCKED)?;
            ready.send(()).map_err(io::Error::other)?;
            loop {
                std::thread::park();
            }
        })
    }
}

impl TransactionStore for CheckpointStore {
    fn increment(&self, worker: usize, sequence: usize) -> io::Result<u64> {
        let key = marker_key(worker, sequence);
        match worker {
            0 => self.transaction(|tx| stage_increment(tx, &key)),
            1 => self.transaction_with_read_set(|tx| stage_increment(tx, &key)),
            2 => self.transaction_with_read_set_fifo(|tx| stage_increment(tx, &key)),
            _ => self.transaction_with_fifo(|tx| stage_increment(tx, &key)),
        }
    }

    fn block_after_staging(&self, ready: mpsc::Sender<()>) -> io::Result<()> {
        self.transaction_with_read_set(|tx| {
            stage_increment(tx, BLOCKED)?;
            ready.send(()).map_err(io::Error::other)?;
            loop {
                std::thread::park();
            }
        })
    }
}
