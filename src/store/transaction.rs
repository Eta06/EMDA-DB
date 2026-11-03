//! Optimistic, serializable point transactions over a bounded index snapshot.
use super::*;
use crate::record::batch::{
    BatchOperation, MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS, encode_payload,
};

/// A process-local point transaction. Changes are staged until the callback
/// succeeds and commit validation completes; dropping it never commits.
///
/// Reads see the initial snapshot plus the transaction's own writes. The
/// engine validates a store-wide generation, so any concurrent successful
/// mutation causes a conservative conflict, even on unrelated keys. There is
/// no automatic retry, and external side effects are not rolled back.
pub struct Transaction<'a> {
    snapshot: Snapshot<'a>,
    staged: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    encoded_bytes: usize,
}

impl Transaction<'_> {
    /// Reads from staged changes first, then from the initial snapshot.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        self.snapshot.ensure_healthy()?;
        if let Some(value) = self.staged.get(key) {
            return Ok(value.clone());
        }
        self.snapshot.get(key)
    }

    /// Stages a replacement. It is not visible to other readers before commit.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.stage(Operation::Put, key, value)
    }

    /// Stages a deletion. It is not visible to other readers before commit.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        self.stage(Operation::Delete, key, &[])
    }

    /// Scans the initial snapshot merged with this transaction's staged writes.
    /// Bounds are exclusive. Returned pages have the same count and byte limits
    /// as `Store::scan`; continue until empty. A later mutation by any writer
    /// causes validation to conflict rather than allowing a phantom at commit.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        for key in [after, before].into_iter().flatten() {
            record::validate_key(key)?;
        }
        if limit == 0 || limit > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scan limit must be between 1 and 1024",
            ));
        }
        self.snapshot.ensure_healthy()?;
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(Vec::new());
        }
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut base = self.snapshot.index.range::<[u8], _>(bounds).peekable();
        let mut pending = self.staged.range::<[u8], _>(bounds).peekable();
        let mut result = Vec::new();
        let mut bytes = 0_usize;
        while result.len() < limit {
            self.snapshot.ensure_healthy()?;
            let choice = match (base.peek(), pending.peek()) {
                (None, None) => break,
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some((base_key, _)), Some((pending_key, _))) => base_key.cmp(pending_key),
            };
            if choice == std::cmp::Ordering::Less {
                let (key, location) = base.next().expect("peeked base entry");
                let size = key.len() + location.length;
                if bytes + size > MAX_SCAN_BYTES {
                    break;
                }
                let value = self
                    .snapshot
                    .get(key)?
                    .ok_or_else(|| io::Error::other("snapshot index changed unexpectedly"))?;
                bytes += size;
                result.push((key.clone(), value));
            } else {
                if choice == std::cmp::Ordering::Equal {
                    base.next();
                }
                let (key, value) = pending.next().expect("peeked staged entry");
                if let Some(value) = value {
                    let size = key.len() + value.len();
                    if bytes + size > MAX_SCAN_BYTES {
                        break;
                    }
                    bytes += size;
                    result.push((key.clone(), value.clone()));
                }
            }
        }
        Ok(result)
    }

    fn stage(&mut self, operation: Operation, key: &[u8], value: &[u8]) -> io::Result<()> {
        record::validate(operation, key, value)?;
        self.snapshot.ensure_healthy()?;
        let previous = self.staged.get(key);
        if previous.is_none() && self.staged.len() >= MAX_ATOMIC_BATCH_OPERATIONS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction write count limit exceeded",
            ));
        }
        let old_size = previous.map_or(0, |old| {
            record::HEADER_SIZE
                + key.len()
                + old.as_ref().map_or(0, Vec::len)
                + record::CHECKSUM_SIZE
        });
        let new_size = record::HEADER_SIZE + key.len() + value.len() + record::CHECKSUM_SIZE;
        let total = self.encoded_bytes - old_size + new_size;
        if total > MAX_ATOMIC_BATCH_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction encoded write limit exceeded",
            ));
        }
        let replacement = if operation == Operation::Put {
            Some(value.to_vec())
        } else {
            None
        };
        self.staged.insert(key.to_vec(), replacement);
        self.encoded_bytes = total;
        Ok(())
    }
}

impl Store {
    /// Runs an optimistic serializable point transaction on this store.
    ///
    /// The callback runs without holding the writer barrier, on a bounded
    /// initial snapshot. If *any* other mutation publishes before validation,
    /// this call returns `WouldBlock` without writing the staged changes. The
    /// caller may retry, but the callback is never automatically rerun. A
    /// callback error/panic discards staged changes; external side effects are
    /// not rolled back. Use the transaction's methods for transactional reads.
    ///
    /// Commit uses one atomic DB02 batch and requires a witnessed V2/V3 store.
    /// A commit I/O error has an unknown outcome. Read-only callbacks do not
    /// create a log record. The initial snapshot currently requires the store
    /// to fit the public per-snapshot limits; aggregate snapshot limits apply.
    pub fn transaction<T>(
        &self,
        action: impl FnOnce(&mut Transaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        let snapshot = self.snapshot(MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_KEY_BYTES)?;
        let mut txn = Transaction {
            snapshot,
            staged: BTreeMap::new(),
            encoded_bytes: 4,
        };
        let result = action(&mut txn)?;
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        if self.read()?.generation != txn.snapshot.generation {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "transaction conflict: store changed since snapshot",
            ));
        }
        if !txn.staged.is_empty() {
            // Deletions first avoid a transient quota overshoot when a
            // transaction atomically replaces one key with another.
            let mut operations = Vec::with_capacity(txn.staged.len());
            for (key, value) in &txn.staged {
                if value.is_none() {
                    operations.push(BatchOperation::Delete { key });
                }
            }
            for (key, value) in &txn.staged {
                if let Some(value) = value {
                    operations.push(BatchOperation::Put { key, value });
                }
            }
            let payload = encode_payload(&operations)?;
            self.write_batch_locked(&operations, &payload)?;
        }
        Ok(result)
    }
}
