//! Optimistic, serializable point transactions over a bounded index snapshot.
use super::*;
use crate::record::batch::{
    BatchOperation, MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS, encode_payload,
};

/// Maximum combined encoded staged-record bytes across transactions on one open store.
/// This does not include allocator overhead, snapshots, returned values, or commit buffers.
pub const MAX_TRANSACTION_STAGED_TOTAL_BYTES: usize = 64 * 1024 * 1024;

pub(super) struct TransactionBudget {
    used: usize,
    limit: usize,
}

impl Default for TransactionBudget {
    fn default() -> Self {
        Self {
            used: 0,
            limit: MAX_TRANSACTION_STAGED_TOTAL_BYTES,
        }
    }
}

impl TransactionBudget {
    fn reserve(&mut self, bytes: usize) -> io::Result<()> {
        if bytes > self.limit - self.used {
            return Err(io::Error::other(
                "aggregate transaction staged byte limit exceeded",
            ));
        }
        self.used += bytes;
        Ok(())
    }

    fn release(&mut self, bytes: usize) {
        self.used -= bytes;
    }
}

// Releases an incremental reservation if constructing a staged entry unwinds.
struct PendingReservation<'a> {
    budget: &'a Mutex<TransactionBudget>,
    bytes: usize,
}

impl<'a> PendingReservation<'a> {
    fn acquire(budget: &'a Mutex<TransactionBudget>, bytes: usize) -> io::Result<Self> {
        if bytes > 0 {
            budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .reserve(bytes)?;
        }
        Ok(Self { budget, bytes })
    }

    fn finish(&mut self) {
        self.bytes = 0;
    }
}

impl Drop for PendingReservation<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(self.bytes);
        }
    }
}

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

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Discard staged data before making its reservation available again.
        self.staged.clear();
        if self.encoded_bytes > 4 {
            self.snapshot
                .store
                .transaction_budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(self.encoded_bytes - 4);
        }
    }
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
        let increase = new_size.saturating_sub(old_size);
        let decrease = old_size.saturating_sub(new_size);
        let mut reservation =
            PendingReservation::acquire(&self.snapshot.store.transaction_budget, increase)?;
        let replacement = if operation == Operation::Put {
            Some(value.to_vec())
        } else {
            None
        };
        self.staged.insert(key.to_vec(), replacement);
        if decrease > 0 {
            self.snapshot
                .store
                .transaction_budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(decrease);
        }
        self.encoded_bytes = total;
        reservation.finish();
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
    /// Across transactions on one store, encoded staged records also share
    /// `MAX_TRANSACTION_STAGED_TOTAL_BYTES`; rejection happens before staging.
    /// This logical byte budget is not a total process-memory limit.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn pending_reservation_is_released_during_unwind() {
        let budget = Mutex::new(TransactionBudget {
            used: 0,
            limit: 128,
        });
        let result = std::panic::catch_unwind(|| {
            let _reservation = PendingReservation::acquire(&budget, 100).unwrap();
            panic!("injected staging panic");
        });
        assert!(result.is_err());
        assert_eq!(budget.lock().unwrap().used, 0);
    }

    #[test]
    fn replaced_entries_release_budget_and_rejected_staging_preserves_old_changes() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.transaction_budget.lock().unwrap().limit = 128;
        let outer: io::Result<()> = store.transaction(|outer| {
            outer.put(b"a", &[1; 70])?;
            let rejected: io::Result<()> = store.transaction(|inner| {
                assert!(inner.put(b"b", &[1; 20]).is_err());
                assert_eq!(inner.get(b"b")?, None);
                Err(io::Error::other("abort nested transaction"))
            });
            assert!(rejected.is_err());
            assert!(outer.put(b"a", &[9; 120]).is_err());
            assert_eq!(outer.get(b"a")?.unwrap(), vec![1; 70]);
            outer.put(b"a", &[2; 10])?;
            assert_eq!(outer.get(b"a")?.unwrap(), vec![2; 10]);
            let staged = std::cell::Cell::new(false);
            let admitted: io::Result<()> = store.transaction(|inner| {
                inner.put(b"b", &[3; 70])?;
                staged.set(true);
                Err(io::Error::other("abort nested transaction"))
            });
            assert!(admitted.is_err());
            assert!(staged.get());
            assert_eq!(store.transaction_budget.lock().unwrap().used, 32);
            Err(io::Error::other("abort outer transaction"))
        });
        assert!(outer.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
        assert_eq!(store.get_many(&[b"a", b"b"]).unwrap(), vec![None, None]);
        store.transaction(|tx| tx.put(b"a", b"committed")).unwrap();
        assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    }

    #[test]
    fn simultaneous_staging_cannot_oversubscribe_the_shared_budget() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        // A staged 70-byte value with a one-byte key occupies 92 encoded bytes.
        store.transaction_budget.lock().unwrap().limit = 184;
        let winners = AtomicUsize::new(0);
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::scope(|scope| {
            let mut releases = Vec::new();
            let mut workers = Vec::new();
            for _ in 0..8 {
                let (release_tx, release_rx) = mpsc::channel();
                releases.push(release_tx);
                let (store, winners, ready_tx) = (&store, &winners, ready_tx.clone());
                workers.push(scope.spawn(move || {
                    let result: io::Result<()> = store.transaction(|tx| {
                        if tx.put(b"k", &[7; 70]).is_ok() {
                            winners.fetch_add(1, Ordering::SeqCst);
                        }
                        ready_tx.send(()).unwrap();
                        release_rx.recv_timeout(Duration::from_secs(20)).unwrap();
                        Err(io::Error::other("abort"))
                    });
                    assert!(result.is_err());
                }));
            }
            let ready = (0..8)
                .map(|_| ready_rx.recv_timeout(Duration::from_secs(20)))
                .collect::<Vec<_>>();
            let used = store.transaction_budget.lock().unwrap().used;
            for release in releases {
                let _ = release.send(());
            }
            for worker in workers {
                worker.join().unwrap();
            }
            assert!(ready.into_iter().all(|result| result.is_ok()));
            assert_eq!(winners.load(Ordering::SeqCst), 2);
            assert_eq!(used, 184);
        });
        assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
        assert_eq!(store.get(b"k").unwrap(), None);
    }

    #[test]
    fn callback_panic_releases_transaction_and_snapshot_reservations() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: io::Result<()> = store.transaction(|tx| {
                tx.put(b"a", &[4; 70])?;
                panic!("abort transaction callback");
            });
        }));
        assert!(result.is_err());
        assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
        store.transaction(|tx| tx.put(b"a", b"ok")).unwrap();
        assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"ok"[..]));
    }
}
