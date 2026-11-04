//! Optimistic, serializable point transactions over a bounded index snapshot.
use super::*;
use crate::record::batch::{
    BatchOperation, MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS, encode_payload,
};
pub use crate::transaction_budget::MAX_TRANSACTION_STAGED_TOTAL_BYTES;
use crate::transaction_budget::PendingReservation;
#[cfg(test)]
use crate::transaction_budget::TransactionBudget;
use std::fmt;
use std::time::Instant;

/// Validation and admission rules for a controlled transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMode {
    /// Conflict on any intervening mutation.
    Conservative,
    /// Validate observed points and complete requested ranges.
    ReadSet,
    /// Read-set validation with FIFO admission among participating callers.
    ReadSetFifo,
}

/// An engine-detected deadline expiry or another transaction error.
#[derive(Debug)]
pub enum TimedTransactionError {
    /// The engine detected expiry before starting this transaction's commit.
    DeadlineExceeded,
    /// A callback, validation, admission, or I/O error. A commit I/O error can
    /// have an unknown outcome, including an I/O error of kind `TimedOut`.
    Operation(io::Error),
}

impl fmt::Display for TimedTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded => {
                formatter.write_str("transaction deadline exceeded before commit")
            }
            Self::Operation(error) => write!(formatter, "transaction failed: {error}"),
        }
    }
}

impl std::error::Error for TimedTransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DeadlineExceeded => None,
            Self::Operation(error) => Some(error),
        }
    }
}

impl From<io::Error> for TimedTransactionError {
    fn from(error: io::Error) -> Self {
        Self::Operation(error)
    }
}

impl TimedTransactionError {
    pub(crate) fn into_io(self) -> io::Error {
        match self {
            Self::DeadlineExceeded => io::Error::new(io::ErrorKind::TimedOut, self),
            Self::Operation(error) => error,
        }
    }
}

fn check_deadline(deadline: Option<Instant>) -> Result<(), TimedTransactionError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(TimedTransactionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

/// A process-local point transaction. Changes are staged until the callback
/// succeeds and commit validation completes; dropping it never commits.
///
/// Reads see the initial snapshot plus the transaction's own writes. The
/// default mode conflicts on every intervening mutation; the opt-in read-set
/// mode validates observed points and ranges instead. There is no automatic
/// retry, and external side effects are not rolled back.
pub struct Transaction<'a> {
    snapshot: Snapshot<'a>,
    staged: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    encoded_bytes: usize,
    read_set: Option<Mutex<super::read_set::ReadSet>>,
    cancellation: Option<crate::CancellationToken>,
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
        crate::cancellation::check(self.cancellation.as_ref())?;
        record::validate_key(key)?;
        let snapshot = self.snapshot.read_index()?;
        if let Some(value) = self.staged.get(key) {
            return Ok(value.clone());
        }
        if let Some(read_set) = &self.read_set {
            read_set
                .lock()
                .map_err(|_| io::Error::other("transaction read set poisoned"))?
                .point(key)?;
        }
        snapshot
            .index
            .get(key)
            .copied()
            .map(|location| self.snapshot.store.log.read(location))
            .transpose()
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
    /// as `Store::scan`; continue until empty. Read-set mode validates the
    /// entire requested range, including entries beyond a limited page.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        crate::cancellation::check(self.cancellation.as_ref())?;
        for key in [after, before].into_iter().flatten() {
            record::validate_key(key)?;
        }
        if limit == 0 || limit > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scan limit must be between 1 and 1024",
            ));
        }
        let snapshot = self.snapshot.read_index()?;
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(Vec::new());
        }
        if let Some(read_set) = &self.read_set {
            read_set
                .lock()
                .map_err(|_| io::Error::other("transaction read set poisoned"))?
                .range(after, before)?;
        }
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut base = snapshot.index.range::<[u8], _>(bounds).peekable();
        let mut pending = self.staged.range::<[u8], _>(bounds).peekable();
        let mut result = Vec::new();
        let mut bytes = 0_usize;
        while result.len() < limit {
            if self.snapshot.store.commits.failed() {
                return Err(uncertain_error());
            }
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
                let value = self.snapshot.store.log.read(*location)?;
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
        crate::cancellation::check(self.cancellation.as_ref())?;
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
    /// Returns this open store's logical transaction reservation usage.
    pub fn transaction_budget_status(&self) -> io::Result<crate::TransactionBudgetStatus> {
        Ok(self
            .transaction_budget
            .lock()
            .map_err(|_| io::Error::other("transaction budget poisoned"))?
            .status())
    }

    /// Sets the aggregate staged-record reservation limit for this open store.
    /// Values above the hard maximum are invalid. Lowering the limit below
    /// current reservations returns `WouldBlock` without changing it. Zero is
    /// allowed and prevents nonempty transaction staging. This does not limit
    /// ordinary writes, snapshots, allocator overhead or total process memory.
    /// The limit is process-local and resets when the store is reopened.
    pub fn set_transaction_staging_limit(&self, limit: usize) -> io::Result<()> {
        self.transaction_budget
            .lock()
            .map_err(|_| io::Error::other("transaction budget poisoned"))?
            .set_limit(limit)
    }

    /// Sets the process-local active transaction limit for this open store.
    /// Admissions include queued calls, callbacks and commits. Reducing the
    /// limit below the active count returns `WouldBlock`; zero rejects all new
    /// transactions. Independent writes and snapshots are unaffected. This is
    /// not a total-memory or cross-store limit, and resets on reopen.
    pub fn set_active_transaction_limit(&self, limit: usize) -> io::Result<()> {
        self.transaction_budget
            .lock()
            .map_err(|_| io::Error::other("transaction budget poisoned"))?
            .set_active_limit(limit)
    }

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
        self.run_transaction(action, false, false, None, None)
            .map_err(TimedTransactionError::into_io)
    }

    /// Runs a serializable transaction validating its point and range reads.
    ///
    /// Unrelated intervening writes do not force a conflict. The validator
    /// compares immutable record locations and the complete requested ranges
    /// under the writer barrier. An absent key inserted and deleted before
    /// validation may return to the same state without a conflict. Callbacks
    /// are not retried and external side effects are not rolled back.
    /// Read dependencies have per-transaction count and byte limits.
    pub fn transaction_with_read_set<T>(
        &self,
        action: impl FnOnce(&mut Transaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.run_transaction(action, true, false, None, None)
            .map_err(TimedTransactionError::into_io)
    }

    /// Runs a read-set transaction with FIFO admission among calls to this method.
    ///
    /// The callback completes before admission. Other store operations and
    /// non-FIFO transactions do not enter this queue, and scheduling or I/O can
    /// delay a turn without bound. FIFO can increase median latency and retries;
    /// measure it for the actual workload. Isolation and commit rules are the
    /// same as `transaction_with_read_set`.
    pub fn transaction_with_read_set_fifo<T>(
        &self,
        action: impl FnOnce(&mut Transaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.run_transaction(action, true, true, None, None)
            .map_err(TimedTransactionError::into_io)
    }

    /// Runs a transaction with a monotonic, operation-boundary deadline.
    ///
    /// `DeadlineExceeded` means this transaction's staged changes were not
    /// submitted for commit. A callback or commit can instead return an
    /// `Operation` error, including an I/O timeout with an unknown outcome.
    /// Callback side effects and independent store operations are not rolled
    /// back. A callback that forwards an expired snapshot read returns that
    /// read error as `Operation`, not `DeadlineExceeded`.
    ///
    /// FIFO admission can time out while waiting in its queue. Callback work,
    /// ordinary lock waits and I/O are not forcibly interrupted. After commit
    /// starts, synchronization runs to completion and can return success after
    /// the deadline; an I/O failure must retain its unknown-outcome semantics.
    pub fn transaction_until<T>(
        &self,
        deadline: Instant,
        mode: TransactionMode,
        action: impl FnOnce(&mut Transaction<'_>) -> io::Result<T>,
    ) -> Result<T, TimedTransactionError> {
        let (track_reads, fifo) = match mode {
            TransactionMode::Conservative => (false, false),
            TransactionMode::ReadSet => (true, false),
            TransactionMode::ReadSetFifo => (true, true),
        };
        self.run_transaction(action, track_reads, fifo, Some(deadline), None)
    }

    /// Runs a transaction with cooperative cancellation and an optional
    /// monotonic deadline. `Cancelled` or `DeadlineExceeded` means staged
    /// writes were not submitted for commit. Cancellation is checked at
    /// operation boundaries and while waiting for FIFO admission; callbacks,
    /// writer-lock waits and I/O are not forcibly interrupted. Once commit
    /// begins, it may succeed after cancellation or return an operation error
    /// with an unknown outcome. Callback side effects are not rolled back.
    pub fn transaction_controlled<T>(
        &self,
        cancellation: &crate::CancellationToken,
        deadline: Option<Instant>,
        mode: TransactionMode,
        action: impl FnOnce(&mut Transaction<'_>) -> io::Result<T>,
    ) -> Result<T, crate::ControlledTransactionError> {
        let (track_reads, fifo) = match mode {
            TransactionMode::Conservative => (false, false),
            TransactionMode::ReadSet => (true, false),
            TransactionMode::ReadSetFifo => (true, true),
        };
        self.run_transaction(action, track_reads, fifo, deadline, Some(cancellation))
            .map_err(crate::cancellation::classify)
    }

    fn run_transaction<T>(
        &self,
        action: impl FnOnce(&mut Transaction<'_>) -> io::Result<T>,
        track_reads: bool,
        fifo: bool,
        deadline: Option<Instant>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<T, TimedTransactionError> {
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        let _admission =
            crate::transaction_budget::ActiveTransaction::acquire(&self.transaction_budget)?;
        let snapshot = if let Some(deadline) = deadline {
            self.snapshot_until(MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_KEY_BYTES, deadline)
                .map_err(|error| {
                    if error.kind() == io::ErrorKind::TimedOut {
                        TimedTransactionError::DeadlineExceeded
                    } else {
                        TimedTransactionError::Operation(error)
                    }
                })?
        } else {
            self.snapshot(MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_KEY_BYTES)?
        };
        crate::cancellation::check(cancellation)?;
        let mut txn = Transaction {
            snapshot,
            staged: BTreeMap::new(),
            encoded_bytes: 4,
            read_set: track_reads.then(|| Mutex::new(super::read_set::ReadSet::default())),
            cancellation: cancellation.cloned(),
        };
        let result = action(&mut txn)?;
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        // Opt-in FIFO admission does not replace the writer barrier protecting
        // validation, publication and synchronization.
        let _turn = if fifo {
            Some(if cancellation.is_some() {
                self.transaction_queue
                    .enter_controlled(deadline, cancellation)
                    .map_err(|error| {
                        if error.kind() == io::ErrorKind::TimedOut {
                            TimedTransactionError::DeadlineExceeded
                        } else {
                            TimedTransactionError::Operation(error)
                        }
                    })?
            } else if deadline.is_some() {
                self.transaction_queue
                    .enter_until(deadline)
                    .map_err(|error| {
                        if error.kind() == io::ErrorKind::TimedOut {
                            TimedTransactionError::DeadlineExceeded
                        } else {
                            TimedTransactionError::Operation(error)
                        }
                    })?
            } else {
                self.transaction_queue.enter()?
            })
        } else {
            None
        };
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        if self.commits.failed() {
            return Err(uncertain_error().into());
        }
        {
            let current = self.read()?;
            let valid = if let Some(read_set) = &txn.read_set {
                let read_set = read_set
                    .lock()
                    .map_err(|_| io::Error::other("transaction read set poisoned"))?;
                if current.generation == txn.snapshot.generation {
                    true
                } else {
                    let snapshot = txn.snapshot.read_index()?;
                    read_set.matches(&snapshot.index, &current.index)?
                }
            } else {
                current.generation == txn.snapshot.generation
            };
            if !valid {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "transaction conflict: observed state changed since snapshot",
                )
                .into());
            }
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
            check_deadline(deadline)?;
            crate::cancellation::check(cancellation)?;
            self.write_batch_locked(&operations, &payload)?;
        } else {
            check_deadline(deadline)?;
            crate::cancellation::check(cancellation)?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn poisoned_transaction_budget_rejects_new_admissions() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.transaction_budget.lock().unwrap();
            panic!("injected transaction budget panic");
        }));
        assert!(panic.is_err());
        let called = std::cell::Cell::new(false);
        let result = store.transaction(|_| {
            called.set(true);
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
        assert!(!called.get());
        assert_eq!(
            store.set_active_transaction_limit(1).unwrap_err().kind(),
            io::ErrorKind::Other
        );
        store.put(b"direct", b"allowed").unwrap();
        assert_eq!(
            store.get(b"direct").unwrap().as_deref(),
            Some(&b"allowed"[..])
        );
        drop(store);
        let recovered = crate::test_support::retry_lock(|| Store::open(directory.path())).unwrap();
        assert_eq!(
            recovered.get(b"direct").unwrap().as_deref(),
            Some(&b"allowed"[..])
        );
        recovered
            .transaction(|tx| tx.put(b"after-reopen", b"ok"))
            .unwrap();
    }

    #[test]
    fn cancellation_removes_a_fifo_waiter_before_the_preceding_turn_ends() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path()).unwrap());
        store.set_active_transaction_limit(2).unwrap();
        let first = store.transaction_queue.enter().unwrap();
        let token = crate::CancellationToken::new();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = Arc::clone(&store);
        let worker_token = token.clone();
        let worker = thread::spawn(move || {
            let result = writer.transaction_controlled(
                &worker_token,
                None,
                TransactionMode::ReadSetFifo,
                |tx| tx.put(b"key", b"value"),
            );
            done_tx.send(result).unwrap();
        });
        let timeout = Instant::now() + Duration::from_secs(10);
        while store.transaction_queue.queued_for_test() != 2 {
            assert!(Instant::now() < timeout);
            thread::yield_now();
        }
        let follower_store = Arc::clone(&store);
        let follower = thread::spawn(move || {
            follower_store.transaction_with_read_set_fifo(|tx| tx.put(b"later", b"ok"))
        });
        while store.transaction_queue.queued_for_test() != 3 {
            assert!(Instant::now() < timeout);
            thread::yield_now();
        }
        assert_eq!(
            store
                .transaction_budget_status()
                .unwrap()
                .active_transactions,
            2
        );
        assert_eq!(
            store.transaction(|_| Ok(())).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        token.cancel();
        let early = done_rx.recv_timeout(Duration::from_secs(5));
        if early.is_ok() {
            assert_eq!(
                store
                    .transaction_budget_status()
                    .unwrap()
                    .active_transactions,
                1
            );
        }
        drop(first);
        let early_result = early.is_ok();
        let result =
            early.unwrap_or_else(|_| done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        worker.join().unwrap();
        follower.join().unwrap().unwrap();
        assert!(
            early_result,
            "cancelled FIFO admission did not finish until the prior turn ended"
        );
        assert!(matches!(
            result,
            Err(crate::ControlledTransactionError::Cancelled)
        ));
        assert_eq!(store.transaction_queue.queued_for_test(), 0);
        assert_eq!(
            store
                .transaction_budget_status()
                .unwrap()
                .active_transactions,
            0
        );
        assert_eq!(store.get(b"key").unwrap(), None);
        assert_eq!(store.get(b"later").unwrap().as_deref(), Some(&b"ok"[..]));
    }

    #[test]
    fn cancellation_after_staging_with_writer_lock_held_aborts_before_append() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path()).unwrap());
        let writer_guard = store.writes.write().unwrap();
        let token = crate::CancellationToken::new();
        let (staged_tx, staged_rx) = mpsc::channel();
        let writer = Arc::clone(&store);
        let worker_token = token.clone();
        let worker = thread::spawn(move || {
            writer.transaction_controlled(
                &worker_token,
                None,
                TransactionMode::Conservative,
                |tx| {
                    tx.put(b"key", b"value")?;
                    staged_tx.send(()).map_err(io::Error::other)
                },
            )
        });
        staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        token.cancel();
        drop(writer_guard);
        assert!(matches!(
            worker.join().unwrap(),
            Err(crate::ControlledTransactionError::Cancelled)
        ));
        assert_eq!(store.get(b"key").unwrap(), None);
        assert_eq!(store.transaction_budget.lock().unwrap().used, 0);
    }

    #[test]
    fn a_deadline_passed_while_waiting_for_the_writer_aborts_before_commit() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path()).unwrap());
        let writer = store.writes.write().unwrap();
        let waiting = Arc::clone(&store);
        let (staged_tx, staged_rx) = mpsc::channel();
        let deadline = Instant::now() + Duration::from_secs(2);
        let worker = thread::spawn(move || {
            waiting.transaction_until(deadline, TransactionMode::Conservative, |tx| {
                tx.put(b"key", b"value")?;
                staged_tx.send(()).map_err(io::Error::other)
            })
        });
        let staged = staged_rx.recv_timeout(Duration::from_secs(5));
        thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
        );
        drop(writer);
        let result = worker.join().unwrap();
        staged.expect("transaction callback did not finish staging");
        assert!(matches!(
            result,
            Err(TimedTransactionError::DeadlineExceeded)
        ));
        assert!(store.get(b"key").unwrap().is_none());
        drop(store);
        assert!(
            Store::open(directory.path())
                .unwrap()
                .get(b"key")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn pending_reservation_is_released_during_unwind() {
        let budget = Mutex::new(TransactionBudget::default());
        budget.lock().unwrap().limit = 128;
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
