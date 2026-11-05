//! Optimistic transactions over the checkpoint-backed store.
use super::*;
use crate::TimedTransactionError;
use crate::log::ValueLocation;
use crate::record::batch::{MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS};
use crate::transaction_budget::PendingReservation;
use std::ops::ControlFlow;
use std::time::Instant;

/// Admission mode for a controlled checkpoint-backed transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointTransactionMode {
    /// Optimistic transaction without FIFO admission.
    Conservative,
    /// Admit participating callbacks and commits through the FIFO queue.
    Fifo,
    /// Validate observed point versions and complete requested scan ranges.
    ReadSet,
    /// Validate point and range observations with FIFO admission.
    ReadSetFifo,
}

/// A bounded, process-local transaction on a checkpoint-backed store.
///
/// Reads and scans see staged changes. The conservative modes conflict on any
/// intervening write. Read-set modes validate observed points and complete
/// scan ranges. Callers must retry a conflicted callback themselves and manage
/// any external side effects.
pub struct CheckpointTransaction<'a> {
    store: &'a CheckpointStore,
    generation: u64,
    deadline: Option<Instant>,
    cancellation: Option<crate::CancellationToken>,
    read_set: Option<Mutex<super::read_set::ReadSet>>,
    staged: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    encoded_bytes: usize,
}

impl Drop for CheckpointTransaction<'_> {
    fn drop(&mut self) {
        self.staged.clear();
        let mut reserved = self.encoded_bytes - 4;
        if let Some(read_set) = &mut self.read_set {
            reserved += read_set
                .get_mut()
                .unwrap_or_else(|poison| poison.into_inner())
                .clear();
        }
        if reserved > 0 {
            self.store
                .transaction_budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(reserved);
        }
    }
}

impl CheckpointTransaction<'_> {
    fn state(&self) -> io::Result<RwLockReadGuard<'_, State>> {
        crate::cancellation::check(self.cancellation.as_ref())?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "transaction deadline exceeded",
            ));
        }
        let state = crate::cancellation::read_lock(
            &self.store.state,
            self.deadline,
            self.cancellation.as_ref(),
            uncertain,
        )
        .map_err(TimedTransactionError::into_io)?;
        if state.failed {
            return Err(uncertain());
        }
        crate::cancellation::check(self.cancellation.as_ref())?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "transaction deadline exceeded",
            ));
        }
        if let Some(read_set) = &self.read_set {
            read_set
                .lock()
                .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))?
                .check_state(&state)?;
        } else if state.generation != self.generation {
            return Err(conflict());
        }
        Ok(state)
    }

    /// Reads a staged value or a value validated at successful commit.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let state = self.state()?;
        match self.staged.get(key) {
            Some(value) => Ok(value.clone()),
            None => {
                let location = state.view.location(key)?;
                if let Some(read_set) = &self.read_set {
                    read_set
                        .lock()
                        .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))?
                        .record_point(self.store, key, location)?;
                }
                location
                    .map(|location| state.view.base.log.read(location))
                    .transpose()
            }
        }
    }

    /// Stages a replacement; it is invisible until a successful commit.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.stage(Operation::Put, key, value)
    }

    /// Stages a deletion; it is invisible until a successful commit.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        self.stage(Operation::Delete, key, &[])
    }

    /// Scans a validated state merged with staged changes.
    /// Bounds are exclusive; pages have the same entry and byte caps as
    /// `CheckpointStore::scan`. Continue pagination until a page is empty.
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
        let state = self.state()?;
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(Vec::new());
        }
        if let Some(read_set) = &self.read_set {
            read_set
                .lock()
                .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))?
                .record_scan(self.store, &state, after, before)?;
        }
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut pending = self.staged.range::<[u8], _>(bounds).peekable();
        let mut page = ScanPage {
            log: &state.view.base.log,
            entries: Vec::new(),
            bytes: 0,
            limit,
        };
        let mut stopped = false;
        state.view.visit_entries(after, before, |key, location| {
            while pending
                .peek()
                .is_some_and(|(pending_key, _)| pending_key.as_slice() < key)
            {
                let (pending_key, value) = pending.next().expect("peeked staged entry");
                if let Some(value) = value {
                    if !page.push_value(pending_key, value) {
                        stopped = true;
                        return Ok(ControlFlow::Break(()));
                    }
                }
            }
            let admitted = if pending
                .peek()
                .is_some_and(|(pending_key, _)| pending_key.as_slice() == key)
            {
                let (pending_key, value) = pending.next().expect("peeked staged entry");
                value
                    .as_ref()
                    .is_none_or(|value| page.push_value(pending_key, value))
            } else {
                page.push_location(key, location)?
            };
            if !admitted {
                stopped = true;
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        })?;
        if !stopped {
            for (key, value) in pending {
                if let Some(value) = value {
                    if !page.push_value(key, value) {
                        break;
                    }
                }
            }
        }
        Ok(page.entries)
    }

    fn stage(&mut self, operation: Operation, key: &[u8], value: &[u8]) -> io::Result<()> {
        record::validate(operation, key, value)?;
        drop(self.state()?);
        let previous = self.staged.get(key);
        if previous.is_none() && self.staged.len() >= MAX_ATOMIC_BATCH_OPERATIONS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction write count limit exceeded",
            ));
        }
        let old_size = previous.map_or(0, |old| {
            HEADER_SIZE + key.len() + old.as_ref().map_or(0, Vec::len) + CHECKSUM_SIZE
        });
        let new_size = HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE;
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
            PendingReservation::acquire(&self.store.transaction_budget, increase)?;
        let replacement = if operation == Operation::Put {
            Some(value.to_vec())
        } else {
            None
        };
        self.staged.insert(key.to_vec(), replacement);
        if decrease > 0 {
            self.store
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

struct ScanPage<'a> {
    log: &'a Log,
    entries: Vec<Entry>,
    bytes: usize,
    limit: usize,
}

impl ScanPage<'_> {
    fn has_room(&self, key: &[u8], value_len: usize) -> bool {
        self.entries.len() < self.limit && self.bytes + key.len() + value_len <= MAX_SCAN_BYTES
    }

    fn push_value(&mut self, key: &[u8], value: &[u8]) -> bool {
        if !self.has_room(key, value.len()) {
            return false;
        }
        self.bytes += key.len() + value.len();
        self.entries.push((key.to_vec(), value.to_vec()));
        true
    }

    fn push_location(&mut self, key: &[u8], location: ValueLocation) -> io::Result<bool> {
        if !self.has_room(key, location.length) {
            return Ok(false);
        }
        let value = self.log.read(location)?;
        self.bytes += key.len() + value.len();
        self.entries.push((key.to_vec(), value));
        Ok(true)
    }
}

impl CheckpointStore {
    /// Returns this open store's logical transaction reservation usage.
    pub fn transaction_budget_status(&self) -> io::Result<crate::TransactionBudgetStatus> {
        Ok(self
            .transaction_budget
            .lock()
            .map_err(|_| io::Error::other("transaction budget poisoned"))?
            .status())
    }

    /// Sets the aggregate reservation limit for staged records and retained
    /// checkpoint read-set observations on this open store. Values above the
    /// hard maximum are invalid; lowering it below active reservations returns
    /// `WouldBlock` without changing it. Zero is allowed. Ordinary writes,
    /// snapshots, allocator overhead and total process memory are not covered.
    /// The setting is process-local and resets when the store is reopened.
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

    /// Runs a bounded optimistic serializable transaction.
    ///
    /// The callback runs without holding the store lock. Reads and the final
    /// commit fail with `WouldBlock` if any successful mutation intervenes;
    /// no automatic retry is performed. Successful writes are one synchronized
    /// atomic batch. Callback errors discard staged writes; external effects
    /// are not rolled back. A commit I/O failure has an unknown outcome and
    /// makes this store instance unusable until recovery. The per-transaction
    /// count and encoded-byte limits are those of an atomic batch, and staged
    /// encoded records share the per-store transaction reservation budget.
    /// The budget does not include returned values, allocation overhead or
    /// the temporary encoded commit buffer. Rollover alone is not a mutation.
    pub fn transaction<T>(
        &self,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.run_transaction(action, false, false, None, None)
            .map_err(TimedTransactionError::into_io)
    }

    /// Runs a conservative transaction with FIFO admission.
    ///
    /// The bounded queue orders callbacks and commits for calls to this method.
    /// Regular transactions and independent writes do not join it and can
    /// still cause a conflict. A callback must not wait for another queued
    /// transaction on this store; same-thread nested admission returns an error.
    /// There is no bounded-wait guarantee, and queueing can increase latency.
    pub fn transaction_with_fifo<T>(
        &self,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.run_transaction(action, false, true, None, None)
            .map_err(TimedTransactionError::into_io)
    }

    /// Runs a transaction validating point versions and complete scan ranges.
    ///
    /// Unrelated writes can commit during the callback. A previously present
    /// value changed and restored has a different log location and conflicts;
    /// an absent key inserted and deleted may match absence again. A scan
    /// records its entire requested range, including entries beyond a limited
    /// page. Dependencies are limited to 2,048 points and ranges, 2 MiB of
    /// retained key and bound bytes, and 65,536 point/range items. Their logical
    /// reservations share the staged-record budget; allocator overhead is excluded.
    /// Reads through independent store methods are not tracked. Callbacks and
    /// external effects are never retried or rolled back automatically.
    pub fn transaction_with_read_set<T>(
        &self,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.run_transaction(action, true, false, None, None)
            .map_err(TimedTransactionError::into_io)
    }

    /// Runs a read-set transaction with FIFO callback and commit admission.
    /// FIFO has the same nesting, blocking and fairness limitations as
    /// `transaction_with_fifo`; nonparticipating writes can still conflict.
    pub fn transaction_with_read_set_fifo<T>(
        &self,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.run_transaction(action, true, true, None, None)
            .map_err(TimedTransactionError::into_io)
    }

    /// Runs a selected transaction mode with an operation-boundary deadline.
    ///
    /// `DeadlineExceeded` means this transaction's staged changes were not
    /// submitted for commit. `Operation` wraps callback and I/O errors; a
    /// commit I/O timeout may have an unknown outcome. A callback that forwards
    /// a timed-out read returns `Operation`. FIFO waiters can expire in the
    /// queue; callbacks, state-lock waits and I/O are not interrupted. A commit
    /// that has started can succeed after its deadline. External callback
    /// effects are never rolled back.
    pub fn transaction_until<T>(
        &self,
        deadline: Instant,
        mode: CheckpointTransactionMode,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> Result<T, TimedTransactionError> {
        let read_set = matches!(
            mode,
            CheckpointTransactionMode::ReadSet | CheckpointTransactionMode::ReadSetFifo
        );
        let fifo = matches!(
            mode,
            CheckpointTransactionMode::Fifo | CheckpointTransactionMode::ReadSetFifo
        );
        self.run_transaction(action, read_set, fifo, Some(deadline), None)
    }

    /// Runs a selected mode with cooperative cancellation and an optional
    /// deadline. Cancellation or deadline expiry reported by the engine means
    /// staged writes were not submitted for commit. FIFO admission observes
    /// cancellation while waiting; callbacks, state-lock waits and I/O are
    /// not forcibly interrupted. Once append starts it may succeed after
    /// cancellation or return an operation error with an unknown outcome.
    /// Automatic checkpoint maintenance can complete before cancellation.
    pub fn transaction_controlled<T>(
        &self,
        cancellation: &crate::CancellationToken,
        deadline: Option<Instant>,
        mode: CheckpointTransactionMode,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> Result<T, crate::ControlledTransactionError> {
        let read_set = matches!(
            mode,
            CheckpointTransactionMode::ReadSet | CheckpointTransactionMode::ReadSetFifo
        );
        let fifo = matches!(
            mode,
            CheckpointTransactionMode::Fifo | CheckpointTransactionMode::ReadSetFifo
        );
        self.run_transaction(action, read_set, fifo, deadline, Some(cancellation))
            .map_err(crate::cancellation::classify)
    }

    fn run_transaction<T>(
        &self,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
        track_points: bool,
        fifo: bool,
        deadline: Option<Instant>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<T, TimedTransactionError> {
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        let _admission =
            crate::transaction_budget::ActiveTransaction::acquire(&self.transaction_budget)?;
        // Admission covers both callback and commit. Release the write guard
        // and staged reservations before releasing the queue turn on unwind.
        let _turn = if fifo {
            Some(
                self.transaction_queue
                    .enter_controlled(deadline, cancellation)
                    .map_err(|error| {
                        if error.kind() == io::ErrorKind::TimedOut {
                            TimedTransactionError::DeadlineExceeded
                        } else {
                            TimedTransactionError::Operation(error)
                        }
                    })?,
            )
        } else {
            None
        };
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        let generation = {
            let state =
                crate::cancellation::read_lock(&self.state, deadline, cancellation, uncertain)?;
            if state.failed {
                return Err(uncertain().into());
            }
            state.generation
        };
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        let mut transaction = CheckpointTransaction {
            store: self,
            generation,
            deadline,
            cancellation: cancellation.cloned(),
            read_set: track_points.then(|| Mutex::new(super::read_set::ReadSet::new(generation))),
            staged: BTreeMap::new(),
            encoded_bytes: 4,
        };
        let result = action(&mut transaction)?;
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        let operations = transaction
            .staged
            .iter()
            .map(|(key, value)| match value {
                Some(value) => BatchOperation::Put { key, value },
                None => BatchOperation::Delete { key },
            })
            .collect::<Vec<_>>();
        let payload = if operations.is_empty() {
            None
        } else {
            Some(encode_payload(&operations)?)
        };
        let mut state =
            crate::cancellation::write_lock(&self.state, deadline, cancellation, uncertain)?;
        if state.failed {
            return Err(uncertain().into());
        }
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        let valid = if let Some(read_set) = &transaction.read_set {
            read_set
                .lock()
                .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))?
                .matches(&state)?
        } else {
            state.generation == generation
        };
        if !valid {
            return Err(conflict().into());
        }
        if let Some(payload) = payload {
            let keys = operations
                .iter()
                .map(|operation| operation.fields().1)
                .collect::<Vec<_>>();
            check_deadline(deadline)?;
            crate::cancellation::check(cancellation)?;
            self.commit_until(
                &mut state,
                Operation::Batch,
                &[0],
                &payload,
                &keys,
                crate::cancellation::CommitControl {
                    deadline,
                    cancellation,
                },
            )?;
        } else {
            check_deadline(deadline)?;
            crate::cancellation::check(cancellation)?;
        }
        Ok(result)
    }
}

fn check_deadline(deadline: Option<Instant>) -> Result<(), TimedTransactionError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(TimedTransactionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

pub(super) fn conflict() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "checkpoint transaction conflict: a write committed since the transaction began",
    )
}
