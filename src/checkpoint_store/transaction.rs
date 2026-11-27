//! Optimistic transactions over the checkpoint-backed store.
use super::read_set::ValidationBudget;
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
/// scan ranges. Callers can identify a directly returned pre-commit conflict
/// with [`crate::is_transaction_conflict`] and manage callback side effects
/// themselves. A `WouldBlock` kind alone does not establish a conflict.
pub struct CheckpointTransaction<'a> {
    store: &'a CheckpointStore,
    generation: u64,
    log_generation: Option<crate::log::GenerationId>,
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
    fn state(&self) -> io::Result<ControlledReadGuard<'_, State>> {
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
        self.store.ensure_healthy(&state)?;
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
        } else if state.generation != self.generation || state.log_generation != self.log_generation
        {
            return Err(conflict());
        }
        Ok(state)
    }

    /// Reads a staged value or a value validated at successful commit.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_admitted(key, |_| Ok(()))
    }

    pub(crate) fn get_admitted(
        &self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let state = self.state()?;
        match self.staged.get(key) {
            Some(value) => {
                if let Some(value) = value {
                    admit(value.len())?;
                }
                Ok(value.clone())
            }
            None => {
                let location = state.view.location(key)?;
                if let Some(read_set) = &self.read_set {
                    read_set
                        .lock()
                        .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))?
                        .record_point(self.store, key, location, state.log_generation)?;
                }
                location
                    .map(|location| {
                        admit(location.length)?;
                        state.view.base.log.read(location)
                    })
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
        self.scan_admitted(after, before, limit, |_, _| true)
    }

    pub(crate) fn scan_admitted(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
        admit: impl FnMut(&[u8], usize) -> bool,
    ) -> io::Result<Vec<Entry>> {
        let mut result = Vec::new();
        self.visit_admitted(after, before, limit, admit, |key, value| {
            result.push((key.to_vec(), value.to_vec()));
            true
        })?;
        Ok(result)
    }

    /// Visits the entries `scan_admitted` would return, in order, lending
    /// each key and verified value instead of copying them. The page ends
    /// when `visit` returns false, as it does when `admit` declines an entry.
    /// `visit` runs while the snapshot is borrowed and must not call back
    /// into this transaction.
    /// Read-set and block verification are those of `scan_admitted`.
    pub(crate) fn visit_admitted(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
        admit: impl FnMut(&[u8], usize) -> bool,
        visit: impl FnMut(&[u8], &[u8]) -> bool,
    ) -> io::Result<()> {
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
            return Ok(());
        }
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut pending = self.staged.range::<[u8], _>(bounds).peekable();
        let mut page = ScanPage {
            read_ahead: crate::log::LogReadAhead::new(&state.view.base.log),
            count: 0,
            bytes: 0,
            limit,
            admit,
            visit,
        };
        let mut stopped = false;
        let merge = |key: &[u8], location, peek: crate::log::LocationPeek<'_>| {
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
                page.push_location(key, location, peek)?
            };
            if !admitted {
                stopped = true;
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        };
        let recorded = self
            .read_set
            .as_ref()
            .map(|read_set| {
                read_set
                    .lock()
                    .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))?
                    .record_scan(self.store, &state, after, before)
            })
            .transpose()?
            .flatten();
        if let Some(range) = recorded {
            range.visit_entries_with_peek(merge)?;
        } else {
            // A range covered by a prior observation still needs fresh
            // block checks for the entries read by this page.
            state.view.visit_entries_with_peek(after, before, merge)?;
        }
        if !stopped {
            for (key, value) in pending {
                if let Some(value) = value {
                    if !page.push_value(key, value) {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Counts live keys in the exclusive range, merging staged writes, and
    /// stops once more than `max` are found. It records the same read-set
    /// scan as `scan_admitted` and visits the same verified checkpoint blocks
    /// and suffix overlay, but reads no log values: no value checksum is
    /// checked because none is returned. `admit` sees every counted key and
    /// can stop the count by returning `false`.
    pub(crate) fn count_keys(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        max: usize,
        mut admit: impl FnMut(&[u8]) -> bool,
    ) -> io::Result<usize> {
        for key in [after, before].into_iter().flatten() {
            record::validate_key(key)?;
        }
        let state = self.state()?;
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(0);
        }
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut pending = self.staged.range::<[u8], _>(bounds).peekable();
        let mut count = 0_usize;
        let mut stopped = false;
        let mut take = |key: &[u8], count: &mut usize| {
            if *count == max || !admit(key) {
                return false;
            }
            *count += 1;
            true
        };
        let merge = |key: &[u8], _location, _peek: crate::log::LocationPeek<'_>| {
            while pending
                .peek()
                .is_some_and(|(pending_key, _)| pending_key.as_slice() < key)
            {
                let (pending_key, value) = pending.next().expect("peeked staged entry");
                if value.is_some() && !take(pending_key, &mut count) {
                    stopped = true;
                    return Ok(ControlFlow::Break(()));
                }
            }
            let live = if pending
                .peek()
                .is_some_and(|(pending_key, _)| pending_key.as_slice() == key)
            {
                pending.next().expect("peeked staged entry").1.is_some()
            } else {
                true
            };
            if live && !take(key, &mut count) {
                stopped = true;
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        };
        let recorded = self
            .read_set
            .as_ref()
            .map(|read_set| {
                read_set
                    .lock()
                    .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))?
                    .record_scan(self.store, &state, after, before)
            })
            .transpose()?
            .flatten();
        if let Some(range) = recorded {
            range.visit_entries_with_peek(merge)?;
        } else {
            state.view.visit_entries_with_peek(after, before, merge)?;
        }
        if stopped {
            return Ok(count + 1);
        }
        for (key, value) in pending {
            if value.is_some() && !take(key, &mut count) {
                return Ok(count + 1);
            }
        }
        Ok(count)
    }

    pub(crate) fn with_point_session<T>(
        &self,
        action: impl FnOnce(&mut CheckpointPointSession<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        let state = self.state()?;
        let read_set = self
            .read_set
            .as_ref()
            .map(|read_set| {
                read_set
                    .lock()
                    .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))
            })
            .transpose()?;
        let mut session = CheckpointPointSession {
            tx: self,
            view: &state.view,
            log_generation: state.log_generation,
            lookup: state.view.base.lookup(),
            read_ahead: crate::log::LogReadAhead::new(&state.view.base.log),
            read_set,
        };
        action(&mut session)
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

struct ScanPage<'a, A, V> {
    read_ahead: crate::log::LogReadAhead<'a>,
    count: usize,
    bytes: usize,
    limit: usize,
    admit: A,
    visit: V,
}

impl<A: FnMut(&[u8], usize) -> bool, V: FnMut(&[u8], &[u8]) -> bool> ScanPage<'_, A, V> {
    fn has_room(&self, key: &[u8], value_len: usize) -> bool {
        self.count < self.limit && self.bytes + key.len() + value_len <= MAX_SCAN_BYTES
    }

    fn push_value(&mut self, key: &[u8], value: &[u8]) -> bool {
        if !self.has_room(key, value.len()) || !(self.admit)(key, value.len()) {
            return false;
        }
        self.bytes += key.len() + value.len();
        self.count += 1;
        (self.visit)(key, value)
    }

    fn push_location(
        &mut self,
        key: &[u8],
        location: ValueLocation,
        peek: crate::log::LocationPeek<'_>,
    ) -> io::Result<bool> {
        if !self.has_room(key, location.length) || !(self.admit)(key, location.length) {
            return Ok(false);
        }
        let remaining = self.limit - self.count - 1;
        let value = self.read_ahead.read_slice_with_peek(location, |push| {
            let mut count = 0;
            peek(&mut |next| {
                if count >= remaining {
                    return false;
                }
                count += 1;
                push(next)
            });
        })?;
        self.bytes += key.len() + value.len();
        self.count += 1;
        Ok((self.visit)(key, &value))
    }
}

pub(crate) struct CheckpointPointSession<'a> {
    tx: &'a CheckpointTransaction<'a>,
    view: &'a RecoveredCheckpointView,
    log_generation: Option<crate::log::GenerationId>,
    lookup: crate::checkpoint_view::CheckpointLookup<'a>,
    read_ahead: crate::log::LogReadAhead<'a>,
    read_set: Option<std::sync::MutexGuard<'a, super::read_set::ReadSet>>,
}

impl CheckpointPointSession<'_> {
    pub(crate) fn get_admitted_with_peek(
        &mut self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
        peek_keys: impl FnOnce(&mut dyn FnMut(&[u8]) -> bool),
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        crate::cancellation::check(self.tx.cancellation.as_ref())?;
        if self
            .tx
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "transaction deadline exceeded",
            ));
        }
        if let Some(value) = self.tx.staged.get(key) {
            if let Some(value) = value {
                admit(value.len())?;
            }
            return Ok(value.clone());
        }
        let location = match self.view.overlay.get(key) {
            Some(location) => *location,
            None => self.lookup.location(key)?,
        };
        if let Some(read_set) = &mut self.read_set {
            read_set.record_point(self.tx.store, key, location, self.log_generation)?;
        }
        let Some(location) = location else {
            return Ok(None);
        };
        admit(location.length)?;
        let value = self.read_ahead.read_with_peek(location, |push| {
            peek_keys(&mut |next_key| {
                if self.tx.staged.contains_key(next_key) {
                    return true;
                }
                let next_loc = match self.view.overlay.get(next_key) {
                    Some(loc) => *loc,
                    None => self.lookup.location_if_loaded(next_key),
                };
                if let Some(next_loc) = next_loc {
                    push(next_loc)
                } else {
                    true
                }
            });
        })?;
        Ok(Some(value))
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
    /// no automatic retry is performed. Use [`crate::is_transaction_conflict`]
    /// to identify a directly returned conflict; `WouldBlock` alone can be an
    /// uncertain I/O error. Successful writes are one synchronized
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
        self.run_transaction(action, None, false, None, None, None)
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
        self.run_transaction(action, None, true, None, None, None)
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
        self.run_transaction(
            action,
            Some(ValidationBudget::Unlimited),
            false,
            None,
            None,
            None,
        )
        .map_err(TimedTransactionError::into_io)
    }

    /// Runs a read-set transaction with an inclusive cumulative limit on
    /// observations revisited after intervening writes. One unit is charged
    /// per point checked and per entry encountered in a previously observed
    /// range, including an extra entry that would reveal a phantom. The first
    /// capture and validations against the same generation charge no units.
    /// Exhaustion rejects the transaction before append, even if the callback
    /// catches the limit error. The limit counts logical work and excludes
    /// block reads, bytes, callback work and elapsed time.
    pub fn transaction_with_read_set_validation_limit<T>(
        &self,
        max_validation_items: u64,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.run_transaction(
            action,
            Some(ValidationBudget::Remaining(max_validation_items)),
            false,
            None,
            None,
            None,
        )
        .map_err(TimedTransactionError::into_io)
    }

    /// Runs a read-set transaction with FIFO callback and commit admission.
    /// FIFO has the same nesting, blocking and fairness limitations as
    /// `transaction_with_fifo`; nonparticipating writes can still conflict.
    pub fn transaction_with_read_set_fifo<T>(
        &self,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.run_transaction(
            action,
            Some(ValidationBudget::Unlimited),
            true,
            None,
            None,
            None,
        )
        .map_err(TimedTransactionError::into_io)
    }

    /// Runs a selected transaction mode with an operation-boundary deadline.
    ///
    /// `DeadlineExceeded` means this transaction's staged changes were not
    /// submitted for commit. `Operation` wraps callback and I/O errors; a
    /// commit I/O timeout may have an unknown outcome. A callback that forwards
    /// a timed-out read returns `Operation`. FIFO waiters can expire in the
    /// queue; transaction state-lock admission also observes expiry while
    /// waiting. Callbacks, other lock waits and I/O are not interrupted, and
    /// completion time is not bounded. A commit that has started can succeed
    /// after its deadline. External callback effects are never rolled back.
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
        self.run_transaction(
            action,
            read_set.then_some(ValidationBudget::Unlimited),
            fifo,
            Some(deadline),
            None,
            None,
        )
    }

    /// Runs a selected mode with cooperative cancellation and an optional
    /// deadline. Cancellation or deadline expiry reported by the engine means
    /// staged writes were not submitted for commit. FIFO and transaction
    /// state-lock admission observe cancellation while waiting. Callbacks,
    /// other lock waits and I/O are not forcibly interrupted, and completion
    /// time is not bounded. Once append starts it may succeed after
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
        self.run_transaction(
            action,
            read_set.then_some(ValidationBudget::Unlimited),
            fifo,
            deadline,
            Some(cancellation),
            None,
        )
        .map_err(crate::cancellation::classify)
    }

    /// Runs a transaction with optional controls and structured failure outcomes.
    ///
    /// Rejection, callback failure, cancellation and expiry guarantee only
    /// that this transaction's staged writes were not submitted. Callback
    /// side effects and independent writes are not rolled back. An unknown
    /// outcome requires reconciliation before retry. A pre-append rejection
    /// does not rule out independent effects or automatic checkpoint
    /// maintenance. Recovery alone may not identify whether a particular
    /// operation took effect. Panics unwind.
    pub fn transaction_detailed<T>(
        &self,
        mode: CheckpointTransactionMode,
        deadline: Option<Instant>,
        cancellation: Option<&crate::CancellationToken>,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> Result<T, crate::TransactionFailure> {
        self.transaction_detailed_with_validation_limit(mode, deadline, cancellation, None, action)
    }

    pub(crate) fn transaction_detailed_with_validation_limit<T>(
        &self,
        mode: CheckpointTransactionMode,
        deadline: Option<Instant>,
        cancellation: Option<&crate::CancellationToken>,
        validation_limit: Option<u64>,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
    ) -> Result<T, crate::TransactionFailure> {
        let read_set = matches!(
            mode,
            CheckpointTransactionMode::ReadSet | CheckpointTransactionMode::ReadSetFifo
        );
        let fifo = matches!(
            mode,
            CheckpointTransactionMode::Fifo | CheckpointTransactionMode::ReadSetFifo
        );
        let tracker = crate::transaction_outcome::Tracker::new();
        self.run_transaction(
            action,
            read_set.then(|| {
                validation_limit.map_or(ValidationBudget::Unlimited, ValidationBudget::Remaining)
            }),
            fifo,
            deadline,
            cancellation,
            Some(&tracker),
        )
        .map_err(|error| tracker.classify(error))
    }

    fn run_transaction<T>(
        &self,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
        validation: Option<ValidationBudget>,
        fifo: bool,
        deadline: Option<Instant>,
        cancellation: Option<&crate::CancellationToken>,
        tracker: Option<&crate::transaction_outcome::Tracker>,
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
        let (generation, log_generation) = {
            let state =
                crate::cancellation::read_lock(&self.state, deadline, cancellation, uncertain)?;
            self.ensure_healthy(&state)?;
            (state.generation, state.log_generation)
        };
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        let track_points = validation.is_some();
        let mut transaction = CheckpointTransaction {
            store: self,
            generation,
            log_generation,
            deadline,
            cancellation: cancellation.cloned(),
            read_set: validation.map(|budget| {
                Mutex::new(super::read_set::ReadSet::new(
                    generation,
                    log_generation,
                    budget,
                ))
            }),
            staged: BTreeMap::new(),
            encoded_bytes: 4,
        };
        if let Some(tracker) = tracker {
            tracker.callback();
        }
        let result = action(&mut transaction)?;
        if let Some(tracker) = tracker {
            tracker.before_commit();
        }
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        let validate = |state: &State| -> Result<(), TimedTransactionError> {
            self.ensure_healthy(state)?;
            check_deadline(deadline)?;
            crate::cancellation::check(cancellation)?;
            let valid = if let Some(read_set) = &transaction.read_set {
                read_set
                    .lock()
                    .map_err(|_| io::Error::other("checkpoint transaction read set poisoned"))?
                    .matches(state)?
            } else {
                state.generation == generation && state.log_generation == log_generation
            };
            if !valid {
                return Err(conflict().into());
            }
            Ok(())
        };

        if transaction.staged.is_empty() {
            let state =
                crate::cancellation::read_lock(&self.state, deadline, cancellation, uncertain)?;
            validate(&state)?;
            check_deadline(deadline)?;
            crate::cancellation::check(cancellation)?;
            return Ok(result);
        }

        let operations = transaction
            .staged
            .iter()
            .map(|(key, value)| match value {
                Some(value) => BatchOperation::Put { key, value },
                None => BatchOperation::Delete { key },
            })
            .collect::<Vec<_>>();
        let batch = BatchLayout::new(&operations)?;
        let state = if track_points {
            let (state, writer) = self.read_before_write_until(deadline, cancellation)?;
            validate(&state)?;
            drop(state);
            self.lock_state_for_write_until(writer, deadline, cancellation)?
        } else {
            let state = self.write_until(deadline, cancellation)?;
            validate(&state)?;
            state
        };
        check_deadline(deadline)?;
        crate::cancellation::check(cancellation)?;
        self.commit_until(
            state,
            Mutation::Batch(batch),
            crate::cancellation::CommitControl {
                deadline,
                cancellation,
                tracker,
            },
        )?;
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
    crate::transaction_conflict::conflict(
        "checkpoint transaction conflict: a write committed since the transaction began",
    )
}
