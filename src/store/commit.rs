//! Bounded group commit for independent, single-key writes.
//!
//! A single leader appends queued records in order, synchronizes the log, and
//! publishes their index updates before waking the callers. Grouping is not a
//! multi-key transaction: a failed or interrupted group has unknown outcomes.
use super::Inner;
use crate::TimedTransactionError;
use crate::log::{AppendRecord, Log, ValueLocation, prepare_batch};
use crate::record::batch::BatchLayout;
use crate::record::{self, CHECKSUM_SIZE, HEADER_SIZE, Operation};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::Duration;

// These bound encoded bytes and entry count, not total process memory. Caller
// buffers, keys, allocator overhead, thread stacks, and the index are separate.
/// Hard maximum encoded bytes reserved by the ordinary store's group commit.
pub const MAX_GROUP_COMMIT_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_RECORDS: usize = 64;
/// Hard maximum records reserved by the ordinary store's group commit.
pub const MAX_GROUP_COMMIT_BUFFERED_RECORDS: usize = 1024;
const LEADER_RECHECK_INTERVAL: Duration = Duration::from_millis(10);

/// Process-local group-commit admission state for one ordinary store.
///
/// Reservations count encoded records, including in-flight preparation. They
/// do not account for caller buffers, other lock waiters or total memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct GroupCommitStatus {
    /// Current encoded-byte admission limit.
    pub limit_bytes: usize,
    /// Current record-count admission limit.
    pub limit_records: usize,
    /// Reserved encoded bytes, including preparation and in-flight records.
    pub reserved_bytes: usize,
    /// Reserved records, including preparation and in-flight records.
    pub reserved_records: usize,
    /// Writers waiting inside group-commit capacity admission.
    pub waiting_writers: usize,
    /// Whether a prior commit or coordinator failure requires reopening.
    pub failed: bool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SnapshotCopyPoint {
    BeforeCopy,
    AfterCopy,
}

#[cfg(test)]
type SnapshotCopyHook = Arc<dyn Fn(SnapshotCopyPoint) + Send + Sync>;

// Runs in the publishing leader just before it synchronizes a group, on
// either log format.
#[cfg(test)]
pub(super) type SyncHook = Arc<dyn Fn() + Send + Sync>;

fn put_index(
    index: &mut BTreeMap<Vec<u8>, ValueLocation>,
    key: Vec<u8>,
    location: ValueLocation,
    key_bytes: &mut usize,
) -> io::Result<()> {
    let length = key.len();
    if index.insert(key, location).is_none() {
        *key_bytes = key_bytes
            .checked_add(length)
            .ok_or_else(|| io::Error::other("live index key byte count overflow"))?;
    }
    Ok(())
}

fn delete_index(
    index: &mut BTreeMap<Vec<u8>, ValueLocation>,
    key: &[u8],
    key_bytes: &mut usize,
) -> io::Result<()> {
    if index.remove(key).is_some() {
        *key_bytes = key_bytes
            .checked_sub(key.len())
            .ok_or_else(|| io::Error::other("live index key byte count is inconsistent"))?;
    }
    Ok(())
}

struct Entry {
    operation: Operation,
    key: Vec<u8>,
    encoded: Vec<u8>,
    value_len: usize,
    checksum: u32,
    completion: Arc<Completion>,
    // Keys this record writes, registered as pending from the moment it is
    // queued until its index update is published.
    pending_keys: Vec<Vec<u8>>,
}

/// Keys written by records that are queued or being synchronized but whose
/// index updates are not yet published. Shown to a submission's validation
/// under the queue mutex, so no record can be queued between the check and
/// the submission.
pub(super) struct PendingWrites<'a>(&'a BTreeMap<Vec<u8>, usize>);

impl PendingWrites<'_> {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn contains(&self, key: &[u8]) -> bool {
        self.0.contains_key(key)
    }

    /// Whether a pending key lies strictly between the optional bounds.
    pub(super) fn any_between(&self, after: Option<&[u8]>, before: Option<&[u8]>) -> bool {
        use std::ops::Bound::{Excluded, Unbounded};
        let lower = after.map_or(Unbounded, Excluded);
        let upper = before.map_or(Unbounded, Excluded);
        if let (Some(after), Some(before)) = (after, before) {
            if after >= before {
                return false;
            }
        }
        self.0.range::<[u8], _>((lower, upper)).next().is_some()
    }
}

impl Entry {
    fn new(
        operation: Operation,
        key: Vec<u8>,
        encoded: Vec<u8>,
        value_len: usize,
        checksum: u32,
    ) -> Self {
        Self {
            operation,
            key,
            encoded,
            value_len,
            checksum,
            completion: Arc::new(Completion {
                done: AtomicBool::new(false),
                waiting: AtomicBool::new(false),
                changed: Condvar::new(),
            }),
            pending_keys: Vec::new(),
        }
    }
}

// A request has one waiter. All condition variables use the same state mutex,
// so checking the predicate and going to sleep cannot miss a notification.
struct Completion {
    done: AtomicBool,
    // Read and changed only while the coordinator state mutex is held.
    waiting: AtomicBool,
    changed: Condvar,
}

struct Failure {
    kind: io::ErrorKind,
    message: String,
}

struct State {
    queue: VecDeque<Entry>,
    buffered_bytes: usize,
    buffered_records: usize,
    admission_waiters: usize,
    max_bytes: usize,
    max_records: usize,
    active: bool,
    active_completions: Vec<Arc<Completion>>,
    failure: Option<Failure>,
    pending: BTreeMap<Vec<u8>, usize>,
}

pub(super) struct Coordinator {
    state: Mutex<State>,
    admission_changed: Condvar,
    // Notified whenever pending writes are published.
    pending_changed: Condvar,
    failed: AtomicBool,
    #[cfg(test)]
    snapshot_copy_hook: Mutex<Option<SnapshotCopyHook>>,
    #[cfg(test)]
    sync_hook: Mutex<Option<SyncHook>>,
}

impl Coordinator {
    pub(super) fn new() -> Self {
        Self::with_limits(
            MAX_GROUP_COMMIT_BUFFERED_BYTES,
            MAX_GROUP_COMMIT_BUFFERED_RECORDS,
        )
    }

    fn with_limits(max_bytes: usize, max_records: usize) -> Self {
        Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                buffered_bytes: 0,
                buffered_records: 0,
                admission_waiters: 0,
                max_bytes,
                max_records,
                active: false,
                active_completions: Vec::new(),
                failure: None,
                pending: BTreeMap::new(),
            }),
            admission_changed: Condvar::new(),
            pending_changed: Condvar::new(),
            failed: AtomicBool::new(false),
            #[cfg(test)]
            snapshot_copy_hook: Mutex::new(None),
            #[cfg(test)]
            sync_hook: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(max_bytes: usize, max_records: usize) -> Self {
        Self::with_limits(max_bytes, max_records)
    }

    #[cfg(test)]
    pub(super) fn set_sync_hook(&self, hook: Option<SyncHook>) {
        *self.sync_hook.lock().unwrap() = hook;
    }

    #[cfg(test)]
    pub(super) fn set_snapshot_copy_hook(&self, hook: Option<SnapshotCopyHook>) {
        *self.snapshot_copy_hook.lock().unwrap() = hook;
    }

    #[cfg(test)]
    fn snapshot_copy_point(&self, point: SnapshotCopyPoint) {
        let hook = self.snapshot_copy_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(point);
        }
    }

    #[cfg(test)]
    pub(super) fn buffered(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.buffered_bytes, state.buffered_records)
    }

    #[cfg(test)]
    pub(super) fn admission_waiters(&self) -> usize {
        self.state.lock().unwrap().admission_waiters
    }

    #[cfg(test)]
    pub(super) fn poison_for_test(&self) {
        let _guard = self.state.lock().unwrap();
        panic!("injected coordinator panic");
    }

    #[cfg(test)]
    pub(super) fn enqueue_unowned_for_test(&self, key: &[u8], value: &[u8]) {
        let encoded = record::encode(Operation::Put, key, value).unwrap();
        let mut state = self.state.lock().unwrap();
        assert!(state.active);
        state.buffered_bytes += encoded.len();
        state.buffered_records += 1;
        state.queue.push_back(Entry {
            operation: Operation::Put,
            key: key.to_vec(),
            encoded,
            value_len: value.len(),
            checksum: crc32fast::hash(value),
            completion: Arc::new(Completion {
                done: AtomicBool::new(false),
                waiting: AtomicBool::new(false),
                changed: Condvar::new(),
            }),
            pending_keys: Vec::new(),
        });
    }

    #[cfg(test)]
    pub(super) fn wake_queued_for_test(&self) {
        let state = self.state.lock().unwrap();
        for entry in &state.queue {
            entry.completion.changed.notify_one();
        }
    }

    #[cfg(test)]
    pub(super) fn queued_len(&self) -> usize {
        self.state.lock().unwrap().queue.len()
    }

    #[cfg(test)]
    pub(super) fn queued_or_active_len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.queue.len() + state.active_completions.len()
    }

    pub(super) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub(super) fn status(&self) -> io::Result<GroupCommitStatus> {
        let state = self.lock()?;
        Ok(GroupCommitStatus {
            limit_bytes: state.max_bytes,
            limit_records: state.max_records,
            reserved_bytes: state.buffered_bytes,
            reserved_records: state.buffered_records,
            waiting_writers: state.admission_waiters,
            failed: self.failed(),
        })
    }

    pub(super) fn set_limits(&self, bytes: usize, records: usize) -> io::Result<()> {
        if bytes > MAX_GROUP_COMMIT_BUFFERED_BYTES || records > MAX_GROUP_COMMIT_BUFFERED_RECORDS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "group commit limit exceeds hard maximum",
            ));
        }
        let mut state = self.lock()?;
        if self.failed() {
            return Err(Self::failure(&state));
        }
        if bytes < state.buffered_bytes || records < state.buffered_records {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "group commit limit is below current reservations",
            ));
        }
        state.max_bytes = bytes;
        state.max_records = records;
        if state.admission_waiters > 0 {
            self.admission_changed.notify_all();
        }
        Ok(())
    }

    fn failure(state: &State) -> io::Error {
        match &state.failure {
            Some(f) => io::Error::new(f.kind, f.message.clone()),
            None => io::Error::other("write outcome is uncertain; reopen the store"),
        }
    }

    // Failure must wake both in-flight and queued requests, plus writers that
    // have not yet been admitted because the queue is full. Called with state
    // locked, including when that lock is poisoned.
    fn notify_failed(&self, state: &State) {
        for completion in &state.active_completions {
            completion.changed.notify_one();
        }
        for entry in &state.queue {
            entry.completion.changed.notify_one();
        }
        self.admission_changed.notify_all();
    }

    /// Waits while `blocked` holds for the pending writes, for example while
    /// a queued write overlaps a transaction's observations. It returns once
    /// they are published, the store fails, the deadline passes or the
    /// transaction is cancelled.
    pub(super) fn wait_while_pending(
        &self,
        mut blocked: impl FnMut(&PendingWrites<'_>) -> bool,
        deadline: Option<std::time::Instant>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<(), TimedTransactionError> {
        let mut state = self.lock()?;
        while !self.failed() && blocked(&PendingWrites(&state.pending)) {
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(TimedTransactionError::DeadlineExceeded);
            }
            crate::cancellation::check(cancellation)?;
            // Bounded waits keep deadlines and cancellation responsive.
            state = self
                .pending_changed
                .wait_timeout(state, std::time::Duration::from_millis(5))
                .map_err(|_| io::Error::other("commit coordinator poisoned"))?
                .0;
        }
        Ok(())
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, State>> {
        self.state.lock().map_err(|poison| {
            self.failed.store(true, Ordering::Release);
            self.notify_failed(&poison.into_inner());
            io::Error::other("write coordinator poisoned; reopen the store")
        })
    }

    fn wait<'a>(
        &self,
        condition: &Condvar,
        state: MutexGuard<'a, State>,
    ) -> io::Result<MutexGuard<'a, State>> {
        condition.wait(state).map_err(|poison| {
            self.failed.store(true, Ordering::Release);
            self.notify_failed(&poison.into_inner());
            io::Error::other("write coordinator poisoned; reopen the store")
        })
    }

    fn wait_request<'a>(
        &self,
        condition: &Condvar,
        state: MutexGuard<'a, State>,
    ) -> io::Result<MutexGuard<'a, State>> {
        condition
            .wait_timeout(state, LEADER_RECHECK_INTERVAL)
            .map(|(state, _)| state)
            .map_err(|poison| {
                self.failed.store(true, Ordering::Release);
                let (state, _) = poison.into_inner();
                self.notify_failed(&state);
                io::Error::other("write coordinator poisoned; reopen the store")
            })
    }

    fn fail(&self, error: io::Error) {
        self.failed.store(true, Ordering::Release);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.failure.get_or_insert_with(|| Failure {
            kind: error.kind(),
            message: format!("write outcome is uncertain; reopen the store: {error}"),
        });
        self.notify_failed(&state);
        state.active_completions.clear();
        state.queue.clear();
        state.buffered_bytes = 0;
        state.buffered_records = 0;
        state.active = false;
    }

    // Reserve queue capacity before doing allocation and checksumming. The
    // reservation keeps preparation bounded without holding the queue lock.
    fn reserve(&self, bytes: usize) -> io::Result<Reservation<'_>> {
        let mut state = self.lock()?;
        loop {
            if self.failed() {
                return Err(Self::failure(&state));
            }
            // Recheck after every wake: a caller can lower limits while this
            // writer is waiting. A record that no longer fits must not wait
            // indefinitely or be submitted after an admission rejection.
            if bytes > state.max_bytes || state.max_records == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "record exceeds configured group commit admission limits",
                ));
            }
            if state.buffered_bytes <= state.max_bytes - bytes
                && state.buffered_records < state.max_records
            {
                break;
            }
            state.admission_waiters += 1;
            state = self.wait(&self.admission_changed, state)?;
            state.admission_waiters -= 1;
        }
        state.buffered_bytes += bytes;
        state.buffered_records += 1;
        Ok(Reservation {
            coordinator: self,
            bytes,
            active: true,
        })
    }

    #[cfg(test)]
    pub(super) fn reserve_for_test(&self, bytes: usize) -> io::Result<impl Drop + '_> {
        self.reserve(bytes)
    }

    /// Submits one record and waits until it is synchronized, calling
    /// `after_enqueue` once it is in the submission queue. Any record
    /// submitted later is appended after this one.
    pub(super) fn commit_then(
        &self,
        log: &Log,
        inner: &RwLock<Inner>,
        (operation, key, value): (Operation, &[u8], &[u8]),
        tracker: Option<&crate::transaction_outcome::Tracker>,
        after_enqueue: impl FnOnce(),
    ) -> io::Result<()> {
        // Store::append validates key and value lengths before admission.
        let bytes = HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE;
        let reservation = self.reserve(bytes)?;
        let encoded = record::encode(operation, key, value)?;
        let checksum = crc32fast::hash(value);
        let mut entry = Entry::new(operation, key.to_vec(), encoded, value.len(), checksum);
        entry.pending_keys.push(key.to_vec());
        self.submit(
            log,
            inner,
            entry,
            reservation,
            tracker,
            (|_: &PendingWrites<'_>| Ok(()), after_enqueue),
        )
        .map_err(TimedTransactionError::into_io)
    }

    pub(super) fn commit_batch(
        &self,
        log: &Log,
        inner: &RwLock<Inner>,
        batch: BatchLayout<'_>,
        tracker: Option<&crate::transaction_outcome::Tracker>,
        before_submit: impl FnOnce(&PendingWrites<'_>) -> Result<(), TimedTransactionError>,
    ) -> Result<(), TimedTransactionError> {
        const PAYLOAD_START: usize = HEADER_SIZE + 1;
        let value_len = batch.payload_len();
        let reservation = self.reserve(batch.encoded_len())?;
        let encoded = batch.encode()?;
        let checksum = crc32fast::hash(&encoded[PAYLOAD_START..PAYLOAD_START + value_len]);
        let mut entry = Entry::new(Operation::Batch, vec![0], encoded, value_len, checksum);
        entry.pending_keys = batch
            .operations()
            .iter()
            .map(|operation| operation.fields().1.to_vec())
            .collect();
        self.submit(
            log,
            inner,
            entry,
            reservation,
            tracker,
            (before_submit, || {}),
        )
    }

    fn submit(
        &self,
        log: &Log,
        inner: &RwLock<Inner>,
        entry: Entry,
        mut reservation: Reservation<'_>,
        tracker: Option<&crate::transaction_outcome::Tracker>,
        (before_submit, after_enqueue): (
            impl FnOnce(&PendingWrites<'_>) -> Result<(), TimedTransactionError>,
            impl FnOnce(),
        ),
    ) -> Result<(), TimedTransactionError> {
        let completion = Arc::clone(&entry.completion);
        let mut state = self.lock()?;
        if self.failed() {
            let error = Self::failure(&state);
            drop(state);
            return Err(error.into());
        }
        // Read the control state under the queue mutex before publishing the
        // entry. Drop the mutex before unwinding the byte reservation.
        if let Err(error) = before_submit(&PendingWrites(&state.pending)) {
            drop(state);
            return Err(error);
        }
        // From this point another leader may dequeue and append the request.
        // Earlier reservation, encoding or coordinator failures cannot have
        // submitted this call, even if a different call remains uncertain.
        if let Some(tracker) = tracker {
            tracker.commit();
        }
        for key in &entry.pending_keys {
            *state.pending.entry(key.clone()).or_default() += 1;
        }
        state.queue.push_back(entry);
        reservation.active = false;
        // Still under the queue mutex, so no later record can be queued
        // ahead of this one.
        after_enqueue();
        loop {
            if completion.done.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.failed() {
                return Err(Self::failure(&state).into());
            }
            if !state.active {
                // Only one leader writes the log. It takes records already
                // queued without delaying an uncontended write to form a batch.
                state.active = true;
                let mut batch = Vec::with_capacity(MAX_BATCH_RECORDS);
                while batch.len() < MAX_BATCH_RECORDS {
                    if let Some(entry) = state.queue.pop_front() {
                        batch.push(entry);
                    } else {
                        break;
                    }
                }
                // Track in-flight waiters as well as the queued ones so a
                // sync failure or leader panic can wake both sets.
                state.active_completions.clear();
                state
                    .active_completions
                    .extend(batch.iter().map(|entry| Arc::clone(&entry.completion)));
                drop(state);
                let mut leader = Leader {
                    coordinator: self,
                    active: true,
                };
                match self.flush(log, inner, batch) {
                    Ok(()) => {
                        leader.active = false;
                    }
                    Err(error) => {
                        self.fail(error);
                        leader.active = false;
                    }
                }
                state = self.lock()?;
            } else {
                // Periodically recheck leadership in case a notified queue
                // head is delayed or unavailable. This is not a latency SLA.
                completion.waiting.store(true, Ordering::Relaxed);
                state = self.wait_request(&completion.changed, state)?;
                completion.waiting.store(false, Ordering::Relaxed);
            }
        }
    }

    fn flush(&self, log: &Log, inner: &RwLock<Inner>, batch: Vec<Entry>) -> io::Result<()> {
        let mut batch = batch;
        let locations = log.append_batch_unsynced(batch.iter().map(|entry| AppendRecord {
            encoded: &entry.encoded,
            key_len: entry.key.len(),
            value_len: entry.value_len,
            checksum: entry.checksum,
        }))?;
        // Fully validate every batch and allocate its index updates before
        // synchronization and before taking the publication lock. Once the
        // lock is held, publication has no fallible parsing or offset math.
        let mut batch_updates = Vec::with_capacity(batch.len());
        for (entry, &location) in batch.iter().zip(&locations) {
            if entry.operation == Operation::Batch {
                let start = HEADER_SIZE + entry.key.len();
                let end = start
                    .checked_add(entry.value_len)
                    .ok_or_else(|| record::invalid_data("batch offset overflow"))?;
                let payload = entry
                    .encoded
                    .get(start..end)
                    .ok_or_else(|| record::invalid_data("incomplete encoded batch"))?;
                batch_updates.push(Some(prepare_batch(payload, location)?));
            } else {
                batch_updates.push(None);
            }
        }
        #[cfg(test)]
        {
            let hook = self.sync_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        // Never publish an index update or complete a caller before sync.
        log.sync_data()?;
        // The coordinator admits only one publishing leader. Clone a shared
        // immutable root without holding the index write lock, so unrelated
        // readers can continue while the copy is prepared. A new snapshot
        // racing after this check falls back to copy-on-write under the lock.
        let shared = {
            let state = inner
                .read()
                .map_err(|_| io::Error::other("store lock poisoned"))?;
            (Arc::strong_count(&state.index) > 1)
                .then(|| (Arc::clone(&state.index), state.generation))
        };
        #[cfg(test)]
        if shared.is_some() {
            self.snapshot_copy_point(SnapshotCopyPoint::BeforeCopy);
        }
        let prepared = shared.map(|(root, generation)| {
            let copy = Arc::new(root.as_ref().clone());
            (root, generation, copy)
        });
        #[cfg(test)]
        if prepared.is_some() {
            self.snapshot_copy_point(SnapshotCopyPoint::AfterCopy);
        }
        {
            let mut inner = inner
                .write()
                .map_err(|_| io::Error::other("store lock poisoned"))?;
            if let Some((root, generation, copy)) = prepared {
                if !Arc::ptr_eq(&inner.index, &root) || inner.generation != generation {
                    self.failed.store(true, Ordering::Release);
                    return Err(io::Error::other("index changed during commit preparation"));
                }
                inner.index = copy;
            }
            let next_generation = inner
                .generation
                .checked_add(1)
                .ok_or_else(|| io::Error::other("commit generation exhausted; reopen the store"))?;
            let mut key_bytes = inner.index_key_bytes;
            let index = Arc::make_mut(&mut inner.index);
            let publication = (|| {
                for ((entry, location), updates) in
                    batch.iter_mut().zip(locations).zip(batch_updates)
                {
                    match entry.operation {
                        Operation::Put => put_index(
                            index,
                            std::mem::take(&mut entry.key),
                            location,
                            &mut key_bytes,
                        )?,
                        Operation::Delete => delete_index(index, &entry.key, &mut key_bytes)?,
                        Operation::Batch => {
                            for update in updates.expect("validated batch updates") {
                                if let Some(location) = update.location {
                                    put_index(index, update.key, location, &mut key_bytes)?;
                                } else {
                                    delete_index(index, &update.key, &mut key_bytes)?;
                                }
                            }
                        }
                    }
                }
                Ok::<(), io::Error>(())
            })();
            if let Err(error) = publication {
                // Accounting can fail after an earlier update in the batch.
                // Disable reads before releasing the index lock.
                self.failed.store(true, Ordering::Release);
                return Err(error);
            }
            inner.index_key_bytes = key_bytes;
            inner.generation = next_generation;
        }
        let mut state = self.lock()?;
        let bytes = batch.iter().map(|entry| entry.encoded.len()).sum::<usize>();
        state.buffered_bytes -= bytes;
        state.buffered_records -= batch.len();
        // Wake completed requests individually and invite the queue head to
        // lead the next batch. Timed rechecks let other writers take over if
        // that head is delayed; this is not a latency or fairness guarantee.
        for entry in batch {
            // Published: validation now sees these writes in the index.
            for key in &entry.pending_keys {
                if let Some(count) = state.pending.get_mut(key.as_slice()) {
                    *count -= 1;
                    if *count == 0 {
                        state.pending.remove(key.as_slice());
                    }
                }
            }
            entry.completion.done.store(true, Ordering::Release);
            if entry.completion.waiting.load(Ordering::Relaxed) {
                entry.completion.changed.notify_one();
            }
        }
        self.pending_changed.notify_all();
        state.active_completions.clear();
        state.active = false;
        if let Some(next) = state.queue.front() {
            next.completion.changed.notify_one();
        }
        if state.admission_waiters > 0 {
            self.admission_changed.notify_all();
        }
        Ok(())
    }
}

// A failed or panicking preparation cannot strand the capacity it reserved.
// Once the record is queued, its batch owns that capacity instead.
struct Reservation<'a> {
    coordinator: &'a Coordinator,
    bytes: usize,
    active: bool,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.coordinator.state.lock().unwrap_or_else(|poison| {
            self.coordinator.failed.store(true, Ordering::Release);
            let state = poison.into_inner();
            self.coordinator.notify_failed(&state);
            state
        });
        // Failure resets accounting for every queued and reserved write.
        if !self.coordinator.failed() {
            state.buffered_bytes -= self.bytes;
            state.buffered_records -= 1;
            if state.admission_waiters > 0 {
                self.coordinator.admission_changed.notify_all();
            }
        }
    }
}

// A panic during append, sync, or index publication must wake waiters and
// invalidate the open store, even though this batch's outcome is unknown.
struct Leader<'a> {
    coordinator: &'a Coordinator,
    active: bool,
}
impl Drop for Leader<'_> {
    fn drop(&mut self) {
        if self.active {
            self.coordinator
                .fail(io::Error::other("writer panicked while committing"));
        }
    }
}
