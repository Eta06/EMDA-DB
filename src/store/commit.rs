//! Bounded group commit for independent, single-key writes.
//!
//! A single leader appends queued records in order, synchronizes the log, and
//! publishes their index updates before waking the callers. Grouping is not a
//! multi-key transaction: a failed or interrupted group has unknown outcomes.
use super::Inner;
use crate::log::Log;
use crate::record::{self, CHECKSUM_SIZE, HEADER_SIZE, Operation};
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::Duration;

// These bound encoded bytes and entry count, not total process memory. Caller
// buffers, keys, allocator overhead, thread stacks, and the index are separate.
const MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_RECORDS: usize = 64;
const MAX_BUFFERED_RECORDS: usize = 1024;
const LEADER_RECHECK_INTERVAL: Duration = Duration::from_millis(10);

struct Entry {
    operation: Operation,
    key: Vec<u8>,
    encoded: Vec<u8>,
    value_len: usize,
    checksum: u32,
    completion: Arc<Completion>,
}

// A request has one waiter. All condition variables use the same state mutex,
// so checking the predicate and going to sleep cannot miss a notification.
struct Completion {
    done: AtomicBool,
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
    #[cfg(test)]
    admission_waiters: usize,
    active: bool,
    active_completions: Vec<Arc<Completion>>,
    failure: Option<Failure>,
}

pub(super) struct Coordinator {
    state: Mutex<State>,
    admission_changed: Condvar,
    failed: AtomicBool,
    max_bytes: usize,
    max_records: usize,
}

impl Coordinator {
    pub(super) fn new() -> Self {
        Self::with_limits(MAX_BUFFERED_BYTES, MAX_BUFFERED_RECORDS)
    }

    fn with_limits(max_bytes: usize, max_records: usize) -> Self {
        Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                buffered_bytes: 0,
                buffered_records: 0,
                #[cfg(test)]
                admission_waiters: 0,
                active: false,
                active_completions: Vec::new(),
                failure: None,
            }),
            admission_changed: Condvar::new(),
            failed: AtomicBool::new(false),
            max_bytes,
            max_records,
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(max_bytes: usize, max_records: usize) -> Self {
        Self::with_limits(max_bytes, max_records)
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
                changed: Condvar::new(),
            }),
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

    pub(super) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
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

    pub(super) fn commit(
        &self,
        log: &Log,
        inner: &RwLock<Inner>,
        operation: Operation,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<()> {
        // Store::append validates key and value lengths before admission.
        let bytes = HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE;
        let completion = Arc::new(Completion {
            done: AtomicBool::new(false),
            changed: Condvar::new(),
        });
        let mut state = self.lock()?;
        if bytes > self.max_bytes {
            return Err(io::Error::other("record exceeds the write buffer limit"));
        }
        while !self.failed()
            && (state.buffered_bytes + bytes > self.max_bytes
                || state.buffered_records >= self.max_records)
        {
            #[cfg(test)]
            {
                state.admission_waiters += 1;
            }
            state = self.wait(&self.admission_changed, state)?;
            #[cfg(test)]
            {
                state.admission_waiters -= 1;
            }
        }
        if self.failed() {
            return Err(Self::failure(&state));
        }
        let encoded = record::encode(operation, key, value)?;
        state.queue.push_back(Entry {
            operation,
            key: key.to_vec(),
            encoded,
            value_len: value.len(),
            checksum: crc32fast::hash(value),
            completion: Arc::clone(&completion),
        });
        state.buffered_bytes += bytes;
        state.buffered_records += 1;
        loop {
            if completion.done.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.failed() {
                return Err(Self::failure(&state));
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
                state = self.wait_request(&completion.changed, state)?;
            }
        }
    }

    fn flush(&self, log: &Log, inner: &RwLock<Inner>, batch: Vec<Entry>) -> io::Result<()> {
        let mut batch = batch;
        let mut locations = Vec::with_capacity(batch.len());
        for entry in &batch {
            locations.push(log.append_unsynced(
                &entry.encoded,
                entry.key.len(),
                entry.value_len,
                entry.checksum,
            )?);
        }
        // Never publish an index update or complete a caller before sync.
        log.sync_data()?;
        {
            let mut inner = inner
                .write()
                .map_err(|_| io::Error::other("store lock poisoned"))?;
            for (entry, location) in batch.iter_mut().zip(locations) {
                match entry.operation {
                    Operation::Put => {
                        inner.index.insert(std::mem::take(&mut entry.key), location);
                    }
                    Operation::Delete => {
                        inner.index.remove(&entry.key);
                    }
                }
            }
        }
        let mut state = self.lock()?;
        let bytes = batch.iter().map(|entry| entry.encoded.len()).sum::<usize>();
        state.buffered_bytes -= bytes;
        state.buffered_records -= batch.len();
        // Wake completed requests individually and invite the queue head to
        // lead the next batch. Timed rechecks let other writers take over if
        // that head is delayed; this is not a latency or fairness guarantee.
        for entry in batch {
            entry.completion.done.store(true, Ordering::Release);
            entry.completion.changed.notify_one();
        }
        state.active_completions.clear();
        state.active = false;
        if let Some(next) = state.queue.front() {
            next.completion.changed.notify_one();
        }
        self.admission_changed.notify_all();
        Ok(())
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
