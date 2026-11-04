//! Conservative optimistic transactions over the checkpoint-backed store.
use super::*;
use crate::log::ValueLocation;
use crate::record::batch::{MAX_ATOMIC_BATCH_BYTES, MAX_ATOMIC_BATCH_OPERATIONS};
use crate::transaction_budget::PendingReservation;
use std::ops::ControlFlow;

/// A bounded, process-local transaction on a checkpoint-backed store.
///
/// Reads and scans see staged changes. Every read checks that no write has
/// committed since the transaction began; otherwise it returns `WouldBlock`.
/// A successful callback is checked again before its changes are committed.
/// Conflicts include unrelated writes and ABA changes. Callers must retry the
/// entire callback themselves and manage any external side effects.
pub struct CheckpointTransaction<'a> {
    store: &'a CheckpointStore,
    generation: u64,
    staged: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    encoded_bytes: usize,
}

impl Drop for CheckpointTransaction<'_> {
    fn drop(&mut self) {
        self.staged.clear();
        if self.encoded_bytes > 4 {
            self.store
                .transaction_budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(self.encoded_bytes - 4);
        }
    }
}

impl CheckpointTransaction<'_> {
    fn state(&self) -> io::Result<RwLockReadGuard<'_, State>> {
        let state = self.store.read()?;
        if state.generation != self.generation {
            return Err(conflict());
        }
        Ok(state)
    }

    /// Reads the transaction's staged value or its consistent starting state.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let state = self.state()?;
        match self.staged.get(key) {
            Some(value) => Ok(value.clone()),
            None => state.view.get(key),
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

    /// Scans the consistent starting state merged with staged changes.
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
        self.run_transaction(action, false)
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
        self.run_transaction(action, true)
    }

    fn run_transaction<T>(
        &self,
        action: impl FnOnce(&mut CheckpointTransaction<'_>) -> io::Result<T>,
        fifo: bool,
    ) -> io::Result<T> {
        // Admission covers both callback and commit. Release the write guard
        // and staged reservations before releasing the queue turn on unwind.
        let _turn = if fifo {
            Some(self.transaction_queue.enter()?)
        } else {
            None
        };
        let generation = self.read()?.generation;
        let mut transaction = CheckpointTransaction {
            store: self,
            generation,
            staged: BTreeMap::new(),
            encoded_bytes: 4,
        };
        let result = action(&mut transaction)?;
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
        let mut state = self.write()?;
        if state.generation != generation {
            return Err(conflict());
        }
        if let Some(payload) = payload {
            let keys = operations
                .iter()
                .map(|operation| operation.fields().1)
                .collect::<Vec<_>>();
            self.commit(&mut state, Operation::Batch, &[0], &payload, &keys)?;
        }
        Ok(result)
    }
}

fn conflict() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "checkpoint transaction conflict: a write committed since the transaction began",
    )
}
