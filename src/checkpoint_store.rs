//! Experimental writable checkpoint index with a bounded log-suffix overlay.
use crate::Entry;
use crate::checkpoint_view::CheckpointView;
use crate::log::{AppendRecord, BatchUpdate, Log, prepare_batch};
use crate::record::batch::{BatchOperation, encode_payload};
use crate::record::{self, CHECKSUM_SIZE, HEADER_SIZE, Operation};
use crate::recovered_view::{
    MAX_RECOVERED_VIEW_TAIL_BYTES, MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS,
    RecoveredCheckpointView,
};
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

struct State {
    view: RecoveredCheckpointView,
    overlay_key_bytes: usize,
    failed: bool,
}

/// An experimental writable store backed by a verified on-disk checkpoint.
///
/// Point operations and blind batches serialize through one write lock. A
/// bounded in-memory overlay records every distinct suffix key, including
/// tombstones; opening still verifies the complete covered log prefix. Limits
/// are not total process-memory quotas. This type excludes other cooperating
/// stores/views, has no transactions or snapshots, and does not automatically
/// checkpoint, compact or reclaim space. If a limit fills, create a fresh
/// checkpoint using the ordinary store before reopening this type. A failed
/// append/sync has an unknown outcome and poisons this instance until reopened.
pub struct CheckpointStore {
    // Drop the writable log before the view, which owns the exclusive lock.
    log: Log,
    state: RwLock<State>,
    max_tail_bytes: u64,
    max_tail_keys: usize,
    max_tail_key_bytes: usize,
}

impl CheckpointStore {
    /// Opens an existing witnessed store and checkpoint with the hard caps.
    pub fn open(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
    ) -> io::Result<Self> {
        Self::open_with_limits(
            path,
            checkpoint,
            trusted_digest,
            MAX_RECOVERED_VIEW_TAIL_BYTES,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        )
    }

    /// Opens with limits no greater than the recovered view's hard limits.
    /// Complete but unacknowledged log tails are rejected, not adopted.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
    ) -> io::Result<Self> {
        RecoveredCheckpointView::validate_limits(
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
        )?;
        let path = path.as_ref();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("store.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let base = CheckpointView::open_locked(path, checkpoint.as_ref(), trusted_digest, lock)?;
        let view = RecoveredCheckpointView::from_base(
            base,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
        )?;
        let log = Log::open_checkpoint_writer(path, view.base.witnessed_boundary)?;
        let overlay_key_bytes = view.overlay.keys().map(Vec::len).sum();
        Ok(Self {
            log,
            state: RwLock::new(State {
                view,
                overlay_key_bytes,
                failed: false,
            }),
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
        })
    }

    fn read(&self) -> io::Result<RwLockReadGuard<'_, State>> {
        let state = self.state.read().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        Ok(state)
    }

    fn write(&self) -> io::Result<RwLockWriteGuard<'_, State>> {
        let state = self.state.write().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        Ok(state)
    }

    /// Reads a key from the verified checkpoint and committed overlay.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.read()?.view.get(key)
    }

    /// Reads one ordered page. Separate calls do not form a snapshot.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        self.read()?.view.scan(after, before, limit)
    }

    /// Number of distinct suffix keys held in memory, including tombstones.
    pub fn overlay_keys(&self) -> io::Result<usize> {
        Ok(self.read()?.view.overlay_keys())
    }

    /// Current byte length of the witnessed suffix since the checkpoint.
    pub fn tail_bytes(&self) -> io::Result<u64> {
        Ok(self.read()?.view.tail_bytes())
    }

    /// Replaces a key and synchronizes the mutation before returning.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        record::validate(Operation::Put, key, value)?;
        let mut state = self.write()?;
        self.commit(&mut state, Operation::Put, key, value, &[key])
    }

    /// Deletes a key, including an absent key, as a synchronized mutation.
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        record::validate(Operation::Delete, key, &[])?;
        let mut state = self.write()?;
        self.commit(&mut state, Operation::Delete, key, &[], &[key])
    }

    /// Conditionally replaces or deletes one key atomically against all writes.
    /// This does not detect ABA changes or provide a multi-key transaction.
    pub fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
    ) -> io::Result<bool> {
        let (operation, value) = match replacement {
            Some(value) => (Operation::Put, value),
            None => (Operation::Delete, &[][..]),
        };
        record::validate(operation, key, value)?;
        let mut state = self.write()?;
        if state.view.get(key)?.as_deref() != expected {
            return Ok(false);
        }
        self.commit(&mut state, operation, key, value, &[key])?;
        Ok(true)
    }

    /// Atomically adds to a little-endian signed 64-bit value, starting at zero.
    /// Invalid lengths and overflow fail without appending a record.
    pub fn increment_i64(&self, key: &[u8], delta: i64) -> io::Result<i64> {
        record::validate_key(key)?;
        let mut state = self.write()?;
        let current = match state.view.get(key)? {
            None => 0,
            Some(bytes) => i64::from_le_bytes(bytes.try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "increment requires an eight-byte signed integer",
                )
            })?),
        };
        let next = current.checked_add(delta).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "integer increment overflow")
        })?;
        self.commit(&mut state, Operation::Put, key, &next.to_le_bytes(), &[key])?;
        Ok(next)
    }

    /// Applies a blind ordered batch atomically as one synchronized record.
    /// This is not a read-modify-write transaction.
    pub fn write_batch(&self, operations: &[BatchOperation<'_>]) -> io::Result<()> {
        let mut state = self.write()?;
        let payload = encode_payload(operations)?;
        let keys = operations
            .iter()
            .map(|operation| operation.fields().1)
            .collect::<Vec<_>>();
        self.commit(&mut state, Operation::Batch, &[0], &payload, &keys)
    }

    // The state write lock serializes admission, append, synchronization and
    // publication. It also prevents readers seeing a partially applied batch.
    fn commit(
        &self,
        state: &mut State,
        operation: Operation,
        key: &[u8],
        value: &[u8],
        touched: &[&[u8]],
    ) -> io::Result<()> {
        let length = HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE;
        let next_tail = state
            .view
            .tail_bytes
            .checked_add(length as u64)
            .filter(|total| *total <= self.max_tail_bytes)
            .ok_or_else(|| io::Error::other("checkpoint suffix byte limit would be exceeded"))?;
        let new_keys = touched
            .iter()
            .copied()
            .filter(|key| !state.view.overlay.contains_key(*key))
            .collect::<BTreeSet<_>>();
        if state.view.overlay.len() + new_keys.len() > self.max_tail_keys {
            return Err(io::Error::other(
                "checkpoint suffix key limit would be exceeded",
            ));
        }
        let new_key_bytes = new_keys
            .iter()
            .try_fold(state.overlay_key_bytes, |sum, key| {
                sum.checked_add(key.len())
                    .filter(|total| *total <= self.max_tail_key_bytes)
                    .ok_or_else(|| {
                        io::Error::other("checkpoint suffix key byte limit would be exceeded")
                    })
            })?;
        // Prepare all input before append. Once append starts, even a failed
        // verification or panic leaves the instance unusable until recovery.
        let encoded = record::encode(operation, key, value)?;
        let owned_key = (operation != Operation::Batch).then(|| key.to_vec());
        state.failed = true;
        let location = self
            .log
            .append_batch_unsynced(std::iter::once(AppendRecord {
                encoded: &encoded,
                key_len: key.len(),
                value_len: value.len(),
                checksum: crc32fast::hash(value),
            }))?
            .into_iter()
            .next()
            .expect("one appended record returns one location");
        let updates = if operation == Operation::Batch {
            prepare_batch(value, location)?
        } else {
            vec![BatchUpdate {
                key: owned_key.expect("single record owns its key"),
                location: (operation == Operation::Put).then_some(location),
            }]
        };
        self.log.sync_data()?;
        for update in updates {
            state.view.overlay.insert(update.key, update.location);
        }
        state.view.tail_bytes = next_tail;
        state.overlay_key_bytes = new_key_bytes;
        state.failed = false;
        Ok(())
    }
}

fn uncertain() -> io::Error {
    io::Error::other("a previous write may have an uncertain outcome; reopen the store")
}

#[cfg(test)]
mod tests;
