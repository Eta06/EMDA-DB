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
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::{Path, PathBuf};
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
/// checkpoint, compact or reclaim space. If a limit fills, `rollover_to` can
/// create and switch to a fresh checkpoint while retaining exclusive ownership.
/// Its digest must be protected independently. A failed append/sync has an
/// unknown outcome and poisons this instance until reopened.
pub struct CheckpointStore {
    // Drop the writable log before the view, which owns the exclusive lock.
    log: Log,
    state: RwLock<State>,
    path: PathBuf,
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
    /// A complete unacknowledged tail is validated and synchronized before
    /// it is adopted; incomplete or invalid tails fail without truncation.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
    ) -> io::Result<Self> {
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            Log::open_checkpoint_writer,
        )
    }

    fn open_using(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
        open_writer: impl FnOnce(&Path, u64, u64) -> io::Result<Log>,
    ) -> io::Result<Self> {
        RecoveredCheckpointView::validate_limits(
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
        )?;
        let path = std::fs::canonicalize(path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("store.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let base = CheckpointView::open_locked(&path, checkpoint, trusted_digest, lock)?;
        let view = RecoveredCheckpointView::from_base_for_writer(
            base,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
        )?;
        let boundary = view.base.witnessed_boundary;
        let length = view
            .base
            .log_boundary()
            .checked_add(view.tail_bytes)
            .ok_or_else(|| record::invalid_data("checkpoint suffix length overflow"))?;
        let log = open_writer(&path, boundary, length)?;
        if length > boundary {
            log.sync_data().map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("failed to synchronize recovered tail; outcome is uncertain: {error}"),
                )
            })?;
        }
        let overlay_key_bytes = view.overlay.keys().map(Vec::len).sum();
        Ok(Self {
            log,
            state: RwLock::new(State {
                view,
                overlay_key_bytes,
                failed: false,
            }),
            path,
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

    /// Creates a verified checkpoint by streaming the on-disk index and the
    /// bounded suffix overlay while mutations are paused. The independently
    /// trusted digest must be retained separately. The current instance keeps
    /// using its original checkpoint; close it and reopen with the new one to
    /// clear the overlay. A publication sync error has an uncertain outcome.
    pub fn checkpoint_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.checkpoint_to_with_sync(destination.as_ref(), |parent| {
            std::fs::File::open(parent)?.sync_all()
        })
    }

    fn checkpoint_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        let state = self.write()?;
        self.create_checkpoint(&state, destination, sync_parent)
    }

    /// Creates a checkpoint and switches the running instance to it under the
    /// same exclusive lock, clearing the suffix overlay after verification.
    /// The returned digest must be persisted separately by the caller. An
    /// error after publication can leave the new checkpoint at its destination;
    /// the instance continues using the previous checkpoint in that case.
    pub fn rollover_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.rollover_to_with_sync(destination.as_ref(), |parent| {
            std::fs::File::open(parent)?.sync_all()
        })
    }

    fn rollover_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        let mut state = self.write()?;
        let digest = self.create_checkpoint(&state, destination, sync_parent)?;
        let next_view = (|| {
            let lock = state.view.base.duplicate_store_lock()?;
            let base = CheckpointView::open_locked(&self.path, destination, &digest, lock)?;
            let view = RecoveredCheckpointView::from_base(
                base,
                self.max_tail_bytes,
                self.max_tail_keys,
                self.max_tail_key_bytes,
            )?;
            if view.tail_bytes != 0 || !view.overlay.is_empty() {
                return Err(record::invalid_data(
                    "new checkpoint has an unexpected suffix",
                ));
            }
            Ok(view)
        })()
        .map_err(|error: io::Error| {
            io::Error::new(
                error.kind(),
                format!("checkpoint was published but switching failed: {error}"),
            )
        })?;
        state.view = next_view;
        state.overlay_key_bytes = 0;
        Ok(digest)
    }

    fn create_checkpoint(
        &self,
        state: &State,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        let (count, peak) = self.verify_for_checkpoint(state)?;
        crate::checkpoint::create_streaming_with_sync(
            &self.log,
            count,
            peak,
            destination,
            |emit| {
                state.view.visit_entries(None, None, |key, location| {
                    emit(key, location)?;
                    Ok(std::ops::ControlFlow::Continue(()))
                })
            },
            sync_parent,
        )
    }

    // Revalidate the separately trusted base and replay the entire bounded
    // suffix, checking both the live overlay and the historical key peak.
    fn verify_for_checkpoint(&self, state: &State) -> io::Result<(u64, usize)> {
        self.log.verify_witness()?;
        let view = &state.view;
        view.base.verify_for_rollover()?;
        let boundary = self.log.len()?;
        if boundary.checked_sub(view.base.log_boundary()) != Some(view.tail_bytes) {
            return Err(record::invalid_data("checkpoint suffix length changed"));
        }
        let mut current = usize::try_from(view.base.len())
            .map_err(|_| record::invalid_data("checkpoint key count does not fit this platform"))?;
        let mut peak = view.base.historical_peak();
        let touched_bytes = view.overlay.keys().try_fold(0_usize, |sum, key| {
            sum.checked_add(key.len())
                .ok_or_else(|| record::invalid_data("checkpoint suffix key byte count overflow"))
        })?;
        if view.tail_bytes > self.max_tail_bytes
            || view.overlay.len() > self.max_tail_keys
            || touched_bytes > self.max_tail_key_bytes
            || touched_bytes != state.overlay_key_bytes
        {
            return Err(record::invalid_data(
                "checkpoint overlay exceeds its limits",
            ));
        }
        // Resolve initial membership by merging the sorted touched keys with
        // checkpoint blocks, instead of rereading an index block per key.
        // The nested option distinguishes an unseen key from a tombstone.
        let mut probes = view
            .overlay
            .keys()
            .cloned()
            .map(|key| (key, (false, None)))
            .collect::<BTreeMap<_, (bool, Option<Option<crate::log::ValueLocation>>)>>();
        if let Some(first) = probes.keys().next().cloned() {
            probes.get_mut(&first).expect("first probe exists").0 =
                view.base.location(&first)?.is_some();
            let mut pending = probes
                .range_mut::<[u8], _>((Excluded(first.as_slice()), Unbounded))
                .peekable();
            if pending.peek().is_some() {
                view.base.visit_entries(Some(&first), None, |base_key, _| {
                    while pending
                        .peek()
                        .is_some_and(|(key, _)| key.as_slice() < base_key)
                    {
                        pending.next();
                    }
                    if pending
                        .peek()
                        .is_some_and(|(key, _)| key.as_slice() == base_key)
                    {
                        pending.next().expect("peeked probe").1.0 = true;
                    }
                    if pending.peek().is_none() {
                        Ok(std::ops::ControlFlow::Break(()))
                    } else {
                        Ok(std::ops::ControlFlow::Continue(()))
                    }
                })?;
            }
        }
        self.log
            .visit_records(view.base.log_boundary(), Some(boundary), |key, location| {
                let probe = probes
                    .get_mut(&key)
                    .ok_or_else(|| record::invalid_data("verified suffix has an unexpected key"))?;
                match (probe.0, location.is_some()) {
                    (false, true) => {
                        current = current.checked_add(1).ok_or_else(|| {
                            record::invalid_data("checkpoint live key count overflow")
                        })?
                    }
                    (true, false) => {
                        current = current.checked_sub(1).ok_or_else(|| {
                            record::invalid_data("checkpoint live key count underflow")
                        })?
                    }
                    _ => {}
                }
                peak = peak.max(current);
                probe.0 = location.is_some();
                probe.1 = Some(location);
                Ok(())
            })?;
        if view
            .overlay
            .iter()
            .any(|(key, location)| probes.get(key).and_then(|probe| probe.1) != Some(*location))
        {
            return Err(record::invalid_data(
                "checkpoint overlay does not match verified suffix",
            ));
        }
        self.log.verify_witness()?;
        Ok((current as u64, peak))
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
