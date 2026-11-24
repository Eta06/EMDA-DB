//! Admission, durable append and publication for checkpoint-backed writes.
use super::*;
use crate::log::ValueLocation;
use crate::record::batch::BatchLayout;
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
pub(super) enum Mutation<'a> {
    Single(Operation, &'a [u8], &'a [u8]),
    Batch(BatchLayout<'a>),
}

impl<'a> Mutation<'a> {
    fn operations(self) -> u64 {
        match self {
            Self::Single(..) => 1,
            Self::Batch(batch) => batch.operations().len() as u64,
        }
    }

    fn keys(self) -> impl Iterator<Item = &'a [u8]> {
        let (key, batch) = match self {
            Self::Single(_, key, _) => (Some(key), &[][..]),
            Self::Batch(batch) => (None, batch.operations()),
        };
        key.into_iter().chain(batch.iter().map(|op| op.fields().1))
    }
}

// Readers may hold the old published view during an append. Publish a
// failure even on unwind before another writer can take the gate.
struct PendingAppend<'a> {
    failed: &'a AtomicBool,
    published: bool,
}

impl Drop for PendingAppend<'_> {
    fn drop(&mut self) {
        if !self.published {
            self.failed.store(true, Ordering::Release);
        }
    }
}

impl CheckpointStore {
    // The writer gate serializes append and maintenance. Only admission and
    // publishing the synchronized batch need exclusive access to the view.
    pub(super) fn commit<'a>(
        &'a self,
        state: StateWrite<'a>,
        mutation: Mutation<'_>,
        tracker: Option<&crate::transaction_outcome::Tracker>,
    ) -> io::Result<()> {
        match self.commit_until(
            state,
            mutation,
            crate::cancellation::CommitControl {
                tracker,
                ..Default::default()
            },
        ) {
            Ok(()) => Ok(()),
            Err(crate::TimedTransactionError::Operation(error)) => Err(error),
            Err(crate::TimedTransactionError::DeadlineExceeded) => {
                unreachable!("a commit without a deadline cannot expire")
            }
        }
    }

    pub(super) fn commit_until<'a>(
        &'a self,
        mut state: StateWrite<'a>,
        mutation: Mutation<'_>,
        control: crate::cancellation::CommitControl<'_>,
    ) -> Result<(), crate::TimedTransactionError> {
        let crate::cancellation::CommitControl {
            deadline,
            cancellation,
            tracker,
        } = control;
        // Refuse a mutation before appending if a transaction generation can
        // no longer advance; wrapping would let a stale transaction commit.
        let next_generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("checkpoint transaction generation exhausted"))?;
        let (operation, key, value_len) = match mutation {
            Mutation::Single(operation, key, value) => (operation, key, value.len()),
            Mutation::Batch(batch) => (Operation::Batch, &[0][..], batch.payload_len()),
        };
        let length = HEADER_SIZE + key.len() + value_len + CHECKSUM_SIZE;
        let next_log = state
            .view
            .base
            .log_boundary()
            .checked_add(state.view.tail_bytes)
            .and_then(|size| size.checked_add(length as u64))
            .ok_or_else(|| io::Error::other("log length overflow"))?;
        if self.max_log_bytes.is_some_and(|max| next_log > max) {
            return Err(io::Error::other("configured log byte limit would be exceeded").into());
        }
        let admission = self.admit_suffix(&state, length, mutation);
        let (next_tail, next_operations, new_key_bytes) = match admission {
            Ok(admitted) => admitted,
            Err(_)
                if state.automatic
                    && state.view.tail_bytes > 0
                    && self.fits_empty_suffix(length, mutation) =>
            {
                state = self.automatic_rollover(state)?;
                self.admit_suffix(&state, length, mutation)?
            }
            Err(error) => return Err(error.into()),
        };
        let owned_key = (operation != Operation::Batch && !state.view.overlay.contains_key(key))
            .then(|| Arc::<[u8]>::from(key));
        // A snapshot can pin this overlay. Detach before submission, keeping
        // the same entries until the append and its witness are synchronized.
        // Immutable keys are shared between the old and new maps.
        let next_overlay = (Arc::strong_count(&state.view.overlay) > 1)
            .then(|| Arc::new(state.view.overlay.as_ref().clone()));
        let StateWrite { state, _writer } = state;
        // Keep the writer gate but let readers use the published view while
        // we encode the admitted mutation.
        drop(state);
        // next_log was checked above. Rollover changes the base and suffix,
        // but leaves their combined log length and every value offset intact.
        let record_start = next_log - length as u64;
        let mut updates = Vec::new();
        let put_location = |offset: usize, value: &[u8]| ValueLocation {
            offset: record_start + offset as u64,
            length: value.len(),
            checksum: crc32fast::hash(value),
        };
        let (encoded, location) = match mutation {
            Mutation::Single(operation, key, value) => {
                let encoded = record::encode(operation, key, value)?;
                let location = (operation == Operation::Put)
                    .then(|| put_location(HEADER_SIZE + key.len(), value));
                (encoded, location)
            }
            Mutation::Batch(batch) => {
                updates
                    .try_reserve_exact(batch.operations().len())
                    .map_err(|_| io::Error::other("checkpoint batch update allocation failed"))?;
                let encoded = batch.encode_with_children(|child, offset| {
                    let (operation, key, value) = child.fields();
                    let location =
                        (operation == Operation::Put).then(|| put_location(offset, value));
                    updates.push((key, location));
                })?;
                (encoded, None)
            }
        };
        debug_assert_eq!(encoded.len(), length);
        // Maintenance, detaching a shared overlay and encoding can take time.
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(crate::TimedTransactionError::DeadlineExceeded);
        }
        crate::cancellation::check(cancellation)?;
        // Past this point a returned error may follow an append. The tracker
        // records only this transaction, not effects of automatic maintenance.
        if let Some(tracker) = tracker {
            tracker.commit();
        }
        let mut pending = PendingAppend {
            failed: &self.append_failed,
            published: false,
        };
        self.log.append_checkpoint_record(record_start, &encoded)?;
        self.log.sync_data()?;
        let mut state = self.state.write().map_err(|_| uncertain())?;
        if let Some(overlay) = next_overlay {
            state.view.overlay = overlay;
        }
        // A snapshot may have pinned the formerly exclusive overlay during
        // the sync. It must keep the old entries through this publication.
        let overlay = Arc::make_mut(&mut state.view.overlay);
        if operation == Operation::Batch {
            for (key, location) in updates {
                if let Some(existing) = overlay.get_mut(key) {
                    *existing = location;
                } else {
                    overlay.insert(key.into(), location);
                }
            }
        } else {
            if let Some(key) = owned_key {
                overlay.insert(key, location);
            } else {
                *overlay
                    .get_mut(key)
                    .expect("writer gate holds admitted key") = location;
            }
        }
        state.view.tail_bytes = next_tail;
        state.view.tail_operations = next_operations;
        state.overlay_key_bytes = new_key_bytes;
        state.generation = next_generation;
        pending.published = true;
        Ok(())
    }

    fn admit_suffix(
        &self,
        state: &State,
        length: usize,
        mutation: Mutation<'_>,
    ) -> io::Result<(u64, u64, usize)> {
        let next_tail = state
            .view
            .tail_bytes
            .checked_add(length as u64)
            .filter(|total| *total <= self.max_tail_bytes)
            .ok_or_else(|| io::Error::other("checkpoint suffix byte limit would be exceeded"))?;
        let next_operations = state
            .view
            .tail_operations
            .checked_add(mutation.operations())
            .filter(|&count| self.max_tail_operations.is_none_or(|limit| count <= limit))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "checkpoint suffix operation limit exceeded",
                )
            })?;
        let new_keys = mutation
            .keys()
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
        Ok((next_tail, next_operations, new_key_bytes))
    }

    fn fits_empty_suffix(&self, length: usize, mutation: Mutation<'_>) -> bool {
        if length as u64 > self.max_tail_bytes
            || self
                .max_tail_operations
                .is_some_and(|limit| mutation.operations() > limit)
        {
            return false;
        }
        let keys = mutation.keys().collect::<BTreeSet<_>>();
        if keys.len() > self.max_tail_keys {
            return false;
        }
        keys.into_iter()
            .try_fold(0_usize, |sum, key| sum.checked_add(key.len()))
            .is_some_and(|bytes| bytes <= self.max_tail_key_bytes)
    }
}
