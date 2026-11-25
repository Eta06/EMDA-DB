//! Admission, durable append and publication for checkpoint-backed writes.
use super::*;
use crate::log::ValueLocation;
use crate::record::batch::BatchLayout;
use std::fmt;

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

    fn distinct_keys(self) -> io::Result<MutationKeys<'a>> {
        match self {
            Self::Single(_, key, _) => Ok(MutationKeys::Single([key])),
            Self::Batch(batch) => {
                if let [child] = batch.operations() {
                    return Ok(MutationKeys::Single([child.fields().1]));
                }
                let mut keys = Vec::new();
                keys.try_reserve_exact(batch.operations().len())
                    .map_err(|_| io::Error::other("checkpoint batch key allocation failed"))?;
                keys.extend(batch.operations().iter().map(|child| child.fields().1));
                keys.sort_unstable();
                keys.dedup();
                Ok(MutationKeys::Batch(keys))
            }
        }
    }
}

enum MutationKeys<'a> {
    Single([&'a [u8]; 1]),
    Batch(Vec<&'a [u8]>),
}

impl<'a> MutationKeys<'a> {
    fn as_slice(&self) -> &[&'a [u8]] {
        match self {
            Self::Single(key) => key,
            Self::Batch(keys) => keys,
        }
    }
}

// Readers may hold the old published view during an append. Publish a
// failure even on unwind before another writer can take the gate.
struct PendingAppend<'a> {
    failed: &'a AtomicBool,
    published: bool,
}

#[derive(Debug)]
struct AutomaticRolloverRejection(crate::ManagedRolloverFailure);

impl fmt::Display for AutomaticRolloverRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.io_error(), formatter)
    }
}

impl std::error::Error for AutomaticRolloverRejection {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
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
        // The disk budget charges the next log length just before the append,
        // after any suffix refusal or rollover, so a refused mutation leaves
        // nothing charged. A rollover's reservation carries the growth.
        let mut reservation = None;
        let operations = mutation.operations();
        let can_rollover = state.automatic
            && state.view.tail_bytes > 0
            && length as u64 <= self.max_tail_bytes
            && self.max_tail_operations.is_none_or(|max| operations <= max);
        let current = self.admit_suffix_length(
            state.view.tail_bytes,
            state.view.tail_operations,
            length,
            operations,
        );
        let current = match current {
            Err(error) if !can_rollover => return Err(error.into()),
            current => current,
        };
        let distinct = mutation.distinct_keys()?;
        let keys = distinct.as_slice();
        let admitted = current.and_then(|(bytes, operations)| {
            self.admit_suffix_keys(&state, keys)
                .map(|key_bytes| (bytes, operations, key_bytes))
        });
        let (next_tail, next_operations, new_key_bytes) = match admitted {
            Ok(value) => value,
            Err(_) if can_rollover && self.fits_empty_suffix_keys(keys) => {
                let rolled;
                (rolled, reservation) = self
                    .automatic_rollover_for_append(state, next_log)
                    .map_err(|failure| {
                        if tracker.is_some() {
                            io::Error::new(
                                failure.io_error().kind(),
                                AutomaticRolloverRejection(failure),
                            )
                        } else {
                            failure.into_io_error()
                        }
                    })?;
                state = rolled;
                (
                    length as u64,
                    operations,
                    self.admit_suffix_keys(&state, keys)?,
                )
            }
            Err(error) => return Err(error.into()),
        };
        drop(distinct);
        // Refuse a predictably over-budget append before detaching the
        // overlay or encoding. It is charged just before the append.
        if reservation.is_none() {
            if let Some(budget) = self.disk_budget()? {
                budget.check_log(next_log)?;
            }
        }
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
        if let Some(budget) = self.disk_budget()? {
            match reservation.take() {
                Some(reserved) => budget.admit_log_reserved(next_log, reserved)?,
                None => budget.admit_log(next_log)?,
            }
        }
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
        } else if let Some(key) = owned_key {
            overlay.insert(key, location);
        } else {
            *overlay
                .get_mut(key)
                .expect("writer gate holds admitted key") = location;
        }
        state.view.tail_bytes = next_tail;
        state.view.tail_operations = next_operations;
        state.overlay_key_bytes = new_key_bytes;
        state.generation = next_generation;
        pending.published = true;
        Ok(())
    }

    fn admit_suffix_length(
        &self,
        tail_bytes: u64,
        tail_operations: u64,
        length: usize,
        operations: u64,
    ) -> io::Result<(u64, u64)> {
        let next_tail = tail_bytes
            .checked_add(length as u64)
            .filter(|total| *total <= self.max_tail_bytes)
            .ok_or_else(|| io::Error::other("checkpoint suffix byte limit would be exceeded"))?;
        let next_operations = tail_operations
            .checked_add(operations)
            .filter(|&count| self.max_tail_operations.is_none_or(|limit| count <= limit))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "checkpoint suffix operation limit exceeded",
                )
            })?;
        Ok((next_tail, next_operations))
    }

    fn admit_suffix_keys(&self, state: &State, keys: &[&[u8]]) -> io::Result<usize> {
        // A validated mutation has at most 1,024 keys of 1,024 bytes each.
        let (count, bytes) = keys
            .iter()
            .filter(|key| !state.view.overlay.contains_key(**key))
            .fold((0, 0), |(count, bytes), key| (count + 1, bytes + key.len()));
        if state.view.overlay.len() + count > self.max_tail_keys {
            return Err(io::Error::other(
                "checkpoint suffix key limit would be exceeded",
            ));
        }
        state
            .overlay_key_bytes
            .checked_add(bytes)
            .filter(|total| *total <= self.max_tail_key_bytes)
            .ok_or_else(|| io::Error::other("checkpoint suffix key byte limit would be exceeded"))
    }

    fn fits_empty_suffix_keys(&self, keys: &[&[u8]]) -> bool {
        keys.len() <= self.max_tail_keys
            && keys.iter().map(|key| key.len()).sum::<usize>() <= self.max_tail_key_bytes
    }
}
