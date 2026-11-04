//! Admission, durable append and publication for checkpoint-backed writes.
use super::*;
use crate::log::{AppendRecord, BatchUpdate, prepare_batch};
use std::collections::BTreeSet;

impl CheckpointStore {
    // The state write lock serializes admission, append, synchronization and
    // publication. It also prevents readers seeing a partially applied batch.
    pub(super) fn commit(
        &self,
        state: &mut State,
        operation: Operation,
        key: &[u8],
        value: &[u8],
        touched: &[&[u8]],
    ) -> io::Result<()> {
        match self.commit_until(state, operation, key, value, touched, None) {
            Ok(()) => Ok(()),
            Err(crate::TimedTransactionError::Operation(error)) => Err(error),
            Err(crate::TimedTransactionError::DeadlineExceeded) => {
                unreachable!("a commit without a deadline cannot expire")
            }
        }
    }

    pub(super) fn commit_until(
        &self,
        state: &mut State,
        operation: Operation,
        key: &[u8],
        value: &[u8],
        touched: &[&[u8]],
        deadline: Option<std::time::Instant>,
    ) -> Result<(), crate::TimedTransactionError> {
        // Refuse a mutation before appending if a transaction generation can
        // no longer advance; wrapping would let a stale transaction commit.
        let next_generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("checkpoint transaction generation exhausted"))?;
        let length = HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE;
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
        let admission = self.admit_suffix(state, length, touched);
        let (next_tail, new_key_bytes) = match admission {
            Ok(admitted) => admitted,
            Err(_)
                if state.automatic
                    && state.view.tail_bytes > 0
                    && self.fits_empty_suffix(length, touched) =>
            {
                self.automatic_rollover(state)?;
                self.admit_suffix(state, length, touched)?
            }
            Err(error) => return Err(error.into()),
        };
        // Prepare all input before append. Once append starts, even a failed
        // verification or panic leaves the instance unusable until recovery.
        let encoded = record::encode(operation, key, value)?;
        let owned_key = (operation != Operation::Batch).then(|| key.to_vec());
        // Automatic maintenance can take time. Preserve the transaction's
        // pre-append expiry guarantee after the checkpoint handoff completes.
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(crate::TimedTransactionError::DeadlineExceeded);
        }
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
        state.generation = next_generation;
        state.failed = false;
        Ok(())
    }

    fn admit_suffix(
        &self,
        state: &State,
        length: usize,
        touched: &[&[u8]],
    ) -> io::Result<(u64, usize)> {
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
        Ok((next_tail, new_key_bytes))
    }

    fn fits_empty_suffix(&self, length: usize, touched: &[&[u8]]) -> bool {
        if length as u64 > self.max_tail_bytes {
            return false;
        }
        let keys = touched.iter().copied().collect::<BTreeSet<_>>();
        if keys.len() > self.max_tail_keys {
            return false;
        }
        keys.into_iter()
            .try_fold(0_usize, |sum, key| sum.checked_add(key.len()))
            .is_some_and(|bytes| bytes <= self.max_tail_key_bytes)
    }
}
