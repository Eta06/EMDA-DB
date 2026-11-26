//! Atomic multi-key mutations encoded as one checksummed DB02 record.
use super::*;
use crate::TimedTransactionError;
use crate::record::batch::{BatchLayout, BatchOperation};

impl Store {
    /// Applies ordered puts and deletes as one atomic, synchronized mutation.
    ///
    /// Readers see the index before or after the complete batch. The result is
    /// durable only under the store's documented filesystem assumptions. On an
    /// append/sync error, the whole batch has an unknown outcome and the store
    /// must be reopened. This operation is available only on witnessed V2/V3
    /// stores; upgrade legacy logs first. The first successful batch adds DB02
    /// records, which older software cannot read. This is a blind atomic write,
    /// not a read-modify-write transaction or snapshot isolation.
    pub fn write_batch(&self, operations: &[BatchOperation<'_>]) -> io::Result<()> {
        self.write_batch_tracked(operations, None)
    }

    /// Applies a blind atomic batch with structured mutation failures.
    pub fn write_batch_detailed(
        &self,
        operations: &[BatchOperation<'_>],
    ) -> Result<(), crate::MutationFailure> {
        crate::transaction_outcome::mutation(|tracker| {
            self.write_batch_tracked(operations, Some(tracker))
        })
    }

    fn write_batch_tracked(
        &self,
        operations: &[BatchOperation<'_>],
        tracker: Option<&crate::transaction_outcome::Tracker>,
    ) -> io::Result<()> {
        let _admission = self.direct_writes.acquire()?;
        // A blind batch is one record and reads nothing, so like an ordinary
        // put it needs only shared access. Live-key limits are checked over
        // the current membership, which only exclusive access keeps stable.
        if self.max_live_keys.is_none() && self.max_live_key_bytes.is_none() {
            let _writer = self
                .writes
                .read()
                .map_err(|_| io::Error::other("write barrier poisoned"))?;
            let batch = BatchLayout::new(operations)?;
            return self
                .write_batch_locked(batch, tracker, || Ok(()))
                .map_err(TimedTransactionError::into_io);
        }
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let batch = BatchLayout::new(operations)?;
        self.write_batch_locked(batch, tracker, || Ok(()))
            .map_err(TimedTransactionError::into_io)
    }

    // Caller holds the writes barrier through durable publication: exclusive
    // access when live-key limits apply or for a transaction, otherwise at
    // least shared access.
    pub(super) fn write_batch_locked(
        &self,
        batch: BatchLayout<'_>,
        tracker: Option<&crate::transaction_outcome::Tracker>,
        before_submit: impl FnOnce() -> Result<(), TimedTransactionError>,
    ) -> Result<(), TimedTransactionError> {
        if self.commits.failed() {
            return Err(uncertain_error().into());
        }
        if self.log.is_legacy() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "atomic batches require a witnessed store; upgrade the legacy store first",
            )
            .into());
        }
        let mut next_key_bytes = None;
        if self.max_live_keys.is_some() || self.max_live_key_bytes.is_some() {
            // The configured limit applies to every ordered intermediate
            // state, matching recovery's historical-prefix enforcement.
            let inner = self.read()?;
            let mut present = BTreeMap::<&[u8], bool>::new();
            let mut count = inner.index.len();
            let mut key_bytes = self.live_key_bytes.load(Ordering::Relaxed);
            for &operation in batch.operations() {
                let (kind, key, _) = operation.fields();
                let exists = present
                    .get(key)
                    .copied()
                    .unwrap_or_else(|| inner.index.contains_key(key));
                match kind {
                    Operation::Put if !exists => {
                        if self.max_live_keys.is_some_and(|max| count >= max) {
                            return Err(io::Error::other(
                                "configured live key limit would be exceeded",
                            )
                            .into());
                        }
                        if let Some(max) = self.max_live_key_bytes {
                            key_bytes = key_bytes
                                .checked_add(key.len())
                                .filter(|bytes| *bytes <= max)
                                .ok_or_else(|| {
                                    io::Error::other(
                                        "configured live key byte limit would be exceeded",
                                    )
                                })?;
                        }
                        count += 1;
                        present.insert(key, true);
                    }
                    Operation::Delete if exists => {
                        count -= 1;
                        if self.max_live_key_bytes.is_some() {
                            key_bytes = key_bytes.checked_sub(key.len()).ok_or_else(|| {
                                io::Error::other("live key byte accounting is inconsistent")
                            })?;
                        }
                        present.insert(key, false);
                    }
                    Operation::Put | Operation::Delete => {}
                    Operation::Batch => unreachable!("public operations cannot contain batches"),
                }
            }
            next_key_bytes = self.max_live_key_bytes.map(|_| key_bytes);
        }
        let bytes = batch.encoded_len() as u64;
        let mut reservation = self
            .log_quota
            .as_ref()
            .map(|quota| quota.reserve(bytes))
            .transpose()?;
        self.commits
            .commit_batch(&self.log, &self.inner, batch, tracker, before_submit)?;
        if let Some(reservation) = reservation.as_mut() {
            reservation.committed();
        }
        if let Some(bytes) = next_key_bytes {
            self.live_key_bytes.store(bytes, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_submission_rejection_releases_both_reservations_and_keeps_wire_format() {
        use crate::record::batch::encode_payload;
        use crate::test_support::retry_lock;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let store = retry_lock(|| Store::open(path)).unwrap();
        let before = store.log.len().unwrap();
        let operations = [
            BatchOperation::Put {
                key: b"a",
                value: b"first",
            },
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put {
                key: b"a",
                value: b"last",
            },
        ];
        let legacy_bytes = record::encode(
            Operation::Batch,
            &[0],
            &encode_payload(&operations).unwrap(),
        )
        .unwrap();
        let batch = BatchLayout::new(&operations).unwrap();
        assert_eq!(batch.encoded_len(), legacy_bytes.len());
        drop(store);
        let store =
            retry_lock(|| Store::open_with_log_limit(path, before + legacy_bytes.len() as u64))
                .unwrap();
        store
            .set_group_commit_limits(legacy_bytes.len(), 1)
            .unwrap();
        {
            let _writer = store.writes.write().unwrap();
            let outcome = store
                .write_batch_locked(batch, None, || Err(TimedTransactionError::DeadlineExceeded));
            assert!(matches!(
                outcome,
                Err(TimedTransactionError::DeadlineExceeded)
            ));
        }
        assert_eq!(store.log.len().unwrap(), before);
        assert_eq!(store.get(b"a").unwrap(), None);
        let status = store.group_commit_status().unwrap();
        assert_eq!((status.reserved_bytes, status.reserved_records), (0, 0));
        store.write_batch(&operations).unwrap();
        let log = std::fs::read(path.join("records.log")).unwrap();
        assert_eq!(&log[before as usize..], legacy_bytes);
        drop(store);
        let reopened = retry_lock(|| Store::open(path)).unwrap();
        assert_eq!(reopened.get(b"a").unwrap().unwrap(), b"last");
    }
}
