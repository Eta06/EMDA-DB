//! Atomic multi-key mutations encoded as one checksummed DB02 record.
use super::*;
use crate::record::batch::{BatchOperation, encode_payload};

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
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        // Construct the additional encoded payload only while holding the
        // exclusive barrier, so concurrent callers cannot each allocate a
        // full batch before entering coordinator admission.
        let payload = encode_payload(operations)?;
        self.write_batch_locked(operations, &payload)
    }

    // Caller holds the exclusive writes barrier through durable publication.
    pub(super) fn write_batch_locked(
        &self,
        operations: &[BatchOperation<'_>],
        payload: &[u8],
    ) -> io::Result<()> {
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        if self.log.is_legacy() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "atomic batches require a witnessed store; upgrade the legacy store first",
            ));
        }
        let mut next_key_bytes = None;
        if self.max_live_keys.is_some() || self.max_live_key_bytes.is_some() {
            // The configured limit applies to every ordered intermediate
            // state, matching recovery's historical-prefix enforcement.
            let inner = self.read()?;
            let mut present = BTreeMap::<&[u8], bool>::new();
            let mut count = inner.index.len();
            let mut key_bytes = self.live_key_bytes.load(Ordering::Relaxed);
            for &operation in operations {
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
                            ));
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
        self.commit_limited(Operation::Batch, &[0], payload)?;
        if let Some(bytes) = next_key_bytes {
            self.live_key_bytes.store(bytes, Ordering::Relaxed);
        }
        Ok(())
    }
}
