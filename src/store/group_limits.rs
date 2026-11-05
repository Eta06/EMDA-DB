//! Per-open group-commit reservation limits for the ordinary writable store.
use super::{GroupCommitStatus, Store};
use std::io;

impl Store {
    /// Reports logical group-commit reservations and capacity waiters.
    ///
    /// Counts cover encoded records from reservation through synchronization and
    /// publication, including preparation and active batches. Reservations
    /// are released when completion is signalled, before callers necessarily
    /// return. They exclude caller buffers, threads waiting for other locks,
    /// allocator overhead and total memory.
    pub fn group_commit_status(&self) -> io::Result<GroupCommitStatus> {
        self.commits.status()
    }

    /// Sets per-open encoded-byte and record-count limits for group commit.
    ///
    /// Limits cannot exceed the exported hard maxima. Reducing either below
    /// currently reserved capacity returns `WouldBlock` without changing them.
    /// A zero record limit disables new group-commit admission. A record too
    /// large for the byte limit is rejected before submission. Writers waiting
    /// for capacity recheck changed limits and can therefore be rejected after
    /// reconfiguration. Ordinary contention otherwise waits; there is no
    /// bounded completion or fairness guarantee. Transactional commits also
    /// use this coordinator; read-only transactions do not submit records.
    ///
    /// These per-open limits are preserved by supported in-process ordinary
    /// compaction handoffs and reset on reopen. They do not account for other
    /// stores, caller buffers, lock waiters, total memory, project quotas, or
    /// the separately implemented checkpoint-backed store. I/O errors after a
    /// record is submitted still have an unknown outcome.
    pub fn set_group_commit_limits(&self, bytes: usize, records: usize) -> io::Result<()> {
        self.commits.set_limits(bytes, records)
    }
}
