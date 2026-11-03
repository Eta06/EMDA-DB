//! Bounded in-process read-only index snapshots over immutable log locations.
use super::*;
use std::time::Instant;

/// Hard per-snapshot limit on the number of copied keys.
pub const MAX_SNAPSHOT_KEYS: usize = 16_384;
/// Hard per-snapshot limit on the total number of copied key bytes.
pub const MAX_SNAPSHOT_KEY_BYTES: usize = 4 * 1024 * 1024;
/// Maximum simultaneously active snapshots per open store.
pub const MAX_ACTIVE_SNAPSHOTS: usize = 16;
/// Maximum combined copied keys across snapshots on one open store.
pub const MAX_SNAPSHOT_TOTAL_KEYS: usize = 65_536;
/// Maximum combined copied key bytes across snapshots on one open store.
pub const MAX_SNAPSHOT_TOTAL_KEY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
pub(super) struct SnapshotBudget {
    active: usize,
    keys: usize,
    key_bytes: usize,
}

impl SnapshotBudget {
    fn reserve(&mut self, keys: usize, key_bytes: usize) -> io::Result<()> {
        if self.active >= MAX_ACTIVE_SNAPSHOTS
            || keys > MAX_SNAPSHOT_TOTAL_KEYS - self.keys
            || key_bytes > MAX_SNAPSHOT_TOTAL_KEY_BYTES - self.key_bytes
        {
            return Err(io::Error::other("aggregate snapshot limit exceeded"));
        }
        self.active += 1;
        self.keys += keys;
        self.key_bytes += key_bytes;
        Ok(())
    }

    fn release(&mut self, keys: usize, key_bytes: usize) {
        self.active -= 1;
        self.keys -= keys;
        self.key_bytes -= key_bytes;
    }
}

/// A stable read-only index view borrowing the open store.
///
/// Drop it to release its copied index. Snapshots are process-local, do not
/// survive reopening, and do not make independent writes transactional.
/// Per-store snapshot count, total keys and key bytes are limited. These are
/// not byte-accurate process memory limits and do not apply across stores.
/// An expired snapshot continues to reserve its resources until cancelled or
/// dropped. Expiry is checked at operation boundaries, not during I/O.
pub struct Snapshot<'a> {
    store: &'a Store,
    pub(super) index: BTreeMap<Vec<u8>, ValueLocation>,
    pub(super) generation: u64,
    reserved_keys: usize,
    reserved_key_bytes: usize,
    active: bool,
    deadline: Option<Instant>,
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl Store {
    /// Captures a stable index view, rejecting a store that exceeds either limit.
    /// Limits cannot exceed the public hard per-snapshot limits. The operation
    /// copies keys and offsets, not values, and never returns a partial snapshot.
    pub fn snapshot(&self, max_keys: usize, max_key_bytes: usize) -> io::Result<Snapshot<'_>> {
        if max_keys > MAX_SNAPSHOT_KEYS || max_key_bytes > MAX_SNAPSHOT_KEY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot limits exceed supported maximum",
            ));
        }
        let inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        if inner.index.len() > max_keys {
            return Err(io::Error::other("snapshot key count limit exceeded"));
        }
        let mut key_bytes = 0_usize;
        for key in inner.index.keys() {
            key_bytes += key.len();
            if key_bytes > max_key_bytes {
                return Err(io::Error::other("snapshot key byte limit exceeded"));
            }
        }
        let key_count = inner.index.len();
        self.snapshot_budget
            .lock()
            .map_err(|_| io::Error::other("snapshot budget poisoned"))?
            .reserve(key_count, key_bytes)?;
        // Once reserved, Drop also releases accounting if copying unwinds.
        let mut snapshot = Snapshot {
            store: self,
            index: BTreeMap::new(),
            generation: inner.generation,
            reserved_keys: key_count,
            reserved_key_bytes: key_bytes,
            active: true,
            deadline: None,
        };
        for (key, &location) in &inner.index {
            snapshot.index.insert(key.clone(), location);
        }
        Ok(snapshot)
    }

    /// Captures a snapshot that rejects new operations at or after `deadline`.
    ///
    /// The deadline is process-local and monotonic. An operation started before
    /// expiry can finish afterwards; I/O and lock waits are not interrupted.
    /// Expiry does not automatically reclaim memory: cancel or drop the view.
    pub fn snapshot_until(
        &self,
        max_keys: usize,
        max_key_bytes: usize,
        deadline: Instant,
    ) -> io::Result<Snapshot<'_>> {
        if Instant::now() >= deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "snapshot expired"));
        }
        let mut snapshot = self.snapshot(max_keys, max_key_bytes)?;
        snapshot.deadline = Some(deadline);
        snapshot.ensure_healthy()?;
        Ok(snapshot)
    }
}

impl Snapshot<'_> {
    pub(super) fn ensure_healthy(&self) -> io::Result<()> {
        if !self.active {
            return Err(io::Error::other("snapshot has been cancelled"));
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "snapshot expired"));
        }
        if self.store.commits.failed() {
            return Err(uncertain_error());
        }
        Ok(())
    }

    /// Cancels this snapshot and releases its copied index and reservations.
    ///
    /// Cancellation is idempotent. Later reads return an error rather than an
    /// empty result. It has no effect on other snapshots or transactions.
    pub fn cancel(&mut self) {
        if !self.active {
            return;
        }
        // Free the copied keys before returning capacity to another snapshot.
        self.index.clear();
        let mut budget = self
            .store
            .snapshot_budget
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        budget.release(self.reserved_keys, self.reserved_key_bytes);
        self.active = false;
    }

    /// Whether this snapshot has been explicitly cancelled.
    pub fn is_cancelled(&self) -> bool {
        !self.active
    }

    /// Reads the value selected when this snapshot was created.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        self.ensure_healthy()?;
        self.index
            .get(key)
            .copied()
            .map(|location| self.store.log.read(location))
            .transpose()
    }

    /// Scans this snapshot in bytewise key order with exclusive bounds.
    /// Page count and byte limits match `Store::scan`; continue until empty.
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
        self.ensure_healthy()?;
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(Vec::new());
        }
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut result = Vec::new();
        let mut bytes = 0_usize;
        for (key, &location) in self.index.range::<[u8], _>(bounds).take(limit) {
            let size = key.len() + location.length;
            if bytes + size > MAX_SCAN_BYTES {
                break;
            }
            bytes += size;
            result.push((key.clone(), self.store.log.read(location)?));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_key_and_byte_budgets_are_released_after_reservation() {
        let mut budget = SnapshotBudget::default();
        budget.reserve(MAX_SNAPSHOT_TOTAL_KEYS, 1).unwrap();
        assert!(budget.reserve(1, 0).is_err());
        budget.release(MAX_SNAPSHOT_TOTAL_KEYS, 1);
        budget.reserve(1, MAX_SNAPSHOT_TOTAL_KEY_BYTES).unwrap();
        assert!(budget.reserve(0, 1).is_err());
        budget.release(1, MAX_SNAPSHOT_TOTAL_KEY_BYTES);
        assert!(budget.reserve(1, 1).is_ok());
    }

    #[test]
    fn expired_snapshot_fails_reads_until_cancelled_and_keeps_its_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put(b"k", b"v").unwrap();
        let mut snapshot = store.snapshot(1, 1).unwrap();
        snapshot.deadline = Some(Instant::now());
        assert_eq!(
            snapshot.get(b"k").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            snapshot.scan(None, None, 1).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 1);
        snapshot.cancel();
        assert!(snapshot.is_cancelled());
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 0);
        assert!(snapshot.get(b"k").is_err());
    }
}
