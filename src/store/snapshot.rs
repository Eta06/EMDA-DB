//! Bounded in-process read-only index snapshots over immutable log locations.
use super::*;

/// Hard per-snapshot limit on the number of copied keys.
pub const MAX_SNAPSHOT_KEYS: usize = 16_384;
/// Hard per-snapshot limit on the total number of copied key bytes.
pub const MAX_SNAPSHOT_KEY_BYTES: usize = 4 * 1024 * 1024;

/// A stable read-only index view borrowing the open store.
///
/// Drop it to release its copied index. Snapshots are process-local, do not
/// survive reopening, and do not make independent writes transactional.
/// Aggregate memory across multiple snapshots is not currently limited.
pub struct Snapshot<'a> {
    store: &'a Store,
    index: BTreeMap<Vec<u8>, ValueLocation>,
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
        let mut index = BTreeMap::new();
        let mut key_bytes = 0_usize;
        for (key, &location) in &inner.index {
            key_bytes += key.len();
            if key_bytes > max_key_bytes {
                return Err(io::Error::other("snapshot key byte limit exceeded"));
            }
            index.insert(key.clone(), location);
        }
        Ok(Snapshot { store: self, index })
    }
}

impl Snapshot<'_> {
    /// Reads the value selected when this snapshot was created.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        if self.store.commits.failed() {
            return Err(uncertain_error());
        }
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
        if self.store.commits.failed() {
            return Err(uncertain_error());
        }
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
