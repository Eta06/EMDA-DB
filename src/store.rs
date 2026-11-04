//! Public store operations, in-process coordination, and exclusive ownership.

use crate::log::{Log, ValueLocation};
use crate::record::{self, Operation};
use crate::{backup, directory};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::sync::{Mutex, RwLock, RwLockReadGuard};

mod atomic_batch;
mod commit;
mod compact;
mod read_set;
mod snapshot;
mod transaction;
mod transaction_queue;
use commit::Coordinator;
pub use read_set::{
    MAX_TRANSACTION_READ_CONSTRAINT_BYTES, MAX_TRANSACTION_READ_CONSTRAINTS,
    MAX_TRANSACTION_VALIDATION_ITEMS,
};
pub use snapshot::{
    MAX_ACTIVE_SNAPSHOTS, MAX_SNAPSHOT_KEY_BYTES, MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_TOTAL_KEY_BYTES,
    MAX_SNAPSHOT_TOTAL_KEYS, Snapshot,
};
pub use transaction::{
    MAX_TRANSACTION_STAGED_TOTAL_BYTES, TimedTransactionError, Transaction, TransactionMode,
};

struct Inner {
    index: BTreeMap<Vec<u8>, ValueLocation>,
    generation: u64,
}

/// A key and its value returned by an ordered scan.
pub type Entry = (Vec<u8>, Vec<u8>);

/// Maximum number of entries returned by a single scan.
pub const MAX_SCAN_ENTRIES: usize = 1024;
/// Maximum total key and value bytes returned by a single scan.
pub const MAX_SCAN_BYTES: usize = 32 * 1024 * 1024;

/// A single-process store. Successful mutations have been synchronized to disk.
pub struct Store {
    log: Log,
    inner: RwLock<Inner>,
    // A backup excludes all in-flight writes and holds this barrier until the
    // snapshot is complete; writers share it while the coordinator batches.
    writes: RwLock<()>,
    commits: Coordinator,
    max_log_bytes: Option<u64>,
    max_live_keys: Option<usize>,
    snapshot_budget: Mutex<snapshot::SnapshotBudget>,
    snapshot_registry: Mutex<snapshot::SnapshotRegistry>,
    transaction_budget: Mutex<transaction::TransactionBudget>,
    transaction_queue: transaction_queue::TransactionQueue,
    // Held for the store lifetime. OS file locks are advisory.
    _lock_file: File,
}

impl Store {
    /// Opens or creates a store. Only one store may hold its directory lock.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_using(path.as_ref(), Log::open)
    }

    /// Opens using an explicit verified index checkpoint and replays its tail.
    ///
    /// The caller must supply the digest returned when the checkpoint was
    /// created and protect it separately from the checkpoint file. The covered
    /// log prefix is also hashed. An invalid, unrelated, or too-new checkpoint
    /// is rejected. Call `open` to ignore it and replay the complete log. The
    /// index still resides in memory; this convenience method applies no limits.
    pub fn open_with_checkpoint(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
    ) -> io::Result<Self> {
        Self::open_with_checkpoint_and_limits(path, checkpoint, trusted_digest, None, None)
    }

    /// Opens using a verified checkpoint and optional per-open limits.
    /// The live-key cap also covers the checkpoint's historical peak.
    pub fn open_with_checkpoint_and_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
    ) -> io::Result<Self> {
        // A checkpoint is meaningful only for an existing log. Avoid creating
        // a new store (or a lock-only directory) when the source is missing.
        std::fs::metadata(path.as_ref().join("records.log"))?;
        let store = Self::open_using(path.as_ref(), |path, initialize| {
            Log::open_with_checkpoint(
                path,
                initialize,
                checkpoint.as_ref(),
                trusted_digest,
                max_live_keys,
                max_log_bytes,
            )
        })?;
        Self::set_open_limits(store, max_log_bytes, max_live_keys)
    }

    /// Opens a store with an in-process limit on the log file's length.
    ///
    /// This is reapplied on each open; it is not a persistent project quota.
    /// The log header and tombstones count toward the cap. It excludes other
    /// files and memory, and limited stores serialize all mutations. Opening
    /// may initialize a new store before checking its resulting length.
    pub fn open_with_log_limit(path: impl AsRef<Path>, max_bytes: u64) -> io::Result<Self> {
        Self::open_with_limits(path, Some(max_bytes), None)
    }

    /// Opens a store with optional per-open log-length and live-key limits.
    ///
    /// The key limit is a count, not a process memory guarantee. It applies to
    /// every replayed prefix, including keys that are later deleted. The log
    /// length is checked before replay. Neither setting is persisted.
    /// Stores using either limit serialize writes to prevent oversubscription.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
    ) -> io::Result<Self> {
        let store = Self::open_using(path.as_ref(), |path, initialize| {
            Log::open_with_recovery_limit(path, initialize, max_live_keys, max_log_bytes)
        })?;
        Self::set_open_limits(store, max_log_bytes, max_live_keys)
    }

    fn set_open_limits(
        mut store: Self,
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
    ) -> io::Result<Self> {
        // Read errors must propagate; never treat an unknown file length as a
        // successful quota check.
        if let Some(max) = max_log_bytes {
            if store.log.len()? > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing log exceeds configured limit",
                ));
            }
        }
        if let Some(max) = max_live_keys {
            if store.read()?.index.len() > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing key count exceeds configured limit",
                ));
            }
        }
        store.max_log_bytes = max_log_bytes;
        store.max_live_keys = max_live_keys;
        Ok(store)
    }

    fn open_using(
        path: &Path,
        open_log: impl FnOnce(&Path, bool) -> io::Result<(Log, BTreeMap<Vec<u8>, ValueLocation>)>,
    ) -> io::Result<Self> {
        directory::create_and_sync(path)?;
        let lock_path = path.join("store.lock");
        let new_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&lock_path);
        let (lock_file, may_initialize) = match new_lock {
            Ok(file) => (file, true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
                OpenOptions::new().read(true).write(true).open(&lock_path)?,
                false,
            ),
            Err(error) => return Err(error),
        };
        fs2::FileExt::try_lock_exclusive(&lock_file)?;
        let (log, index) = open_log(path, may_initialize)?;
        Ok(Self {
            log,
            inner: RwLock::new(Inner {
                index,
                generation: 0,
            }),
            writes: RwLock::new(()),
            commits: Coordinator::new(),
            max_log_bytes: None,
            max_live_keys: None,
            snapshot_budget: Mutex::new(snapshot::SnapshotBudget::default()),
            snapshot_registry: Mutex::new(snapshot::SnapshotRegistry::default()),
            transaction_budget: Mutex::new(transaction::TransactionBudget::default()),
            transaction_queue: transaction_queue::TransactionQueue::new(),
            _lock_file: lock_file,
        })
    }

    /// Reads a value by key, returning `None` if the key is absent.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        // The log is append-only, so a location remains valid even if a
        // concurrent writer publishes a newer value after this lookup.
        let location = inner.index.get(key).copied();
        drop(inner);
        location.map(|location| self.log.read(location)).transpose()
    }

    /// Reads up to 1,024 keys from one captured index state, in input order.
    ///
    /// Missing keys return `None`, and duplicate keys produce duplicate results.
    /// The call rejects results whose total key and value bytes exceed 32 MiB;
    /// callers can split a larger request. No partial results are returned on
    /// error. This is one read operation, not a multi-key transaction.
    pub fn get_many(&self, keys: &[&[u8]]) -> io::Result<Vec<Option<Vec<u8>>>> {
        if keys.len() > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many keys in bulk read",
            ));
        }
        for key in keys {
            record::validate_key(key)?;
        }
        let inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        let mut size = 0_usize;
        let mut locations = Vec::with_capacity(keys.len());
        for key in keys {
            let location = inner.index.get(*key).copied();
            size += key.len() + location.map_or(0, |entry| entry.length);
            if size > MAX_SCAN_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "bulk read exceeds 32 MiB",
                ));
            }
            locations.push(location);
        }
        drop(inner);
        locations
            .into_iter()
            .map(|location| location.map(|location| self.log.read(location)).transpose())
            .collect()
    }

    /// Returns at most `limit` entries in bytewise key order.
    ///
    /// `after` and `before` are exclusive bounds; `None` means unbounded.
    /// Use the last returned key as `after` for the next page. Each call captures
    /// the index at one instant; separate pages are not a stable snapshot and
    /// may reflect concurrent changes. A page may contain fewer than `limit`
    /// entries because of the byte limit; continue until a page is empty.
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
        let inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(Vec::new());
        }
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut locations = Vec::new();
        let mut bytes = 0_usize;
        for (key, &location) in inner.index.range::<[u8], _>(bounds).take(limit) {
            let size = key.len() + location.length;
            if bytes + size > MAX_SCAN_BYTES {
                break;
            }
            bytes += size;
            locations.push((key.clone(), location));
        }
        drop(inner);
        locations
            .into_iter()
            .map(|(key, location)| self.log.read(location).map(|value| (key, value)))
            .collect()
    }

    /// Replaces the value at `key`; synchronization completes before success.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append(Operation::Put, key, value)
    }

    /// Deletes a key. Deleting an absent key is valid and is persisted.
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.append(Operation::Delete, key, &[])
    }

    /// Atomically replaces or deletes one key if its value matches `expected`.
    ///
    /// `None` means an absent value. A mismatch returns `Ok(false)` without
    /// writing. A successful change is synchronized before `Ok(true)`.
    /// I/O errors during a write can leave the outcome uncertain, as with `put`.
    /// This does not provide a multi-key transaction or detect ABA changes.
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
        // Ordinary mutations hold the shared side through acknowledgement.
        // The exclusive side makes the read and commit indivisible relative
        // to every other mutation, including other compare-exchange calls.
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        if self.get(key)?.as_deref() != expected {
            return Ok(false);
        }
        self.commit_limited(operation, key, value)?;
        Ok(true)
    }

    /// Atomically adds `delta` to a little-endian signed 64-bit value.
    ///
    /// An absent key starts at zero. Existing values must be exactly eight
    /// bytes. Overflow and malformed values return an error without a write.
    /// The new value is synchronized before it is returned; a write I/O error
    /// can leave the outcome uncertain.
    pub fn increment_i64(&self, key: &[u8], delta: i64) -> io::Result<i64> {
        record::validate_key(key)?;
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let current = match self.get(key)? {
            None => 0,
            Some(bytes) => {
                let array: [u8; 8] = bytes.try_into().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "increment requires an eight-byte signed integer",
                    )
                })?;
                i64::from_le_bytes(array)
            }
        };
        let next = current.checked_add(delta).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "integer increment overflow")
        })?;
        self.commit_limited(Operation::Put, key, &next.to_le_bytes())?;
        Ok(next)
    }

    /// Creates a consistent, checksummed backup at a new path.
    ///
    /// Mutations are blocked until the backup has been fully written and
    /// verified. An existing destination is never overwritten. A failed
    /// directory synchronization after publication has an uncertain outcome.
    pub fn backup(&self, destination: impl AsRef<Path>) -> io::Result<()> {
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let _inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        backup::create(&self.log, destination.as_ref())
    }

    /// Writes a verified, non-overwriting index checkpoint to a new path.
    ///
    /// Mutations wait while the source log and index are validated and copied.
    /// It contains keys and locations, not values, and is not an independent
    /// backup. The returned digest must be retained in trusted metadata
    /// separately from the checkpoint. A directory sync failure after
    /// publication has an uncertain outcome. A witnessed store is required.
    pub fn checkpoint_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.checkpoint_to_with_sync(destination.as_ref(), |parent| {
            File::open(parent)?.sync_all()
        })
    }

    fn checkpoint_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        crate::checkpoint::create_with_sync(&self.log, &inner.index, destination, sync_parent)
    }

    /// Restores a validated backup into a new directory; existing paths are
    /// never overwritten. A failed directory synchronization after publication
    /// has an uncertain outcome and the destination must be inspected.
    pub fn restore(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> io::Result<()> {
        backup::restore(source.as_ref(), destination.as_ref())
    }

    /// Copies a legacy store into a new directory with a commit witness.
    ///
    /// The source is not modified. This establishes a boundary for the data
    /// that is present and valid now; it cannot detect prior loss from a legacy
    /// log. The destination must not exist. Mutations wait until the copy and
    /// verification finish. A publication sync error leaves the result uncertain.
    pub fn upgrade_legacy(&self, destination: impl AsRef<Path>) -> io::Result<()> {
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let _inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        backup::upgrade_legacy(&self.log, destination.as_ref())
    }

    /// Copies any supported store into the current format at a new path.
    ///
    /// The source remains unchanged. On Linux filesystems supporting durable
    /// user xattrs this creates V3; otherwise creation uses the V2 fallback.
    /// The destination is verified and synchronized before publication, and
    /// never replaces an existing path. An error after publication is uncertain.
    pub fn upgrade_format(&self, destination: impl AsRef<Path>) -> io::Result<()> {
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let _inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        backup::upgrade_format(&self.log, destination.as_ref())
    }

    fn append(&self, operation: Operation, key: &[u8], value: &[u8]) -> io::Result<()> {
        record::validate(operation, key, value)?;
        if self.max_log_bytes.is_some() || self.max_live_keys.is_some() {
            let _writer = self
                .writes
                .write()
                .map_err(|_| io::Error::other("write barrier poisoned"))?;
            return self.commit_limited(operation, key, value);
        }
        let _writer = self
            .writes
            .read()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        self.commits
            .commit(&self.log, &self.inner, operation, key, value)
    }

    // Limited stores hold the exclusive write barrier through the check and
    // append, preventing concurrent writes from oversubscribing the quota.
    fn commit_limited(&self, operation: Operation, key: &[u8], value: &[u8]) -> io::Result<()> {
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        if operation == Operation::Put {
            if let Some(max) = self.max_live_keys {
                let inner = self.read()?;
                if !inner.index.contains_key(key) && inner.index.len() >= max {
                    return Err(io::Error::other(
                        "configured live key limit would be exceeded",
                    ));
                }
            }
        }
        if let Some(max) = self.max_log_bytes {
            let record_bytes =
                (record::HEADER_SIZE + key.len() + value.len() + record::CHECKSUM_SIZE) as u64;
            if self
                .log
                .len()?
                .checked_add(record_bytes)
                .is_none_or(|end| end > max)
            {
                return Err(io::Error::other(
                    "configured log size limit would be exceeded",
                ));
            }
        }
        self.commits
            .commit(&self.log, &self.inner, operation, key, value)
    }

    fn read(&self) -> io::Result<RwLockReadGuard<'_, Inner>> {
        self.inner
            .read()
            .map_err(|_| io::Error::other("store lock poisoned"))
    }
}

fn uncertain_error() -> io::Error {
    io::Error::other("a previous write had an uncertain outcome; reopen the store")
}

#[cfg(test)]
mod fault_tests;

#[cfg(test)]
mod group_tests;

#[cfg(test)]
mod read_tests;

#[cfg(test)]
mod reservation_tests;

#[cfg(test)]
mod wake_tests;

#[cfg(all(test, target_os = "linux"))]
mod metadata_tests;
