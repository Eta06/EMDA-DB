//! Public store operations, in-process coordination, and exclusive ownership.

use crate::log::{Log, ValueLocation};
use crate::record::{self, Operation};
use crate::{backup, directory};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

mod atomic_batch;
mod commit;
mod compact;
pub(crate) mod compact_exchange;
mod compact_stream;
mod group_limits;
mod log_quota;
mod online;
mod point_read;
mod read_set;
mod snapshot;
mod transaction;
mod transaction_queue;
use commit::Coordinator;
pub use commit::{
    GroupCommitStatus, MAX_GROUP_COMMIT_BUFFERED_BYTES, MAX_GROUP_COMMIT_BUFFERED_RECORDS,
};
pub use compact_stream::COMPACTED_CHECKPOINT_FILE;
pub(crate) use compact_stream::compact_checkpoint;
#[cfg(test)]
pub(crate) use compact_stream::compact_checkpoint_with_hooks;
use log_quota::LogQuota;
pub use online::{
    OnlineCompactionOptions, OnlineCompactionReport, OnlineReclaimError, OnlineReclamationFailure,
    OnlineReclamationFailureStage, OnlineReclamationWorker, OnlineReclamationWorkerStatus,
    OnlineStore, RetiredCleanupStatus,
};
pub use read_set::{
    MAX_TRANSACTION_READ_CONSTRAINT_BYTES, MAX_TRANSACTION_READ_CONSTRAINTS,
    MAX_TRANSACTION_VALIDATION_ITEMS,
};
pub use snapshot::{
    MAX_ACTIVE_SNAPSHOTS, MAX_SNAPSHOT_KEY_BYTES, MAX_SNAPSHOT_KEYS, MAX_SNAPSHOT_TOTAL_KEY_BYTES,
    MAX_SNAPSHOT_TOTAL_KEYS, Snapshot, SnapshotBudgetStatus,
};
pub use transaction::{
    MAX_TRANSACTION_STAGED_TOTAL_BYTES, TimedTransactionError, Transaction, TransactionMode,
};
pub(crate) use transaction_queue::TransactionQueue;

struct Inner {
    index: Arc<BTreeMap<Vec<u8>, ValueLocation>>,
    index_key_bytes: usize,
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
    writes: crate::controlled_lock::ControlledRwLock<()>,
    pub(crate) direct_writes: crate::direct_write::DirectWrites,
    pub(crate) direct_reads: crate::direct_read::DirectReads,
    commits: Coordinator,
    log_quota: Option<LogQuota>,
    max_live_keys: Option<usize>,
    max_live_key_bytes: Option<usize>,
    // With this limit enabled, the exclusive write barrier serializes every
    // mutation and the counter update. It excludes allocator overhead.
    live_key_bytes: AtomicUsize,
    snapshot_budget: Mutex<snapshot::SnapshotBudget>,
    snapshot_registry: Mutex<snapshot::SnapshotRegistry>,
    pub(crate) snapshot_reaper_active: AtomicBool,
    transaction_budget: Mutex<crate::transaction_budget::TransactionBudget>,
    transaction_queue: transaction_queue::TransactionQueue,
    // Held for the store lifetime. OS file locks are advisory.
    _lock_file: File,
}

impl Store {
    // A private, coordinated handoff can temporarily share this open file
    // description without releasing the store's exclusive advisory lock.
    pub(crate) fn duplicate_store_lock(&self) -> io::Result<File> {
        self._lock_file.try_clone()
    }

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
        Self::set_open_limits(store, max_log_bytes, max_live_keys, None)
    }

    /// Opens a store with an in-process limit on the log file's length.
    ///
    /// This is reapplied on each open; it is not a persistent project quota.
    /// The log header and tombstones count toward the cap. It excludes other
    /// files and memory. In-flight records reserve space before admission and
    /// independent writes may share a sync. Opening may initialize a new store
    /// before checking its resulting length.
    pub fn open_with_log_limit(path: impl AsRef<Path>, max_bytes: u64) -> io::Result<Self> {
        Self::open_with_limits(path, Some(max_bytes), None)
    }

    /// Opens a store with optional per-open log-length and live-key limits.
    ///
    /// The key limit is a count, not a process memory guarantee. It applies to
    /// every replayed prefix, including keys that are later deleted. The log
    /// length is checked before replay. Neither setting is persisted.
    /// A log-length cap reserves bytes for in-flight writes. With a live-key
    /// cap, operations that may change membership serialize; updates to
    /// existing keys may share synchronization under a shared membership barrier.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
    ) -> io::Result<Self> {
        Self::open_with_resource_limits(path, max_log_bytes, max_live_keys, None)
    }

    /// Opens with optional per-open log, live-key count and live-key byte caps.
    ///
    /// Live-key bytes count the lengths of currently present keys, not values,
    /// allocator overhead, snapshots, other files or total process memory. The
    /// cap is checked for every historical replay prefix, including each
    /// ordered operation inside an atomic batch. It is not persisted and must
    /// be supplied on every open. New-key puts, deletes and compound mutations
    /// serialize. Existing-key updates may share synchronization, reserving
    /// log bytes first if a log cap is set. This method uses full-log recovery;
    /// checkpoints do not record the historical live-key byte peak required to
    /// enforce the cap.
    pub fn open_with_resource_limits(
        path: impl AsRef<Path>,
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
        max_live_key_bytes: Option<usize>,
    ) -> io::Result<Self> {
        let store = Self::open_using(path.as_ref(), |path, initialize| {
            Log::open_with_recovery_limit(
                path,
                initialize,
                max_live_keys,
                max_log_bytes,
                max_live_key_bytes,
            )
        })?;
        Self::set_open_limits(store, max_log_bytes, max_live_keys, max_live_key_bytes)
    }

    fn set_open_limits(
        mut store: Self,
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
        max_live_key_bytes: Option<usize>,
    ) -> io::Result<Self> {
        // Read errors must propagate; never treat an unknown file length as a
        // successful quota check.
        if let Some(max) = max_log_bytes {
            store.log_quota = Some(LogQuota::new(max, store.log.len()?)?);
        }
        if let Some(max) = max_live_keys {
            if store.read()?.index.len() > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing key count exceeds configured limit",
                ));
            }
        }
        if let Some(max) = max_live_key_bytes {
            let bytes = store.read()?.index.keys().try_fold(0_usize, |total, key| {
                total
                    .checked_add(key.len())
                    .ok_or_else(|| io::Error::other("live key byte count overflow"))
            })?;
            if bytes > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing live key bytes exceed configured limit",
                ));
            }
            store.live_key_bytes.store(bytes, Ordering::Relaxed);
        }
        store.max_live_keys = max_live_keys;
        store.max_live_key_bytes = max_live_key_bytes;
        Ok(store)
    }

    fn open_using(
        path: &Path,
        open_log: impl FnOnce(&Path, bool) -> io::Result<(Log, BTreeMap<Vec<u8>, ValueLocation>)>,
    ) -> io::Result<Self> {
        Self::open_using_with_hook(path, open_log, || Ok(()))
    }

    fn open_using_with_hook(
        path: &Path,
        open_log: impl FnOnce(&Path, bool) -> io::Result<(Log, BTreeMap<Vec<u8>, ValueLocation>)>,
        after_lock_open: impl FnOnce() -> io::Result<()>,
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
        after_lock_open()?;
        fs2::FileExt::try_lock_exclusive(&lock_file)?;
        directory::verify_lock_identity(path, &lock_file)?;
        let (log, index) = open_log(path, may_initialize)?;
        let index_key_bytes = index.keys().try_fold(0_usize, |total, key| {
            total
                .checked_add(key.len())
                .ok_or_else(|| io::Error::other("live index key byte count overflow"))
        })?;
        Ok(Self {
            log,
            inner: RwLock::new(Inner {
                index: Arc::new(index),
                index_key_bytes,
                generation: 0,
            }),
            writes: crate::controlled_lock::ControlledRwLock::new(()),
            direct_writes: crate::direct_write::DirectWrites::default(),
            direct_reads: crate::direct_read::DirectReads::default(),
            commits: Coordinator::new(),
            log_quota: None,
            max_live_keys: None,
            max_live_key_bytes: None,
            live_key_bytes: AtomicUsize::new(0),
            snapshot_budget: Mutex::new(snapshot::SnapshotBudget::default()),
            snapshot_registry: Mutex::new(snapshot::SnapshotRegistry::default()),
            snapshot_reaper_active: AtomicBool::new(false),
            transaction_budget: Mutex::new(crate::transaction_budget::TransactionBudget::default()),
            transaction_queue: transaction_queue::TransactionQueue::new(),
            _lock_file: lock_file,
        })
    }

    /// Reads a value by key, returning `None` if the key is absent.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let _admission = self.direct_reads.acquire()?;
        self.get_validated(key)
    }

    // Callers have already validated the key. Internal read-modify-write
    // operations use this path so direct-read saturation cannot prevent a
    // separately admitted write from inspecting its current value.
    fn get_validated(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
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
        let _admission = self.direct_reads.acquire()?;
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
        let _admission = self.direct_reads.acquire()?;
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
        let _admission = self.direct_writes.acquire()?;
        // Ordinary mutations hold the shared side through acknowledgement.
        // The exclusive side makes the read and commit indivisible relative
        // to every other mutation, including other compare-exchange calls.
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        if self.get_validated(key)?.as_deref() != expected {
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
        let _admission = self.direct_writes.acquire()?;
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let current = match self.get_validated(key)? {
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
        self.checkpoint_to_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    pub(crate) fn checkpoint_to_with_sync(
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
    /// never overwritten. A validated zero-byte legacy log is restored as a
    /// new witnessed empty store with a new identity. A failed directory sync
    /// after publication has an uncertain outcome and must be inspected.
    pub fn restore(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> io::Result<()> {
        backup::restore(source.as_ref(), destination.as_ref())
    }

    /// Copies an independently verified empty, unwitnessed legacy log into a
    /// new witnessed store without changing the source log.
    ///
    /// An empty log may instead be the result of interruption or data loss;
    /// this method cannot tell. Call it only after independently verifying the
    /// source is intended to be empty. Lost data cannot be reconstructed. The
    /// source directory may gain a lock file. The destination must not exist,
    /// and a publication synchronization failure leaves its outcome uncertain.
    pub fn upgrade_empty_legacy(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> io::Result<()> {
        let source = source.as_ref();
        let metadata = std::fs::metadata(source.join("records.log"))?;
        if !metadata.is_file() || metadata.len() != 0 {
            return Err(record::invalid_data(
                "source log is not an empty regular file",
            ));
        }
        let store = Self::open_using(source, |path, _| Log::open_empty_legacy_for_upgrade(path))?;
        store.upgrade_legacy(destination)
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
        let _admission = self.direct_writes.acquire()?;
        if operation == Operation::Put
            && (self.max_live_keys.is_some() || self.max_live_key_bytes.is_some())
        {
            // Updating a key that already exists does not change either
            // live-key quota. Keep the shared barrier until acknowledgement:
            // every operation that can change membership takes the exclusive
            // side, so this observation remains valid. A log quota, if set,
            // is reserved separately before entering the coordinator.
            let _writer = self
                .writes
                .read()
                .map_err(|_| io::Error::other("write barrier poisoned"))?;
            let exists = self.read()?.index.contains_key(key);
            if exists {
                return self.commit_with_log_quota(operation, key, value);
            }
        }
        if self.max_live_keys.is_some() || self.max_live_key_bytes.is_some() {
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
        self.commit_with_log_quota(operation, key, value)
    }

    // Membership-changing and compound writes hold the exclusive barrier
    // through the check and append, preventing key quota oversubscription.
    fn commit_limited(&self, operation: Operation, key: &[u8], value: &[u8]) -> io::Result<()> {
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        let mut next_key_bytes = None;
        if matches!(operation, Operation::Put | Operation::Delete)
            && (self.max_live_keys.is_some() || self.max_live_key_bytes.is_some())
        {
            let inner = self.read()?;
            let exists = inner.index.contains_key(key);
            if operation == Operation::Put
                && !exists
                && self
                    .max_live_keys
                    .is_some_and(|max| inner.index.len() >= max)
            {
                return Err(io::Error::other(
                    "configured live key limit would be exceeded",
                ));
            }
            if let Some(max) = self.max_live_key_bytes {
                let current = self.live_key_bytes.load(Ordering::Relaxed);
                let next = match (operation, exists) {
                    (Operation::Put, false) => current
                        .checked_add(key.len())
                        .filter(|bytes| *bytes <= max)
                        .ok_or_else(|| {
                            io::Error::other("configured live key byte limit would be exceeded")
                        })?,
                    (Operation::Delete, true) => {
                        current.checked_sub(key.len()).ok_or_else(|| {
                            io::Error::other("live key byte accounting is inconsistent")
                        })?
                    }
                    _ => current,
                };
                next_key_bytes = Some(next);
            }
        }
        self.commit_with_log_quota(operation, key, value)?;
        if let Some(bytes) = next_key_bytes {
            self.live_key_bytes.store(bytes, Ordering::Relaxed);
        }
        Ok(())
    }

    fn commit_with_log_quota(
        &self,
        operation: Operation,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<()> {
        let Some(quota) = &self.log_quota else {
            return self
                .commits
                .commit(&self.log, &self.inner, operation, key, value);
        };
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        let bytes = (record::HEADER_SIZE + key.len() + value.len() + record::CHECKSUM_SIZE) as u64;
        let mut reservation = quota.reserve(bytes).map_err(|error| {
            if self.commits.failed() {
                uncertain_error()
            } else {
                error
            }
        })?;
        // A coordinator error after append poisons the store. Before-append
        // errors release the reservation; uncertain outcomes cannot be retried
        // on this open instance and are re-counted from the log after reopen.
        self.commits
            .commit(&self.log, &self.inner, operation, key, value)?;
        reservation.committed();
        Ok(())
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
mod direct_admission_tests;

#[cfg(test)]
mod shared_snapshot_tests;

#[cfg(all(test, target_os = "linux"))]
mod array_document_fault_tests;
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod collection_drop_fault_tests;

#[cfg(test)]
mod fault_tests;

#[cfg(test)]
mod group_tests;

#[cfg(test)]
mod initialization_tests;

#[cfg(test)]
mod read_tests;

#[cfg(test)]
mod reservation_tests;

#[cfg(test)]
mod wake_tests;

#[cfg(all(test, target_os = "linux"))]
mod metadata_tests;
