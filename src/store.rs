//! Public store operations, in-process coordination, and exclusive ownership.

use crate::log::{Log, ValueLocation};
use crate::record::{self, Operation};
use crate::{backup, directory};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard};

mod commit;
use commit::Coordinator;

struct Inner {
    index: BTreeMap<Vec<u8>, ValueLocation>,
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
    // Held for the store lifetime. OS file locks are advisory.
    _lock_file: File,
}

impl Store {
    /// Opens or creates a store. Only one store may hold its directory lock.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_using(path.as_ref(), Log::open)
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
            inner: RwLock::new(Inner { index }),
            writes: RwLock::new(()),
            commits: Coordinator::new(),
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
        self.commits
            .commit(&self.log, &self.inner, operation, key, value)?;
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
        self.commits.commit(
            &self.log,
            &self.inner,
            Operation::Put,
            key,
            &next.to_le_bytes(),
        )?;
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
        let _writer = self
            .writes
            .read()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
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
