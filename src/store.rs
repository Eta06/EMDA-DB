//! Public store operations, in-process coordination, and exclusive ownership.

use crate::log::{Log, ValueLocation};
use crate::record::{self, Operation};
use crate::{backup, directory};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard};

mod commit;
use commit::Coordinator;

struct Inner {
    index: BTreeMap<Vec<u8>, ValueLocation>,
}

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
        inner
            .index
            .get(key)
            .map(|location| self.log.read(*location))
            .transpose()
    }

    /// Replaces the value at `key`; synchronization completes before success.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append(Operation::Put, key, value)
    }

    /// Deletes a key. Deleting an absent key is valid and is persisted.
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.append(Operation::Delete, key, &[])
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
mod wake_tests;
