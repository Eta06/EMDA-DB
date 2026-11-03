//! Public store operations, in-process coordination, and exclusive ownership.

use crate::log::{Log, ValueLocation};
use crate::record::{self, Operation};
use crate::{backup, directory};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

struct Inner {
    index: BTreeMap<Vec<u8>, ValueLocation>,
    uncertain: bool,
}

/// A single-process store. Successful mutations have been synchronized to disk.
pub struct Store {
    log: Log,
    inner: RwLock<Inner>,
    writer: Mutex<()>,
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
            inner: RwLock::new(Inner {
                index,
                uncertain: false,
            }),
            writer: Mutex::new(()),
            _lock_file: lock_file,
        })
    }

    /// Reads a value by key, returning `None` if the key is absent.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let inner = self.read()?;
        if inner.uncertain || self.writer.is_poisoned() {
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
        let _writer = self.writer()?;
        let inner = self.read()?;
        if inner.uncertain {
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
        // Encoding happens under the lock to avoid unbounded concurrent buffers.
        record::validate_key(key)?;
        let _writer = self.writer()?;
        if self.read()?.uncertain {
            return Err(uncertain_error());
        }
        let encoded = record::encode(operation, key, value)?;
        let location = match self.log.append(&encoded, key, value) {
            Ok(location) => location,
            Err(error) => {
                // A failed write or sync can have written some or all of the record.
                self.write()?.uncertain = true;
                return Err(io::Error::new(
                    error.kind(),
                    format!("write outcome is uncertain; reopen the store: {error}"),
                ));
            }
        };
        let mut inner = self.write().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("write outcome is uncertain; reopen the store: {error}"),
            )
        })?;
        match operation {
            Operation::Put => {
                inner.index.insert(key.to_vec(), location);
            }
            Operation::Delete => {
                inner.index.remove(key);
            }
        }
        Ok(())
    }

    fn read(&self) -> io::Result<RwLockReadGuard<'_, Inner>> {
        self.inner
            .read()
            .map_err(|_| io::Error::other("store lock poisoned"))
    }
    fn write(&self) -> io::Result<RwLockWriteGuard<'_, Inner>> {
        self.inner
            .write()
            .map_err(|_| io::Error::other("store lock poisoned"))
    }

    fn writer(&self) -> io::Result<MutexGuard<'_, ()>> {
        self.writer.lock().map_err(|_| {
            io::Error::other("writer lock poisoned; a write may have an uncertain outcome")
        })
    }
}

fn uncertain_error() -> io::Error {
    io::Error::other("a previous write had an uncertain outcome; reopen the store")
}

#[cfg(test)]
mod fault_tests;
