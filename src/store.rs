//! Public store operations, in-process coordination, and exclusive ownership.

use crate::log::{Log, ValueLocation};
use crate::record::{self, Operation};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

struct Inner {
    log: Log,
    index: BTreeMap<Vec<u8>, ValueLocation>,
    uncertain: bool,
}

/// A single-process store. Successful mutations have been synchronized to disk.
pub struct Store {
    inner: RwLock<Inner>,
    // Held for the store lifetime. OS file locks are advisory.
    _lock_file: File,
}

impl Store {
    /// Opens or creates a store. Only one store may hold its directory lock.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        fs::create_dir_all(path)?;
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("store.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock_file)?;
        let (log, index) = Log::open(path)?;
        Ok(Self {
            inner: RwLock::new(Inner {
                log,
                index,
                uncertain: false,
            }),
            _lock_file: lock_file,
        })
    }

    /// Reads a value by key, returning `None` if the key is absent.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let inner = self.read()?;
        if inner.uncertain {
            return Err(uncertain_error());
        }
        inner
            .index
            .get(key)
            .map(|location| inner.log.read(*location))
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

    fn append(&self, operation: Operation, key: &[u8], value: &[u8]) -> io::Result<()> {
        // Encoding happens under the lock to avoid unbounded concurrent buffers.
        record::validate_key(key)?;
        let mut inner = self.write()?;
        if inner.uncertain {
            return Err(uncertain_error());
        }
        let encoded = record::encode(operation, key, value)?;
        let location = match inner.log.append(&encoded, key, value) {
            Ok(location) => location,
            Err(error) => {
                // A failed write or sync can have written some or all of the record.
                inner.uncertain = true;
                return Err(io::Error::new(
                    error.kind(),
                    format!("write outcome is uncertain; reopen the store: {error}"),
                ));
            }
        };
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
}

fn uncertain_error() -> io::Error {
    io::Error::other("a previous write had an uncertain outcome; reopen the store")
}
