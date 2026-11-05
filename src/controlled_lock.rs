//! A wakeable read/write lock with standard-style poison-on-writer-panic.
//!
//! `parking_lot` provides timed, queued admission but does not poison itself.
//! We publish the poison flag while the exclusive guard is still held, before
//! its underlying guard is dropped and the next operation can enter. As with
//! `std::sync::RwLock`, a reader panic does not poison and a write guard first
//! acquired during unwinding does not poison merely because it is dropped.
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LockResult, PoisonError, TryLockError, TryLockResult};
use std::time::Duration;

pub(crate) struct ControlledRwLock<T> {
    inner: RwLock<T>,
    poisoned: AtomicBool,
}

pub(crate) struct ReadGuard<'a, T> {
    inner: RwLockReadGuard<'a, T>,
}

pub(crate) struct WriteGuard<'a, T> {
    inner: RwLockWriteGuard<'a, T>,
    poisoned: &'a AtomicBool,
    acquired_during_panic: bool,
}

impl<T> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        if !self.acquired_during_panic && std::thread::panicking() {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

impl<T> fmt::Debug for ReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReadGuard(..)")
    }
}

impl<T> fmt::Debug for WriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WriteGuard(..)")
    }
}

impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}
impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}
impl<T> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> ControlledRwLock<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            inner: RwLock::new(value),
            poisoned: AtomicBool::new(false),
        }
    }

    fn wrap_read<'a>(&'a self, inner: RwLockReadGuard<'a, T>) -> LockResult<ReadGuard<'a, T>> {
        let guard = ReadGuard { inner };
        if self.poisoned.load(Ordering::Acquire) {
            Err(PoisonError::new(guard))
        } else {
            Ok(guard)
        }
    }

    fn wrap_write<'a>(&'a self, inner: RwLockWriteGuard<'a, T>) -> LockResult<WriteGuard<'a, T>> {
        let guard = WriteGuard {
            inner,
            poisoned: &self.poisoned,
            acquired_during_panic: std::thread::panicking(),
        };
        if self.poisoned.load(Ordering::Acquire) {
            Err(PoisonError::new(guard))
        } else {
            Ok(guard)
        }
    }

    pub(crate) fn read(&self) -> LockResult<ReadGuard<'_, T>> {
        self.wrap_read(self.inner.read())
    }

    pub(crate) fn write(&self) -> LockResult<WriteGuard<'_, T>> {
        self.wrap_write(self.inner.write())
    }

    #[cfg(test)]
    pub(crate) fn try_read(&self) -> TryLockResult<ReadGuard<'_, T>> {
        let guard = self.inner.try_read().ok_or(TryLockError::WouldBlock)?;
        self.wrap_read(guard).map_err(TryLockError::Poisoned)
    }

    pub(crate) fn try_read_for(&self, timeout: Duration) -> TryLockResult<ReadGuard<'_, T>> {
        let guard = self
            .inner
            .try_read_for(timeout)
            .ok_or(TryLockError::WouldBlock)?;
        self.wrap_read(guard).map_err(TryLockError::Poisoned)
    }

    pub(crate) fn try_write_for(&self, timeout: Duration) -> TryLockResult<WriteGuard<'_, T>> {
        let guard = self
            .inner
            .try_write_for(timeout)
            .ok_or(TryLockError::WouldBlock)?;
        self.wrap_write(guard).map_err(TryLockError::Poisoned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};
    use std::time::{Duration, Instant};

    #[test]
    fn writer_panic_poisoning_is_preserved_for_all_paths() {
        let lock = Arc::new(ControlledRwLock::new(7));
        let worker_lock = Arc::clone(&lock);
        assert!(
            std::thread::spawn(move || {
                let mut guard = worker_lock.write().unwrap();
                *guard = 8;
                panic!("poison writer");
            })
            .join()
            .is_err()
        );
        assert_eq!(**lock.read().unwrap_err().get_ref(), 8);
        assert_eq!(**lock.write().unwrap_err().get_ref(), 8);
        assert!(matches!(lock.try_read(), Err(TryLockError::Poisoned(_))));
        assert!(matches!(
            lock.try_read_for(Duration::from_secs(1)),
            Err(TryLockError::Poisoned(_))
        ));
        assert!(matches!(
            lock.try_write_for(Duration::from_secs(1)),
            Err(TryLockError::Poisoned(_))
        ));
    }

    #[test]
    fn reader_panic_does_not_poison() {
        let lock = Arc::new(ControlledRwLock::new(7));
        let worker_lock = Arc::clone(&lock);
        assert!(
            std::thread::spawn(move || {
                let _guard = worker_lock.read().unwrap();
                panic!("reader panic");
            })
            .join()
            .is_err()
        );
        assert_eq!(*lock.read().unwrap(), 7);
        *lock.write().unwrap() = 8;
    }

    #[test]
    fn caught_panics_and_write_guards_acquired_during_unwind_match_std_poisoning() {
        let lock = Arc::new(ControlledRwLock::new(0));
        {
            let mut held = lock.write().unwrap();
            let caught = std::panic::catch_unwind(|| panic!("caught within guard"));
            assert!(caught.is_err());
            *held = 1;
        }
        assert_eq!(*lock.read().unwrap(), 1);

        struct AcquireOnDrop(Arc<ControlledRwLock<usize>>);
        impl Drop for AcquireOnDrop {
            fn drop(&mut self) {
                *self.0.write().unwrap() = 2;
            }
        }
        let during_unwind = Arc::clone(&lock);
        assert!(
            std::thread::spawn(move || {
                let _guard = AcquireOnDrop(during_unwind);
                panic!("start unwinding");
            })
            .join()
            .is_err()
        );
        assert_eq!(*lock.read().unwrap(), 2);
    }

    #[test]
    fn timed_waiters_wake_when_the_lock_is_released() {
        for read in [false, true] {
            let lock = Arc::new(ControlledRwLock::new(()));
            let held = lock.write().unwrap();
            let worker_lock = Arc::clone(&lock);
            let (started_tx, started_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                let before = Instant::now();
                let acquired = if read {
                    worker_lock.try_read_for(Duration::from_secs(5)).is_ok()
                } else {
                    worker_lock.try_write_for(Duration::from_secs(5)).is_ok()
                };
                done_tx.send((acquired, before.elapsed())).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            drop(held);
            let (acquired, elapsed) = done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            worker.join().unwrap();
            assert!(acquired);
            assert!(elapsed < Duration::from_secs(3));
        }
    }
}
