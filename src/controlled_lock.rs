//! A wakeable admission gate around a standard, poisoning read/write lock.
//!
//! Every access to `inner` must first hold the corresponding gate guard. The
//! standard guard is declared first and therefore dropped first, so a panic
//! poisons the inner lock before the gate admits the next operation.
use parking_lot::{RwLock as Gate, RwLockReadGuard as GateRead, RwLockWriteGuard as GateWrite};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{
    LockResult, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError, TryLockResult,
};
use std::time::Duration;

pub(crate) struct ControlledRwLock<T> {
    gate: Gate<()>,
    inner: RwLock<T>,
}

pub(crate) struct ReadGuard<'a, T> {
    inner: RwLockReadGuard<'a, T>,
    _gate: GateRead<'a, ()>,
}

pub(crate) struct WriteGuard<'a, T> {
    inner: RwLockWriteGuard<'a, T>,
    _gate: GateWrite<'a, ()>,
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
            gate: Gate::new(()),
            inner: RwLock::new(value),
        }
    }

    fn read_after_gate<'a>(&'a self, gate: GateRead<'a, ()>) -> LockResult<ReadGuard<'a, T>> {
        match self.inner.try_read() {
            Ok(inner) => Ok(ReadGuard { inner, _gate: gate }),
            Err(TryLockError::Poisoned(error)) => Err(PoisonError::new(ReadGuard {
                inner: error.into_inner(),
                _gate: gate,
            })),
            Err(TryLockError::WouldBlock) => unreachable!("controlled read gate invariant"),
        }
    }

    fn write_after_gate<'a>(&'a self, gate: GateWrite<'a, ()>) -> LockResult<WriteGuard<'a, T>> {
        match self.inner.try_write() {
            Ok(inner) => Ok(WriteGuard { inner, _gate: gate }),
            Err(TryLockError::Poisoned(error)) => Err(PoisonError::new(WriteGuard {
                inner: error.into_inner(),
                _gate: gate,
            })),
            Err(TryLockError::WouldBlock) => unreachable!("controlled write gate invariant"),
        }
    }

    pub(crate) fn read(&self) -> LockResult<ReadGuard<'_, T>> {
        self.read_after_gate(self.gate.read())
    }

    pub(crate) fn write(&self) -> LockResult<WriteGuard<'_, T>> {
        self.write_after_gate(self.gate.write())
    }

    #[cfg(test)]
    pub(crate) fn try_read(&self) -> TryLockResult<ReadGuard<'_, T>> {
        let gate = self.gate.try_read().ok_or(TryLockError::WouldBlock)?;
        self.read_after_gate(gate).map_err(TryLockError::Poisoned)
    }

    pub(crate) fn try_read_for(&self, timeout: Duration) -> TryLockResult<ReadGuard<'_, T>> {
        let gate = self
            .gate
            .try_read_for(timeout)
            .ok_or(TryLockError::WouldBlock)?;
        self.read_after_gate(gate).map_err(TryLockError::Poisoned)
    }

    pub(crate) fn try_write_for(&self, timeout: Duration) -> TryLockResult<WriteGuard<'_, T>> {
        let gate = self
            .gate
            .try_write_for(timeout)
            .ok_or(TryLockError::WouldBlock)?;
        self.write_after_gate(gate).map_err(TryLockError::Poisoned)
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
