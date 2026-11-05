//! Cooperative transaction cancellation and unambiguous pre-commit results.
use crate::TimedTransactionError;
use crate::controlled_lock::{ControlledRwLock, ReadGuard, WriteGuard};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, TryLockError, TryLockResult};
use std::time::{Duration, Instant};

const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Clone, Copy, Default)]
pub(crate) struct CommitControl<'a> {
    pub(crate) deadline: Option<Instant>,
    pub(crate) cancellation: Option<&'a CancellationToken>,
}

/// A cloneable, one-way cancellation signal shared by transaction callers.
/// Cancellation is cooperative and does not interrupt callbacks or I/O.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    /// Creates a fresh, uncancelled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation. Repeated calls have no additional effect.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Reports whether cancellation was requested for this token.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// An engine-detected pre-commit cancellation/deadline, or an operation error.
#[derive(Debug)]
#[non_exhaustive]
pub enum ControlledTransactionError {
    /// The engine observed cancellation before submitting this transaction's
    /// staged writes for commit. Callback side effects are not rolled back.
    Cancelled,
    /// The deadline was detected before submitting staged writes for commit.
    DeadlineExceeded,
    /// A callback, validation, admission or I/O error. A commit I/O error can
    /// have an unknown outcome even if cancellation was requested concurrently.
    Operation(io::Error),
}
impl fmt::Display for ControlledTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("transaction cancelled before commit"),
            Self::DeadlineExceeded => {
                formatter.write_str("transaction deadline exceeded before commit")
            }
            Self::Operation(error) => write!(formatter, "transaction failed: {error}"),
        }
    }
}
impl std::error::Error for ControlledTransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(error) => Some(error),
            _ => None,
        }
    }
}

// A private marker distinguishes our pre-commit cancellation from arbitrary
// Interrupted I/O errors, whose commit outcome may be unknown.
#[derive(Debug)]
struct BeforeCommitCancellation;
impl fmt::Display for BeforeCommitCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transaction cancelled before commit")
    }
}
impl std::error::Error for BeforeCommitCancellation {}

#[inline]
pub(crate) fn check(token: Option<&CancellationToken>) -> io::Result<()> {
    if token.is_some_and(CancellationToken::is_cancelled) {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            BeforeCommitCancellation,
        ))
    } else {
        Ok(())
    }
}

// Wait in the fair gate rather than repeatedly attempting a nonblocking lock.
// The wait wakes on release, and periodically checks cancellation or deadline.
// Callers recheck control before append; commit I/O is not interrupted.
fn wait_for_lock<T>(
    mut attempt: impl FnMut(Duration) -> TryLockResult<T>,
    deadline: Option<Instant>,
    cancellation: Option<&CancellationToken>,
    poisoned: impl Fn() -> io::Error,
) -> Result<T, TimedTransactionError> {
    loop {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(TimedTransactionError::DeadlineExceeded);
        }
        check(cancellation)?;
        let delay = deadline.map_or(LOCK_POLL_INTERVAL, |deadline| {
            deadline
                .saturating_duration_since(Instant::now())
                .min(LOCK_POLL_INTERVAL)
        });
        match attempt(delay) {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => return Err(poisoned().into()),
            Err(TryLockError::WouldBlock) => continue,
        }
    }
}

pub(crate) fn read_lock<'a, T>(
    lock: &'a ControlledRwLock<T>,
    deadline: Option<Instant>,
    cancellation: Option<&CancellationToken>,
    poisoned: impl Fn() -> io::Error,
) -> Result<ReadGuard<'a, T>, TimedTransactionError> {
    if deadline.is_none() && cancellation.is_none() {
        return lock.read().map_err(|_| poisoned().into());
    }
    wait_for_lock(
        |delay| lock.try_read_for(delay),
        deadline,
        cancellation,
        poisoned,
    )
}

pub(crate) fn write_lock<'a, T>(
    lock: &'a ControlledRwLock<T>,
    deadline: Option<Instant>,
    cancellation: Option<&CancellationToken>,
    poisoned: impl Fn() -> io::Error,
) -> Result<WriteGuard<'a, T>, TimedTransactionError> {
    if deadline.is_none() && cancellation.is_none() {
        return lock.write().map_err(|_| poisoned().into());
    }
    wait_for_lock(
        |delay| lock.try_write_for(delay),
        deadline,
        cancellation,
        poisoned,
    )
}

pub(crate) fn classify(error: TimedTransactionError) -> ControlledTransactionError {
    match error {
        TimedTransactionError::DeadlineExceeded => ControlledTransactionError::DeadlineExceeded,
        TimedTransactionError::Operation(error)
            if error
                .get_ref()
                .is_some_and(|inner| inner.is::<BeforeCommitCancellation>()) =>
        {
            ControlledTransactionError::Cancelled
        }
        TimedTransactionError::Operation(error) => ControlledTransactionError::Operation(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{PoisonError, mpsc};

    // Drop any acquired guard immediately; the test intentionally keeps the
    // writer held until the waiting operation has returned.
    fn try_lock(lock: &ControlledRwLock<()>, read: bool, delay: Duration) -> TryLockResult<()> {
        if read {
            match lock.try_read_for(delay) {
                Ok(guard) => {
                    drop(guard);
                    Ok(())
                }
                Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
                Err(TryLockError::Poisoned(_)) => Err(TryLockError::Poisoned(PoisonError::new(()))),
            }
        } else {
            match lock.try_write_for(delay) {
                Ok(guard) => {
                    drop(guard);
                    Ok(())
                }
                Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
                Err(TryLockError::Poisoned(_)) => Err(TryLockError::Poisoned(PoisonError::new(()))),
            }
        }
    }

    #[test]
    fn cancelled_and_expired_waits_finish_while_read_or_write_lock_is_busy() {
        for read in [false, true] {
            for cancelled in [false, true] {
                let lock = Arc::new(ControlledRwLock::new(()));
                let held = lock.write().unwrap();
                let token = CancellationToken::new();
                let worker_token = token.clone();
                let worker_lock = Arc::clone(&lock);
                let (blocked_tx, blocked_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel();
                let deadline = (!cancelled).then(|| Instant::now() + Duration::from_secs(2));
                let worker = std::thread::spawn(move || {
                    let mut first = Some(blocked_tx);
                    let result = wait_for_lock(
                        |delay| {
                            let result = try_lock(&worker_lock, read, delay);
                            if matches!(result, Err(TryLockError::WouldBlock)) {
                                if let Some(signal) = first.take() {
                                    signal.send(()).unwrap();
                                }
                            }
                            result
                        },
                        deadline,
                        Some(&worker_token),
                        || io::Error::other("poisoned fixture"),
                    );
                    done_tx.send(result).unwrap();
                });
                blocked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                if cancelled {
                    token.cancel();
                }
                let early = done_rx.recv_timeout(Duration::from_secs(5));
                drop(held);
                let finished_while_locked = early.is_ok();
                let result =
                    early.unwrap_or_else(|_| done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
                worker.join().unwrap();
                assert!(
                    finished_while_locked,
                    "controlled wait did not exit while lock remained held"
                );
                if cancelled {
                    assert!(matches!(
                        classify(result.unwrap_err()),
                        ControlledTransactionError::Cancelled
                    ));
                } else {
                    assert!(matches!(
                        result,
                        Err(TimedTransactionError::DeadlineExceeded)
                    ));
                }
            }
        }
    }

    #[test]
    fn poisoned_locks_remain_operation_errors() {
        let lock = Arc::new(ControlledRwLock::new(()));
        let poisoning = Arc::clone(&lock);
        let _ = std::thread::spawn(move || {
            let _guard = poisoning.write().unwrap();
            panic!("poison fixture");
        })
        .join();
        let token = CancellationToken::new();
        for error in [
            read_lock(&lock, None, Some(&token), || {
                io::Error::other("read poison")
            })
            .err()
            .unwrap(),
            write_lock(&lock, None, Some(&token), || {
                io::Error::other("write poison")
            })
            .err()
            .unwrap(),
        ] {
            assert!(
                matches!(error, TimedTransactionError::Operation(ref inner) if inner.kind() == io::ErrorKind::Other)
            );
        }
    }
}
