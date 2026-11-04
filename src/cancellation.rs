//! Cooperative transaction cancellation and unambiguous pre-commit results.
use crate::TimedTransactionError;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

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
