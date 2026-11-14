//! Conservative, opt-in classification of a transaction's own staged writes.
use crate::{ControlledTransactionError, TimedTransactionError};
use std::cell::Cell;
use std::fmt;
use std::io;

/// A failed transaction, classified by what the engine knows about its own
/// staged writes. Independent operations and external callback effects are
/// never rolled back. Panics do not return a `TransactionFailure`.
#[derive(Debug)]
#[non_exhaustive]
pub enum TransactionFailure {
    /// The callback or document staging returned an error. This transaction's
    /// staged writes were not submitted for commit. The error may describe
    /// an independent operation whose own outcome remains unknown.
    Callback(io::Error),
    /// The engine rejected the transaction before submitting its staged writes.
    /// This includes admission and validation failures, including conflicts.
    Rejected(io::Error),
    /// The engine detected cancellation before submitting staged writes.
    Cancelled,
    /// The engine detected expiry before submitting staged writes.
    DeadlineExceeded,
    /// Commit may have started. Staged writes can be absent, fully recovered,
    /// or recovery may fail closed; do not blindly retry a non-idempotent write.
    /// Some failures conservatively classified here actually precede append.
    OutcomeUnknown(io::Error),
}

impl fmt::Display for TransactionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Callback(error) => write!(formatter, "transaction callback failed: {error}"),
            Self::Rejected(error) => {
                write!(formatter, "transaction rejected before commit: {error}")
            }
            Self::Cancelled => formatter.write_str("transaction cancelled before commit"),
            Self::DeadlineExceeded => {
                formatter.write_str("transaction deadline exceeded before commit")
            }
            Self::OutcomeUnknown(error) => {
                write!(formatter, "transaction commit outcome unknown: {error}")
            }
        }
    }
}

impl std::error::Error for TransactionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Callback(error) | Self::Rejected(error) | Self::OutcomeUnknown(error) => {
                Some(error)
            }
            Self::Cancelled | Self::DeadlineExceeded => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    BeforeCallback,
    Callback,
    BeforeCommit,
    Commit,
}

pub(crate) struct Tracker(Cell<Phase>);

impl Tracker {
    pub(crate) fn new() -> Self {
        Self(Cell::new(Phase::BeforeCallback))
    }

    pub(crate) fn callback(&self) {
        self.0.set(Phase::Callback);
    }

    pub(crate) fn before_commit(&self) {
        self.0.set(Phase::BeforeCommit);
    }

    pub(crate) fn commit(&self) {
        self.0.set(Phase::Commit);
    }

    pub(crate) fn classify(&self, error: TimedTransactionError) -> TransactionFailure {
        // A callback can itself return any I/O error, including an engine
        // marker from an independent operation. It must remain a callback
        // failure instead of being attributed to this call's controls.
        if self.0.get() == Phase::Callback {
            return TransactionFailure::Callback(error.into_io());
        }
        match crate::cancellation::classify(error) {
            ControlledTransactionError::Cancelled => TransactionFailure::Cancelled,
            ControlledTransactionError::DeadlineExceeded => TransactionFailure::DeadlineExceeded,
            ControlledTransactionError::Operation(error) if self.0.get() == Phase::Commit => {
                TransactionFailure::OutcomeUnknown(error)
            }
            ControlledTransactionError::Operation(error) => TransactionFailure::Rejected(error),
        }
    }
}
