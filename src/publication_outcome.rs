//! Conservative outcomes for verified copy publication.
use std::cell::Cell;
use std::fmt;
use std::io;

/// A failed publication, classified by this call's destination attempt.
///
/// A pre-existing destination may exist in either case. Temporary staging
/// artifacts can also remain after failures; these variants do not certify
/// cleanup, source health, or an independently recoverable backup. Panics
/// unwind instead of returning this value.
#[derive(Debug)]
#[non_exhaustive]
pub enum PublicationFailure {
    /// This call did not begin publishing its destination. The destination
    /// could already exist, and staging artifacts may remain.
    NotPublished(io::Error),
    /// Publication may have happened; inspect and reconcile the destination
    /// before retrying. Some pre-publication failures are classified here
    /// conservatively, including a non-overwrite destination conflict.
    OutcomeUnknown(io::Error),
}

impl fmt::Display for PublicationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublished(error) => write!(formatter, "publication not started: {error}"),
            Self::OutcomeUnknown(error) => {
                write!(formatter, "publication outcome unknown: {error}")
            }
        }
    }
}

impl std::error::Error for PublicationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error) | Self::OutcomeUnknown(error) => Some(error),
        }
    }
}

/// A failed managed checkpoint rollover, classified by this call's two
/// publication attempts. Pre-existing files and staging artifacts may exist
/// in every case. These variants do not certify store health, cleanup or an
/// independently recoverable checkpoint; panics unwind instead.
#[derive(Debug)]
#[non_exhaustive]
pub enum ManagedRolloverFailure {
    /// This call began neither checkpoint nor selection publication.
    NotPublished(io::Error),
    /// The checkpoint may have been published, but this call did not begin
    /// selection publication. No new digest is returned on failure.
    CheckpointOutcomeUnknown(io::Error),
    /// Selection publication may have occurred. The handle rejects access;
    /// reopen the visible selection, or recover with the previous trusted
    /// checkpoint or full log if no selection is visible.
    SelectionOutcomeUnknown(io::Error),
}

impl fmt::Display for ManagedRolloverFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublished(error) => write!(formatter, "publication not started: {error}"),
            Self::CheckpointOutcomeUnknown(error) => {
                write!(formatter, "checkpoint publication outcome unknown: {error}")
            }
            Self::SelectionOutcomeUnknown(error) => {
                write!(formatter, "selection publication outcome unknown: {error}")
            }
        }
    }
}

impl std::error::Error for ManagedRolloverFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error)
            | Self::CheckpointOutcomeUnknown(error)
            | Self::SelectionOutcomeUnknown(error) => Some(error),
        }
    }
}

pub(crate) struct Tracker(Cell<bool>);

impl Tracker {
    pub(crate) fn begin_publication(&self) {
        self.0.set(true);
    }
}

pub(crate) struct ManagedTracker {
    pub(crate) checkpoint: Tracker,
    selection_started: Cell<bool>,
}

impl ManagedTracker {
    pub(crate) fn begin_selection(&self) {
        self.selection_started.set(true);
    }
}

pub(crate) fn track_managed<T>(
    action: impl FnOnce(&ManagedTracker) -> io::Result<T>,
) -> Result<T, ManagedRolloverFailure> {
    let tracker = ManagedTracker {
        checkpoint: Tracker(Cell::new(false)),
        selection_started: Cell::new(false),
    };
    action(&tracker).map_err(|error| {
        if tracker.selection_started.get() {
            ManagedRolloverFailure::SelectionOutcomeUnknown(error)
        } else if tracker.checkpoint.0.get() {
            ManagedRolloverFailure::CheckpointOutcomeUnknown(error)
        } else {
            ManagedRolloverFailure::NotPublished(error)
        }
    })
}

pub(crate) fn track<T>(
    action: impl FnOnce(&Tracker) -> io::Result<T>,
) -> Result<T, PublicationFailure> {
    let tracker = Tracker(Cell::new(false));
    action(&tracker).map_err(|error| {
        if tracker.0.get() {
            PublicationFailure::OutcomeUnknown(error)
        } else {
            PublicationFailure::NotPublished(error)
        }
    })
}
