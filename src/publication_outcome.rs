//! Conservative outcomes for backup, restore, checkpoint and compaction publication.
use std::cell::Cell;
use std::fmt;
use std::io;

/// A failed copy publication, classified by this call's publication attempt.
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

pub(crate) struct Tracker(Cell<bool>);

impl Tracker {
    pub(crate) fn begin_publication(&self) {
        self.0.set(true);
    }
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
