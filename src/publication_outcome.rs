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

/// A failed consuming compaction handoff, classified by this call's
/// publication attempts. The original handle is consumed in every case.
/// Existing paths and temporary artifacts may remain; these outcomes do not
/// certify store health, cleanup or a separately recoverable backup. Panics
/// unwind rather than returning a classification.
#[derive(Debug)]
#[non_exhaustive]
pub enum HandoffFailure {
    /// This call began neither candidate publication nor directory exchange.
    NotPublished(io::Error),
    /// The candidate may exist or be partly configured, but this call did not
    /// begin the directory exchange. Reconcile it before retrying.
    CandidateOutcomeUnknown(io::Error),
    /// The directory exchange may have happened. Coordinate access and reopen
    /// both paths to inspect and reconcile them before retrying. No new
    /// checkpoint digest is returned on error.
    ExchangeOutcomeUnknown(io::Error),
}

impl fmt::Display for HandoffFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublished(error) => {
                write!(formatter, "handoff publication not started: {error}")
            }
            Self::CandidateOutcomeUnknown(error) => {
                write!(formatter, "handoff candidate outcome unknown: {error}")
            }
            Self::ExchangeOutcomeUnknown(error) => {
                write!(formatter, "handoff exchange outcome unknown: {error}")
            }
        }
    }
}

impl std::error::Error for HandoffFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error)
            | Self::CandidateOutcomeUnknown(error)
            | Self::ExchangeOutcomeUnknown(error) => Some(error),
        }
    }
}

/// A failed online compaction handoff, classified by this call's publication
/// attempts. The handle is not consumed; independent writes can occur during
/// preparation. Existing paths and temporary artifacts may remain. These
/// outcomes do not certify source health, cleanup or an independent backup.
/// Panics unwind rather than returning a classification.
#[derive(Debug)]
#[non_exhaustive]
pub enum OnlineHandoffFailure {
    /// This call began neither destination publication nor directory exchange.
    NotPublished(io::Error),
    /// The candidate may exist, but this call did not begin the exchange. It
    /// may omit concurrent source writes and must be reconciled before use.
    CandidateOutcomeUnknown(io::Error),
    /// Exchange may have happened and the handle rejects access. Coordinate
    /// callers, drop it, and reopen both paths to reconcile before retrying.
    ExchangeOutcomeUnknown(io::Error),
}

impl fmt::Display for OnlineHandoffFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublished(error) => {
                write!(formatter, "online handoff publication not started: {error}")
            }
            Self::CandidateOutcomeUnknown(error) => {
                write!(
                    formatter,
                    "online handoff candidate outcome unknown: {error}"
                )
            }
            Self::ExchangeOutcomeUnknown(error) => {
                write!(
                    formatter,
                    "online handoff exchange outcome unknown: {error}"
                )
            }
        }
    }
}

impl std::error::Error for OnlineHandoffFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error)
            | Self::CandidateOutcomeUnknown(error)
            | Self::ExchangeOutcomeUnknown(error) => Some(error),
        }
    }
}

#[derive(Clone, Copy)]
enum HandoffPhase {
    Preparation,
    Candidate,
    Exchange,
}

pub(crate) struct HandoffTracker(Cell<HandoffPhase>);

impl HandoffTracker {
    pub(crate) fn begin_candidate(&self) {
        self.0.set(HandoffPhase::Candidate);
    }

    pub(crate) fn begin_exchange(&self) {
        self.0.set(HandoffPhase::Exchange);
    }
}

pub(crate) fn track_handoff<T>(
    action: impl FnOnce(&HandoffTracker) -> io::Result<T>,
) -> Result<T, HandoffFailure> {
    let tracker = HandoffTracker(Cell::new(HandoffPhase::Preparation));
    action(&tracker).map_err(|error| match tracker.0.get() {
        HandoffPhase::Preparation => HandoffFailure::NotPublished(error),
        HandoffPhase::Candidate => HandoffFailure::CandidateOutcomeUnknown(error),
        HandoffPhase::Exchange => HandoffFailure::ExchangeOutcomeUnknown(error),
    })
}

pub(crate) fn track_online_handoff<T>(
    action: impl FnOnce(&HandoffTracker) -> io::Result<T>,
) -> Result<T, OnlineHandoffFailure> {
    track_handoff(action).map_err(|failure| match failure {
        HandoffFailure::NotPublished(error) => OnlineHandoffFailure::NotPublished(error),
        HandoffFailure::CandidateOutcomeUnknown(error) => {
            OnlineHandoffFailure::CandidateOutcomeUnknown(error)
        }
        HandoffFailure::ExchangeOutcomeUnknown(error) => {
            OnlineHandoffFailure::ExchangeOutcomeUnknown(error)
        }
    })
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
