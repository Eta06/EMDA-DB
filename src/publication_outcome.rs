//! Conservative outcomes for publication and recovery.
use std::cell::Cell;
use std::fmt;
use std::io;

/// A failed writable recovery open, classified by this call's tail-witness update.
/// These outcomes describe the recovery attempt. Initialization may still
/// have created files, and an earlier attempt may have changed the witness.
#[derive(Debug)]
#[non_exhaustive]
pub enum RecoveryOpenFailure {
    /// This call never attempted to advance the witness for a replayed tail.
    NotAdopted(io::Error),
    /// The witness write or its required sync failed. Coordinate writers and
    /// inspect the source before retrying. The witness may have advanced.
    TailAdoptionOutcomeUnknown(io::Error),
    /// The witness update completed and synchronized before a later open
    /// error. The caller did not receive a store handle.
    TailAdopted(io::Error),
}

impl fmt::Display for RecoveryOpenFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAdopted(error) => {
                write!(formatter, "open failed before tail adoption: {error}")
            }
            Self::TailAdoptionOutcomeUnknown(error) => {
                write!(formatter, "open tail adoption outcome unknown: {error}")
            }
            Self::TailAdopted(error) => {
                write!(formatter, "open failed after tail adoption: {error}")
            }
        }
    }
}

impl std::error::Error for RecoveryOpenFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotAdopted(error)
            | Self::TailAdoptionOutcomeUnknown(error)
            | Self::TailAdopted(error) => Some(error),
        }
    }
}

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

impl ManagedRolloverFailure {
    pub(crate) fn io_error(&self) -> &io::Error {
        match self {
            Self::NotPublished(error)
            | Self::CheckpointOutcomeUnknown(error)
            | Self::SelectionOutcomeUnknown(error) => error,
        }
    }

    pub(crate) fn into_io_error(self) -> io::Error {
        match self {
            Self::NotPublished(error)
            | Self::CheckpointOutcomeUnknown(error)
            | Self::SelectionOutcomeUnknown(error) => error,
        }
    }
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
        Some(self.io_error())
    }
}

/// A failed retained rotation, classified by this call's successor and
/// selection attempts. Existing slots and staging artifacts can remain in
/// any case. The outcome does not certify store health or cleanup.
#[derive(Debug)]
#[non_exhaustive]
pub enum RetainedRotationFailure {
    /// This call began neither successor publication nor selection.
    NotPublished(io::Error),
    /// A successor may occupy the inactive slot. This call did not attempt
    /// selection. Inspect the slot and use catalog reclamation before retrying.
    SuccessorOutcomeUnknown(io::Error),
    /// Replacing the local selection or advancing the independent authority
    /// may have succeeded. Preserve both slots and reopen before retrying.
    SelectionOutcomeUnknown(io::Error),
}

impl RetainedRotationFailure {
    // The original compact methods classify the selection attempt only.
    pub(crate) fn into_selection_failure(self) -> PublicationFailure {
        match self {
            Self::NotPublished(error) | Self::SuccessorOutcomeUnknown(error) => {
                PublicationFailure::NotPublished(error)
            }
            Self::SelectionOutcomeUnknown(error) => PublicationFailure::OutcomeUnknown(error),
        }
    }
}

impl fmt::Display for RetainedRotationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublished(error) => {
                write!(formatter, "rotation publication not started: {error}")
            }
            Self::SuccessorOutcomeUnknown(error) => {
                write!(
                    formatter,
                    "rotation successor publication outcome unknown: {error}"
                )
            }
            Self::SelectionOutcomeUnknown(error) => {
                write!(formatter, "rotation selection outcome unknown: {error}")
            }
        }
    }
}

impl std::error::Error for RetainedRotationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error)
            | Self::SuccessorOutcomeUnknown(error)
            | Self::SelectionOutcomeUnknown(error) => Some(error),
        }
    }
}

/// A failed offline managed-selection rebuild, classified by this call's
/// tail adoption and publication attempts. A later failure can follow a
/// synchronized tail adoption. Existing paths and
/// staging artifacts may remain; these variants do not certify source health,
/// cleanup, or an independently recoverable checkpoint. The rebuild releases
/// its store lock before returning, so callers must coordinate other writers.
/// Panics unwind instead of returning a classification.
#[derive(Debug)]
#[non_exhaustive]
pub enum ManagedRebuildFailure {
    /// This call began neither checkpoint nor selection publication and
    /// did not return an uncertain witness update.
    NotPublished(io::Error),
    /// Updating the source witness for a complete replayed tail failed.
    /// The witness may have changed. Coordinate writers and inspect the
    /// source before retrying. Checkpoint and selection publication did
    /// not begin.
    TailAdoptionOutcomeUnknown(io::Error),
    /// This call may have published the checkpoint, but did not begin
    /// selection publication. No new digest is returned on failure.
    CheckpointOutcomeUnknown(io::Error),
    /// Selection publication may have occurred. Coordinate other writers and
    /// reopen the visible selection, or use trusted prior metadata or full-log
    /// recovery if needed. No new digest is returned on failure.
    SelectionOutcomeUnknown(io::Error),
}

impl fmt::Display for ManagedRebuildFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublished(error) => {
                write!(formatter, "rebuild publication not started: {error}")
            }
            Self::TailAdoptionOutcomeUnknown(error) => {
                write!(formatter, "rebuild tail adoption outcome unknown: {error}")
            }
            Self::CheckpointOutcomeUnknown(error) => {
                write!(
                    formatter,
                    "rebuild checkpoint publication outcome unknown: {error}"
                )
            }
            Self::SelectionOutcomeUnknown(error) => {
                write!(
                    formatter,
                    "rebuild selection publication outcome unknown: {error}"
                )
            }
        }
    }
}

impl std::error::Error for ManagedRebuildFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error)
            | Self::TailAdoptionOutcomeUnknown(error)
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

#[derive(Default)]
pub(crate) struct Tracker(Cell<bool>);

impl Tracker {
    pub(crate) fn begin_publication(&self) {
        self.0.set(true);
    }

    pub(crate) fn started(&self) -> bool {
        self.0.get()
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum TailAdoption {
    #[default]
    NotStarted,
    OutcomeUnknown,
    Synchronized,
}

#[derive(Default)]
pub(crate) struct TailAdoptionTracker(Cell<TailAdoption>);

impl TailAdoptionTracker {
    pub(crate) fn update(&self, write_witness: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        self.0.set(TailAdoption::OutcomeUnknown);
        write_witness()?;
        self.0.set(TailAdoption::Synchronized);
        Ok(())
    }
}

pub(crate) fn track_recovery_open<T>(
    action: impl FnOnce(&TailAdoptionTracker) -> io::Result<T>,
) -> Result<T, RecoveryOpenFailure> {
    let tracker = TailAdoptionTracker::default();
    action(&tracker).map_err(|error| match tracker.0.get() {
        TailAdoption::NotStarted => RecoveryOpenFailure::NotAdopted(error),
        TailAdoption::OutcomeUnknown => RecoveryOpenFailure::TailAdoptionOutcomeUnknown(error),
        TailAdoption::Synchronized => RecoveryOpenFailure::TailAdopted(error),
    })
}

#[derive(Default)]
pub(crate) struct ManagedTracker {
    pub(crate) checkpoint: Tracker,
    pub(crate) selection: Tracker,
    pub(crate) tail_adoption: TailAdoptionTracker,
}

pub(crate) fn track_managed<T>(
    action: impl FnOnce(&ManagedTracker) -> io::Result<T>,
) -> Result<T, ManagedRolloverFailure> {
    let tracker = ManagedTracker::default();
    action(&tracker).map_err(|error| {
        if tracker.selection.started() {
            ManagedRolloverFailure::SelectionOutcomeUnknown(error)
        } else if tracker.checkpoint.started() {
            ManagedRolloverFailure::CheckpointOutcomeUnknown(error)
        } else {
            ManagedRolloverFailure::NotPublished(error)
        }
    })
}

pub(crate) fn track_managed_rebuild<T>(
    action: impl FnOnce(&ManagedTracker) -> io::Result<T>,
) -> Result<T, ManagedRebuildFailure> {
    let tracker = ManagedTracker::default();
    action(&tracker).map_err(|error| {
        if tracker.selection.started() {
            ManagedRebuildFailure::SelectionOutcomeUnknown(error)
        } else if tracker.checkpoint.started() {
            ManagedRebuildFailure::CheckpointOutcomeUnknown(error)
        } else if tracker.tail_adoption.0.get() == TailAdoption::OutcomeUnknown {
            ManagedRebuildFailure::TailAdoptionOutcomeUnknown(error)
        } else {
            ManagedRebuildFailure::NotPublished(error)
        }
    })
}

pub(crate) fn track_retained_rotation<T>(
    action: impl FnOnce(&ManagedTracker) -> io::Result<T>,
) -> Result<T, RetainedRotationFailure> {
    track_managed(action).map_err(|failure| match failure {
        ManagedRolloverFailure::NotPublished(error) => RetainedRotationFailure::NotPublished(error),
        ManagedRolloverFailure::CheckpointOutcomeUnknown(error) => {
            RetainedRotationFailure::SuccessorOutcomeUnknown(error)
        }
        ManagedRolloverFailure::SelectionOutcomeUnknown(error) => {
            RetainedRotationFailure::SelectionOutcomeUnknown(error)
        }
    })
}

pub(crate) fn track<T>(
    action: impl FnOnce(&Tracker) -> io::Result<T>,
) -> Result<T, PublicationFailure> {
    let tracker = Tracker::default();
    action(&tracker).map_err(|error| {
        if tracker.started() {
            PublicationFailure::OutcomeUnknown(error)
        } else {
            PublicationFailure::NotPublished(error)
        }
    })
}
