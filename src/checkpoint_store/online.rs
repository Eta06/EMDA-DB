//! A guarded checkpoint-backed store with online compaction and reclamation.
//!
//! The handoff mirrors [`crate::OnlineStore`]: a verified compacted copy of a
//! captured view is prepared while callbacks continue, acknowledged writes
//! are replayed into it, and callbacks pause only for the final suffix,
//! verification, managed selection and the atomic directory exchange.
use super::{
    CheckpointCompactionLimits, CheckpointSnapshot, CheckpointStore, automatic, selection,
};
use crate::record::batch::BatchOperation;
use crate::{
    MAX_ATOMIC_BATCH_BYTES, OnlineCompactionReport, OnlineReclaimError, OnlineReclaimFailure,
};
use sha2::Sha256;
use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, TryLockError};
use std::time::Instant;

const MAX_GROUP_RECORDS: usize = 64;
const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
struct MaintenanceBusy;

impl std::fmt::Display for MaintenanceBusy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("online checkpoint compaction already running")
    }
}

impl std::error::Error for MaintenanceBusy {}

// A scheduled check found no safe room; nothing was reserved or published.
#[derive(Debug)]
struct ScheduledDeferral;

impl std::fmt::Display for ScheduledDeferral {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("scheduled checkpoint compaction deferred")
    }
}

impl std::error::Error for ScheduledDeferral {}

thread_local! { static ACTIVE_ACCESS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) }; }

struct Access(usize);

impl Access {
    fn enter(id: usize) -> io::Result<Self> {
        ACTIVE_ACCESS.with(|active| {
            let mut active = active.borrow_mut();
            if active.contains(&id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "nested access to the same online checkpoint store is unsupported",
                ));
            }
            active.push(id);
            Ok(Self(id))
        })
    }

    fn check_not_active(id: usize) -> io::Result<()> {
        ACTIVE_ACCESS.with(|active| {
            if active.borrow().contains(&id) {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot compact from an online checkpoint store callback",
                ))
            } else {
                Ok(())
            }
        })
    }
}

impl Drop for Access {
    fn drop(&mut self) {
        ACTIVE_ACCESS.with(|active| active.borrow_mut().retain(|id| *id != self.0));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Point {
    // After the disk-budget reservation is durable, before any allocation.
    AfterReservation,
    InitialCopy,
    BeforeFinalBarrier,
    SelectionBeforeRename,
    SelectionBeforeSync,
    BeforeExchange,
    AfterExchange,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetirePoint {
    BeforeMove,
    AfterMove,
    AfterMoveParentSync,
    AfterAutomaticRoot,
    AfterCheckpointFiles,
    AfterRecords,
    AfterLock,
    AfterRetiredRemoval,
    AfterParentSync,
}

struct State {
    store: Arc<CheckpointStore>,
    failed: bool,
}

/// A guarded managed checkpoint-backed store that can compact its log while
/// callbacks continue, then optionally remove the previous generation.
///
/// It is experimental and single-process. Only callbacks through this
/// handle are coordinated; it is not a server, cross-process transaction
/// manager or security boundary.
pub struct OnlineCheckpointStore {
    path: PathBuf,
    state: RwLock<State>,
    maintenance: Mutex<()>,
    reclamation_worker: std::sync::atomic::AtomicBool,
    // Previous generations replaced through this handle. A live token means
    // an owned snapshot still holds that generation and its disk blocks.
    retired: Mutex<Vec<std::sync::Weak<super::disk_budget::GenerationToken>>>,
}

/// An owned read-only view captured from an online checkpoint store.
///
/// The view keeps its generation's descriptors open. After reclamation the
/// retired names are removed, but their disk blocks stay allocated until the
/// last such view is dropped or cancelled.
pub struct OnlineCheckpointSnapshot {
    snapshot: Option<CheckpointSnapshot<'static>>,
}

impl OnlineCheckpointSnapshot {
    /// Borrows the captured view for reads.
    pub fn as_snapshot(&self) -> io::Result<&CheckpointSnapshot<'static>> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| io::Error::other("checkpoint snapshot has been cancelled"))
    }

    /// Cancels the view and releases its reservation. Later reads fail.
    pub fn cancel(&mut self) {
        self.snapshot = None;
    }
}

/// Logical limits for one online checkpoint compaction attempt. These are
/// not total process-memory or disk quotas.
#[derive(Clone, Copy, Debug)]
pub struct OnlineCheckpointCompactionOptions {
    /// Maximum synchronized catch-up passes while normal operations continue.
    pub catchup_rounds: usize,
    /// Reject final handoff if the remaining source suffix exceeds this size.
    pub max_final_delta_bytes: u64,
    /// With a disk budget, the replacement log may grow by at most this many
    /// bytes during catch-up; it is reserved up front and enforced as the
    /// replacement's log limit until the exchange. Ignored without a budget.
    pub max_catchup_bytes: u64,
    /// Ceilings for the initial copy of the captured view, as for
    /// [`CheckpointStore::compact_to_with_limits_detailed`]. Catch-up and
    /// final verification reads are not charged.
    pub copy_limits: CheckpointCompactionLimits,
}

impl Default for OnlineCheckpointCompactionOptions {
    fn default() -> Self {
        Self {
            catchup_rounds: 2,
            max_final_delta_bytes: 16 * 1024 * 1024,
            max_catchup_bytes: 16 * 1024 * 1024,
            copy_limits: CheckpointCompactionLimits::default(),
        }
    }
}

impl OnlineCheckpointCompactionOptions {
    fn validate(&self) -> io::Result<()> {
        if self.catchup_rounds > 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "online compaction options exceed supported limits",
            ));
        }
        Ok(())
    }
}

fn uncertain() -> io::Error {
    io::Error::other(
        "online checkpoint store handoff may have an uncertain outcome; reopen required",
    )
}

// The writer gate excludes appends and checkpoint maintenance, so the log
// length and witness describe exactly the committed state.
fn boundary_locked(store: &CheckpointStore) -> io::Result<u64> {
    let state = store.read()?;
    store.log.verify_witness()?;
    let length = store.log.len()?;
    if length.checked_sub(state.view.base.log_boundary()) != Some(state.view.tail_bytes) {
        return Err(crate::record::invalid_data(
            "checkpoint suffix length changed",
        ));
    }
    Ok(length)
}

fn boundary(store: &CheckpointStore) -> io::Result<u64> {
    let _writer = store.writers.write().map_err(|_| uncertain())?;
    boundary_locked(store)
}

struct Writer<'a> {
    target: &'a CheckpointStore,
    operations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    bytes: usize,
}

impl<'a> Writer<'a> {
    fn new(target: &'a CheckpointStore) -> Self {
        Self {
            target,
            operations: Vec::new(),
            bytes: 4,
        }
    }

    fn push(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> io::Result<()> {
        let length = crate::record::HEADER_SIZE
            + key.len()
            + value.as_ref().map_or(0, Vec::len)
            + crate::record::CHECKSUM_SIZE;
        if !self.operations.is_empty()
            && (self.operations.len() >= MAX_GROUP_RECORDS || self.bytes + length > MAX_GROUP_BYTES)
        {
            self.flush()?;
        }
        if length + 4 > MAX_ATOMIC_BATCH_BYTES {
            match value {
                Some(value) => self.target.put(&key, &value)?,
                None => self.target.delete(&key)?,
            }
        } else {
            self.bytes += length;
            self.operations.push((key, value));
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.operations.is_empty() {
            return Ok(());
        }
        let operations = self
            .operations
            .iter()
            .map(|(key, value)| match value {
                Some(value) => BatchOperation::Put { key, value },
                None => BatchOperation::Delete { key },
            })
            .collect::<Vec<_>>();
        self.target.write_batch(&operations)?;
        self.operations.clear();
        self.bytes = 4;
        Ok(())
    }
}

// Replays committed source records into the private candidate. Each
// candidate batch is synchronized before the next source range is read.
fn replay(
    source: &CheckpointStore,
    target: &CheckpointStore,
    start: u64,
    end: u64,
    digest: &mut Sha256,
) -> io::Result<()> {
    if end < start {
        return Err(io::Error::other("source log boundary moved backward"));
    }
    let mut writer = Writer::new(target);
    source
        .log
        .visit_records_with_digest(start, end, end, digest, |key, location| {
            let value = location
                .map(|location| source.log.read(location))
                .transpose()?;
            writer.push(key.into_owned(), value)
        })?;
    writer.flush()
}

const VERIFY_WINDOW_KEYS: usize = 1024;
const VERIFY_WINDOW_KEY_BYTES: usize = 256 * 1024;
const VERIFY_CHUNK_BYTES: usize = crate::recovery::RECOVERY_READ_BUFFER_BYTES;

fn mismatch() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "online checkpoint candidate does not match the paused source",
    )
}

// Compares the complete ordered live sets of the paused source and the
// candidate. Source keys and locations are taken in bounded windows; the
// candidate is walked over the same key range and each value pair is
// streamed through two fixed buffers. Scratch is bounded by the window caps
// plus two chunk buffers, independently of value sizes. Elapsed time still
// grows with the store.
fn verify_candidate(source: &CheckpointStore, candidate: &CheckpointStore) -> io::Result<()> {
    let source_state = source.read()?;
    let candidate_state = candidate.read()?;
    let mut keys = Vec::new();
    let mut window: Vec<(usize, usize, crate::log::ValueLocation)> = Vec::new();
    let (mut buffer, mut other_buffer) = (vec![0; VERIFY_CHUNK_BYTES], vec![0; VERIFY_CHUNK_BYTES]);
    let mut after: Option<Vec<u8>> = None;
    loop {
        keys.clear();
        window.clear();
        source_state
            .view
            .visit_entries(after.as_deref(), None, |key, location| {
                if !window.is_empty()
                    && (window.len() >= VERIFY_WINDOW_KEYS
                        || keys.len() + key.len() > VERIFY_WINDOW_KEY_BYTES)
                {
                    return Ok(std::ops::ControlFlow::Break(()));
                }
                window.push((keys.len(), key.len(), location));
                keys.extend_from_slice(key);
                Ok(std::ops::ControlFlow::Continue(()))
            })?;
        let last = window
            .last()
            .map(|&(start, length, _)| &keys[start..start + length]);
        let mut matched = 0;
        candidate_state
            .view
            .visit_entries(after.as_deref(), None, |key, location| {
                let Some(&(start, length, expected)) = window.get(matched) else {
                    // Past the window: stop at the next window, or report a
                    // candidate key the source lacks.
                    return match last {
                        Some(last) if key > last => Ok(std::ops::ControlFlow::Break(())),
                        _ => Err(mismatch()),
                    };
                };
                if key != &keys[start..start + length]
                    || !source.log.value_equals(
                        expected,
                        &candidate.log,
                        location,
                        (&mut buffer, &mut other_buffer),
                    )?
                {
                    return Err(mismatch());
                }
                matched += 1;
                Ok(std::ops::ControlFlow::Continue(()))
            })?;
        if matched != window.len() {
            return Err(mismatch());
        }
        match last {
            Some(last) => after = Some(last.to_vec()),
            None => return Ok(()),
        }
    }
}

impl OnlineCheckpointStore {
    /// Opens a managed checkpoint-backed store through the guarded handle.
    pub fn open_managed(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::from_store(CheckpointStore::open_managed(path)?)
    }

    /// Guards an existing managed checkpoint-backed store. Configure its
    /// limits and automatic checkpoints first; they carry over to each
    /// replacement generation. An unmanaged store is rejected.
    pub fn from_store(store: CheckpointStore) -> io::Result<Self> {
        let path = fs::canonicalize(&store.path)?;
        {
            let state = store.read()?;
            if state.selection.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "online checkpoint compaction requires a managed store",
                ));
            }
            state.view.base.share_store_lock()?.verify_identity(&path)?;
        }
        Ok(Self {
            path,
            state: RwLock::new(State {
                store: Arc::new(store),
                failed: false,
            }),
            maintenance: Mutex::new(()),
            reclamation_worker: std::sync::atomic::AtomicBool::new(false),
            retired: Mutex::new(Vec::new()),
        })
    }

    fn id(&self) -> usize {
        self as *const Self as usize
    }

    /// Uses the current generation for the duration of the callback. The
    /// callback must not re-enter this handle or wait for a thread that is
    /// compacting it. No borrowed transaction, snapshot or worker may escape.
    pub fn with_store<T>(
        &self,
        action: impl FnOnce(&CheckpointStore) -> io::Result<T>,
    ) -> io::Result<T> {
        let _access = Access::enter(self.id())?;
        let state = self.state.read().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        action(&state.store)
    }

    /// Captures a view that can outlive callbacks and handoffs. It sees only
    /// its captured state, including after the previous generation is
    /// reclaimed. Its reservation shares one budget across generations.
    pub fn snapshot(&self) -> io::Result<OnlineCheckpointSnapshot> {
        self.capture(None)
    }

    /// Captures a view whose new reads expire at a process-local deadline.
    /// Later captures or explicit reaping release idle expired views of any
    /// generation; a read in progress can finish.
    pub fn snapshot_until(&self, deadline: Instant) -> io::Result<OnlineCheckpointSnapshot> {
        self.capture(Some(deadline))
    }

    fn capture(&self, deadline: Option<Instant>) -> io::Result<OnlineCheckpointSnapshot> {
        let _access = Access::enter(self.id())?;
        let state = self.state.read().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        let store = Arc::clone(&state.store);
        let borrowed = match deadline {
            Some(deadline) => store.snapshot_until(deadline)?,
            None => store.snapshot()?,
        };
        Ok(OnlineCheckpointSnapshot {
            snapshot: Some(borrowed.into_owned(Arc::clone(&store))),
        })
    }

    /// Installs, changes or removes the store's persistent disk budget.
    ///
    /// The limit is an inclusive bound on charged logical bytes, described
    /// by [`DiskBudgetStatus`](crate::DiskBudgetStatus). It is stored in the
    /// store directory and enforced by every later checkpoint-store open.
    /// Ordinary `Store` opens and managed rebuilds refuse a budgeted
    /// directory. Installing requires automatic checkpoints to be disabled
    /// and no open snapshots, so no unaccounted retired generation can be
    /// held. A limit below the current charge returns `WouldBlock` unchanged.
    /// Removing requires no outstanding maintenance or pinned generations.
    pub fn set_disk_budget(&self, limit: Option<u64>) -> io::Result<()> {
        Access::check_not_active(self.id())?;
        let _maintenance = match self.maintenance.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, MaintenanceBusy));
            }
            Err(TryLockError::Poisoned(_)) => return Err(uncertain()),
        };
        let state = self.state.write().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        let current = &state.store;
        let _writer = current.writers.write().map_err(|_| uncertain())?;
        let existing = current.disk_budget()?;
        match (existing, limit) {
            (Some(budget), Some(limit)) => budget.set_limit(limit),
            (Some(budget), None) => {
                budget.remove()?;
                *current.disk_budget.write().map_err(|_| uncertain())? = None;
                Ok(())
            }
            (None, None) => Ok(()),
            (None, Some(limit)) => {
                let (selected, automatic) = {
                    let view = current.read()?;
                    (
                        view.selection
                            .as_ref()
                            .map(|selection| selection.checkpoint.clone()),
                        view.automatic,
                    )
                };
                if automatic {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "disable automatic checkpoints before installing a disk budget",
                    ));
                }
                if current.snapshot_budget_status()?.active != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "close snapshots before installing a disk budget",
                    ));
                }
                let record = super::disk_budget::Record {
                    limit,
                    pending: super::disk_budget::Pending::Idle,
                };
                let open = |record| {
                    super::disk_budget::DiskBudget::new(
                        &self.path,
                        record,
                        current.log.len()?,
                        current.log.id(),
                        selected.as_deref(),
                    )
                };
                // The record file itself will add one more record length.
                let projected = open(record.clone())?
                    .status()?
                    .charged
                    .checked_add(super::disk_budget::RECORD_BYTES as u64)
                    .ok_or_else(|| io::Error::other("disk budget charge overflow"))?;
                if projected > limit {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "disk budget limit is below the charged bytes",
                    ));
                }
                super::disk_budget::write(&self.path, &record)?;
                *current.disk_budget.write().map_err(|_| uncertain())? =
                    Some(Arc::new(open(record)?));
                Ok(())
            }
        }
    }

    /// Reports usage under the persistent disk budget, if one is installed.
    pub fn disk_budget_status(&self) -> io::Result<Option<crate::DiskBudgetStatus>> {
        self.with_store(CheckpointStore::disk_budget_status)
    }

    /// Clears outstanding maintenance recorded by the disk budget once
    /// nothing it could have allocated remains, and returns the new status.
    /// For an interrupted compaction, remove the unselected candidate at the
    /// recorded previous path and run [`crate::cleanup_abandoned_staging`]
    /// on the parent first; for a retirement, finish it with
    /// [`Self::cleanup_retired_staging_detailed`] or remove the named copy.
    /// While anything remains, the charge is kept.
    pub fn reconcile_disk_budget(&self) -> io::Result<Option<crate::DiskBudgetStatus>> {
        Access::check_not_active(self.id())?;
        let _maintenance = match self.maintenance.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, MaintenanceBusy));
            }
            Err(TryLockError::Poisoned(_)) => return Err(uncertain()),
        };
        let state = self.state.read().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        let Some(budget) = state.store.disk_budget()? else {
            return Ok(None);
        };
        budget.reconcile()?;
        budget.status().map(Some)
    }

    /// Releases idle expired views, including views of retired generations.
    pub fn reap_expired_snapshots(&self) -> io::Result<usize> {
        self.with_store(CheckpointStore::reap_expired_snapshots)
    }

    /// Creates a verified compacted sibling while callbacks continue, catches
    /// up acknowledged writes, then pauses callbacks for the final suffix,
    /// full comparison, managed selection and an atomic directory exchange on
    /// supported Linux filesystems or macOS APFS. The previous complete
    /// generation remains at `previous`.
    ///
    /// The candidate is published at `previous` after the initial copy, so
    /// an error before the exchange can leave an unselected, possibly
    /// incomplete candidate there; the source stays active. A publication or
    /// sync error after the exchange poisons this handle; drop it and reopen
    /// both paths to reconcile them. The directory must be trusted, contain
    /// only checkpoint-store files and be used by cooperating clients. This
    /// does not bound total memory, disk use or pause time and does not
    /// establish physical power-loss behavior.
    pub fn compact_and_replace(
        &self,
        previous: impl AsRef<Path>,
        options: &OnlineCheckpointCompactionOptions,
    ) -> io::Result<OnlineCompactionReport> {
        self.compact_internal(
            previous.as_ref(),
            options,
            (|_| Ok(()), crate::directory::sync_existing),
            None::<fn(RetirePoint) -> io::Result<()>>,
            None,
        )
        .map(|(report, _)| report)
    }

    /// Like [`Self::compact_and_replace`], classifying candidate-publication
    /// and exchange failures.
    pub fn compact_and_replace_detailed(
        &self,
        previous: impl AsRef<Path>,
        options: &OnlineCheckpointCompactionOptions,
    ) -> Result<OnlineCompactionReport, crate::OnlineHandoffFailure> {
        crate::publication_outcome::track_online_handoff(|tracker| {
            self.compact_internal(
                previous.as_ref(),
                options,
                (|_| Ok(()), crate::directory::sync_existing),
                None::<fn(RetirePoint) -> io::Result<()>>,
                Some(tracker),
            )
            .map(|(report, _)| report)
        })
    }

    /// Performs online compaction, then removes the previous generation after
    /// the exchange has been synchronized. This permanently discards its
    /// history. Mutations of the replacement wait until removal finishes.
    ///
    /// Owned snapshots of the previous generation keep reading it: removal
    /// unlinks its names, and the space is released when the last such view
    /// is dropped or cancelled. A reclamation error means the handoff
    /// succeeded and the handle stays usable; the old copy may be partial or
    /// retained under an owned staging name for
    /// [`Self::cleanup_retired_staging_detailed`].
    pub fn compact_and_reclaim(
        &self,
        previous: impl AsRef<Path>,
        options: &OnlineCheckpointCompactionOptions,
    ) -> Result<OnlineCompactionReport, OnlineReclaimError> {
        let (report, reclamation) = self
            .compact_internal(
                previous.as_ref(),
                options,
                (|_| Ok(()), crate::directory::sync_existing),
                Some(|_| Ok(())),
                None,
            )
            .map_err(OnlineReclaimError::Compaction)?;
        reclaim_result(report, reclamation, |report, error| {
            OnlineReclaimError::Reclamation { report, error }
        })
    }

    /// Like [`Self::compact_and_reclaim`], classifying candidate-publication
    /// and exchange failures as well as a subsequent reclamation failure.
    pub fn compact_and_reclaim_detailed(
        &self,
        previous: impl AsRef<Path>,
        options: &OnlineCheckpointCompactionOptions,
    ) -> Result<OnlineCompactionReport, OnlineReclaimFailure> {
        self.compact_reclaim_detailed_with_hooks(
            previous.as_ref(),
            options,
            (|_| Ok(()), crate::directory::sync_existing),
            |_| Ok(()),
        )
    }

    fn compact_reclaim_detailed_with_hooks(
        &self,
        previous: &Path,
        options: &OnlineCheckpointCompactionOptions,
        hooks: (
            impl FnMut(Point) -> io::Result<()>,
            impl FnOnce(&Path) -> io::Result<()>,
        ),
        retire_hook: impl FnMut(RetirePoint) -> io::Result<()>,
    ) -> Result<OnlineCompactionReport, OnlineReclaimFailure> {
        let (report, reclamation) = crate::publication_outcome::track_online_handoff(|tracker| {
            self.compact_internal(previous, options, hooks, Some(retire_hook), Some(tracker))
        })
        .map_err(OnlineReclaimFailure::Compaction)?;
        reclaim_result(report, reclamation, |report, error| {
            OnlineReclaimFailure::Reclamation { report, error }
        })
    }

    // A worker's compaction: the deferral conditions are decided under the
    // same maintenance lease as the compaction itself.
    fn compact_reclaim_scheduled(
        &self,
        previous: &Path,
        options: &OnlineCheckpointCompactionOptions,
        hooks: (
            impl FnMut(Point) -> io::Result<()>,
            impl FnOnce(&Path) -> io::Result<()>,
        ),
    ) -> Result<OnlineCompactionReport, OnlineReclaimFailure> {
        let (report, reclamation) = crate::publication_outcome::track_online_handoff(|tracker| {
            self.compact_internal_with(
                previous,
                options,
                hooks,
                Some(|_: RetirePoint| Ok(())),
                Some(tracker),
                true,
            )
        })
        .map_err(OnlineReclaimFailure::Compaction)?;
        reclaim_result(report, reclamation, |report, error| {
            OnlineReclaimFailure::Reclamation { report, error }
        })
    }

    /// Removes marked retired stages tied to the current generation's log
    /// identity, finishing an interrupted or failed reclamation. While
    /// excluding mutations it first verifies the current generation against
    /// its checkpoint and complete log. Stages that still contain an
    /// automatic checkpoint directory, unknown entries or a locked store are
    /// skipped and preserved. `NotStarted` means no removal began.
    pub fn cleanup_retired_staging_detailed(
        &self,
    ) -> Result<crate::StagingCleanup, crate::StagingCleanupFailure> {
        self.cleanup_retired_staging_using(None)
    }

    /// Like [`Self::cleanup_retired_staging_detailed`], with an inclusive
    /// ceiling on the logical bytes its verification reads: the open
    /// generation's pinned checkpoint file plus its complete log, the same
    /// scan as a clean bounded recovery open. It is admitted against that
    /// checkpoint descriptor after reading only its header, and against the
    /// log length under the writer gate, which is the verified boundary. Verification, and so the
    /// ceiling, applies only when a matching stage is about to be removed. A
    /// larger scan returns `NotStarted(InvalidInput)` before any removal and
    /// preserves every stage. Physical retries and other operations' reads
    /// are separate.
    pub fn cleanup_retired_staging_with_scan_limit(
        &self,
        max_scan_bytes: u64,
    ) -> Result<crate::StagingCleanup, crate::StagingCleanupFailure> {
        self.cleanup_retired_staging_using(Some(max_scan_bytes))
    }

    fn cleanup_retired_staging_using(
        &self,
        max_scan_bytes: Option<u64>,
    ) -> Result<crate::StagingCleanup, crate::StagingCleanupFailure> {
        use crate::StagingCleanupFailure::NotStarted;

        Access::check_not_active(self.id()).map_err(NotStarted)?;
        let _maintenance = match self.maintenance.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(NotStarted(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    MaintenanceBusy,
                )));
            }
            Err(TryLockError::Poisoned(_)) => return Err(NotStarted(uncertain())),
        };
        let state = self.state.read().map_err(|_| NotStarted(uncertain()))?;
        if state.failed {
            return Err(NotStarted(uncertain()));
        }
        let current = &state.store;
        let _writer = current
            .writers
            .write()
            .map_err(|_| NotStarted(uncertain()))?;
        let view = current.read().map_err(NotStarted)?;
        view.view
            .base
            .share_store_lock()
            .and_then(|lock| lock.verify_identity(&self.path))
            .map_err(NotStarted)?;
        super::compact_exchange::source_directory_is_owned(&self.path).map_err(NotStarted)?;
        let id = current
            .log
            .id()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "retired cleanup requires a witnessed store",
                )
            })
            .map_err(NotStarted)?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| NotStarted(io::Error::other("store has no parent directory")))?;
        let report = crate::staging::cleanup_retired_detailed(
            parent,
            &id,
            || match max_scan_bytes {
                // Admitted from the pinned checkpoint descriptor and the
                // log length under the writer gate, before any entry is read.
                Some(max) => current.verify_with_total_scan_limit(&view, max),
                None => current.verify_for_checkpoint(&view).map(|_| ()),
            },
            fs::File::sync_all,
            fs::File::sync_all,
        )?;
        // Clear a pending retirement only if nothing of it remains.
        if let Some(budget) = current.disk_budget().ok().flatten() {
            let _ = budget.reconcile();
        }
        Ok(report)
    }

    fn compact_internal(
        &self,
        previous: &Path,
        options: &OnlineCheckpointCompactionOptions,
        hooks: (
            impl FnMut(Point) -> io::Result<()>,
            impl FnOnce(&Path) -> io::Result<()>,
        ),
        retire_hook: Option<impl FnMut(RetirePoint) -> io::Result<()>>,
        tracker: Option<&crate::publication_outcome::HandoffTracker>,
    ) -> io::Result<(OnlineCompactionReport, Option<io::Result<()>>)> {
        self.compact_internal_with(previous, options, hooks, retire_hook, tracker, false)
    }

    // `scheduled` adds the worker's deferral conditions. They are evaluated
    // while holding the maintenance lease, so no manual maintenance can run
    // between the decision and the compaction it admits.
    fn compact_internal_with(
        &self,
        previous: &Path,
        options: &OnlineCheckpointCompactionOptions,
        (mut hook, sync_parent): (
            impl FnMut(Point) -> io::Result<()>,
            impl FnOnce(&Path) -> io::Result<()>,
        ),
        retire_hook: Option<impl FnMut(RetirePoint) -> io::Result<()>>,
        tracker: Option<&crate::publication_outcome::HandoffTracker>,
        scheduled: bool,
    ) -> io::Result<(OnlineCompactionReport, Option<io::Result<()>>)> {
        Access::check_not_active(self.id())?;
        let _maintenance = match self.maintenance.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, MaintenanceBusy));
            }
            Err(TryLockError::Poisoned(_)) => return Err(uncertain()),
        };
        if scheduled && self.scheduled_deferral_locked()? {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, ScheduledDeferral));
        }
        options.validate()?;
        let (source, previous, parent) =
            crate::store::compact_exchange::paths(&self.path, previous)?;
        let current = {
            let state = self.state.read().map_err(|_| uncertain())?;
            if state.failed {
                return Err(uncertain());
            }
            Arc::clone(&state.store)
        };
        let original_lock = current.read()?.view.base.share_store_lock()?;
        original_lock.verify_identity(&source)?;
        super::compact_exchange::source_directory_is_owned(&source)?;
        let budget = current.disk_budget()?;
        if budget.is_some() && retire_hook.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "a disk-budgeted store must reclaim its previous generation",
            ));
        }

        // Capture a consistent view and its exact committed log boundary.
        let (snapshot, initial) = {
            let _writer = current.writers.write().map_err(|_| uncertain())?;
            let snapshot = current.snapshot()?;
            (snapshot, boundary_locked(&current)?)
        };
        // Verify the captured view against the immutable log prefix, then
        // copy it. Commits continue; the witness is checked at the barrier.
        let (count, mut digest) = snapshot.with_captured(|view, key_bytes| {
            current
                .verify_view_at(
                    view,
                    key_bytes,
                    initial,
                    false,
                    crate::checkpoint::RecoveryScanLimits::default(),
                )
                .map(|(count, _, digest)| (count, digest))
        })?;
        let mut copy = crate::store::CheckpointCopyOptions {
            checkpoint_limits: Some(current.checkpoint_view_limits),
            source_value_read_bytes: options.copy_limits.max_source_log_read_bytes,
            target_log_bytes: options.copy_limits.max_target_log_bytes,
        };
        // Reserve the complete replacement durably before allocating any of
        // it, then cap each part at its reserved size.
        let mut reservation = ReservationGuard(None);
        let mut reserved = None;
        if let Some(budget) = &budget {
            let (log_bytes, checkpoint_bytes) =
                snapshot.with_captured(|view, _| replacement_bytes(view))?;
            let total = [
                options.max_catchup_bytes,
                checkpoint_bytes,
                super::disk_budget::METADATA_ALLOWANCE,
            ]
            .into_iter()
            .try_fold(log_bytes, u64::checked_add)
            .ok_or_else(|| io::Error::other("replacement reservation overflow"))?;
            budget.reserve_compaction(&previous, total)?;
            reservation.0 = Some(Arc::clone(budget));
            hook(Point::AfterReservation)?;
            reserved = Some(total);
            copy.target_log_bytes = Some(
                copy.target_log_bytes
                    .map_or(log_bytes, |limit| limit.min(log_bytes)),
            );
            let mut limits = current.checkpoint_view_limits;
            limits.max_file_bytes = limits.max_file_bytes.min(checkpoint_bytes);
            copy.checkpoint_limits = Some(limits);
        }
        if copy.source_value_read_bytes.is_some() {
            copy.charge_source_reads(initial)?;
        }
        copy.charge_target_log(crate::witness::LOG_HEADER_SIZE as u64)?;
        let checkpoint_digest = snapshot.with_captured(|view, _| {
            crate::store::compact_checkpoint_concurrent(
                &current.log,
                view,
                count,
                (&previous, copy),
                || {
                    if let Some(tracker) = tracker {
                        tracker.begin_candidate();
                    }
                    Ok(())
                },
            )
        })?;
        let mut candidate = CheckpointStore::open_using(
            &previous,
            &previous.join(crate::COMPACTED_CHECKPOINT_FILE),
            &checkpoint_digest,
            current.limits(),
            current.suffix_operation_limit(),
            crate::log::Log::open_checkpoint_writer,
        )?;
        if budget.is_some() {
            candidate.max_log_bytes = Some(
                candidate
                    .log
                    .len()?
                    .checked_add(options.max_catchup_bytes)
                    .ok_or_else(|| io::Error::other("replacement log limit overflow"))?,
            );
        }
        hook(Point::InitialCopy)?;

        let mut offset = initial;
        for _ in 0..options.catchup_rounds {
            let end = boundary(&current)?;
            if end == offset {
                break;
            }
            replay(&current, &candidate, offset, end, &mut digest)?;
            offset = end;
        }
        let concurrent_replay_bytes = offset - initial;
        hook(Point::BeforeFinalBarrier)?;

        // Exclude callbacks and captures, then every source append.
        let mut state = self.state.write().map_err(|_| uncertain())?;
        if state.failed || !Arc::ptr_eq(&state.store, &current) {
            return Err(uncertain());
        }
        let _writer = current.writers.write().map_err(|_| uncertain())?;
        let final_boundary = boundary_locked(&current)?;
        let final_replay_bytes = final_boundary
            .checked_sub(offset)
            .ok_or_else(|| io::Error::other("source log boundary moved backward"))?;
        if final_replay_bytes > options.max_final_delta_bytes {
            return Err(io::Error::other(
                "final compaction suffix exceeds configured limit",
            ));
        }
        replay(&current, &candidate, offset, final_boundary, &mut digest)?;
        let (original_selection, enabled, protect_pinned_slots, automatic_scan_limit) = {
            let view = current.read()?;
            (
                view.selection.clone(),
                view.automatic,
                view.protect_pinned_automatic_slots,
                view.automatic_checkpoint_log_scan_limit(),
            )
        };
        if original_selection.is_none() || selection::load(&source)? != original_selection {
            return Err(io::Error::other("managed checkpoint selection changed"));
        }
        let configured = automatic::open_existing(&source)?;
        if (configured == Some(true)) != enabled {
            return Err(io::Error::other(
                "automatic checkpoint configuration changed",
            ));
        }
        verify_candidate(&current, &candidate)?;
        candidate.log.verify_witness()?;
        // Confirm that no replayed source byte changed after it was read.
        crate::checkpoint::VerifiedLog::from_replay(final_boundary, digest)
            .verify_current_bytes(&current.log)?;
        current.log.verify_witness()?;

        candidate
            .set_transaction_staging_limit(current.transaction_budget_status()?.limit_bytes)?;
        candidate
            .set_active_transaction_limit(current.transaction_budget_status()?.active_limit)?;
        candidate.set_active_direct_write_limit(current.direct_write_status().limit)?;
        candidate.set_automatic_checkpoint_slot_protection(protect_pinned_slots)?;
        candidate.set_automatic_checkpoint_log_scan_limit(automatic_scan_limit)?;
        // Owned views may still reserve capacity on the retired generation.
        // Keep one aggregate snapshot budget and read admission counter.
        candidate.snapshot_budget = Arc::clone(&current.snapshot_budget);
        candidate.snapshot_registry = Arc::clone(&current.snapshot_registry);
        candidate.direct_reads = current.direct_reads.clone();
        // The selection names the final path. Nobody may use this private
        // candidate until its directory is exchanged.
        let next = selection::Selection::new(
            source.join(crate::COMPACTED_CHECKPOINT_FILE),
            checkpoint_digest,
        )?;
        {
            let hook = RefCell::new(&mut hook);
            selection::publish(
                &previous,
                &next,
                || (hook.borrow_mut())(Point::SelectionBeforeRename),
                |path| {
                    (hook.borrow_mut())(Point::SelectionBeforeSync)?;
                    selection::sync_ancestors(path)
                },
            )?;
        }
        candidate.write()?.selection = Some(next);
        if enabled {
            candidate.enable_automatic_checkpoints()?;
        }
        // Carry the budget into the replacement and prove that the private
        // replacement stayed within its reservation before exchanging.
        let mut exchange_accounting = None;
        if let (Some(budget), Some(reserved)) = (&budget, reserved) {
            super::disk_budget::write(&previous, &budget.replacement_record(&previous)?)?;
            let measured = crate::staging::tree_bytes(&previous)?;
            if measured > reserved {
                return Err(io::Error::other(
                    "replacement exceeded its disk-budget reservation",
                ));
            }
            candidate.max_log_bytes = current.max_log_bytes;
            let live_log = candidate.log.len()?;
            let live_other = super::disk_budget::live_other_bytes(&previous, live_log, None)?;
            // Writers are excluded, so this is the retired generation's size.
            let retired = crate::staging::tree_bytes(&source)?;
            *candidate.disk_budget.get_mut().map_err(|_| uncertain())? = Some(Arc::clone(budget));
            exchange_accounting = Some((live_other, live_log, retired));
        }
        let candidate_lock = candidate.read()?.view.base.share_store_lock()?;
        original_lock.verify_identity(&source)?;
        candidate_lock.verify_identity(&previous)?;
        hook(Point::BeforeExchange)?;

        state.failed = true;
        if let Some(tracker) = tracker {
            tracker.begin_exchange();
        }
        crate::store::compact_exchange::exchange(
            &source,
            &previous,
            &original_lock,
            &candidate_lock,
        )
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("online checkpoint exchange may have occurred; reopen required: {error}"),
            )
        })?;
        hook(Point::AfterExchange).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("online checkpoint exchange may have occurred; reopen required: {error}"),
            )
        })?;
        sync_parent(&parent).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "online checkpoint exchange may have occurred; directory synchronization failed: {error}"
                ),
            )
        })?;
        drop(candidate_lock);
        candidate.path = source.clone();
        if let (Some(budget), Some((live_other, live_log, retired))) =
            (&budget, exchange_accounting)
        {
            budget.complete_exchange(
                &previous,
                (live_other, live_log, candidate.log.id()),
                (current.generation_token(), retired),
            )?;
            reservation.0 = None;
        }
        let candidate = Arc::new(candidate);
        // Do not let a new mutation make the replacement uncertain while
        // its last complete predecessor is being removed. Reads continue.
        let _reclamation_writer = if retire_hook.is_some() {
            Some(
                candidate
                    .writers
                    .write()
                    .map_err(|_| io::Error::other("replacement write barrier poisoned"))?,
            )
        } else {
            None
        };
        state.store = Arc::clone(&candidate);
        state.failed = false;
        drop(state);
        {
            let mut retired = self
                .retired
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            retired.retain(|token| token.strong_count() != 0);
            retired.push(current.generation_token());
        }
        drop(_writer);
        drop(snapshot);
        drop(original_lock);
        let report = OnlineCompactionReport {
            initial_log_bytes: initial,
            final_log_bytes: final_boundary,
            concurrent_replay_bytes,
            final_replay_bytes,
            compacted_log_bytes: candidate.log.len()?,
        };
        let reclamation = retire_hook.map(|hook| {
            retire(&source, &previous, &parent, &candidate, &current, hook).and_then(|()| {
                // Nothing of the previous generation remains named or staged.
                match &budget {
                    Some(budget) if !budget.reconcile()? => Err(io::Error::other(
                        "previous generation is still charged after retirement",
                    )),
                    _ => Ok(()),
                }
            })
        });
        drop(reservation);
        Ok((report, reclamation))
    }
}

// Exact encoded sizes of a replacement holding the captured live entries.
fn replacement_bytes(view: &super::RecoveredCheckpointView) -> io::Result<(u64, u64)> {
    let overflow = || io::Error::other("replacement size overflow");
    let mut log = crate::witness::LOG_HEADER_SIZE as u64;
    let (mut count, mut key_bytes) = (0_u64, 0_u64);
    view.visit_entries(None, None, |key, location| {
        let record = (crate::record::HEADER_SIZE
            + key.len()
            + location.length
            + crate::record::CHECKSUM_SIZE) as u64;
        log = log.checked_add(record).ok_or_else(overflow)?;
        key_bytes = key_bytes
            .checked_add(key.len() as u64)
            .ok_or_else(overflow)?;
        count = count.checked_add(1).ok_or_else(overflow)?;
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
    let checkpoint = crate::checkpoint::file_bytes(count, key_bytes).ok_or_else(overflow)?;
    Ok((log, checkpoint))
}

// Releases an interrupted compaction's reservation only once nothing it
// could have allocated remains; otherwise the charge persists on disk.
struct ReservationGuard(Option<Arc<super::disk_budget::DiskBudget>>);

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Some(budget) = self.0.take() {
            let _ = budget.reconcile();
        }
    }
}

fn reclaim_result<E>(
    report: OnlineCompactionReport,
    reclamation: Option<io::Result<()>>,
    failure: impl FnOnce(OnlineCompactionReport, io::Error) -> E,
) -> Result<OnlineCompactionReport, E> {
    match reclamation {
        Some(Ok(())) => Ok(report),
        Some(Err(error)) => Err(failure(report, error)),
        None => Err(failure(
            report,
            io::Error::other("retired-store reclamation was not performed"),
        )),
    }
}

fn remove_optional(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

// Removes the previous generation only after the exchange and parent sync,
// so the replacement is the complete recoverable copy. The old directory is
// first hidden under an owned private name, as for ordinary reclamation.
fn retire(
    source: &Path,
    previous: &Path,
    parent: &Path,
    replacement: &CheckpointStore,
    retired: &CheckpointStore,
    mut hook: impl FnMut(RetirePoint) -> io::Result<()>,
) -> io::Result<()> {
    let replacement_lock = replacement.read()?.view.base.share_store_lock()?;
    let retired_lock = retired.read()?.view.base.share_store_lock()?;
    replacement_lock.verify_identity(source)?;
    retired_lock.verify_identity(previous)?;
    super::compact_exchange::source_directory_is_owned(previous)?;
    let replacement_id = replacement.log.id().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "retired cleanup requires a witnessed replacement",
        )
    })?;
    let mut stage = crate::staging::Stage::retired_directory(parent, &replacement_id)?;
    let hidden = stage.path().join("retired");
    let mut moved = false;
    let result: io::Result<()> = (|| {
        hook(RetirePoint::BeforeMove)?;
        replacement_lock.verify_identity(source)?;
        retired_lock.verify_identity(previous)?;
        super::compact_exchange::source_directory_is_owned(previous)?;
        crate::backup::publish_noreplace(previous, &hidden)?;
        moved = true;
        stage.preserve_payload_on_drop();
        hook(RetirePoint::AfterMove)?;
        retired_lock.verify_identity(&hidden)?;
        super::compact_exchange::source_directory_is_owned(&hidden)?;
        fs::File::open(stage.path())?.sync_all()?;
        fs::File::open(parent)?.sync_all()?;
        hook(RetirePoint::AfterMoveParentSync)?;
        // Checkpoint metadata first, leaving an ordinary retired layout that
        // scoped cleanup can finish if a later step is interrupted.
        automatic::remove_retired_root(&hidden)?;
        hook(RetirePoint::AfterAutomaticRoot)?;
        remove_optional(&hidden.join("checkpoint.selection"))?;
        remove_optional(&hidden.join(crate::COMPACTED_CHECKPOINT_FILE))?;
        remove_optional(&hidden.join(super::disk_budget::BUDGET_FILE))?;
        remove_optional(&hidden.join(super::disk_budget::BUDGET_TEMP))?;
        fs::File::open(&hidden)?.sync_all()?;
        hook(RetirePoint::AfterCheckpointFiles)?;
        fs::remove_file(hidden.join("records.log"))?;
        remove_optional(&hidden.join("commit.state"))?;
        // A retry needs the lock name whenever data files can reappear.
        fs::File::open(&hidden)?.sync_all()?;
        hook(RetirePoint::AfterRecords)?;
        fs::remove_file(hidden.join("store.lock"))?;
        fs::File::open(&hidden)?.sync_all()?;
        hook(RetirePoint::AfterLock)?;
        fs::remove_dir(&hidden)?;
        fs::File::open(stage.path())?.sync_all()?;
        fs::remove_dir(stage.path())?;
        hook(RetirePoint::AfterRetiredRemoval)?;
        crate::directory::sync_existing(parent)?;
        hook(RetirePoint::AfterParentSync)?;
        Ok(())
    })();
    if let Err(error) = result {
        if moved {
            let retained_path = stage.path().to_path_buf();
            let preserve = stage.retain_payload();
            let extra = preserve.err().map_or(String::new(), |failure| {
                format!("; retaining its marker also failed: {failure}")
            });
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "retired checkpoint-store cleanup incomplete in {}: {error}{extra}",
                    retained_path.display()
                ),
            ));
        }
        return Err(io::Error::new(
            error.kind(),
            format!("retired store remains at {}: {error}", previous.display()),
        ));
    }
    stage.finish_retained_cleanup();
    Ok(())
}

mod worker;

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
