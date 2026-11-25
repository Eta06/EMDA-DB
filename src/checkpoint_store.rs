//! Experimental writable checkpoint index with a bounded log-suffix overlay.
use crate::checkpoint_view::{CheckpointView, CheckpointViewLimits};
use crate::controlled_lock::{
    ControlledRwLock, ReadGuard as ControlledReadGuard, WriteGuard as ControlledWriteGuard,
};
use crate::log::{GenerationId, Log, ValueLocation};
use crate::publication_outcome::{RecoveryOpenFailure, TailAdoptionTracker, track_recovery_open};
#[cfg(test)]
use crate::record::batch::encode_payload;
use crate::record::batch::{BatchLayout, BatchOperation};
use crate::record::{self, CHECKSUM_SIZE, HEADER_SIZE, Operation};
use crate::recovered_view::{
    MAX_RECOVERED_VIEW_TAIL_BYTES, MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS,
    RecoveredCheckpointView,
};
use crate::{Entry, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

pub(crate) mod automatic;
pub(crate) mod disk_budget;
pub use disk_budget::{DiskBudgetPending, DiskBudgetStatus};
mod commit;
use commit::Mutation;
mod compact_exchange;
mod online;
pub use online::{
    OnlineCheckpointCompactionOptions, OnlineCheckpointSnapshot, OnlineCheckpointStore,
};
mod read_set;
mod rebuild;
mod rebuild_spill;
pub use rebuild::{
    BoundedFullRebuildLimits, BoundedRebuildReadLimits, BoundedSuffixAdmission,
    BoundedSuffixRebuildLimits,
};
mod selection;
mod snapshot;
mod transaction;
mod worker;
pub use snapshot::{
    CheckpointSnapshot, CheckpointSnapshotBudgetStatus, MAX_CHECKPOINT_ACTIVE_SNAPSHOTS,
    MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS, MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES,
    MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
};
pub use transaction::{CheckpointTransaction, CheckpointTransactionMode};
pub use worker::{CheckpointWorker, CheckpointWorkerStatus};

struct State {
    view: RecoveredCheckpointView,
    log_generation: Option<GenerationId>,
    overlay_key_bytes: usize,
    generation: u64,
    failed: bool,
    selection: Option<selection::Selection>,
    automatic: bool,
    automatic_checkpoint: Option<Box<AutomaticCheckpointState>>,
    protect_pinned_automatic_slots: bool,
}

#[derive(Default)]
struct AutomaticCheckpointState {
    slot_pins: [Option<crate::checkpoint_view::CheckpointPin>; 2],
    log_scan_limit: Option<u64>,
}

impl State {
    fn automatic_checkpoint_log_scan_limit(&self) -> Option<u64> {
        self.automatic_checkpoint
            .as_ref()
            .and_then(|options| options.log_scan_limit)
    }
}

// Dropping the state guard can let readers use the published view. The
// writer gate continues to exclude every append and checkpoint maintenance.
struct StateWrite<'a> {
    state: ControlledWriteGuard<'a, State>,
    _writer: ControlledWriteGuard<'a, ()>,
}

impl std::ops::Deref for StateWrite<'_> {
    type Target = State;

    fn deref(&self) -> &State {
        &self.state
    }
}

impl std::ops::DerefMut for StateWrite<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.state
    }
}

enum OpenSource<'a> {
    Explicit(&'a Path, &'a [u8; 32]),
    Managed,
}

/// Per-open logical admission limits for a writable checkpoint-backed store.
/// These do not bound allocations outside the named structures, elapsed time,
/// backups, other files, or total process/project resource consumption.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointStoreLimits {
    /// Maximum bytes in the replayed suffix after the checkpoint.
    pub max_tail_bytes: u64,
    /// Maximum distinct keys retained in the suffix overlay.
    pub max_tail_keys: usize,
    /// Maximum combined bytes of suffix overlay keys.
    pub max_tail_key_bytes: usize,
    /// Optional maximum whole log length, including headers and old records.
    pub max_log_bytes: Option<u64>,
    /// Maximum checkpoint file length and retained block-directory entries.
    pub checkpoint_view: CheckpointViewLimits,
}

impl Default for CheckpointStoreLimits {
    fn default() -> Self {
        Self {
            max_tail_bytes: MAX_RECOVERED_VIEW_TAIL_BYTES,
            max_tail_keys: MAX_RECOVERED_VIEW_TAIL_KEYS,
            max_tail_key_bytes: MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            max_log_bytes: None,
            checkpoint_view: CheckpointViewLimits::default(),
        }
    }
}

type OpenLimits = CheckpointStoreLimits;

/// Additional admission limits for writable checkpoint recovery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointRecoveryLimits {
    /// Maximum suffix operations, including batch children. This also applies
    /// to later suffix appends until rollover clears the suffix.
    pub max_tail_operations: Option<u64>,
    /// Maximum logical log bytes scanned during open. Counts prefix
    /// verification and suffix replay, plus a second suffix pass before adoption
    /// when there is a pending tail. Header identity checks outside these
    /// scans, witness and checkpoint reads, and physical retries are separate.
    pub max_log_scan_bytes: Option<u64>,
}

/// Optional logical limits for a checkpoint-backed compacted copy.
#[derive(Clone, Copy, Debug, Default)]
pub struct CheckpointCompactionLimits {
    /// One complete source log scan and the live value bytes read for copying.
    /// Checkpoint reads, witness and identity probes, target verification and
    /// physical retries have separate costs.
    pub max_source_log_read_bytes: Option<u64>,
    /// Complete encoded replacement log length, including its header.
    /// Checkpoint, witness, and staging metadata bytes are separate.
    pub max_target_log_bytes: Option<u64>,
}

/// An experimental writable store backed by a verified on-disk checkpoint.
///
/// Point operations and blind batches serialize through one write lock. A
/// bounded in-memory overlay records every distinct suffix key, including
/// tombstones; opening still verifies the complete covered log prefix. Limits
/// are not total process-memory quotas. This type excludes other cooperating
/// stores/views and has no persistent snapshots or automatic log compaction.
/// Explicit rollover and opt-in on-demand automatic checkpoints can clear the
/// suffix overlay without releasing ownership. Managed selection and automatic
/// checkpoint metadata must be trusted. A failed append/sync has an unknown
/// outcome and poisons this instance until reopened.
pub struct CheckpointStore {
    // Drop the writable log before the view, which owns the exclusive lock.
    log: Log,
    state: ControlledRwLock<State>,
    writers: ControlledRwLock<()>,
    append_failed: AtomicBool,
    pub(crate) direct_writes: crate::direct_write::DirectWrites,
    pub(crate) direct_reads: crate::direct_read::DirectReads,
    transaction_budget: Mutex<crate::transaction_budget::TransactionBudget>,
    transaction_queue: crate::store::TransactionQueue,
    // Shared with later generations of an online checkpoint store, so owned
    // snapshots of a retired generation keep counting against one budget.
    snapshot_budget: Arc<Mutex<snapshot::SnapshotBudget>>,
    snapshot_registry: Arc<Mutex<snapshot::SnapshotRegistry>>,
    pub(crate) snapshot_reaper_active: AtomicBool,
    checkpoint_worker: AtomicBool,
    path: PathBuf,
    max_tail_bytes: u64,
    max_tail_operations: Option<u64>,
    max_tail_keys: usize,
    max_tail_key_bytes: usize,
    max_log_bytes: Option<u64>,
    checkpoint_view_limits: CheckpointViewLimits,
    disk_budget: RwLock<Option<Arc<disk_budget::DiskBudget>>>,
    // Must remain the last field: its release marks every descriptor closed.
    generation_token: Arc<disk_budget::GenerationToken>,
}

impl CheckpointStore {
    /// Opens an existing witnessed store and checkpoint with the hard caps.
    /// A managed selection must instead be opened with `open_managed`.
    pub fn open(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
    ) -> io::Result<Self> {
        Self::open_with_limits(
            path,
            checkpoint,
            trusted_digest,
            MAX_RECOVERED_VIEW_TAIL_BYTES,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
        )
    }

    /// Opens with limits no greater than the recovered view's hard limits.
    /// An oversized suffix is rejected after verifying the checkpoint file
    /// but before hashing its covered log. This leaves log verification incomplete.
    /// A complete unacknowledged tail is validated and synchronized before
    /// it is adopted; incomplete or invalid tails fail without truncation.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
    ) -> io::Result<Self> {
        Self::open_with_resource_limits(
            path,
            checkpoint,
            trusted_digest,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            None,
        )
    }

    /// Opens with bounded suffix limits and an optional whole-log byte cap.
    /// The cap is reapplied on each open and covers log headers, old records
    /// and tombstones, not other files or total project storage. Oversized
    /// logs are rejected before replay or unacknowledged-tail adoption.
    pub fn open_with_resource_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
        max_log_bytes: Option<u64>,
    ) -> io::Result<Self> {
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            OpenLimits {
                max_tail_bytes,
                max_tail_keys,
                max_tail_key_bytes,
                max_log_bytes,
                ..Default::default()
            },
            None,
            Log::open_checkpoint_writer,
        )
    }

    /// Opens an explicit checkpoint with configurable per-open logical limits.
    /// The limits also apply to new views during rollover and replacement;
    /// they must be supplied again on reopen. A published checkpoint that
    /// exceeds them cannot be adopted and may remain at its destination.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: CheckpointStoreLimits,
    ) -> io::Result<Self> {
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            limits,
            None,
            Log::open_checkpoint_writer,
        )
    }

    /// Opens an explicit checkpoint with the selected limits and detailed
    /// tail adoption errors. The optional operation cap also applies to later
    /// suffix writes, as in [`Self::open_with_suffix_operation_limit`].
    pub fn open_with_options_detailed(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: CheckpointStoreLimits,
        max_tail_operations: Option<u64>,
    ) -> Result<Self, RecoveryOpenFailure> {
        Self::open_with_recovery_limits(
            path,
            checkpoint,
            trusted_digest,
            limits,
            CheckpointRecoveryLimits {
                max_tail_operations,
                ..Default::default()
            },
        )
    }

    /// Opens an explicit checkpoint with both store and recovery limits.
    /// The checkpoint header can reject an over-budget log scan before
    /// the index entries are verified. An admitted open authenticates the
    /// checkpoint and rechecks the budget before replay or tail adoption.
    /// Reapply the budget on each open. Reads after open do not use it.
    pub fn open_with_recovery_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: CheckpointStoreLimits,
        recovery: CheckpointRecoveryLimits,
    ) -> Result<Self, RecoveryOpenFailure> {
        Self::open_recovery_using(
            path.as_ref(),
            OpenSource::Explicit(checkpoint.as_ref(), trusted_digest),
            limits,
            (recovery, None),
        )
    }

    /// Applies an additional inclusive budget to the combined logical bytes
    /// of the checkpoint scan and the log scans in
    /// [`CheckpointRecoveryLimits::max_log_scan_bytes`]. A pending tail
    /// includes a second suffix pass. An over-budget open rejects before
    /// checkpoint entry validation or tail adoption. Metadata, framing
    /// probes, physical retries, and reads after open are separate.
    pub fn open_with_total_recovery_scan_limit(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: CheckpointStoreLimits,
        recovery: CheckpointRecoveryLimits,
        max_total_scan_bytes: u64,
    ) -> Result<Self, RecoveryOpenFailure> {
        Self::open_recovery_using(
            path.as_ref(),
            OpenSource::Explicit(checkpoint.as_ref(), trusted_digest),
            limits,
            (recovery, Some(max_total_scan_bytes)),
        )
    }

    /// Bounds suffix mutations on open and before each append, counting every
    /// batch child. A zero limit permits an empty suffix and rejects writes.
    /// Supply this per-open limit again after restart. Replay can reject an
    /// over-budget suffix before hashing the covered log prefix. Every
    /// successful open verifies the prefix. This limit does not cap all
    /// recovery I/O or memory.
    pub fn open_with_suffix_operation_limit(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: CheckpointStoreLimits,
        max_tail_operations: u64,
    ) -> io::Result<Self> {
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            limits,
            Some(max_tail_operations),
            Log::open_checkpoint_writer,
        )
    }

    /// Opens the checkpoint recorded by a prior managed rollover. The store
    /// directory and selection file must be trusted and protected against
    /// modification. A missing, corrupt or unknown selection fails closed.
    /// The selected checkpoint must remain accessible at its recorded path.
    pub fn open_managed(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_managed_with_resource_limits(
            path,
            MAX_RECOVERED_VIEW_TAIL_BYTES,
            MAX_RECOVERED_VIEW_TAIL_KEYS,
            MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
            None,
        )
    }

    /// Opens a managed selection with bounded suffix and optional log limits.
    pub fn open_managed_with_resource_limits(
        path: impl AsRef<Path>,
        max_tail_bytes: u64,
        max_tail_keys: usize,
        max_tail_key_bytes: usize,
        max_log_bytes: Option<u64>,
    ) -> io::Result<Self> {
        Self::open_source_using(
            path.as_ref(),
            OpenSource::Managed,
            OpenLimits {
                max_tail_bytes,
                max_tail_keys,
                max_tail_key_bytes,
                max_log_bytes,
                ..Default::default()
            },
            (CheckpointRecoveryLimits::default(), None),
            Log::open_checkpoint_writer,
            selection::synchronize,
            None,
        )
    }

    /// Opens a managed selection with configurable per-open logical limits.
    /// Selection, checkpoint and store directories must be trusted as for
    /// `open_managed`; limits are not persisted and must be reapplied.
    pub fn open_managed_with_options(
        path: impl AsRef<Path>,
        limits: CheckpointStoreLimits,
    ) -> io::Result<Self> {
        Self::open_source_using(
            path.as_ref(),
            OpenSource::Managed,
            limits,
            (CheckpointRecoveryLimits::default(), None),
            Log::open_checkpoint_writer,
            selection::synchronize,
            None,
        )
    }

    /// Opens the managed checkpoint with detailed tail adoption errors.
    /// Selection and configuration checks still precede witness adoption.
    /// The optional operation cap also applies to later suffix writes.
    pub fn open_managed_with_options_detailed(
        path: impl AsRef<Path>,
        limits: CheckpointStoreLimits,
        max_tail_operations: Option<u64>,
    ) -> Result<Self, RecoveryOpenFailure> {
        Self::open_managed_with_recovery_limits(
            path,
            limits,
            CheckpointRecoveryLimits {
                max_tail_operations,
                ..Default::default()
            },
        )
    }

    /// Opens a managed selection with the same budgets as
    /// [`Self::open_with_recovery_limits`]. The log scan budget is checked
    /// before stabilizing the selection or cleaning automatic stages.
    pub fn open_managed_with_recovery_limits(
        path: impl AsRef<Path>,
        limits: CheckpointStoreLimits,
        recovery: CheckpointRecoveryLimits,
    ) -> Result<Self, RecoveryOpenFailure> {
        Self::open_recovery_using(path.as_ref(), OpenSource::Managed, limits, (recovery, None))
    }

    /// Opens a managed selection with the combined read budget of
    /// [`Self::open_with_total_recovery_scan_limit`]. Admission precedes
    /// selection synchronization, abandoned-stage cleanup, and adoption.
    pub fn open_managed_with_total_recovery_scan_limit(
        path: impl AsRef<Path>,
        limits: CheckpointStoreLimits,
        recovery: CheckpointRecoveryLimits,
        max_total_scan_bytes: u64,
    ) -> Result<Self, RecoveryOpenFailure> {
        Self::open_recovery_using(
            path.as_ref(),
            OpenSource::Managed,
            limits,
            (recovery, Some(max_total_scan_bytes)),
        )
    }

    fn open_recovery_using(
        path: &Path,
        source: OpenSource<'_>,
        limits: CheckpointStoreLimits,
        recovery: (CheckpointRecoveryLimits, Option<u64>),
    ) -> Result<Self, RecoveryOpenFailure> {
        track_recovery_open(|tracker| {
            Self::open_source_using(
                path,
                source,
                limits,
                recovery,
                Log::open_checkpoint_writer,
                selection::synchronize,
                Some(tracker),
            )
        })
    }

    /// Opens the managed checkpoint with the same per-open operation ceiling
    /// as `open_with_suffix_operation_limit`. Automatic rollover may clear
    /// the suffix before an otherwise over-limit write.
    pub fn open_managed_with_suffix_operation_limit(
        path: impl AsRef<Path>,
        limits: CheckpointStoreLimits,
        max_tail_operations: u64,
    ) -> io::Result<Self> {
        Self::open_source_using(
            path.as_ref(),
            OpenSource::Managed,
            limits,
            (
                CheckpointRecoveryLimits {
                    max_tail_operations: Some(max_tail_operations),
                    ..Default::default()
                },
                None,
            ),
            Log::open_checkpoint_writer,
            selection::synchronize,
            None,
        )
    }

    /// Returns the separately supplied suffix mutation ceiling, if configured.
    pub fn suffix_operation_limit(&self) -> Option<u64> {
        self.max_tail_operations
    }

    /// Returns the byte and key limits for this open instance. Use
    /// `suffix_operation_limit` for the separately configured mutation limit.
    pub fn limits(&self) -> CheckpointStoreLimits {
        CheckpointStoreLimits {
            max_tail_bytes: self.max_tail_bytes,
            max_tail_keys: self.max_tail_keys,
            max_tail_key_bytes: self.max_tail_key_bytes,
            max_log_bytes: self.max_log_bytes,
            checkpoint_view: self.checkpoint_view_limits,
        }
    }

    fn open_using(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        limits: OpenLimits,
        max_tail_operations: Option<u64>,
        open_writer: impl FnOnce(&Path, u64, u64) -> io::Result<Log>,
    ) -> io::Result<Self> {
        Self::open_source_using(
            path,
            OpenSource::Explicit(checkpoint, trusted_digest),
            limits,
            (
                CheckpointRecoveryLimits {
                    max_tail_operations,
                    ..Default::default()
                },
                None,
            ),
            open_writer,
            selection::synchronize,
            None,
        )
    }

    fn open_source_using(
        path: &Path,
        source: OpenSource<'_>,
        limits: OpenLimits,
        (recovery, max_total_scan_bytes): (CheckpointRecoveryLimits, Option<u64>),
        open_writer: impl FnOnce(&Path, u64, u64) -> io::Result<Log>,
        sync_selection: impl FnOnce(&Path) -> io::Result<()>,
        adoption_tracker: Option<&TailAdoptionTracker>,
    ) -> io::Result<Self> {
        let OpenLimits {
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            max_log_bytes,
            checkpoint_view,
        } = limits;
        checkpoint_view.validate()?;
        RecoveredCheckpointView::validate_limits(
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
        )?;
        let path = std::fs::canonicalize(path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("store.lock"))?;
        let lock_result = fs2::FileExt::try_lock_exclusive(&lock);
        #[cfg(test)]
        let lock_result = crate::directory::diagnose_lock_failure(
            lock_result,
            &path.join("store.lock"),
            &lock,
            "checkpoint source store exclusive",
        );
        lock_result?;
        let lock = crate::store_lock::StoreLock::acquired(lock);
        lock.verify_identity(&path)?;
        let selection = selection::load(&path)?;
        let (checkpoint, digest, selection) = match source {
            OpenSource::Explicit(checkpoint, digest) => {
                if selection.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "managed checkpoint selection exists; use open_managed",
                    ));
                }
                (checkpoint.to_path_buf(), *digest, None)
            }
            OpenSource::Managed => {
                let selection = selection.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "managed checkpoint selection missing",
                    )
                })?;
                (
                    selection.checkpoint.clone(),
                    selection.digest,
                    Some(selection),
                )
            }
        };
        if let Some(max) = max_log_bytes {
            if std::fs::metadata(path.join("records.log"))?.len() > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing log exceeds configured limit",
                ));
            }
        }
        let prepared = CheckpointView::prepare_locked(
            &path,
            &checkpoint,
            &digest,
            lock,
            checkpoint_view,
            (max_log_bytes, None),
            crate::checkpoint_view::TailAdmission::Writable {
                max_tail_bytes,
                recovery_scan: crate::checkpoint::RecoveryScanLimits {
                    log_bytes: recovery.max_log_scan_bytes,
                    total_bytes: max_total_scan_bytes,
                    prefix_only: false,
                },
            },
        )?;
        let (_, range, boundary) = prepared.suffix();
        let length = range.end;
        let max_tail_operations = recovery.max_tail_operations;
        let suffix = RecoveredCheckpointView::replay_prepared(
            &prepared,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            max_tail_operations,
        )?;
        let stabilize_configuration = || {
            // A previous selection rename may have succeeded even if its
            // directory sync failed. Stabilize the visible selection before
            // allowing writes, including adoption of a complete tail.
            if selection.is_some() {
                sync_selection(&path)?;
            }
            let config = automatic::open_existing(&path)?;
            if config.is_some() && selection.is_none() {
                return Err(record::invalid_data(
                    "automatic checkpointing requires a managed selection",
                ));
            }
            if config.is_some() {
                // Only a verified, stabilized selection can authorize this
                // cleanup. A cleanup failure must precede witness adoption.
                crate::cleanup_abandoned_staging(&path)?;
                crate::cleanup_abandoned_staging(automatic::root(&path))?;
            }
            Ok(config)
        };
        let (base, log, automatic_config) = if let Some(suffix_digest) = suffix.adoption_digest {
            let log = open_writer(&path, boundary, length)?;
            let base = prepared.verify_for_adoption(&log, &suffix_digest)?;
            let config = stabilize_configuration()?;
            if let Some(tracker) = adoption_tracker {
                log.sync_validated_boundary_tracked(length, Some(tracker))?;
            } else {
                log.sync_validated_boundary(length).map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!(
                            "failed to synchronize recovered tail; outcome is uncertain: {error}"
                        ),
                    )
                })?;
            }
            (base, log, config)
        } else {
            let (base, _) = prepared.verify()?;
            let log = open_writer(&path, boundary, length)?;
            log.require_same_checkpoint_log(&base.log)?;
            (base, log, stabilize_configuration()?)
        };
        let mut view = suffix.attach(base);
        let automatic = automatic_config.unwrap_or(false);
        if let Some(view_log) = Arc::get_mut(&mut view.base.log) {
            view_log.try_share_checkpoint_view_file(&log);
        }
        let overlay_key_bytes = view.overlay.keys().map(|key| key.len()).sum();
        let log_generation = log.id().map(GenerationId);
        // The record is read under the exclusive store lock, so every
        // checkpoint-store open of this directory enforces it.
        let disk_budget = match disk_budget::read(&path)? {
            None => None,
            Some(record) => {
                let Some(selected) = &selection else {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "a disk-budgeted store must be opened as a managed store",
                    ));
                };
                if automatic {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "a disk-budgeted store cannot use automatic checkpoints",
                    ));
                }
                Some(Arc::new(disk_budget::DiskBudget::new(
                    &path,
                    record,
                    log.len()?,
                    log.id(),
                    Some(&selected.checkpoint),
                )?))
            }
        };
        let automatic_checkpoint = match &selection {
            Some(selected) if automatic_config.is_some() => {
                let pins = automatic::indexes(&automatic::root(&path)).map(|index| {
                    (selected.checkpoint == index).then(|| view.base.checkpoint_pin())
                });
                pins.iter().any(Option::is_some).then(|| {
                    Box::new(AutomaticCheckpointState {
                        slot_pins: pins,
                        log_scan_limit: None,
                    })
                })
            }
            _ => None,
        };
        Ok(Self {
            log,
            state: ControlledRwLock::new(State {
                view,
                log_generation,
                overlay_key_bytes,
                generation: 0,
                failed: false,
                selection,
                automatic,
                automatic_checkpoint,
                protect_pinned_automatic_slots: false,
            }),
            writers: ControlledRwLock::new(()),
            append_failed: AtomicBool::new(false),
            direct_writes: crate::direct_write::DirectWrites::default(),
            direct_reads: crate::direct_read::DirectReads::default(),
            transaction_budget: Mutex::new(crate::transaction_budget::TransactionBudget::default()),
            transaction_queue: crate::store::TransactionQueue::new(),
            snapshot_budget: Arc::new(Mutex::new(snapshot::SnapshotBudget::default())),
            snapshot_registry: Arc::new(Mutex::new(snapshot::SnapshotRegistry::new())),
            snapshot_reaper_active: AtomicBool::new(false),
            checkpoint_worker: AtomicBool::new(false),
            path,
            max_tail_bytes,
            max_tail_operations,
            max_tail_keys,
            max_tail_key_bytes,
            max_log_bytes,
            checkpoint_view_limits: checkpoint_view,
            disk_budget: RwLock::new(disk_budget),
            generation_token: disk_budget::generation_token(),
        })
    }

    fn ensure_healthy(&self, state: &State) -> io::Result<()> {
        if state.failed || self.append_failed.load(Ordering::Acquire) || self.writers.is_poisoned()
        {
            return Err(uncertain());
        }
        Ok(())
    }

    fn read(&self) -> io::Result<ControlledReadGuard<'_, State>> {
        let state = self.state.read().map_err(|_| uncertain())?;
        self.ensure_healthy(&state)?;
        Ok(state)
    }

    fn write(&self) -> io::Result<StateWrite<'_>> {
        self.write_until(None, None)
            .map_err(crate::TimedTransactionError::into_io)
    }

    // Hold the writer gate while inspecting committed values. Readers can
    // still use the view while a condition reads and verifies its value.
    fn read_before_write(
        &self,
    ) -> io::Result<(ControlledReadGuard<'_, State>, ControlledWriteGuard<'_, ()>)> {
        self.read_before_write_until(None, None)
            .map_err(crate::TimedTransactionError::into_io)
    }

    fn read_before_write_until(
        &self,
        deadline: Option<std::time::Instant>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<
        (ControlledReadGuard<'_, State>, ControlledWriteGuard<'_, ()>),
        crate::TimedTransactionError,
    > {
        let writer =
            crate::cancellation::write_lock(&self.writers, deadline, cancellation, uncertain)?;
        let state = crate::cancellation::read_lock(&self.state, deadline, cancellation, uncertain)?;
        self.ensure_healthy(&state)?;
        Ok((state, writer))
    }

    fn write_after_read<'a>(
        &'a self,
        state: ControlledReadGuard<'a, State>,
        writer: ControlledWriteGuard<'a, ()>,
    ) -> io::Result<StateWrite<'a>> {
        drop(state);
        self.lock_state_for_write_until(writer, None, None)
            .map_err(crate::TimedTransactionError::into_io)
    }

    fn write_until(
        &self,
        deadline: Option<std::time::Instant>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<StateWrite<'_>, crate::TimedTransactionError> {
        let writer =
            crate::cancellation::write_lock(&self.writers, deadline, cancellation, uncertain)?;
        self.lock_state_for_write_until(writer, deadline, cancellation)
    }

    // The caller keeps the writer gate when moving from a shared read to
    // this lock, so no mutation or checkpoint handoff can change the view.
    fn lock_state_for_write_until<'a>(
        &'a self,
        writer: ControlledWriteGuard<'a, ()>,
        deadline: Option<std::time::Instant>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<StateWrite<'a>, crate::TimedTransactionError> {
        let state =
            crate::cancellation::write_lock(&self.state, deadline, cancellation, uncertain)?;
        self.ensure_healthy(&state)?;
        Ok(StateWrite {
            state,
            _writer: writer,
        })
    }

    /// Reads a key from the verified checkpoint and committed overlay.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let _admission = self.direct_reads.acquire()?;
        self.read()?.view.get(key)
    }

    /// Reads up to 1,024 keys from one captured state, preserving input order
    /// and duplicates. The combined key and value bytes are limited to 32 MiB.
    pub fn get_many(&self, keys: &[&[u8]]) -> io::Result<Vec<Option<Vec<u8>>>> {
        if keys.len() > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many keys in bulk read",
            ));
        }
        for key in keys {
            record::validate_key(key)?;
        }
        let _admission = self.direct_reads.acquire()?;
        self.read()?.view.get_many(keys)
    }

    /// Reads one ordered page. Separate calls do not form a snapshot.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        for key in [after, before].into_iter().flatten() {
            record::validate_key(key)?;
        }
        if limit == 0 || limit > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scan limit must be between 1 and 1024",
            ));
        }
        let _admission = self.direct_reads.acquire()?;
        self.read()?.view.scan(after, before, limit)
    }

    /// Creates a verified full-log backup at a new path. Mutations wait until
    /// the copy is synchronized and published; concurrent readers may proceed.
    /// A publication-directory sync error leaves the destination uncertain.
    pub fn backup(&self, destination: impl AsRef<Path>) -> io::Result<()> {
        self.backup_using(destination.as_ref(), crate::backup::create)
    }

    /// Creates a backup with structured destination-publication failures.
    /// A rejection does not establish store health or guarantee cleanup.
    pub fn backup_detailed(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<(), crate::PublicationFailure> {
        self.backup_with_limits_detailed(destination, crate::FullLogBackupLimits::default())
    }

    /// Creates a full-log backup with a byte ceiling on the complete backup
    /// file, including its header and footer. An exact fit is accepted.
    /// A backup over the limit returns `NotPublished(InvalidInput)` before
    /// creating a stage or copying the log. This rejection does not establish
    /// that the source log passes full verification.
    pub fn backup_with_file_limit_detailed(
        &self,
        destination: impl AsRef<Path>,
        max_file_bytes: u64,
    ) -> Result<(), crate::PublicationFailure> {
        self.backup_with_limits_detailed(
            destination,
            crate::FullLogBackupLimits {
                max_file_bytes: Some(max_file_bytes),
                ..Default::default()
            },
        )
    }

    /// Creates a verified full-log backup with the independent ceilings of
    /// [`Store::backup_with_limits_detailed`](crate::Store::backup_with_limits_detailed).
    pub fn backup_with_limits_detailed(
        &self,
        destination: impl AsRef<Path>,
        limits: crate::FullLogBackupLimits,
    ) -> Result<(), crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.backup_using(destination.as_ref(), |log, destination| {
                crate::backup::create_tracked(log, destination, limits, tracker)
            })
        })
    }

    /// Creates a verified, non-overwriting compacted copy with its checkpoint
    /// at `COMPACTED_CHECKPOINT_FILE`. The returned digest must be protected
    /// separately. Mutations wait for the copy; readers may continue. The
    /// source remains unchanged and later source writes are not in the copy.
    /// An error after publication can leave an uncertain destination.
    pub fn compact_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.compact_to_using(
            destination.as_ref(),
            CheckpointCompactionLimits::default(),
            crate::store::compact_checkpoint,
        )
    }

    /// Creates a compacted copy with structured destination-publication
    /// failures. Its returned checkpoint digest requires separately trusted
    /// metadata; an uncertain failure does not return that digest.
    pub fn compact_to_detailed(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        self.compact_to_with_limits_detailed(destination, CheckpointCompactionLimits::default())
    }

    /// Creates a fully verified compacted copy under independent inclusive
    /// source-log read and replacement log ceilings. If a ceiling is
    /// exceeded, returns `NotPublished(InvalidInput)`; a private stage may
    /// have been created. Admission alone does not establish source health.
    /// The publication and digest requirements of `compact_to_detailed` apply.
    pub fn compact_to_with_limits_detailed(
        &self,
        destination: impl AsRef<Path>,
        limits: CheckpointCompactionLimits,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        self.compact_to_detailed_with_sync(
            destination.as_ref(),
            limits,
            crate::directory::sync_existing,
        )
    }

    fn compact_to_detailed_with_sync(
        &self,
        destination: &Path,
        limits: CheckpointCompactionLimits,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.compact_to_using(destination, limits, |log, view, count, target| {
                crate::store::compact_checkpoint_with_hooks(
                    log,
                    view,
                    count,
                    target,
                    sync_parent,
                    || {
                        tracker.begin_publication();
                        Ok(())
                    },
                    |_| Ok(()),
                )
            })
        })
    }

    fn compact_to_using(
        &self,
        destination: &Path,
        limits: CheckpointCompactionLimits,
        create: impl FnOnce(
            &Log,
            &RecoveredCheckpointView,
            u64,
            (&Path, crate::store::CheckpointCopyOptions),
        ) -> io::Result<[u8; 32]>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let _writer = self.writers.read().map_err(|_| uncertain())?;
        let state = self.read()?;
        let mut options = crate::store::CheckpointCopyOptions {
            source_value_read_bytes: limits.max_source_log_read_bytes,
            target_log_bytes: limits.max_target_log_bytes,
            ..Default::default()
        };
        if options.source_value_read_bytes.is_some() {
            options.charge_source_reads(self.log.len()?)?;
        }
        options.charge_target_log(crate::witness::LOG_HEADER_SIZE as u64)?;
        let (count, _, _) = self.verify_for_checkpoint(&state)?;
        create(&self.log, &state.view, count, (destination, options))
    }

    fn backup_using(
        &self,
        destination: &Path,
        create: impl FnOnce(&Log, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        self.check_selection_destination(destination)?;
        let _writer = self.writers.read().map_err(|_| uncertain())?;
        let _state = self.read()?;
        create(&self.log, destination)
    }

    /// Number of distinct suffix keys held in memory, including tombstones.
    pub fn overlay_keys(&self) -> io::Result<usize> {
        Ok(self.read()?.view.overlay_keys())
    }

    /// Current byte length of the witnessed suffix since the checkpoint.
    pub fn tail_bytes(&self) -> io::Result<u64> {
        Ok(self.read()?.view.tail_bytes())
    }

    /// Creates a verified checkpoint by streaming the on-disk index and the
    /// bounded suffix overlay while mutations are paused. Readers can use
    /// the committed view. Retain the trusted digest separately. The instance keeps
    /// using its original checkpoint; close it and reopen with the new one to
    /// clear the overlay. This independent copy is not subject to the current
    /// view's lower file-size or block limits. A publication sync error has an
    /// uncertain outcome.
    pub fn checkpoint_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.checkpoint_to_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    /// Creates a streamed checkpoint with structured publication failures.
    /// The returned digest must be retained separately in trusted metadata;
    /// an uncertain result does not return a digest and needs reconciliation.
    pub fn checkpoint_to_detailed(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        self.checkpoint_to_detailed_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    /// Creates a copy with the inclusive logical read ceiling of
    /// [`Store::checkpoint_to_with_scan_limit`](crate::Store::checkpoint_to_with_scan_limit).
    /// Verification of the covered prefix and replay of the suffix together
    /// charge one pass over the existing log. Staged readback charges another.
    pub fn checkpoint_to_with_scan_limit(
        &self,
        destination: impl AsRef<Path>,
        max_scan_bytes: u64,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.checkpoint_to_tracked(
                destination.as_ref(),
                crate::directory::sync_existing,
                Some(tracker),
                Some(max_scan_bytes),
            )
        })
    }

    fn checkpoint_to_detailed_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.checkpoint_to_tracked(destination, sync_parent, Some(tracker), None)
        })
    }

    fn checkpoint_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        self.checkpoint_to_tracked(destination, sync_parent, None, None)
    }

    fn checkpoint_to_tracked(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        tracker: Option<&crate::publication_outcome::Tracker>,
        max_scan_bytes: Option<u64>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let _writer = self.writers.write().map_err(|_| uncertain())?;
        let state = self.read()?;
        self.create_checkpoint_tracked(
            &state,
            destination,
            sync_parent,
            false,
            tracker,
            max_scan_bytes,
        )
    }

    /// Creates a checkpoint and switches the running instance to it while
    /// holding the writer gate, clearing the suffix overlay after verification.
    /// Readers can use the committed view until the final switch.
    /// The returned digest must be persisted separately by the caller. An
    /// error after publication can leave the new checkpoint at its destination;
    /// the instance continues using the previous checkpoint in that case. The
    /// configured view file-size and block limits are checked during creation;
    /// exceeding either rejects the checkpoint before its publication.
    pub fn rollover_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.rollover_to_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    /// Creates and adopts an unmanaged checkpoint with structured publication
    /// failures. An error after publication keeps the prior view active, but
    /// the destination may exist and the digest is not returned. Reconcile an
    /// uncertain outcome before retrying; managed stores use their own API.
    pub fn rollover_to_detailed(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        self.rollover_to_detailed_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    /// Creates and adopts an unmanaged checkpoint under an inclusive logical
    /// log read ceiling. It charges two passes for creation, as in
    /// [`Self::checkpoint_to_with_scan_limit`], plus one pass to open the
    /// new view. Admission precedes replay and publication.
    pub fn rollover_to_with_scan_limit(
        &self,
        destination: impl AsRef<Path>,
        max_scan_bytes: u64,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.rollover_to_tracked(
                destination.as_ref(),
                crate::directory::sync_existing,
                Some(tracker),
                Some(max_scan_bytes),
            )
        })
    }

    fn rollover_to_detailed_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.rollover_to_tracked(destination, sync_parent, Some(tracker), None)
        })
    }

    fn rollover_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        self.rollover_to_tracked(destination, sync_parent, None, None)
    }

    fn rollover_to_tracked(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        tracker: Option<&crate::publication_outcome::Tracker>,
        max_scan_bytes: Option<u64>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let _writer = self.writers.write().map_err(|_| uncertain())?;
        let state = self.read()?;
        if state.selection.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed stores require rollover_managed",
            ));
        }
        let digest = self.create_checkpoint_tracked(
            &state,
            destination,
            sync_parent,
            true,
            tracker,
            max_scan_bytes,
        )?;
        let next_view = self.rollover_view(&state, destination, &digest)?;
        drop(state);
        let mut state = self.state.write().map_err(|_| uncertain())?;
        state.view = next_view;
        state.overlay_key_bytes = 0;
        Ok(digest)
    }

    // Prevent other publication APIs from creating an unrelated file at the
    // reserved selection path, even before the store opts into management.
    fn check_selection_destination(&self, destination: &Path) -> io::Result<()> {
        let parent = crate::backup::parent_directory(destination)?;
        let resolved = std::fs::canonicalize(parent)?.join(
            destination
                .file_name()
                .expect("parent_directory validates the name"),
        );
        if resolved == selection::path(&self.path)
            || resolved == automatic::root(&self.path)
            || resolved.starts_with(automatic::root(&self.path))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination uses the reserved managed checkpoint namespace",
            ));
        }
        Ok(())
    }

    /// Creates a checkpoint and atomically records its selection before
    /// allowing more writes. Readers can use the committed view while the
    /// selection is published and synchronized. Protect the selection file as trusted
    /// metadata. A publication error poisons this instance; reopen it using
    /// the visible managed selection, or recover explicitly if none exists.
    pub fn rollover_managed(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.rollover_managed_with_hooks(
            destination.as_ref(),
            |_| Ok(()),
            || Ok(()),
            selection::sync_ancestors,
        )
    }

    /// Creates and selects a managed checkpoint with separate conservative
    /// failure classifications for checkpoint and selection publication.
    /// A selection staging error leaves the prior view active and falls in
    /// the checkpoint classification. A failure after selection replacement
    /// begins disables this instance. Reopen the
    /// visible selection, or recover with the previous trusted checkpoint or
    /// full log if none exists. No failure returns a new checkpoint digest,
    /// and classifications do not certify store health.
    pub fn rollover_managed_detailed(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<[u8; 32], crate::ManagedRolloverFailure> {
        self.rollover_managed_detailed_with_hooks(
            destination.as_ref(),
            |_| Ok(()),
            || Ok(()),
            selection::sync_ancestors,
        )
    }

    /// Creates and selects a managed checkpoint with the three-pass ceiling
    /// of [`Self::rollover_to_with_scan_limit`] and the outcome classifications
    /// of [`Self::rollover_managed_detailed`]. Budget refusal precedes replay
    /// and leaves the prior selection in place.
    pub fn rollover_managed_with_scan_limit(
        &self,
        destination: impl AsRef<Path>,
        max_scan_bytes: u64,
    ) -> Result<[u8; 32], crate::ManagedRolloverFailure> {
        crate::publication_outcome::track_managed(|tracker| {
            self.rollover_managed_tracked(
                destination.as_ref(),
                |_| Ok(()),
                || Ok(()),
                selection::sync_ancestors,
                Some(tracker),
                Some(max_scan_bytes),
            )
        })
    }

    fn rollover_managed_detailed_with_hooks(
        &self,
        destination: &Path,
        sync_checkpoint_parent: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 32], crate::ManagedRolloverFailure> {
        crate::publication_outcome::track_managed(|tracker| {
            self.rollover_managed_tracked(
                destination,
                sync_checkpoint_parent,
                before_selection_rename,
                sync_selection_parent,
                Some(tracker),
                None,
            )
        })
    }

    fn rollover_managed_with_hooks(
        &self,
        destination: &Path,
        sync_checkpoint_parent: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        self.rollover_managed_tracked(
            destination,
            sync_checkpoint_parent,
            before_selection_rename,
            sync_selection_parent,
            None,
            None,
        )
    }

    fn rollover_managed_tracked(
        &self,
        destination: &Path,
        sync_checkpoint_parent: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
        tracker: Option<&crate::publication_outcome::ManagedTracker>,
        max_scan_bytes: Option<u64>,
    ) -> io::Result<[u8; 32]> {
        // A managed rollover writes a selected checkpoint the budget does
        // not reserve; online compaction replaces it for budgeted stores.
        self.reject_under_disk_budget("managed rollover")?;
        self.check_selection_destination(destination)?;
        let _writer = self.writers.write().map_err(|_| uncertain())?;
        let state = self.read()?;
        let existing = selection::load(&self.path);
        if !matches!(&existing, Ok(found) if *found == state.selection) {
            if state.selection.is_some() {
                drop(state);
                self.state.write().map_err(|_| uncertain())?.failed = true;
            }
            return Err(io::Error::other(
                "managed checkpoint selection is missing, invalid or changed",
            ));
        }
        let parent = crate::backup::parent_directory(destination)?;
        let destination = std::fs::canonicalize(parent)?.join(
            destination
                .file_name()
                .expect("parent_directory validates the name"),
        );
        if destination == selection::path(&self.path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint destination is the managed selection file",
            ));
        }
        // Check path encoding and length before creating any checkpoint.
        selection::Selection::new(destination.clone(), [0; 32])?;
        let digest = self.create_checkpoint_tracked(
            &state,
            &destination,
            |parent| {
                sync_checkpoint_parent(parent)?;
                selection::sync_ancestors(parent)
            },
            true,
            tracker.map(|tracker| &tracker.checkpoint),
            max_scan_bytes,
        )?;
        let next_view = self.rollover_view(&state, &destination, &digest)?;
        let next = selection::Selection::new(destination, digest)?;
        drop(state);
        let local_tracker = crate::publication_outcome::Tracker::default();
        drop(self.publish_managed_view(
            next_view,
            next,
            before_selection_rename,
            sync_selection_parent,
            tracker.map_or(&local_tracker, |tracker| &tracker.selection),
        )?);
        Ok(digest)
    }

    // The caller holds the writer gate throughout. Readers can use the old
    // committed view during synchronization because the log and its indexed
    // values cannot change. The writer gate also poisons on unwind.
    fn publish_managed_view(
        &self,
        view: RecoveredCheckpointView,
        next: selection::Selection,
        before_rename: impl FnOnce() -> io::Result<()>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        tracker: &crate::publication_outcome::Tracker,
    ) -> io::Result<ControlledWriteGuard<'_, State>> {
        let result =
            selection::publish_tracked(&self.path, &next, before_rename, sync_parent, tracker);
        if let Err(error) = result {
            if !tracker.started() {
                return Err(error);
            }
            self.state.write().map_err(|_| uncertain())?.failed = true;
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "managed selection publication may have occurred; reopen required: {error}"
                ),
            ));
        }
        let mut state = self.state.write().map_err(|_| uncertain())?;
        state.view = view;
        state.overlay_key_bytes = 0;
        state.selection = Some(next);
        Ok(state)
    }

    fn rollover_view(
        &self,
        state: &State,
        destination: &Path,
        digest: &[u8; 32],
    ) -> io::Result<RecoveredCheckpointView> {
        (|| {
            let lock = state.view.base.share_store_lock()?;
            let base = CheckpointView::open_locked_with_limits(
                &self.path,
                destination,
                digest,
                lock,
                self.checkpoint_view_limits,
            )?;
            let mut view = RecoveredCheckpointView::from_base(
                base,
                self.max_tail_bytes,
                self.max_tail_keys,
                self.max_tail_key_bytes,
                self.max_tail_operations,
            )?;
            if view.tail_bytes != 0 || !view.overlay.is_empty() {
                return Err(record::invalid_data(
                    "new checkpoint has an unexpected suffix",
                ));
            }
            if let Some(view_log) = Arc::get_mut(&mut view.base.log) {
                view_log.try_share_checkpoint_view_file(&self.log);
            }
            Ok(view)
        })()
        .map_err(|error: io::Error| {
            io::Error::new(
                error.kind(),
                format!("checkpoint was published but switching failed: {error}"),
            )
        })
    }

    fn create_checkpoint_tracked(
        &self,
        state: &State,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        for_rollover: bool,
        tracker: Option<&crate::publication_outcome::Tracker>,
        max_scan_bytes: Option<u64>,
    ) -> io::Result<[u8; 32]> {
        crate::checkpoint::check_log_scan_limit(
            &self.log,
            2 + u64::from(for_rollover),
            max_scan_bytes,
            "checkpoint log scan byte limit exceeded",
        )?;
        let (count, peak, verified_log) = self.verify_for_checkpoint(state)?;
        crate::checkpoint::create_streaming_tracked(
            &self.log,
            count,
            peak,
            destination,
            |emit| {
                state.view.visit_entries(None, None, |key, location| {
                    emit(key, location)?;
                    Ok(std::ops::ControlFlow::Continue(()))
                })
            },
            sync_parent,
            crate::checkpoint::StreamingOptions {
                limits: for_rollover.then_some(self.checkpoint_view_limits),
                tracker,
                verified_log,
                pending_tail: None,
            },
        )
    }

    // Revalidate the separately trusted base and replay the entire bounded
    // suffix, checking both the live overlay and the historical key peak.
    fn verify_for_checkpoint(
        &self,
        state: &State,
    ) -> io::Result<(u64, usize, crate::checkpoint::VerifiedLog)> {
        let boundary = self.log.len()?;
        let (count, peak, log_hasher) = self.verify_view_at(
            &state.view,
            state.overlay_key_bytes,
            boundary,
            true,
            crate::checkpoint::RecoveryScanLimits::default(),
        )?;
        Ok((
            count,
            peak,
            crate::checkpoint::VerifiedLog::from_replay(boundary, log_hasher),
        ))
    }

    /// Verifies the committed state, including the complete log, under a
    /// combined scan ceiling. The caller holds the writer gate, so the
    /// admitted log length is the verified boundary.
    fn verify_with_total_scan_limit(&self, state: &State, max_scan_bytes: u64) -> io::Result<()> {
        let boundary = self.log.len()?;
        self.verify_view_at(
            &state.view,
            state.overlay_key_bytes,
            boundary,
            true,
            crate::checkpoint::RecoveryScanLimits {
                total_bytes: Some(max_scan_bytes),
                ..Default::default()
            },
        )
        .map(|_| ())
    }

    /// Verifies a captured view against the log prefix ending at `boundary`.
    /// The prefix is immutable, so later appends do not affect this check.
    /// Returns the live key count, historical peak and the prefix digest state.
    /// Without the writer gate, concurrent commits can advance the witness, so
    /// callers outside the gate skip the witness checks and repeat them later
    /// while excluding writers.
    fn verify_view_at(
        &self,
        view: &RecoveredCheckpointView,
        overlay_key_bytes: usize,
        boundary: u64,
        check_witness: bool,
        scan: crate::checkpoint::RecoveryScanLimits,
    ) -> io::Result<(u64, usize, sha2::Sha256)> {
        // A scan ceiling is admitted against the view's own log length; it
        // must be the boundary verified below. Callers hold the writer gate.
        if scan.total_bytes.is_some() && view.base.log.len()? != boundary {
            return Err(record::invalid_data(
                "log length changed before bounded verification",
            ));
        }
        if check_witness {
            self.log.verify_witness()?;
        }
        if boundary.checked_sub(view.base.log_boundary()) != Some(view.tail_bytes) {
            return Err(record::invalid_data("checkpoint suffix length changed"));
        }
        let mut current = usize::try_from(view.base.len())
            .map_err(|_| record::invalid_data("checkpoint key count does not fit this platform"))?;
        let mut peak = view.base.historical_peak();
        let touched_bytes = view.overlay.keys().try_fold(0_usize, |sum, key| {
            sum.checked_add(key.len())
                .ok_or_else(|| record::invalid_data("checkpoint suffix key byte count overflow"))
        })?;
        if view.tail_bytes > self.max_tail_bytes
            || self
                .max_tail_operations
                .is_some_and(|limit| view.tail_operations > limit)
            || view.overlay.len() > self.max_tail_keys
            || touched_bytes > self.max_tail_key_bytes
            || touched_bytes != overlay_key_bytes
        {
            return Err(record::invalid_data(
                "checkpoint overlay exceeds its limits",
            ));
        }
        struct Probe<'a> {
            key: &'a [u8],
            present: bool,
            // The outer option distinguishes an unseen key from a tombstone.
            replayed: Option<Option<ValueLocation>>,
        }

        // The committed overlay is immutable during verification and
        // already sorted. Borrow its keys and reserve all probe slots once.
        let mut probes = Vec::new();
        probes
            .try_reserve_exact(view.overlay.len())
            .map_err(io::Error::other)?;
        probes.extend(view.overlay.keys().map(|key| Probe {
            key,
            present: false,
            replayed: None,
        }));
        // Collect initial membership during the trusted index scan. Replay
        // uses these private results only after the index and log prefix verify.
        let mut pending = probes.iter_mut().peekable();
        // On the bounded path the witness was just verified for `boundary`
        // under the writer gate, so the scan is one pass over the log.
        let witnessed = (check_witness && scan.total_bytes.is_some()).then_some(boundary);
        let mut log_hasher =
            view.base
                .verify_for_rollover_admitted(scan, witnessed, |base_key| {
                    while let Some(probe) = pending.peek_mut() {
                        match probe.key.cmp(base_key) {
                            std::cmp::Ordering::Less => {
                                pending.next();
                            }
                            std::cmp::Ordering::Equal => {
                                probe.present = true;
                                pending.next();
                                break;
                            }
                            std::cmp::Ordering::Greater => break,
                        }
                    }
                })?;
        let mut operations = 0_u64;
        self.log.visit_records_with_digest(
            view.base.log_boundary(),
            boundary,
            boundary,
            &mut log_hasher,
            |key, location| {
                operations += 1;
                let position = probes
                    .binary_search_by(|probe| probe.key.cmp(key.as_ref()))
                    .map_err(|_| record::invalid_data("verified suffix has an unexpected key"))?;
                let probe = &mut probes[position];
                match (probe.present, location.is_some()) {
                    (false, true) => {
                        current = current.checked_add(1).ok_or_else(|| {
                            record::invalid_data("checkpoint live key count overflow")
                        })?
                    }
                    (true, false) => {
                        current = current.checked_sub(1).ok_or_else(|| {
                            record::invalid_data("checkpoint live key count underflow")
                        })?
                    }
                    _ => {}
                }
                peak = peak.max(current);
                probe.present = location.is_some();
                probe.replayed = Some(location);
                Ok(())
            },
        )?;
        if operations != view.tail_operations {
            return Err(record::invalid_data(
                "checkpoint suffix operation count changed",
            ));
        }
        if probes
            .iter()
            .zip(view.overlay.values())
            .any(|(probe, location)| probe.replayed != Some(*location))
        {
            return Err(record::invalid_data(
                "checkpoint overlay does not match verified suffix",
            ));
        }
        if check_witness {
            self.log.verify_witness()?;
        }
        Ok((current as u64, peak, log_hasher))
    }

    /// Replaces a key and synchronizes the mutation before returning.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.mutate_tracked(Operation::Put, key, value, None)
    }

    /// Replaces a key with structured rejection or uncertain-outcome errors.
    pub fn put_detailed(&self, key: &[u8], value: &[u8]) -> Result<(), crate::MutationFailure> {
        crate::transaction_outcome::mutation(|tracker| {
            self.mutate_tracked(Operation::Put, key, value, Some(tracker))
        })
    }

    /// Deletes a key, including an absent key, as a synchronized mutation.
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.mutate_tracked(Operation::Delete, key, &[], None)
    }

    /// Deletes a key with structured rejection or uncertain-outcome errors.
    pub fn delete_detailed(&self, key: &[u8]) -> Result<(), crate::MutationFailure> {
        crate::transaction_outcome::mutation(|tracker| {
            self.mutate_tracked(Operation::Delete, key, &[], Some(tracker))
        })
    }

    fn mutate_tracked(
        &self,
        operation: Operation,
        key: &[u8],
        value: &[u8],
        tracker: Option<&crate::transaction_outcome::Tracker>,
    ) -> io::Result<()> {
        record::validate(operation, key, value)?;
        let _admission = self.direct_writes.acquire()?;
        let state = self.write()?;
        self.commit(state, Mutation::Single(operation, key, value), tracker)
    }

    /// Conditionally replaces or deletes one key atomically against all writes.
    /// This does not detect ABA changes or provide a multi-key transaction.
    pub fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
    ) -> io::Result<bool> {
        self.compare_exchange_tracked(key, expected, replacement, None)
    }

    /// Performs compare-exchange with structured mutation failures. A
    /// mismatch returns `Ok(false)` without submitting a mutation.
    pub fn compare_exchange_detailed(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
    ) -> Result<bool, crate::MutationFailure> {
        crate::transaction_outcome::mutation(|tracker| {
            self.compare_exchange_tracked(key, expected, replacement, Some(tracker))
        })
    }

    fn compare_exchange_tracked(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
        tracker: Option<&crate::transaction_outcome::Tracker>,
    ) -> io::Result<bool> {
        let (operation, value) = match replacement {
            Some(value) => (Operation::Put, value),
            None => (Operation::Delete, &[][..]),
        };
        record::validate(operation, key, value)?;
        let _admission = self.direct_writes.acquire()?;
        let (state, writer) = self.read_before_write()?;
        if !state
            .view
            .base
            .log
            .value_matches(state.view.location(key)?, expected)?
        {
            return Ok(false);
        }
        let state = self.write_after_read(state, writer)?;
        self.commit(state, Mutation::Single(operation, key, value), tracker)?;
        Ok(true)
    }

    /// Atomically adds to a little-endian signed 64-bit value, starting at zero.
    /// Invalid lengths and overflow fail without appending a record.
    pub fn increment_i64(&self, key: &[u8], delta: i64) -> io::Result<i64> {
        self.increment_i64_tracked(key, delta, None)
    }

    /// Increments an integer with structured rejection or unknown-outcome
    /// errors. Do not blindly retry an unknown increment.
    pub fn increment_i64_detailed(
        &self,
        key: &[u8],
        delta: i64,
    ) -> Result<i64, crate::MutationFailure> {
        crate::transaction_outcome::mutation(|tracker| {
            self.increment_i64_tracked(key, delta, Some(tracker))
        })
    }

    fn increment_i64_tracked(
        &self,
        key: &[u8],
        delta: i64,
        tracker: Option<&crate::transaction_outcome::Tracker>,
    ) -> io::Result<i64> {
        record::validate_key(key)?;
        let _admission = self.direct_writes.acquire()?;
        let (state, writer) = self.read_before_write()?;
        let current = state
            .view
            .base
            .log
            .read_i64_or_zero(state.view.location(key)?)?;
        let next = current.checked_add(delta).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "integer increment overflow")
        })?;
        let state = self.write_after_read(state, writer)?;
        self.commit(
            state,
            Mutation::Single(Operation::Put, key, &next.to_le_bytes()),
            tracker,
        )?;
        Ok(next)
    }

    /// Applies a blind ordered batch atomically as one synchronized record.
    /// This is not a read-modify-write transaction.
    pub fn write_batch(&self, operations: &[BatchOperation<'_>]) -> io::Result<()> {
        self.write_batch_tracked(operations, None)
    }

    /// Applies a blind atomic batch with structured mutation failures.
    pub fn write_batch_detailed(
        &self,
        operations: &[BatchOperation<'_>],
    ) -> Result<(), crate::MutationFailure> {
        crate::transaction_outcome::mutation(|tracker| {
            self.write_batch_tracked(operations, Some(tracker))
        })
    }

    fn write_batch_tracked(
        &self,
        operations: &[BatchOperation<'_>],
        tracker: Option<&crate::transaction_outcome::Tracker>,
    ) -> io::Result<()> {
        let _admission = self.direct_writes.acquire()?;
        let state = self.write()?;
        let batch = BatchLayout::new(operations)?;
        self.commit(state, Mutation::Batch(batch), tracker)
    }

    /// Atomically applies a blind batch if all point-value conditions match.
    /// `None` expects an absent key; duplicates are checked independently.
    /// Conditions are limited to 1,024 entries and 32 MiB of combined key and
    /// expected-value bytes. A mismatch returns `Ok(false)` without appending.
    /// This does not detect ABA changes, validate ranges or run a transaction
    /// callback; callers must include every point value their decision uses.
    pub fn compare_exchange_batch(
        &self,
        conditions: &[(&[u8], Option<&[u8]>)],
        operations: &[BatchOperation<'_>],
    ) -> io::Result<bool> {
        self.compare_exchange_batch_tracked(conditions, operations, None)
    }

    /// Conditionally applies an atomic batch with structured mutation errors.
    /// A condition mismatch returns `Ok(false)` without submitting a batch.
    pub fn compare_exchange_batch_detailed(
        &self,
        conditions: &[(&[u8], Option<&[u8]>)],
        operations: &[BatchOperation<'_>],
    ) -> Result<bool, crate::MutationFailure> {
        crate::transaction_outcome::mutation(|tracker| {
            self.compare_exchange_batch_tracked(conditions, operations, Some(tracker))
        })
    }

    fn compare_exchange_batch_tracked(
        &self,
        conditions: &[(&[u8], Option<&[u8]>)],
        operations: &[BatchOperation<'_>],
        tracker: Option<&crate::transaction_outcome::Tracker>,
    ) -> io::Result<bool> {
        if conditions.len() > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many batch conditions",
            ));
        }
        let mut size = 0_usize;
        for (key, expected) in conditions {
            record::validate_key(key)?;
            size = size
                .checked_add(key.len())
                .and_then(|sum| sum.checked_add(expected.map_or(0, |value| value.len())))
                .filter(|sum| *sum <= MAX_SCAN_BYTES)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "batch conditions exceed 32 MiB",
                    )
                })?;
        }
        let _admission = self.direct_writes.acquire()?;
        let (state, writer) = self.read_before_write()?;
        let batch = BatchLayout::new(operations)?;
        for (key, expected) in conditions {
            if !state
                .view
                .base
                .log
                .value_matches(state.view.location(key)?, *expected)?
            {
                return Ok(false);
            }
        }
        let state = self.write_after_read(state, writer)?;
        self.commit(state, Mutation::Batch(batch), tracker)?;
        Ok(true)
    }
}

fn uncertain() -> io::Error {
    io::Error::other(
        "a previous write or checkpoint publication may have an uncertain outcome; reopen the store",
    )
}

#[cfg(test)]
mod tests;
