//! Experimental writable checkpoint index with a bounded log-suffix overlay.
use crate::checkpoint_view::{CheckpointView, CheckpointViewLimits};
use crate::controlled_lock::{
    ControlledRwLock, ReadGuard as ControlledReadGuard, WriteGuard as ControlledWriteGuard,
};
use crate::log::{GenerationId, Log};
use crate::record::batch::{BatchOperation, encode_payload};
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

mod automatic;
mod commit;
mod compact_exchange;
mod read_set;
mod rebuild;
mod rebuild_spill;
pub use rebuild::{BoundedFullRebuildLimits, BoundedSuffixRebuildLimits};
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
    snapshot_budget: Mutex<snapshot::SnapshotBudget>,
    snapshot_registry: Mutex<snapshot::SnapshotRegistry>,
    pub(crate) snapshot_reaper_active: AtomicBool,
    checkpoint_worker: AtomicBool,
    path: PathBuf,
    max_tail_bytes: u64,
    max_tail_keys: usize,
    max_tail_key_bytes: usize,
    max_log_bytes: Option<u64>,
    checkpoint_view_limits: CheckpointViewLimits,
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
            Log::open_checkpoint_writer,
            selection::synchronize,
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
            Log::open_checkpoint_writer,
            selection::synchronize,
        )
    }

    /// Returns the logical limits applied to this open instance.
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
        open_writer: impl FnOnce(&Path, u64, u64) -> io::Result<Log>,
    ) -> io::Result<Self> {
        Self::open_source_using(
            path,
            OpenSource::Explicit(checkpoint, trusted_digest),
            limits,
            open_writer,
            selection::synchronize,
        )
    }

    fn open_source_using(
        path: &Path,
        source: OpenSource<'_>,
        limits: OpenLimits,
        open_writer: impl FnOnce(&Path, u64, u64) -> io::Result<Log>,
        sync_selection: impl FnOnce(&Path) -> io::Result<()>,
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
        let (base, _) = CheckpointView::open_locked_with_recovery_limits(
            &path,
            &checkpoint,
            &digest,
            lock,
            checkpoint_view,
            max_log_bytes,
            Some(max_tail_bytes),
        )?;
        let (mut view, adoption_digest) = RecoveredCheckpointView::from_base_for_writer(
            base,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            max_log_bytes,
        )?;
        let boundary = view.base.witnessed_boundary;
        let length = view
            .base
            .log_boundary()
            .checked_add(view.tail_bytes)
            .ok_or_else(|| record::invalid_data("checkpoint suffix length overflow"))?;
        if max_log_bytes.is_some_and(|max| length > max) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "existing log exceeds configured limit",
            ));
        }
        let log = open_writer(&path, boundary, length)?;
        if log.id() != view.base.log.id() {
            return Err(record::invalid_data(
                "log identity changed while opening checkpoint writer",
            ));
        }
        if let Some(suffix_digest) = adoption_digest {
            crate::checkpoint::bind_replayed_log(
                &log,
                view.base.log_boundary(),
                length,
                view.base.covered_log_digest(),
                &suffix_digest,
            )?;
            log.sync_validated_boundary(length).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("failed to synchronize recovered tail; outcome is uncertain: {error}"),
                )
            })?;
        }
        // A previous selection rename may have succeeded even if its directory
        // sync failed. Stabilize the currently visible selection before writes.
        if selection.is_some() {
            sync_selection(&path)?;
        }
        let automatic_config = automatic::open_existing(&path)?;
        if automatic_config.is_some() && selection.is_none() {
            return Err(record::invalid_data(
                "automatic checkpointing requires a managed selection",
            ));
        }
        if automatic_config.is_some() {
            // The durable selection was verified and stabilized above. Stale
            // recognized stages cannot be the selected published checkpoint.
            crate::cleanup_abandoned_staging(&path)?;
            crate::cleanup_abandoned_staging(automatic::root(&path))?;
        }
        let automatic = automatic_config.unwrap_or(false);
        if let Some(view_log) = Arc::get_mut(&mut view.base.log) {
            view_log.try_share_checkpoint_view_file(&log);
        }
        let overlay_key_bytes = view.overlay.keys().map(Vec::len).sum();
        let log_generation = log.id().map(GenerationId);
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
            }),
            writers: ControlledRwLock::new(()),
            append_failed: AtomicBool::new(false),
            direct_writes: crate::direct_write::DirectWrites::default(),
            direct_reads: crate::direct_read::DirectReads::default(),
            transaction_budget: Mutex::new(crate::transaction_budget::TransactionBudget::default()),
            transaction_queue: crate::store::TransactionQueue::new(),
            snapshot_budget: Mutex::new(snapshot::SnapshotBudget::default()),
            snapshot_registry: Mutex::new(snapshot::SnapshotRegistry::new()),
            snapshot_reaper_active: AtomicBool::new(false),
            checkpoint_worker: AtomicBool::new(false),
            path,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
            max_log_bytes,
            checkpoint_view_limits: checkpoint_view,
        })
    }

    fn ensure_healthy(&self, state: &State) -> io::Result<()> {
        if state.failed || self.append_failed.load(Ordering::Acquire) {
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

    fn write_until(
        &self,
        deadline: Option<std::time::Instant>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<StateWrite<'_>, crate::TimedTransactionError> {
        let writer =
            crate::cancellation::write_lock(&self.writers, deadline, cancellation, uncertain)?;
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
        let state = self.read()?;
        let mut size = 0_usize;
        let mut locations = Vec::with_capacity(keys.len());
        for key in keys {
            let location = state.view.location(key)?;
            size = size
                .checked_add(key.len() + location.map_or(0, |entry| entry.length))
                .filter(|bytes| *bytes <= MAX_SCAN_BYTES)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "bulk read exceeds 32 MiB")
                })?;
            locations.push(location);
        }
        locations
            .into_iter()
            .map(|location| {
                location
                    .map(|location| state.view.base.log.read(location))
                    .transpose()
            })
            .collect()
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
        self.backup_detailed_with_limit(destination.as_ref(), None)
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
        self.backup_detailed_with_limit(destination.as_ref(), Some(max_file_bytes))
    }

    fn backup_detailed_with_limit(
        &self,
        destination: &Path,
        max_file_bytes: Option<u64>,
    ) -> Result<(), crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.backup_using(destination, |log, destination| {
                crate::backup::create_tracked(log, destination, max_file_bytes, tracker)
            })
        })
    }

    /// Creates a verified, non-overwriting compacted copy with its checkpoint
    /// at `COMPACTED_CHECKPOINT_FILE`. The returned digest must be protected
    /// separately. Mutations wait for the copy; readers may continue. The
    /// source remains unchanged and later source writes are not in the copy.
    /// An error after publication can leave an uncertain destination.
    pub fn compact_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.compact_to_using(destination.as_ref(), crate::store::compact_checkpoint)
    }

    /// Creates a compacted copy with structured destination-publication
    /// failures. Its returned checkpoint digest requires separately trusted
    /// metadata; an uncertain failure does not return that digest.
    pub fn compact_to_detailed(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        self.compact_to_detailed_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    fn compact_to_detailed_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.compact_to_using(destination, |log, view, count, destination| {
                crate::store::compact_checkpoint_with_hooks(
                    log,
                    view,
                    count,
                    destination,
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
        create: impl FnOnce(&Log, &RecoveredCheckpointView, u64, &Path) -> io::Result<[u8; 32]>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let _writer = self.writers.read().map_err(|_| uncertain())?;
        let state = self.read()?;
        let (count, _, _) = self.verify_for_checkpoint(&state)?;
        create(&self.log, &state.view, count, destination)
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
    /// bounded suffix overlay while mutations are paused. The independently
    /// trusted digest must be retained separately. The current instance keeps
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

    fn checkpoint_to_detailed_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.check_selection_destination(destination)?;
            let state = self.write()?;
            self.create_checkpoint_tracked(&state, destination, sync_parent, false, Some(tracker))
        })
    }

    fn checkpoint_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let state = self.write()?;
        self.create_checkpoint(&state, destination, sync_parent, false)
    }

    /// Creates a checkpoint and switches the running instance to it under the
    /// same exclusive lock, clearing the suffix overlay after verification.
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

    fn rollover_to_detailed_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 32], crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.rollover_to_tracked(destination, sync_parent, Some(tracker))
        })
    }

    fn rollover_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        self.rollover_to_tracked(destination, sync_parent, None)
    }

    fn rollover_to_tracked(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        tracker: Option<&crate::publication_outcome::Tracker>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let mut state = self.write()?;
        if state.selection.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed stores require rollover_managed",
            ));
        }
        let digest =
            self.create_checkpoint_tracked(&state, destination, sync_parent, true, tracker)?;
        let next_view = self.rollover_view(&state, destination, &digest)?;
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
    /// allowing more writes. Protect the store's selection file as trusted
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
    /// A selection-publication failure disables this instance. Reopen the
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
        )
    }

    fn rollover_managed_tracked(
        &self,
        destination: &Path,
        sync_checkpoint_parent: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
        tracker: Option<&crate::publication_outcome::ManagedTracker>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let mut state = self.write()?;
        let existing = selection::load(&self.path);
        if !matches!(&existing, Ok(found) if *found == state.selection) {
            if state.selection.is_some() {
                state.failed = true;
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
        )?;
        let next_view = self.rollover_view(&state, &destination, &digest)?;
        let next = selection::Selection::new(destination, digest)?;
        if let Some(tracker) = tracker {
            tracker.begin_selection();
        }
        if let Err(error) = selection::publish(
            &self.path,
            &next,
            before_selection_rename,
            sync_selection_parent,
        ) {
            state.failed = true;
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "managed selection publication may have occurred; reopen required: {error}"
                ),
            ));
        }
        state.view = next_view;
        state.overlay_key_bytes = 0;
        state.selection = Some(next);
        Ok(digest)
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

    fn create_checkpoint(
        &self,
        state: &State,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        for_rollover: bool,
    ) -> io::Result<[u8; 32]> {
        self.create_checkpoint_tracked(state, destination, sync_parent, for_rollover, None)
    }

    fn create_checkpoint_tracked(
        &self,
        state: &State,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        for_rollover: bool,
        tracker: Option<&crate::publication_outcome::Tracker>,
    ) -> io::Result<[u8; 32]> {
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
        self.log.verify_witness()?;
        let view = &state.view;
        let mut log_hasher = view.base.verify_for_rollover()?;
        let boundary = self.log.len()?;
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
            || view.overlay.len() > self.max_tail_keys
            || touched_bytes > self.max_tail_key_bytes
            || touched_bytes != state.overlay_key_bytes
        {
            return Err(record::invalid_data(
                "checkpoint overlay exceeds its limits",
            ));
        }
        // Resolve initial membership by merging the sorted touched keys with
        // checkpoint blocks, instead of rereading an index block per key.
        // The nested option distinguishes an unseen key from a tombstone.
        let mut probes = view
            .overlay
            .keys()
            .cloned()
            .map(|key| (key, (false, None)))
            .collect::<BTreeMap<_, (bool, Option<Option<crate::log::ValueLocation>>)>>();
        if let Some(first) = probes.keys().next().cloned() {
            probes.get_mut(&first).expect("first probe exists").0 =
                view.base.location(&first)?.is_some();
            let mut pending = probes
                .range_mut::<[u8], _>((Excluded(first.as_slice()), Unbounded))
                .peekable();
            if pending.peek().is_some() {
                view.base.visit_entries(Some(&first), None, |base_key, _| {
                    while pending
                        .peek()
                        .is_some_and(|(key, _)| key.as_slice() < base_key)
                    {
                        pending.next();
                    }
                    if pending
                        .peek()
                        .is_some_and(|(key, _)| key.as_slice() == base_key)
                    {
                        pending.next().expect("peeked probe").1.0 = true;
                    }
                    if pending.peek().is_none() {
                        Ok(std::ops::ControlFlow::Break(()))
                    } else {
                        Ok(std::ops::ControlFlow::Continue(()))
                    }
                })?;
            }
        }
        self.log.visit_records_with_digest(
            view.base.log_boundary(),
            boundary,
            boundary,
            &mut log_hasher,
            |key, location| {
                let probe = probes
                    .get_mut(&key)
                    .ok_or_else(|| record::invalid_data("verified suffix has an unexpected key"))?;
                match (probe.0, location.is_some()) {
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
                probe.0 = location.is_some();
                probe.1 = Some(location);
                Ok(())
            },
        )?;
        if view
            .overlay
            .iter()
            .any(|(key, location)| probes.get(key).and_then(|probe| probe.1) != Some(*location))
        {
            return Err(record::invalid_data(
                "checkpoint overlay does not match verified suffix",
            ));
        }
        self.log.verify_witness()?;
        let verified_log = crate::checkpoint::VerifiedLog::from_replay(boundary, log_hasher);
        Ok((current as u64, peak, verified_log))
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
        self.commit(state, operation, key, value, &[key], tracker)
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
        let state = self.write()?;
        if state.view.get(key)?.as_deref() != expected {
            return Ok(false);
        }
        self.commit(state, operation, key, value, &[key], tracker)?;
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
        let state = self.write()?;
        let current = match state.view.get(key)? {
            None => 0,
            Some(bytes) => i64::from_le_bytes(bytes.try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "increment requires an eight-byte signed integer",
                )
            })?),
        };
        let next = current.checked_add(delta).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "integer increment overflow")
        })?;
        self.commit(
            state,
            Operation::Put,
            key,
            &next.to_le_bytes(),
            &[key],
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
        let payload = encode_payload(operations)?;
        let keys = operations
            .iter()
            .map(|operation| operation.fields().1)
            .collect::<Vec<_>>();
        self.commit(state, Operation::Batch, &[0], &payload, &keys, tracker)
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
        let state = self.write()?;
        let payload = encode_payload(operations)?;
        for (key, expected) in conditions {
            if state.view.get(key)?.as_deref() != *expected {
                return Ok(false);
            }
        }
        let keys = operations
            .iter()
            .map(|operation| operation.fields().1)
            .collect::<Vec<_>>();
        self.commit(state, Operation::Batch, &[0], &payload, &keys, tracker)?;
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
