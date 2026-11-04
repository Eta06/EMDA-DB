//! Experimental writable checkpoint index with a bounded log-suffix overlay.
use crate::checkpoint_view::{CheckpointView, CheckpointViewLimits};
use crate::log::Log;
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
use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

mod automatic;
mod commit;
mod compact_exchange;
mod read_set;
mod rebuild;
mod selection;
mod snapshot;
mod transaction;
mod worker;
pub use snapshot::{
    CheckpointSnapshot, MAX_CHECKPOINT_ACTIVE_SNAPSHOTS, MAX_CHECKPOINT_SNAPSHOT_TOTAL_BLOCKS,
    MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEY_BYTES, MAX_CHECKPOINT_SNAPSHOT_TOTAL_OVERLAY_KEYS,
};
pub use transaction::{CheckpointTransaction, CheckpointTransactionMode};
pub use worker::{CheckpointWorker, CheckpointWorkerStatus};

struct State {
    view: RecoveredCheckpointView,
    overlay_key_bytes: usize,
    generation: u64,
    failed: bool,
    selection: Option<selection::Selection>,
    automatic: bool,
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
    state: RwLock<State>,
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
        fs2::FileExt::try_lock_exclusive(&lock)?;
        crate::directory::verify_lock_identity(&path, &lock)?;
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
        let base = CheckpointView::open_locked_with_limits(
            &path,
            &checkpoint,
            &digest,
            lock,
            checkpoint_view,
        )?;
        let view = RecoveredCheckpointView::from_base_for_writer(
            base,
            max_tail_bytes,
            max_tail_keys,
            max_tail_key_bytes,
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
        if length > boundary {
            log.sync_data().map_err(|error| {
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
        let overlay_key_bytes = view.overlay.keys().map(Vec::len).sum();
        Ok(Self {
            log,
            state: RwLock::new(State {
                view,
                overlay_key_bytes,
                generation: 0,
                failed: false,
                selection,
                automatic,
            }),
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

    fn read(&self) -> io::Result<RwLockReadGuard<'_, State>> {
        let state = self.state.read().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        Ok(state)
    }

    fn write(&self) -> io::Result<RwLockWriteGuard<'_, State>> {
        let state = self.state.write().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        Ok(state)
    }

    /// Reads a key from the verified checkpoint and committed overlay.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
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
        self.read()?.view.scan(after, before, limit)
    }

    /// Creates a verified full-log backup at a new path. Mutations wait until
    /// the copy is synchronized and published; concurrent readers may proceed.
    /// A publication-directory sync error leaves the destination uncertain.
    pub fn backup(&self, destination: impl AsRef<Path>) -> io::Result<()> {
        self.backup_using(destination.as_ref(), crate::backup::create)
    }

    /// Creates a verified, non-overwriting compacted copy with its checkpoint
    /// at `COMPACTED_CHECKPOINT_FILE`. The returned digest must be protected
    /// separately. Mutations wait for the copy; readers may continue. The
    /// source remains unchanged and later source writes are not in the copy.
    /// An error after publication can leave an uncertain destination.
    pub fn compact_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination.as_ref())?;
        let state = self.read()?;
        let (count, _) = self.verify_for_checkpoint(&state)?;
        crate::store::compact_checkpoint(&self.log, &state.view, count, destination.as_ref())
    }

    fn backup_using(
        &self,
        destination: &Path,
        create: impl FnOnce(&Log, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        self.check_selection_destination(destination)?;
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
    /// clear the overlay. A publication sync error has an uncertain outcome.
    pub fn checkpoint_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.checkpoint_to_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    fn checkpoint_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let state = self.write()?;
        self.create_checkpoint(&state, destination, sync_parent)
    }

    /// Creates a checkpoint and switches the running instance to it under the
    /// same exclusive lock, clearing the suffix overlay after verification.
    /// The returned digest must be persisted separately by the caller. An
    /// error after publication can leave the new checkpoint at its destination;
    /// the instance continues using the previous checkpoint in that case.
    pub fn rollover_to(&self, destination: impl AsRef<Path>) -> io::Result<[u8; 32]> {
        self.rollover_to_with_sync(destination.as_ref(), crate::directory::sync_existing)
    }

    fn rollover_to_with_sync(
        &self,
        destination: &Path,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        self.check_selection_destination(destination)?;
        let mut state = self.write()?;
        if state.selection.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed stores require rollover_managed",
            ));
        }
        let digest = self.create_checkpoint(&state, destination, sync_parent)?;
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

    fn rollover_managed_with_hooks(
        &self,
        destination: &Path,
        sync_checkpoint_parent: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
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
        let digest = self.create_checkpoint(&state, &destination, |parent| {
            sync_checkpoint_parent(parent)?;
            selection::sync_ancestors(parent)
        })?;
        let next_view = self.rollover_view(&state, &destination, &digest)?;
        let next = selection::Selection::new(destination, digest)?;
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
            let lock = state.view.base.duplicate_store_lock()?;
            let base = CheckpointView::open_locked_with_limits(
                &self.path,
                destination,
                digest,
                lock,
                self.checkpoint_view_limits,
            )?;
            let view = RecoveredCheckpointView::from_base(
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
    ) -> io::Result<[u8; 32]> {
        let (count, peak) = self.verify_for_checkpoint(state)?;
        crate::checkpoint::create_streaming_with_sync(
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
        )
    }

    // Revalidate the separately trusted base and replay the entire bounded
    // suffix, checking both the live overlay and the historical key peak.
    fn verify_for_checkpoint(&self, state: &State) -> io::Result<(u64, usize)> {
        self.log.verify_witness()?;
        let view = &state.view;
        view.base.verify_for_rollover()?;
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
        self.log
            .visit_records(view.base.log_boundary(), Some(boundary), |key, location| {
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
            })?;
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
        Ok((current as u64, peak))
    }

    /// Replaces a key and synchronizes the mutation before returning.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        record::validate(Operation::Put, key, value)?;
        let mut state = self.write()?;
        self.commit(&mut state, Operation::Put, key, value, &[key])
    }

    /// Deletes a key, including an absent key, as a synchronized mutation.
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        record::validate(Operation::Delete, key, &[])?;
        let mut state = self.write()?;
        self.commit(&mut state, Operation::Delete, key, &[], &[key])
    }

    /// Conditionally replaces or deletes one key atomically against all writes.
    /// This does not detect ABA changes or provide a multi-key transaction.
    pub fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
    ) -> io::Result<bool> {
        let (operation, value) = match replacement {
            Some(value) => (Operation::Put, value),
            None => (Operation::Delete, &[][..]),
        };
        record::validate(operation, key, value)?;
        let mut state = self.write()?;
        if state.view.get(key)?.as_deref() != expected {
            return Ok(false);
        }
        self.commit(&mut state, operation, key, value, &[key])?;
        Ok(true)
    }

    /// Atomically adds to a little-endian signed 64-bit value, starting at zero.
    /// Invalid lengths and overflow fail without appending a record.
    pub fn increment_i64(&self, key: &[u8], delta: i64) -> io::Result<i64> {
        record::validate_key(key)?;
        let mut state = self.write()?;
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
        self.commit(&mut state, Operation::Put, key, &next.to_le_bytes(), &[key])?;
        Ok(next)
    }

    /// Applies a blind ordered batch atomically as one synchronized record.
    /// This is not a read-modify-write transaction.
    pub fn write_batch(&self, operations: &[BatchOperation<'_>]) -> io::Result<()> {
        let mut state = self.write()?;
        let payload = encode_payload(operations)?;
        let keys = operations
            .iter()
            .map(|operation| operation.fields().1)
            .collect::<Vec<_>>();
        self.commit(&mut state, Operation::Batch, &[0], &payload, &keys)
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
        let mut state = self.write()?;
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
        self.commit(&mut state, Operation::Batch, &[0], &payload, &keys)?;
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
