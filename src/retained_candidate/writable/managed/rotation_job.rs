//! Retained rotation with its expensive preparation off the writer path.
//!
//! [`ManagedRetainedStore::begin_rotation`] captures the committed state
//! and reserves room for the successor. [`RetainedRotationJob::prepare`]
//! verifies the captured generation and writes the successor base into a
//! private stage without borrowing the store, so it can run on another
//! thread while writes continue. [`ManagedRetainedStore::complete_rotation`]
//! then replays the records committed since the capture into the successor,
//! re-encoding each one identically, rechecks the live generation, and
//! publishes the successor and the selection exactly as `compact` does.
use super::super::rebase::FrozenRetained;
use super::{
    ACTIVE_FILE, Active, CatalogStorageLimits, ManagedRetainedStore, RetainedStore,
    RetainedStoreLimits, Slot, catalog_storage, generation_role, intersect,
};
use crate::directory::sync_existing;
use crate::retained_candidate::Expected;
use crate::staging::Stage;
use crate::{BatchOperation, MutationFailure, RetainedRotationFailure};
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Shared by a catalog handle and its outstanding rotation job.
#[derive(Default)]
pub(super) struct RotationState {
    active: AtomicBool,
    pub(super) reserved: AtomicU64,
}

/// A retained rotation captured from a managed catalog. It owns its
/// private stage and duplicated read-only handles of the captured
/// generation, and borrows nothing, so [`Self::prepare`] can run on another
/// thread while the store keeps writing. Dropping it removes the stage and
/// releases its storage reservation.
pub struct RetainedRotationJob {
    frozen: FrozenRetained,
    source: [u8; 16],
    slot: Slot,
    limits: RetainedStoreLimits,
    persisted_storage: Option<CatalogStorageLimits>,
    role: super::GenerationRole,
    max_base_bytes: u64,
    // Declared before the stage so the successor's files close first.
    prepared: Option<(Expected, RetainedStore)>,
    stage: Option<Stage>,
    state: Arc<RotationState>,
}

impl Drop for RetainedRotationJob {
    fn drop(&mut self) {
        self.state.reserved.store(0, Ordering::Release);
        self.state.active.store(false, Ordering::Release);
    }
}

impl RetainedRotationJob {
    /// Verifies the captured generation's suffix bytes and every base block,
    /// then writes and opens the successor in the private stage. The store is
    /// not borrowed. Calling it again after success does nothing.
    pub fn prepare(&mut self) -> io::Result<()> {
        if self.prepared.is_some() {
            return Ok(());
        }
        let stage = self
            .stage
            .as_ref()
            .ok_or_else(|| io::Error::other("rotation stage is missing"))?;
        self.frozen.verify()?;
        let expected = self.frozen.write_rebased_base(
            &stage.path().join("base"),
            self.limits,
            self.max_base_bytes,
            &mut None,
        )?;
        let next = RetainedStore::initialize(
            stage.path(),
            self.limits,
            &expected,
            None,
            self.role,
            self.persisted_storage,
            None,
        )?;
        if next.generation_id() == self.source {
            return Err(crate::record::invalid_data(
                "retained generation identifier collision",
            ));
        }
        self.prepared = Some((expected, next));
        Ok(())
    }

    /// Whether [`Self::prepare`] has finished.
    pub fn is_prepared(&self) -> bool {
        self.prepared.is_some()
    }
}

impl ManagedRetainedStore {
    /// Captures the committed state for a rotation prepared off the writer
    /// path. It makes the checks `compact` makes before it allocates: a
    /// verified selection, no pending retirement stage and an empty inactive
    /// slot. Under a catalog storage quota it also reserves room for the
    /// successor base, bounded by the current base plus suffix. Later appends
    /// are admitted against that reservation until the job completes or is
    /// dropped. One job may be outstanding at a time.
    pub fn begin_rotation(&mut self) -> io::Result<RetainedRotationJob> {
        self.begin_rotation_with_limits(self.store.limits)
    }

    /// Like [`Self::begin_rotation`] with explicit successor limits.
    pub fn begin_rotation_with_limits(
        &mut self,
        limits: RetainedStoreLimits,
    ) -> io::Result<RetainedRotationJob> {
        self.verify_selection()?;
        self.capture_rotation(limits)
    }

    // The caller has verified the selection or the authority record.
    pub(super) fn capture_rotation(
        &mut self,
        limits: RetainedStoreLimits,
    ) -> io::Result<RetainedRotationJob> {
        if self.rotation.active.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "another retained rotation job is outstanding",
            ));
        }
        if crate::staging::has_retained_stage(&self.root)? {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retained retirement requires cleanup before rotation",
            ));
        }
        let slot = self.active.slot.other();
        match std::fs::symlink_metadata(slot.path(&self.root)) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "inactive generation slot is occupied",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let persisted_storage = self.store.header.storage_limits();
        // The successor base holds at most the current base and suffix.
        let bound = self.store.rebase_bound();
        let mut max_base_bytes = u64::MAX;
        if self.storage.is_some() {
            // The stage marker, the successor's metadata files and the base.
            let bytes = bound
                .checked_add(Stage::DIRECTORY_MARKER_BYTES)
                .and_then(|bytes| bytes.checked_add(64 * 1024))
                .ok_or_else(super::storage::byte_limit)?;
            self.admit_append(bytes)?;
            max_base_bytes = bound;
            self.rotation.reserved.store(bytes, Ordering::Release);
        }
        self.rotation.active.store(true, Ordering::Release);
        let state = Arc::clone(&self.rotation);
        let job = (|| {
            Ok::<_, io::Error>(RetainedRotationJob {
                frozen: self.store.freeze()?,
                source: self.active.generation,
                slot,
                limits,
                persisted_storage,
                role: generation_role(self.catalog_magic, self.active.catalog),
                max_base_bytes,
                prepared: None,
                stage: Some(Stage::directory(&self.root)?),
                state: Arc::clone(&state),
            })
        })();
        if job.is_err() {
            state.reserved.store(0, Ordering::Release);
            state.active.store(false, Ordering::Release);
        }
        job
    }

    // Replays the records committed since the capture into the prepared
    // successor, rechecks the live generation and publishes the successor
    // slot. The caller then publishes the selection or authority record.
    pub(super) fn publish_rotation_job(
        &mut self,
        mut job: RetainedRotationJob,
        tracker: &crate::publication_outcome::Tracker,
    ) -> io::Result<(Active, RetainedStore, Option<super::CatalogStorage>)> {
        if self.active.generation != job.source || self.active.slot.other() != job.slot {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the catalog rotated since this job was captured",
            ));
        }
        let (expected, mut next) = job.prepared.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "prepare the rotation first")
        })?;
        // The base is written; only the catch-up still allocates.
        job.state.reserved.store(0, Ordering::Release);
        let records = self.store.changes_since(&job.frozen)?;
        // Replay in commit order, merged into as few batches as the batch
        // limits allow, so catch-up costs about one sync. Nothing in the
        // successor is visible until it is published, so merging records
        // keeps every acknowledged write and its order.
        let changes = records
            .iter()
            .flat_map(|record| record.changes.iter())
            .map(|(key, value)| match value {
                Some(value) => BatchOperation::Put { key, value },
                None => BatchOperation::Delete { key },
            })
            .collect::<Vec<_>>();
        let mut chunks = Vec::new();
        let mut start = 0;
        let mut bytes = 0_usize;
        for (index, operation) in changes.iter().enumerate() {
            let (_, key, value) = operation.fields();
            let size = key.len() + value.len() + 16;
            if index > start
                && (index - start == crate::record::batch::MAX_ATOMIC_BATCH_OPERATIONS
                    || bytes + size > crate::record::batch::MAX_ATOMIC_BATCH_BYTES / 2)
            {
                chunks.push(&changes[start..index]);
                start = index;
                bytes = 0;
            }
            bytes += size;
        }
        if start < changes.len() {
            chunks.push(&changes[start..]);
        }
        // Admit the exact encoded catch-up before writing any of it.
        let encoded = chunks.iter().try_fold(0_u64, |total, chunk| {
            let length = crate::record::batch::BatchLayout::new(chunk)?.encoded_len();
            total
                .checked_add(length as u64)
                .ok_or_else(super::storage::byte_limit)
        })?;
        self.admit_append(encoded)?;
        for chunk in chunks {
            next.write_batch(chunk).map_err(|failure| match failure {
                MutationFailure::Rejected(error) | MutationFailure::OutcomeUnknown(error) => {
                    io::Error::new(
                        error.kind(),
                        format!("rotation catch-up failed before publication: {error}"),
                    )
                }
            })?;
        }
        self.store
            .verify_current(&self.active.slot.path(&self.root))?;
        let destination = job.slot.path(&self.root);
        let mut stage = job
            .stage
            .take()
            .ok_or_else(|| io::Error::other("rotation stage is missing"))?;
        tracker.begin_publication();
        stage.publish(&destination)?;
        sync_existing(&self.root)?;
        stage.publication_synchronized();
        next._lock.verify_identity(&destination)?;
        let generation = next.generation_id();
        let active = Active {
            catalog: self.active.catalog,
            slot: job.slot,
            generation,
            expected,
        };
        let next_limits = intersect(self.requested_storage, job.persisted_storage);
        let storage = catalog_storage(&self.root, self.catalog_magic, next_limits);
        Ok((active, next, storage))
    }

    /// Completes a prepared rotation. It replays every record committed
    /// since the capture into the successor, after admitting their exact
    /// bytes against the catalog quota, rechecks that the live generation is
    /// the captured one with unchanged files and witness, and publishes the
    /// successor slot and then the selection, as `compact` does. Writes are
    /// excluded only for this step. A rotation of another kind since the
    /// capture, or a missing preparation, refuses the job before publishing.
    pub fn complete_rotation(
        &mut self,
        job: RetainedRotationJob,
    ) -> Result<[u8; 16], RetainedRotationFailure> {
        crate::publication_outcome::track_retained_rotation(|tracker| {
            self.verify_selection()?;
            let (active, next, storage) = self.publish_rotation_job(job, &tracker.checkpoint)?;
            let generation = active.generation;
            let (mut selection, mut file) = Stage::file(&self.root)?;
            file.write_all(&active.encode())?;
            file.sync_all()?;
            drop(file);
            tracker.selection.begin_publication();
            self.uncertain = true;
            selection.replace_file(&self.root.join(ACTIVE_FILE))?;
            sync_existing(&self.root)?;
            selection.publication_synchronized();
            self.store = next;
            self.active = active;
            self.storage = storage;
            self.uncertain = false;
            Ok(generation)
        })
    }
}
