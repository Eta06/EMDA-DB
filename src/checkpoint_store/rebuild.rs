//! Explicit offline recovery of a managed checkpoint selection.
use super::rebuild_spill::{SpillLimits, SpillSession};
use super::{CheckpointStore, CheckpointView, selection};
use crate::log::Log;
use std::fs::{self, OpenOptions};
use std::io;
use std::ops::ControlFlow;
use std::path::Path;

#[cfg(all(test, target_os = "linux"))]
thread_local! {
    static BOUNDED_CHILD: std::cell::RefCell<Option<crate::test_support::fork_lock::Child>> = const { std::cell::RefCell::new(None) };
    static CAPTURE_BOUNDED_LOCK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct BoundedHooks<'a> {
    before_adopt: &'a mut dyn FnMut() -> io::Result<()>,
    sync_checkpoint_ancestors: &'a mut dyn FnMut(&Path) -> io::Result<()>,
    before_selection_rename: &'a mut dyn FnMut() -> io::Result<()>,
    sync_selection_parent: &'a mut dyn FnMut(&Path) -> io::Result<()>,
}

fn with_standard_hooks<T>(
    body: impl FnOnce(&mut BoundedHooks<'_>) -> io::Result<T>,
) -> io::Result<T> {
    let mut before_adopt = || Ok(());
    let mut sync_checkpoint = |parent: &Path| selection::sync_ancestors(parent);
    let mut before_selection = || Ok(());
    let mut sync_selection = |parent: &Path| selection::sync_ancestors(parent);
    let mut hooks = BoundedHooks {
        before_adopt: &mut before_adopt,
        sync_checkpoint_ancestors: &mut sync_checkpoint,
        before_selection_rename: &mut before_selection,
        sync_selection_parent: &mut sync_selection,
    };
    body(&mut hooks)
}

impl CheckpointStore {
    /// Rebuilds managed selection from full-log recovery while exclusively
    /// holding the existing store lock. This is useful when the old checkpoint
    /// is missing or its suffix exceeds managed limits. It loads the ordinary
    /// in-memory index and is not memory- or time-bounded. The store and
    /// selection directory must be trusted; malformed selection metadata is
    /// rejected. The new checkpoint path must not already exist.
    ///
    /// A selection-publication error has an uncertain outcome: reopen the
    /// visible selection, or use full-log recovery if none is visible. This
    /// method returns after releasing the store lock; callers must coordinate
    /// other writers before reopening. It does not reclaim log space.
    pub fn rebuild_managed_selection(
        path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> io::Result<[u8; 32]> {
        Self::rebuild_managed_with_hooks(
            path.as_ref(),
            destination.as_ref(),
            || Ok(()),
            selection::sync_ancestors,
        )
    }

    /// Rebuilds managed selection with separate conservative classifications
    /// for checkpoint and selection publication. This releases the store lock
    /// before returning, including on errors; coordinate other writers before
    /// inspecting or reopening either path. Full-log recovery may synchronize
    /// an unacknowledged tail even if no publication begins. No failure returns
    /// a new digest or certifies source health or cleanup.
    pub fn rebuild_managed_selection_detailed(
        path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<[u8; 32], crate::ManagedRebuildFailure> {
        crate::publication_outcome::track_managed_rebuild(|tracker| {
            Self::rebuild_managed_using(
                path.as_ref(),
                destination.as_ref(),
                |path| crate::Store::open(path),
                selection::sync_ancestors,
                || Ok(()),
                selection::sync_ancestors,
                Some(tracker),
            )
        })
    }

    /// Rebuilds using optional full-log and live-key limits during recovery.
    /// The log limit counts the whole file. Key count and key-byte limits
    /// apply to every replayed historical state, not just the current state.
    /// These are logical limits, not total process-memory limits, and they
    /// are not persisted for subsequent opens.
    pub fn rebuild_managed_selection_with_limits(
        path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
        max_live_key_bytes: Option<usize>,
    ) -> io::Result<[u8; 32]> {
        Self::rebuild_managed_using(
            path.as_ref(),
            destination.as_ref(),
            |path| {
                crate::Store::open_with_resource_limits(
                    path,
                    max_log_bytes,
                    max_live_keys,
                    max_live_key_bytes,
                )
            },
            selection::sync_ancestors,
            || Ok(()),
            selection::sync_ancestors,
            None,
        )
    }

    /// Rebuilds with full-log and historical live-key limits, and returns
    /// structured publication outcomes. The limits have the same semantics
    /// as `rebuild_managed_selection_with_limits` and are not persisted.
    pub fn rebuild_managed_selection_with_limits_detailed(
        path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
        max_live_key_bytes: Option<usize>,
    ) -> Result<[u8; 32], crate::ManagedRebuildFailure> {
        crate::publication_outcome::track_managed_rebuild(|tracker| {
            Self::rebuild_managed_using(
                path.as_ref(),
                destination.as_ref(),
                |path| {
                    crate::Store::open_with_resource_limits(
                        path,
                        max_log_bytes,
                        max_live_keys,
                        max_live_key_bytes,
                    )
                },
                selection::sync_ancestors,
                || Ok(()),
                selection::sync_ancestors,
                Some(tracker),
            )
        })
    }

    pub(super) fn rebuild_managed_with_hooks(
        path: &Path,
        destination: &Path,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        Self::rebuild_managed_with_sync_hooks(
            path,
            destination,
            selection::sync_ancestors,
            before_selection_rename,
            sync_selection_parent,
        )
    }

    pub(super) fn rebuild_managed_with_sync_hooks(
        path: &Path,
        destination: &Path,
        sync_checkpoint_ancestors: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<[u8; 32]> {
        Self::rebuild_managed_using(
            path,
            destination,
            |path| crate::Store::open(path),
            sync_checkpoint_ancestors,
            before_selection_rename,
            sync_selection_parent,
            None,
        )
    }

    pub(super) fn rebuild_managed_using(
        path: &Path,
        destination: &Path,
        open_store: impl FnOnce(&Path) -> io::Result<crate::Store>,
        sync_checkpoint_ancestors: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
        tracker: Option<&crate::publication_outcome::ManagedTracker>,
    ) -> io::Result<[u8; 32]> {
        let path = fs::canonicalize(path)?;
        // This is recovery, never creation or initialization of a new store.
        for name in ["store.lock", "records.log"] {
            if !fs::metadata(path.join(name))?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "existing store file is not regular",
                ));
            }
        }
        let parent = crate::backup::parent_directory(destination)?;
        let destination = fs::canonicalize(parent)?.join(
            destination
                .file_name()
                .expect("parent_directory validates the name"),
        );
        if destination == selection::path(&path)
            || destination == super::automatic::root(&path)
            || destination.starts_with(super::automatic::root(&path))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint destination uses the reserved managed checkpoint namespace",
            ));
        }
        selection::Selection::new(destination.clone(), [0; 32])?;
        let store = open_store(&path)?;
        // Refuse to overwrite unknown, damaged, symlink or non-file metadata.
        // A valid but stale selection need not have an accessible checkpoint.
        selection::load(&path)?;
        let digest = if let Some(tracker) = tracker {
            match store.checkpoint_to_detailed_with_sync(&destination, sync_checkpoint_ancestors) {
                Ok(digest) => {
                    tracker.checkpoint.begin_publication();
                    digest
                }
                Err(crate::PublicationFailure::NotPublished(error)) => return Err(error),
                Err(crate::PublicationFailure::OutcomeUnknown(error)) => {
                    tracker.checkpoint.begin_publication();
                    return Err(error);
                }
            }
        } else {
            store.checkpoint_to_with_sync(&destination, sync_checkpoint_ancestors)?
        };
        let lock = store.share_store_lock();
        let view =
            CheckpointView::open_locked(&path, &destination, &digest, lock).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("checkpoint was published but managed verification failed: {error}"),
                )
            })?;
        if view.has_log_tail() {
            return Err(crate::record::invalid_data(
                "rebuilt checkpoint has an unexpected suffix",
            ));
        }
        drop(view);
        let next = selection::Selection::new(destination, digest)?;
        if let Some(tracker) = tracker {
            tracker.begin_selection();
        }
        selection::publish(&path, &next, before_selection_rename, sync_selection_parent)
            .map_err(|error| io::Error::new(error.kind(), format!("managed selection publication may have occurred; recovery required: {error}")))?;
        Ok(digest)
    }

    /// Rebuilds a managed selection from a trusted base checkpoint and the
    /// witnessed suffix without loading that suffix into an unbounded index.
    ///
    /// The base file must be a regular, non-symlink checkpoint whose digest
    /// matches `trusted_digest`. Its covered log prefix is hashed before any
    /// suffix state is trusted, again before an unacknowledged tail can be
    /// adopted, and again immediately before the new checkpoint is published.
    /// Adoption and publication also check the exact suffix bytes seen during
    /// validation, including records that no longer contribute live values.
    /// A missing, unreadable, or digest-mismatched base fails before
    /// publication. This does not fall back to an in-memory full-log rebuild.
    ///
    /// Suffix records are applied in log order. Every DB02 child is applied
    /// before the next child, even when those children share one outer record.
    /// The historical peak is the base checkpoint's key-count peak extended by
    /// every intermediate live-key count. EMDAIDX1 does not store a key-byte
    /// peak, and this method does not invent one.
    ///
    /// `max_chunk_keys` and `max_chunk_key_bytes` bound the distinct suffix
    /// keys retained before a spill flush (1..=65,536 keys and 1..=64 MiB of
    /// logical key bytes). `max_spill_bytes` bounds spill files while merge
    /// inputs and the offset sidecar still exist, not only the finished runs;
    /// the fixed staging marker and filesystem allocation overhead are excluded.
    /// `max_merge_fan_in` is the maximum number of run inputs opened for one
    /// merge (2..=32); the output and its offset file use two further
    /// descriptors. `max_operations` bounds the number of suffix child
    /// operations applied. Exceeding any limit fails before publication.
    ///
    /// A complete unacknowledged tail is validated, including that the old
    /// witness boundary falls on a record boundary, before the writer advances
    /// the witness. A failure there is not a checkpoint publication, but the
    /// witness may still need inspection. The exclusive store lock is held
    /// until selection publication and its directory synchronization return.
    /// Checkpoint and selection outcomes use the same classification as
    /// `rebuild_managed_selection_detailed` when the detailed method is used.
    ///
    /// Spill bytes live in a private staging directory under the store and are
    /// cleaned up on return on a best-effort basis. A crash or cleanup error can leave a marked stage for
    /// `cleanup_abandoned_staging`. These limits are not a total process-memory
    /// or elapsed-time bound: validating one DB02 record can retain its
    /// existing maximum of 1,024 child keys, and the base view retains its
    /// bounded block directory. Existing rebuild methods are unchanged.
    #[allow(clippy::too_many_arguments)]
    pub fn rebuild_managed_selection_bounded(
        path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        base_checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_chunk_keys: usize,
        max_chunk_key_bytes: usize,
        max_spill_bytes: u64,
        max_merge_fan_in: usize,
        max_operations: u64,
    ) -> io::Result<[u8; 32]> {
        with_standard_hooks(|hooks| {
            Self::rebuild_bounded_using(
                path.as_ref(),
                destination.as_ref(),
                base_checkpoint.as_ref(),
                trusted_digest,
                SpillLimits::new(
                    max_chunk_keys,
                    max_chunk_key_bytes,
                    max_spill_bytes,
                    max_merge_fan_in,
                    max_operations,
                )?,
                None,
                hooks,
            )
        })
    }

    /// Rebuilds from a trusted base with bounded suffix spill and classified
    /// publication failures. The limits and recovery rules are the same as
    /// `rebuild_managed_selection_bounded`. No failure returns a digest.
    #[allow(clippy::too_many_arguments)]
    pub fn rebuild_managed_selection_bounded_detailed(
        path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        base_checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_chunk_keys: usize,
        max_chunk_key_bytes: usize,
        max_spill_bytes: u64,
        max_merge_fan_in: usize,
        max_operations: u64,
    ) -> Result<[u8; 32], crate::ManagedRebuildFailure> {
        crate::publication_outcome::track_managed_rebuild(|tracker| {
            with_standard_hooks(|hooks| {
                Self::rebuild_bounded_using(
                    path.as_ref(),
                    destination.as_ref(),
                    base_checkpoint.as_ref(),
                    trusted_digest,
                    SpillLimits::new(
                        max_chunk_keys,
                        max_chunk_key_bytes,
                        max_spill_bytes,
                        max_merge_fan_in,
                        max_operations,
                    )?,
                    Some(tracker),
                    hooks,
                )
            })
        })
    }

    /// Like `rebuild_managed_selection_bounded_detailed`, but rejects a whole
    /// log larger than `max_log_bytes` under the exclusive store lock, before
    /// checkpoint validation or tail adoption. This is an admission limit, not
    /// a bound on repeated verification I/O, elapsed time, or total disk usage.
    #[allow(clippy::too_many_arguments)]
    pub fn rebuild_managed_selection_bounded_with_log_limit_detailed(
        path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        base_checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        max_chunk_keys: usize,
        max_chunk_key_bytes: usize,
        max_spill_bytes: u64,
        max_merge_fan_in: usize,
        max_operations: u64,
        max_log_bytes: u64,
    ) -> Result<[u8; 32], crate::ManagedRebuildFailure> {
        crate::publication_outcome::track_managed_rebuild(|tracker| {
            with_standard_hooks(|hooks| {
                let mut limits = SpillLimits::new(
                    max_chunk_keys,
                    max_chunk_key_bytes,
                    max_spill_bytes,
                    max_merge_fan_in,
                    max_operations,
                )?;
                limits.max_log_bytes = Some(max_log_bytes);
                Self::rebuild_bounded_using(
                    path.as_ref(),
                    destination.as_ref(),
                    base_checkpoint.as_ref(),
                    trusted_digest,
                    limits,
                    Some(tracker),
                    hooks,
                )
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn rebuild_bounded_using(
        path: &Path,
        destination: &Path,
        base_checkpoint: &Path,
        trusted_digest: &[u8; 32],
        limits: SpillLimits,
        tracker: Option<&crate::publication_outcome::ManagedTracker>,
        hooks: &mut BoundedHooks<'_>,
    ) -> io::Result<[u8; 32]> {
        let path = fs::canonicalize(path)?;
        for name in ["store.lock", "records.log"] {
            if !fs::metadata(path.join(name))?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "existing store file is not regular",
                ));
            }
        }
        let base_meta = fs::symlink_metadata(base_checkpoint)?;
        if !base_meta.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "base checkpoint must be a regular non-symlink file",
            ));
        }
        let base_checkpoint = fs::canonicalize(base_checkpoint)?;
        let parent = crate::backup::parent_directory(destination)?;
        let destination = fs::canonicalize(parent)?.join(
            destination
                .file_name()
                .expect("parent_directory validates the name"),
        );
        if destination == base_checkpoint {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint destination overwrites the trusted base",
            ));
        }
        if destination == selection::path(&path)
            || destination == super::automatic::root(&path)
            || destination.starts_with(super::automatic::root(&path))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint destination uses the reserved managed checkpoint namespace",
            ));
        }
        selection::Selection::new(destination.clone(), [0; 32])?;
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("store.lock"))?;
        let lock_result = fs2::FileExt::try_lock_exclusive(&lock_file);
        #[cfg(test)]
        let lock_result = crate::directory::diagnose_lock_failure(
            lock_result,
            &path.join("store.lock"),
            &lock_file,
            "bounded rebuild store exclusive",
        );
        lock_result?;
        let lock_file = crate::store_lock::StoreLock::acquired(lock_file);
        lock_file.verify_identity(&path)?;
        #[cfg(all(test, target_os = "linux"))]
        CAPTURE_BOUNDED_LOCK.with(|capture| {
            if capture.get() {
                BOUNDED_CHILD.with(|slot| {
                    *slot.borrow_mut() = Some(crate::test_support::fork_lock::Child::spawn())
                });
            }
        });
        if let Some(max) = limits.max_log_bytes {
            if fs::metadata(path.join("records.log"))?.len() > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing log exceeds configured limit",
                ));
            }
        }
        // Malformed selection metadata is rejected before the base is trusted.
        // A valid selection may name a missing checkpoint; the caller-supplied
        // base, not that selection, is the trusted input.
        selection::load(&path)?;
        let view_lock = lock_file.clone();
        let view = CheckpointView::open_locked(&path, &base_checkpoint, trusted_digest, view_lock)?;
        let witness = view.witnessed_boundary;
        let prefix = view.log_boundary();
        let length = view.log.len()?;
        if prefix > witness || witness > length {
            return Err(crate::record::invalid_data(
                "checkpoint boundary is outside the witnessed log",
            ));
        }
        let base_live = usize::try_from(view.len()).map_err(|_| {
            crate::record::invalid_data("checkpoint key count does not fit this platform")
        })?;
        let writer = Log::open_checkpoint_writer(&path, witness, length)?;
        if writer.len()? != length {
            return Err(crate::record::invalid_data(
                "log length changed while opening its witness",
            ));
        }
        let mut spill = SpillSession::create(&path, limits, base_live, view.historical_peak())?;
        // `visit_records` yields DB02 children only after the outer record is
        // valid, and it yields them in order. Each child is applied before
        // the next one; their value offsets are not collapsed to the outer
        // record offset.
        let suffix_digest =
            writer.visit_observed_suffix(prefix, length, witness, |key, location| {
                spill.push(&key, location, |key| view.location(key))?;
                Ok(())
            })?;
        if writer.len()? != length {
            return Err(crate::record::invalid_data(
                "log length changed during suffix replay",
            ));
        }
        spill.prepare_for_emit()?;
        if length > witness {
            (hooks.before_adopt)()?;
        }
        let base = view.verify_checkpoint_file()?;
        let verified_log = crate::checkpoint::bind_replayed_log(
            &writer,
            prefix,
            length,
            &base.log_digest,
            &suffix_digest,
        )?;
        if length > witness {
            writer.sync_validated_boundary(length).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "unacknowledged tail adoption may have started; inspect the witness before retrying: {error}"
                    ),
                )
            })?;
        }
        if writer.len()? != length {
            return Err(crate::record::invalid_data(
                "log length changed while adopting its suffix; the witness may have advanced to the validated boundary",
            ));
        }
        writer.verify_witness()?;
        // The staged checkpoint verifies the whole source against verified_log
        // before publication. Recheck the base index before using its entries.
        view.verify_checkpoint_file()?;
        let count = u64::try_from(spill.live_count())
            .map_err(|_| io::Error::other("live key count overflow"))?;
        let peak = spill.peak();
        if peak < spill.live_count() {
            return Err(crate::record::invalid_data(
                "reconstructed peak is below the live key count",
            ));
        }
        let digest = crate::checkpoint::create_streaming_tracked(
            &writer,
            count,
            peak,
            &destination,
            |emit| {
                let emitted = emit_merged(&view, &mut spill, emit)?;
                if emitted != count {
                    return Err(crate::record::invalid_data(
                        "rebuilt checkpoint count does not match suffix replay",
                    ));
                }
                Ok(())
            },
            |parent| (hooks.sync_checkpoint_ancestors)(parent),
            crate::checkpoint::StreamingOptions {
                tracker: tracker.map(|tracker| &tracker.checkpoint),
                verified_log,
                limits: None,
            },
        )?;
        drop(spill);
        let published_lock = lock_file.clone();
        let published = CheckpointView::open_locked(&path, &destination, &digest, published_lock)
            .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("checkpoint was published but managed verification failed: {error}"),
            )
        })?;
        if published.has_log_tail() {
            return Err(crate::record::invalid_data(
                "rebuilt checkpoint has an unexpected suffix",
            ));
        }
        drop(published);
        drop(view);
        drop(writer);
        let next = selection::Selection::new(destination, digest)?;
        if let Some(tracker) = tracker {
            tracker.begin_selection();
        }
        selection::publish(
            &path,
            &next,
            || (hooks.before_selection_rename)(),
            |parent| (hooks.sync_selection_parent)(parent),
        )
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "managed selection publication may have occurred; recovery required: {error}"
                ),
            )
        })?;
        drop(lock_file);
        Ok(digest)
    }
}

fn emit_merged(
    view: &CheckpointView,
    spill: &mut SpillSession,
    mut emit: impl FnMut(&[u8], crate::log::ValueLocation) -> io::Result<()>,
) -> io::Result<u64> {
    let mut emitted = 0_u64;
    let mut spill_next = spill.next_latest()?;
    view.visit_entries(None, None, |key, location| {
        while spill_next
            .as_ref()
            .is_some_and(|(spill_key, _)| spill_key.as_slice() < key)
        {
            let (spill_key, state) = spill_next.take().expect("peeked spill entry");
            if let Some(location) = state {
                emit(&spill_key, location)?;
                emitted = emitted
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("index count overflow"))?;
            }
            spill_next = spill.next_latest()?;
        }
        if spill_next
            .as_ref()
            .is_some_and(|(spill_key, _)| spill_key.as_slice() == key)
        {
            let (_, state) = spill_next.take().expect("matched spill entry");
            if let Some(location) = state {
                emit(key, location)?;
                emitted = emitted
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("index count overflow"))?;
            }
            spill_next = spill.next_latest()?;
        } else {
            emit(key, location)?;
            emitted = emitted
                .checked_add(1)
                .ok_or_else(|| io::Error::other("index count overflow"))?;
        }
        Ok(ControlFlow::Continue(()))
    })?;
    while let Some((key, state)) = spill_next {
        if let Some(location) = state {
            emit(&key, location)?;
            emitted = emitted
                .checked_add(1)
                .ok_or_else(|| io::Error::other("index count overflow"))?;
        }
        spill_next = spill.next_latest()?;
    }
    Ok(emitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publication_outcome::track_managed_rebuild;
    use crate::{ManagedRebuildFailure, Store};
    use std::io::{BufRead, Write};
    use std::os::unix::fs::FileExt;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    fn small_limits() -> SpillLimits {
        SpillLimits::new(1, 4096, 32 << 20, 2, 10_000).unwrap()
    }

    fn witness_bytes(path: &Path) -> Vec<u8> {
        let log = fs::read(path.join("records.log")).unwrap();
        if log.starts_with(b"EMDALOG3") {
            let file = fs::File::open(path.join("records.log")).unwrap();
            let mut buffer = vec![0_u8; 128];
            let count = rustix::fs::fgetxattr(&file, "user.db.commit.v3", &mut buffer).unwrap();
            buffer[..count].to_vec()
        } else {
            fs::read(path.join("commit.state")).unwrap()
        }
    }

    fn restore_witness(path: &Path, bytes: &[u8]) {
        let log = fs::read(path.join("records.log")).unwrap();
        if log.starts_with(b"EMDALOG3") {
            let file = OpenOptions::new()
                .write(true)
                .open(path.join("records.log"))
                .unwrap();
            rustix::fs::fsetxattr(
                &file,
                "user.db.commit.v3",
                bytes,
                rustix::fs::XattrFlags::REPLACE,
            )
            .unwrap();
            file.sync_all().unwrap();
        } else {
            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path.join("commit.state"))
                .unwrap();
            file.write_all(bytes).unwrap();
            file.sync_all().unwrap();
        }
    }

    fn log_id(path: &Path) -> [u8; 16] {
        let log = fs::read(path.join("records.log")).unwrap();
        let header = log[..crate::witness::LOG_HEADER_SIZE].try_into().unwrap();
        if log.starts_with(b"EMDALOG3") {
            crate::metadata_witness::parse_header(&header).unwrap()
        } else {
            crate::witness::parse_header(&header).unwrap()
        }
    }

    fn boundary_of(bytes: &[u8], id: &[u8; 16]) -> u64 {
        let bytes: &[u8; crate::witness::WITNESS_SIZE] = bytes.try_into().unwrap();
        crate::witness::decode(bytes, id).unwrap()
    }

    fn publish_bounded(
        path: &Path,
        destination: &Path,
        base: &Path,
        digest: &[u8; 32],
    ) -> Result<[u8; 32], ManagedRebuildFailure> {
        let limits = small_limits();
        CheckpointStore::rebuild_managed_selection_bounded_detailed(
            path,
            destination,
            base,
            digest,
            limits.max_chunk_keys,
            limits.max_chunk_key_bytes,
            limits.max_spill_bytes,
            limits.max_merge_fan_in,
            limits.max_operations,
        )
    }

    fn retry<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
        let mut last = None;
        for _ in 0..100 {
            match operation() {
                Ok(value) => return Ok(value),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    last = Some(error);
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(error) => return Err(error),
            }
        }
        Err(last.expect("retry observed a lock conflict"))
    }

    #[test]
    fn bounded_store_lock_failure_diagnostic() {
        use std::os::unix::fs::MetadataExt;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let base = root.path().join("base");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&base).unwrap();
        let lock_path = path.join("store.lock");
        let metadata = fs::metadata(&lock_path).unwrap();
        let result = publish_bounded(&path, &root.path().join("rebuilt"), &base, &digest);
        assert!(
            matches!(result, Err(ManagedRebuildFailure::NotPublished(ref e)) if e.kind() == io::ErrorKind::WouldBlock)
        );
        assert_eq!(
            crate::directory::take_lock_diagnostic(),
            Some((
                fs::canonicalize(&lock_path).unwrap(),
                "bounded rebuild store exclusive".into(),
                Some((metadata.dev(), metadata.ino()))
            ))
        );
    }

    #[test]
    fn bounded_rebuild_log_limit_rejects_before_publication_and_accepts_exact_size() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let base = directory.path().join("base");
        let destination = directory.path().join("rebuilt");
        let store = Store::open(&path).unwrap();
        store.put(b"base", b"value").unwrap();
        let digest = store.checkpoint_to(&base).unwrap();
        store.put(b"tail", b"value").unwrap();
        drop(store);
        let log = fs::read(path.join("records.log")).unwrap();
        let witness = witness_bytes(&path);
        let run = |cap| {
            CheckpointStore::rebuild_managed_selection_bounded_with_log_limit_detailed(
                &path,
                &destination,
                &base,
                &digest,
                1,
                4096,
                32 << 20,
                2,
                10_000,
                cap,
            )
        };
        assert!(matches!(
            run(log.len() as u64 - 1),
            Err(ManagedRebuildFailure::NotPublished(_))
        ));
        assert!(!destination.exists());
        assert!(!selection::path(&path).exists());
        assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
        assert_eq!(witness_bytes(&path), witness);
        run(log.len() as u64).unwrap();
        let reopened = CheckpointStore::open_managed(&path).unwrap();
        assert_eq!(reopened.get(b"base").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.get(b"tail").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn log_limited_rebuild_still_rejects_corrupt_superseded_history() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let base = directory.path().join("base");
        let destination = directory.path().join("rebuilt");
        let store = Store::open(&path).unwrap();
        store.put(b"key", b"old-unique-value").unwrap();
        store.put(b"key", b"new").unwrap();
        let digest = store.checkpoint_to(&base).unwrap();
        drop(store);
        let log_path = path.join("records.log");
        let mut log = fs::read(&log_path).unwrap();
        let offset = log
            .windows(b"old-unique-value".len())
            .position(|w| w == b"old-unique-value")
            .unwrap();
        log[offset] ^= 1;
        fs::write(&log_path, &log).unwrap();
        let result = CheckpointStore::rebuild_managed_selection_bounded_with_log_limit_detailed(
            &path,
            &destination,
            &base,
            &digest,
            1,
            4096,
            32 << 20,
            2,
            10_000,
            log.len() as u64,
        );
        assert!(matches!(
            result,
            Err(ManagedRebuildFailure::NotPublished(_))
        ));
        assert!(!destination.exists());
        assert_eq!(fs::read(log_path).unwrap(), log);
    }

    #[test]
    fn bounded_rebuild_classifies_publication_and_keeps_the_lock_through_selection() {
        bounded_rebuild_fixture(false);
    }

    fn bounded_rebuild_fixture(
        #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] fork: bool,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let base = directory.path().join("base");
        let store = Store::open(&path).unwrap();
        store.put(b"safe", b"acked").unwrap();
        let digest = store.checkpoint_to(&base).unwrap();
        #[cfg(target_os = "linux")]
        let child = fork.then(crate::test_support::fork_lock::Child::spawn);
        drop(store);
        #[cfg(target_os = "linux")]
        if let Some(child) = child {
            crate::test_support::fork_lock::check_reopened(&path, child);
        }
        let store = Store::open(&path).unwrap();
        store.put(b"tail", b"one").unwrap();
        store.put(b"tail", b"two").unwrap();
        #[cfg(target_os = "linux")]
        let child = fork.then(crate::test_support::fork_lock::Child::spawn);
        drop(store);
        #[cfg(target_os = "linux")]
        if let Some(child) = child {
            crate::test_support::fork_lock::check_reopened(&path, child);
            drop(Store::open(&path).unwrap());
        }
        let destination = directory.path().join("published");
        let failure = track_managed_rebuild(|tracker| {
            let mut before_adopt = || Ok(());
            let mut sync_checkpoint =
                |_parent: &Path| Err(io::Error::other("injected checkpoint sync"));
            let mut before_selection = || Ok(());
            let mut sync_selection = |parent: &Path| selection::sync_ancestors(parent);
            let mut hooks = BoundedHooks {
                before_adopt: &mut before_adopt,
                sync_checkpoint_ancestors: &mut sync_checkpoint,
                before_selection_rename: &mut before_selection,
                sync_selection_parent: &mut sync_selection,
            };
            CheckpointStore::rebuild_bounded_using(
                &path,
                &destination,
                &base,
                &digest,
                small_limits(),
                Some(tracker),
                &mut hooks,
            )
        })
        .unwrap_err();
        assert!(
            matches!(failure, ManagedRebuildFailure::CheckpointOutcomeUnknown(_)),
            "checkpoint sync hook was not classified as uncertain: {failure:?}"
        );
        assert!(destination.is_file());
        assert!(!selection::path(&path).exists());

        let replacement = directory.path().join("next");
        let mut saw_lock = false;
        let failure = track_managed_rebuild(|tracker| {
            let mut before_adopt = || Ok(());
            let mut sync_checkpoint = |parent: &Path| selection::sync_ancestors(parent);
            let mut before_selection = || {
                let error = match Store::open(&path) {
                    Ok(_) => panic!("store lock was not held through selection"),
                    Err(error) => error,
                };
                assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
                saw_lock = true;
                Err(io::Error::other("injected before selection rename"))
            };
            let mut sync_selection = |parent: &Path| selection::sync_ancestors(parent);
            let mut hooks = BoundedHooks {
                before_adopt: &mut before_adopt,
                sync_checkpoint_ancestors: &mut sync_checkpoint,
                before_selection_rename: &mut before_selection,
                sync_selection_parent: &mut sync_selection,
            };
            CheckpointStore::rebuild_bounded_using(
                &path,
                &replacement,
                &base,
                &digest,
                small_limits(),
                Some(tracker),
                &mut hooks,
            )
        })
        .unwrap_err();
        if let ManagedRebuildFailure::NotPublished(error) = &failure {
            crate::test_support::report_lock_error(&path, "bounded second rebuild", error);
        }
        assert!(saw_lock, "selection hook was not reached: {failure:?}");
        assert!(matches!(
            failure,
            ManagedRebuildFailure::SelectionOutcomeUnknown(_)
        ));
        assert!(replacement.is_file());
        assert!(!selection::path(&path).exists());

        let adopted = directory.path().join("adopted");
        let witness = witness_bytes(&path);
        let store = Store::open(&path).unwrap();
        store.put(b"pending", b"complete").unwrap();
        drop(store);
        let complete_log = fs::read(path.join("records.log")).unwrap();
        restore_witness(&path, &witness);
        let failure = track_managed_rebuild(|tracker| {
            let mut before_adopt = || Err(io::Error::other("injected before adoption"));
            let mut sync_checkpoint = |parent: &Path| selection::sync_ancestors(parent);
            let mut before_selection = || Ok(());
            let mut sync_selection = |parent: &Path| selection::sync_ancestors(parent);
            let mut hooks = BoundedHooks {
                before_adopt: &mut before_adopt,
                sync_checkpoint_ancestors: &mut sync_checkpoint,
                before_selection_rename: &mut before_selection,
                sync_selection_parent: &mut sync_selection,
            };
            CheckpointStore::rebuild_bounded_using(
                &path,
                &adopted,
                &base,
                &digest,
                small_limits(),
                Some(tracker),
                &mut hooks,
            )
        })
        .unwrap_err();
        assert!(matches!(failure, ManagedRebuildFailure::NotPublished(_)));
        assert!(!adopted.exists());
        assert_eq!(witness_bytes(&path), witness);
        assert_eq!(fs::read(path.join("records.log")).unwrap(), complete_log);

        let failure = track_managed_rebuild(|tracker| {
            let mut before_adopt = || {
                OpenOptions::new()
                    .append(true)
                    .open(path.join("records.log"))?
                    .write_all(&[0])
            };
            let mut sync_checkpoint = |parent: &Path| selection::sync_ancestors(parent);
            let mut before_selection = || Ok(());
            let mut sync_selection = |parent: &Path| selection::sync_ancestors(parent);
            let mut hooks = BoundedHooks {
                before_adopt: &mut before_adopt,
                sync_checkpoint_ancestors: &mut sync_checkpoint,
                before_selection_rename: &mut before_selection,
                sync_selection_parent: &mut sync_selection,
            };
            CheckpointStore::rebuild_bounded_using(
                &path,
                &adopted,
                &base,
                &digest,
                small_limits(),
                Some(tracker),
                &mut hooks,
            )
        })
        .unwrap_err();
        assert!(matches!(failure, ManagedRebuildFailure::NotPublished(_)));
        assert_eq!(witness_bytes(&path), witness);
        assert!(!adopted.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_rebuild_fixture_fork_lock_lifetime() {
        if crate::test_support::fork_lock::isolated(
            "checkpoint_store::rebuild::tests::bounded_rebuild_fixture_fork_lock_lifetime",
            "EMDA_BOUNDED_FORK_LIFETIME",
        ) {
            return;
        }
        bounded_rebuild_fixture(true);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_acquisition_fork_outcomes() {
        if crate::test_support::fork_lock::isolated(
            "checkpoint_store::rebuild::tests::bounded_acquisition_fork_outcomes",
            "EMDA_BOUNDED_ACQUISITION_FORK",
        ) {
            return;
        }
        for mode in ["success", "cap", "checkpoint", "selection", "panic"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store");
            let base = dir.path().join("base");
            let destination = dir.path().join("published");
            let store = Store::open(&path).unwrap();
            store.put(b"key", b"value").unwrap();
            let digest = store.checkpoint_to(&base).unwrap();
            drop(store);
            CAPTURE_BOUNDED_LOCK.with(|v| v.set(true));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                track_managed_rebuild(|tracker| {
                    let mut before_adopt = || Ok(());
                    let mut checkpoint = |p: &Path| {
                        if mode == "checkpoint" {
                            Err(io::Error::other("sync"))
                        } else {
                            selection::sync_ancestors(p)
                        }
                    };
                    let mut selection_hook = || {
                        if mode == "panic" {
                            panic!("injected panic");
                        }
                        if mode == "selection" {
                            Err(io::Error::other("selection"))
                        } else {
                            Ok(())
                        }
                    };
                    let mut sync_selection = |p: &Path| selection::sync_ancestors(p);
                    let mut hooks = BoundedHooks {
                        before_adopt: &mut before_adopt,
                        sync_checkpoint_ancestors: &mut checkpoint,
                        before_selection_rename: &mut selection_hook,
                        sync_selection_parent: &mut sync_selection,
                    };
                    let mut limits = small_limits();
                    if mode == "cap" {
                        limits.max_log_bytes = Some(0);
                    }
                    CheckpointStore::rebuild_bounded_using(
                        &path,
                        &destination,
                        &base,
                        &digest,
                        limits,
                        Some(tracker),
                        &mut hooks,
                    )
                })
            }));
            CAPTURE_BOUNDED_LOCK.with(|v| v.set(false));
            match mode {
                "success" => {
                    assert!(result.unwrap().is_ok());
                    assert!(destination.is_file());
                    assert!(selection::path(&path).exists());
                }
                "cap" => {
                    assert!(matches!(
                        result.unwrap(),
                        Err(ManagedRebuildFailure::NotPublished(_))
                    ));
                    assert!(!destination.exists());
                }
                "checkpoint" => {
                    assert!(matches!(
                        result.unwrap(),
                        Err(ManagedRebuildFailure::CheckpointOutcomeUnknown(_))
                    ));
                    assert!(destination.is_file());
                    assert!(!selection::path(&path).exists());
                }
                "selection" => {
                    assert!(matches!(
                        result.unwrap(),
                        Err(ManagedRebuildFailure::SelectionOutcomeUnknown(_))
                    ));
                    assert!(destination.is_file());
                    assert!(!selection::path(&path).exists());
                }
                "panic" => assert!(result.is_err()),
                _ => unreachable!(),
            }
            let child = BOUNDED_CHILD.with(|v| v.borrow_mut().take().unwrap());
            crate::test_support::fork_lock::check_reopened(&path, child);
            drop(Store::open(&path).unwrap());
        }
    }

    struct AdoptionCase {
        _directory: tempfile::TempDir,
        v2: bool,
        path: PathBuf,
        base: PathBuf,
        digest: [u8; 32],
        metadata: bool,
        id: [u8; 16],
        rolled_back: Vec<u8>,
        complete_log: Vec<u8>,
        anchor: Vec<u8>,
        base_bytes: Vec<u8>,
    }

    impl AdoptionCase {
        fn root(&self) -> &Path {
            self._directory.path()
        }
    }

    fn prepare_unacknowledged_tail(v2: bool) -> Option<AdoptionCase> {
        if !v2 && !cfg!(target_os = "linux") {
            return None;
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let base = directory.path().join("base");
        if v2 {
            fs::create_dir(&path).unwrap();
            fs::write(
                path.join("records.log"),
                include_bytes!("../../tests/fixtures/v2-empty-records.log"),
            )
            .unwrap();
            fs::write(
                path.join("commit.state"),
                include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
            )
            .unwrap();
        }
        let store = Store::open(&path).unwrap();
        let metadata = fs::read(path.join("records.log"))
            .unwrap()
            .starts_with(b"EMDALOG3");
        if v2 {
            assert!(!metadata, "v2 fixture opened as V3");
        } else if !metadata {
            return None;
        }
        store.put(b"kept", b"base").unwrap();
        store.put(b"safe", b"acked").unwrap();
        let digest = store.checkpoint_to(&base).unwrap();
        drop(store);

        let rolled_back = witness_bytes(&path);
        let store = Store::open(&path).unwrap();
        store.put(b"p", b"one").unwrap();
        store
            .write_batch(&[
                crate::BatchOperation::Put {
                    key: b"q",
                    value: b"batch",
                },
                crate::BatchOperation::Delete { key: b"safe" },
                crate::BatchOperation::Put {
                    key: b"p",
                    value: b"two",
                },
            ])
            .unwrap();
        store.put(b"r", b"after").unwrap();
        drop(store);
        restore_witness(&path, &rolled_back);

        let complete_log = fs::read(path.join("records.log")).unwrap();
        assert_eq!(
            &complete_log[..8],
            if metadata { b"EMDALOG3" } else { b"EMDALOG2" },
            "v2={v2}"
        );
        let id = log_id(&path);
        assert!(
            boundary_of(&rolled_back, &id) < complete_log.len() as u64,
            "v2={v2}"
        );
        let anchor = fs::read(path.join("commit.state")).unwrap();
        if metadata {
            assert_eq!(
                boundary_of(&anchor, &id),
                crate::witness::LOG_HEADER_SIZE as u64,
                "v2={v2}"
            );
        } else {
            assert_eq!(anchor, rolled_back, "v2={v2}");
        }
        let base_bytes = fs::read(&base).unwrap();
        Some(AdoptionCase {
            _directory: directory,
            v2,
            path,
            base,
            digest,
            metadata,
            id,
            rolled_back,
            complete_log,
            anchor,
            base_bytes,
        })
    }

    fn rebuild_after_substituting_witness(
        case: &AdoptionCase,
        destination: &Path,
    ) -> ManagedRebuildFailure {
        let mut substituted = false;
        let failure = track_managed_rebuild(|tracker| {
            let mut before_adopt = || {
                assert!(!substituted, "v2={}", case.v2);
                substituted = true;
                let length = fs::metadata(case.path.join("records.log"))?.len();
                let installed = crate::witness::encode_for_test(&case.id, length + 1);
                if case.metadata {
                    let file = OpenOptions::new()
                        .write(true)
                        .open(case.path.join("records.log"))?;
                    rustix::fs::fsetxattr(
                        &file,
                        "user.db.commit.v3",
                        &installed,
                        rustix::fs::XattrFlags::REPLACE,
                    )?;
                    file.sync_all()?;
                } else {
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(case.path.join("commit.state"))?;
                    file.write_all_at(&installed, 0)?;
                    file.sync_all()?;
                }
                Ok(())
            };
            let mut sync_checkpoint = |parent: &Path| selection::sync_ancestors(parent);
            let mut before_selection = || Ok(());
            let mut sync_selection = |parent: &Path| selection::sync_ancestors(parent);
            let mut hooks = BoundedHooks {
                before_adopt: &mut before_adopt,
                sync_checkpoint_ancestors: &mut sync_checkpoint,
                before_selection_rename: &mut before_selection,
                sync_selection_parent: &mut sync_selection,
            };
            CheckpointStore::rebuild_bounded_using(
                &case.path,
                destination,
                &case.base,
                &case.digest,
                small_limits(),
                Some(tracker),
                &mut hooks,
            )
        })
        .unwrap_err();
        if let ManagedRebuildFailure::NotPublished(error) = &failure {
            crate::test_support::report_lock_error(
                &case.path,
                "substituted witness rebuild",
                error,
            );
        }
        assert!(substituted, "v2={} failure={failure:?}", case.v2);
        failure
    }

    fn assert_source_unchanged(case: &AdoptionCase) {
        assert_eq!(
            fs::read(case.path.join("records.log")).unwrap(),
            case.complete_log,
            "v2={}",
            case.v2
        );
        assert_eq!(
            fs::read(&case.base).unwrap(),
            case.base_bytes,
            "v2={}",
            case.v2
        );
    }

    fn assert_conflict_preserved(case: &AdoptionCase, illegal: &[u8]) {
        let on_disk = witness_bytes(&case.path);
        assert_eq!(on_disk, illegal, "v2={}", case.v2);
        assert!(
            boundary_of(&on_disk, &case.id) > case.complete_log.len() as u64,
            "v2={}",
            case.v2
        );
        let log_file = fs::File::open(case.path.join("records.log")).unwrap();
        if case.metadata {
            assert_eq!(
                fs::read(case.path.join("commit.state")).unwrap(),
                case.anchor,
                "v2={}",
                case.v2
            );
            assert!(
                crate::metadata_witness::is_present(&log_file).unwrap(),
                "v2={}",
                case.v2
            );
        } else {
            assert_eq!(
                fs::read(case.path.join("commit.state")).unwrap(),
                illegal,
                "v2={}",
                case.v2
            );
            assert!(
                !crate::metadata_witness::is_present(&log_file).unwrap(),
                "v2={}",
                case.v2
            );
        }
    }

    fn assert_conflict_is_not_published(
        case: &AdoptionCase,
        destination: &Path,
        illegal: &[u8],
        failure: ManagedRebuildFailure,
    ) {
        match failure {
            ManagedRebuildFailure::NotPublished(error) => {
                assert_eq!(
                    error.kind(),
                    io::ErrorKind::InvalidData,
                    "v2={}: {error}",
                    case.v2
                );
                let message = error.to_string();
                assert!(
                    message.contains("unacknowledged tail adoption may have started"),
                    "v2={}: {message}",
                    case.v2
                );
                assert!(
                    message.contains("commit witness changed or moved backward"),
                    "v2={}: {message}",
                    case.v2
                );
            }
            other => panic!("v2={}: {other}", case.v2),
        }
        assert!(!destination.exists(), "v2={}", case.v2);
        assert!(!selection::path(&case.path).exists(), "v2={}", case.v2);
        assert_source_unchanged(case);
        assert_conflict_preserved(case, illegal);
        assert_eq!(
            crate::cleanup_abandoned_staging(&case.path)
                .unwrap()
                .removed_markers,
            0,
            "v2={}",
            case.v2
        );
    }

    fn assert_open_rejects_the_conflicting_boundary(case: &AdoptionCase, illegal: &[u8]) {
        let open_error = match retry(|| Store::open(&case.path)) {
            Ok(_) => panic!("v2={}: open adopted a witness past the log", case.v2),
            Err(error) => error,
        };
        assert_eq!(
            open_error.kind(),
            io::ErrorKind::InvalidData,
            "v2={}: {open_error}",
            case.v2
        );
        assert!(
            open_error
                .to_string()
                .contains("acknowledged commit boundary"),
            "v2={}: {open_error}",
            case.v2
        );
        assert_source_unchanged(case);
        assert_conflict_preserved(case, illegal);
    }

    fn assert_public_retry_does_not_adopt(case: &AdoptionCase, destination: &Path, illegal: &[u8]) {
        let again = publish_bounded(&case.path, destination, &case.base, &case.digest).unwrap_err();
        match again {
            ManagedRebuildFailure::NotPublished(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("outside the log"),
                    "v2={}: {message}",
                    case.v2
                );
                assert!(
                    !message.contains("adoption may have started"),
                    "v2={}: {message}",
                    case.v2
                );
            }
            other => panic!("v2={}: retry repaired or published: {other}", case.v2),
        }
        assert!(!destination.exists(), "v2={}", case.v2);
        assert!(!selection::path(&case.path).exists(), "v2={}", case.v2);
        assert_source_unchanged(case);
        assert_conflict_preserved(case, illegal);
    }

    fn assert_restored_boundary_publishes_the_tail(case: &AdoptionCase) {
        let destination = case.root().join("rebuilt");
        let rebuilt = publish_bounded(&case.path, &destination, &case.base, &case.digest)
            .unwrap_or_else(|error| panic!("v2={}: {error}", case.v2));
        assert_eq!(
            fs::read(case.path.join("records.log")).unwrap(),
            case.complete_log,
            "v2={}",
            case.v2
        );
        assert_eq!(
            witness_bytes(&case.path),
            crate::witness::encode_for_test(&case.id, case.complete_log.len() as u64),
            "v2={}",
            case.v2
        );
        if case.metadata {
            assert_eq!(
                fs::read(case.path.join("commit.state")).unwrap(),
                case.anchor,
                "v2={}",
                case.v2
            );
        }
        let reopened = retry(|| CheckpointStore::open_managed(&case.path)).unwrap();
        assert_eq!(reopened.overlay_keys().unwrap(), 0, "v2={}", case.v2);
        assert_eq!(
            reopened.get(b"kept").unwrap().as_deref(),
            Some(&b"base"[..])
        );
        assert!(reopened.get(b"safe").unwrap().is_none());
        assert_eq!(reopened.get(b"p").unwrap().as_deref(), Some(&b"two"[..]));
        assert_eq!(reopened.get(b"q").unwrap().as_deref(), Some(&b"batch"[..]));
        assert_eq!(reopened.get(b"r").unwrap().as_deref(), Some(&b"after"[..]));
        drop(reopened);
        let reference = case.root().join("reference");
        let store = retry(|| Store::open(&case.path)).unwrap();
        let reference_digest = store.checkpoint_to(&reference).unwrap();
        drop(store);
        assert_eq!(rebuilt, reference_digest, "v2={}", case.v2);
        assert_eq!(
            fs::read(&destination).unwrap(),
            fs::read(&reference).unwrap(),
            "v2={}",
            case.v2
        );
        assert_eq!(
            crate::cleanup_abandoned_staging(&case.path)
                .unwrap()
                .removed_markers,
            0,
            "v2={}",
            case.v2
        );
    }

    // After the tail is validated, adoption observes a checksum-valid witness
    // past the log. It must not rewrite that boundary or publish. V2 keeps the
    // mutable boundary in commit.state. V3 keeps it in the log xattr and leaves
    // the header-sized commit.state anchor unchanged.
    #[test]
    fn substituted_witness_during_adoption_is_not_repaired_or_published() {
        for v2 in [true, false] {
            let Some(case) = prepare_unacknowledged_tail(v2) else {
                continue;
            };
            let rejected = case.root().join("rejected");
            let illegal =
                crate::witness::encode_for_test(&case.id, case.complete_log.len() as u64 + 1);
            let failure = rebuild_after_substituting_witness(&case, &rejected);
            assert_conflict_is_not_published(&case, &rejected, &illegal, failure);
            assert_open_rejects_the_conflicting_boundary(&case, &illegal);
            assert_public_retry_does_not_adopt(&case, &rejected, &illegal);
            restore_witness(&case.path, &case.rolled_back);
            assert_restored_boundary_publishes_the_tail(&case);
        }
    }

    fn encode_hex(bytes: &[u8; 32]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in bytes {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0xf) as usize] as char);
        }
        encoded
    }

    fn decode_hex(text: &str) -> [u8; 32] {
        let bytes = text.as_bytes();
        assert_eq!(bytes.len(), 64);
        let mut digest = [0_u8; 32];
        for (index, chunk) in bytes.chunks(2).enumerate() {
            digest[index] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
        }
        digest
    }

    fn pause_for(mode: &str, expected: &str) -> io::Result<()> {
        if mode == expected {
            println!("BOUNDED_REBUILD_BOUNDARY {expected}");
            let _ = std::io::stdout().flush();
            loop {
                std::thread::park();
            }
        }
        Ok(())
    }

    #[test]
    fn bounded_rebuild_crash_child() {
        let Ok(mode) = std::env::var("EMDA_BOUNDED_REBUILD_CRASH") else {
            return;
        };
        let path = std::path::PathBuf::from(std::env::var("EMDA_BOUNDED_REBUILD_PATH").unwrap());
        let base = std::path::PathBuf::from(std::env::var("EMDA_BOUNDED_REBUILD_BASE").unwrap());
        let destination =
            std::path::PathBuf::from(std::env::var("EMDA_BOUNDED_REBUILD_DEST").unwrap());
        let digest = decode_hex(&std::env::var("EMDA_BOUNDED_REBUILD_DIGEST").unwrap());
        let mut before_adopt = || pause_for(&mode, "adopt");
        let mut sync_checkpoint = |parent: &Path| {
            pause_for(&mode, "checkpoint")?;
            selection::sync_ancestors(parent)
        };
        let mut before_selection = || pause_for(&mode, "selection");
        let mut sync_selection = |parent: &Path| selection::sync_ancestors(parent);
        let mut hooks = BoundedHooks {
            before_adopt: &mut before_adopt,
            sync_checkpoint_ancestors: &mut sync_checkpoint,
            before_selection_rename: &mut before_selection,
            sync_selection_parent: &mut sync_selection,
        };
        let result = CheckpointStore::rebuild_bounded_using(
            &path,
            &destination,
            &base,
            &digest,
            small_limits(),
            None,
            &mut hooks,
        );
        panic!("bounded rebuild child returned before {mode}: {result:?}");
    }

    fn crash_fixture_initial(
        path: &Path,
        base: &Path,
        selected: &Path,
        managed: bool,
        retry_managed: bool,
        before_drop: impl FnOnce(),
        after_drop: impl FnOnce(&Path),
    ) -> [u8; 32] {
        let store = Store::open(path).unwrap();
        store.put(b"safe", b"acked").unwrap();
        let digest = store.checkpoint_to(base).unwrap();
        before_drop();
        drop(store);
        after_drop(path);
        if managed {
            if retry_managed {
                retry(|| CheckpointStore::rebuild_managed_selection(path, selected)).unwrap();
            } else {
                CheckpointStore::rebuild_managed_selection(path, selected).unwrap();
            }
        }
        digest
    }

    #[test]
    fn crash_boundaries_leave_a_recoverable_store() {
        for mode in ["adopt", "checkpoint", "selection"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("store");
            let base = directory.path().join("base");
            let destination = directory.path().join("rebuilt");
            let digest = crash_fixture_initial(
                &path,
                &base,
                &directory.path().join("selected"),
                mode != "adopt",
                true,
                || {},
                |_| {},
            );
            let witness = witness_bytes(&path);
            let selection_before = fs::read(selection::path(&path)).ok();
            let store = crate::test_support::diagnose_lock(
                &path,
                &format!("crash fixture reopen mode={mode}"),
                Store::open(&path),
            )
            .unwrap();
            store.put(b"one", b"pending-1").unwrap();
            store.put(b"two", b"pending-2").unwrap();
            drop(store);
            if mode == "adopt" {
                restore_witness(&path, &witness);
            }
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "checkpoint_store::rebuild::tests::bounded_rebuild_crash_child",
                    "--nocapture",
                ])
                .env("EMDA_BOUNDED_REBUILD_CRASH", mode)
                .env("EMDA_BOUNDED_REBUILD_PATH", &path)
                .env("EMDA_BOUNDED_REBUILD_BASE", &base)
                .env("EMDA_BOUNDED_REBUILD_DEST", &destination)
                .env("EMDA_BOUNDED_REBUILD_DIGEST", encode_hex(&digest))
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let output = child.stdout.take().unwrap();
            let (sender, receiver) = mpsc::channel();
            std::thread::spawn(move || {
                for line in std::io::BufReader::new(output).lines() {
                    let Ok(line) = line else { break };
                    if line.contains("BOUNDED_REBUILD_BOUNDARY") {
                        let _ = sender.send(());
                        break;
                    }
                }
            });
            let reached = receiver.recv_timeout(Duration::from_secs(30));
            let cleanup_root = if mode == "checkpoint" {
                directory.path()
            } else {
                path.as_path()
            };
            let busy = crate::cleanup_abandoned_staging(cleanup_root);
            if reached.is_err() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not reach {mode}: {busy:?}");
            }
            assert_eq!(
                busy.unwrap_err().kind(),
                io::ErrorKind::WouldBlock,
                "{mode}"
            );
            if mode == "adopt" {
                assert_eq!(witness_bytes(&path), witness, "{mode}");
                assert!(!destination.exists(), "{mode}");
            } else {
                assert_eq!(
                    fs::read(selection::path(&path)).ok(),
                    selection_before,
                    "{mode}"
                );
                assert!(destination.is_file(), "{mode}");
            }
            child.kill().unwrap();
            child.wait().unwrap();
            let cleaned = retry(|| crate::cleanup_abandoned_staging(cleanup_root)).unwrap();
            assert!(cleaned.removed_markers >= 1, "{mode}");
            if mode == "checkpoint" {
                let spill_cleanup = retry(|| crate::cleanup_abandoned_staging(&path)).unwrap();
                assert!(spill_cleanup.removed_markers >= 1);
                assert_eq!(
                    retry(|| crate::cleanup_abandoned_staging(&path))
                        .unwrap()
                        .removed_markers,
                    0
                );
            }
            if mode == "adopt" {
                assert_eq!(witness_bytes(&path), witness);
                assert!(!destination.exists());
                let recovered = retry(|| Store::open(&path)).unwrap();
                assert_eq!(
                    recovered.get(b"one").unwrap().as_deref(),
                    Some(&b"pending-1"[..])
                );
                assert_eq!(
                    recovered.get(b"two").unwrap().as_deref(),
                    Some(&b"pending-2"[..])
                );
            } else {
                assert_eq!(fs::read(selection::path(&path)).ok(), selection_before);
                assert!(destination.is_file());
                let reopened = retry(|| CheckpointStore::open_managed(&path)).unwrap();
                assert_eq!(
                    reopened.get(b"safe").unwrap().as_deref(),
                    Some(&b"acked"[..])
                );
                assert_eq!(
                    reopened.get(b"one").unwrap().as_deref(),
                    Some(&b"pending-1"[..])
                );
                assert_eq!(
                    reopened.get(b"two").unwrap().as_deref(),
                    Some(&b"pending-2"[..])
                );
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum SpillDamage {
        Truncate,
        EntryTag,
        Key,
        SubstituteRun,
    }

    struct UnacknowledgedSuffix {
        _directory: tempfile::TempDir,
        path: PathBuf,
        base: PathBuf,
        digest: [u8; 32],
        witness: Vec<u8>,
        log: Vec<u8>,
        base_bytes: Vec<u8>,
    }

    fn unacknowledged_suffix_for_spill() -> UnacknowledgedSuffix {
        unacknowledged_suffix_before_drop(|| {}, |_| {})
    }

    fn unacknowledged_suffix_before_drop(
        before_drop: impl FnOnce(),
        after_drop: impl FnOnce(&Path),
    ) -> UnacknowledgedSuffix {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let base = directory.path().join("base");
        let store = Store::open(&path).unwrap();
        store.put(b"a", b"base").unwrap();
        let digest = store.checkpoint_to(&base).unwrap();
        before_drop();
        drop(store);
        after_drop(&path);
        let witness = witness_bytes(&path);
        let store =
            crate::test_support::diagnose_lock(&path, "spill fixture reopen", Store::open(&path))
                .unwrap();
        store.put(b"b", b"one").unwrap();
        store.put(b"c", b"two").unwrap();
        drop(store);
        let log = fs::read(path.join("records.log")).unwrap();
        let base_bytes = fs::read(&base).unwrap();
        restore_witness(&path, &witness);
        UnacknowledgedSuffix {
            _directory: directory,
            path,
            base,
            digest,
            witness,
            log,
            base_bytes,
        }
    }

    #[test]
    fn changed_base_checkpoint_after_spill_replay_does_not_advance_witness() {
        let case = unacknowledged_suffix_for_spill();
        let destination = case._directory.path().join("rebuilt");
        let mut damaged = case.base_bytes.clone();
        *damaged.last_mut().unwrap() ^= 1;
        let mut changed_base = false;
        let mut before_adopt = || {
            fs::write(&case.base, &damaged)?;
            changed_base = true;
            Ok(())
        };
        let failure = track_managed_rebuild(|tracker| {
            with_standard_hooks(|standard| {
                let mut hooks = BoundedHooks {
                    before_adopt: &mut before_adopt,
                    sync_checkpoint_ancestors: standard.sync_checkpoint_ancestors,
                    before_selection_rename: standard.before_selection_rename,
                    sync_selection_parent: standard.sync_selection_parent,
                };
                CheckpointStore::rebuild_bounded_using(
                    &case.path,
                    &destination,
                    &case.base,
                    &case.digest,
                    small_limits(),
                    Some(tracker),
                    &mut hooks,
                )
            })
        });
        assert!(changed_base);
        let Err(ManagedRebuildFailure::NotPublished(error)) = failure else {
            panic!("rebuild did not reject the changed checkpoint: {failure:?}");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "index checkpoint checksum mismatch");
        assert_eq!(witness_bytes(&case.path), case.witness);
        assert_eq!(fs::read(case.path.join("records.log")).unwrap(), case.log);
        assert_eq!(fs::read(&case.base).unwrap(), damaged);
        assert!(!destination.exists());
        assert!(!selection::path(&case.path).exists());
        assert_no_staged_markers(&case.path);
        assert_no_staged_markers(case._directory.path());

        fs::write(&case.base, &case.base_bytes).unwrap();
        with_standard_hooks(|hooks| {
            CheckpointStore::rebuild_bounded_using(
                &case.path,
                &destination,
                &case.base,
                &case.digest,
                small_limits(),
                None,
                hooks,
            )
        })
        .unwrap();
        let store = CheckpointStore::open_managed(&case.path).unwrap();
        assert_eq!(store.get(b"c").unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn same_length_tail_change_after_replay_does_not_advance_witness_or_publish() {
        let case = unacknowledged_suffix_for_spill();
        let destination = case._directory.path().join("rebuilt");
        let replacement =
            crate::record::encode(crate::record::Operation::Put, b"d", b"two").unwrap();
        let offset = (case.log.len() - replacement.len()) as u64;
        let log_path = case.path.join("records.log");
        let mut reached_adoption = false;
        let mut change_tail = || {
            reached_adoption = true;
            OpenOptions::new()
                .write(true)
                .open(&log_path)?
                .write_all_at(&replacement, offset)
        };
        let failure = track_managed_rebuild(|tracker| {
            with_standard_hooks(|standard| {
                let mut hooks = BoundedHooks {
                    before_adopt: &mut change_tail,
                    sync_checkpoint_ancestors: standard.sync_checkpoint_ancestors,
                    before_selection_rename: standard.before_selection_rename,
                    sync_selection_parent: standard.sync_selection_parent,
                };
                CheckpointStore::rebuild_bounded_using(
                    &case.path,
                    &destination,
                    &case.base,
                    &case.digest,
                    small_limits(),
                    Some(tracker),
                    &mut hooks,
                )
            })
        });
        assert!(reached_adoption);
        let Err(ManagedRebuildFailure::NotPublished(error)) = failure else {
            panic!("rebuild accepted a changed suffix: {failure:?}");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
        assert_eq!(
            error.to_string(),
            "checkpoint log suffix changed during replay"
        );
        assert_eq!(witness_bytes(&case.path), case.witness);
        assert_eq!(fs::read(&case.base).unwrap(), case.base_bytes);
        assert_eq!(
            &fs::read(&log_path).unwrap()[offset as usize..],
            &replacement
        );
        assert!(!destination.exists());
        assert!(!selection::path(&case.path).exists());
        assert_no_staged_markers(&case.path);
        assert_no_staged_markers(case._directory.path());

        fs::write(&log_path, &case.log).unwrap();
        CheckpointStore::rebuild_managed_selection_bounded(
            &case.path,
            &destination,
            &case.base,
            &case.digest,
            1,
            4096,
            32 << 20,
            2,
            10_000,
        )
        .unwrap();
        let rebuilt = CheckpointStore::open_managed(&case.path).unwrap();
        assert_eq!(
            rebuilt.get(b"c").unwrap().as_deref(),
            Some(b"two".as_slice())
        );
        assert_eq!(rebuilt.get(b"d").unwrap(), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rebuild_fixtures_reopen_before_inheriting_child_exits() {
        use crate::test_support::fork_lock::{Child, check_reopened, isolated};

        if isolated(
            concat!(
                module_path!(),
                "::rebuild_fixtures_reopen_before_inheriting_child_exits"
            ),
            "EMDA_REBUILD_FIXTURE_FORK_CHILD",
        ) {
            return;
        }
        let child = std::cell::RefCell::new(None);
        let spawn = || *child.borrow_mut() = Some(Child::spawn());
        let check = |path: &Path| check_reopened(path, child.borrow_mut().take().unwrap());
        let fixture = unacknowledged_suffix_before_drop(spawn, check);
        drop(fixture);
        for managed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store");
            crash_fixture_initial(
                &path,
                &dir.path().join("base"),
                &dir.path().join("selected"),
                managed,
                false,
                spawn,
                check,
            );
            drop(Store::open(&path).unwrap());
        }
    }

    #[test]
    fn log_limit_rejection_does_not_adopt_unacknowledged_tail() {
        let fixture = unacknowledged_suffix_for_spill();
        let destination = fixture._directory.path().join("rejected");
        let result = CheckpointStore::rebuild_managed_selection_bounded_with_log_limit_detailed(
            &fixture.path,
            &destination,
            &fixture.base,
            &fixture.digest,
            1,
            4096,
            32 << 20,
            2,
            10_000,
            fixture.log.len() as u64 - 1,
        );
        match result {
            Err(ManagedRebuildFailure::NotPublished(error)) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert!(!destination.exists());
        assert!(!selection::path(&fixture.path).exists());
        assert_eq!(witness_bytes(&fixture.path), fixture.witness);
        assert_eq!(
            fs::read(fixture.path.join("records.log")).unwrap(),
            fixture.log
        );
        assert_eq!(fs::read(&fixture.base).unwrap(), fixture.base_bytes);
    }

    fn finished_spill_runs(store: &Path) -> Vec<PathBuf> {
        let mut runs = Vec::new();
        for entry in fs::read_dir(store).unwrap().flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with(".emda-stage-v1-d-") || !name.ends_with(".data") {
                continue;
            }
            for file in fs::read_dir(entry.path()).unwrap().flatten() {
                let file_name = file.file_name();
                let file_name = file_name.to_string_lossy();
                if file_name.starts_with("run-") && !file_name.ends_with(".off") {
                    runs.push(file.path());
                }
            }
        }
        runs
    }

    fn corrupt_first_spill_tag(path: &Path) {
        const HEADER: u64 = 28;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut key_len_bytes = [0_u8; 2];
        file.read_exact_at(&mut key_len_bytes, HEADER).unwrap();
        let key_len = u16::from_le_bytes(key_len_bytes) as u64;
        assert!((1..=1024).contains(&key_len), "key length {key_len}");
        let tag_at = HEADER + 2 + key_len;
        let mut tag = [0_u8; 1];
        file.read_exact_at(&mut tag, tag_at).unwrap();
        assert_eq!(tag[0], 1, "expected a live spill entry");
        file.write_all_at(&[0x7f], tag_at).unwrap();
    }

    fn substitute_spill_run(case: &UnacknowledgedSuffix, destination: &Path) {
        let foreign = tempfile::tempdir().unwrap();
        let (log, witness) = Log::open_checkpoint_view(&case.path).unwrap();
        let mut suffix = Vec::new();
        log.visit_records(witness, Some(witness), |key, location| {
            suffix.push((key, location));
            Ok(())
        })
        .unwrap();
        assert_eq!(suffix.len(), 2);
        assert!(suffix.iter().all(|(_, location)| location.is_some()));
        let mut spill = SpillSession::create(foreign.path(), small_limits(), 1, 1).unwrap();
        for ((key, _), (_, location)) in suffix.iter().zip(suffix.iter().rev()) {
            spill.push(key, *location, |_| Ok(None)).unwrap();
        }
        spill.prepare_for_emit().unwrap();
        let files = finished_spill_runs(foreign.path());
        assert_eq!(files.len(), 1);
        let alternate = fs::read(&files[0]).unwrap();
        assert_eq!(
            alternate.len() as u64,
            fs::metadata(destination).unwrap().len()
        );
        fs::write(destination, alternate).unwrap();
    }

    fn assert_no_staged_markers(parent: &Path) {
        let cleaned = retry(|| crate::cleanup_abandoned_staging(parent)).unwrap();
        assert_eq!(cleaned.removed_markers, 0, "{}", parent.display());
        assert_eq!(cleaned.removed_payloads, 0, "{}", parent.display());
    }

    // The only hook after a finished run is opened and before emission is
    // `before_adopt`. It runs before witness adoption. Returning success lets
    // that adoption happen, then emission reads the damaged run and fails
    // before checkpoint or selection publication.
    #[test]
    fn damaged_finished_spill_does_not_publish_checkpoint_or_selection() {
        for damage in [
            SpillDamage::Truncate,
            SpillDamage::EntryTag,
            SpillDamage::Key,
            SpillDamage::SubstituteRun,
        ] {
            let case = unacknowledged_suffix_for_spill();
            let destination = case._directory.path().join(format!("rebuilt-{damage:?}"));
            let mut saw_finished_run = false;
            let mut checkpoint_synced = false;
            let mut selection_started = false;
            let mut selection_synced = false;
            let failure = track_managed_rebuild(|tracker| {
                let mut before_adopt = || {
                    assert_eq!(
                        witness_bytes(&case.path),
                        case.witness,
                        "{damage:?} published the witness before spill emission"
                    );
                    assert!(!destination.exists(), "{damage:?}");
                    assert!(!selection::path(&case.path).exists(), "{damage:?}");
                    let runs = finished_spill_runs(&case.path);
                    assert_eq!(runs.len(), 1, "{damage:?}: {runs:?}");
                    let bytes = fs::read(&runs[0]).unwrap();
                    assert!(bytes.starts_with(b"EMDASP01"), "{damage:?}");
                    assert!(bytes.len() > 60, "{damage:?}");
                    saw_finished_run = true;
                    match damage {
                        SpillDamage::Truncate => {
                            OpenOptions::new()
                                .write(true)
                                .open(&runs[0])
                                .unwrap()
                                .set_len(0)
                                .unwrap();
                        }
                        SpillDamage::EntryTag => corrupt_first_spill_tag(&runs[0]),
                        SpillDamage::Key => {
                            let file = OpenOptions::new().write(true).open(&runs[0]).unwrap();
                            file.write_all_at(b"B", 30).unwrap();
                        }
                        SpillDamage::SubstituteRun => substitute_spill_run(&case, &runs[0]),
                    }
                    Ok(())
                };
                let mut sync_checkpoint = |parent: &Path| {
                    checkpoint_synced = true;
                    selection::sync_ancestors(parent)
                };
                let mut before_selection = || {
                    selection_started = true;
                    Ok(())
                };
                let mut sync_selection = |parent: &Path| {
                    selection_synced = true;
                    selection::sync_ancestors(parent)
                };
                let mut hooks = BoundedHooks {
                    before_adopt: &mut before_adopt,
                    sync_checkpoint_ancestors: &mut sync_checkpoint,
                    before_selection_rename: &mut before_selection,
                    sync_selection_parent: &mut sync_selection,
                };
                CheckpointStore::rebuild_bounded_using(
                    &case.path,
                    &destination,
                    &case.base,
                    &case.digest,
                    small_limits(),
                    Some(tracker),
                    &mut hooks,
                )
            })
            .unwrap_err();
            assert!(saw_finished_run, "{damage:?}: {failure}");
            assert!(!checkpoint_synced, "{damage:?}");
            assert!(!selection_started, "{damage:?}");
            assert!(!selection_synced, "{damage:?}");
            match (damage, failure) {
                (SpillDamage::Truncate, ManagedRebuildFailure::NotPublished(error)) => {
                    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof, "{error}");
                }
                (SpillDamage::EntryTag, ManagedRebuildFailure::NotPublished(error)) => {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
                    assert!(
                        error.to_string().contains("unknown spill entry tag"),
                        "{error}"
                    );
                }
                (
                    SpillDamage::Key | SpillDamage::SubstituteRun,
                    ManagedRebuildFailure::NotPublished(error),
                ) => {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
                    assert_eq!(error.to_string(), "spill entry checksum mismatch");
                }
                (_, other) => panic!("{damage:?}: {other}"),
            }
            assert!(!destination.exists(), "{damage:?}");
            assert!(!selection::path(&case.path).exists(), "{damage:?}");
            assert_eq!(fs::read(case.path.join("records.log")).unwrap(), case.log);
            assert_eq!(fs::read(&case.base).unwrap(), case.base_bytes);
            assert_eq!(
                witness_bytes(&case.path),
                crate::witness::encode_for_test(&log_id(&case.path), case.log.len() as u64),
                "{damage:?}: validated tail was not adopted before the failed spill read"
            );
            assert_no_staged_markers(&case.path);
            assert_no_staged_markers(case._directory.path());
            if matches!(damage, SpillDamage::SubstituteRun) {
                CheckpointStore::rebuild_managed_selection_bounded(
                    &case.path,
                    &destination,
                    &case.base,
                    &case.digest,
                    1,
                    4096,
                    32 << 20,
                    2,
                    10_000,
                )
                .unwrap();
                let recovered = CheckpointStore::open_managed(&case.path).unwrap();
                assert_eq!(
                    recovered.get(b"a").unwrap().as_deref(),
                    Some(b"base".as_slice())
                );
                assert_eq!(
                    recovered.get(b"b").unwrap().as_deref(),
                    Some(b"one".as_slice())
                );
                assert_eq!(
                    recovered.get(b"c").unwrap().as_deref(),
                    Some(b"two".as_slice())
                );
            }
        }
    }
}
