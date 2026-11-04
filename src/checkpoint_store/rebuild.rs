//! Explicit offline recovery of a managed checkpoint selection.
use super::{CheckpointStore, CheckpointView, selection};
use std::fs;
use std::io;
use std::path::Path;

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
        )
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
        )
    }

    fn rebuild_managed_using(
        path: &Path,
        destination: &Path,
        open_store: impl FnOnce(&Path) -> io::Result<crate::Store>,
        sync_checkpoint_ancestors: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
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
        if destination == selection::path(&path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint destination is the managed selection file",
            ));
        }
        selection::Selection::new(destination.clone(), [0; 32])?;
        let store = open_store(&path)?;
        // Refuse to overwrite unknown, damaged, symlink or non-file metadata.
        // A valid but stale selection need not have an accessible checkpoint.
        selection::load(&path)?;
        let digest = store.checkpoint_to_with_sync(&destination, sync_checkpoint_ancestors)?;
        let lock = store.duplicate_store_lock()?;
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
        selection::publish(&path, &next, before_selection_rename, sync_selection_parent)
            .map_err(|error| io::Error::new(error.kind(), format!("managed selection publication may have occurred; recovery required: {error}")))?;
        Ok(digest)
    }
}
