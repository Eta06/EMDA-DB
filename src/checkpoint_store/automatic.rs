//! On-demand managed checkpoint rollover using two owned checkpoint slots.
use super::{CheckpointStore, State, selection};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

const DIRECTORY: &str = ".emda-auto-checkpoints-v1";
const OWNER: &str = "owner";
const SLOT_OWNER: &[u8] = b"EMDA-AUTO-CHECKPOINT-SLOT-V1\n";
const ROOT_ENABLED: &[u8] = b"EMDA-AUTO-CHECKPOINT-ROOT-V1:ON\n";
const ROOT_DISABLED: &[u8] = b"EMDA-AUTO-CHECKPOINT-ROOT-V1:OFF\n";
const INDEX: &str = crate::store::COMPACTED_CHECKPOINT_FILE;
const SLOTS: [&str; 2] = ["slot-0", "slot-1"];

pub(super) fn root(store: &Path) -> PathBuf {
    store.join(DIRECTORY)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_marker(directory: &Path, expected: &[u8]) -> io::Result<File> {
    let mut file = selection::open_regular(&directory.join(OWNER))?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(expected.len() as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes != expected {
        return Err(invalid("automatic checkpoint ownership marker is invalid"));
    }
    Ok(file)
}

fn write_marker(directory: &Path, content: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.join(OWNER))?;
    file.write_all(content)?;
    file.sync_all()?;
    File::open(directory)?.sync_all()
}

/// Check and stabilize a previously published opt-in marker. Symlinks,
/// unexpected types and malformed markers are rejected without deletion.
pub(super) fn open_existing(store: &Path) -> io::Result<Option<bool>> {
    let root = root(store);
    match fs::symlink_metadata(&root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(invalid("automatic checkpoint directory is not a directory"));
        }
        Ok(_) => {}
    }
    let mut marker = selection::open_regular(&root.join(OWNER))?;
    let mut bytes = Vec::new();
    (&mut marker)
        .take(ROOT_DISABLED.len() as u64 + 1)
        .read_to_end(&mut bytes)?;
    let enabled = if bytes == ROOT_ENABLED {
        true
    } else if bytes == ROOT_DISABLED {
        false
    } else {
        return Err(invalid("automatic checkpoint configuration is invalid"));
    };
    marker.sync_all()?;
    crate::directory::sync_existing(&root)?;
    Ok(Some(enabled))
}

fn initialize(store: &Path) -> io::Result<()> {
    initialize_with_hooks(store, || Ok(()), crate::directory::sync_existing)
}

pub(super) fn initialize_with_hooks(
    store: &Path,
    before_publish: impl FnOnce() -> io::Result<()>,
    sync_store_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    match open_existing(store)? {
        Some(true) => return Ok(()),
        Some(false) => {
            return publish_configuration(store, true, || Ok(()), crate::directory::sync_existing);
        }
        None => {}
    }
    // New stages use recognized ownership markers. Cleanup takes an exclusive
    // parent lock and refuses to run while another new stage is active.
    crate::cleanup_abandoned_staging(store)?;
    let destination = root(store);
    let mut stage = crate::staging::Stage::directory(store)?;
    write_marker(stage.path(), ROOT_ENABLED)?;
    before_publish()?;
    stage.publish(&destination)?;
    sync_store_parent(store).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("automatic checkpoint configuration may exist: {error}"),
        )
    })?;
    if open_existing(store)? != Some(true) {
        return Err(invalid("automatic checkpoint configuration disappeared"));
    }
    Ok(())
}

pub(super) fn publish_configuration(
    store: &Path,
    enabled: bool,
    before_rename: impl FnOnce() -> io::Result<()>,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let root = root(store);
    crate::cleanup_abandoned_staging(&root)?;
    let (stage, mut file) = crate::staging::Stage::file(&root)?;
    file.write_all(if enabled { ROOT_ENABLED } else { ROOT_DISABLED })?;
    file.sync_all()?;
    drop(file);
    before_rename()?;
    fs::rename(stage.path(), root.join(OWNER))?;
    sync_parent(&root)
}

impl CheckpointStore {
    /// Enables persistent, on-demand checkpoint rollover when a write would
    /// exceed a suffix limit. A managed selection must already exist. Two
    /// engine-owned slots are reused; existing snapshots retain their open
    /// checkpoint handles. No live-log bytes are reclaimed. The store and its
    /// reserved `.emda-auto-checkpoints-v1` directory must remain trusted.
    ///
    /// An oversized record or exhausted whole-log limit still fails before
    /// append. Rollover blocks reads and writes and may fail before the user's
    /// mutation is submitted. A selection-publication error poisons the store
    /// until it is reopened. This does not bound total memory or disk usage.
    pub fn enable_automatic_checkpoints(&self) -> io::Result<()> {
        let mut state = self.write()?;
        if state.selection.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "automatic checkpoints require a managed selection",
            ));
        }
        self.validate_automatic_selection(&mut state)?;
        if state.automatic {
            if !matches!(open_existing(&self.path), Ok(Some(true))) {
                state.failed = true;
                return Err(io::Error::other(
                    "automatic checkpoint configuration is missing, invalid or changed",
                ));
            }
        } else {
            if let Err(error) = initialize(&self.path) {
                state.failed = true;
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "automatic checkpoint configuration may have changed; reopen required: {error}"
                    ),
                ));
            }
            state.automatic = true;
        }
        Ok(())
    }

    /// Disables automatic checkpointing persistently. Existing checkpoint
    /// slots are retained because the current selection or snapshots may
    /// still reference them. Re-enabling resumes safe slot reuse.
    pub fn disable_automatic_checkpoints(&self) -> io::Result<()> {
        self.disable_automatic_checkpoints_with_hooks(|| Ok(()), crate::directory::sync_existing)
    }

    pub(super) fn disable_automatic_checkpoints_with_hooks(
        &self,
        before_rename: impl FnOnce() -> io::Result<()>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut state = self.write()?;
        if state.automatic {
            self.validate_automatic_selection(&mut state)?;
            if !matches!(open_existing(&self.path), Ok(Some(true))) {
                state.failed = true;
                return Err(io::Error::other(
                    "automatic checkpoint configuration is missing, invalid or changed",
                ));
            }
            if let Err(error) = publish_configuration(&self.path, false, before_rename, sync_parent)
            {
                state.failed = true;
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "automatic checkpoint configuration may have changed; reopen required: {error}"
                    ),
                ));
            }
            state.automatic = false;
        }
        Ok(())
    }

    /// Whether automatic checkpointing is enabled for this managed store.
    pub fn automatic_checkpoints_enabled(&self) -> io::Result<bool> {
        Ok(self.read()?.automatic)
    }

    pub(super) fn automatic_rollover(&self, state: &mut State) -> io::Result<()> {
        self.automatic_rollover_with_hooks(
            state,
            || Ok(()),
            crate::directory::sync_existing,
            || Ok(()),
            selection::sync_ancestors,
        )
    }

    fn validate_automatic_selection(&self, state: &mut State) -> io::Result<()> {
        let existing = selection::load(&self.path);
        if !matches!(&existing, Ok(found) if *found == state.selection) {
            state.failed = true;
            return Err(io::Error::other(
                "managed checkpoint selection is missing, invalid or changed",
            ));
        }
        Ok(())
    }

    pub(super) fn automatic_rollover_with_hooks(
        &self,
        state: &mut State,
        before_slot_publish: impl FnOnce() -> io::Result<()>,
        sync_slot_parent: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        if !state.automatic {
            return Err(invalid("automatic checkpointing is not enabled"));
        }
        match open_existing(&self.path) {
            Ok(Some(true)) => {}
            Ok(None | Some(false)) => {
                state.failed = true;
                return Err(invalid("automatic checkpoint configuration is missing"));
            }
            Err(error) => {
                state.failed = true;
                return Err(error);
            }
        }
        self.validate_automatic_selection(state)?;
        let selected = &state
            .selection
            .as_ref()
            .expect("managed selection")
            .checkpoint;
        let root = root(&self.path);
        let indexes = SLOTS.map(|slot| root.join(slot).join(INDEX));
        if selected.starts_with(&root) && selected != &indexes[0] && selected != &indexes[1] {
            return Err(invalid(
                "managed selection points to an unknown automatic checkpoint",
            ));
        }
        let next_slot = usize::from(selected == &indexes[0]);
        let destination = root.join(SLOTS[next_slot]);
        crate::cleanup_abandoned_staging(&root)?;
        remove_inactive_slot(&root, &destination)?;
        let (count, peak) = self.verify_for_checkpoint(state)?;
        let mut stage = crate::staging::Stage::directory(&root)?;
        write_marker(stage.path(), SLOT_OWNER)?;
        let digest = crate::checkpoint::create_streaming_in_private_stage(
            &self.log,
            count,
            peak,
            stage.path(),
            Some(self.checkpoint_view_limits),
            |emit| {
                state.view.visit_entries(None, None, |key, location| {
                    emit(key, location)?;
                    Ok(std::ops::ControlFlow::Continue(()))
                })
            },
        )?;
        before_slot_publish()?;
        stage.publish(&destination)?;
        sync_slot_parent(&root).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("automatic checkpoint slot may exist: {error}"),
            )
        })?;
        let next_view = self.rollover_view(state, &indexes[next_slot], &digest)?;
        let next = selection::Selection::new(indexes[next_slot].clone(), digest)?;
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
        Ok(())
    }
}

// A slot is removed only inside the reserved engine-owned root and only when
// its known contents and ownership marker are valid. An empty slot directory
// is an interrupted deletion and can be removed. Never follow a symlink.
fn remove_inactive_slot(root: &Path, slot: &Path) -> io::Result<()> {
    remove_inactive_slot_with_hooks(root, slot, |_| Ok(()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RemovalPoint {
    Index,
    Owner,
    Directory,
}

pub(super) fn remove_inactive_slot_with_hooks(
    root: &Path,
    slot: &Path,
    mut after_removal: impl FnMut(RemovalPoint) -> io::Result<()>,
) -> io::Result<()> {
    match fs::symlink_metadata(slot) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(invalid("automatic checkpoint slot is not a directory"));
        }
        Ok(_) => {}
    }
    let mut owner = false;
    let mut index = false;
    for entry in fs::read_dir(slot)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == OsStr::new(OWNER) {
            if !entry.file_type()?.is_file() {
                return Err(invalid("automatic checkpoint owner is not a file"));
            }
            owner = true;
        } else if name == OsStr::new(INDEX) {
            if !entry.file_type()?.is_file() {
                return Err(invalid("automatic checkpoint index is not a file"));
            }
            index = true;
        } else {
            return Err(invalid("automatic checkpoint slot has an unknown entry"));
        }
    }
    if owner {
        drop(read_marker(slot, SLOT_OWNER)?);
    } else if index {
        return Err(invalid(
            "automatic checkpoint slot lacks an ownership marker",
        ));
    }
    if index {
        fs::remove_file(slot.join(INDEX))?;
        after_removal(RemovalPoint::Index)?;
    }
    if owner {
        fs::remove_file(slot.join(OWNER))?;
        after_removal(RemovalPoint::Owner)?;
    }
    fs::remove_dir(slot)?;
    after_removal(RemovalPoint::Directory)?;
    crate::directory::sync_existing(root)
}
