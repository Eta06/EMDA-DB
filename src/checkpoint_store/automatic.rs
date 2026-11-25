//! On-demand managed checkpoint rollover using two owned checkpoint slots.
use super::disk_budget::{AutomaticReservation, DiskBudget};
use super::{CheckpointStore, State, StateWrite, selection};
use crate::ManagedRolloverFailure;
use crate::publication_outcome::{ManagedTracker, track_managed};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Reports whether a retired store's automatic checkpoint root is absent or
/// contains only (a subset of) the owned layout: the root marker and both
/// slot directories, each holding only its marker and checkpoint index.
/// Partly removed layouts are accepted so an interrupted retirement can
/// resume. Any other entry or file type returns `false`.
pub(crate) fn retired_root_is_known(store: &Path) -> io::Result<bool> {
    let root = root(store);
    match fs::symlink_metadata(&root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
        Ok(metadata) if !metadata.is_dir() => return Ok(false),
        Ok(_) => {}
    }
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        let name = entry.file_name();
        let kind = entry.file_type()?;
        if name == OsStr::new(OWNER) && kind.is_file() {
            continue;
        }
        if !SLOTS.iter().any(|slot| name == OsStr::new(slot)) || !kind.is_dir() {
            return Ok(false);
        }
        for inner in fs::read_dir(entry.path())? {
            let inner = inner?;
            let inner_name = inner.file_name();
            if !(inner_name == OsStr::new(OWNER) || inner_name == OsStr::new(INDEX))
                || !inner.file_type()?.is_file()
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Removes the automatic checkpoint root of a retired store that has already
/// been moved under a private owned stage. The whole layout is checked before
/// any removal; an unknown entry is preserved and returned as an error.
/// Removal is resumable after interruption at any step.
pub(crate) fn remove_retired_root(store: &Path) -> io::Result<()> {
    if !retired_root_is_known(store)? {
        return Err(invalid(
            "retired automatic checkpoint root has an unknown entry",
        ));
    }
    let root = root(store);
    if !root.exists() {
        return Ok(());
    }
    for slot in SLOTS {
        let slot = root.join(slot);
        if slot.exists() {
            remove_if_present(&slot.join(INDEX))?;
            remove_if_present(&slot.join(OWNER))?;
            File::open(&slot)?.sync_all()?;
            fs::remove_dir(&slot)?;
        }
    }
    remove_if_present(&root.join(OWNER))?;
    File::open(&root)?.sync_all()?;
    fs::remove_dir(&root)?;
    File::open(store)?.sync_all()
}

pub(super) fn indexes(root: &Path) -> [PathBuf; 2] {
    SLOTS.map(|slot| root.join(slot).join(INDEX))
}

#[derive(Debug)]
pub(super) struct PinnedSlot;

impl std::fmt::Display for PinnedSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("inactive automatic checkpoint slot is still pinned")
    }
}

impl std::error::Error for PinnedSlot {}

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

#[cfg(test)]
pub(super) fn initialize_with_hooks(
    store: &Path,
    before_publish: impl FnOnce() -> io::Result<()>,
    sync_store_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    initialize_with_hooks_tracked(store, before_publish, sync_store_parent, None)
}

fn initialize_with_hooks_tracked(
    store: &Path,
    before_publish: impl FnOnce() -> io::Result<()>,
    sync_store_parent: impl FnOnce(&Path) -> io::Result<()>,
    tracker: Option<&crate::publication_outcome::Tracker>,
) -> io::Result<()> {
    match open_existing(store)? {
        Some(true) => return Ok(()),
        Some(false) => {
            return publish_configuration(store, true, before_publish, sync_store_parent, tracker);
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
    if let Some(tracker) = tracker {
        tracker.begin_publication();
    }
    stage.publish(&destination)?;
    sync_store_parent(store).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("automatic checkpoint configuration may exist: {error}"),
        )
    })?;
    stage.publication_synchronized();
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
    tracker: Option<&crate::publication_outcome::Tracker>,
) -> io::Result<()> {
    let root = root(store);
    crate::cleanup_abandoned_staging(&root)?;
    let (mut stage, mut file) = crate::staging::Stage::file(&root)?;
    file.write_all(if enabled { ROOT_ENABLED } else { ROOT_DISABLED })?;
    file.sync_all()?;
    drop(file);
    before_rename()?;
    if let Some(tracker) = tracker {
        tracker.begin_publication();
    }
    stage.replace_file(&root.join(OWNER))?;
    sync_parent(&root)?;
    stage.publication_synchronized();
    Ok(())
}

impl CheckpointStore {
    /// Controls whether automatic rollover may unlink a slot pinned by a
    /// captured view from this open store. Disabled by default and on reopen.
    ///
    /// When enabled, a rollover needing a pinned slot returns `WouldBlock`
    /// before changing the slot, selection, or log. The checkpoint worker
    /// skips that rollover and can try again on its next check. Drop or cancel
    /// the snapshot holding the view to free it.
    /// This setting affects future slot reuse. It does not recover space from
    /// already unlinked files or impose a byte quota on the log or checkpoints.
    /// Under a disk budget pinned slots are always protected, whatever this
    /// setting says.
    pub fn set_automatic_checkpoint_slot_protection(&self, enabled: bool) -> io::Result<()> {
        self.write()?.protect_pinned_automatic_slots = enabled;
        Ok(())
    }

    /// Set an inclusive logical log read ceiling for each automatic
    /// checkpoint rollover on this open handle. The ceiling charges the
    /// complete existing log for source verification, staged checkpoint
    /// verification, and opening the new view. Framing and metadata probes,
    /// checkpoint-file reads, physical retries, and writes have separate
    /// costs.
    ///
    /// An insufficient ceiling returns `InvalidInput` before slot cleanup,
    /// publication, or the triggering append. A background check reports
    /// that error and stops its worker. The ceiling survives a consuming
    /// compaction handoff and resets on reopen. `None` restores the default.
    pub fn set_automatic_checkpoint_log_scan_limit(&self, limit: Option<u64>) -> io::Result<()> {
        let mut state = self.write()?;
        if limit.is_some() || state.automatic_checkpoint.is_some() {
            state
                .automatic_checkpoint
                .get_or_insert_with(Default::default)
                .log_scan_limit = limit;
        }
        Ok(())
    }

    /// Enables persistent, on-demand checkpoint rollover when a write would
    /// exceed a suffix limit. A managed selection must already exist. Two
    /// engine-owned slots are reused; existing snapshots retain their open
    /// checkpoint handles. No live-log bytes are reclaimed. The store and its
    /// reserved `.emda-auto-checkpoints-v1` directory must remain trusted.
    ///
    /// An oversized record or exhausted whole-log limit still fails before
    /// append. Rollover pauses writes and lets readers use the committed view
    /// during checkpoint preparation. It may fail before the user's mutation
    /// is submitted. A selection-publication error poisons the store until
    /// it is reopened. Without a disk budget this does not bound disk usage.
    ///
    /// Under a persistent disk budget, enabling and each rollover are
    /// admitted against the budget before they remove or write anything.
    /// A rollover admits its exact checkpoint file, staging markers,
    /// selection replacement and the triggering append, credits the
    /// inactive slot it replaces, and never reuses a slot a reader still
    /// holds. A refusal returns `StorageFull` and leaves the store healthy
    /// and unchanged. This does not bound memory.
    pub fn enable_automatic_checkpoints(&self) -> io::Result<()> {
        self.enable_automatic_checkpoints_tracked(None, || Ok(()), crate::directory::sync_existing)
    }

    /// Like [`Self::enable_automatic_checkpoints`], with a conservative
    /// classification of this call's configuration-publication attempt.
    /// `NotPublished` does not certify store health or cleanup; existing
    /// configuration may still be present. After `OutcomeUnknown`, reopen and
    /// inspect the configuration before retrying. Errors can disable the
    /// handle even before publication begins.
    pub fn enable_automatic_checkpoints_detailed(&self) -> Result<(), crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.enable_automatic_checkpoints_tracked(
                Some(tracker),
                || Ok(()),
                crate::directory::sync_existing,
            )
        })
    }

    pub(super) fn enable_automatic_checkpoints_tracked(
        &self,
        tracker: Option<&crate::publication_outcome::Tracker>,
        before_publish: impl FnOnce() -> io::Result<()>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
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
            // A budget refusal precedes every write and leaves the store
            // healthy; only a failed publication fences it.
            let charge = self.reserve_configuration(&state)?;
            let changed =
                initialize_with_hooks_tracked(&self.path, before_publish, sync_parent, tracker);
            let settled = charge.map_or(Ok(()), |charge| charge.settle(&mut state));
            if let Err(error) = changed {
                state.failed = true;
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "automatic checkpoint configuration may have changed; reopen required: {error}"
                    ),
                ));
            }
            settled?;
            state.automatic = true;
        }
        Ok(())
    }

    /// Disables automatic checkpointing persistently. Existing checkpoint
    /// slots are retained because the current selection or snapshots may
    /// still reference them. Re-enabling resumes safe slot reuse.
    pub fn disable_automatic_checkpoints(&self) -> io::Result<()> {
        self.disable_automatic_checkpoints_tracked(None, || Ok(()), crate::directory::sync_existing)
    }

    /// Like [`Self::disable_automatic_checkpoints`], with a conservative
    /// classification of this call's configuration-publication attempt.
    /// `NotPublished` does not certify store health or cleanup. After
    /// `OutcomeUnknown`, reopen and inspect the configuration before retrying.
    /// Errors can disable the handle even before publication begins.
    pub fn disable_automatic_checkpoints_detailed(&self) -> Result<(), crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.disable_automatic_checkpoints_tracked(
                Some(tracker),
                || Ok(()),
                crate::directory::sync_existing,
            )
        })
    }

    #[cfg(test)]
    pub(super) fn disable_automatic_checkpoints_with_hooks(
        &self,
        before_rename: impl FnOnce() -> io::Result<()>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        self.disable_automatic_checkpoints_tracked(None, before_rename, sync_parent)
    }

    pub(super) fn disable_automatic_checkpoints_tracked(
        &self,
        tracker: Option<&crate::publication_outcome::Tracker>,
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
            // A budget refusal precedes every write and leaves the store
            // healthy; only a failed publication fences it.
            let charge = self.reserve_configuration(&state)?;
            let changed =
                publish_configuration(&self.path, false, before_rename, sync_parent, tracker);
            let settled = charge.map_or(Ok(()), |charge| charge.settle(&mut state));
            if let Err(error) = changed {
                state.failed = true;
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "automatic checkpoint configuration may have changed; reopen required: {error}"
                    ),
                ));
            }
            settled?;
            state.automatic = false;
        }
        Ok(())
    }

    // Under a disk budget, admits a configuration change before it writes:
    // a new root needs a directory stage marker and its marker, and a
    // replacement needs a file stage marker and its temporary copy. The
    // refusal is the typed `BudgetExceeded` and changes nothing.
    fn reserve_configuration(&self, state: &State) -> io::Result<Option<ConfigurationCharge>> {
        let Some(budget) = self.disk_budget()? else {
            return Ok(None);
        };
        let marker = ROOT_DISABLED.len().max(ROOT_ENABLED.len()) as u64;
        let bytes = crate::staging::Stage::DIRECTORY_MARKER_BYTES
            + crate::staging::Stage::FILE_MARKER_BYTES
            + 2 * marker;
        let log_length = self.log.len()?;
        let reservation = budget.reserve_automatic(bytes, 0, None)?;
        Ok(Some(ConfigurationCharge {
            budget,
            reservation,
            selected: state
                .selection
                .as_ref()
                .map(|selected| selected.checkpoint.clone()),
            log_length,
        }))
    }

    /// Whether automatic checkpointing is enabled for this managed store.
    pub fn automatic_checkpoints_enabled(&self) -> io::Result<bool> {
        Ok(self.read()?.automatic)
    }

    pub(super) fn automatic_rollover<'a>(
        &'a self,
        state: StateWrite<'a>,
    ) -> Result<StateWrite<'a>, ManagedRolloverFailure> {
        self.automatic_rollover_with_hooks(
            state,
            || Ok(()),
            crate::directory::sync_existing,
            || Ok(()),
            selection::sync_ancestors,
        )
    }

    /// Rolls over before an append that will grow the log to `next_log`.
    /// Under a disk budget the rollover's peak includes that growth, and the
    /// growth stays reserved in the returned reservation until the append is
    /// admitted with it.
    pub(super) fn automatic_rollover_for_append<'a>(
        &'a self,
        state: StateWrite<'a>,
        next_log: u64,
    ) -> Result<(StateWrite<'a>, Option<AutomaticReservation>), ManagedRolloverFailure> {
        track_managed(|tracker| {
            self.automatic_rollover_tracked(
                state,
                (
                    || Ok(()),
                    crate::directory::sync_existing,
                    || Ok(()),
                    selection::sync_ancestors,
                ),
                Some(next_log),
                tracker,
            )
        })
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

    pub(super) fn automatic_rollover_with_hooks<'a>(
        &'a self,
        state: StateWrite<'a>,
        before_slot_publish: impl FnOnce() -> io::Result<()>,
        sync_slot_parent: impl FnOnce(&Path) -> io::Result<()>,
        before_selection_rename: impl FnOnce() -> io::Result<()>,
        sync_selection_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<StateWrite<'a>, ManagedRolloverFailure> {
        track_managed(|tracker| {
            self.automatic_rollover_tracked(
                state,
                (
                    before_slot_publish,
                    sync_slot_parent,
                    before_selection_rename,
                    sync_selection_parent,
                ),
                None,
                tracker,
            )
            .map(|(state, _)| state)
        })
    }

    fn automatic_rollover_tracked<'a>(
        &'a self,
        mut state: StateWrite<'a>,
        (before_slot_publish, sync_slot_parent, before_selection_rename, sync_selection_parent): (
            impl FnOnce() -> io::Result<()>,
            impl FnOnce(&Path) -> io::Result<()>,
            impl FnOnce() -> io::Result<()>,
            impl FnOnce(&Path) -> io::Result<()>,
        ),
        next_log: Option<u64>,
        tracker: &ManagedTracker,
    ) -> io::Result<(StateWrite<'a>, Option<AutomaticReservation>)> {
        let budget = self.disk_budget()?;
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
        self.validate_automatic_selection(&mut state)?;
        let selected = &state
            .selection
            .as_ref()
            .expect("managed selection")
            .checkpoint;
        let root = root(&self.path);
        let indexes = indexes(&root);
        if selected.starts_with(&root) && selected != &indexes[0] && selected != &indexes[1] {
            return Err(invalid(
                "managed selection points to an unknown automatic checkpoint",
            ));
        }
        // Captured readers can only clone the current view. Once an inactive
        // slot's last owner goes away, it cannot acquire a new owner here.
        // Under a disk budget an unlinked but still open slot would hold
        // uncharged space, so a pinned slot is never reused.
        let protect = state.protect_pinned_automatic_slots || budget.is_some();
        let pinned = |slot: usize| {
            protect
                && state
                    .automatic_checkpoint
                    .as_ref()
                    .and_then(|options| options.slot_pins[slot].as_ref())
                    .is_some_and(crate::checkpoint_view::CheckpointPin::is_live)
        };
        let next_slot =
            usize::from(selected == &indexes[0] || (selected != &indexes[1] && pinned(0)));
        if pinned(next_slot) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, PinnedSlot));
        }
        crate::checkpoint::check_log_scan_limit(
            &self.log,
            3,
            state.automatic_checkpoint_log_scan_limit(),
            "automatic checkpoint log scan byte limit exceeded",
        )?;
        let destination = root.join(SLOTS[next_slot]);
        // Keep the writer gate across preparation and publication. The old
        // selection and its open files remain usable while the slot is built.
        let StateWrite { state, _writer } = state;
        drop(state);
        let state = self.read()?;
        let (count, peak, verified_log) = self.verify_for_checkpoint(&state)?;
        let previous = state
            .selection
            .as_ref()
            .expect("managed selection")
            .checkpoint
            .clone();
        let next_selection_path = indexes[next_slot].clone();
        // Admit the whole peak before the first removal or allocation.
        let mut limits = self.checkpoint_view_limits;
        let mut settle = match &budget {
            Some(budget) => {
                let checkpoint_bytes = checkpoint_file_bytes(&state, count)?;
                limits.max_file_bytes = limits.max_file_bytes.min(checkpoint_bytes);
                Some(self.reserve_slot(
                    budget,
                    checkpoint_bytes,
                    &destination,
                    &next_selection_path,
                    &previous,
                    next_log,
                )?)
            }
            None => None,
        };
        crate::cleanup_abandoned_staging(&root)?;
        remove_inactive_slot(&root, &destination)?;
        if let Some(settle) = &settle {
            settle.refresh(&previous, false)?;
        }
        let mut stage = crate::staging::Stage::directory(&root)?;
        write_marker(stage.path(), SLOT_OWNER)?;
        let digest = crate::checkpoint::create_streaming_in_private_stage(
            &self.log,
            count,
            peak,
            stage.path(),
            crate::checkpoint::StreamingOptions {
                limits: Some(limits),
                verified_log,
                tracker: None,
                pending_tail: None,
            },
            |emit| {
                state.view.visit_entries(None, None, |key, location| {
                    emit(key, location)?;
                    Ok(std::ops::ControlFlow::Continue(()))
                })
            },
        )?;
        before_slot_publish()?;
        tracker.checkpoint.begin_publication();
        stage.publish(&destination)?;
        sync_slot_parent(&root).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("automatic checkpoint slot may exist: {error}"),
            )
        })?;
        stage.publication_synchronized();
        let previous_pin = state.view.base.checkpoint_pin();
        let next_view = self.rollover_view(&state, &indexes[next_slot], &digest)?;
        let pin = next_view.base.checkpoint_pin();
        let next = selection::Selection::new(indexes[next_slot].clone(), digest)?;
        drop(state);
        let mut state = self.publish_managed_view(
            next_view,
            next,
            before_selection_rename,
            sync_selection_parent,
            &tracker.selection,
        )?;
        state
            .automatic_checkpoint
            .get_or_insert_with(Default::default)
            .slot_pins[next_slot] = Some(pin);
        // Remove the slot stage's marker before the final measurement. On
        // a failed exit it drops before `settle`, which measures after it.
        drop(stage);
        let reservation = match settle.take() {
            Some(settle) => match settle.finish(&self.path, &next_selection_path, previous_pin) {
                Ok(reservation) => Some(reservation),
                Err(error) => {
                    // The rollover is durable but its charge is unknown.
                    state.failed = true;
                    return Err(error);
                }
            },
            None => None,
        };
        Ok((StateWrite { state, _writer }, reservation))
    }

    // Reserves a slot rollover's peak: the exact checkpoint file, the outer
    // stage's directory marker, the slot's ownership marker, the selection
    // replacement's file marker and temporary copy, and the append's log
    // growth. The inactive slot is removed before anything is allocated, so
    // its measured bytes are credited. Every other remaining file, including
    // the previously selected slot, stays charged.
    fn reserve_slot(
        &self,
        budget: &Arc<DiskBudget>,
        checkpoint_bytes: u64,
        destination: &Path,
        next_selection_path: &Path,
        previous: &Path,
        next_log: Option<u64>,
    ) -> io::Result<Settle> {
        let next = selection::Selection::new(next_selection_path.to_path_buf(), [0; 32])?;
        let bytes = [
            crate::staging::Stage::DIRECTORY_MARKER_BYTES,
            SLOT_OWNER.len() as u64,
            crate::staging::Stage::FILE_MARKER_BYTES,
            next.encoded_len(),
        ]
        .into_iter()
        .try_fold(checkpoint_bytes, u64::checked_add)
        .ok_or_else(|| io::Error::other("automatic checkpoint reservation overflow"))?;
        let credit = crate::staging::tree_bytes(destination)?;
        // An external selection stays charged while old readers keep it.
        let external = if previous.starts_with(&self.path) {
            0
        } else {
            crate::staging::tree_bytes(previous)?
        };
        let log_length = self.log.len()?;
        let reservation = budget.reserve_automatic(bytes, credit, next_log)?;
        Ok(Settle {
            budget: Arc::clone(budget),
            reservation: Some(reservation),
            external,
            previous: previous.to_path_buf(),
            log_length,
        })
    }
}

/// A configuration change's reservation. Settling re-measures the live
/// files whatever the change did; if that fails the store is fenced,
/// because its charge is unknown.
struct ConfigurationCharge {
    budget: Arc<DiskBudget>,
    reservation: AutomaticReservation,
    selected: Option<PathBuf>,
    log_length: u64,
}

impl ConfigurationCharge {
    fn settle(self, state: &mut State) -> io::Result<()> {
        let refreshed = self.budget.refresh_automatic(
            &self.reservation,
            self.log_length,
            self.selected.as_deref(),
            true,
            None,
        );
        if refreshed.is_err() {
            state.failed = true;
        }
        refreshed
    }
}

// Exact length of the checkpoint a rollover of this view writes.
fn checkpoint_file_bytes(state: &State, count: u64) -> io::Result<u64> {
    let mut key_bytes = 0_u64;
    state.view.visit_entries(None, None, |key, _| {
        key_bytes += key.len() as u64;
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
    crate::checkpoint::file_bytes(count, key_bytes)
        .ok_or_else(|| io::Error::other("automatic checkpoint size overflow"))
}

/// Holds an automatic rollover's reservation and re-measures the live files
/// on every exit. A failed exit measures whatever the rollover left, then
/// releases the reservation. A measurement failure marks the budget
/// uncertain, so later admissions fail closed until reopen. The writer gate
/// is held throughout, so the log length cannot change.
struct Settle {
    budget: Arc<DiskBudget>,
    reservation: Option<AutomaticReservation>,
    external: u64,
    previous: PathBuf,
    log_length: u64,
}

impl Settle {
    fn refresh(&self, selected: &Path, published: bool) -> io::Result<()> {
        let reservation = self.reservation.as_ref().expect("armed reservation");
        self.budget.refresh_automatic(
            reservation,
            self.log_length,
            Some(selected),
            published,
            None,
        )
    }

    fn finish(
        mut self,
        store_path: &Path,
        selected: &Path,
        previous_pin: crate::checkpoint_view::CheckpointPin,
    ) -> io::Result<AutomaticReservation> {
        let reservation = self.reservation.take().expect("armed reservation");
        let deselected =
            (!self.previous.starts_with(store_path)).then_some((previous_pin, self.external));
        self.budget.refresh_automatic(
            &reservation,
            self.log_length,
            Some(selected),
            true,
            deselected,
        )?;
        Ok(reservation)
    }
}

impl Drop for Settle {
    fn drop(&mut self) {
        let Some(reservation) = self.reservation.take() else {
            return;
        };
        // A failed rollover may have removed the inactive slot or published
        // a new one. Measure what remains with the old selection, which
        // keeps an external checkpoint charged, before releasing.
        let _ = self.budget.refresh_automatic(
            &reservation,
            self.log_length,
            Some(&self.previous),
            false,
            None,
        );
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
