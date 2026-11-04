//! Blocking checkpoint-store handoff with persistent managed configuration.
use super::{CheckpointStore, automatic, selection};
use std::fs;
use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExchangePoint {
    CopyPublished,
    SelectionBeforeRename,
    SelectionBeforeSync,
    CandidateReady,
    AfterExchange,
}

fn source_directory_is_owned(path: &Path) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let kind = entry.file_type()?;
        let known_file = [
            "store.lock",
            "records.log",
            "commit.state",
            "checkpoint.selection",
            crate::COMPACTED_CHECKPOINT_FILE,
        ]
        .iter()
        .any(|allowed| name == *allowed)
            && kind.is_file();
        let automatic = automatic::root(path)
            .file_name()
            .is_some_and(|allowed| name == allowed)
            && kind.is_dir();
        if !known_file && !automatic {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint store contains unknown files or symlinks",
            ));
        }
    }
    Ok(())
}

impl CheckpointStore {
    /// Consumes this checkpoint-backed store and atomically exchanges it with
    /// a verified compacted sibling on Linux. `previous` must not exist and
    /// must have the same resolved parent as the source. The returned store
    /// stays at the original path; the previous full log remains at `previous`.
    /// The returned digest identifies the new `COMPACTED_CHECKPOINT_FILE` and
    /// must be protected separately for an unmanaged store.
    ///
    /// Managed selection and the enabled/disabled automatic setting are
    /// retained. The operation blocks access, preserves per-open limits, and
    /// requires trusted directories and cooperating current-version clients.
    /// Unknown source files are rejected. Old selections or external
    /// checkpoints may no longer be usable at their previous paths; ordinary
    /// full-log recovery can open the retained copy. A failure after exchange
    /// has an uncertain publication outcome. This is not online compaction,
    /// secure erasure, a total-memory limit or proof of power-loss safety.
    pub fn compact_and_replace(self, previous: impl AsRef<Path>) -> io::Result<(Self, [u8; 32])> {
        self.compact_and_replace_with_hooks(
            previous.as_ref(),
            |_| Ok(()),
            crate::directory::sync_existing,
        )
    }

    fn compact_and_replace_with_hooks(
        self,
        previous: &Path,
        hook: impl FnMut(ExchangePoint) -> io::Result<()>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<(Self, [u8; 32])> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "atomic directory exchange currently requires Linux",
            ));
        }
        let hook = std::cell::RefCell::new(hook);
        let (source, previous, parent) =
            crate::store::compact_exchange::paths(&self.path, previous)?;
        let state = self.read()?;
        let original_selection = state.selection.clone();
        let enabled = state.automatic;
        let original_lock = state.view.base.duplicate_store_lock()?;
        crate::directory::verify_lock_identity(&source, &original_lock)?;
        if selection::load(&source)? != original_selection {
            return Err(io::Error::other("managed checkpoint selection changed"));
        }
        let configured = automatic::open_existing(&source)?;
        if (configured == Some(true)) != enabled
            || (configured.is_some() && original_selection.is_none())
        {
            return Err(io::Error::other(
                "automatic checkpoint configuration changed",
            ));
        }
        drop(state);
        source_directory_is_owned(&source)?;
        let digest = self.compact_to(&previous)?;
        (hook.borrow_mut())(ExchangePoint::CopyPublished)?;
        let mut candidate = Self::open_with_resource_limits(
            &previous,
            previous.join(crate::COMPACTED_CHECKPOINT_FILE),
            &digest,
            self.max_tail_bytes,
            self.max_tail_keys,
            self.max_tail_key_bytes,
            self.max_log_bytes,
        )?;
        if original_selection.is_some() {
            // The file's final absolute path is used before exchange. Nobody
            // may use this private candidate until its directory is exchanged.
            let next =
                selection::Selection::new(source.join(crate::COMPACTED_CHECKPOINT_FILE), digest)?;
            selection::publish(
                &previous,
                &next,
                || (hook.borrow_mut())(ExchangePoint::SelectionBeforeRename),
                |path| {
                    (hook.borrow_mut())(ExchangePoint::SelectionBeforeSync)?;
                    selection::sync_ancestors(path)
                },
            )?;
            candidate.write()?.selection = Some(next);
            if enabled {
                candidate.enable_automatic_checkpoints()?;
            }
        }
        let candidate_lock = candidate.read()?.view.base.duplicate_store_lock()?;
        crate::directory::verify_lock_identity(&source, &original_lock)?;
        crate::directory::verify_lock_identity(&previous, &candidate_lock)?;
        (hook.borrow_mut())(ExchangePoint::CandidateReady)?;
        crate::store::compact_exchange::exchange(&source, &previous)?;
        (hook.borrow_mut())(ExchangePoint::AfterExchange).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("checkpoint-store exchange may have occurred; reopen both paths: {error}"),
            )
        })?;
        sync_parent(&parent).map_err(|error|io::Error::new(error.kind(),format!("checkpoint-store exchange may have occurred; directory synchronization failed: {error}")))?;
        candidate.path = source;
        drop(original_lock);
        drop(self);
        drop(candidate_lock);
        Ok((candidate, digest))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
