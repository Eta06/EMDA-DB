//! Blocking, atomic handoff to a verified compacted sibling store.
use super::{LogQuota, Store};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExchangePoint {
    CopyPublished,
    BeforeExchange,
    AfterExchange,
}

pub(super) fn source_directory_is_owned(path: &Path) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        if !["store.lock", "records.log", "commit.state"]
            .iter()
            .any(|allowed| name == *allowed)
            || !entry.file_type()?.is_file()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "store contains unknown files, symlinks or managed checkpoint metadata",
            ));
        }
    }
    Ok(())
}

pub(crate) fn paths(source: &Path, previous: &Path) -> io::Result<(PathBuf, PathBuf, PathBuf)> {
    let source = fs::canonicalize(source)?;
    let parent = source
        .parent()
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "store has no parent directory")
        })?
        .to_path_buf();
    let previous_parent = crate::backup::parent_directory(previous)?;
    if fs::canonicalize(previous_parent)? != parent {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "previous-store path must have the same resolved parent",
        ));
    }
    let previous = parent.join(
        previous
            .file_name()
            .expect("parent_directory validates name"),
    );
    match fs::symlink_metadata(&previous) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "previous-store path already exists",
            ));
        }
    }
    Ok((source, previous, parent))
}

#[cfg(target_os = "linux")]
pub(crate) fn exchange(source: &Path, previous: &Path) -> io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    renameat_with(CWD, source, CWD, previous, RenameFlags::EXCHANGE).map_err(Into::into)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn exchange(_source: &Path, _previous: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic directory exchange currently requires Linux",
    ))
}

impl Store {
    /// Consumes an exclusively owned store, creates a verified compacted sibling,
    /// then atomically exchanges the two directories on Linux. The returned
    /// store owns the compacted data at `source`; the complete old store remains
    /// at `previous` for explicit inspection or deletion. Both paths must have
    /// the same resolved parent, and `previous` must not already exist.
    ///
    /// This is a blocking handoff, not online compaction. No snapshots can
    /// borrow the consumed store. The directory must be trusted, contain only
    /// recognized ordinary-store files, and be accessed only by cooperating
    /// current-version clients. Existing per-open resource limits are preserved.
    /// Checkpoints referencing the old log must be recreated. A failure after
    /// exchange can leave either directory visible; both contain the same live
    /// data, and the caller must reopen and inspect them. Parent synchronization
    /// and the underlying filesystem/device assumptions still apply.
    pub fn compact_and_replace(
        self,
        source: impl AsRef<Path>,
        previous: impl AsRef<Path>,
    ) -> io::Result<Self> {
        self.compact_and_replace_with_hooks(
            source.as_ref(),
            previous.as_ref(),
            |_| Ok(()),
            crate::directory::sync_existing,
        )
    }

    fn compact_and_replace_with_hooks(
        self,
        source: &Path,
        previous: &Path,
        mut hook: impl FnMut(ExchangePoint) -> io::Result<()>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "atomic directory exchange currently requires Linux",
            ));
        }
        let (source, previous, parent) = paths(source, previous)?;
        crate::directory::verify_lock_identity(&source, &self._lock_file)?;
        source_directory_is_owned(&source)?;
        self.compact_to(&previous)?;
        hook(ExchangePoint::CopyPublished)?;
        let candidate = Store::open_with_resource_limits(
            &previous,
            self.log_quota.as_ref().map(LogQuota::limit),
            self.max_live_keys,
            self.max_live_key_bytes,
        )?;
        candidate.set_transaction_staging_limit(self.transaction_budget_status()?.limit_bytes)?;
        candidate.set_active_transaction_limit(self.transaction_budget_status()?.active_limit)?;
        let group_limits = self.group_commit_status()?;
        candidate.set_group_commit_limits(group_limits.limit_bytes, group_limits.limit_records)?;
        crate::directory::verify_lock_identity(&source, &self._lock_file)?;
        crate::directory::verify_lock_identity(&previous, &candidate._lock_file)?;
        hook(ExchangePoint::BeforeExchange)?;
        exchange(&source, &previous)?;
        hook(ExchangePoint::AfterExchange).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("store exchange may have occurred; reopen both paths: {error}"),
            )
        })?;
        sync_parent(&parent).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "store exchange may have occurred; directory synchronization failed: {error}"
                ),
            )
        })?;
        drop(self);
        Ok(candidate)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
