//! Cooperative ownership and explicit cleanup of newly created staging paths.
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

const RESERVED: &[u8] = b".emda-stage-v1-";
const FILE_PREFIX: &str = ".emda-stage-v1-f-";
const DIR_PREFIX: &str = ".emda-stage-v1-d-";
const RETIRED_PREFIX: &str = ".emda-stage-v1-r-";
const RETAINED_PREFIX: &str = ".emda-stage-v1-g-";
const OWNER_SUFFIX: &str = ".owner";
const RETAINED_PENDING_SUFFIX: &str = ".pending";
const PAYLOAD_SUFFIX: &str = ".data";
const FILE_MARKER: &[u8] = b"EMDA-STAGING-V1-FILE\n";
const DIR_MARKER: &[u8] = b"EMDA-STAGING-V1-DIRECTORY\n";
const RETIRED_MARKER: &[u8] = b"EMDA-STAGING-V1-RETIRED\n";
const RETAINED_MARKER: &[u8] = b"EMDA-STAGING-V1-RETAINED\n";
const RANDOM_LEN: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    File,
    Directory,
    Retired,
    Retained,
}

impl Kind {
    fn prefix(self) -> &'static str {
        match self {
            Self::File => FILE_PREFIX,
            Self::Directory => DIR_PREFIX,
            Self::Retired => RETIRED_PREFIX,
            Self::Retained => RETAINED_PREFIX,
        }
    }

    fn marker(self) -> &'static [u8] {
        match self {
            Self::File => FILE_MARKER,
            Self::Directory => DIR_MARKER,
            Self::Retired => RETIRED_MARKER,
            Self::Retained => RETAINED_MARKER,
        }
    }

    fn marker_len(self) -> usize {
        self.marker().len() + if self.is_retired() { 20 } else { 0 }
    }

    fn is_retired(self) -> bool {
        matches!(self, Self::Retired | Self::Retained)
    }
}

// The lock is on the parent directory inode, not a removable lock-file name.
// All new staging operations hold it shared until publication and parent sync
// finish. Cleanup takes it exclusively. External processes must cooperate.
struct ParentLock(File);

// Only this specific pre-cleanup parent lock conflict is safely retryable.
#[derive(Debug)]
struct RetiredParentBusy;
impl std::fmt::Display for RetiredParentBusy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("retired staging parent is busy")
    }
}
impl std::error::Error for RetiredParentBusy {}

pub(crate) fn is_retired_parent_busy(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        && error
            .get_ref()
            .is_some_and(|inner| inner.is::<RetiredParentBusy>())
}

impl ParentLock {
    fn shared(parent: &Path) -> io::Result<Self> {
        let file = File::open(parent)?;
        let result = fs2::FileExt::try_lock_shared(&file);
        #[cfg(test)]
        let result =
            crate::directory::diagnose_lock_failure(result, parent, &file, "staging parent shared");
        result?;
        Ok(Self(file))
    }

    fn exclusive(parent: &Path) -> io::Result<Self> {
        let file = File::open(parent)?;
        let result = fs2::FileExt::try_lock_exclusive(&file);
        #[cfg(test)]
        let result = crate::directory::diagnose_lock_failure(
            result,
            parent,
            &file,
            "staging parent exclusive",
        );
        result?;
        Ok(Self(file))
    }
}

impl Drop for ParentLock {
    fn drop(&mut self) {
        // Explicit unlock avoids an unrelated duplicated descriptor retaining
        // the lock after this operation completes. A killed process can still
        // leave a lock held by a child that inherited its descriptor.
        let _ = fs2::FileExt::unlock(&self.0);
    }
}

pub(crate) fn check_path(path: &Path) -> io::Result<()> {
    if path.components().any(|component| {
        matches!(component, Component::Normal(name) if name.as_bytes().starts_with(RESERVED))
    }) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination uses the reserved staging namespace",
        ));
    }
    Ok(())
}

fn payload_path(marker: &Path) -> PathBuf {
    let mut name = marker.as_os_str().to_os_string();
    name.push(PAYLOAD_SUFFIX);
    PathBuf::from(name)
}

pub(crate) struct Stage {
    parent: ParentLock,
    marker: Option<tempfile::NamedTempFile>,
    payload: PathBuf,
    kind: Kind,
    created: bool,
    published: bool,
    publication_synced: bool,
    preserve_on_drop: bool,
}

impl Stage {
    pub(crate) const DIRECTORY_MARKER_BYTES: u64 = DIR_MARKER.len() as u64;
    pub(crate) const FILE_MARKER_BYTES: u64 = FILE_MARKER.len() as u64;
    pub(crate) const RETAINED_MARKER_BYTES: u64 = (RETAINED_MARKER.len() + 20) as u64;

    fn new(parent: &Path, kind: Kind) -> io::Result<Self> {
        Self::new_with_sync(parent, kind, File::sync_all, File::sync_all)
    }

    fn new_with_sync(
        parent: &Path,
        kind: Kind,
        sync_marker: impl FnOnce(&File) -> io::Result<()>,
        sync_parent: impl FnOnce(&File) -> io::Result<()>,
    ) -> io::Result<Self> {
        Self::new_with_marker(parent, kind, kind.marker(), sync_marker, sync_parent)
    }

    fn new_with_marker(
        parent: &Path,
        kind: Kind,
        content: &[u8],
        sync_marker: impl FnOnce(&File) -> io::Result<()>,
        sync_parent: impl FnOnce(&File) -> io::Result<()>,
    ) -> io::Result<Self> {
        let parent_lock = ParentLock::shared(parent)?;
        let suffix = if kind == Kind::Retained {
            RETAINED_PENDING_SUFFIX
        } else {
            OWNER_SUFFIX
        };
        let mut marker = tempfile::Builder::new()
            .prefix(kind.prefix())
            .suffix(suffix)
            .rand_bytes(RANDOM_LEN)
            .tempfile_in(parent)?;
        marker.write_all(content)?;
        sync_marker(marker.as_file())?;
        if kind == Kind::Retained {
            // A killed process can leave a partial preparation file. Only a
            // complete, synced marker may become visible as an owner.
            let owner = marker.path().with_extension(&OWNER_SUFFIX[1..]);
            crate::backup::publish_noreplace(marker.path(), &owner)?;
            marker.disable_cleanup(true);
            let (file, _) = marker.into_parts();
            marker = tempfile::NamedTempFile::from_parts(
                file,
                tempfile::TempPath::try_from_path(owner)?,
            );
        }
        // Make the ownership marker durable before creating a payload. If a
        // crash leaves payload data, cleanup can find the persisted marker.
        sync_parent(&parent_lock.0)?;
        let payload = payload_path(marker.path());
        Ok(Self {
            parent: parent_lock,
            marker: Some(marker),
            payload,
            kind,
            created: false,
            published: false,
            publication_synced: false,
            preserve_on_drop: false,
        })
    }

    pub(crate) fn file(parent: &Path) -> io::Result<(Self, File)> {
        let mut stage = Self::new(parent, Kind::File)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&stage.payload)?;
        stage.created = true;
        Ok((stage, file))
    }

    pub(crate) fn directory(parent: &Path) -> io::Result<Self> {
        let mut stage = Self::new(parent, Kind::Directory)?;
        fs::DirBuilder::new().mode(0o700).create(&stage.payload)?;
        stage.created = true;
        Ok(stage)
    }

    /// Marks a redundant retired-store stage with the verified replacement's
    /// log identity. Called only after the replacement handoff is durable.
    pub(crate) fn retired_directory(parent: &Path, replacement_id: &[u8; 16]) -> io::Result<Self> {
        Self::replacement_stage(parent, Kind::Retired, replacement_id)
    }

    pub(crate) fn retained_directory(parent: &Path, replacement_id: &[u8; 16]) -> io::Result<Self> {
        Self::replacement_stage(parent, Kind::Retained, replacement_id)
    }

    fn replacement_stage(parent: &Path, kind: Kind, replacement_id: &[u8; 16]) -> io::Result<Self> {
        let mut content = kind.marker().to_vec();
        content.extend_from_slice(replacement_id);
        content.extend_from_slice(&crc32fast::hash(&content).to_le_bytes());
        let mut stage =
            Self::new_with_marker(parent, kind, &content, File::sync_all, File::sync_all)?;
        fs::DirBuilder::new().mode(0o700).create(&stage.payload)?;
        stage.created = true;
        Ok(stage)
    }

    /// Avoid automatic deletion if unwinding while a retired store is staged.
    pub(crate) fn preserve_payload_on_drop(&mut self) {
        self.preserve_on_drop = true;
    }

    /// Return to normal marker cleanup after the retired payload was removed.
    pub(crate) fn finish_retained_cleanup(&mut self) {
        self.preserve_on_drop = false;
    }

    /// Keep a staged payload and its marker for later explicit inspection or cleanup.
    pub(crate) fn retain_payload(mut self) -> io::Result<()> {
        self.created = false;
        if let Some(marker) = self.marker.take() {
            marker.keep().map_err(|error| error.error)?;
        }
        Ok(())
    }

    /// A single non-reserved child of a live, privately owned directory stage.
    pub(crate) fn child_path(&self, name: &OsStr) -> io::Result<PathBuf> {
        let path = Path::new(name);
        if !matches!(self.kind, Kind::Directory)
            || !self.created
            || self.published
            || !matches!(
                (path.components().next(), path.components().nth(1)),
                (Some(Component::Normal(_)), None)
            )
            || name.as_bytes().starts_with(RESERVED)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid staged child",
            ));
        }
        Ok(self.payload.join(name))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.payload
    }

    pub(crate) fn publish(&mut self, destination: &Path) -> io::Result<()> {
        match self.kind {
            Kind::File => tempfile::TempPath::try_from_path(&self.payload)?
                .persist_noclobber(destination)
                .map_err(|error| error.error)?,
            Kind::Directory => {
                crate::backup::publish_noreplace(&self.payload, destination)?;
                self.created = false;
            }
            Kind::Retired | Kind::Retained => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "retired staging cannot be published",
                ));
            }
        }
        // tempfile can fall back to linking and unlinking for file publication;
        // its fallback ignores an unlink failure. Keep responsibility for the
        // old name until Drop confirms it is gone or retains its marker.
        self.published = true;
        Ok(())
    }

    /// Atomically replace a file in the staging parent. The caller must sync
    /// that parent before recording publication as synchronized.
    pub(crate) fn replace_file(&mut self, destination: &Path) -> io::Result<()> {
        if !matches!(self.kind, Kind::File)
            || destination == self.payload.as_path()
            || destination.parent() != self.payload.parent()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "staged file replacement requires another name in the same parent",
            ));
        }
        fs::rename(&self.payload, destination)?;
        self.published = true;
        Ok(())
    }

    /// Record that the parent sync completed after publication. This avoids
    /// another sync if the old stage name was already absent at that point.
    pub(crate) fn publication_synchronized(&mut self) {
        debug_assert!(self.published);
        self.publication_synced = true;
    }

    fn cleanup_on_drop_with_sync(&mut self, sync_parent: impl FnOnce(&File) -> io::Result<()>) {
        if self.marker.is_none() {
            return;
        }
        if self.preserve_on_drop {
            if let Some(marker) = self.marker.take() {
                let _ = marker.keep();
            }
            return;
        }
        let (removed, needs_sync) = if self.created {
            let result = match self.kind {
                Kind::File => fs::remove_file(&self.payload),
                Kind::Directory | Kind::Retired | Kind::Retained => {
                    fs::remove_dir_all(&self.payload)
                }
            };
            match result {
                Ok(()) => (true, true),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    (true, !self.publication_synced)
                }
                Err(_) => (false, false),
            }
        } else {
            (true, !self.publication_synced)
        };
        // The marker must remain discoverable until the payload's removal or
        // publication is durable. Otherwise a crash could persist marker
        // deletion but resurrect its now-unmarked payload. A failed sync keeps
        // the marker for a later explicit cleanup.
        let removal_stable = removed && (!needs_sync || sync_parent(&self.parent.0).is_ok());
        if let Some(marker) = self.marker.take() {
            if removal_stable {
                let _ = marker.close();
            } else {
                let _ = marker.keep();
            }
        }
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        self.cleanup_on_drop_with_sync(File::sync_all);
    }
}

/// Outcome of an explicit staging cleanup in one parent directory.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct StagingCleanup {
    /// Number of abandoned payload files or directories removed.
    pub removed_payloads: usize,
    /// Number of recognized ownership markers removed.
    pub removed_markers: usize,
    /// Marker-shaped entries left untouched because their type or contents
    /// were unexpected, or their corresponding payload had the wrong type.
    pub skipped: usize,
}

/// A failed explicit staging cleanup, classified by this call's
/// removal attempts. Existing stages and unrelated files may still exist in
/// either case. The error does not certify directory durability, cleanup of
/// every stage, source health, or an independent backup. Panics unwind instead
/// of returning a classification.
#[derive(Debug)]
#[non_exhaustive]
pub enum StagingCleanupFailure {
    /// This call began no payload or ownership-marker removal. Synchronization
    /// or other non-removal effects may still have occurred.
    NotStarted(io::Error),
    /// This call may have removed a payload, marker, or part of a directory.
    /// Inspect the trusted directory before deciding how to resume cleanup.
    OutcomeUnknown(io::Error),
}

impl StagingCleanupFailure {
    pub(crate) fn into_io_error(self) -> io::Error {
        match self {
            Self::NotStarted(error) | Self::OutcomeUnknown(error) => error,
        }
    }
}

impl std::fmt::Display for StagingCleanupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStarted(error) => write!(formatter, "staging removal not started: {error}"),
            Self::OutcomeUnknown(error) => {
                write!(formatter, "staging removal outcome unknown: {error}")
            }
        }
    }
}

impl std::error::Error for StagingCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotStarted(error) | Self::OutcomeUnknown(error) => Some(error),
        }
    }
}

fn marker_kind(name: &OsStr) -> Option<Kind> {
    let name = name.to_str()?;
    let (kind, remainder) = if let Some(rest) = name.strip_prefix(FILE_PREFIX) {
        (Kind::File, rest)
    } else if let Some(rest) = name.strip_prefix(DIR_PREFIX) {
        (Kind::Directory, rest)
    } else if let Some(rest) = name.strip_prefix(RETAINED_PREFIX) {
        (Kind::Retained, rest)
    } else {
        (Kind::Retired, name.strip_prefix(RETIRED_PREFIX)?)
    };
    let random = remainder.strip_suffix(OWNER_SUFFIX)?;
    valid_stage_id(random).then_some(kind)
}

fn valid_stage_id(id: &str) -> bool {
    id.len() == RANDOM_LEN && id.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn is_pending_retained_marker(name: &OsStr) -> bool {
    name.to_str()
        .and_then(|name| name.strip_prefix(RETAINED_PREFIX))
        .and_then(|name| name.strip_suffix(RETAINED_PENDING_SUFFIX))
        .is_some_and(valid_stage_id)
}

/// Deletes recognized, abandoned staging payloads in one trusted directory.
///
/// This opt-in operation takes a nonblocking exclusive advisory lock on the
/// parent directory. It returns `WouldBlock` while a cooperating new staging
/// operation holds a shared lock. It only processes matching versioned markers
/// and their associated payloads, never follows a payload symlink, and does not
/// touch old `.db-*` staging names. Both parent and entries must be controlled
/// by a trusted caller: other software or older engine versions do not honor
/// the lock, and concurrent external renames or replacements are unsupported.
/// The reserved `.emda-stage-v1-` namespace must not contain user data. A
/// cleanup error may follow partial deletion; the directory is synchronized
/// before return where possible, and callers may run cleanup again. This is not
/// secure erasure, an automatic janitor, or a total disk-usage quota.
/// An unchanged directory is not synchronized; a successful no-op does not
/// establish that the filesystem supports directory synchronization.
/// Abandoned retained-marker preparation files can be removed under the
/// parent lock. Preparation files never own a payload.
///
/// Retired-store markers are skipped. Verify their replacement through
/// `OnlineStore::cleanup_retired_staging` or the corresponding retained
/// catalog's `reclaim_inactive` before their contents are removed.
///
/// A process killed before it finishes can leave a marker or payload behind.
/// Published destinations are outside the reserved namespace and are untouched.
pub fn cleanup_abandoned_staging(parent: impl AsRef<Path>) -> io::Result<StagingCleanup> {
    cleanup_with_sync(parent.as_ref(), File::sync_all)
}

/// Removes recognized generic stages with a conservative removal outcome on
/// failure. The same trusted-directory, cooperative-locking, marker, and
/// durability requirements as [`cleanup_abandoned_staging`] apply. A
/// `NotStarted` error only concerns this call's removal attempts; `OutcomeUnknown`
/// requires inspecting the directory before resuming. Neither result protects
/// against external mutation or establishes source health or secure erasure.
pub fn cleanup_abandoned_staging_detailed(
    parent: impl AsRef<Path>,
) -> Result<StagingCleanup, StagingCleanupFailure> {
    cleanup_detailed_with_hooks(parent.as_ref(), File::sync_all, File::sync_all, || Ok(()))
}

fn cleanup_detailed_with_hooks(
    parent: &Path,
    sync_payload_removal: impl FnMut(&File) -> io::Result<()>,
    sync_parent: impl FnOnce(&File) -> io::Result<()>,
    before_remove: impl FnMut() -> io::Result<()>,
) -> Result<StagingCleanup, StagingCleanupFailure> {
    cleanup_selected_detailed(
        parent,
        None,
        sync_payload_removal,
        sync_parent,
        before_remove,
    )
}

fn cleanup_selected_detailed(
    parent: &Path,
    replacement: Option<(Kind, &[u8; 16])>,
    sync_payload_removal: impl FnMut(&File) -> io::Result<()>,
    sync_parent: impl FnOnce(&File) -> io::Result<()>,
    mut before_remove: impl FnMut() -> io::Result<()>,
) -> Result<StagingCleanup, StagingCleanupFailure> {
    let mut removal_started = false;
    cleanup_selected_with_syncs(
        parent,
        replacement,
        sync_payload_removal,
        sync_parent,
        || {
            before_remove()?;
            removal_started = true;
            Ok(())
        },
    )
    .map_err(|error| {
        if removal_started {
            StagingCleanupFailure::OutcomeUnknown(error)
        } else {
            StagingCleanupFailure::NotStarted(error)
        }
    })
}

fn cleanup_with_sync(
    parent: &Path,
    sync: impl FnOnce(&File) -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    cleanup_selected_with_sync(parent, None, sync, || Ok(()))
}

/// Removes only marked retired copies belonging to one verified replacement.
/// Caller must hold the replacement's writer barrier and verify its witness.
pub(crate) fn cleanup_retired_detailed(
    parent: &Path,
    replacement_id: &[u8; 16],
    verify: impl FnOnce() -> io::Result<()>,
    sync_payload_removal: impl FnMut(&File) -> io::Result<()>,
    sync_parent: impl FnOnce(&File) -> io::Result<()>,
) -> Result<StagingCleanup, StagingCleanupFailure> {
    let mut verify = Some(verify);
    cleanup_selected_detailed(
        parent,
        Some((Kind::Retired, replacement_id)),
        sync_payload_removal,
        sync_parent,
        || {
            if let Some(verify) = verify.take() {
                verify()?;
            }
            Ok(())
        },
    )
}

pub(crate) fn cleanup_retained(
    parent: &Path,
    replacement_id: &[u8; 16],
    verify: impl FnOnce() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    let mut verify = Some(verify);
    cleanup_selected_with_sync(
        parent,
        Some((Kind::Retained, replacement_id)),
        File::sync_all,
        || verify.take().map_or(Ok(()), |verify| verify()),
    )
}

pub(crate) fn has_retained_stage(parent: &Path) -> io::Result<bool> {
    for entry in fs::read_dir(parent)? {
        if marker_kind(&entry?.file_name()) == Some(Kind::Retained) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
fn cleanup_retired(
    parent: &Path,
    replacement_id: &[u8; 16],
    verify: impl FnOnce() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    cleanup_retired_detailed(
        parent,
        replacement_id,
        verify,
        File::sync_all,
        File::sync_all,
    )
    .map_err(StagingCleanupFailure::into_io_error)
}

fn cleanup_selected_with_sync(
    parent: &Path,
    replacement: Option<(Kind, &[u8; 16])>,
    sync: impl FnOnce(&File) -> io::Result<()>,
    before_remove: impl FnMut() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    cleanup_selected_with_syncs(parent, replacement, File::sync_all, sync, before_remove)
}

fn cleanup_selected_with_syncs(
    parent: &Path,
    replacement: Option<(Kind, &[u8; 16])>,
    mut sync_payload_removal: impl FnMut(&File) -> io::Result<()>,
    sync: impl FnOnce(&File) -> io::Result<()>,
    before_remove: impl FnMut() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    check_path(parent)?;
    let guard = match ParentLock::exclusive(parent) {
        Ok(guard) => guard,
        Err(error) if replacement.is_some() && error.kind() == io::ErrorKind::WouldBlock => {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, RetiredParentBusy));
        }
        Err(error) => return Err(error),
    };
    let result = cleanup_locked(parent, replacement, before_remove, || {
        sync_payload_removal(&guard.0)
    });
    if matches!(&result, Ok(report) if report.removed_payloads == 0 && report.removed_markers == 0)
    {
        return result;
    }
    let synchronized = sync(&guard.0);
    match (result, synchronized) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(io::Error::new(
            error.kind(),
            format!("staging cleanup may be incomplete after directory sync failure: {error}"),
        )),
        (Ok(report), Ok(())) => Ok(report),
    }
}

fn cleanup_locked(
    parent: &Path,
    replacement: Option<(Kind, &[u8; 16])>,
    mut before_remove: impl FnMut() -> io::Result<()>,
    mut sync_payload_removal: impl FnMut() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    let mut report = StagingCleanup::default();
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        if replacement.is_none() && is_pending_retained_marker(&name) {
            let meta = fs::symlink_metadata(entry.path())?;
            if meta.file_type().is_file() && meta.len() <= Kind::Retained.marker_len() as u64 {
                // A preparation name never owns a payload. The exclusive
                // parent lock excludes a cooperating marker writer.
                before_remove()?;
                fs::remove_file(entry.path())?;
                report.removed_markers += 1;
            } else {
                report.skipped += 1;
            }
            continue;
        }
        let Some(kind) = marker_kind(&name) else {
            continue;
        };
        if replacement.is_none() && kind.is_retired() {
            report.skipped += 1;
            continue;
        }
        if replacement.is_some_and(|(selected, _)| kind != selected) {
            continue;
        }
        let marker = entry.path();
        let meta = fs::symlink_metadata(&marker)?;
        if !meta.file_type().is_file() || meta.len() != kind.marker_len() as u64 {
            report.skipped += 1;
            continue;
        }
        let mut content = Vec::new();
        File::open(&marker)?
            .take(kind.marker_len() as u64 + 1)
            .read_to_end(&mut content)?;
        let valid = if kind.is_retired() {
            let split = content.len().saturating_sub(4);
            content.starts_with(kind.marker())
                && content.len() == kind.marker_len()
                && crc32fast::hash(&content[..split])
                    == u32::from_le_bytes(content[split..].try_into().expect("four checksum bytes"))
        } else {
            content == kind.marker()
        };
        if !valid {
            report.skipped += 1;
            continue;
        }
        if let Some((_, expected)) = replacement {
            let prefix_len = kind.marker().len();
            if &content[prefix_len..prefix_len + 16] != expected {
                continue;
            }
        }
        let payload = payload_path(&marker);
        match fs::symlink_metadata(&payload) {
            Ok(meta) => {
                let expected = match kind {
                    Kind::File => meta.file_type().is_file(),
                    Kind::Directory | Kind::Retired | Kind::Retained => meta.file_type().is_dir(),
                };
                if !expected {
                    report.skipped += 1;
                    continue;
                }
                // A selective cleanup must not remove unknown files or an
                // actively owned retired store. Keep its lock through unlink.
                let retired_lock = if kind.is_retired() {
                    match lock_safe_retired_payload(&payload, kind)? {
                        Some(lock) => lock,
                        None => {
                            report.skipped += 1;
                            continue;
                        }
                    }
                } else {
                    None
                };
                if kind.is_retired() {
                    // The initial move may have been interrupted before either
                    // directory was synchronized. Preserve the old files
                    // until the move is durable.
                    File::open(&payload)?.sync_all()?;
                    sync_payload_removal()?;
                }
                before_remove()?;
                match kind {
                    Kind::File => fs::remove_file(&payload)?,
                    Kind::Directory => fs::remove_dir_all(&payload)?,
                    Kind::Retired | Kind::Retained => {
                        remove_retired_stage(&payload, kind, |_| Ok(()))?;
                    }
                }
                drop(retired_lock);
                report.removed_payloads += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        // Synchronize the absence of the payload before unlinking its owner
        // marker, even when it was already absent in the current namespace.
        // If this fails, a retry can still identify any resurrected payload.
        sync_payload_removal().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("staging payload removal may be incomplete; marker retained: {error}"),
            )
        })?;
        before_remove()?;
        fs::remove_file(marker)?;
        report.removed_markers += 1;
    }
    Ok(report)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetiredRemovalPoint {
    Base,
    Records,
    Witness,
    RetiredFilesSync,
    Lock,
    RetiredSync,
    RetiredDirectory,
    StageSync,
    StageDirectory,
}

// Keep the lock until the log and witness are unlinked. This makes cleanup
// resumable after a process interruption while preserving unexpected entries.
fn remove_retired_stage(
    stage: &Path,
    kind: Kind,
    mut hook: impl FnMut(RetiredRemovalPoint) -> io::Result<()>,
) -> io::Result<()> {
    let retired = stage.join("retired");
    if retired.exists() {
        for (name, point) in [
            ("base", RetiredRemovalPoint::Base),
            ("records.log", RetiredRemovalPoint::Records),
            ("commit.state", RetiredRemovalPoint::Witness),
            ("store.lock", RetiredRemovalPoint::Lock),
        ] {
            if kind == Kind::Retired && point == RetiredRemovalPoint::Base {
                continue;
            }
            if point == RetiredRemovalPoint::Lock {
                // Keep the lock name recoverable until the file removal is durable.
                File::open(&retired)?.sync_all()?;
                hook(RetiredRemovalPoint::RetiredFilesSync)?;
            }
            match fs::remove_file(retired.join(name)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            hook(point)?;
        }
        File::open(&retired)?.sync_all()?;
        hook(RetiredRemovalPoint::RetiredSync)?;
        fs::remove_dir(&retired)?;
        hook(RetiredRemovalPoint::RetiredDirectory)?;
    }
    File::open(stage)?.sync_all()?;
    hook(RetiredRemovalPoint::StageSync)?;
    fs::remove_dir(stage)?;
    hook(RetiredRemovalPoint::StageDirectory)?;
    Ok(())
}

pub(crate) fn remove_retained_stage(stage: &Path) -> io::Result<()> {
    remove_retired_stage(stage, Kind::Retained, |_| Ok(()))
}

// Outer Option: None means unexpected contents or a busy lock. Inner Option:
// an empty stage or fully unlinked retired directory needs no lock descriptor.
fn lock_safe_retired_payload(
    stage: &Path,
    kind: Kind,
) -> io::Result<Option<Option<crate::store_lock::StoreLock>>> {
    let mut retired = None;
    for entry in fs::read_dir(stage)? {
        let entry = entry?;
        if entry.file_name() != "retired" || !entry.file_type()?.is_dir() || retired.is_some() {
            return Ok(None);
        }
        retired = Some(entry.path());
    }
    let Some(retired) = retired else {
        return Ok(Some(None));
    };
    let mut has_lock = false;
    let mut has_other = false;
    let known: &[_] = if kind == Kind::Retained {
        &["base", "records.log", "commit.state", "store.lock"]
    } else {
        &["records.log", "commit.state", "store.lock"]
    };
    for entry in fs::read_dir(&retired)? {
        let entry = entry?;
        let name = entry.file_name();
        if !known.iter().any(|known| name == *known) || !entry.file_type()?.is_file() {
            return Ok(None);
        }
        if name == "store.lock" {
            has_lock = true;
        } else {
            has_other = true;
        }
    }
    if !has_lock {
        return Ok((!has_other).then_some(None));
    }
    let lock = File::open(retired.join("store.lock"))?;
    let lock_result = fs2::FileExt::try_lock_exclusive(&lock);
    #[cfg(test)]
    let lock_result = crate::directory::diagnose_lock_failure(
        lock_result,
        &retired.join("store.lock"),
        &lock,
        "retired store exclusive",
    );
    match lock_result {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
        Err(error) => return Err(error),
    }
    let lock = crate::store_lock::StoreLock::acquired(lock);
    lock.verify_identity(&retired)?;
    Ok(Some(Some(lock)))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ordering_tests;

#[cfg(test)]
mod cleanup_outcome_tests;
