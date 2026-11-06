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
const OWNER_SUFFIX: &str = ".owner";
const PAYLOAD_SUFFIX: &str = ".data";
const FILE_MARKER: &[u8] = b"EMDA-STAGING-V1-FILE\n";
const DIR_MARKER: &[u8] = b"EMDA-STAGING-V1-DIRECTORY\n";
const RETIRED_MARKER: &[u8] = b"EMDA-STAGING-V1-RETIRED\n";
const RANDOM_LEN: usize = 16;

#[derive(Clone, Copy)]
enum Kind {
    File,
    Directory,
    Retired,
}

impl Kind {
    fn prefix(self) -> &'static str {
        match self {
            Self::File => FILE_PREFIX,
            Self::Directory => DIR_PREFIX,
            Self::Retired => RETIRED_PREFIX,
        }
    }

    fn marker(self) -> &'static [u8] {
        match self {
            Self::File => FILE_MARKER,
            Self::Directory => DIR_MARKER,
            Self::Retired => RETIRED_MARKER,
        }
    }

    fn marker_len(self) -> usize {
        self.marker().len() + if matches!(self, Self::Retired) { 20 } else { 0 }
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
        fs2::FileExt::try_lock_shared(&file)?;
        Ok(Self(file))
    }

    fn exclusive(parent: &Path) -> io::Result<Self> {
        let file = File::open(parent)?;
        fs2::FileExt::try_lock_exclusive(&file)?;
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
        let mut marker = tempfile::Builder::new()
            .prefix(kind.prefix())
            .suffix(OWNER_SUFFIX)
            .rand_bytes(RANDOM_LEN)
            .tempfile_in(parent)?;
        marker.write_all(content)?;
        sync_marker(marker.as_file())?;
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
        let mut content = RETIRED_MARKER.to_vec();
        content.extend_from_slice(replacement_id);
        content.extend_from_slice(&crc32fast::hash(&content).to_le_bytes());
        let mut stage = Self::new_with_marker(
            parent,
            Kind::Retired,
            &content,
            File::sync_all,
            File::sync_all,
        )?;
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

    pub(crate) fn path(&self) -> &Path {
        &self.payload
    }

    pub(crate) fn publish(&mut self, destination: &Path) -> io::Result<()> {
        match self.kind {
            Kind::File => tempfile::TempPath::try_from_path(&self.payload)?
                .persist_noclobber(destination)
                .map_err(|error| error.error)?,
            Kind::Directory => {
                crate::backup::publish_directory(&self.payload, destination)?;
                self.created = false;
            }
            Kind::Retired => {
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
                Kind::Directory | Kind::Retired => fs::remove_dir_all(&self.payload),
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

fn marker_kind(name: &OsStr) -> Option<Kind> {
    let name = name.to_str()?;
    let (kind, remainder) = if let Some(rest) = name.strip_prefix(FILE_PREFIX) {
        (Kind::File, rest)
    } else if let Some(rest) = name.strip_prefix(DIR_PREFIX) {
        (Kind::Directory, rest)
    } else {
        (Kind::Retired, name.strip_prefix(RETIRED_PREFIX)?)
    };
    let random = remainder.strip_suffix(OWNER_SUFFIX)?;
    (random.len() == RANDOM_LEN && random.bytes().all(|byte| byte.is_ascii_alphanumeric()))
        .then_some(kind)
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
/// Retired-store markers are skipped: their replacement must be verified with
/// `OnlineStore::cleanup_retired_staging` before their contents are removed.
///
/// A process killed before it finishes can leave a marker or payload behind.
/// Published destinations are outside the reserved namespace and are untouched.
pub fn cleanup_abandoned_staging(parent: impl AsRef<Path>) -> io::Result<StagingCleanup> {
    cleanup_with_sync(parent.as_ref(), File::sync_all)
}

fn cleanup_with_sync(
    parent: &Path,
    sync: impl FnOnce(&File) -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    cleanup_selected_with_sync(parent, None, sync, || Ok(()))
}

/// Removes only marked retired copies belonging to one verified replacement.
/// Caller must hold the replacement's writer barrier and verify its witness.
pub(crate) fn cleanup_retired(
    parent: &Path,
    replacement_id: &[u8; 16],
    verify: impl FnOnce() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    let mut verify = Some(verify);
    cleanup_selected_with_sync(parent, Some(replacement_id), File::sync_all, || {
        if let Some(verify) = verify.take() {
            verify()?;
        }
        Ok(())
    })
}

fn cleanup_selected_with_sync(
    parent: &Path,
    replacement_id: Option<&[u8; 16]>,
    sync: impl FnOnce(&File) -> io::Result<()>,
    before_remove: impl FnMut() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    cleanup_selected_with_syncs(parent, replacement_id, File::sync_all, sync, before_remove)
}

fn cleanup_selected_with_syncs(
    parent: &Path,
    replacement_id: Option<&[u8; 16]>,
    mut sync_payload_removal: impl FnMut(&File) -> io::Result<()>,
    sync: impl FnOnce(&File) -> io::Result<()>,
    before_remove: impl FnMut() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    check_path(parent)?;
    let guard = match ParentLock::exclusive(parent) {
        Ok(guard) => guard,
        Err(error) if replacement_id.is_some() && error.kind() == io::ErrorKind::WouldBlock => {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, RetiredParentBusy));
        }
        Err(error) => return Err(error),
    };
    let result = cleanup_locked(parent, replacement_id, before_remove, || {
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
    replacement_id: Option<&[u8; 16]>,
    mut before_remove: impl FnMut() -> io::Result<()>,
    mut sync_payload_removal: impl FnMut() -> io::Result<()>,
) -> io::Result<StagingCleanup> {
    let mut report = StagingCleanup::default();
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let Some(kind) = marker_kind(&entry.file_name()) else {
            continue;
        };
        if replacement_id.is_none() && matches!(kind, Kind::Retired) {
            report.skipped += 1;
            continue;
        }
        if replacement_id.is_some() && !matches!(kind, Kind::Retired) {
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
        let valid = if matches!(kind, Kind::Retired) {
            let split = content.len().saturating_sub(4);
            content.starts_with(RETIRED_MARKER)
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
        if let Some(expected) = replacement_id {
            if &content[RETIRED_MARKER.len()..RETIRED_MARKER.len() + 16] != expected {
                continue;
            }
        }
        let payload = payload_path(&marker);
        match fs::symlink_metadata(&payload) {
            Ok(meta) => {
                let expected = match kind {
                    Kind::File => meta.file_type().is_file(),
                    Kind::Directory | Kind::Retired => meta.file_type().is_dir(),
                };
                if !expected {
                    report.skipped += 1;
                    continue;
                }
                // A selective cleanup must not remove unknown files or an
                // actively owned retired store. Keep its lock through unlink.
                let retired_lock = if replacement_id.is_some() {
                    match lock_safe_retired_payload(&payload)? {
                        Some(lock) => lock,
                        None => {
                            report.skipped += 1;
                            continue;
                        }
                    }
                } else {
                    None
                };
                before_remove()?;
                match kind {
                    Kind::File => fs::remove_file(&payload)?,
                    Kind::Directory => fs::remove_dir_all(&payload)?,
                    Kind::Retired if replacement_id.is_some() => {
                        remove_retired_stage(&payload, |_| Ok(()))?;
                    }
                    Kind::Retired => unreachable!("generic cleanup skips retired stages"),
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
    Records,
    Witness,
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
    mut hook: impl FnMut(RetiredRemovalPoint) -> io::Result<()>,
) -> io::Result<()> {
    let retired = stage.join("retired");
    if retired.exists() {
        for (name, point) in [
            ("records.log", RetiredRemovalPoint::Records),
            ("commit.state", RetiredRemovalPoint::Witness),
            ("store.lock", RetiredRemovalPoint::Lock),
        ] {
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

// Outer Option: None means unexpected contents or a busy lock. Inner Option:
// an empty stage or fully unlinked retired directory needs no lock descriptor.
fn lock_safe_retired_payload(stage: &Path) -> io::Result<Option<Option<File>>> {
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
    for entry in fs::read_dir(&retired)? {
        let entry = entry?;
        let name = entry.file_name();
        if !["records.log", "commit.state", "store.lock"]
            .iter()
            .any(|known| name == *known)
            || !entry.file_type()?.is_file()
        {
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
    match fs2::FileExt::try_lock_exclusive(&lock) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
        Err(error) => return Err(error),
    }
    crate::directory::verify_lock_identity(&retired, &lock)?;
    Ok(Some(Some(lock)))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ordering_tests;
