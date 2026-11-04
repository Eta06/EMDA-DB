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
const OWNER_SUFFIX: &str = ".owner";
const PAYLOAD_SUFFIX: &str = ".data";
const FILE_MARKER: &[u8] = b"EMDA-STAGING-V1-FILE\n";
const DIR_MARKER: &[u8] = b"EMDA-STAGING-V1-DIRECTORY\n";
const RANDOM_LEN: usize = 16;

#[derive(Clone, Copy)]
enum Kind {
    File,
    Directory,
}

impl Kind {
    fn prefix(self) -> &'static str {
        match self {
            Self::File => FILE_PREFIX,
            Self::Directory => DIR_PREFIX,
        }
    }

    fn marker(self) -> &'static [u8] {
        match self {
            Self::File => FILE_MARKER,
            Self::Directory => DIR_MARKER,
        }
    }
}

// The lock is on the parent directory inode, not a removable lock-file name.
// All new staging operations hold it shared until publication and parent sync
// finish. Cleanup takes it exclusively. External processes must cooperate.
struct ParentLock(File);

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
        let parent_lock = ParentLock::shared(parent)?;
        let mut marker = tempfile::Builder::new()
            .prefix(kind.prefix())
            .suffix(OWNER_SUFFIX)
            .rand_bytes(RANDOM_LEN)
            .tempfile_in(parent)?;
        marker.write_all(kind.marker())?;
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
        }
        // tempfile can fall back to linking and unlinking for file publication;
        // its fallback ignores an unlink failure. Keep responsibility for the
        // old name until Drop confirms it is gone or retains its marker.
        Ok(())
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if self.preserve_on_drop {
            if let Some(marker) = self.marker.take() {
                let _ = marker.keep();
            }
            return;
        }
        let removed = if self.created {
            let result = match self.kind {
                Kind::File => fs::remove_file(&self.payload),
                Kind::Directory => fs::remove_dir_all(&self.payload),
            };
            result.is_ok() || result.is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
        } else {
            true
        };
        if let Some(marker) = self.marker.take() {
            if removed {
                let _ = marker.close();
            } else {
                // Retain ownership information if normal cleanup fails.
                let _ = marker.keep();
            }
        }
        // Access the guard to make its lifetime through marker removal clear.
        let _ = &self.parent;
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
    } else {
        (Kind::Directory, name.strip_prefix(DIR_PREFIX)?)
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
    check_path(parent)?;
    let guard = ParentLock::exclusive(parent)?;
    let result = cleanup_locked(parent);
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

fn cleanup_locked(parent: &Path) -> io::Result<StagingCleanup> {
    let mut report = StagingCleanup::default();
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let Some(kind) = marker_kind(&entry.file_name()) else {
            continue;
        };
        let marker = entry.path();
        let meta = fs::symlink_metadata(&marker)?;
        if !meta.file_type().is_file() || meta.len() != kind.marker().len() as u64 {
            report.skipped += 1;
            continue;
        }
        let mut content = Vec::new();
        File::open(&marker)?
            .take(kind.marker().len() as u64 + 1)
            .read_to_end(&mut content)?;
        if content != kind.marker() {
            report.skipped += 1;
            continue;
        }
        let payload = payload_path(&marker);
        match fs::symlink_metadata(&payload) {
            Ok(meta) => {
                let expected = match kind {
                    Kind::File => meta.file_type().is_file(),
                    Kind::Directory => meta.file_type().is_dir(),
                };
                if !expected {
                    report.skipped += 1;
                    continue;
                }
                match kind {
                    Kind::File => fs::remove_file(&payload)?,
                    Kind::Directory => fs::remove_dir_all(&payload)?,
                }
                report.removed_payloads += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::remove_file(marker)?;
        report.removed_markers += 1;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn orphan(mut stage: Stage) -> PathBuf {
        let path = stage.payload.clone();
        stage.created = false;
        stage.marker.take().unwrap().keep().unwrap();
        drop(stage);
        path
    }

    #[test]
    fn active_operations_block_cleanup_and_orphans_can_be_removed() {
        let directory = tempfile::tempdir().unwrap();
        let (file_stage, mut file) = Stage::file(directory.path()).unwrap();
        file.write_all(b"sensitive backup").unwrap();
        let dir_stage = Stage::directory(directory.path()).unwrap();
        fs::write(dir_stage.path().join("data"), b"sensitive store").unwrap();
        assert_eq!(
            cleanup_abandoned_staging(directory.path())
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let file_path = orphan(file_stage);
        assert_eq!(
            cleanup_abandoned_staging(directory.path())
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(file_path.exists());
        let dir_path = orphan(dir_stage);
        drop(file);
        assert_eq!(
            cleanup_abandoned_staging(directory.path()).unwrap(),
            StagingCleanup {
                removed_payloads: 2,
                removed_markers: 2,
                skipped: 0,
            }
        );
        assert!(!file_path.exists());
        assert!(!dir_path.exists());
        assert_eq!(
            cleanup_abandoned_staging(directory.path()).unwrap(),
            StagingCleanup::default()
        );
    }

    #[test]
    fn cleanup_preserves_unknown_legacy_and_symlink_entries() {
        let directory = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("sentinel"), b"preserve").unwrap();
        let legacy = directory.path().join(".db-backup-old");
        fs::write(&legacy, b"old data").unwrap();
        let bogus = directory
            .path()
            .join(".emda-stage-v1-f-1234567890123456.owner");
        fs::write(&bogus, b"unknown").unwrap();
        fs::write(payload_path(&bogus), b"unowned").unwrap();
        let link_marker = directory
            .path()
            .join(".emda-stage-v1-d-1234567890123456.owner");
        fs::write(&link_marker, DIR_MARKER).unwrap();
        symlink(outside.path(), payload_path(&link_marker)).unwrap();
        let marker_link = directory
            .path()
            .join(".emda-stage-v1-f-abcdefghijklmnop.owner");
        symlink(&bogus, &marker_link).unwrap();
        let report = cleanup_abandoned_staging(directory.path()).unwrap();
        assert_eq!(report.removed_payloads, 0);
        assert_eq!(report.removed_markers, 0);
        assert_eq!(report.skipped, 3);
        assert_eq!(fs::read(&legacy).unwrap(), b"old data");
        assert_eq!(fs::read(payload_path(&bogus)).unwrap(), b"unowned");
        assert_eq!(
            fs::read(outside.path().join("sentinel")).unwrap(),
            b"preserve"
        );
        assert!(link_marker.exists());
    }

    #[test]
    fn cleanup_removes_orphan_marker_and_reports_sync_failure() {
        let directory = tempfile::tempdir().unwrap();
        let (stage, file) = Stage::file(directory.path()).unwrap();
        let payload = orphan(stage);
        drop(file);
        fs::remove_file(payload).unwrap();
        let error = cleanup_with_sync(directory.path(), |_| {
            Err(io::Error::other(
                "injected directory synchronization failure",
            ))
        })
        .unwrap_err();
        assert!(error.to_string().contains("may be incomplete"));
        assert_eq!(
            cleanup_abandoned_staging(directory.path()).unwrap(),
            StagingCleanup::default()
        );
    }

    #[test]
    fn cleanup_never_follows_links_inside_a_staging_directory() {
        let directory = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("sentinel"), b"preserve").unwrap();
        let stage = Stage::directory(directory.path()).unwrap();
        symlink(outside.path(), stage.path().join("link")).unwrap();
        orphan(stage);
        let report = cleanup_abandoned_staging(directory.path()).unwrap();
        assert_eq!(report.removed_payloads, 1);
        assert_eq!(
            fs::read(outside.path().join("sentinel")).unwrap(),
            b"preserve"
        );
    }

    #[test]
    fn cleanup_only_removes_the_staging_name_of_a_published_hard_link() {
        let directory = tempfile::tempdir().unwrap();
        let (stage, mut file) = Stage::file(directory.path()).unwrap();
        file.write_all(b"published data").unwrap();
        let published = directory.path().join("published");
        fs::hard_link(stage.path(), &published).unwrap();
        let payload = orphan(stage);
        drop(file);
        let report = cleanup_abandoned_staging(directory.path()).unwrap();
        assert_eq!((report.removed_payloads, report.removed_markers), (1, 1));
        assert!(!payload.exists());
        assert_eq!(fs::read(published).unwrap(), b"published data");
    }

    #[test]
    fn ownership_sync_failures_stop_before_any_payload_is_created() {
        use std::cell::RefCell;
        for fail_at in 0..2 {
            let directory = tempfile::tempdir().unwrap();
            let calls = RefCell::new(Vec::new());
            let result = Stage::new_with_sync(
                directory.path(),
                Kind::File,
                |file| {
                    calls.borrow_mut().push("marker");
                    if fail_at == 0 {
                        Err(io::Error::other("injected marker sync failure"))
                    } else {
                        file.sync_all()
                    }
                },
                |_| {
                    calls.borrow_mut().push("parent");
                    Err(io::Error::other("injected parent sync failure"))
                },
            );
            assert!(result.is_err());
            assert_eq!(
                *calls.borrow(),
                if fail_at == 0 {
                    vec!["marker"]
                } else {
                    vec!["marker", "parent"]
                }
            );
            assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
            assert_eq!(
                cleanup_abandoned_staging(directory.path()).unwrap(),
                StagingCleanup::default()
            );
        }
    }

    #[test]
    fn parent_lock_release_is_not_delayed_by_a_duplicate_descriptor() {
        let directory = tempfile::tempdir().unwrap();
        let guard = ParentLock::shared(directory.path()).unwrap();
        let duplicate = guard.0.try_clone().unwrap();
        assert_eq!(
            ParentLock::exclusive(directory.path())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(guard);
        let exclusive = ParentLock::exclusive(directory.path()).unwrap();
        drop(duplicate);
        drop(exclusive);
    }
}
