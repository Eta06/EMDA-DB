use super::*;
use std::fmt::Write as _;
use std::io::Read;

mod cleanup;

#[cfg(test)]
mod tests;

const MARKER_MAGIC: &[u8; 8] = b"EMDALOG4";
const RECORD_END: usize = 24 + file::LENGTH;
const MARKER_LEN: usize = RECORD_END + 32;
const ARCHIVE_LOG: &str = "legacy.records";
const ARCHIVE_WITNESS: &str = "legacy.witness";

/// Fingerprints of the ordinary log and witness file captured under the
/// source lock before cutover, or obtained from an independently trusted
/// copy. The witness digest covers the file, including the V3 identity
/// anchor. The archive filenames alone are insufficient to identify it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CutoverArchiveBinding {
    pub log_bytes: u64,
    pub log_digest: [u8; 32],
    pub witness_digest: [u8; 32],
}

/// The operation that failed during a cutover archive retirement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CutoverArchiveRetirementStep {
    /// Archive, authority or backup checks failed before any unlink attempt.
    Verify,
    /// The archived log unlink did not return success.
    UnlinkLog,
    /// The archived witness unlink did not return success.
    UnlinkWitness,
    /// The visible archive state could not be synchronized.
    SyncCatalog,
}

/// A failed archive retirement and the unlink calls it observed succeeding.
///
/// A false flag also covers an already absent file or an unconfirmed unlink.
/// A true flag records a successful syscall, not persistence across a crash.
/// After a failed unlink or catalog sync, inspect the trusted catalog and
/// retry with the original archive binding and an exact current backup.
#[derive(Debug)]
#[non_exhaustive]
pub struct CutoverArchiveRetirementFailure {
    /// The failed operation in this call.
    pub step: CutoverArchiveRetirementStep,
    /// The archived log unlink returned success in this call.
    pub log_unlinked: bool,
    /// The archived witness unlink returned success in this call.
    pub witness_unlinked: bool,
    /// The underlying verification, unlink or synchronization error.
    pub error: io::Error,
}

impl std::fmt::Display for CutoverArchiveRetirementFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let operation = match self.step {
            CutoverArchiveRetirementStep::Verify => "archive retirement verification",
            CutoverArchiveRetirementStep::UnlinkLog => "archived log unlink",
            CutoverArchiveRetirementStep::UnlinkWitness => "archived witness unlink",
            CutoverArchiveRetirementStep::SyncCatalog => "archive catalog sync",
        };
        write!(formatter, "{operation} failed: {}", self.error)
    }
}

impl std::error::Error for CutoverArchiveRetirementFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

pub(super) struct Fence {
    root: PathBuf,
    lock: StoreLock,
    marker: Marker,
}

impl Fence {
    pub(super) fn load(root: &Path) -> io::Result<Self> {
        let lock = lock_directory(root)?;
        let root = root.canonicalize()?;
        lock.verify_identity(&root)?;
        let marker = Marker::load(&root)?;
        sync_existing(&root)?;
        Ok(Self { root, lock, marker })
    }

    pub(super) fn verify(&self) -> io::Result<()> {
        self.lock.verify_identity(&self.root)?;
        if Marker::load(&self.root)? != self.marker {
            return Err(record::invalid_data("retained cutover fence changed"));
        }
        Ok(())
    }

    pub(super) fn catalog_path(&self) -> PathBuf {
        self.marker.catalog_path(&self.root)
    }

    pub(super) fn initial(&self) -> AuthorityRecord {
        self.marker.initial
    }

    pub(super) fn check_backup_parent(&self, path: &Path) -> io::Result<()> {
        check_separation(&self.root, crate::backup::parent_directory(path)?)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Marker {
    directory: [u8; 16],
    initial: AuthorityRecord,
}

impl Marker {
    fn encode(self) -> [u8; MARKER_LEN] {
        let mut marker = [0; MARKER_LEN];
        marker[..8].copy_from_slice(MARKER_MAGIC);
        marker[8..24].copy_from_slice(&self.directory);
        marker[24..RECORD_END].copy_from_slice(&file::encode(Some(self.initial)));
        let digest = Sha256::digest(&marker[..RECORD_END]);
        marker[RECORD_END..].copy_from_slice(&digest);
        marker
    }

    fn load(root: &Path) -> io::Result<Self> {
        let bytes = exact(&root.join("records.log"), MARKER_LEN)?;
        let invalid = || record::invalid_data("invalid retained cutover marker");
        let initial = file::decode(&bytes[24..RECORD_END])?.ok_or_else(invalid)?;
        let marker = Self {
            directory: bytes[8..24].try_into().unwrap(),
            initial,
        };
        if marker.directory == [0; 16]
            || initial.revision != 0
            || initial.pending.is_some()
            || bytes != marker.encode()
        {
            return Err(invalid());
        }
        Ok(marker)
    }

    fn catalog_path(&self, root: &Path) -> PathBuf {
        catalog_path(root, self.directory)
    }
}

fn catalog_path(root: &Path, directory: [u8; 16]) -> PathBuf {
    let mut name = String::from("retained-cutover-");
    for byte in directory {
        write!(name, "{byte:02x}").unwrap();
    }
    root.join(name)
}

impl<A: RetainedAuthority> AuthorizedRetainedStore<A> {
    /// Convert an exclusively owned witnessed store at its original path.
    /// The old log and witness remain preserved under non-store filenames.
    /// After success, register the returned initial record in an absent
    /// independent authority. Open the original path through this type.
    ///
    /// Keep the directory trusted and stable. Older backups and independent
    /// copies require operational fencing before migrating a shared workload.
    /// If publication is uncertain, inspect the original path with
    /// `recover_cutover` before registering authority or attempting a retry.
    pub fn cutover_from_store(
        source: Store,
        root: impl AsRef<Path>,
        export: RetainedExportLimits,
        limits: RetainedStoreLimits,
    ) -> Result<AuthorityRecord, PublicationFailure> {
        Self::cutover_using(
            source,
            root.as_ref(),
            export,
            limits.into(),
            || Ok(()),
            sync_existing,
        )
    }

    /// Convert at the original path under a persistent logical storage quota.
    /// Admission includes the legacy files, archive links, temporary staging
    /// and fence. Place the independent authority outside the original root.
    pub fn cutover_from_store_with_storage_limits(
        source: Store,
        root: impl AsRef<Path>,
        export: RetainedExportLimits,
        limits: RetainedStoreLimits,
        storage: CatalogStorageLimits,
    ) -> Result<AuthorityRecord, PublicationFailure> {
        Self::cutover_using(
            source,
            root.as_ref(),
            export,
            InitialLimits::with_storage(limits, storage),
            || Ok(()),
            sync_existing,
        )
    }

    fn cutover_using(
        source: Store,
        root: &Path,
        export: RetainedExportLimits,
        mut limits: InitialLimits,
        before_replace: impl FnOnce() -> io::Result<()>,
        sync_root: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<AuthorityRecord, PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            let root = root.canonicalize()?;
            let lock = source.share_store_lock();
            lock.verify_identity(&root)?;
            match fs::symlink_metadata(root.join(CATALOG_FILE)) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
                Ok(_) => return Err(invalid_input("source already has catalog metadata")),
            }
            use crate::staging::Stage;
            let fence_stage_bytes = Stage::FILE_MARKER_BYTES + MARKER_LEN as u64;
            if let Some(storage) = &mut limits.storage {
                let quota = CatalogStorage {
                    root: root.clone(),
                    limits: storage.policy,
                };
                // The nested directory-stage marker is gone before fencing.
                // The published directory remains beside the fence stage's
                // file and marker, for three entries outside the catalog.
                storage.available =
                    quota.remaining(fence_stage_bytes.max(Stage::DIRECTORY_MARKER_BYTES), 3)?;
                storage.archives = CatalogStorageLimits {
                    bytes: open_regular(&root.join("records.log"))?
                        .metadata()?
                        .len()
                        .checked_add(open_regular(&root.join("commit.state"))?.metadata()?.len())
                        .ok_or_else(storage::byte_limit)?,
                    entries: 2,
                };
            }
            let directory = random_id()?;
            let nested = catalog_path(&root, directory);
            let committed = ManagedRetainedStore::publish_initial(
                &nested,
                &root,
                limits,
                InitialPublication::new(CUTOVER_MAGIC, sync_existing),
                |stage, path, max_base_bytes| {
                    let expected = export_into_generation(
                        &source,
                        stage,
                        path,
                        export,
                        limits.generation,
                        max_base_bytes,
                    )?;
                    for (original, archive) in [
                        ("records.log", ARCHIVE_LOG),
                        ("commit.state", ARCHIVE_WITNESS),
                    ] {
                        let original = root.join(original);
                        open_regular(&original)?;
                        fs::hard_link(original, stage.path().join(archive))?;
                    }
                    Ok(InitialGeneration {
                        expected,
                        suffix: None,
                        base: None,
                    })
                },
            )
            .map_err(unpack_publication)?;
            let initial = AuthorityRecord {
                revision: 0,
                committed,
                pending: None,
            };
            let marker = Marker { directory, initial };
            if let Some(storage) = limits.storage {
                CatalogStorage {
                    root: root.clone(),
                    limits: storage.policy,
                }
                .room(fence_stage_bytes, 2)?;
            }
            let (mut stage, mut file) = Stage::file(&root)?;
            file.write_all(&marker.encode())?;
            file.sync_all()?;
            drop(file);
            lock.verify_identity(&root)?;
            before_replace()?;
            tracker.begin_publication();
            stage.replace_file(&root.join("records.log"))?;
            sync_root(&root)?;
            stage.publication_synchronized();
            Ok(initial)
        })
    }

    /// Recover the verified initial authority record after an uncertain
    /// cutover. This requires the published fence and an intact initial
    /// generation. A legacy store that still opens can be prepared again.
    pub fn recover_cutover(
        root: impl AsRef<Path>,
        limits: RetainedStoreLimits,
    ) -> io::Result<AuthorityRecord> {
        Self::recover_cutover_using(root.as_ref(), limits, None)
    }

    /// Recover an uncertain cutover under the combined logical scan ceiling of
    /// [`Self::open_with_recovery_scan_limit`]. The initial generation remains
    /// bound to the exact authority point recorded in the published fence.
    pub fn recover_cutover_with_recovery_scan_limit(
        root: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        max_scan_bytes: u64,
    ) -> io::Result<AuthorityRecord> {
        Self::recover_cutover_using(
            root.as_ref(),
            limits,
            Some(RecoveryScanLimit::BaseAndSuffix(max_scan_bytes)),
        )
    }

    fn recover_cutover_using(
        root: &Path,
        limits: RetainedStoreLimits,
        scan_limit: Option<RecoveryScanLimit>,
    ) -> io::Result<AuthorityRecord> {
        let fence = Fence::load(root)?;
        let initial = fence.initial();
        let _store = ManagedRetainedStore::open_role(
            &fence.catalog_path(),
            limits,
            CUTOVER_MAGIC,
            (None, scan_limit),
            |root, catalog| select_authorized(root, catalog, initial.committed),
            Some(initial),
            None,
        )?;
        fence.verify()?;
        Ok(initial)
    }

    /// Retire the legacy archive of an original-path cutover. The caller
    /// supplies independently retained fingerprints of the archived bytes
    /// and a bound backup of the current retained generation. Keep the
    /// backup and its directories trusted and stable through retirement.
    ///
    /// Success reports whether any archive file was removed. On
    /// `OutcomeUnknown`, either file may already be absent. Retry with the
    /// original archive binding and a backup of the then-current generation.
    /// The cutover fence and the original root's witness are preserved.
    pub fn retire_cutover_archive(
        &mut self,
        archive: &CutoverArchiveBinding,
        backup: impl AsRef<Path>,
        expected: &SuffixExpected,
    ) -> Result<bool, PublicationFailure> {
        self.retire_cutover_archive_detailed(archive, backup, expected)
            .map_err(|failure| match failure.step {
                CutoverArchiveRetirementStep::Verify => {
                    PublicationFailure::NotPublished(failure.error)
                }
                _ => PublicationFailure::OutcomeUnknown(failure.error),
            })
    }

    /// Retire with the same checks as [`Self::retire_cutover_archive`]
    /// and report the failed step and observed unlinks separately.
    /// Synchronization is required even when both names are already absent,
    /// since a previous call could have failed after removing them.
    pub fn retire_cutover_archive_detailed(
        &mut self,
        archive: &CutoverArchiveBinding,
        backup: impl AsRef<Path>,
        expected: &SuffixExpected,
    ) -> Result<bool, CutoverArchiveRetirementFailure> {
        self.retire_archive_using(
            archive,
            (backup.as_ref(), expected),
            None,
            sync_existing,
            |path| fs::remove_file(path),
            sync_existing,
        )
    }

    /// Retire under an inclusive logical read ceiling. Charge the original
    /// root witness, each present archive file, and both the active and backup
    /// base and suffix. Admission precedes payload verification and unlinks.
    /// Metadata and EOF probes, I/O retries and synchronization are separate.
    pub fn retire_cutover_archive_with_scan_limit(
        &mut self,
        archive: &CutoverArchiveBinding,
        backup: impl AsRef<Path>,
        expected: &SuffixExpected,
        max_scan_bytes: u64,
    ) -> Result<bool, CutoverArchiveRetirementFailure> {
        self.retire_archive_using(
            archive,
            (backup.as_ref(), expected),
            Some(max_scan_bytes),
            sync_existing,
            |path| fs::remove_file(path),
            sync_existing,
        )
    }

    fn retire_archive_using(
        &mut self,
        archive: &CutoverArchiveBinding,
        (backup, expected): (&Path, &SuffixExpected),
        max_scan_bytes: Option<u64>,
        sync_backup_directory: impl FnOnce(&Path) -> io::Result<()>,
        mut remove: impl FnMut(&Path) -> io::Result<()>,
        sync_catalog: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<bool, CutoverArchiveRetirementFailure> {
        use CutoverArchiveRetirementStep::{SyncCatalog, UnlinkLog, UnlinkWitness, Verify};
        let mut step = Verify;
        let mut log_unlinked = false;
        let mut witness_unlinked = false;
        let result = (|| {
            self.verify_current()?;
            let fence = self
                .fence
                .as_ref()
                .ok_or_else(|| invalid_input("missing original-path cutover"))?;
            let root = fence.catalog_path();
            if root != self.inner.root
                || self.current.pending.is_some()
                || self.current.committed
                    != AuthorityPoint::from_store(self.inner.active.catalog, &self.inner.store)
                || expected.candidate != self.inner.active.expected
                || expected.end != self.current.committed.suffix_end
                || expected.digest != self.current.committed.suffix_digest
            {
                return Err(record::invalid_data(
                    "backup does not match the selected generation",
                ));
            }
            if !fs::symlink_metadata(backup)?.file_type().is_dir() {
                return Err(record::invalid_data("invalid retained backup directory"));
            }
            check_separation(&fence.root, backup)?;
            let witness_bytes = crate::witness::WITNESS_SIZE as u64;
            let original = open_archive_file(&fence.root.join("commit.state"), witness_bytes)?
                .ok_or_else(|| record::invalid_data("cutover source witness is missing"))?;
            let log = root.join(ARCHIVE_LOG);
            let witness = root.join(ARCHIVE_WITNESS);
            let log_file = open_archive_file(&log, archive.log_bytes)?;
            let witness_file = open_archive_file(&witness, witness_bytes)?;
            let have_log = log_file.is_some();
            let have_witness = witness_file.is_some();
            if let Some(limit) = max_scan_bytes {
                let base = expected.candidate.base_bytes();
                let suffix = expected.end;
                let scans = [
                    witness_bytes,
                    if have_log { archive.log_bytes } else { 0 },
                    if have_witness { witness_bytes } else { 0 },
                    base,
                    suffix,
                    base,
                    suffix,
                ];
                scans
                    .into_iter()
                    .try_fold(limit, u64::checked_sub)
                    .ok_or_else(|| {
                        invalid_input("retained archive retirement scan byte limit exceeded")
                    })?;
            }
            verify_archive_file(original, witness_bytes, &archive.witness_digest)?;
            if let Some(file) = log_file {
                verify_archive_file(file, archive.log_bytes, &archive.log_digest)?;
            }
            if let Some(file) = witness_file {
                verify_archive_file(file, witness_bytes, &archive.witness_digest)?;
            }
            self.inner
                .store
                .verify_complete(&self.inner.active.slot.path(&root))?;
            let backup = super::backup::sync_verified_backup(
                backup,
                self.inner.store.limits,
                expected,
                sync_backup_directory,
            )?;
            self.verify_current()?;
            backup.check_paths()?;
            if have_log {
                step = UnlinkLog;
                remove(&log)?;
                log_unlinked = true;
            }
            if have_witness {
                step = UnlinkWitness;
                remove(&witness)?;
                witness_unlinked = true;
            }
            step = SyncCatalog;
            sync_catalog(&root)?;
            Ok(log_unlinked || witness_unlinked)
        })();
        result.map_err(|error| CutoverArchiveRetirementFailure {
            step,
            log_unlinked,
            witness_unlinked,
            error,
        })
    }
}

fn check_archive_file(path: &Path, bytes: u64, digest: &[u8; 32]) -> io::Result<bool> {
    let Some(file) = open_archive_file(path, bytes)? else {
        return Ok(false);
    };
    verify_archive_file(file, bytes, digest)?;
    Ok(true)
}

fn open_archive_file(path: &Path, bytes: u64) -> io::Result<Option<fs::File>> {
    let file = match open_regular(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() != bytes {
        return Err(record::invalid_data("cutover archive fingerprint mismatch"));
    }
    Ok(Some(file))
}

fn verify_archive_file(file: fs::File, bytes: u64, digest: &[u8; 32]) -> io::Result<()> {
    let mut hash = Sha256::new();
    if io::copy(&mut file.take(bytes.saturating_add(1)), &mut hash)? != bytes
        || <[u8; 32]>::from(hash.finalize()) != *digest
    {
        return Err(record::invalid_data("cutover archive fingerprint mismatch"));
    }
    Ok(())
}
