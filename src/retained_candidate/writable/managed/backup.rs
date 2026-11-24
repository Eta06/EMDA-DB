use super::*;
use crate::SuffixExportFailure;
use crate::retained_candidate::{check_binding, check_file_names, exact_from, open_regular};
use std::io::Read;
use std::os::unix::fs::MetadataExt;

const CANDIDATE: &str = "candidate";
const SUFFIX: &str = "suffix";

impl ManagedRetainedStore {
    /// Publish a backup of the selected generation at a new path outside
    /// this catalog. The source must remain trusted and stable. Recovery
    /// reads only the bounded base and suffix of that generation.
    ///
    /// Keep the returned binding independently of the backup, including on
    /// an uncertain publication. The binding authenticates this snapshot
    /// when compared with the retained value.
    pub fn backup_to(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<SuffixExpected, SuffixExportFailure> {
        self.backup_using(destination.as_ref(), None, sync_existing)
    }

    /// Back up under an inclusive logical read ceiling. Copying the base
    /// and suffix and verifying the staged files each charge their full
    /// encoded lengths. Metadata and framing probes, I/O retries, and writes
    /// have separate costs. Refusal precedes staging and publication.
    pub fn backup_to_with_scan_limit(
        &self,
        destination: impl AsRef<Path>,
        max_scan_bytes: u64,
    ) -> Result<SuffixExpected, SuffixExportFailure> {
        self.backup_using(destination.as_ref(), Some(max_scan_bytes), sync_existing)
    }

    pub(super) fn backup_using(
        &self,
        destination: &Path,
        max_scan_bytes: Option<u64>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<SuffixExpected, SuffixExportFailure> {
        let mut verified = None;
        let result = crate::publication_outcome::track(|tracker| {
            self.verify_selection()?;
            let digest = self.store.hash.clone().finalize().into();
            self.store.verify_boundary(&digest)?;
            let base_path = self.active.slot.path(&self.root).join("base");
            self.store.base.check_path(&base_path)?;
            admit_copy_scans(
                max_scan_bytes,
                self.store.base.metadata().file_bytes,
                self.store.suffix_len as u64,
                "retained backup scan byte limit exceeded",
            )?;
            let parent = crate::backup::parent_directory(destination)?;
            check_separation(&self.root, parent)?;
            let mut stage = crate::staging::Stage::directory(parent)?;
            let candidate = stage.path().join(CANDIDATE);
            let suffix = stage.path().join(SUFFIX);
            fs::DirBuilder::new().mode(0o700).create(&candidate)?;
            let expected = SuffixExpected {
                candidate: self.active.expected.clone(),
                end: self.store.suffix_len as u64,
                digest,
            };
            let limits = self.store.limits;
            let mut base = create_file(&candidate.join("base"))?;
            self.store.base.copy_to(&mut base)?;
            base.sync_all()?;
            drop(base);
            write_new(&candidate.join("records.log"), &expected.candidate.header)?;
            write_new(&candidate.join("witness"), &expected.candidate.witness)?;
            write_new(&candidate.join("store.lock"), &[])?;
            let mut output = create_file(&suffix)?;
            self.store.suffix_file().copy_to(&mut output)?;
            output.sync_all()?;
            drop(output);
            verify_backup(stage.path(), limits, &expected)?;
            sync_existing(&candidate)?;
            self.store.base.check_path(&base_path)?;
            self.store.verify_boundary(&digest)?;
            self.verify_selection()?;
            verified = Some(expected);
            tracker.begin_publication();
            stage.publish(destination)?;
            sync_parent(parent)?;
            stage.publication_synchronized();
            Ok(())
        });
        match result {
            Ok(()) => Ok(verified.expect("backup verified before publication")),
            Err(PublicationFailure::NotPublished(error)) => {
                Err(SuffixExportFailure::NotPublished(error))
            }
            Err(PublicationFailure::OutcomeUnknown(error)) => {
                Err(SuffixExportFailure::OutcomeUnknown {
                    error,
                    expected: Box::new(verified.expect("backup verified before publication")),
                })
            }
        }
    }

    /// Restore an independently bound backup into a new managed catalog.
    /// A new catalog and generation identity are allocated. The backup
    /// stays at its original path. The backup and its paths must remain
    /// trusted and stable during restore.
    pub fn restore_backup(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &SuffixExpected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::restore_backup_using(
            backup.as_ref(),
            destination.as_ref(),
            limits.into(),
            None,
            expected,
        )
    }

    /// Restore a backup into a new catalog under a persistent storage quota.
    pub fn restore_backup_with_storage_limits(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        storage: CatalogStorageLimits,
        expected: &SuffixExpected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::restore_backup_using(
            backup.as_ref(),
            destination.as_ref(),
            InitialLimits::with_storage(limits, storage),
            None,
            expected,
        )
    }

    /// Restore under an inclusive logical read ceiling for copying both
    /// files, replaying the staged suffix, and verifying the staged base.
    /// Metadata and framing probes, retries, and writes have separate costs.
    /// Scan budget refusal precedes staging. The optional catalog quota
    /// applies independently and persists in the new catalog.
    pub fn restore_backup_with_scan_limit(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        max_scan_bytes: u64,
        storage: Option<CatalogStorageLimits>,
        expected: &SuffixExpected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::restore_backup_using(
            backup.as_ref(),
            destination.as_ref(),
            InitialLimits::with_optional_storage(limits, storage),
            Some(max_scan_bytes),
            expected,
        )
    }

    fn restore_backup_using(
        backup: &Path,
        destination: &Path,
        limits: InitialLimits,
        max_scan_bytes: Option<u64>,
        expected: &SuffixExpected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::restore_backup_bound(
            backup,
            destination,
            limits,
            max_scan_bytes,
            expected,
            InitialPublication::new(CATALOG_MAGIC, sync_existing),
        )
        .map(|point| point.generation)
    }

    pub(super) fn restore_backup_bound(
        backup: &Path,
        destination: &Path,
        limits: InitialLimits,
        max_scan_bytes: Option<u64>,
        expected: &SuffixExpected,
        publication: InitialPublication<
            impl FnOnce(&Path) -> io::Result<()>,
            impl FnOnce(AuthorityPoint) -> io::Result<()>,
        >,
    ) -> Result<AuthorityPoint, PublicationFailure> {
        let parent = crate::backup::parent_directory(destination)
            .map_err(PublicationFailure::NotPublished)?;
        check_separation(backup, parent).map_err(PublicationFailure::NotPublished)?;
        admit_copy_scans(
            max_scan_bytes,
            expected.candidate.base_bytes(),
            expected.end,
            "retained restore scan byte limit exceeded",
        )
        .map_err(PublicationFailure::NotPublished)?;
        Self::create_bound(
            &backup.join(CANDIDATE),
            destination,
            limits,
            &expected.candidate,
            Some((&backup.join(SUFFIX), expected)),
            publication,
        )
    }
}

fn admit_copy_scans(
    limit: Option<u64>,
    base: u64,
    suffix: u64,
    message: &'static str,
) -> io::Result<()> {
    if limit.is_some_and(|limit| {
        base.checked_add(suffix)
            .and_then(|bytes| bytes.checked_mul(2))
            .is_none_or(|scans| scans > limit)
    }) {
        return Err(invalid_input(message));
    }
    Ok(())
}

pub(super) fn verify_backup(
    root: &Path,
    limits: RetainedStoreLimits,
    expected: &SuffixExpected,
) -> io::Result<()> {
    let candidate = root.join(CANDIDATE);
    check_files(&candidate, &expected.candidate)?;
    let base = open_regular(&candidate.join("base"))?;
    let suffix = open_regular(&root.join(SUFFIX))?;
    verify_backup_contents(&base, &suffix, limits, expected)
}

fn verify_backup_contents(
    base: &fs::File,
    suffix: &fs::File,
    limits: RetainedStoreLimits,
    expected: &SuffixExpected,
) -> io::Result<()> {
    if expected.end > limits.suffix.bytes || suffix.metadata()?.len() != expected.end {
        return Err(record::invalid_data("backup suffix mismatch"));
    }
    let base = crate::retained_base_v2::validate_buffered(base, limits.base)?;
    check_binding(&base, &expected.candidate)?;
    let mut hash = Sha256::new();
    if io::copy(&mut suffix.take(expected.end.saturating_add(1)), &mut hash)? != expected.end
        || <[u8; 32]>::from(hash.finalize()) != expected.digest
    {
        return Err(record::invalid_data("backup suffix mismatch"));
    }
    Ok(())
}

pub(super) struct SyncedBackup {
    paths: [PathBuf; 5],
    files: [fs::File; 5],
    lengths: [u64; 5],
}

impl SyncedBackup {
    pub(super) fn check_paths(&self) -> io::Result<()> {
        for ((path, file), &length) in self.paths.iter().zip(&self.files).zip(&self.lengths) {
            let visible = path.symlink_metadata()?;
            let pinned = file.metadata()?;
            if !visible.is_file()
                || (visible.dev(), visible.ino()) != (pinned.dev(), pinned.ino())
                || visible.len() != length
            {
                return Err(record::invalid_data("backup file path or length changed"));
            }
        }
        Ok(())
    }
}

pub(super) fn sync_verified_backup(
    root: &Path,
    limits: RetainedStoreLimits,
    expected: &SuffixExpected,
    sync_directory: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<SyncedBackup> {
    let candidate = root.join(CANDIDATE);
    check_file_names(&candidate)?;
    let paths = [
        candidate.join("base"),
        candidate.join("records.log"),
        candidate.join("witness"),
        candidate.join("store.lock"),
        root.join(SUFFIX),
    ];
    let [base, header, witness, lock, suffix] = &paths;
    let files = [
        open_regular(base)?,
        open_regular(header)?,
        open_regular(witness)?,
        open_regular(lock)?,
        open_regular(suffix)?,
    ];
    let lengths = [
        u64::from_le_bytes(expected.candidate.header[48..56].try_into().unwrap()),
        expected.candidate.header.len() as u64,
        expected.candidate.witness.len() as u64,
        0,
        expected.end,
    ];
    let backup = SyncedBackup {
        paths,
        files,
        lengths,
    };
    backup.check_paths()?;
    for file in &backup.files {
        file.sync_all()?;
    }
    sync_directory(&candidate)?;
    let [base, header, witness, lock, suffix] = &backup.files;
    exact_from(lock, 0)?;
    if exact_from(header, expected.candidate.header.len())? != expected.candidate.header
        || exact_from(witness, expected.candidate.witness.len())? != expected.candidate.witness
    {
        return Err(record::invalid_data("backup candidate binding mismatch"));
    }
    verify_backup_contents(base, suffix, limits, expected)?;
    Ok(backup)
}
