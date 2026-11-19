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
            limits,
            || Ok(()),
            sync_existing,
        )
    }

    fn cutover_using(
        source: Store,
        root: &Path,
        export: RetainedExportLimits,
        limits: RetainedStoreLimits,
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
            let directory = random_id()?;
            let nested = catalog_path(&root, directory);
            let committed = ManagedRetainedStore::publish_initial(
                &nested,
                &root,
                limits,
                CUTOVER_MAGIC,
                sync_existing,
                |stage, path| {
                    let expected = export_into_generation(&source, stage, path, export, limits)?;
                    for (original, archive) in [
                        ("records.log", ARCHIVE_LOG),
                        ("commit.state", ARCHIVE_WITNESS),
                    ] {
                        let original = root.join(original);
                        open_regular(&original)?;
                        fs::hard_link(original, stage.path().join(archive))?;
                    }
                    Ok((expected, None))
                },
            )
            .map_err(unpack_publication)?;
            let initial = AuthorityRecord {
                revision: 0,
                committed,
                pending: None,
            };
            let marker = Marker { directory, initial };
            let (mut stage, mut file) = crate::staging::Stage::file(&root)?;
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
        let fence = Fence::load(root.as_ref())?;
        let initial = fence.initial();
        let _store = ManagedRetainedStore::open_role(
            &fence.catalog_path(),
            limits,
            CUTOVER_MAGIC,
            |root, catalog| select_authorized(root, catalog, initial.committed),
            |observed| initial.check(observed),
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
        self.retire_archive_using(archive, backup.as_ref(), expected, |path| {
            fs::remove_file(path)
        })
    }

    fn retire_archive_using(
        &mut self,
        archive: &CutoverArchiveBinding,
        backup: &Path,
        expected: &SuffixExpected,
        mut remove: impl FnMut(&Path) -> io::Result<()>,
    ) -> Result<bool, PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
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
            if !check_archive_file(
                &fence.root.join("commit.state"),
                witness_bytes,
                &archive.witness_digest,
            )? {
                return Err(record::invalid_data("cutover source witness is missing"));
            }
            let log = root.join(ARCHIVE_LOG);
            let witness = root.join(ARCHIVE_WITNESS);
            let have_log = check_archive_file(&log, archive.log_bytes, &archive.log_digest)?;
            let have_witness =
                check_archive_file(&witness, witness_bytes, &archive.witness_digest)?;
            self.inner.store.verify_complete()?;
            super::backup::verify_backup(backup, self.inner.store.limits, expected)?;
            super::backup::sync_backup(backup)?;
            self.verify_current()?;
            tracker.begin_publication();
            if have_log {
                remove(&log)?;
            }
            if have_witness {
                remove(&witness)?;
            }
            sync_existing(&root)?;
            Ok(have_log || have_witness)
        })
    }
}

fn check_archive_file(path: &Path, bytes: u64, digest: &[u8; 32]) -> io::Result<bool> {
    let file = match open_regular(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let mut hash = Sha256::new();
    if file.metadata()?.len() != bytes
        || io::copy(&mut file.take(bytes.saturating_add(1)), &mut hash)? != bytes
        || <[u8; 32]>::from(hash.finalize()) != *digest
    {
        return Err(record::invalid_data("cutover archive fingerprint mismatch"));
    }
    Ok(true)
}
