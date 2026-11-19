use super::*;
use std::fmt::Write as _;

#[cfg(test)]
mod tests;

const MARKER_MAGIC: &[u8; 8] = b"EMDALOG4";
const RECORD_END: usize = 24 + file::LENGTH;
const MARKER_LEN: usize = RECORD_END + 32;

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
                        ("records.log", "legacy.records"),
                        ("commit.state", "legacy.witness"),
                    ] {
                        let original = root.join(original);
                        open_regular(&original)?;
                        fs::hard_link(original, stage.path().join(archive))?;
                    }
                    Ok((expected, Vec::new()))
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
}
