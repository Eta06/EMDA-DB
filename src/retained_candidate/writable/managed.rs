use super::{
    Expected, Header, RecoveryLimits, RetainedStore, RetainedStoreLimits, StoreLock, copy_base,
    create_file, decode_marker, encode_batch, invalid_input, open_regular, open_regular_with,
    random_id, read_marker, suffix_end,
};
use crate::directory::sync_existing;
use crate::record::{self, Operation};
use crate::retained_candidate::{
    SuffixCapacity, SuffixExpected, check_files, check_suffix_binding, exact, read_suffix,
};
use crate::{
    BatchOperation, Entry, MutationFailure, PublicationFailure, RetainedExportLimits, Store,
};
use rustix::fs::OFlags;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Write},
    num::NonZeroU64,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
};

const CATALOG_FILE: &str = "catalog.id";
const CATALOG_MAGIC: &[u8; 8] = b"EMDACT01";
const ACTIVE_FILE: &str = "active.retained";
const ACTIVE_MAGIC: &[u8; 8] = b"EMDAAC01";

#[cfg(test)]
mod tests;

/// A retained store with one durable active generation. One exclusive
/// catalog lock spans the lifetime of the handle and every cutover. Managed
/// generation directories can only be opened through their catalog.
///
/// The catalog is trusted local metadata, as is the generation witness.
/// Cooperating callers must keep the directories stable. This mechanism
/// detects damage and mismatches, with coordinated disk rollback outside
/// its guarantees.
pub struct ManagedRetainedStore {
    store: RetainedStore,
    root: PathBuf,
    active: Active,
    uncertain: bool,
    auto_reclaim: Option<NonZeroU64>,
    cleanup_pending: bool,
    lock: StoreLock,
}

impl ManagedRetainedStore {
    /// Publish a catalog with the verified candidate as its initial state.
    /// The destination must be absent and outside the candidate directory.
    pub fn create(
        candidate: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &Expected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::create_using(
            candidate.as_ref(),
            destination.as_ref(),
            limits,
            expected,
            || Ok(Vec::new()),
        )
    }

    /// Publish a catalog with the independently bound detached suffix
    /// imported into its first generation.
    pub fn create_from_suffix(
        candidate: impl AsRef<Path>,
        suffix: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &SuffixExpected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::create_using(
            candidate.as_ref(),
            destination.as_ref(),
            limits,
            &expected.candidate,
            || {
                let bytes = read_suffix(suffix.as_ref(), limits.suffix, expected.end)?;
                check_suffix_binding(&bytes, limits.suffix, expected)?;
                Ok(bytes)
            },
        )
    }

    fn create_using(
        candidate: &Path,
        destination: &Path,
        limits: RetainedStoreLimits,
        expected: &Expected,
        suffix: impl FnOnce() -> io::Result<Vec<u8>>,
    ) -> Result<[u8; 16], PublicationFailure> {
        let parent = crate::backup::parent_directory(destination)
            .map_err(PublicationFailure::NotPublished)?;
        check_separation(candidate, parent).map_err(PublicationFailure::NotPublished)?;
        check_files(candidate, expected).map_err(PublicationFailure::NotPublished)?;
        Self::publish_initial(destination, parent, limits, sync_existing, |_, path| {
            copy_base(candidate, path, limits, expected)?;
            Ok((expected.clone(), suffix()?))
        })
    }

    /// Copy an exclusively owned witnessed store into a new managed catalog.
    /// The source lock stays held through publication. The source path must
    /// name that store and the destination must be absent and outside it.
    /// Source and destination paths must remain trusted and stable.
    ///
    /// Replay and output must fit the supplied export and generation limits.
    /// This publishes an independent writable copy. The source files stay
    /// available at their original path. After an uncertain publication,
    /// inspect the destination with `ManagedRetainedStore::open`.
    pub fn copy_from_store(
        source: Store,
        source_path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        export: RetainedExportLimits,
        limits: RetainedStoreLimits,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::copy_from_store_using(
            source,
            source_path.as_ref(),
            destination.as_ref(),
            export,
            limits,
            sync_existing,
        )
    }

    fn copy_from_store_using(
        source: Store,
        source_path: &Path,
        destination: &Path,
        mut export: RetainedExportLimits,
        limits: RetainedStoreLimits,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 16], PublicationFailure> {
        let parent = crate::backup::parent_directory(destination)
            .map_err(PublicationFailure::NotPublished)?;
        check_separation(source_path, parent).map_err(PublicationFailure::NotPublished)?;
        let source_lock = source.share_store_lock();
        source_lock
            .verify_identity(source_path)
            .map_err(PublicationFailure::NotPublished)?;
        export.max_output_bytes = export
            .max_output_bytes
            .min(limits.base.file_bytes)
            .min(limits.base.work_bytes.saturating_sub(1));
        Self::publish_initial(destination, parent, limits, sync_parent, |stage, path| {
            let name = std::ffi::OsStr::new("base");
            let base = source
                .export_retained_base_in_stage(stage, name, random_id()?, export)
                .map_err(|failure| match failure {
                    PublicationFailure::NotPublished(error)
                    | PublicationFailure::OutcomeUnknown(error) => error,
                })?;
            let expected = crate::retained_candidate::binding(&base, random_id()?)?;
            fs::rename(stage.child_path(name)?, path.join(name))?;
            Ok((expected, Vec::new()))
        })
    }

    fn publish_initial(
        destination: &Path,
        parent: &Path,
        limits: RetainedStoreLimits,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        prepare: impl FnOnce(&crate::staging::Stage, &Path) -> io::Result<(Expected, Vec<u8>)>,
    ) -> Result<[u8; 16], PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            let mut stage = crate::staging::Stage::directory(parent)?;
            let root = stage.path();
            create_file(&root.join("store.lock"))?.sync_all()?;
            let catalog = random_id()?;
            write_new(&root.join(CATALOG_FILE), &catalog_bytes(catalog))?;
            let slot = Slot::A;
            let path = slot.path(root);
            fs::DirBuilder::new().mode(0o700).create(&path)?;
            let (expected, suffix) = prepare(&stage, &path)?;
            let store = RetainedStore::initialize(&path, limits, &expected, suffix, Some(catalog))?;
            let generation = store.generation_id();
            drop(store);
            let active = Active {
                catalog,
                slot,
                generation,
                expected,
            };
            write_new(&root.join(ACTIVE_FILE), &active.encode())?;
            sync_existing(root)?;
            tracker.begin_publication();
            stage.publish(destination)?;
            sync_parent(parent)?;
            stage.publication_synchronized();
            Ok(generation)
        })
    }

    /// Recover the exact selected generation and synchronize its namespace
    /// before admitting writes. Invalid or missing selections fail closed.
    /// The selected generation's persisted limits must fit within the
    /// recovery ceiling. Writes use the persisted limits.
    pub fn open(root: impl AsRef<Path>, ceiling: RetainedStoreLimits) -> io::Result<Self> {
        let root = root.as_ref();
        let lock = lock_directory(root)?;
        let root = root.canonicalize()?;
        lock.verify_identity(&root)?;
        let catalog = read_catalog(&root)?;
        let active = Active::load(&root, catalog)?;
        let path = active.slot.path(&root);
        let store = RetainedStore::open_role(
            &path,
            RecoveryLimits::Ceiling(ceiling),
            &active.expected,
            Some(catalog),
            Some(active.generation),
            |directory| {
                sync_existing(directory)?;
                lock.verify_identity(&root)?;
                verify_active(&root, &active)
            },
        )?;
        Ok(Self {
            store,
            root,
            active,
            uncertain: false,
            auto_reclaim: None,
            cleanup_pending: false,
            lock,
        })
    }

    /// Recover a managed catalog and retire any verified inactive slot before
    /// returning. This handle compacts when a write would cross the suffix
    /// byte threshold or exhaust suffix admission. The write must fit an
    /// empty suffix. Maintenance is synchronous and can reject a write before
    /// its append. A selection-uncertain handle requires reopen.
    ///
    /// The policy is per open. The catalog root is reserved for engine files.
    /// Open and subsequent automatic maintenance clean abandoned private
    /// stages inside that root.
    pub fn open_with_auto_reclaim(
        root: impl AsRef<Path>,
        ceiling: RetainedStoreLimits,
        suffix_threshold: NonZeroU64,
    ) -> io::Result<Self> {
        let mut store = Self::open(root, ceiling)?;
        store.finish_maintenance()?;
        store.auto_reclaim = Some(suffix_threshold);
        Ok(store)
    }

    pub fn generation_id(&self) -> io::Result<[u8; 16]> {
        self.ensure_selected()?;
        Ok(self.active.generation)
    }

    /// Return the selected generation's persisted limits.
    pub fn limits(&self) -> io::Result<RetainedStoreLimits> {
        self.ensure_selected()?;
        Ok(self.store.limits)
    }

    pub fn get(&self, key: &[u8], max_value_bytes: usize) -> io::Result<Option<Vec<u8>>> {
        self.ensure_selected()?;
        self.store.get(key, max_value_bytes)
    }

    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        self.ensure_selected()?;
        self.store.scan(after, before, limit)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MutationFailure> {
        self.write(|| record::encode(Operation::Put, key, value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), MutationFailure> {
        self.write(|| record::encode(Operation::Delete, key, &[]))
    }

    pub fn write_batch(
        &mut self,
        operations: &[BatchOperation<'_>],
    ) -> Result<(), MutationFailure> {
        self.write(|| encode_batch(operations))
    }

    fn write(
        &mut self,
        encode: impl FnOnce() -> io::Result<Vec<u8>>,
    ) -> Result<(), MutationFailure> {
        self.prepare_write()?;
        let bytes = encode().map_err(MutationFailure::Rejected)?;
        let Some(threshold) = self.auto_reclaim else {
            return self.store.write_encoded(&bytes, |_| Ok(()));
        };
        if self.cleanup_pending {
            self.finish_maintenance()
                .map_err(MutationFailure::Rejected)?;
        }
        let suffix_len = self.store.suffix.len() as u64;
        if suffix_len == 0 {
            return self.store.write_encoded(&bytes, |_| Ok(()));
        }
        if suffix_len.saturating_add(bytes.len() as u64) <= threshold.get() {
            match self.store.write_encoded(&bytes, |_| Ok(())) {
                Err(MutationFailure::Rejected(ref error))
                    if error
                        .get_ref()
                        .is_some_and(|cause| cause.is::<SuffixCapacity>()) => {}
                result => return result,
            }
        }
        self.compact_for_write(&bytes)?;
        self.prepare_write()?;
        self.store.write_encoded(&bytes, |_| Ok(()))
    }

    fn compact_for_write(&mut self, bytes: &[u8]) -> Result<(), MutationFailure> {
        let limits = self.store.limits;
        suffix_end(0, bytes.len(), limits).map_err(MutationFailure::Rejected)?;
        super::Overlay::default()
            .prepare_append(bytes, 0, limits.suffix, limits.overlay)
            .map_err(MutationFailure::Rejected)?;
        self.finish_maintenance()
            .map_err(MutationFailure::Rejected)?;
        self.compact().map_err(|error| {
            let kind = match &error {
                PublicationFailure::NotPublished(cause)
                | PublicationFailure::OutcomeUnknown(cause) => cause.kind(),
            };
            MutationFailure::Rejected(io::Error::new(kind, error))
        })?;
        self.cleanup_pending = true;
        self.finish_maintenance().map_err(MutationFailure::Rejected)
    }

    fn finish_maintenance(&mut self) -> io::Result<()> {
        self.cleanup_pending = true;
        self.verify_selection()?;
        crate::cleanup_abandoned_staging(&self.root)?;
        self.reclaim_inactive()?;
        self.cleanup_pending = false;
        Ok(())
    }

    /// Compact the complete live state to a verified base and empty suffix in
    /// the inactive slot, then select it durably. An occupied inactive slot
    /// requires explicit reclamation first. Errors before selection can leave
    /// an inactive candidate. Selection uncertainty requires reopening.
    pub fn compact(&mut self) -> Result<[u8; 16], PublicationFailure> {
        self.compact_with_limits(self.store.limits)
    }

    /// Compact into a successor with new persisted limits. The existing
    /// limits still bound reading and verifying the active generation.
    /// If selection is uncertain, reopen using a recovery ceiling that
    /// covers both the prior and requested limits.
    pub fn compact_with_limits(
        &mut self,
        limits: RetainedStoreLimits,
    ) -> Result<[u8; 16], PublicationFailure> {
        self.compact_using_limits(limits, || Ok(()), sync_existing)
    }

    #[cfg(test)]
    fn compact_using(
        &mut self,
        before_select: impl FnOnce() -> io::Result<()>,
        sync_selection: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 16], PublicationFailure> {
        self.compact_using_limits(self.store.limits, before_select, sync_selection)
    }

    fn compact_using_limits(
        &mut self,
        limits: RetainedStoreLimits,
        before_select: impl FnOnce() -> io::Result<()>,
        sync_selection: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 16], PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.verify_selection()?;
            let slot = self.active.slot.other();
            let destination = slot.path(&self.root);
            match fs::symlink_metadata(&destination) {
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "inactive generation slot is occupied",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            let mut stage = crate::staging::Stage::directory(&self.root)?;
            let expected = self
                .store
                .write_rebased_base(&stage.path().join("base"), limits.base)?;
            let next = RetainedStore::initialize(
                stage.path(),
                limits,
                &expected,
                Vec::new(),
                Some(self.active.catalog),
            )?;
            let generation = next.generation_id();
            if generation == self.active.generation {
                return Err(record::invalid_data(
                    "retained generation identifier collision",
                ));
            }
            stage.publish(&destination)?;
            sync_existing(&self.root)?;
            stage.publication_synchronized();
            next._lock.verify_identity(&destination)?;
            let active = Active {
                catalog: self.active.catalog,
                slot,
                generation,
                expected,
            };
            let (mut selection, mut file) = crate::staging::Stage::file(&self.root)?;
            file.write_all(&active.encode())?;
            file.sync_all()?;
            drop(file);
            before_select()?;

            tracker.begin_publication();
            self.uncertain = true;
            selection.replace_file(&self.root.join(ACTIVE_FILE))?;
            sync_selection(&self.root)?;
            selection.publication_synchronized();
            self.store = next;
            self.active = active;
            self.uncertain = false;
            Ok(generation)
        })
    }

    /// Retire only the catalog-owned inactive slot after verifying and
    /// synchronizing the active selection. A failed removal can leave an
    /// owned stage for explicit staging cleanup.
    pub fn reclaim_inactive(&mut self) -> io::Result<bool> {
        self.reclaim_using(sync_existing)
    }

    fn reclaim_using(
        &mut self,
        sync_after_move: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<bool> {
        self.verify_selection()?;
        let inactive = self.active.slot.other().path(&self.root);
        match fs::symlink_metadata(&inactive) {
            Ok(meta) if meta.file_type().is_dir() => {}
            Ok(_) => {
                return Err(record::invalid_data(
                    "invalid inactive generation directory",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        }
        self.store.verify_complete()?;
        sync_existing(&self.root)?;
        let _retired_lock = lock_directory(&inactive)?;
        check_retired(&inactive, &self.active)?;
        let mut stage = crate::staging::Stage::directory(&self.root)?;
        let moved = stage.child_path("retired".as_ref())?;
        crate::backup::publish_directory(&inactive, &moved)?;
        stage.preserve_payload_on_drop();
        sync_after_move(stage.path())?;
        fs::remove_dir_all(&moved)?;
        sync_existing(stage.path())?;
        stage.finish_retained_cleanup();
        Ok(true)
    }

    fn ensure_selected(&self) -> io::Result<()> {
        if self.uncertain {
            return Err(io::Error::other(
                "retained selection outcome uncertain; reopen required",
            ));
        }
        self.store.ensure_healthy()
    }

    fn verify_selection(&self) -> io::Result<()> {
        self.ensure_selected()?;
        self.lock.verify_identity(&self.root)?;
        self.store
            ._lock
            .verify_identity(&self.active.slot.path(&self.root))?;
        verify_active(&self.root, &self.active)
    }

    fn prepare_write(&mut self) -> Result<(), MutationFailure> {
        self.verify_selection().map_err(|error| {
            self.uncertain = true;
            MutationFailure::Rejected(error)
        })
    }
}

fn check_separation(source: &Path, parent: &Path) -> io::Result<()> {
    if parent.canonicalize()?.starts_with(source.canonicalize()?) {
        return Err(invalid_input(
            "destination parent is inside source directory",
        ));
    }
    Ok(())
}

fn verify_active(root: &Path, active: &Active) -> io::Result<()> {
    if read_catalog(root)? != active.catalog || Active::load(root, active.catalog)? != *active {
        return Err(record::invalid_data("active retained selection changed"));
    }
    Ok(())
}

fn write_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = create_file(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn lock_directory(path: &Path) -> io::Result<StoreLock> {
    if !path.symlink_metadata()?.file_type().is_dir() {
        return Err(record::invalid_data("invalid retained directory"));
    }
    let file = open_regular_with(&path.join("store.lock"), OFlags::RDWR)?;
    fs2::FileExt::try_lock_exclusive(&file)?;
    let lock = StoreLock::acquired(file);
    lock.verify_identity(path)?;
    Ok(lock)
}

fn catalog_bytes(id: [u8; 16]) -> [u8; 56] {
    let mut bytes = [0; 56];
    bytes[..8].copy_from_slice(CATALOG_MAGIC);
    bytes[8..24].copy_from_slice(&id);
    let digest = Sha256::digest(&bytes[..24]);
    bytes[24..].copy_from_slice(&digest);
    bytes
}

fn read_catalog(root: &Path) -> io::Result<[u8; 16]> {
    let bytes = exact(&root.join(CATALOG_FILE), 56)?;
    let id = bytes[8..24].try_into().unwrap();
    if id == [0; 16] || bytes != catalog_bytes(id) {
        return Err(record::invalid_data("invalid retained catalog identity"));
    }
    Ok(id)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slot {
    A,
    B,
}

impl Slot {
    fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    fn path(self, root: &Path) -> PathBuf {
        root.join(match self {
            Self::A => "generation-a",
            Self::B => "generation-b",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Active {
    catalog: [u8; 16],
    slot: Slot,
    generation: [u8; 16],
    expected: Expected,
}

impl Active {
    fn encode(&self) -> [u8; 256] {
        let mut bytes = [0; 256];
        bytes[..8].copy_from_slice(ACTIVE_MAGIC);
        bytes[8..24].copy_from_slice(&self.catalog);
        bytes[24] = match self.slot {
            Slot::A => 0,
            Slot::B => 1,
        };
        bytes[32..48].copy_from_slice(&self.generation);
        bytes[48..160].copy_from_slice(&self.expected.header);
        bytes[160..192].copy_from_slice(&self.expected.witness);
        bytes[192..224].copy_from_slice(&self.expected.base_digest);
        let digest = Sha256::digest(&bytes[..224]);
        bytes[224..].copy_from_slice(&digest);
        bytes
    }

    fn load(root: &Path, catalog: [u8; 16]) -> io::Result<Self> {
        let bytes = exact(&root.join(ACTIVE_FILE), 256)?;
        let invalid = || record::invalid_data("invalid active retained selection");
        let slot = match bytes[24] {
            0 => Slot::A,
            1 => Slot::B,
            _ => return Err(invalid()),
        };
        let active = Self {
            catalog,
            slot,
            generation: bytes[32..48].try_into().unwrap(),
            expected: Expected {
                header: bytes[48..160].try_into().unwrap(),
                witness: bytes[160..192].try_into().unwrap(),
                base_digest: bytes[192..224].try_into().unwrap(),
            },
        };
        if active.generation == [0; 16] || bytes != active.encode() {
            return Err(invalid());
        }
        Ok(active)
    }
}

fn check_retired(path: &Path, active: &Active) -> io::Result<()> {
    let mut count = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let known = ["base", "records.log", "commit.state", "store.lock"];
        if !known.iter().any(|&name| entry.file_name() == name) || !entry.file_type()?.is_file() {
            return Err(record::invalid_data(
                "unknown file in inactive retained generation",
            ));
        }
        count += 1;
    }
    if count != 4 {
        return Err(record::invalid_data(
            "incomplete inactive retained generation",
        ));
    }
    let log = open_regular(&path.join("records.log"))?;
    let retired = Header::read(&log, Some(active.catalog))?;
    let id = retired.id();
    if !retired.bytes().starts_with(b"EMDALOG4") || id == [0; 16] || id == active.generation {
        return Err(record::invalid_data(
            "invalid inactive retained generation identity",
        ));
    }
    let witness = open_regular(&path.join("commit.state"))?;
    decode_marker(&read_marker(&witness)?, retired.bytes())?;
    Ok(())
}
