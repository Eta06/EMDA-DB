use super::{
    Expected, GenerationRole, Header, OverlaySource, RecoveryLimits, RetainedStore,
    RetainedStoreLimits, StoreLock, SuffixImport, copy_base, create_file, decode_marker,
    encode_batch, invalid_input, open_regular, open_regular_with, random_id, read_marker,
    suffix_end,
};
use crate::directory::sync_existing;
use crate::record::{self, Operation};
use crate::retained_candidate::{SuffixCapacity, SuffixExpected, check_files, exact};
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
const AUTHORIZED_MAGIC: &[u8; 8] = b"EMDACT02";
const CUTOVER_MAGIC: &[u8; 8] = b"EMDACT03";
const ACTIVE_FILE: &str = "active.retained";
const ACTIVE_MAGIC: &[u8; 8] = b"EMDAAC01";
const ACTIVE_BYTES: usize = 256;
const CATALOG_BYTES: usize = 56;

#[derive(Clone, Copy)]
struct InitialLimits {
    generation: RetainedStoreLimits,
    storage: Option<InitialStorage>,
}

#[derive(Clone, Copy)]
struct InitialStorage {
    policy: CatalogStorageLimits,
    available: CatalogStorageLimits,
    archives: CatalogStorageLimits,
}

impl From<RetainedStoreLimits> for InitialLimits {
    fn from(generation: RetainedStoreLimits) -> Self {
        Self {
            generation,
            storage: None,
        }
    }
}

impl InitialLimits {
    fn with_storage(generation: RetainedStoreLimits, policy: CatalogStorageLimits) -> Self {
        Self {
            generation,
            storage: Some(InitialStorage {
                policy,
                available: policy,
                archives: CatalogStorageLimits {
                    bytes: 0,
                    entries: 0,
                },
            }),
        }
    }

    fn max_base_bytes(self) -> io::Result<u64> {
        let Some(storage) = self.storage else {
            return Ok(u64::MAX);
        };
        // Catalog lock, ID and selection, plus the generation directory
        // containing its lock, base, log and witness.
        if storage.available.entries < 8 + storage.archives.entries {
            return Err(storage::entry_limit());
        }
        let fixed = CATALOG_BYTES + ACTIVE_BYTES + super::QUOTA_HEADER_BYTES + super::WITNESS_BYTES;
        storage
            .available
            .bytes
            .checked_sub(fixed as u64)
            .and_then(|bytes| bytes.checked_sub(storage.archives.bytes))
            .ok_or_else(storage::byte_limit)
    }
}

mod authority;
mod backup;
mod storage;
pub use storage::CatalogStorageLimits;
use storage::{CatalogStorage, intersect};

pub use authority::{
    AuthorityPoint, AuthorityRecord, AuthorizedRetainedStore, CutoverArchiveBinding,
    FileRetainedAuthority, RetainedAuthority,
};

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
    catalog_magic: &'static [u8; 8],
    uncertain: bool,
    auto_reclaim: Option<NonZeroU64>,
    cleanup_pending: bool,
    storage: Option<CatalogStorage>,
    requested_storage: Option<CatalogStorageLimits>,
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
            limits.into(),
            expected,
            None,
        )
    }

    /// Publish a candidate as a catalog with a persistent logical storage quota.
    /// The quota applies to the catalog contents during staging and after open.
    pub fn create_with_storage_limits(
        candidate: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        storage: CatalogStorageLimits,
        expected: &Expected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::create_using(
            candidate.as_ref(),
            destination.as_ref(),
            InitialLimits::with_storage(limits, storage),
            expected,
            None,
        )
    }

    /// Publish a catalog with the independently bound detached suffix
    /// imported into its first generation.
    /// The suffix is copied in fixed-size chunks and replayed from staging.
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
            limits.into(),
            &expected.candidate,
            Some((suffix.as_ref(), expected)),
        )
    }

    /// Publish a bound candidate and detached suffix under a persistent quota.
    /// Admission accounts for the entire bound suffix before copying the base.
    pub fn create_from_suffix_with_storage_limits(
        candidate: impl AsRef<Path>,
        suffix: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        storage: CatalogStorageLimits,
        expected: &SuffixExpected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::create_using(
            candidate.as_ref(),
            destination.as_ref(),
            InitialLimits::with_storage(limits, storage),
            &expected.candidate,
            Some((suffix.as_ref(), expected)),
        )
    }

    fn create_using(
        candidate: &Path,
        destination: &Path,
        limits: InitialLimits,
        expected: &Expected,
        suffix: Option<(&Path, &SuffixExpected)>,
    ) -> Result<[u8; 16], PublicationFailure> {
        let parent = crate::backup::parent_directory(destination)
            .map_err(PublicationFailure::NotPublished)?;
        check_separation(candidate, parent).map_err(PublicationFailure::NotPublished)?;
        check_files(candidate, expected).map_err(PublicationFailure::NotPublished)?;
        let suffix_bytes = suffix.map_or(0, |(_, expected)| expected.end);
        let suffix = suffix
            .map(|(path, binding)| SuffixImport::open(path, limits.generation.suffix, binding))
            .transpose()
            .map_err(PublicationFailure::NotPublished)?;
        Self::publish_initial(
            destination,
            parent,
            limits,
            CATALOG_MAGIC,
            sync_existing,
            |_, path, max_base_bytes| {
                let max_base_bytes = max_base_bytes
                    .checked_sub(suffix_bytes)
                    .ok_or_else(storage::byte_limit)?;
                copy_base(candidate, path, limits.generation, expected, max_base_bytes)?;
                Ok((expected.clone(), suffix))
            },
        )
        .map(|point| point.generation)
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

    /// Copy a witnessed store into a catalog with a persistent storage quota.
    /// Source verification and staging complete before destination publication.
    pub fn copy_from_store_with_storage_limits(
        source: Store,
        source_path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        export: RetainedExportLimits,
        limits: RetainedStoreLimits,
        storage: CatalogStorageLimits,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::copy_from_store_role(
            source,
            source_path.as_ref(),
            destination.as_ref(),
            export,
            InitialLimits::with_storage(limits, storage),
            CATALOG_MAGIC,
            sync_existing,
        )
        .map(|point| point.generation)
    }

    fn copy_from_store_using(
        source: Store,
        source_path: &Path,
        destination: &Path,
        export: RetainedExportLimits,
        limits: RetainedStoreLimits,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::copy_from_store_role(
            source,
            source_path,
            destination,
            export,
            limits.into(),
            CATALOG_MAGIC,
            sync_parent,
        )
        .map(|point| point.generation)
    }

    fn copy_from_store_role(
        source: Store,
        source_path: &Path,
        destination: &Path,
        export: RetainedExportLimits,
        limits: InitialLimits,
        catalog_magic: &'static [u8; 8],
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<AuthorityPoint, PublicationFailure> {
        let parent = crate::backup::parent_directory(destination)
            .map_err(PublicationFailure::NotPublished)?;
        check_separation(source_path, parent).map_err(PublicationFailure::NotPublished)?;
        let source_lock = source.share_store_lock();
        source_lock
            .verify_identity(source_path)
            .map_err(PublicationFailure::NotPublished)?;
        Self::publish_initial(
            destination,
            parent,
            limits,
            catalog_magic,
            sync_parent,
            |stage, path, max_base_bytes| {
                let expected = export_into_generation(
                    &source,
                    stage,
                    path,
                    export,
                    limits.generation,
                    max_base_bytes,
                )?;
                Ok((expected, None))
            },
        )
    }

    fn publish_initial(
        destination: &Path,
        parent: &Path,
        limits: InitialLimits,
        catalog_magic: &'static [u8; 8],
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        prepare: impl FnOnce(
            &crate::staging::Stage,
            &Path,
            u64,
        ) -> io::Result<(Expected, Option<SuffixImport>)>,
    ) -> Result<AuthorityPoint, PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            let max_base_bytes = limits.max_base_bytes()?;
            let mut stage = crate::staging::Stage::directory(parent)?;
            let root = stage.path();
            create_file(&root.join("store.lock"))?.sync_all()?;
            let catalog = random_id()?;
            write_new(
                &root.join(CATALOG_FILE),
                &catalog_bytes(catalog, catalog_magic),
            )?;
            let slot = Slot::A;
            let path = slot.path(root);
            fs::DirBuilder::new().mode(0o700).create(&path)?;
            let (expected, suffix) = prepare(&stage, &path, max_base_bytes)?;
            let store = RetainedStore::initialize(
                &path,
                limits.generation,
                &expected,
                suffix,
                generation_role(catalog_magic, catalog),
                limits.storage.map(|storage| storage.policy),
            )?;
            let point = AuthorityPoint::from_store(catalog, &store);
            drop(store);
            let active = Active {
                catalog,
                slot,
                generation: point.generation,
                expected,
            };
            write_new(&root.join(ACTIVE_FILE), &active.encode())?;
            if let Some(storage) = limits.storage {
                CatalogStorage {
                    root: root.to_path_buf(),
                    limits: storage.available,
                }
                .room(0, 0)?;
            }
            sync_existing(root)?;
            tracker.begin_publication();
            stage.publish(destination)?;
            sync_parent(parent)?;
            stage.publication_synchronized();
            Ok(point)
        })
    }

    /// Recover the exact selected generation and synchronize its namespace
    /// before admitting writes. Invalid or missing selections fail closed.
    /// The selected generation's persisted limits must fit within the
    /// recovery ceiling. Writes use the persisted limits.
    pub fn open(root: impl AsRef<Path>, ceiling: RetainedStoreLimits) -> io::Result<Self> {
        Self::open_role(
            root.as_ref(),
            ceiling,
            CATALOG_MAGIC,
            None,
            Active::load,
            None,
        )
    }

    /// Recover with a per-open whole-catalog logical storage limit. When the
    /// selected generation carries a persistent limit, the lower ceiling for
    /// each resource applies. Admission checks precede tail recovery, appends
    /// and generation publication. Automatic reclamation follows the same limit.
    pub fn open_with_storage_limits(
        root: impl AsRef<Path>,
        ceiling: RetainedStoreLimits,
        storage_limits: CatalogStorageLimits,
        suffix_threshold: Option<NonZeroU64>,
    ) -> io::Result<Self> {
        let root = root.as_ref();
        let mut store = Self::open_role(
            root,
            ceiling,
            CATALOG_MAGIC,
            Some(storage_limits),
            Active::load,
            None,
        )?;
        if suffix_threshold.is_some() {
            store.finish_maintenance()?;
            store.auto_reclaim = suffix_threshold;
        }
        Ok(store)
    }

    fn open_role(
        root: &Path,
        ceiling: RetainedStoreLimits,
        catalog_magic: &'static [u8; 8],
        requested_storage: Option<CatalogStorageLimits>,
        select: impl FnOnce(&Path, [u8; 16]) -> io::Result<Active>,
        authority: Option<AuthorityRecord>,
    ) -> io::Result<Self> {
        let lock = lock_directory(root)?;
        let root = root.canonicalize()?;
        lock.verify_identity(&root)?;
        let mut storage = None;
        let catalog = read_catalog(&root, catalog_magic)?;
        let active = select(&root, catalog)?;
        let path = active.slot.path(&root);
        let store = RetainedStore::open_role(
            &path,
            RecoveryLimits::Ceiling(ceiling),
            &active.expected,
            (
                generation_role(catalog_magic, catalog),
                Some(active.generation),
            ),
            |header, end| {
                if let Some(authority) = authority {
                    authority.check_end(end)?;
                }
                storage = catalog_storage(
                    &root,
                    catalog_magic,
                    intersect(requested_storage, header.storage_limits()),
                );
                if let Some(storage) = &storage {
                    storage.room(0, 0)?;
                }
                Ok(())
            },
            |directory, header, end, digest| {
                if let Some(authority) = authority {
                    authority.check(AuthorityPoint::new(catalog, header, end, digest))?;
                }
                sync_existing(directory)
            },
            || {
                lock.verify_identity(&root)?;
                verify_active(&root, &active, catalog_magic)
            },
        )?;
        Ok(Self {
            store,
            root,
            active,
            catalog_magic,
            uncertain: false,
            auto_reclaim: None,
            cleanup_pending: false,
            storage,
            requested_storage,
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

    pub(crate) fn get_admitted(
        &self,
        key: &[u8],
        admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        self.ensure_selected()?;
        self.store.get_admitted(key, admit)
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

    pub(crate) fn write_batch_checking(
        &mut self,
        operations: &[BatchOperation<'_>],
        admit: impl Fn() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        self.write_checking(|| encode_batch(operations), admit)
    }

    pub(crate) fn scan_with_staged<'a>(
        &'a self,
        after: &[u8],
        before: &[u8],
        limit: usize,
        staged: impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)>,
        admit: impl FnMut(&[u8], usize) -> bool,
    ) -> io::Result<Vec<Entry>> {
        self.ensure_selected()?;
        self.store.overlay.scan_with(
            &self.store.base,
            OverlaySource::Disk(self.store.suffix_file()),
            (Some(after), Some(before)),
            limit,
            staged,
            admit,
        )
    }

    fn write(
        &mut self,
        encode: impl FnOnce() -> io::Result<Vec<u8>>,
    ) -> Result<(), MutationFailure> {
        self.write_checking(encode, || Ok(()))
    }

    fn write_checking(
        &mut self,
        encode: impl FnOnce() -> io::Result<Vec<u8>>,
        admit: impl Fn() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        admit().map_err(MutationFailure::Rejected)?;
        self.prepare_write()?;
        let bytes = encode().map_err(MutationFailure::Rejected)?;
        let Some(threshold) = self.auto_reclaim else {
            return self.append(&bytes, admit);
        };
        if self.cleanup_pending {
            self.finish_maintenance()
                .map_err(MutationFailure::Rejected)?;
        }
        let suffix_len = self.store.suffix_len as u64;
        if suffix_len == 0 {
            return self.append(&bytes, admit);
        }
        if suffix_len.saturating_add(bytes.len() as u64) <= threshold.get() {
            match self.append(&bytes, &admit) {
                Err(MutationFailure::Rejected(ref error))
                    if error
                        .get_ref()
                        .is_some_and(|cause| cause.is::<SuffixCapacity>()) => {}
                result => return result,
            }
        }
        admit().map_err(MutationFailure::Rejected)?;
        self.compact_for_write(&bytes)?;
        self.prepare_write()?;
        self.append(&bytes, admit)
    }

    fn append(
        &mut self,
        bytes: &[u8],
        admit: impl FnOnce() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        self.storage_room(bytes.len() as u64, 0)
            .map_err(MutationFailure::Rejected)?;
        self.store.write_encoded(bytes, |_, _| admit(), |_| Ok(()))
    }

    fn storage_room(&self, reserve_bytes: u64, reserve_entries: u64) -> io::Result<u64> {
        self.storage.as_ref().map_or(Ok(u64::MAX), |storage| {
            storage.room(reserve_bytes, reserve_entries)
        })
    }

    fn compact_for_write(&mut self, bytes: &[u8]) -> Result<(), MutationFailure> {
        check_empty_suffix(bytes, self.store.limits).map_err(MutationFailure::Rejected)?;
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
        sync_existing(&self.root)?;
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
        self.compact_using_limits(limits, None, || Ok(()), sync_existing)
    }

    /// Publish a successor with persistent catalog storage limits. The
    /// preparation must fit both the live and requested policies. Retain
    /// headroom for rotation and reclamation before lowering a limit.
    pub fn compact_with_persisted_storage_limits(
        &mut self,
        limits: CatalogStorageLimits,
    ) -> Result<[u8; 16], PublicationFailure> {
        self.compact_using_limits(self.store.limits, Some(limits), || Ok(()), sync_existing)
    }

    #[cfg(test)]
    fn compact_using(
        &mut self,
        before_select: impl FnOnce() -> io::Result<()>,
        sync_selection: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 16], PublicationFailure> {
        self.compact_using_limits(self.store.limits, None, before_select, sync_selection)
    }

    fn compact_using_limits(
        &mut self,
        limits: RetainedStoreLimits,
        persisted_storage: Option<CatalogStorageLimits>,
        before_select: impl FnOnce() -> io::Result<()>,
        sync_selection: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 16], PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.verify_selection()?;
            let (active, next, storage) =
                self.prepare_successor(limits, self.auto_reclaim.is_some(), persisted_storage)?;
            let generation = active.generation;
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
            self.storage = storage;
            self.uncertain = false;
            Ok(generation)
        })
    }

    fn prepare_successor(
        &self,
        limits: RetainedStoreLimits,
        reclaim_after: bool,
        persisted_storage: Option<CatalogStorageLimits>,
    ) -> io::Result<(Active, RetainedStore, Option<CatalogStorage>)> {
        if crate::staging::has_retained_stage(&self.root)? {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retained retirement requires cleanup before rotation",
            ));
        }
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
        use crate::staging::Stage;
        let persisted_storage = persisted_storage.or_else(|| self.store.header.storage_limits());
        let next_limits = intersect(self.requested_storage, persisted_storage);
        let admission_limits =
            intersect(self.storage.as_ref().map(|quota| quota.limits), next_limits);
        let header_bytes = if persisted_storage.is_some() {
            super::QUOTA_HEADER_BYTES
        } else {
            super::MANAGED_HEADER_BYTES
        };
        let generation_metadata = (header_bytes + super::WITNESS_BYTES) as u64;
        let selection_bytes = if self.catalog_magic == CATALOG_MAGIC {
            Stage::FILE_MARKER_BYTES + ACTIVE_BYTES as u64
        } else {
            0
        };
        // Directory stage and marker, four successor files, plus a possible
        // selection stage. Reserving both markers covers failed cleanup.
        let entries = if selection_bytes != 0 {
            8
        } else if reclaim_after {
            // The published successor adds five paths. Reclamation needs
            // two more after the preparation marker has been cleaned.
            7
        } else {
            6
        };
        let max_base_bytes = catalog_storage(&self.root, self.catalog_magic, admission_limits)
            .map_or(Ok(u64::MAX), |quota| {
                quota.room(
                    (Stage::DIRECTORY_MARKER_BYTES + selection_bytes).max(if reclaim_after {
                        Stage::RETAINED_MARKER_BYTES
                    } else {
                        0
                    }) + generation_metadata,
                    entries,
                )
            })?;
        let mut stage = Stage::directory(&self.root)?;
        let expected =
            self.store
                .write_rebased_base(&stage.path().join("base"), limits, max_base_bytes)?;
        let next = RetainedStore::initialize(
            stage.path(),
            limits,
            &expected,
            None,
            generation_role(self.catalog_magic, self.active.catalog),
            persisted_storage,
        )?;
        self.store
            .verify_complete(&self.active.slot.path(&self.root))?;
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
        let storage = catalog_storage(&self.root, self.catalog_magic, next_limits);
        Ok((active, next, storage))
    }

    /// Retire only the catalog-owned inactive slot after verifying and
    /// synchronizing the active selection. Retry this operation to resume
    /// interrupted retirement of a stage bound to this replacement.
    pub fn reclaim_inactive(&mut self) -> io::Result<bool> {
        let resumed = self.resume_retirement()?;
        Ok(self.reclaim_using(sync_existing)? || resumed)
    }

    fn resume_retirement(&self) -> io::Result<bool> {
        if !crate::staging::has_retained_stage(&self.root)? {
            return Ok(false);
        }
        let result = crate::staging::cleanup_retained(&self.root, &self.active.generation, || {
            self.verify_selection()?;
            self.store
                .verify_complete(&self.active.slot.path(&self.root))
        })?;
        if crate::staging::has_retained_stage(&self.root)? {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retained retirement needs inspection",
            ));
        }
        Ok(result.removed_markers != 0)
    }

    fn reclaim_using(
        &mut self,
        mut sync_after_move: impl FnMut(&Path) -> io::Result<()>,
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
        self.reclaim_room()?;
        self.store
            .verify_complete(&self.active.slot.path(&self.root))?;
        sync_existing(&self.root)?;
        let _retired_lock = lock_directory(&inactive)?;
        check_retired(&inactive, &self.active, self.catalog_magic)?;
        let mut stage =
            crate::staging::Stage::retained_directory(&self.root, &self.active.generation)?;
        let moved = stage.path().join("retired");
        crate::backup::publish_noreplace(&inactive, &moved)?;
        stage.preserve_payload_on_drop();
        sync_after_move(stage.path())?;
        // Retire the original slot name durably before deleting its contents.
        sync_after_move(&self.root)?;
        crate::staging::remove_retained_stage(stage.path())?;
        stage.finish_retained_cleanup();
        Ok(true)
    }

    fn reclaim_room(&self) -> io::Result<()> {
        self.storage_room(crate::staging::Stage::RETAINED_MARKER_BYTES, 2)?;
        Ok(())
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
        verify_active(&self.root, &self.active, self.catalog_magic)
    }

    fn prepare_write(&mut self) -> Result<(), MutationFailure> {
        self.verify_selection().map_err(|error| {
            self.uncertain = true;
            MutationFailure::Rejected(error)
        })
    }
}

fn check_empty_suffix(bytes: &[u8], limits: RetainedStoreLimits) -> io::Result<()> {
    suffix_end(0, bytes.len(), limits)?;
    super::Overlay::default().prepare_append(bytes, 0, limits.suffix, limits.overlay)?;
    Ok(())
}

fn check_separation(source: &Path, parent: &Path) -> io::Result<()> {
    if parent.canonicalize()?.starts_with(source.canonicalize()?) {
        return Err(invalid_input(
            "destination parent is inside source directory",
        ));
    }
    Ok(())
}

fn verify_active(root: &Path, active: &Active, magic: &[u8; 8]) -> io::Result<()> {
    if read_catalog(root, magic)? != active.catalog
        || (magic == CATALOG_MAGIC && Active::load(root, active.catalog)? != *active)
    {
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

fn export_into_generation(
    source: &Store,
    stage: &crate::staging::Stage,
    path: &Path,
    mut export: RetainedExportLimits,
    limits: RetainedStoreLimits,
    max_base_bytes: u64,
) -> io::Result<Expected> {
    export.max_output_bytes = export
        .max_output_bytes
        .min(limits.base.file_bytes)
        .min(max_base_bytes)
        .min(limits.base.work_bytes.saturating_sub(1));
    let name = std::ffi::OsStr::new("base");
    let base = source.write_retained_base_in_stage(stage, name, random_id()?, export, limits)?;
    let expected = crate::retained_candidate::binding(&base, random_id()?)?;
    fs::rename(stage.child_path(name)?, path.join(name))?;
    Ok(expected)
}

fn unpack_publication(failure: PublicationFailure) -> io::Error {
    match failure {
        PublicationFailure::NotPublished(error) | PublicationFailure::OutcomeUnknown(error) => {
            error
        }
    }
}

fn generation_role(magic: &[u8; 8], catalog: [u8; 16]) -> GenerationRole {
    if magic == AUTHORIZED_MAGIC || magic == CUTOVER_MAGIC {
        GenerationRole::Authorized(catalog)
    } else {
        GenerationRole::Managed(catalog)
    }
}

fn catalog_storage(
    catalog: &Path,
    magic: &[u8; 8],
    limits: Option<CatalogStorageLimits>,
) -> Option<CatalogStorage> {
    limits.map(|limits| CatalogStorage {
        root: if magic == CUTOVER_MAGIC {
            catalog
                .parent()
                .expect("cutover catalog has an original-path parent")
                .to_path_buf()
        } else {
            catalog.to_path_buf()
        },
        limits,
    })
}

fn catalog_bytes(id: [u8; 16], magic: &[u8; 8]) -> [u8; CATALOG_BYTES] {
    let mut bytes = [0; CATALOG_BYTES];
    bytes[..8].copy_from_slice(magic);
    bytes[8..24].copy_from_slice(&id);
    let digest = Sha256::digest(&bytes[..24]);
    bytes[24..].copy_from_slice(&digest);
    bytes
}

fn read_catalog(root: &Path, magic: &[u8; 8]) -> io::Result<[u8; 16]> {
    let bytes = exact(&root.join(CATALOG_FILE), CATALOG_BYTES)?;
    let id = bytes[8..24].try_into().unwrap();
    if id == [0; 16] || bytes != catalog_bytes(id, magic) {
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
    fn encode(&self) -> [u8; ACTIVE_BYTES] {
        let mut bytes = [0; ACTIVE_BYTES];
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
        let bytes = exact(&root.join(ACTIVE_FILE), ACTIVE_BYTES)?;
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

fn check_retired(path: &Path, active: &Active, magic: &[u8; 8]) -> io::Result<()> {
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
    let retired = Header::read(&log, generation_role(magic, active.catalog))?;
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
