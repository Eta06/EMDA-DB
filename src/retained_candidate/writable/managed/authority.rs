use super::*;

mod cutover;
mod file;
pub use cutover::{
    CutoverArchiveBinding, CutoverArchiveRetirementFailure, CutoverArchiveRetirementStep,
};
pub use file::FileRetainedAuthority;

#[cfg(test)]
mod tests;

/// Identity and exact suffix of one verified retained generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityPoint {
    pub catalog: [u8; 16],
    pub generation: [u8; 16],
    /// Binds the generation role, base binding, and recovery limits.
    pub header_digest: [u8; 32],
    pub suffix_end: u64,
    pub suffix_digest: [u8; 32],
}

impl AuthorityPoint {
    pub(super) fn new(catalog: [u8; 16], header: &Header, end: u64, digest: [u8; 32]) -> Self {
        Self {
            catalog,
            generation: header.id(),
            header_digest: Sha256::digest(header.bytes()).into(),
            suffix_end: end,
            suffix_digest: digest,
        }
    }

    pub(super) fn from_store(catalog: [u8; 16], store: &RetainedStore) -> Self {
        Self::new(
            catalog,
            &store.header,
            store.suffix_len as u64,
            store.hash.clone().finalize().into(),
        )
    }
}

/// A committed suffix and an optional durable intent for exactly one append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityRecord {
    pub revision: u64,
    pub committed: AuthorityPoint,
    pub pending: Option<AuthorityPoint>,
}

/// The authority fence attempted by one inactive-generation reclamation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReclamationFenceOutcome {
    /// This call did not submit an authority fence.
    NotAttempted,
    /// The provider did not confirm the fence. Reopen before retrying.
    Unconfirmed,
    /// The provider confirmed the fence before cleanup failed.
    Confirmed,
}

/// A failed authorized reclamation with independent fence and removal outcomes.
#[derive(Debug)]
#[non_exhaustive]
pub struct AuthorizedReclamationFailure {
    /// Outcome of this call's authority fence.
    pub fence: ReclamationFenceOutcome,
    /// Outcome of any removal, including previously interrupted cleanup resumed
    /// in this call before the authority fence.
    pub cleanup: StagingCleanupFailure,
}

impl std::fmt::Display for AuthorizedReclamationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fence = match self.fence {
            ReclamationFenceOutcome::NotAttempted => "authority fence not attempted",
            ReclamationFenceOutcome::Unconfirmed => "authority fence unconfirmed",
            ReclamationFenceOutcome::Confirmed => "authority fence confirmed",
        };
        write!(formatter, "{fence}, {}", self.cleanup)
    }
}

impl std::error::Error for AuthorizedReclamationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cleanup)
    }
}

/// Publication of a fresh authorized backup restore failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum AuthorizedRestoreFailure {
    /// This call did not start publishing its verified destination.
    NotPublished(io::Error),
    /// Reconcile the destination against this independently retained record
    /// with `AuthorizedRetainedStore::register_prepared` before opening it.
    OutcomeUnknown {
        error: io::Error,
        prepared: Box<AuthorityRecord>,
    },
}

impl std::fmt::Display for AuthorizedRestoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPublished(error) => {
                write!(formatter, "restore publication not started: {error}")
            }
            Self::OutcomeUnknown { error, .. } => {
                write!(formatter, "restore publication outcome unknown: {error}")
            }
        }
    }
}

impl std::error::Error for AuthorizedRestoreFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error) | Self::OutcomeUnknown { error, .. } => Some(error),
        }
    }
}

impl AuthorityRecord {
    fn initial(committed: AuthorityPoint) -> Self {
        Self {
            revision: 0,
            committed,
            pending: None,
        }
    }

    fn finalized(self, point: AuthorityPoint) -> io::Result<Self> {
        Ok(Self {
            revision: self
                .revision
                .checked_add(1)
                .ok_or_else(|| io::Error::other("authority revision exhausted"))?,
            committed: point,
            pending: None,
        })
    }

    fn validate(&self) -> io::Result<()> {
        if let Some(next) = self.pending {
            if next.catalog != self.committed.catalog
                || next.generation != self.committed.generation
                || next.header_digest != self.committed.header_digest
                || next.suffix_end <= self.committed.suffix_end
            {
                return Err(record::invalid_data("invalid retained authority intent"));
            }
        }
        Ok(())
    }

    pub(super) fn check(&self, observed: AuthorityPoint) -> io::Result<()> {
        self.validate()?;
        if self.committed != observed && self.pending != Some(observed) {
            return Err(record::invalid_data(
                "retained state does not match independent authority",
            ));
        }
        Ok(())
    }

    pub(super) fn check_end(&self, end: u64) -> io::Result<()> {
        if end != self.committed.suffix_end
            && self.pending.is_none_or(|next| end != next.suffix_end)
        {
            return Err(record::invalid_data(
                "retained suffix length does not match independent authority",
            ));
        }
        Ok(())
    }
}

/// A trusted, independent authority for a single logical catalog lineage.
/// Reads and comparisons must be linearizable. Each reported CAS success
/// must survive provider restart and storage rollback of the local catalog.
/// CAS compares the entire record atomically. Errors can mean the replacement
/// took effect. The caller must prevent rollback, deletion, or reuse of this
/// authority's lineage outside the protocol.
pub trait RetainedAuthority {
    fn read(&mut self) -> io::Result<Option<AuthorityRecord>>;

    fn compare_and_swap(
        &mut self,
        previous: Option<AuthorityRecord>,
        next: AuthorityRecord,
    ) -> io::Result<bool>;
}

/// A bounded retained catalog whose appends and generation switches require
/// durable external consent.
pub struct AuthorizedRetainedStore<A> {
    inner: ManagedRetainedStore,
    authority: A,
    current: AuthorityRecord,
    fence: Option<cutover::Fence>,
    auto_reclaim: Option<NonZeroU64>,
    cleanup_pending: bool,
}

impl<A: RetainedAuthority> AuthorizedRetainedStore<A> {
    /// Prepare a new externally fenced catalog from a separate witnessed store.
    /// Save the returned record and register it with the independent provider
    /// using CAS from an absent authority record before opening the catalog.
    /// The original store remains independently writable.
    pub fn prepare_from_store(
        source: Store,
        source_path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        export: RetainedExportLimits,
        limits: RetainedStoreLimits,
    ) -> Result<AuthorityRecord, PublicationFailure> {
        Self::prepare_from_store_using(
            source,
            source_path.as_ref(),
            destination.as_ref(),
            export,
            limits.into(),
        )
    }

    /// Prepare a separately authorized catalog under a persistent quota.
    /// Keep the authority provider outside the catalog directory.
    pub fn prepare_from_store_with_storage_limits(
        source: Store,
        source_path: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        export: RetainedExportLimits,
        limits: RetainedStoreLimits,
        storage: CatalogStorageLimits,
    ) -> Result<AuthorityRecord, PublicationFailure> {
        Self::prepare_from_store_using(
            source,
            source_path.as_ref(),
            destination.as_ref(),
            export,
            InitialLimits::with_storage(limits, storage),
        )
    }

    fn prepare_from_store_using(
        source: Store,
        source_path: &Path,
        destination: &Path,
        export: RetainedExportLimits,
        limits: InitialLimits,
    ) -> Result<AuthorityRecord, PublicationFailure> {
        let committed = ManagedRetainedStore::copy_from_store_role(
            source,
            source_path,
            destination,
            export,
            limits,
            AUTHORIZED_MAGIC,
            sync_existing,
        )?;
        Ok(AuthorityRecord::initial(committed))
    }

    /// Restore an independently bound backup as a fresh authorized catalog.
    /// The source and backup remain unchanged. Use a new destination and a
    /// fresh independent authority for the restored catalog. The binding
    /// identifies the snapshot; the caller determines its freshness. The
    /// backup and its directories must remain trusted and stable.
    ///
    /// Retain the resulting record independently, including on an uncertain
    /// publication. Register it with `Self::register_prepared` before use.
    pub fn prepare_from_backup(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &SuffixExpected,
    ) -> Result<AuthorityRecord, AuthorizedRestoreFailure> {
        Self::prepare_from_backup_using(
            backup.as_ref(),
            destination.as_ref(),
            limits.into(),
            expected,
            sync_existing,
            |_| Ok(()),
        )
    }

    /// Prepare a bound backup restore with a persistent catalog quota.
    pub fn prepare_from_backup_with_storage_limits(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        storage: CatalogStorageLimits,
        expected: &SuffixExpected,
    ) -> Result<AuthorityRecord, AuthorizedRestoreFailure> {
        Self::prepare_from_backup_using(
            backup.as_ref(),
            destination.as_ref(),
            InitialLimits::with_storage(limits, storage),
            expected,
            sync_existing,
            |_| Ok(()),
        )
    }

    /// Prepare a bound restore and durably record its verified initial
    /// authority record before attempting destination publication.
    ///
    /// `record_prepared` runs after the private generation has been verified
    /// and synchronized. It must persist the record independently of the
    /// destination and return success only after that record is durable.
    /// Keep this receipt separate from the authority used to open the
    /// catalog. If the callback fails, the catalog is left unpublished.
    /// A saved receipt can outlive a failed or interrupted publication.
    /// Reconcile it with the exact destination using `Self::register_prepared`.
    pub fn prepare_from_backup_recorded(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        storage: Option<CatalogStorageLimits>,
        expected: &SuffixExpected,
        record_prepared: impl FnOnce(AuthorityRecord) -> io::Result<()>,
    ) -> Result<AuthorityRecord, AuthorizedRestoreFailure> {
        let limits = match storage {
            Some(storage) => InitialLimits::with_storage(limits, storage),
            None => limits.into(),
        };
        Self::prepare_from_backup_using(
            backup.as_ref(),
            destination.as_ref(),
            limits,
            expected,
            sync_existing,
            record_prepared,
        )
    }

    fn prepare_from_backup_using(
        backup: &Path,
        destination: &Path,
        limits: InitialLimits,
        expected: &SuffixExpected,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
        record_prepared: impl FnOnce(AuthorityRecord) -> io::Result<()>,
    ) -> Result<AuthorityRecord, AuthorizedRestoreFailure> {
        let mut prepared = None;
        let result = ManagedRetainedStore::restore_backup_bound(
            backup,
            destination,
            limits,
            expected,
            InitialPublication {
                magic: AUTHORIZED_MAGIC,
                sync_parent,
                verified: |point| {
                    let initial = AuthorityRecord::initial(point);
                    record_prepared(initial)?;
                    prepared = Some(Box::new(initial));
                    Ok(())
                },
            },
        );
        result
            .map(AuthorityRecord::initial)
            .map_err(|failure| match failure {
                PublicationFailure::NotPublished(error) => {
                    AuthorizedRestoreFailure::NotPublished(error)
                }
                PublicationFailure::OutcomeUnknown(error) => {
                    AuthorizedRestoreFailure::OutcomeUnknown {
                        error,
                        prepared: prepared.expect("publication follows verification"),
                    }
                }
            })
    }

    /// Verify and synchronize a prepared catalog before installing its exact
    /// initial record in a fresh independent authority. Retain the trusted
    /// preparation record for retries, including after an uncertain restore
    /// or an authority error. The same record may already exist if a prior
    /// registration took effect. A different record is never replaced.
    /// The destination and provider must remain trusted; this is for a new
    /// lineage, with the authority outside the measured catalog.
    pub fn register_prepared(
        root: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        mut authority: A,
        prepared: AuthorityRecord,
    ) -> io::Result<Self> {
        if prepared.revision != 0 || prepared.pending.is_some() {
            return Err(invalid_input(
                "expected an initial retained authority record",
            ));
        }
        let previous = authority.read()?;
        if previous.is_some() && previous != Some(prepared) {
            return Err(record::invalid_data(
                "retained authority already has a different record",
            ));
        }
        let inner = ManagedRetainedStore::open_role(
            root.as_ref(),
            limits,
            AUTHORIZED_MAGIC,
            (None, None),
            |root, catalog| select_authorized(root, catalog, prepared.committed),
            Some(prepared),
        )?;
        // Recheck the provider under the catalog lock, including on a retry.
        // CAS publishes only the independently bound, synchronized state.
        if previous.is_none() && !authority.compare_and_swap(None, prepared)? {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retained authority comparison lost",
            ));
        }
        let mut store = Self {
            inner,
            authority,
            current: prepared,
            fence: None,
            auto_reclaim: None,
            cleanup_pending: false,
        };
        store.verify_current()?;
        Ok(store)
    }

    /// Match the fully verified local suffix against the external record.
    /// A durable intent can resolve to its exact prior or proposed suffix.
    /// Recovery synchronizes local state before finalizing that choice.
    pub fn open(
        root: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        authority: A,
    ) -> io::Result<Self> {
        Self::open_using(
            root.as_ref(),
            limits,
            authority,
            (None, None),
            RetainedStore::sync_recovered_commit,
        )
    }

    /// Recover under the same logical suffix scan budget as
    /// [`RetainedStore::open_with_suffix_scan_limit`]. Admission precedes
    /// local witness adoption and resolution of any authority intent. An
    /// optional catalog storage ceiling also applies, and this open does
    /// not start automatic reclamation.
    pub fn open_with_suffix_scan_limit(
        root: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        authority: A,
        max_suffix_scan_bytes: u64,
        storage_limits: Option<CatalogStorageLimits>,
    ) -> io::Result<Self> {
        Self::open_using(
            root.as_ref(),
            limits,
            authority,
            (storage_limits, Some(max_suffix_scan_bytes)),
            RetainedStore::sync_recovered_commit,
        )
    }

    /// Recover with a per-open catalog limit, intersected with any persistent
    /// limit on the selected generation. For original-path cutovers this
    /// covers archives, abandoned preparations and stages. Authority-provider
    /// storage must be outside the measured tree.
    pub fn open_with_storage_limits(
        root: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        authority: A,
        storage_limits: CatalogStorageLimits,
        suffix_threshold: Option<NonZeroU64>,
    ) -> io::Result<Self> {
        let mut store = Self::open_using(
            root.as_ref(),
            limits,
            authority,
            (Some(storage_limits), None),
            RetainedStore::sync_recovered_commit,
        )?;
        if suffix_threshold.is_some() {
            store.finish_maintenance()?;
            store.auto_reclaim = suffix_threshold;
        }
        Ok(store)
    }

    fn open_using(
        root: &Path,
        limits: RetainedStoreLimits,
        mut authority: A,
        recovery_admission: (Option<CatalogStorageLimits>, Option<u64>),
        sync_recovered_commit: impl FnOnce(&RetainedStore) -> io::Result<()>,
    ) -> io::Result<Self> {
        let current = authority
            .read()?
            .ok_or_else(|| record::invalid_data("retained authority record is absent"))?;
        let fence = match fs::symlink_metadata(root.join(CATALOG_FILE)) {
            Ok(_) => None,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Some(cutover::Fence::load(root)?)
            }
            Err(error) => return Err(error),
        };
        let (catalog_path, role) = if let Some(fence) = &fence {
            if fence.initial().committed.catalog != current.committed.catalog {
                return Err(record::invalid_data("cutover authority catalog mismatch"));
            }
            (fence.catalog_path(), CUTOVER_MAGIC)
        } else {
            (root.to_path_buf(), AUTHORIZED_MAGIC)
        };
        let inner = ManagedRetainedStore::open_role(
            &catalog_path,
            limits,
            role,
            recovery_admission,
            |root, catalog| select_authorized(root, catalog, current.committed),
            Some(current),
        )?;
        let mut this = Self {
            inner,
            authority,
            current,
            fence,
            auto_reclaim: None,
            cleanup_pending: false,
        };
        if current.pending.is_some() {
            // A prior writer can stop after updating the visible witness but
            // before synchronizing it. Confirm durability on the pinned files
            // before the independent authority acknowledges this endpoint.
            sync_recovered_commit(&this.inner.store)?;
            let actual = AuthorityPoint::from_store(this.inner.active.catalog, &this.inner.store);
            this.advance(current.finalized(actual)?)?;
        } else {
            this.verify_current()?;
        }
        Ok(this)
    }

    /// Open with synchronous, per-handle maintenance. Reclaim a verified
    /// inactive slot on open and rotate before a write crosses the byte
    /// threshold or exhausts suffix admission. A write must fit an empty
    /// suffix. Rotation and reclamation each require an authority CAS.
    ///
    /// Maintenance can complete before a write is rejected. An uncertain
    /// authority update requires reopen before using the handle again.
    pub fn open_with_auto_reclaim(
        root: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        authority: A,
        suffix_threshold: NonZeroU64,
    ) -> io::Result<Self> {
        let mut store = Self::open(root, limits, authority)?;
        store.finish_maintenance()?;
        store.auto_reclaim = Some(suffix_threshold);
        Ok(store)
    }

    /// Back up the exact generation checked against the independent authority.
    /// Keep the returned binding separately from the backup.
    pub fn backup_to(
        &mut self,
        destination: impl AsRef<Path>,
    ) -> Result<SuffixExpected, crate::SuffixExportFailure> {
        self.verify_current()
            .map_err(crate::SuffixExportFailure::NotPublished)?;
        if let Some(fence) = &self.fence {
            fence
                .check_backup_parent(destination.as_ref())
                .map_err(crate::SuffixExportFailure::NotPublished)?;
        }
        self.inner.backup_to(destination)
    }

    pub fn get(&mut self, key: &[u8], max_value_bytes: usize) -> io::Result<Option<Vec<u8>>> {
        self.verify_current()?;
        self.inner.get(key, max_value_bytes)
    }

    pub fn scan(
        &mut self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        self.verify_current()?;
        self.inner.scan(after, before, limit)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MutationFailure> {
        self.write(Append::Put(key, value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), MutationFailure> {
        self.write(Append::Delete(key))
    }

    pub fn write_batch(
        &mut self,
        operations: &[BatchOperation<'_>],
    ) -> Result<(), MutationFailure> {
        self.write(Append::Batch(operations))
    }

    pub(crate) fn checked_document_reader(&mut self) -> io::Result<&ManagedRetainedStore> {
        self.verify_current()?;
        Ok(&self.inner)
    }

    pub(crate) fn write_batch_checking(
        &mut self,
        operations: &[BatchOperation<'_>],
        admit: impl Fn() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        admit().map_err(MutationFailure::Rejected)?;
        self.write_checking(Append::Batch(operations), admit)
    }

    /// Compact into the inactive slot and select it with one authority CAS.
    /// The successor is verified and synchronized before selection. An
    /// occupied inactive slot requires explicit reclamation.
    pub fn compact(&mut self) -> Result<[u8; 16], PublicationFailure> {
        self.compact_with_limits(self.inner.store.limits)
    }

    /// Compact with separate successor publication and authority outcomes.
    pub fn compact_detailed(&mut self) -> Result<[u8; 16], RetainedRotationFailure> {
        self.compact_with_limits_detailed(self.inner.store.limits)
    }

    /// Compact and adopt new limits. If selection is uncertain, retain both
    /// generations and reopen with a ceiling that admits either configuration.
    pub fn compact_with_limits(
        &mut self,
        limits: RetainedStoreLimits,
    ) -> Result<[u8; 16], PublicationFailure> {
        self.compact_with_limits_detailed(limits)
            .map_err(RetainedRotationFailure::into_selection_failure)
    }

    /// Compact and adopt new limits, distinguishing successor publication
    /// from an authority compare-and-swap attempt.
    pub fn compact_with_limits_detailed(
        &mut self,
        limits: RetainedStoreLimits,
    ) -> Result<[u8; 16], RetainedRotationFailure> {
        self.compact_detailed_using(limits, None, sync_existing)
    }

    /// Select a successor carrying persistent catalog storage limits. For
    /// an original-path catalog, the policy covers the whole original
    /// directory. Put the independent authority outside that scope.
    pub fn compact_with_persisted_storage_limits(
        &mut self,
        limits: CatalogStorageLimits,
    ) -> Result<[u8; 16], PublicationFailure> {
        self.compact_with_persisted_storage_limits_detailed(limits)
            .map_err(RetainedRotationFailure::into_selection_failure)
    }

    /// Publish a successor carrying a persistent storage policy and classify
    /// both publication attempts.
    pub fn compact_with_persisted_storage_limits_detailed(
        &mut self,
        limits: CatalogStorageLimits,
    ) -> Result<[u8; 16], RetainedRotationFailure> {
        self.compact_detailed_using(self.inner.store.limits, Some(limits), sync_existing)
    }

    fn compact_detailed_using(
        &mut self,
        limits: RetainedStoreLimits,
        persisted_storage: Option<CatalogStorageLimits>,
        sync_successor: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 16], RetainedRotationFailure> {
        crate::publication_outcome::track_retained_rotation(|tracker| {
            self.verify_current()?;
            let mut next = self.current.finalized(self.current.committed)?;
            let (active, store, storage) = self.inner.prepare_successor(
                limits,
                self.auto_reclaim.is_some(),
                persisted_storage,
                &tracker.checkpoint,
                sync_successor,
            )?;
            let point = AuthorityPoint::from_store(active.catalog, &store);
            next.committed = point;
            tracker.selection.begin_publication();
            self.inner.uncertain = true;
            self.advance(next)?;
            self.inner.store = store;
            self.inner.active = active;
            self.inner.storage = storage;
            self.inner.uncertain = false;
            self.cleanup_pending = self.auto_reclaim.is_some();
            Ok(point.generation)
        })
    }

    /// Lock and validate the catalog-owned inactive slot, then fence pending
    /// selection attempts from older revisions before retiring it. A failed
    /// comparison leaves both slots. Later errors may follow partial or
    /// complete removal of the inactive slot or its retirement stage.
    pub fn reclaim_inactive(&mut self) -> io::Result<bool> {
        self.reclaim_inactive_detailed()
            .map_err(StagingCleanupFailure::into_io_error)
    }

    /// Retire an inactive generation with the removal attempt classified.
    /// Moving the inactive slot counts as removal of its catalog name.
    /// The classification includes any stage cleanup resumed by this call.
    /// An error can leave the authority fence uncertain even if no removal
    /// started. Reopen if the handle reports an uncertain selection.
    pub fn reclaim_inactive_detailed(&mut self) -> Result<bool, StagingCleanupFailure> {
        self.reclaim_inactive_with_outcome()
            .map_err(|error| error.cleanup)
    }

    /// Reclaims the inactive generation and distinguishes authority fence
    /// failures from removal failures. The two outcomes describe this call.
    /// An unconfirmed fence disables this handle, even when this call began no
    /// removal. Earlier interrupted retirement can be resumed before a new
    /// fence is submitted. Inspect uncertain removal and reopen after an
    /// unconfirmed fence before deciding whether to retry.
    pub fn reclaim_inactive_with_outcome(&mut self) -> Result<bool, AuthorizedReclamationFailure> {
        self.reclaim_inactive_using(sync_existing)
    }

    fn reclaim_inactive_using(
        &mut self,
        sync_retirement: impl FnMut(&Path) -> io::Result<()>,
    ) -> Result<bool, AuthorizedReclamationFailure> {
        use ReclamationFenceOutcome::{Confirmed, NotAttempted, Unconfirmed};

        let mut outcome = NotAttempted;
        StagingCleanupFailure::track_removal(|removal_started| {
            self.verify_current()?;
            let resumed = self.inner.resume_retirement(removal_started)?;
            let Some((inactive, retired_lock)) = self.inner.lock_inactive_for_retirement()? else {
                return Ok(resumed);
            };
            let fence = self.current.finalized(self.current.committed)?;
            outcome = Unconfirmed;
            self.inner.uncertain = true;
            self.advance(fence)?;
            outcome = Confirmed;
            self.inner.uncertain = false;
            self.inner
                .reclaim_locked(&inactive, retired_lock, removal_started, sync_retirement)?;
            Ok(true)
        })
        .map_err(|cleanup| AuthorizedReclamationFailure {
            fence: outcome,
            cleanup,
        })
    }

    fn write(&mut self, append: Append<'_>) -> Result<(), MutationFailure> {
        self.write_checking(append, || Ok(()))
    }

    fn write_checking(
        &mut self,
        append: Append<'_>,
        admit: impl Fn() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        let encoded = self
            .inner
            .store
            .encode_append(append, self.auto_reclaim.is_some())?;
        self.verify_current().map_err(MutationFailure::Rejected)?;
        let Some(threshold) = self.auto_reclaim else {
            return self.append(&encoded, admit);
        };
        if self.cleanup_pending {
            admit().map_err(MutationFailure::Rejected)?;
            self.finish_maintenance()
                .map_err(MutationFailure::Rejected)?;
        }
        let suffix_len = self.inner.store.suffix_len as u64;
        if suffix_len == 0 {
            return self.append(&encoded, admit);
        }
        if suffix_len.saturating_add(encoded.len() as u64) <= threshold.get() {
            match self.append(&encoded, &admit) {
                Err(MutationFailure::Rejected(ref error))
                    if !self.inner.uncertain
                        && error
                            .get_ref()
                            .is_some_and(|cause| cause.is::<SuffixCapacity>()) => {}
                result => return result,
            }
        }
        admit().map_err(MutationFailure::Rejected)?;
        check_empty_suffix(&encoded, self.inner.store.limits).map_err(MutationFailure::Rejected)?;
        self.finish_maintenance()
            .map_err(MutationFailure::Rejected)?;
        admit().map_err(MutationFailure::Rejected)?;
        self.compact().map_err(|error| {
            MutationFailure::Rejected(io::Error::new(
                match &error {
                    PublicationFailure::NotPublished(cause)
                    | PublicationFailure::OutcomeUnknown(cause) => cause.kind(),
                },
                error,
            ))
        })?;
        self.finish_maintenance()
            .map_err(MutationFailure::Rejected)?;
        self.append(&encoded, admit)
    }

    fn finish_maintenance(&mut self) -> io::Result<()> {
        self.cleanup_pending = true;
        self.verify_current()?;
        sync_existing(&self.inner.root)?;
        crate::cleanup_abandoned_staging(&self.inner.root)?;
        self.reclaim_inactive()?;
        self.cleanup_pending = false;
        Ok(())
    }

    fn append(
        &mut self,
        encoded: &[u8],
        admit: impl FnOnce() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        self.inner
            .admit_append(encoded.len() as u64)
            .map_err(MutationFailure::Rejected)?;
        let current = self.current;
        let mut attempted = None;
        let authority = &mut self.authority;
        let result = self.inner.store.write_encoded(
            encoded,
            |end, digest| {
                let point = AuthorityPoint {
                    suffix_end: end,
                    suffix_digest: digest,
                    ..current.committed
                };
                let pending = AuthorityRecord {
                    pending: Some(point),
                    ..current.finalized(current.committed)?
                };
                let final_record = pending.finalized(point)?;
                admit()?;
                attempted = Some((pending, final_record));
                replace(authority, current, pending)
            },
            |_point| {
                #[cfg(test)]
                tests::crash::pause_at_commit_point(_point)?;
                Ok(())
            },
        );
        if attempted.is_some() {
            self.inner.uncertain = true;
        }
        result?;
        let (pending, final_record) =
            attempted.expect("append requires a pending authority record");
        self.current = pending;
        self.advance(final_record)
            .map_err(MutationFailure::OutcomeUnknown)?;
        self.inner.uncertain = false;
        Ok(())
    }

    fn verify_current(&mut self) -> io::Result<()> {
        if let Some(fence) = &self.fence {
            fence.verify()?;
        }
        self.inner.verify_selection()?;
        if self.authority.read()? != Some(self.current) {
            self.inner.uncertain = true;
            return Err(record::invalid_data("retained authority changed"));
        }
        Ok(())
    }

    fn advance(&mut self, next: AuthorityRecord) -> io::Result<()> {
        replace(&mut self.authority, self.current, next)?;
        self.current = next;
        Ok(())
    }
}

fn select_authorized(root: &Path, catalog: [u8; 16], point: AuthorityPoint) -> io::Result<Active> {
    if point.catalog != catalog {
        return Err(record::invalid_data("retained authority catalog mismatch"));
    }
    let mut selected = None;
    for slot in [Slot::A, Slot::B] {
        let path = slot.path(root);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        }
        let result = open_regular(&path.join("records.log"))
            .and_then(|log| Header::read(&log, GenerationRole::Authorized(catalog)));
        let header = match result {
            Ok(header) => header,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound
                        | io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::InvalidData
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        if header.id() != point.generation
            || <[u8; 32]>::from(Sha256::digest(header.bytes())) != point.header_digest
        {
            continue;
        }
        if selected.is_some() {
            return Err(record::invalid_data(
                "duplicate authorized retained generation",
            ));
        }
        selected = Some(Active {
            catalog,
            slot,
            generation: header.id(),
            expected: crate::retained_candidate::binding_from_header(
                header.bytes()[..112].try_into().unwrap(),
            ),
        });
    }
    selected
        .ok_or_else(|| record::invalid_data("independently selected retained generation is absent"))
}

fn replace(
    authority: &mut impl RetainedAuthority,
    old: AuthorityRecord,
    next: AuthorityRecord,
) -> io::Result<()> {
    if authority.compare_and_swap(Some(old), next)? {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "retained authority comparison lost",
        ))
    }
}
