use super::*;

mod cutover;
mod file;
pub use cutover::CutoverArchiveBinding;
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

impl AuthorityRecord {
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
            && !self.pending.is_some_and(|next| end == next.suffix_end)
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
        Ok(AuthorityRecord {
            revision: 0,
            committed,
            pending: None,
        })
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
            None,
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
            Some(storage_limits),
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
        storage_limits: Option<CatalogStorageLimits>,
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
            storage_limits,
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
        self.write(record::encode(Operation::Put, key, value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), MutationFailure> {
        self.write(record::encode(Operation::Delete, key, &[]))
    }

    pub fn write_batch(
        &mut self,
        operations: &[BatchOperation<'_>],
    ) -> Result<(), MutationFailure> {
        self.write(encode_batch(operations))
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
        self.write_checking(encode_batch(operations), admit)
    }

    /// Compact into the inactive slot and select it with one authority CAS.
    /// The successor is verified and synchronized before selection. An
    /// occupied inactive slot requires explicit reclamation.
    pub fn compact(&mut self) -> Result<[u8; 16], PublicationFailure> {
        self.compact_with_limits(self.inner.store.limits)
    }

    /// Compact and adopt new limits. If selection is uncertain, retain both
    /// generations and reopen with a ceiling that admits either configuration.
    pub fn compact_with_limits(
        &mut self,
        limits: RetainedStoreLimits,
    ) -> Result<[u8; 16], PublicationFailure> {
        self.compact_using_limits(limits, None)
    }

    /// Select a successor carrying persistent catalog storage limits. For
    /// an original-path catalog, the policy covers the whole original
    /// directory. Put the independent authority outside that scope.
    pub fn compact_with_persisted_storage_limits(
        &mut self,
        limits: CatalogStorageLimits,
    ) -> Result<[u8; 16], PublicationFailure> {
        self.compact_using_limits(self.inner.store.limits, Some(limits))
    }

    fn compact_using_limits(
        &mut self,
        limits: RetainedStoreLimits,
        persisted_storage: Option<CatalogStorageLimits>,
    ) -> Result<[u8; 16], PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            self.verify_current()?;
            let mut next = self.current.finalized(self.current.committed)?;
            let (active, store, storage) = self.inner.prepare_successor(
                limits,
                self.auto_reclaim.is_some(),
                persisted_storage,
            )?;
            let point = AuthorityPoint::from_store(active.catalog, &store);
            next.committed = point;
            tracker.begin_publication();
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

    /// Retire the catalog-owned inactive slot after fencing pending selection
    /// attempts from older revisions. A failed comparison leaves both slots.
    pub fn reclaim_inactive(&mut self) -> io::Result<bool> {
        self.verify_current()?;
        let resumed = self.inner.resume_retirement()?;
        let inactive = self.inner.active.slot.other().path(&self.inner.root);
        match fs::symlink_metadata(&inactive) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(resumed),
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        self.inner.reclaim_room()?;
        let fence = self.current.finalized(self.current.committed)?;
        self.inner.uncertain = true;
        self.advance(fence)?;
        self.inner.uncertain = false;
        Ok(self.inner.reclaim_using(sync_existing)? || resumed)
    }

    fn write(&mut self, encoded: io::Result<Vec<u8>>) -> Result<(), MutationFailure> {
        self.write_checking(encoded, || Ok(()))
    }

    fn write_checking(
        &mut self,
        encoded: io::Result<Vec<u8>>,
        admit: impl Fn() -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        let encoded = encoded.map_err(MutationFailure::Rejected)?;
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
            .storage_room(encoded.len() as u64, 0)
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
