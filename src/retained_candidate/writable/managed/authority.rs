use super::*;
use std::cell::Cell;

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
            store.suffix.len() as u64,
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

    fn check(&self, observed: AuthorityPoint) -> io::Result<()> {
        if let Some(next) = self.pending {
            if next.catalog != self.committed.catalog
                || next.generation != self.committed.generation
                || next.header_digest != self.committed.header_digest
                || next.suffix_end <= self.committed.suffix_end
            {
                return Err(record::invalid_data("invalid retained authority intent"));
            }
        }
        if self.committed != observed && self.pending != Some(observed) {
            return Err(record::invalid_data(
                "retained state does not match independent authority",
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
        let committed = ManagedRetainedStore::copy_from_store_role(
            source,
            source_path.as_ref(),
            destination.as_ref(),
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
        mut authority: A,
    ) -> io::Result<Self> {
        let current = authority
            .read()?
            .ok_or_else(|| record::invalid_data("retained authority record is absent"))?;
        let inner = ManagedRetainedStore::open_role(
            root.as_ref(),
            limits,
            AUTHORIZED_MAGIC,
            |root, catalog| select_authorized(root, catalog, current.committed),
            |observed| current.check(observed),
        )?;
        let mut this = Self {
            inner,
            authority,
            current,
        };
        if current.pending.is_some() {
            let actual = AuthorityPoint::from_store(this.inner.active.catalog, &this.inner.store);
            this.advance(current.finalized(actual)?)?;
        } else {
            this.verify_current()?;
        }
        Ok(this)
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
        crate::publication_outcome::track(|tracker| {
            self.verify_current()?;
            let mut next = self.current.finalized(self.current.committed)?;
            let (active, store) = self.inner.prepare_successor(limits)?;
            let point = AuthorityPoint::from_store(active.catalog, &store);
            next.committed = point;
            tracker.begin_publication();
            self.inner.uncertain = true;
            self.advance(next)?;
            self.inner.store = store;
            self.inner.active = active;
            self.inner.uncertain = false;
            Ok(point.generation)
        })
    }

    /// Retire the catalog-owned inactive slot after fencing pending selection
    /// attempts from older revisions. A failed comparison leaves both slots.
    pub fn reclaim_inactive(&mut self) -> io::Result<bool> {
        self.verify_current()?;
        let inactive = self.inner.active.slot.other().path(&self.inner.root);
        match fs::symlink_metadata(&inactive) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        let fence = self.current.finalized(self.current.committed)?;
        self.inner.uncertain = true;
        self.advance(fence)?;
        self.inner.uncertain = false;
        self.inner.reclaim_inactive()
    }

    fn write(&mut self, encoded: io::Result<Vec<u8>>) -> Result<(), MutationFailure> {
        let encoded = encoded.map_err(MutationFailure::Rejected)?;
        self.verify_current().map_err(MutationFailure::Rejected)?;
        let mut hash = self.inner.store.hash.clone();
        hash.update(&encoded);
        let end = suffix_end(
            self.inner.store.suffix.len(),
            encoded.len(),
            self.inner.store.limits,
        )
        .map_err(MutationFailure::Rejected)?;
        let point = AuthorityPoint::new(
            self.inner.active.catalog,
            &self.inner.store.header,
            end as u64,
            hash.finalize().into(),
        );
        let pending = AuthorityRecord {
            pending: Some(point),
            ..self
                .current
                .finalized(self.current.committed)
                .map_err(MutationFailure::Rejected)?
        };
        let final_record = pending
            .finalized(point)
            .map_err(MutationFailure::Rejected)?;
        let attempted = Cell::new(false);
        let authority = &mut self.authority;
        let result = self.inner.store.write_encoded(
            &encoded,
            || {
                attempted.set(true);
                replace(authority, self.current, pending)
            },
            |_| Ok(()),
        );
        if attempted.get() {
            self.inner.uncertain = true;
        }
        result?;
        self.current = pending;
        self.advance(final_record)
            .map_err(MutationFailure::OutcomeUnknown)?;
        self.inner.uncertain = false;
        Ok(())
    }

    fn verify_current(&mut self) -> io::Result<()> {
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
