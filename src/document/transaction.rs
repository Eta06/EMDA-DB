//! Shared document transaction dispatch and atomic collection operations.
use super::{
    index::{self, IndexDefinition, Keys},
    key,
    online_index::IndexJob,
    schema::CollectionSchema,
    schema_migration::SchemaJob,
};
use crate::{
    CheckpointSnapshot, CheckpointTransaction, CheckpointView, Entry, Snapshot, Transaction,
    retained_candidate::ManagedRetainedStore,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Mutex};

pub(super) const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_INDEX_BUILD_DOCUMENTS: usize = 256;
const MAX_INDEX_BUILD_BYTES: usize = 32 * 1024 * 1024;
const MAX_INDEX_DROP_KEYS: usize = 768;
const MAX_INDEX_DROP_BYTES: usize = 32 * 1024 * 1024;

/// A stored document decoded by `DocumentTransaction::decode_stored`.
// The flat variant holds its members inline so that decoding a row does not
// allocate; values of this type live briefly on the stack, one row at a time.
#[allow(clippy::large_enum_variant)]
pub(crate) enum StoredDocument<'a> {
    /// Borrowed members of a flat document.
    Flat(super::flat::FlatDocument<'a>),
    /// Any other document, fully decoded.
    Full(Value),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Metadata {
    pub version: u32,
    pub schema: CollectionSchema,
    pub indexes: Vec<IndexDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<IndexJob>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_job: Option<SchemaJob>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dropping: bool,
}

/// Collections whose decoded metadata each thread remembers.
const METADATA_MEMO_ENTRIES: usize = 8;
/// Larger metadata is decoded every time instead of being remembered.
const METADATA_MEMO_MAX_BYTES: usize = 64 * 1024;

type MetadataMemo = Vec<(String, Vec<u8>, Arc<Metadata>)>;

thread_local! {
    /// Recently decoded metadata, most recent first, keyed by collection
    /// name and exact stored bytes.
    static METADATA_MEMO: std::cell::RefCell<MetadataMemo> =
        const { std::cell::RefCell::new(Vec::new()) };
}

enum BackendTransaction<'a, 'store> {
    Ordinary(&'a mut Transaction<'store>),
    Checkpoint(&'a mut CheckpointTransaction<'store>),
    Retained(&'a mut super::retained::RetainedTransaction<'store>),
    OrdinarySnapshot(&'a Snapshot<'store>),
    CheckpointSnapshot(&'a CheckpointSnapshot<'store>),
    HistoricalCheckpoint(&'a CheckpointView),
    RetainedSnapshot(&'a ManagedRetainedStore),
}

/// Cumulative logical read limits for a document transaction or snapshot wrapper.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocumentReadLimits {
    /// Maximum point requests, scan page requests, and admitted scan entries
    /// combined. Missing point reads and empty pages still consume one unit.
    pub max_read_units: usize,
    /// Maximum logical key and value bytes. Counts each point key, each
    /// admitted point value, and both key and value for each admitted scan entry.
    pub max_read_bytes: usize,
}

struct ReadBudget {
    remaining: DocumentReadLimits,
    exhausted: bool,
}

/// Logical work a document transaction or snapshot request has admitted so
/// far, counted in the same units as [`DocumentReadLimits`] and the write
/// limits of `DocumentRequestLimits`, whether or not limits are set. Refused
/// charges are not counted. Read-set validation at commit, encoding,
/// allocations and elapsed time are not included.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DocumentRequestUsage {
    /// Point requests, scan page requests and admitted scan entries.
    pub read_units: usize,
    /// Point keys, admitted point values, and admitted entry keys and values.
    pub read_bytes: usize,
    /// Puts and deletes submitted to staging, including index maintenance.
    pub write_units: usize,
    /// Staged key and value bytes; a delete counts its key.
    pub write_bytes: usize,
}

struct WriteBudget {
    units: usize,
    bytes: usize,
    exhausted: bool,
}

fn read_limit_exceeded() -> io::Error {
    key::invalid("document transaction read limit exceeded")
}

fn write_limit_exceeded() -> io::Error {
    key::invalid("document transaction write limit exceeded")
}

/// A document transaction backed by the same atomic commit as raw key/value
/// operations. Any failed mutation aborts the whole transaction, even when the
/// callback handles the error. Callback errors also discard all staged changes.
pub struct DocumentTransaction<'a, 'store> {
    backend: BackendTransaction<'a, 'store>,
    failed: bool,
    read_budget: Option<Mutex<ReadBudget>>,
    write_budget: Option<WriteBudget>,
    read_units: std::sync::atomic::AtomicUsize,
    read_bytes: std::sync::atomic::AtomicUsize,
    write_units: usize,
    write_bytes: usize,
}

impl<'a, 'store> DocumentTransaction<'a, 'store> {
    /// The logical work this request has admitted so far. Call it at the
    /// end of a callback to account for the whole request.
    pub fn request_usage(&self) -> DocumentRequestUsage {
        use std::sync::atomic::Ordering::Relaxed;
        DocumentRequestUsage {
            read_units: self.read_units.load(Relaxed),
            read_bytes: self.read_bytes.load(Relaxed),
            write_units: self.write_units,
            write_bytes: self.write_bytes,
        }
    }

    pub(super) fn checkpoint_backed(&self) -> bool {
        matches!(&self.backend, BackendTransaction::Checkpoint(_))
    }

    pub(super) fn ordinary(tx: &'a mut Transaction<'store>) -> Self {
        Self::with_backend(BackendTransaction::Ordinary(tx))
    }

    pub(super) fn checkpoint(tx: &'a mut CheckpointTransaction<'store>) -> Self {
        Self::with_backend(BackendTransaction::Checkpoint(tx))
    }

    pub(super) fn retained(tx: &'a mut super::retained::RetainedTransaction<'store>) -> Self {
        Self::with_backend(BackendTransaction::Retained(tx))
    }

    pub(super) fn ordinary_snapshot(snapshot: &'a Snapshot<'store>) -> Self {
        Self::with_backend(BackendTransaction::OrdinarySnapshot(snapshot))
    }

    pub(super) fn checkpoint_snapshot(snapshot: &'a CheckpointSnapshot<'store>) -> Self {
        Self::with_backend(BackendTransaction::CheckpointSnapshot(snapshot))
    }

    pub(super) fn historical_checkpoint(view: &'a CheckpointView) -> Self {
        Self::with_backend(BackendTransaction::HistoricalCheckpoint(view))
    }

    pub(super) fn retained_snapshot(view: &'a ManagedRetainedStore) -> Self {
        Self::with_backend(BackendTransaction::RetainedSnapshot(view))
    }

    fn with_backend(backend: BackendTransaction<'a, 'store>) -> Self {
        Self {
            backend,
            failed: false,
            read_budget: None,
            write_budget: None,
            read_units: std::sync::atomic::AtomicUsize::new(0),
            read_bytes: std::sync::atomic::AtomicUsize::new(0),
            write_units: 0,
            write_bytes: 0,
        }
    }

    pub(super) fn limit_reads(&mut self, limits: DocumentReadLimits) {
        self.read_budget = Some(Mutex::new(ReadBudget {
            remaining: limits,
            exhausted: false,
        }));
    }

    pub(super) fn limit_writes(&mut self, units: usize, bytes: usize) {
        self.write_budget = Some(WriteBudget {
            units,
            bytes,
            exhausted: false,
        });
    }

    fn charge_write(&mut self, key_bytes: usize, value_bytes: usize) -> io::Result<()> {
        let bytes = key_bytes.saturating_add(value_bytes);
        let Some(budget) = &mut self.write_budget else {
            self.count_write(bytes);
            return Ok(());
        };
        if let Some(bytes) = key_bytes.checked_add(value_bytes) {
            if !budget.exhausted && budget.units != 0 && bytes <= budget.bytes {
                budget.units -= 1;
                budget.bytes -= bytes;
                self.count_write(bytes);
                return Ok(());
            }
        }
        budget.exhausted = true;
        Err(write_limit_exceeded())
    }

    fn count_write(&mut self, bytes: usize) {
        self.write_units = self.write_units.saturating_add(1);
        self.write_bytes = self.write_bytes.saturating_add(bytes);
    }

    fn charge_read(&self, units: usize, bytes: usize) -> io::Result<()> {
        if let Some(budget) = &self.read_budget {
            let mut budget = budget
                .lock()
                .map_err(|_| io::Error::other("document read budget poisoned"))?;
            if budget.exhausted
                || units > budget.remaining.max_read_units
                || bytes > budget.remaining.max_read_bytes
            {
                budget.exhausted = true;
                return Err(match self.backend {
                    BackendTransaction::OrdinarySnapshot(_)
                    | BackendTransaction::CheckpointSnapshot(_)
                    | BackendTransaction::HistoricalCheckpoint(_)
                    | BackendTransaction::RetainedSnapshot(_) => {
                        key::invalid("document snapshot read limit exceeded")
                    }
                    _ => read_limit_exceeded(),
                });
            }
            budget.remaining.max_read_units -= units;
            budget.remaining.max_read_bytes -= bytes;
        }
        use std::sync::atomic::Ordering::Relaxed;
        self.read_units.fetch_add(units, Relaxed);
        self.read_bytes.fetch_add(bytes, Relaxed);
        Ok(())
    }

    pub(super) fn finish<T>(self, result: io::Result<T>) -> io::Result<T> {
        let value = result?;
        if let Some(budget) = self.read_budget {
            let budget = budget
                .into_inner()
                .map_err(|_| io::Error::other("document read budget poisoned"))?;
            if budget.exhausted {
                return Err(read_limit_exceeded());
            }
        }
        if self.write_budget.is_some_and(|budget| budget.exhausted) {
            return Err(write_limit_exceeded());
        }
        if self.failed {
            return Err(io::Error::other(
                "document transaction contains a failed mutation",
            ));
        }
        Ok(value)
    }

    pub(super) fn mutate<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> io::Result<T>,
    ) -> io::Result<T> {
        if self.failed {
            return Err(io::Error::other("document transaction has already failed"));
        }
        match operation(self) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }

    pub(super) fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.read_admitted(key, |_| Ok(()))
    }

    fn read_admitted(
        &self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        self.charge_read(1, key.len())?;
        let admit = |bytes| {
            admit(bytes)?;
            self.charge_read(0, bytes)
        };
        match &self.backend {
            BackendTransaction::Ordinary(tx) => tx.get_admitted(key, admit),
            BackendTransaction::Checkpoint(tx) => tx.get_admitted(key, admit),
            BackendTransaction::Retained(tx) => tx.get_admitted(key, admit),
            BackendTransaction::OrdinarySnapshot(snapshot) => snapshot.get_admitted(key, admit),
            BackendTransaction::CheckpointSnapshot(snapshot) => snapshot.get_admitted(key, admit),
            BackendTransaction::HistoricalCheckpoint(view) => view.get_admitted(key, admit),
            BackendTransaction::RetainedSnapshot(view) => view.get_admitted(key, admit),
        }
    }

    pub(super) fn read_document_bytes(
        &self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        self.read_admitted(key, |len| {
            if len > MAX_DOCUMENT_BYTES {
                return Err(invalid_data("stored document exceeds size limit"));
            }
            admit(len)
        })
    }

    pub(super) fn with_point_reader<T>(
        &self,
        action: impl FnOnce(&mut PointReader<'_, '_, 'a, 'store>) -> io::Result<T>,
    ) -> io::Result<T> {
        match &self.backend {
            BackendTransaction::Ordinary(tx) => tx.with_point_session(|session| {
                let mut reader = PointReader {
                    tx: self,
                    backend: PointReaderBackend::Ordinary(session),
                };
                action(&mut reader)
            }),
            BackendTransaction::Checkpoint(tx) => tx.with_point_session(|session| {
                let mut reader = PointReader {
                    tx: self,
                    backend: PointReaderBackend::Checkpoint(session),
                };
                action(&mut reader)
            }),
            BackendTransaction::OrdinarySnapshot(snapshot) => {
                snapshot.with_point_session(|session| {
                    let mut reader = PointReader {
                        tx: self,
                        backend: PointReaderBackend::OrdinarySnapshot(session),
                    };
                    action(&mut reader)
                })
            }
            BackendTransaction::CheckpointSnapshot(snapshot) => {
                snapshot.with_point_session(|session| {
                    let mut reader = PointReader {
                        tx: self,
                        backend: PointReaderBackend::CheckpointSnapshot(session),
                    };
                    action(&mut reader)
                })
            }
            BackendTransaction::HistoricalCheckpoint(view) => view.with_point_session(|session| {
                let mut reader = PointReader {
                    tx: self,
                    backend: PointReaderBackend::HistoricalCheckpoint(session),
                };
                action(&mut reader)
            }),
            BackendTransaction::Retained(_) | BackendTransaction::RetainedSnapshot(_) => {
                let mut reader = PointReader {
                    tx: self,
                    backend: PointReaderBackend::Direct,
                };
                action(&mut reader)
            }
        }
    }

    pub(super) fn has_index_entry(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self
            .read_admitted(key, |len| {
                if len != 0 {
                    return Err(invalid_data("document index entry is invalid"));
                }
                Ok(())
            })?
            .is_some())
    }

    pub(super) fn read_unique_owner(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let owner = self.read_admitted(key, |len| {
            if !(1..=key::MAX_ID_BYTES).contains(&len) {
                return Err(invalid_data("invalid unique index owner length"));
            }
            Ok(())
        })?;
        if owner
            .as_deref()
            .is_some_and(|bytes| std::str::from_utf8(bytes).is_err())
        {
            return Err(invalid_data("invalid unique index owner"));
        }
        Ok(owner)
    }

    pub(super) fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.charge_write(key.len(), value.len())?;
        match &mut self.backend {
            BackendTransaction::Ordinary(tx) => tx.put(key, value),
            BackendTransaction::Checkpoint(tx) => tx.put(key, value),
            BackendTransaction::Retained(tx) => tx.stage(crate::BatchOperation::Put { key, value }),
            BackendTransaction::OrdinarySnapshot(_)
            | BackendTransaction::CheckpointSnapshot(_)
            | BackendTransaction::HistoricalCheckpoint(_)
            | BackendTransaction::RetainedSnapshot(_) => {
                Err(key::invalid("document snapshot is read-only"))
            }
        }
    }

    pub(super) fn remove(&mut self, key: &[u8]) -> io::Result<()> {
        self.charge_write(key.len(), 0)?;
        match &mut self.backend {
            BackendTransaction::Ordinary(tx) => tx.delete(key),
            BackendTransaction::Checkpoint(tx) => tx.delete(key),
            BackendTransaction::Retained(tx) => tx.stage(crate::BatchOperation::Delete { key }),
            BackendTransaction::OrdinarySnapshot(_)
            | BackendTransaction::CheckpointSnapshot(_)
            | BackendTransaction::HistoricalCheckpoint(_)
            | BackendTransaction::RetainedSnapshot(_) => {
                Err(key::invalid("document snapshot is read-only"))
            }
        }
    }

    pub(super) fn scan(&self, after: &[u8], before: &[u8], limit: usize) -> io::Result<Vec<Entry>> {
        self.scan_admitted(after, before, limit, |_, _| true)
    }

    pub(super) fn scan_admitted(
        &self,
        after: &[u8],
        before: &[u8],
        limit: usize,
        mut admit: impl FnMut(&[u8], usize) -> bool,
    ) -> io::Result<Vec<Entry>> {
        self.charge_read(1, 0)?;
        let mut budget_error = None;
        let admit = |key: &[u8], bytes| {
            if !admit(key, bytes) {
                return false;
            }
            budget_error = self.charge_read(1, key.len() + bytes).err();
            budget_error.is_none()
        };
        let page = match &self.backend {
            BackendTransaction::Ordinary(tx) => {
                tx.scan_admitted(Some(after), Some(before), limit, admit)
            }
            BackendTransaction::Checkpoint(tx) => {
                tx.scan_admitted(Some(after), Some(before), limit, admit)
            }
            BackendTransaction::Retained(tx) => tx.scan_admitted(after, before, limit, admit),
            BackendTransaction::OrdinarySnapshot(snapshot) => {
                snapshot.scan_admitted(Some(after), Some(before), limit, admit)
            }
            BackendTransaction::CheckpointSnapshot(snapshot) => {
                snapshot.scan_admitted(Some(after), Some(before), limit, admit)
            }
            BackendTransaction::HistoricalCheckpoint(view) => {
                view.scan_admitted(Some(after), Some(before), limit, admit)
            }
            BackendTransaction::RetainedSnapshot(view) => {
                view.scan_with_staged(after, before, limit, std::iter::empty(), admit)
            }
        }?;
        budget_error.map_or(Ok(page), Err)
    }

    /// Visits one bounded page as `scan_admitted` returns it, lending each
    /// entry. The ordinary and checkpoint backends read values in place;
    /// the others copy a page first. Budget charging and `admit` are those
    /// of `scan_admitted`.
    fn visit_admitted(
        &self,
        after: &[u8],
        before: &[u8],
        limit: usize,
        mut admit: impl FnMut(&[u8], usize) -> bool,
        mut visit: impl FnMut(&[u8], &[u8]) -> bool,
    ) -> io::Result<()> {
        let direct = matches!(
            &self.backend,
            BackendTransaction::Ordinary(_) | BackendTransaction::Checkpoint(_)
        );
        if !direct {
            for (key, value) in self.scan_admitted(after, before, limit, admit)? {
                if !visit(&key, &value) {
                    break;
                }
            }
            return Ok(());
        }
        self.charge_read(1, 0)?;
        let mut budget_error = None;
        let admit = |key: &[u8], bytes| {
            if !admit(key, bytes) {
                return false;
            }
            budget_error = self.charge_read(1, key.len() + bytes).err();
            budget_error.is_none()
        };
        match &self.backend {
            BackendTransaction::Ordinary(tx) => {
                tx.visit_admitted(Some(after), Some(before), limit, admit, visit)
            }
            BackendTransaction::Checkpoint(tx) => {
                tx.visit_admitted(Some(after), Some(before), limit, admit, visit)
            }
            _ => unreachable!("checked above"),
        }?;
        budget_error.map_or(Ok(()), Err)
    }

    /// Visits one bounded page of keys as `scan_admitted` would return them,
    /// lending each key without copying keys or values on ordinary and
    /// checkpoint transactions.
    pub(super) fn visit_admitted_keys(
        &self,
        after: &[u8],
        before: &[u8],
        limit: usize,
        admit: impl FnMut(&[u8], usize) -> bool,
        mut visit: impl FnMut(&[u8]) -> bool,
    ) -> io::Result<()> {
        self.visit_admitted(after, before, limit, admit, |key, _| visit(key))
    }

    pub(super) fn scan_document_page(
        &self,
        after: &[u8],
        before: &[u8],
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        let mut oversized = false;
        let page = self.scan_admitted(after, before, limit, |_, len| {
            oversized = len > MAX_DOCUMENT_BYTES;
            !oversized
        })?;
        if oversized {
            return Err(invalid_data("stored document exceeds size limit"));
        }
        Ok(page)
    }

    /// Reads an unreserved raw key within this document transaction.
    pub fn get_raw(&self, key_value: &[u8]) -> io::Result<Option<Vec<u8>>> {
        key::raw_key(key_value)?;
        self.read(key_value)
    }

    /// Stages an unreserved raw key in the same atomic commit as document work.
    pub fn put_raw(&mut self, key_value: &[u8], value: &[u8]) -> io::Result<()> {
        self.mutate(|this| {
            key::raw_key(key_value)?;
            this.write(key_value, value)
        })
    }

    /// Stages deletion of an unreserved raw key in this transaction.
    pub fn delete_raw(&mut self, key_value: &[u8]) -> io::Result<()> {
        self.mutate(|this| {
            key::raw_key(key_value)?;
            this.remove(key_value)
        })
    }

    pub(super) fn metadata(&self, collection: &str) -> io::Result<Arc<Metadata>> {
        let key = key::collection(b'M', collection)?;
        let bytes = self.read(&key)?;
        Self::decode_metadata(collection, bytes)
    }

    /// Reads and decodes collection metadata with the same read-set, read-budget,
    /// checksum, and dropping checks as `metadata`, avoiding heap allocations for
    /// the key and value buffer on memo hits.
    pub(super) fn metadata_fast(&self, collection: &str) -> io::Result<Arc<Metadata>> {
        key::name(collection)?;
        let mut key_buf = [0_u8; 160];
        let prefix = key::PREFIX;
        let key_len = prefix.len() + 1 + 2 + collection.len();
        key_buf[..prefix.len()].copy_from_slice(prefix);
        key_buf[prefix.len()] = b'M';
        key_buf[prefix.len() + 1..prefix.len() + 3]
            .copy_from_slice(&(collection.len() as u16).to_be_bytes());
        key_buf[prefix.len() + 3..key_len].copy_from_slice(collection.as_bytes());
        let key = &key_buf[..key_len];

        let decode_slice = |bytes: &[u8]| -> io::Result<Arc<Metadata>> {
            let found = METADATA_MEMO.with(|memo| {
                let mut memo = memo.borrow_mut();
                let at = memo.iter().position(|(name, stored, _)| {
                    name == collection && stored.as_slice() == bytes
                })?;
                let entry = memo.remove(at);
                let metadata = Arc::clone(&entry.2);
                memo.insert(0, entry);
                Some(metadata)
            });
            let metadata = match found {
                Some(metadata) => metadata,
                None => {
                    let metadata = Arc::new(Self::decode_metadata_uncached(collection, bytes)?);
                    if bytes.len() <= METADATA_MEMO_MAX_BYTES {
                        METADATA_MEMO.with(|memo| {
                            let mut memo = memo.borrow_mut();
                            memo.truncate(METADATA_MEMO_ENTRIES - 1);
                            memo.insert(
                                0,
                                (collection.to_owned(), bytes.to_vec(), Arc::clone(&metadata)),
                            );
                        });
                    }
                    metadata
                }
            };
            if metadata.dropping {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "collection deletion is in progress",
                ));
            }
            Ok(metadata)
        };

        let decoded = match &self.backend {
            BackendTransaction::Ordinary(tx) => {
                self.charge_read(1, key.len())?;
                tx.with_point_session(|session| {
                    session.visit_admitted_with_peek(
                        key,
                        false,
                        |len| self.charge_read(0, len),
                        |_| {},
                        |bytes, _| decode_slice(bytes),
                    )
                })?
            }
            BackendTransaction::Checkpoint(tx) => {
                self.charge_read(1, key.len())?;
                tx.with_point_session(|session| {
                    session.visit_admitted_with_peek(
                        key,
                        false,
                        |len| self.charge_read(0, len),
                        |_| {},
                        |bytes, _| decode_slice(bytes),
                    )
                })?
            }
            _ => {
                let bytes = self.read(key)?;
                return Self::decode_metadata(collection, bytes);
            }
        };
        decoded.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "collection does not exist"))
    }

    pub(super) fn metadata_including_drop(&self, collection: &str) -> io::Result<Arc<Metadata>> {
        let bytes = self.read(&key::collection(b'M', collection)?)?;
        Self::decode_metadata_including_drop(collection, bytes)
    }

    pub(super) fn decode_metadata(
        collection: &str,
        bytes: Option<Vec<u8>>,
    ) -> io::Result<Arc<Metadata>> {
        let metadata = Self::decode_metadata_including_drop(collection, bytes)?;
        if metadata.dropping {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "collection deletion is in progress",
            ));
        }
        Ok(metadata)
    }

    /// Decodes and checks collection metadata. Decoding is a function of the
    /// collection name and the exact stored bytes alone, so each thread
    /// remembers its last few results and reuses one when both match; the
    /// bytes are still read, with their read-set and budget effects, by
    /// every caller.
    fn decode_metadata_including_drop(
        collection: &str,
        bytes: Option<Vec<u8>>,
    ) -> io::Result<Arc<Metadata>> {
        let bytes = bytes
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "collection does not exist"))?;
        let found = METADATA_MEMO.with(|memo| {
            let mut memo = memo.borrow_mut();
            let at = memo
                .iter()
                .position(|(name, stored, _)| name == collection && *stored == bytes)?;
            let entry = memo.remove(at);
            let metadata = Arc::clone(&entry.2);
            memo.insert(0, entry);
            Some(metadata)
        });
        if let Some(metadata) = found {
            return Ok(metadata);
        }
        let metadata = Arc::new(Self::decode_metadata_uncached(collection, &bytes)?);
        if bytes.len() <= METADATA_MEMO_MAX_BYTES {
            METADATA_MEMO.with(|memo| {
                let mut memo = memo.borrow_mut();
                memo.truncate(METADATA_MEMO_ENTRIES - 1);
                memo.insert(0, (collection.to_owned(), bytes, Arc::clone(&metadata)));
            });
        }
        Ok(metadata)
    }

    fn decode_metadata_uncached(collection: &str, bytes: &[u8]) -> io::Result<Metadata> {
        let metadata: Metadata =
            serde_json::from_slice(bytes).map_err(|_| invalid_data("invalid document metadata"))?;
        let mut names = BTreeSet::new();
        if !matches!(metadata.version, 1..=4)
            || (matches!(metadata.version, 3 | 4) != metadata.dropping)
            || (metadata.dropping && (!metadata.jobs.is_empty() || metadata.schema_job.is_some()))
            || (metadata.version == 1
                && (!metadata.jobs.is_empty() || metadata.schema_job.is_some()))
            || metadata.schema.validate_definition().is_err()
            || metadata
                .schema_job
                .as_ref()
                .is_some_and(|job| job.validate().is_err())
            || metadata.indexes.len() + metadata.jobs.len() > 16
            || metadata
                .indexes
                .iter()
                .any(|index| index.validate().is_err() || !names.insert(index.name.as_str()))
            || metadata
                .jobs
                .iter()
                .any(|job| job.validate(collection).is_err() || !names.insert(job.name()))
        {
            return Err(invalid_data("unsupported or invalid document metadata"));
        }
        Ok(metadata)
    }

    pub(super) fn save_metadata(
        &mut self,
        collection: &str,
        metadata: &Metadata,
    ) -> io::Result<()> {
        let bytes = serde_json::to_vec(metadata).map_err(io::Error::other)?;
        self.write(&key::collection(b'M', collection)?, &bytes)
    }

    /// Creates a collection and schema. Collection names are case-sensitive.
    pub fn create_collection(
        &mut self,
        collection: &str,
        schema: CollectionSchema,
    ) -> io::Result<()> {
        self.mutate(|this| {
            schema.validate_definition()?;
            let key = key::collection(b'M', collection)?;
            if this.read(&key)?.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "collection already exists",
                ));
            }
            if this.read(&key::collection(b'P', collection)?)?.is_some() {
                return Err(invalid_data("collection has an unexpected drop cursor"));
            }
            this.save_metadata(
                collection,
                &Metadata {
                    version: 1,
                    schema,
                    indexes: Vec::new(),
                    jobs: Vec::new(),
                    schema_job: None,
                    dropping: false,
                },
            )
        })
    }

    /// Decodes a stored document as `decode_document` does, returning
    /// borrowed fields when the bytes are a flat document the flat reader
    /// accepts and the schema has only top-level rules. The same size,
    /// syntax and schema checks apply either way, and the same errors.
    pub(crate) fn decode_stored<'b>(
        bytes: &'b [u8],
        schema: &CollectionSchema,
    ) -> io::Result<StoredDocument<'b>> {
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(invalid_data("stored document exceeds size limit"));
        }
        if let Some(flat) = super::flat::parse(bytes) {
            if let Some(valid) = schema.validate_flat(&flat) {
                valid.map_err(|_| invalid_data("stored document violates its schema"))?;
                return Ok(StoredDocument::Flat(flat));
            }
        }
        Self::decode_document(bytes, schema).map(StoredDocument::Full)
    }

    /// Visits one bounded page of a collection's stored documents in ID
    /// order, lending each ID and its undecoded bytes, with the checks of
    /// `scan_stored_documents`. `visit` gets the collection schema to decode
    /// with through `decode_stored` and returns false to end the page early.
    /// It runs while the backend snapshot is borrowed and must not call back
    /// into this transaction; collect what it needs and act after the page.
    /// Returns the number of documents visited and the last one's ID, from
    /// which the next page continues.
    pub(crate) fn visit_stored_documents(
        &self,
        collection: &str,
        after_id: Option<&str>,
        limit: usize,
        mut visit: impl FnMut(&CollectionSchema, &str, &[u8]) -> bool,
    ) -> io::Result<(usize, Option<String>)> {
        if limit == 0 || limit > crate::MAX_SCAN_ENTRIES {
            return Err(key::invalid("scan limit must be between 1 and 1024"));
        }
        let metadata = self.metadata(collection)?;
        let prefix = key::collection(b'D', collection)?;
        let end = key::end(&prefix);
        let after = match after_id {
            Some(id) => key::document(collection, id)?,
            None => prefix.clone(),
        };
        let mut oversized = false;
        let mut error = None;
        let mut count = 0;
        let mut last_id = Vec::new();
        self.visit_admitted(
            &after,
            &end,
            limit,
            |_, len| {
                oversized = len > MAX_DOCUMENT_BYTES;
                !oversized
            },
            |entry_key, value| {
                let Some(id) = entry_key.strip_prefix(prefix.as_slice()) else {
                    error = Some(invalid_data("invalid document key"));
                    return false;
                };
                let Some(id) = std::str::from_utf8(id)
                    .ok()
                    .filter(|id| key::id(id).is_ok())
                else {
                    error = Some(invalid_data("invalid document ID"));
                    return false;
                };
                count += 1;
                last_id.clear();
                last_id.extend_from_slice(id.as_bytes());
                visit(&metadata.schema, id, value)
            },
        )?;
        if oversized {
            return Err(invalid_data("stored document exceeds size limit"));
        }
        if let Some(error) = error {
            return Err(error);
        }
        let last_id = (count > 0).then(|| String::from_utf8(last_id).expect("checked as UTF-8"));
        Ok((count, last_id))
    }

    pub(super) fn decode_document(bytes: &[u8], schema: &CollectionSchema) -> io::Result<Value> {
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(invalid_data("stored document exceeds size limit"));
        }
        let document: Value =
            serde_json::from_slice(bytes).map_err(|_| invalid_data("invalid stored document"))?;
        schema
            .validate_stored_document(&document, bytes.len())
            .map_err(|_| invalid_data("stored document violates its schema"))?;
        Ok(document)
    }

    fn read_document(
        &self,
        collection: &str,
        id: &str,
        schema: &CollectionSchema,
    ) -> io::Result<Option<Value>> {
        self.read_document_bytes(&key::document(collection, id)?, |_| Ok(()))?
            .map(|bytes| Self::decode_document(&bytes, schema))
            .transpose()
    }

    /// Reads a document, distinguishing a missing ID from a missing collection.
    pub fn get(&self, collection: &str, id: &str) -> io::Result<Option<Value>> {
        let metadata = self.metadata(collection)?;
        self.read_document(collection, id, &metadata.schema)
    }

    fn check_old_index(&self, keys: &Keys, id: &str, allow_missing: bool) -> io::Result<bool> {
        let entry = self.has_index_entry(&keys.entry)?;
        let unique = keys
            .unique
            .as_ref()
            .map(|key| self.read_unique_owner(key))
            .transpose()?
            .flatten();
        match entry {
            true => {
                if keys.unique.is_some() && unique.as_deref() != Some(id.as_bytes()) {
                    return Err(invalid_data(
                        "document unique index entry is missing or invalid",
                    ));
                }
                Ok(true)
            }
            false if !allow_missing => Err(invalid_data("document index entry is missing")),
            false if unique.as_deref() == Some(id.as_bytes()) => {
                Err(invalid_data("unique index entry has no document entry"))
            }
            false => Ok(false),
        }
    }

    fn update_indexes(
        &mut self,
        collection: &str,
        id: &str,
        indexes: &[IndexDefinition],
        old: Option<&Value>,
        new: Option<&Value>,
        allow_missing: bool,
    ) -> io::Result<()> {
        let mut changes = Vec::with_capacity(indexes.len());
        for definition in indexes {
            let mut previous = match old
                .map(|value| index::keys(collection, definition, value, id))
                .transpose()
            {
                Ok(value) => value.flatten(),
                Err(error) if allow_missing && error.kind() == io::ErrorKind::InvalidInput => None,
                Err(error) => return Err(error),
            };
            let next = new
                .map(|value| index::keys(collection, definition, value, id))
                .transpose()?
                .flatten();
            if let Some(keys) = &previous {
                if !self.check_old_index(keys, id, allow_missing)? {
                    previous = None;
                }
            }
            if let Some(keys) = &next {
                let same_entry = previous.as_ref().is_some_and(|old| old.entry == keys.entry);
                if !same_entry && self.has_index_entry(&keys.entry)? {
                    return Err(invalid_data("unexpected document index entry"));
                }
                if let Some(unique) = &keys.unique {
                    let same_unique =
                        previous.as_ref().and_then(|old| old.unique.as_ref()) == Some(unique);
                    if !same_unique && self.read_unique_owner(unique)?.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "unique index constraint violated",
                        ));
                    }
                }
            }
            changes.push((previous, next));
        }
        for (old, new) in changes {
            let same_entry = old
                .as_ref()
                .zip(new.as_ref())
                .is_some_and(|(old, new)| old.entry == new.entry);
            let same_unique = old
                .as_ref()
                .and_then(|old| old.unique.as_ref())
                .zip(new.as_ref().and_then(|new| new.unique.as_ref()))
                .is_some_and(|(old, new)| old == new);
            if let Some(old) = old {
                if !same_entry {
                    self.remove(&old.entry)?;
                }
                if !same_unique {
                    if let Some(unique) = old.unique {
                        self.remove(&unique)?;
                    }
                }
            }
            if let Some(new) = new {
                if !same_entry {
                    self.write(&new.entry, &[])?;
                }
                if !same_unique {
                    if let Some(unique) = new.unique {
                        self.write(&unique, id.as_bytes())?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn set(
        &mut self,
        collection: &str,
        id: &str,
        document: &Value,
        insert_only: bool,
        upsert: bool,
    ) -> io::Result<()> {
        let metadata = self.metadata(collection)?;
        let key = key::document(collection, id)?;
        let existing = self.read_document(collection, id, &metadata.schema)?;
        if insert_only && existing.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "document already exists",
            ));
        }
        if !insert_only && !upsert && existing.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "document does not exist",
            ));
        }
        metadata.schema.validate_document(document)?;
        if let Some(job) = &metadata.schema_job {
            job.schema.validate_document(document)?;
        }
        let bytes = serde_json::to_vec(document).map_err(io::Error::other)?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(key::invalid("document exceeds 1 MiB"));
        }
        self.update_indexes(
            collection,
            id,
            &metadata.indexes,
            existing.as_ref(),
            Some(document),
            false,
        )?;
        let building = metadata
            .jobs
            .iter()
            .filter_map(IndexJob::building_definition)
            .cloned()
            .collect::<Vec<_>>();
        self.update_indexes(
            collection,
            id,
            &building,
            existing.as_ref(),
            Some(document),
            true,
        )?;
        self.write(&key, &bytes)
    }

    /// Inserts only if the ID is absent.
    pub fn insert(&mut self, collection: &str, id: &str, document: &Value) -> io::Result<()> {
        self.mutate(|this| this.set(collection, id, document, true, false))
    }

    /// Replaces a document, optionally inserting if it does not exist.
    pub fn replace(
        &mut self,
        collection: &str,
        id: &str,
        document: &Value,
        upsert: bool,
    ) -> io::Result<()> {
        self.mutate(|this| this.set(collection, id, document, false, upsert))
    }

    /// Deletes a document, returning false when it was absent.
    pub fn delete(&mut self, collection: &str, id: &str) -> io::Result<bool> {
        self.mutate(|this| {
            let metadata = this.metadata(collection)?;
            let key = key::document(collection, id)?;
            let Some(existing) = this.read_document(collection, id, &metadata.schema)? else {
                return Ok(false);
            };
            this.update_indexes(
                collection,
                id,
                &metadata.indexes,
                Some(&existing),
                None,
                false,
            )?;
            let building = metadata
                .jobs
                .iter()
                .filter_map(IndexJob::building_definition)
                .cloned()
                .collect::<Vec<_>>();
            this.update_indexes(collection, id, &building, Some(&existing), None, true)?;
            this.remove(&key)?;
            Ok(true)
        })
    }

    /// Counts a collection's documents without reading them. Returns at
    /// most `max + 1`; a result above `max` means the count stopped there.
    ///
    /// The count records the collection's document range in the read set, as
    /// a full scan would, so a concurrent insert or delete still conflicts.
    /// On an ordinary `Store` it reads only the in-memory snapshot index; on
    /// a `CheckpointStore` it visits the verified checkpoint blocks and
    /// suffix overlay. No stored document is read, so no value checksum or
    /// schema is checked, because nothing stored is returned. Other backends
    /// page through the
    /// documents, reading and checksum-verifying each stored value without
    /// decoding it. Logical read limits are
    /// charged per counted key.
    pub fn count_documents(&self, collection: &str, max: usize) -> io::Result<usize> {
        self.metadata(collection)?;
        let prefix = key::collection(b'D', collection)?;
        let end = key::end(&prefix);
        if let BackendTransaction::Ordinary(tx) = &self.backend {
            self.charge_read(1, 0)?;
            let mut budget_error = None;
            let count = tx.count_keys(Some(&prefix), Some(&end), max, |key| {
                budget_error = self.charge_read(1, key.len()).err();
                budget_error.is_none()
            })?;
            return budget_error.map_or(Ok(count), Err);
        }
        if let BackendTransaction::Checkpoint(tx) = &self.backend {
            self.charge_read(1, 0)?;
            let mut budget_error = None;
            let count = tx.count_keys(Some(&prefix), Some(&end), max, |key| {
                budget_error = self.charge_read(1, key.len()).err();
                budget_error.is_none()
            })?;
            return budget_error.map_or(Ok(count), Err);
        }
        let mut after = prefix;
        let mut count = 0_usize;
        loop {
            let page = self.scan_document_page(&after, &end, crate::MAX_SCAN_ENTRIES)?;
            let Some((last, _)) = page.last() else {
                return Ok(count);
            };
            after = last.clone();
            count += page.len();
            if count > max {
                return Ok(max + 1);
            }
        }
    }

    /// Scans one bounded page of documents in ascending ID byte order.
    ///
    /// Pass `after_id: None` for the first page, then
    /// `after_id: page.last().map(|entry| entry.id.as_str())` until an empty
    /// vector is returned. `limit` must be between `1` and `1024`
    /// (`MAX_SCAN_ENTRIES`), and each page is also capped at 32 MiB of key and
    /// value bytes. Reads collection metadata first so missing or dropping
    /// collections fail consistently even when empty.
    pub fn scan_documents(
        &self,
        collection: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> io::Result<Vec<super::DocumentEntry>> {
        if limit == 0 || limit > crate::MAX_SCAN_ENTRIES {
            return Err(key::invalid("scan limit must be between 1 and 1024"));
        }
        let metadata = self.metadata(collection)?;
        let prefix = key::collection(b'D', collection)?;
        let end = key::end(&prefix);
        let after = match after_id {
            Some(id) => key::document(collection, id)?,
            None => prefix.clone(),
        };
        let page = self.scan_document_page(&after, &end, limit)?;
        let mut documents = Vec::with_capacity(page.len());
        for (entry_key, value) in page {
            let id = std::str::from_utf8(
                entry_key
                    .strip_prefix(prefix.as_slice())
                    .ok_or_else(|| invalid_data("invalid document key"))?,
            )
            .map_err(|_| invalid_data("invalid document ID"))?;
            key::id(id).map_err(|_| invalid_data("invalid document ID"))?;
            let document = Self::decode_document(&value, &metadata.schema)?;
            documents.push(super::DocumentEntry {
                id: id.to_owned(),
                document,
            });
        }
        Ok(documents)
    }

    pub(super) fn document_rows(
        &self,
        collection: &str,
        schema: &CollectionSchema,
        max: usize,
        max_bytes: usize,
    ) -> io::Result<Vec<(String, Value)>> {
        let mut rows = Vec::new();
        self.visit_document_rows(collection, schema, max, max_bytes, |id, document| {
            rows.push((id, document));
        })?;
        Ok(rows)
    }

    // Visit every document in the same order and with the same scan and byte
    // limits as document_rows, without retaining all decoded documents.
    pub(super) fn visit_document_rows(
        &self,
        collection: &str,
        schema: &CollectionSchema,
        max: usize,
        max_bytes: usize,
        mut visit: impl FnMut(String, Value),
    ) -> io::Result<()> {
        let prefix = key::collection(b'D', collection)?;
        let end = key::end(&prefix);
        let mut cursor = prefix.clone();
        let mut count = 0_usize;
        let mut bytes = 0_usize;
        loop {
            let mut rejected = None;
            let page = self.scan_admitted(&cursor, &end, 32, |key, len| {
                let total = bytes + key.len() + len;
                if count >= max || total > max_bytes {
                    rejected = Some(key::invalid("document scan exceeds the operation limit"));
                    return false;
                }
                if len > MAX_DOCUMENT_BYTES {
                    rejected = Some(invalid_data("stored document exceeds size limit"));
                    return false;
                }
                count += 1;
                bytes = total;
                true
            })?;
            let empty = page.is_empty();
            for (key, value) in page {
                let id = std::str::from_utf8(
                    key.strip_prefix(prefix.as_slice())
                        .ok_or_else(|| invalid_data("invalid document key"))?,
                )
                .map_err(|_| invalid_data("invalid document ID"))?;
                key::id(id).map_err(|_| invalid_data("invalid document ID"))?;
                visit(id.to_owned(), Self::decode_document(&value, schema)?);
                cursor = key;
            }
            if let Some(error) = rejected {
                return Err(error);
            }
            if empty {
                break;
            }
        }
        Ok(())
    }

    /// Builds a complete index atomically, subject to the documented small
    /// collection and transaction limits. Existing documents are validated.
    pub fn create_index(
        &mut self,
        collection: &str,
        definition: IndexDefinition,
    ) -> io::Result<()> {
        self.mutate(|this| {
            definition.validate()?;
            let mut metadata = (*this.metadata(collection)?).clone();
            if metadata
                .indexes
                .iter()
                .any(|item| item.name == definition.name)
                || metadata
                    .jobs
                    .iter()
                    .any(|job| job.name() == definition.name)
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "index already exists",
                ));
            }
            if metadata.indexes.len() + metadata.jobs.len() >= 16 {
                return Err(key::invalid("collection exceeds 16 indexes"));
            }
            for kind in *b"IU" {
                let prefix = key::index(kind, collection, &definition.name)?;
                if !this.scan(&prefix, &key::end(&prefix), 1)?.is_empty() {
                    return Err(invalid_data("unexpected pre-existing index keys"));
                }
            }
            let documents = this.document_rows(
                collection,
                &metadata.schema,
                MAX_INDEX_BUILD_DOCUMENTS,
                MAX_INDEX_BUILD_BYTES,
            )?;
            let mut entries = Vec::new();
            let mut unique = BTreeSet::new();
            for (id, document) in documents {
                if let Some(keys) = index::keys(collection, &definition, &document, &id)? {
                    if let Some(key) = &keys.unique {
                        if !unique.insert(key.clone()) {
                            return Err(io::Error::new(
                                io::ErrorKind::AlreadyExists,
                                "unique index constraint violated",
                            ));
                        }
                    }
                    entries.push((id, keys));
                }
            }
            for (id, keys) in entries {
                this.write(&keys.entry, &[])?;
                if let Some(unique) = keys.unique {
                    this.write(&unique, id.as_bytes())?;
                }
            }
            metadata.indexes.push(definition);
            this.save_metadata(collection, &metadata)
        })
    }

    /// Removes an index and all its keys in one transaction, subject to the
    /// documented limit on the number of index keys removed at once.
    pub fn drop_index(&mut self, collection: &str, name: &str) -> io::Result<()> {
        self.mutate(|this| {
            let mut metadata = (*this.metadata(collection)?).clone();
            let position = metadata
                .indexes
                .iter()
                .position(|index| index.name == name)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index does not exist"))?;
            let mut keys = Vec::new();
            let mut bytes = 0_usize;
            for kind in *b"IU" {
                let prefix = key::index(kind, collection, name)?;
                let end = key::end(&prefix);
                let mut cursor = prefix;
                loop {
                    let page = this.scan(&cursor, &end, 128)?;
                    if page.is_empty() {
                        break;
                    }
                    for (key, value) in page {
                        bytes = bytes
                            .checked_add(key.len())
                            .and_then(|n| n.checked_add(value.len()))
                            .ok_or_else(|| key::invalid("index drop byte count overflow"))?;
                        if keys.len() >= MAX_INDEX_DROP_KEYS || bytes > MAX_INDEX_DROP_BYTES {
                            return Err(key::invalid("index drop exceeds key or byte limits"));
                        }
                        cursor = key.clone();
                        keys.push(key);
                    }
                }
            }
            for key in keys {
                this.remove(&key)?;
            }
            metadata.indexes.remove(position);
            this.save_metadata(collection, &metadata)
        })
    }

    /// Lists index definitions stored with the collection.
    pub fn indexes(&self, collection: &str) -> io::Result<Vec<IndexDefinition>> {
        Ok(self.metadata(collection)?.indexes.clone())
    }

    /// Loads and validates metadata for each listed collection once, coalescing
    /// nearby metadata reads through a shared point reader. Repeated collection
    /// names share the same metadata read and handle.
    pub(crate) fn load_stored_metadata_batch(
        &self,
        collections: &[&str],
    ) -> io::Result<Vec<Arc<dyn std::any::Any + Send + Sync>>> {
        const INLINE: usize = 8;
        if collections.len() <= INLINE {
            let mut unique_names = [""; INLINE];
            let mut unique_len = 0_usize;
            let mut mapping = [0_usize; INLINE];
            for (idx, &name) in collections.iter().enumerate() {
                key::name(name)?;
                if let Some(pos) = unique_names[..unique_len]
                    .iter()
                    .position(|&existing| existing == name)
                {
                    mapping[idx] = pos;
                } else {
                    mapping[idx] = unique_len;
                    unique_names[unique_len] = name;
                    unique_len += 1;
                }
            }
            let mut loaded: [Option<Arc<dyn std::any::Any + Send + Sync>>; INLINE] =
                [const { None }; INLINE];
            match unique_len {
                0 => {}
                1 => {
                    loaded[0] = Some(self.metadata(unique_names[0])?);
                }
                _ => {
                    let mut key_bufs = [[0u8; 160]; INLINE];
                    let mut key_lens = [0_usize; INLINE];
                    for i in 0..unique_len {
                        key_lens[i] =
                            write_collection_key(&mut key_bufs[i], b'M', unique_names[i])?;
                    }
                    let mut raw_entries: [Option<Vec<u8>>; INLINE] = [const { None }; INLINE];
                    self.with_point_reader(|reader| {
                        for i in 0..unique_len {
                            let bytes = reader.read_admitted_with_peek(
                                &key_bufs[i][..key_lens[i]],
                                |_| Ok(()),
                                |push| {
                                    for j in (i + 1)..unique_len {
                                        if !push(&key_bufs[j][..key_lens[j]]) {
                                            break;
                                        }
                                    }
                                },
                            )?;
                            raw_entries[i] = bytes;
                        }
                        Ok(())
                    })?;
                    for i in 0..unique_len {
                        let meta = Self::decode_metadata(unique_names[i], raw_entries[i].take())?;
                        loaded[i] = Some(meta);
                    }
                }
            }
            let mut out = Vec::with_capacity(collections.len());
            for &m in &mapping[..collections.len()] {
                out.push(Arc::clone(loaded[m].as_ref().expect("loaded metadata")));
            }
            return Ok(out);
        }

        let mut unique_names: Vec<&str> = Vec::with_capacity(collections.len());
        let mut mapping: Vec<usize> = Vec::with_capacity(collections.len());
        for &name in collections {
            key::name(name)?;
            if let Some(pos) = unique_names.iter().position(|&existing| existing == name) {
                mapping.push(pos);
            } else {
                mapping.push(unique_names.len());
                unique_names.push(name);
            }
        }
        let mut loaded: Vec<Arc<dyn std::any::Any + Send + Sync>> =
            Vec::with_capacity(unique_names.len());
        match unique_names.len() {
            0 => {}
            1 => {
                let meta = self.metadata(unique_names[0])?;
                loaded.push(meta);
            }
            _ => {
                let mut key_bufs: Vec<([u8; 160], usize)> = Vec::with_capacity(unique_names.len());
                for &name in &unique_names {
                    let mut buf = [0u8; 160];
                    let len = write_collection_key(&mut buf, b'M', name)?;
                    key_bufs.push((buf, len));
                }
                let mut raw_entries: Vec<Option<Vec<u8>>> = Vec::with_capacity(unique_names.len());
                self.with_point_reader(|reader| {
                    for i in 0..key_bufs.len() {
                        let (ref buf, len) = key_bufs[i];
                        let bytes = reader.read_admitted_with_peek(
                            &buf[..len],
                            |_| Ok(()),
                            |push| {
                                for (next_buf, next_len) in &key_bufs[i + 1..] {
                                    if !push(&next_buf[..*next_len]) {
                                        break;
                                    }
                                }
                            },
                        )?;
                        raw_entries.push(bytes);
                    }
                    Ok(())
                })?;
                for (name, bytes) in unique_names.into_iter().zip(raw_entries) {
                    let meta = Self::decode_metadata(name, bytes)?;
                    loaded.push(meta);
                }
            }
        }
        Ok(mapping
            .into_iter()
            .map(|i| Arc::clone(&loaded[i]))
            .collect())
    }

    /// Reads one stored document using previously loaded collection metadata
    /// and lends its decoded form (flat when possible) to `visit`. Returns
    /// whether the document existed.
    pub(crate) fn read_stored_document_with_meta(
        &self,
        collection: &str,
        meta: &dyn std::any::Any,
        id: &str,
        visit: impl FnOnce(StoredDocument<'_>),
    ) -> io::Result<bool> {
        let metadata = meta
            .downcast_ref::<Metadata>()
            .ok_or_else(|| key::invalid("invalid collection metadata handle"))?;
        let mut key_buf = [0u8; 416];
        let key_len = write_document_key(&mut key_buf, collection, id)?;
        let Some(bytes) = self.read_document_bytes(&key_buf[..key_len], |_| Ok(()))? else {
            return Ok(false);
        };
        let stored = Self::decode_stored(&bytes, &metadata.schema)?;
        visit(stored);
        Ok(true)
    }

    /// Reads a batch of stored documents by ID in one point-reader session,
    /// coalescing nearby log reads and lending each decoded document to
    /// `visit`. `visit` returns false to stop early and must not call back
    /// into this transaction.
    pub(crate) fn read_stored_documents_batch_with_meta(
        &self,
        collection: &str,
        meta: &dyn std::any::Any,
        ids: &[&str],
        mut visit: impl FnMut(usize, &str, Option<StoredDocument<'_>>) -> bool,
    ) -> io::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let metadata = meta
            .downcast_ref::<Metadata>()
            .ok_or_else(|| key::invalid("invalid collection metadata handle"))?;
        if ids.len() == 1 {
            let id = ids[0];
            let mut key_buf = [0u8; 416];
            let key_len = write_document_key(&mut key_buf, collection, id)?;
            let bytes = self.read_document_bytes(&key_buf[..key_len], |_| Ok(()))?;
            let stored = bytes
                .as_deref()
                .map(|b| Self::decode_stored(b, &metadata.schema))
                .transpose()?;
            visit(0, id, stored);
            return Ok(());
        }
        let mut prefix_buf = [0u8; 160];
        let prefix_len = write_collection_key(&mut prefix_buf, b'D', collection)?;
        let prefix = &prefix_buf[..prefix_len];
        self.with_point_reader(|reader| {
            let mut key_buf = [0u8; 416];
            let mut peek_buf = [0u8; 416];
            key_buf[..prefix_len].copy_from_slice(prefix);
            peek_buf[..prefix_len].copy_from_slice(prefix);
            let mut consecutive_misses: usize = 0;
            for (index, &id) in ids.iter().enumerate() {
                key::id(id)?;
                let key_len = prefix_len + id.len();
                key_buf[prefix_len..key_len].copy_from_slice(id.as_bytes());
                let mut peeked = false;
                let bytes = reader.read_document_bytes_with_peek(
                    &key_buf[..key_len],
                    |_| Ok(()),
                    |push_key| {
                        peeked = true;
                        if consecutive_misses >= 2 && (index & 31) != 0 {
                            return;
                        }
                        let end = ids.len().min(index + 1 + 64);
                        for &next_id in &ids[index + 1..end] {
                            if key::id(next_id).is_err() {
                                break;
                            }
                            let next_len = prefix_len + next_id.len();
                            peek_buf[prefix_len..next_len].copy_from_slice(next_id.as_bytes());
                            if !push_key(&peek_buf[..next_len]) {
                                break;
                            }
                        }
                    },
                )?;
                if peeked {
                    consecutive_misses += 1;
                } else {
                    consecutive_misses = 0;
                }
                let stored = bytes
                    .as_deref()
                    .map(|b| Self::decode_stored(b, &metadata.schema))
                    .transpose()?;
                if !visit(index, id, stored) {
                    break;
                }
            }
            Ok(())
        })
    }

    /// Visits one bounded page of stored documents using previously loaded
    /// collection metadata, lending each ID and decoded `StoredDocument`.
    /// `visit` runs while the backend snapshot is borrowed and must not call
    /// back into this transaction.
    pub(crate) fn visit_stored_documents_with_meta(
        &self,
        collection: &str,
        meta: &dyn std::any::Any,
        after_id: Option<&str>,
        limit: usize,
        mut visit: impl FnMut(&str, StoredDocument<'_>) -> bool,
    ) -> io::Result<(usize, Option<String>)> {
        if limit == 0 || limit > crate::MAX_SCAN_ENTRIES {
            return Err(key::invalid("scan limit must be between 1 and 1024"));
        }
        let metadata = meta
            .downcast_ref::<Metadata>()
            .ok_or_else(|| key::invalid("invalid collection metadata handle"))?;
        let mut prefix_buf = [0u8; 160];
        let prefix_len = write_collection_key(&mut prefix_buf, b'D', collection)?;
        let prefix = &prefix_buf[..prefix_len];
        let mut end_buf = [0u8; 160];
        let end_len = write_prefix_end(&mut end_buf, prefix);
        let mut after_buf = [0u8; 416];
        let after_len = match after_id {
            Some(id) => {
                key::id(id)?;
                after_buf[..prefix_len].copy_from_slice(prefix);
                after_buf[prefix_len..prefix_len + id.len()].copy_from_slice(id.as_bytes());
                prefix_len + id.len()
            }
            None => {
                after_buf[..prefix_len].copy_from_slice(prefix);
                prefix_len
            }
        };
        let mut oversized = false;
        let mut error = None;
        let mut count = 0;
        let mut last_id = [0u8; key::MAX_ID_BYTES];
        let mut last_id_len = 0;
        self.visit_admitted(
            &after_buf[..after_len],
            &end_buf[..end_len],
            limit,
            |_, len| {
                oversized = len > MAX_DOCUMENT_BYTES;
                !oversized
            },
            |entry_key, value| {
                let Some(id_bytes) = entry_key.strip_prefix(prefix) else {
                    error = Some(invalid_data("invalid document key"));
                    return false;
                };
                let Some(id) = std::str::from_utf8(id_bytes)
                    .ok()
                    .filter(|id| key::id(id).is_ok())
                else {
                    error = Some(invalid_data("invalid document ID"));
                    return false;
                };
                count += 1;
                last_id_len = id.len();
                last_id[..last_id_len].copy_from_slice(id.as_bytes());
                match Self::decode_stored(value, &metadata.schema) {
                    Ok(stored) => visit(id, stored),
                    Err(err) => {
                        error = Some(err);
                        false
                    }
                }
            },
        )?;
        if oversized {
            return Err(invalid_data("stored document exceeds size limit"));
        }
        if let Some(error) = error {
            return Err(error);
        }
        let last_id = (count > 0)
            .then(|| String::from_utf8(last_id[..last_id_len].to_vec()).expect("checked as UTF-8"));
        Ok((count, last_id))
    }

    /// Reads one bounded page of a leading-column equality lookup on a ready
    /// secondary index using previously loaded collection metadata, lending
    /// each ID and decoded `StoredDocument` without building JSON values or
    /// cursor scope digests. Applies the same index consistency, unique-owner,
    /// response-size, and schema checks as `query_index`.
    pub(crate) fn visit_index_stored_eq_with_meta(
        &self,
        (collection, meta): (&str, &dyn std::any::Any),
        index_name: &str,
        equal: &index::IndexValue,
        after: Option<&[u8]>,
        limit: usize,
        mut visit: impl FnMut(&str, StoredDocument<'_>) -> bool,
    ) -> io::Result<(usize, Option<Vec<u8>>)> {
        const MAX_INDEX_PAGE: usize = 256;
        const MAX_INDEX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
        if limit == 0 || limit > MAX_INDEX_PAGE {
            return Err(key::invalid("query limit must be between 1 and 256"));
        }
        let metadata = meta
            .downcast_ref::<Metadata>()
            .ok_or_else(|| key::invalid("invalid collection metadata handle"))?;
        let definition = metadata
            .indexes
            .iter()
            .find(|index| index.name == index_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index does not exist"))?;
        if definition.fields.is_empty() {
            return Err(key::invalid("query bounds exceed the index field count"));
        }
        let mut index_prefix_buf = [0u8; 300];
        let index_prefix_len =
            write_index_key(&mut index_prefix_buf, b'I', collection, &definition.name)?;
        let index_prefix = &index_prefix_buf[..index_prefix_len];
        // A single-value encoded index prefix is at most 276 + 515 = 791 bytes,
        // so it fits on the stack without allocating.
        let mut equal_prefix_buf = [0u8; 832];
        let equal_prefix_len =
            write_index_equal_prefix(&mut equal_prefix_buf, index_prefix, equal)?;
        let equal_prefix = &equal_prefix_buf[..equal_prefix_len];
        let mut before_buf = [0u8; 1024];
        let mut before_len = write_prefix_end_slice(&mut before_buf, equal_prefix);
        let after_slice = if let Some(cursor) = after {
            if !cursor.starts_with(equal_prefix)
                || cursor >= &before_buf[..before_len]
                || cursor < equal_prefix
            {
                return Err(key::invalid("cursor is outside the query bounds"));
            }
            cursor
        } else {
            equal_prefix
        };
        let after_slice = &after_slice[..after_slice.len().min(crate::record::MAX_KEY)];
        if before_len > crate::record::MAX_KEY {
            let mut tmp = [0u8; 1024];
            tmp[..crate::record::MAX_KEY].copy_from_slice(&before_buf[..crate::record::MAX_KEY]);
            before_len = write_prefix_end_slice(&mut before_buf, &tmp[..crate::record::MAX_KEY]);
        }
        let before = &before_buf[..before_len];
        if after_slice >= before {
            return Ok((0, None));
        }

        let mut nonempty_entry = false;
        let mut entry_bytes: Vec<u8> = Vec::new();
        let mut entry_ranges: Vec<(usize, usize)> = Vec::new();
        self.visit_admitted(
            after_slice,
            before,
            limit,
            |_, len| {
                nonempty_entry = len != 0;
                !nonempty_entry
            },
            |entry_key, _| {
                let start = entry_bytes.len();
                entry_bytes.extend_from_slice(entry_key);
                entry_ranges.push((start, entry_bytes.len()));
                true
            },
        )?;
        let entries_read = entry_ranges.len();
        if entries_read == 0 {
            if nonempty_entry {
                return Err(invalid_data("invalid index entry value"));
            }
            return Ok((0, None));
        }

        let single_field_fast =
            definition.fields.len() == 1 && equal_prefix.len() <= crate::record::MAX_KEY;
        let flat_verify_ok =
            definition.partial.is_none() && definition.fields.iter().all(|path| path.len() == 1);
        let mut doc_prefix_buf = [0u8; 160];
        let doc_prefix_len = write_collection_key(&mut doc_prefix_buf, b'D', collection)?;
        let doc_prefix = &doc_prefix_buf[..doc_prefix_len];
        let mut unique_prefix_buf = [0u8; 300];
        let unique_prefix_len = if definition.unique {
            write_index_key(&mut unique_prefix_buf, b'U', collection, &definition.name)?
        } else {
            0
        };

        let mut returned = 0_usize;
        let mut used = 0_usize;
        let mut cursor_index: Option<usize> = None;
        let mut stopped_by_caller = false;
        let mut stopped_by_response_limit = false;

        self.with_point_reader(|reader| {
            let mut key_buf = [0u8; 416];
            let mut peek_buf = [0u8; 416];
            key_buf[..doc_prefix_len].copy_from_slice(doc_prefix);
            peek_buf[..doc_prefix_len].copy_from_slice(doc_prefix);
            let mut unique_buf = [0u8; 1024];
            let mut consecutive_misses: usize = 0;

            for (index, &(start, end)) in entry_ranges.iter().enumerate() {
                let entry = &entry_bytes[start..end];
                let id = extract_index_entry_id(
                    entry,
                    index_prefix,
                    equal_prefix,
                    definition.fields.len(),
                    single_field_fast,
                )?;
                let key_len = doc_prefix_len + id.len();
                key_buf[doc_prefix_len..key_len].copy_from_slice(id.as_bytes());
                let mut peeked = false;
                let encoded = reader
                    .read_document_bytes_with_peek(
                        &key_buf[..key_len],
                        |_| Ok(()),
                        |push_key| {
                            peeked = true;
                            if consecutive_misses >= 2 && (index & 31) != 0 {
                                return;
                            }
                            let peek_end = entry_ranges.len().min(index + 1 + 64);
                            for &(ns, ne) in &entry_ranges[index + 1..peek_end] {
                                let Ok(next_id) = extract_index_entry_id(
                                    &entry_bytes[ns..ne],
                                    index_prefix,
                                    equal_prefix,
                                    definition.fields.len(),
                                    single_field_fast,
                                ) else {
                                    break;
                                };
                                let next_len = doc_prefix_len + next_id.len();
                                peek_buf[doc_prefix_len..next_len]
                                    .copy_from_slice(next_id.as_bytes());
                                if !push_key(&peek_buf[..next_len]) {
                                    break;
                                }
                            }
                        },
                    )?
                    .ok_or_else(|| invalid_data("index references a missing document"))?;
                if peeked {
                    consecutive_misses += 1;
                } else {
                    consecutive_misses = 0;
                }
                let next_used = used
                    .checked_add(id.len())
                    .and_then(|n| n.checked_add(encoded.len()))
                    .ok_or_else(|| key::invalid("query response size overflow"))?;
                if next_used > MAX_INDEX_RESPONSE_BYTES {
                    stopped_by_response_limit = true;
                    break;
                }
                let stored = Self::decode_stored(&encoded, &metadata.schema)?;
                verify_stored_index_entry(
                    collection,
                    definition,
                    index_prefix,
                    &unique_prefix_buf[..unique_prefix_len],
                    flat_verify_ok,
                    id,
                    entry,
                    &stored,
                    reader,
                    &mut unique_buf,
                )?;
                used = next_used;
                returned += 1;
                cursor_index = Some(index);
                if !visit(id, stored) {
                    stopped_by_caller = true;
                    break;
                }
            }
            Ok(())
        })?;

        if nonempty_entry && returned == entries_read {
            return Err(invalid_data("invalid index entry value"));
        }
        let next = if !stopped_by_caller && (entries_read == limit || stopped_by_response_limit) {
            cursor_index.map(|i| {
                let (s, e) = entry_ranges[i];
                entry_bytes[s..e].to_vec()
            })
        } else {
            None
        };
        Ok((returned, next))
    }
}

enum PointReaderBackend<'r, 's> {
    Ordinary(&'r mut crate::store::OrdinaryPointSession<'s>),
    Checkpoint(&'r mut crate::checkpoint_store::CheckpointPointSession<'s>),
    OrdinarySnapshot(&'r mut crate::store::SnapshotPointSession<'s>),
    CheckpointSnapshot(&'r mut crate::checkpoint_store::CheckpointSnapshotPointSession<'s>),
    HistoricalCheckpoint(&'r mut crate::checkpoint_view::CheckpointViewPointSession<'s>),
    Direct,
}

pub(super) struct PointReader<'r, 's, 'a, 'store> {
    tx: &'r DocumentTransaction<'a, 'store>,
    backend: PointReaderBackend<'r, 's>,
}

impl PointReader<'_, '_, '_, '_> {
    fn read_admitted_with_peek(
        &mut self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
        peek_keys: impl FnOnce(&mut dyn FnMut(&[u8]) -> bool),
    ) -> io::Result<Option<Vec<u8>>> {
        self.tx.charge_read(1, key.len())?;
        let admit = |bytes| {
            admit(bytes)?;
            self.tx.charge_read(0, bytes)
        };
        match &mut self.backend {
            PointReaderBackend::Ordinary(session) => {
                session.get_admitted_with_peek(key, admit, peek_keys)
            }
            PointReaderBackend::Checkpoint(session) => {
                session.get_admitted_with_peek(key, admit, peek_keys)
            }
            PointReaderBackend::OrdinarySnapshot(session) => {
                session.get_admitted_with_peek(key, admit, peek_keys)
            }
            PointReaderBackend::CheckpointSnapshot(session) => {
                session.get_admitted_with_peek(key, admit, peek_keys)
            }
            PointReaderBackend::HistoricalCheckpoint(session) => {
                session.get_admitted_with_peek(key, admit, peek_keys)
            }
            PointReaderBackend::Direct => match &self.tx.backend {
                BackendTransaction::Retained(tx) => tx.get_admitted(key, admit),
                BackendTransaction::RetainedSnapshot(view) => view.get_admitted(key, admit),
                _ => unreachable!("direct point reader is only used for retained backends"),
            },
        }
    }

    #[allow(dead_code)]
    pub(super) fn read_document_bytes_with_peek(
        &mut self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
        peek_keys: impl FnOnce(&mut dyn FnMut(&[u8]) -> bool),
    ) -> io::Result<Option<Vec<u8>>> {
        self.read_admitted_with_peek(
            key,
            |len| {
                if len > MAX_DOCUMENT_BYTES {
                    return Err(invalid_data("stored document exceeds size limit"));
                }
                admit(len)
            },
            peek_keys,
        )
    }

    pub(super) fn read_unique_owner(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let owner = self.read_admitted_with_peek(
            key,
            |len| {
                if !(1..=key::MAX_ID_BYTES).contains(&len) {
                    return Err(invalid_data("invalid unique index owner length"));
                }
                Ok(())
            },
            |_| {},
        )?;
        if owner
            .as_deref()
            .is_some_and(|bytes| std::str::from_utf8(bytes).is_err())
        {
            return Err(invalid_data("invalid unique index owner"));
        }
        Ok(owner)
    }
}

impl<'r, 's, 'a, 'store> PointReader<'r, 's, 'a, 'store> {
    /// Reads document bytes as `read_document_bytes_with_peek` does, lending
    /// the verified slice without copying on ordinary and checkpoint
    /// transactions. Passes a disjoint unique-owner verifier so unique index
    /// checks can run before returning while the document slice is borrowed.
    pub(super) fn visit_document_bytes_with_peek<R>(
        &mut self,
        key: &[u8],
        has_upcoming: bool,
        mut admit: impl FnMut(usize) -> io::Result<()>,
        peek_keys: impl FnOnce(&mut dyn FnMut(&[u8]) -> bool),
        visit: impl FnOnce(&[u8], &mut UniqueOwnerVerifier<'_, 'r, 's, 'a, 'store>) -> io::Result<R>,
    ) -> io::Result<Option<R>> {
        let tx = self.tx;
        tx.charge_read(1, key.len())?;
        let admit = |len| {
            if len > MAX_DOCUMENT_BYTES {
                return Err(invalid_data("stored document exceeds size limit"));
            }
            admit(len)?;
            tx.charge_read(0, len)
        };
        match &mut self.backend {
            PointReaderBackend::Ordinary(session) => session.visit_admitted_with_peek(
                key,
                has_upcoming,
                admit,
                peek_keys,
                |bytes, sub| {
                    let mut verifier = UniqueOwnerVerifier {
                        tx,
                        backend: UniqueVerifierBackend::SubRead(sub),
                    };
                    visit(bytes, &mut verifier)
                },
            ),
            PointReaderBackend::Checkpoint(session) => session.visit_admitted_with_peek(
                key,
                has_upcoming,
                admit,
                peek_keys,
                |bytes, sub| {
                    let mut verifier = UniqueOwnerVerifier {
                        tx,
                        backend: UniqueVerifierBackend::SubRead(sub),
                    };
                    visit(bytes, &mut verifier)
                },
            ),
            _ => {
                let bytes = match &mut self.backend {
                    PointReaderBackend::OrdinarySnapshot(session) => {
                        session.get_admitted_with_peek(key, admit, peek_keys)
                    }
                    PointReaderBackend::CheckpointSnapshot(session) => {
                        session.get_admitted_with_peek(key, admit, peek_keys)
                    }
                    PointReaderBackend::HistoricalCheckpoint(session) => {
                        session.get_admitted_with_peek(key, admit, peek_keys)
                    }
                    PointReaderBackend::Direct => match &tx.backend {
                        BackendTransaction::Retained(tx) => tx.get_admitted(key, admit),
                        BackendTransaction::RetainedSnapshot(view) => view.get_admitted(key, admit),
                        _ => unreachable!("direct point reader is only used for retained backends"),
                    },
                    PointReaderBackend::Ordinary(_) | PointReaderBackend::Checkpoint(_) => {
                        unreachable!("handled above")
                    }
                }?;
                match bytes {
                    Some(bytes) => {
                        let mut verifier = UniqueOwnerVerifier {
                            tx,
                            backend: UniqueVerifierBackend::Fallback(self),
                        };
                        visit(&bytes, &mut verifier).map(Some)
                    }
                    None => Ok(None),
                }
            }
        }
    }
}

#[allow(clippy::type_complexity)]
enum UniqueVerifierBackend<'u, 'r, 's, 'a, 'store> {
    SubRead(
        &'u mut dyn FnMut(
            &[u8],
            bool,
            &mut dyn FnMut(usize) -> io::Result<()>,
            &mut dyn FnMut(&mut dyn FnMut(&[u8]) -> bool),
            &mut dyn FnMut(&[u8]) -> io::Result<()>,
        ) -> io::Result<bool>,
    ),
    Fallback(&'u mut PointReader<'r, 's, 'a, 'store>),
}

pub(super) struct UniqueOwnerVerifier<'u, 'r, 's, 'a, 'store> {
    tx: &'u DocumentTransaction<'a, 'store>,
    backend: UniqueVerifierBackend<'u, 'r, 's, 'a, 'store>,
}

impl UniqueOwnerVerifier<'_, '_, '_, '_, '_> {
    pub(super) fn verify_owner(
        &mut self,
        key: &[u8],
        expected_id: &str,
        has_upcoming: bool,
        mut peek_keys: impl FnMut(&mut dyn FnMut(&[u8]) -> bool),
    ) -> io::Result<()> {
        match &mut self.backend {
            UniqueVerifierBackend::SubRead(sub) => {
                self.tx.charge_read(1, key.len())?;
                let mut admit = |len| {
                    if !(1..=key::MAX_ID_BYTES).contains(&len) {
                        return Err(invalid_data("invalid unique index owner length"));
                    }
                    self.tx.charge_read(0, len)
                };
                let mut owner_ok = false;
                let mut check_bytes = |bytes: &[u8]| {
                    if bytes == expected_id.as_bytes() {
                        owner_ok = true;
                        Ok(())
                    } else if std::str::from_utf8(bytes).is_err() {
                        Err(invalid_data("invalid unique index owner"))
                    } else {
                        Err(invalid_data("invalid unique index entry"))
                    }
                };
                let found = sub(
                    key,
                    has_upcoming,
                    &mut admit,
                    &mut peek_keys,
                    &mut check_bytes,
                )?;
                if !found || !owner_ok {
                    return Err(invalid_data("invalid unique index entry"));
                }
                Ok(())
            }
            UniqueVerifierBackend::Fallback(reader) => {
                if reader.read_unique_owner(key)?.as_deref() != Some(expected_id.as_bytes()) {
                    return Err(invalid_data("invalid unique index entry"));
                }
                Ok(())
            }
        }
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn write_collection_key(buf: &mut [u8; 160], kind: u8, name: &str) -> io::Result<usize> {
    key::name(name)?;
    let p = key::PREFIX.len();
    buf[..p].copy_from_slice(key::PREFIX);
    buf[p] = kind;
    buf[p + 1..p + 3].copy_from_slice(&(name.len() as u16).to_be_bytes());
    let start = p + 3;
    let end = start + name.len();
    buf[start..end].copy_from_slice(name.as_bytes());
    Ok(end)
}

fn write_document_key(buf: &mut [u8; 416], collection: &str, id: &str) -> io::Result<usize> {
    key::id(id)?;
    key::name(collection)?;
    let p = key::PREFIX.len();
    buf[..p].copy_from_slice(key::PREFIX);
    buf[p] = b'D';
    buf[p + 1..p + 3].copy_from_slice(&(collection.len() as u16).to_be_bytes());
    let c_start = p + 3;
    let c_end = c_start + collection.len();
    buf[c_start..c_end].copy_from_slice(collection.as_bytes());
    let end = c_end + id.len();
    buf[c_end..end].copy_from_slice(id.as_bytes());
    Ok(end)
}

fn write_index_key(
    buf: &mut [u8; 300],
    kind: u8,
    collection: &str,
    index_name: &str,
) -> io::Result<usize> {
    key::name(collection)?;
    key::name(index_name)?;
    let p = key::PREFIX.len();
    buf[..p].copy_from_slice(key::PREFIX);
    buf[p] = kind;
    buf[p + 1..p + 3].copy_from_slice(&(collection.len() as u16).to_be_bytes());
    let c_end = p + 3 + collection.len();
    buf[p + 3..c_end].copy_from_slice(collection.as_bytes());
    buf[c_end..c_end + 2].copy_from_slice(&(index_name.len() as u16).to_be_bytes());
    let end = c_end + 2 + index_name.len();
    buf[c_end + 2..end].copy_from_slice(index_name.as_bytes());
    Ok(end)
}

fn write_prefix_end(buf: &mut [u8; 160], prefix: &[u8]) -> usize {
    write_prefix_end_slice(buf, prefix)
}

fn write_prefix_end_slice(buf: &mut [u8], prefix: &[u8]) -> usize {
    let mut len = prefix.len();
    buf[..len].copy_from_slice(prefix);
    while len > 0 {
        if buf[len - 1] != u8::MAX {
            buf[len - 1] += 1;
            return len;
        }
        len -= 1;
    }
    unreachable!("reserved prefixes contain non-0xff bytes")
}

fn write_index_equal_prefix(
    buf: &mut [u8; 832],
    index_prefix: &[u8],
    value: &index::IndexValue,
) -> io::Result<usize> {
    let mut len = index_prefix.len();
    buf[..len].copy_from_slice(index_prefix);
    match value {
        index::IndexValue::Null => {
            buf[len] = 0x10;
            len += 1;
        }
        index::IndexValue::Boolean(b) => {
            buf[len] = 0x20;
            buf[len + 1] = u8::from(*b);
            len += 2;
        }
        index::IndexValue::Integer(n) => {
            buf[len] = 0x30;
            buf[len + 1..len + 9].copy_from_slice(&((*n as u64) ^ (1_u64 << 63)).to_be_bytes());
            len += 9;
        }
        index::IndexValue::String(s) => {
            if s.len() > 256 {
                return Err(key::invalid("indexed string exceeds 256 bytes"));
            }
            buf[len] = 0x40;
            len += 1;
            for byte in s.bytes() {
                if byte == 0 {
                    buf[len] = 0;
                    buf[len + 1] = 0xff;
                    len += 2;
                } else {
                    buf[len] = byte;
                    len += 1;
                }
            }
            buf[len] = 0;
            buf[len + 1] = 0;
            len += 2;
        }
    }
    Ok(len)
}

fn consume_index_value(bytes: &[u8]) -> io::Result<usize> {
    match bytes.first() {
        Some(0x10) => Ok(1),
        Some(0x20) if matches!(bytes.get(1), Some(0 | 1)) => Ok(2),
        Some(0x30) if bytes.len() >= 9 => Ok(9),
        Some(0x40) => {
            let mut offset = 1;
            let mut decoded = [0u8; 256];
            let mut decoded_len = 0_usize;
            loop {
                let byte = *bytes
                    .get(offset)
                    .ok_or_else(|| invalid_data("truncated index string"))?;
                offset += 1;
                if byte != 0 {
                    if decoded_len >= 256 {
                        return Err(invalid_data("index string is too long"));
                    }
                    decoded[decoded_len] = byte;
                    decoded_len += 1;
                } else {
                    let escaped = *bytes
                        .get(offset)
                        .ok_or_else(|| invalid_data("truncated index escape"))?;
                    offset += 1;
                    match escaped {
                        0 => break,
                        0xff => {
                            if decoded_len >= 256 {
                                return Err(invalid_data("index string is too long"));
                            }
                            decoded[decoded_len] = 0;
                            decoded_len += 1;
                        }
                        _ => return Err(invalid_data("invalid index escape")),
                    }
                }
            }
            std::str::from_utf8(&decoded[..decoded_len])
                .map_err(|_| invalid_data("invalid UTF-8 index string"))?;
            Ok(offset)
        }
        _ => Err(invalid_data("invalid index tuple")),
    }
}

fn extract_index_entry_id<'b>(
    entry: &'b [u8],
    index_prefix: &[u8],
    equal_prefix: &[u8],
    fields: usize,
    single_field_fast: bool,
) -> io::Result<&'b str> {
    let remaining = if single_field_fast {
        entry
            .strip_prefix(equal_prefix)
            .ok_or_else(|| invalid_data("invalid index prefix"))?
    } else {
        let mut rem = entry
            .strip_prefix(index_prefix)
            .ok_or_else(|| invalid_data("invalid index prefix"))?;
        for _ in 0..fields {
            let len = consume_index_value(rem)?;
            rem = &rem[len..];
        }
        rem
    };
    let id =
        std::str::from_utf8(remaining).map_err(|_| invalid_data("invalid document ID in index"))?;
    key::id(id).map_err(|_| invalid_data("invalid document ID in index"))?;
    Ok(id)
}

#[allow(clippy::too_many_arguments)]
fn verify_stored_index_entry(
    collection: &str,
    definition: &IndexDefinition,
    index_prefix: &[u8],
    unique_prefix: &[u8],
    flat_verify_ok: bool,
    id: &str,
    entry: &[u8],
    stored: &StoredDocument<'_>,
    reader: &mut PointReader<'_, '_, '_, '_>,
    unique_buf: &mut [u8; 1024],
) -> io::Result<()> {
    use super::flat::FlatScalar;
    if flat_verify_ok {
        if let StoredDocument::Flat(flat) = stored {
            let mut matched = entry.starts_with(index_prefix);
            let mut pos = index_prefix.len();
            for path in &definition.fields {
                let Some(scalar) = flat.get(&path[0]) else {
                    return Err(invalid_data("document does not belong to its index"));
                };
                match scalar {
                    FlatScalar::Null => {
                        if matched && entry.get(pos) == Some(&0x10) {
                            pos += 1;
                        } else {
                            matched = false;
                        }
                    }
                    FlatScalar::Bool(b) => {
                        let enc = [0x20, u8::from(b)];
                        if matched && entry.get(pos..pos + 2) == Some(enc.as_slice()) {
                            pos += 2;
                        } else {
                            matched = false;
                        }
                    }
                    FlatScalar::Int(n) => {
                        let enc = ((n as u64) ^ (1_u64 << 63)).to_be_bytes();
                        if matched
                            && entry.get(pos) == Some(&0x30)
                            && entry.get(pos + 1..pos + 9) == Some(enc.as_slice())
                        {
                            pos += 9;
                        } else {
                            matched = false;
                        }
                    }
                    FlatScalar::Str(s) if s.len() <= 256 => {
                        if matched && entry.get(pos) == Some(&0x40) {
                            pos += 1;
                            for byte in s.bytes() {
                                if byte == 0 {
                                    if entry.get(pos..pos + 2) == Some(&[0, 0xff][..]) {
                                        pos += 2;
                                    } else {
                                        matched = false;
                                        break;
                                    }
                                } else if entry.get(pos) == Some(&byte) {
                                    pos += 1;
                                } else {
                                    matched = false;
                                    break;
                                }
                            }
                            if matched && entry.get(pos..pos + 2) == Some(&[0, 0][..]) {
                                pos += 2;
                            } else {
                                matched = false;
                            }
                        } else {
                            matched = false;
                        }
                    }
                    _ => {
                        return Err(invalid_data("document cannot be represented by its index"));
                    }
                }
            }
            let tuple_end = pos;
            if !matched || entry.get(pos..) != Some(id.as_bytes()) {
                return Err(invalid_data(
                    "document index entry does not match the document",
                ));
            }
            crate::record::validate_key(entry)
                .map_err(|_| invalid_data("document cannot be represented by its index"))?;
            if definition.unique {
                let tuple = &entry[index_prefix.len()..tuple_end];
                let u_len = unique_prefix.len() + tuple.len();
                if u_len > unique_buf.len() {
                    return Err(invalid_data("document cannot be represented by its index"));
                }
                unique_buf[..unique_prefix.len()].copy_from_slice(unique_prefix);
                unique_buf[unique_prefix.len()..u_len].copy_from_slice(tuple);
                crate::record::validate_key(&unique_buf[..u_len])
                    .map_err(|_| invalid_data("document cannot be represented by its index"))?;
                if reader.read_unique_owner(&unique_buf[..u_len])?.as_deref() != Some(id.as_bytes())
                {
                    return Err(invalid_data("invalid unique index entry"));
                }
            }
            return Ok(());
        }
    }
    let owned_value;
    let document = match stored {
        StoredDocument::Full(value) => value,
        StoredDocument::Flat(flat) => {
            owned_value = Value::Object(flat.to_map());
            &owned_value
        }
    };
    let expected = index::keys(collection, definition, document, id)
        .map_err(|_| invalid_data("document cannot be represented by its index"))?
        .ok_or_else(|| invalid_data("document does not belong to its index"))?;
    if expected.entry != entry {
        return Err(invalid_data(
            "document index entry does not match the document",
        ));
    }
    if let Some(unique) = expected.unique {
        if reader.read_unique_owner(&unique)?.as_deref() != Some(id.as_bytes()) {
            return Err(invalid_data("invalid unique index entry"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod metadata_memo_tests {
    use super::*;

    fn bytes(fields: &str) -> Vec<u8> {
        format!(r#"{{"version":1,"schema":{{"fields":[{fields}]}},"indexes":[]}}"#).into_bytes()
    }

    /// Remembered metadata is reused only for the same collection and the
    /// same bytes; any other bytes, valid or not, are decoded afresh.
    #[test]
    fn remembered_metadata_matches_name_and_exact_bytes() {
        let plain = bytes("");
        let first = DocumentTransaction::decode_metadata("a", Some(plain.clone())).unwrap();
        let again = DocumentTransaction::decode_metadata("a", Some(plain.clone())).unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        let other = DocumentTransaction::decode_metadata("b", Some(plain.clone())).unwrap();
        assert!(!Arc::ptr_eq(&first, &other));

        let typed =
            bytes(r#"{"path":["n"],"field_type":"Integer","required":true,"nullable":false}"#);
        let changed = DocumentTransaction::decode_metadata("a", Some(typed)).unwrap();
        assert_eq!(changed.schema.fields.len(), 1);
        assert!(first.schema.fields.is_empty());

        let mut corrupt = plain.clone();
        corrupt.push(b'x');
        assert!(DocumentTransaction::decode_metadata("a", Some(corrupt)).is_err());
        assert!(DocumentTransaction::decode_metadata("a", None).is_err());
        // A dropping collection is refused even when its bytes are remembered.
        let dropping =
            br#"{"version":3,"schema":{"fields":[]},"indexes":[],"dropping":true}"#.to_vec();
        for _ in 0..2 {
            assert_eq!(
                DocumentTransaction::decode_metadata("a", Some(dropping.clone()))
                    .err()
                    .map(|error| error.kind()),
                Some(io::ErrorKind::WouldBlock)
            );
            assert!(
                DocumentTransaction::decode_metadata_including_drop("a", Some(dropping.clone()))
                    .is_ok()
            );
        }
    }

    #[test]
    fn the_memo_keeps_a_bounded_number_of_entries() {
        for index in 0..3 * METADATA_MEMO_ENTRIES {
            DocumentTransaction::decode_metadata(&format!("c{index}"), Some(bytes(""))).unwrap();
        }
        METADATA_MEMO.with(|memo| assert_eq!(memo.borrow().len(), METADATA_MEMO_ENTRIES));
        let large = format!(
            r#"{{"version":1,"schema":{{"fields":[]}},"indexes":[]}}{}"#,
            " ".repeat(METADATA_MEMO_MAX_BYTES)
        );
        let first =
            DocumentTransaction::decode_metadata("big", Some(large.clone().into_bytes())).unwrap();
        let again = DocumentTransaction::decode_metadata("big", Some(large.into_bytes())).unwrap();
        assert!(!Arc::ptr_eq(&first, &again));
    }
}
