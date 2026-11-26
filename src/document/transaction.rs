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
use std::sync::Mutex;

pub(super) const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_INDEX_BUILD_DOCUMENTS: usize = 256;
const MAX_INDEX_BUILD_BYTES: usize = 32 * 1024 * 1024;
const MAX_INDEX_DROP_KEYS: usize = 768;
const MAX_INDEX_DROP_BYTES: usize = 32 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
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

    pub(super) fn metadata(&self, collection: &str) -> io::Result<Metadata> {
        let key = key::collection(b'M', collection)?;
        let bytes = self.read(&key)?;
        Self::decode_metadata(collection, bytes)
    }

    pub(super) fn metadata_including_drop(&self, collection: &str) -> io::Result<Metadata> {
        let bytes = self.read(&key::collection(b'M', collection)?)?;
        Self::decode_metadata_including_drop(collection, bytes)
    }

    pub(super) fn decode_metadata(
        collection: &str,
        bytes: Option<Vec<u8>>,
    ) -> io::Result<Metadata> {
        let metadata = Self::decode_metadata_including_drop(collection, bytes)?;
        if metadata.dropping {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "collection deletion is in progress",
            ));
        }
        Ok(metadata)
    }

    fn decode_metadata_including_drop(
        collection: &str,
        bytes: Option<Vec<u8>>,
    ) -> io::Result<Metadata> {
        let bytes = bytes
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "collection does not exist"))?;
        let metadata: Metadata = serde_json::from_slice(&bytes)
            .map_err(|_| invalid_data("invalid document metadata"))?;
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

    pub(super) fn decode_document(bytes: &[u8], schema: &CollectionSchema) -> io::Result<Value> {
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(invalid_data("stored document exceeds size limit"));
        }
        let document: Value =
            serde_json::from_slice(bytes).map_err(|_| invalid_data("invalid stored document"))?;
        schema
            .validate_document(&document)
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
            let mut metadata = this.metadata(collection)?;
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
            let mut metadata = this.metadata(collection)?;
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
        Ok(self.metadata(collection)?.indexes)
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
