//! Shared document transaction dispatch and atomic collection operations.
use super::{
    index::{self, IndexDefinition, Keys},
    key,
    online_index::IndexJob,
    schema::CollectionSchema,
    schema_migration::SchemaJob,
};
use crate::{CheckpointTransaction, Entry, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io;

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
}

/// A document transaction backed by the same atomic commit as raw key/value
/// operations. Any failed mutation aborts the whole transaction, even when the
/// callback handles the error. Callback errors also discard all staged changes.
pub struct DocumentTransaction<'a, 'store> {
    backend: BackendTransaction<'a, 'store>,
    failed: bool,
}

impl<'a, 'store> DocumentTransaction<'a, 'store> {
    pub(super) fn ordinary(tx: &'a mut Transaction<'store>) -> Self {
        Self {
            backend: BackendTransaction::Ordinary(tx),
            failed: false,
        }
    }

    pub(super) fn checkpoint(tx: &'a mut CheckpointTransaction<'store>) -> Self {
        Self {
            backend: BackendTransaction::Checkpoint(tx),
            failed: false,
        }
    }

    pub(super) fn finish<T>(self, result: io::Result<T>) -> io::Result<T> {
        match result {
            Err(error) => Err(error),
            Ok(_) if self.failed => Err(io::Error::other(
                "document transaction contains a failed mutation",
            )),
            Ok(value) => Ok(value),
        }
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
        match &self.backend {
            BackendTransaction::Ordinary(tx) => tx.get(key),
            BackendTransaction::Checkpoint(tx) => tx.get(key),
        }
    }

    pub(super) fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        match &mut self.backend {
            BackendTransaction::Ordinary(tx) => tx.put(key, value),
            BackendTransaction::Checkpoint(tx) => tx.put(key, value),
        }
    }

    pub(super) fn remove(&mut self, key: &[u8]) -> io::Result<()> {
        match &mut self.backend {
            BackendTransaction::Ordinary(tx) => tx.delete(key),
            BackendTransaction::Checkpoint(tx) => tx.delete(key),
        }
    }

    pub(super) fn scan(&self, after: &[u8], before: &[u8], limit: usize) -> io::Result<Vec<Entry>> {
        match &self.backend {
            BackendTransaction::Ordinary(tx) => tx.scan(Some(after), Some(before), limit),
            BackendTransaction::Checkpoint(tx) => tx.scan(Some(after), Some(before), limit),
        }
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
        if !matches!(metadata.version, 1..=3)
            || ((metadata.version == 3) != metadata.dropping)
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
        self.read(&key::document(collection, id)?)?
            .map(|bytes| Self::decode_document(&bytes, schema))
            .transpose()
    }

    /// Reads a document, distinguishing a missing ID from a missing collection.
    pub fn get(&self, collection: &str, id: &str) -> io::Result<Option<Value>> {
        let metadata = self.metadata(collection)?;
        self.read_document(collection, id, &metadata.schema)
    }

    fn check_old_index(&self, keys: &Keys, id: &str, allow_missing: bool) -> io::Result<bool> {
        let entry = self.read(&keys.entry)?;
        let unique = keys
            .unique
            .as_ref()
            .map(|key| self.read(key))
            .transpose()?
            .flatten();
        match entry {
            Some(value) if value.is_empty() => {
                if keys.unique.is_some() && unique.as_deref() != Some(id.as_bytes()) {
                    return Err(invalid_data(
                        "document unique index entry is missing or invalid",
                    ));
                }
                Ok(true)
            }
            Some(_) => Err(invalid_data("document index entry is invalid")),
            None if !allow_missing => Err(invalid_data("document index entry is missing")),
            None if unique.as_deref() == Some(id.as_bytes()) => {
                Err(invalid_data("unique index entry has no document entry"))
            }
            None => Ok(false),
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
                if !same_entry && self.read(&keys.entry)?.is_some() {
                    return Err(invalid_data("unexpected document index entry"));
                }
                if let Some(unique) = &keys.unique {
                    let same_unique =
                        previous.as_ref().and_then(|old| old.unique.as_ref()) == Some(unique);
                    if !same_unique && self.read(unique)?.is_some() {
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
            let page = self.scan(&cursor, &end, 32)?;
            if page.is_empty() {
                break;
            }
            for (key, value) in page {
                bytes = bytes
                    .checked_add(key.len())
                    .and_then(|n| n.checked_add(value.len()))
                    .ok_or_else(|| key::invalid("scan byte count overflow"))?;
                if count >= max || bytes > max_bytes {
                    return Err(key::invalid("document scan exceeds the operation limit"));
                }
                let id = std::str::from_utf8(
                    key.strip_prefix(prefix.as_slice())
                        .ok_or_else(|| invalid_data("invalid document key"))?,
                )
                .map_err(|_| invalid_data("invalid document ID"))?;
                key::id(id).map_err(|_| invalid_data("invalid document ID"))?;
                visit(id.to_owned(), Self::decode_document(&value, schema)?);
                count += 1;
                cursor = key;
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
