//! Bounded, atomic collection schema changes and deletion.
use super::{key, schema::CollectionSchema, transaction::DocumentTransaction};
use std::io;

const MAX_SCHEMA_DOCUMENTS: usize = 256;
const MAX_COLLECTION_DROP_KEYS: usize = 768;
const MAX_MAINTENANCE_BYTES: usize = 32 * 1024 * 1024;
const MAX_DROP_STEP: usize = 128;

pub(super) fn drop_cursor_kind(collection: &str, cursor: &[u8]) -> io::Result<u8> {
    crate::record::validate_key(cursor)?;
    for kind in *b"DIU" {
        let prefix = key::collection(kind, collection)?;
        if cursor.starts_with(&prefix) && cursor.len() > prefix.len() {
            return Ok(kind);
        }
    }
    Err(key::invalid("invalid collection drop cursor"))
}

/// The result of one atomic incremental collection-drop step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectionDropStep {
    /// Number of document or index keys removed by this step.
    pub removed: usize,
    /// Whether the collection metadata was removed by this step.
    pub complete: bool,
}

impl DocumentTransaction<'_, '_> {
    /// Replaces a schema only if every existing document satisfies it. The
    /// validation and metadata update use one transaction and are bounded.
    pub fn set_schema(&mut self, collection: &str, schema: CollectionSchema) -> io::Result<()> {
        self.mutate(|this| {
            schema.validate_definition()?;
            let mut metadata = this.metadata(collection)?;
            if metadata.schema_job.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "a schema migration is in progress",
                ));
            }
            let documents = this.document_rows(
                collection,
                &metadata.schema,
                MAX_SCHEMA_DOCUMENTS,
                MAX_MAINTENANCE_BYTES,
            )?;
            for (_, document) in documents {
                schema.validate_document(&document)?;
            }
            metadata.schema = schema;
            this.save_metadata(collection, &metadata)
        })
    }

    /// Deletes a collection, its documents, metadata and indexes atomically.
    /// Large collections fail before commit; this method never silently
    /// performs a partial drop. Use the separate incremental API for them.
    pub fn drop_collection(&mut self, collection: &str) -> io::Result<()> {
        self.mutate(|this| {
            this.metadata(collection)?;
            let mut keys = Vec::new();
            let mut bytes = 0_usize;
            for kind in *b"DIU" {
                let prefix = key::collection(kind, collection)?;
                let end = key::end(&prefix);
                let mut cursor = prefix;
                loop {
                    let page = this.scan(&cursor, &end, 32)?;
                    if page.is_empty() {
                        break;
                    }
                    for (key, value) in page {
                        bytes = bytes
                            .checked_add(key.len())
                            .and_then(|n| n.checked_add(value.len()))
                            .ok_or_else(|| key::invalid("drop byte count overflow"))?;
                        if keys.len() >= MAX_COLLECTION_DROP_KEYS || bytes > MAX_MAINTENANCE_BYTES {
                            return Err(key::invalid("collection drop exceeds maintenance limits"));
                        }
                        cursor = key.clone();
                        keys.push(key);
                    }
                }
            }
            for key in keys {
                this.remove(&key)?;
            }
            this.remove(&key::collection(b'M', collection)?)
        })
    }

    /// Starts a durable, irreversible incremental deletion. Normal operations
    /// on this collection then return `WouldBlock` until deletion completes.
    /// Existing index or schema jobs must first be completed or cancelled.
    /// Older readers cannot open the marked collection's metadata.
    pub fn start_collection_drop(&mut self, collection: &str) -> io::Result<()> {
        self.mutate(|this| {
            let mut metadata = this.metadata_including_drop(collection)?;
            if metadata.dropping {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "collection deletion is already in progress",
                ));
            }
            if !metadata.jobs.is_empty() || metadata.schema_job.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "finish or cancel collection maintenance first",
                ));
            }
            if this.read(&key::collection(b'P', collection)?)?.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "collection has an unexpected drop cursor",
                ));
            }
            metadata.version = 3;
            metadata.dropping = true;
            this.save_metadata(collection, &metadata)
        })
    }

    /// Reports whether a collection has a durable incremental deletion marker.
    pub fn collection_drop_in_progress(&self, collection: &str) -> io::Result<bool> {
        Ok(self.metadata_including_drop(collection)?.dropping)
    }

    /// Removes at most 128 keys in one transaction. A short nonempty scan is
    /// never interpreted as exhaustion because the scan byte cap may stop it.
    /// A full checkpoint-backed page starts an atomically persisted progress
    /// cursor. Once all namespaces are verified empty, metadata is removed.
    pub fn advance_collection_drop(
        &mut self,
        collection: &str,
        limit: usize,
    ) -> io::Result<CollectionDropStep> {
        self.mutate(|this| {
            if limit == 0 || limit > MAX_DROP_STEP {
                return Err(key::invalid(
                    "collection drop step must be between 1 and 128",
                ));
            }
            let mut metadata = this.metadata_including_drop(collection)?;
            if !metadata.dropping {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "collection deletion is not in progress",
                ));
            }
            let cursor_key = key::collection(b'P', collection)?;
            let cursor = this.read(&cursor_key)?;
            if (metadata.version == 4) != cursor.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "collection drop cursor does not match metadata",
                ));
            }
            let cursor_kind = cursor
                .as_ref()
                .map(|cursor| {
                    drop_cursor_kind(collection, cursor).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid collection drop cursor")
                    })
                })
                .transpose()?;
            for kind in *b"DIU" {
                if cursor_kind.is_some_and(|previous| kind < previous) {
                    continue;
                }
                let prefix = key::collection(kind, collection)?;
                let after = if cursor_kind == Some(kind) {
                    cursor.as_deref().expect("cursor kind requires cursor")
                } else {
                    &prefix
                };
                let rows = this.scan(after, &key::end(&prefix), limit)?;
                if !rows.is_empty() {
                    let removed = rows.len();
                    let next_cursor = (this.checkpoint_backed()
                        && (cursor.is_some() || rows.len() == limit))
                        .then(|| rows.last().expect("nonempty rows").0.clone());
                    for (key, _) in rows {
                        this.remove(&key)?;
                    }
                    if let Some(next_cursor) = next_cursor {
                        if metadata.version == 3 {
                            metadata.version = 4;
                            this.save_metadata(collection, &metadata)?;
                        }
                        this.write(&cursor_key, &next_cursor)?;
                    }
                    return Ok(CollectionDropStep {
                        removed,
                        complete: false,
                    });
                }
            }
            // A stale or forged cursor must never silently orphan remaining
            // keys. Valid jobs have no surviving keys before their cursor.
            if cursor.is_some() {
                for kind in *b"DIU" {
                    let prefix = key::collection(kind, collection)?;
                    if !this.scan(&prefix, &key::end(&prefix), 1)?.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "collection drop cursor skipped existing keys",
                        ));
                    }
                }
                this.remove(&cursor_key)?;
            }
            this.remove(&key::collection(b'M', collection)?)?;
            Ok(CollectionDropStep {
                removed: 0,
                complete: true,
            })
        })
    }
}
