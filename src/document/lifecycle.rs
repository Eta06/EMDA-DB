//! Bounded, atomic collection schema changes and deletion.
use super::{key, schema::CollectionSchema, transaction::DocumentTransaction};
use std::io;

const MAX_SCHEMA_DOCUMENTS: usize = 256;
const MAX_COLLECTION_DROP_KEYS: usize = 768;
const MAX_MAINTENANCE_BYTES: usize = 32 * 1024 * 1024;
const MAX_DROP_STEP: usize = 128;

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
    /// Once all three namespaces are empty, the metadata is atomically removed.
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
            if !this.metadata_including_drop(collection)?.dropping {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "collection deletion is not in progress",
                ));
            }
            for kind in *b"DIU" {
                let prefix = key::collection(kind, collection)?;
                let rows = this.scan(&prefix, &key::end(&prefix), limit)?;
                if !rows.is_empty() {
                    let removed = rows.len();
                    for (key, _) in rows {
                        this.remove(&key)?;
                    }
                    return Ok(CollectionDropStep {
                        removed,
                        complete: false,
                    });
                }
            }
            this.remove(&key::collection(b'M', collection)?)?;
            Ok(CollectionDropStep {
                removed: 0,
                complete: true,
            })
        })
    }
}
