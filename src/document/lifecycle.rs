//! Bounded, atomic collection schema changes and deletion.
use super::{key, schema::CollectionSchema, transaction::DocumentTransaction};
use std::io;

const MAX_SCHEMA_DOCUMENTS: usize = 256;
const MAX_COLLECTION_DROP_KEYS: usize = 768;
const MAX_MAINTENANCE_BYTES: usize = 32 * 1024 * 1024;

impl DocumentTransaction<'_, '_> {
    /// Replaces a schema only if every existing document satisfies it. The
    /// validation and metadata update use one transaction and are bounded.
    pub fn set_schema(&mut self, collection: &str, schema: CollectionSchema) -> io::Result<()> {
        self.mutate(|this| {
            schema.validate_definition()?;
            let mut metadata = this.metadata(collection)?;
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
    /// Large collections fail before commit and require a future incremental
    /// deletion facility; this method never silently performs a partial drop.
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
}
