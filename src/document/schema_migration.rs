//! Resumable, bounded schema validation with concurrent-write enforcement.
use super::{key, schema::CollectionSchema, transaction::DocumentTransaction};
use serde::{Deserialize, Serialize};
use std::io;

const MAX_STEP: usize = 128;

/// A pending schema and the last document ID durably validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaMigrationStatus {
    /// Schema enforced on new writes while existing documents are validated.
    pub schema: CollectionSchema,
    /// Last validated document ID, or `None` before the first step.
    pub after: Option<String>,
}

/// Result of one bounded schema migration transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaStep {
    /// Number of documents validated in this step.
    pub processed: usize,
    /// Whether the new schema was atomically published.
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SchemaJob {
    pub schema: CollectionSchema,
    pub after: Option<String>,
}

impl SchemaJob {
    pub(super) fn validate(&self) -> io::Result<()> {
        self.schema.validate_definition()?;
        if let Some(id) = &self.after {
            key::id(id)?;
        }
        Ok(())
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl DocumentTransaction<'_, '_> {
    /// Starts a durable migration. New and changed documents must satisfy both
    /// the current and pending schemas until publication or cancellation.
    pub fn start_schema_migration(
        &mut self,
        collection: &str,
        schema: CollectionSchema,
    ) -> io::Result<()> {
        self.mutate(|this| {
            schema.validate_definition()?;
            let mut metadata = this.metadata(collection)?;
            if metadata.schema_job.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "a schema migration is already in progress",
                ));
            }
            metadata.version = 2;
            metadata.schema_job = Some(SchemaJob {
                schema,
                after: None,
            });
            this.save_metadata(collection, &metadata)
        })
    }

    /// Validates at most 128 documents in one transaction and saves progress.
    /// An empty page atomically publishes the new schema. A validation error
    /// commits nothing; fix or delete the document and retry the step.
    pub fn advance_schema_migration(
        &mut self,
        collection: &str,
        limit: usize,
    ) -> io::Result<SchemaStep> {
        self.mutate(|this| {
            if limit == 0 || limit > MAX_STEP {
                return Err(key::invalid(
                    "schema migration step must be between 1 and 128",
                ));
            }
            let mut metadata = this.metadata(collection)?;
            let job = metadata.schema_job.clone().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "schema migration does not exist")
            })?;
            let prefix = key::collection(b'D', collection)?;
            let end = key::end(&prefix);
            let cursor = match &job.after {
                Some(id) => key::document(collection, id)?,
                None => prefix.clone(),
            };
            let rows = this.scan(&cursor, &end, limit)?;
            if rows.is_empty() {
                metadata.schema = job.schema;
                metadata.schema_job = None;
                this.save_metadata(collection, &metadata)?;
                return Ok(SchemaStep {
                    processed: 0,
                    complete: true,
                });
            }
            let mut last = None;
            for (key_bytes, bytes) in &rows {
                let id = std::str::from_utf8(
                    key_bytes
                        .strip_prefix(prefix.as_slice())
                        .ok_or_else(|| invalid_data("invalid document key"))?,
                )
                .map_err(|_| invalid_data("invalid document ID"))?;
                key::id(id).map_err(|_| invalid_data("invalid document ID"))?;
                let document = Self::decode_document(bytes, &metadata.schema)?;
                job.schema.validate_document(&document)?;
                last = Some(id.to_owned());
            }
            metadata.schema_job = Some(SchemaJob {
                schema: job.schema,
                after: last,
            });
            this.save_metadata(collection, &metadata)?;
            Ok(SchemaStep {
                processed: rows.len(),
                complete: false,
            })
        })
    }

    /// Cancels a migration and keeps the existing active schema.
    pub fn cancel_schema_migration(&mut self, collection: &str) -> io::Result<()> {
        self.mutate(|this| {
            let mut metadata = this.metadata(collection)?;
            if metadata.schema_job.take().is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "schema migration does not exist",
                ));
            }
            this.save_metadata(collection, &metadata)
        })
    }

    /// Returns the pending schema and durable cursor, if a migration exists.
    pub fn schema_migration(&self, collection: &str) -> io::Result<Option<SchemaMigrationStatus>> {
        Ok(self
            .metadata(collection)?
            .schema_job
            .map(|job| SchemaMigrationStatus {
                schema: job.schema,
                after: job.after,
            }))
    }
}
