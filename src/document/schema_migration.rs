//! Resumable, bounded schema validation with concurrent-write enforcement.
use super::{
    key,
    maintenance_key::{self, MaintenanceKey, Seal},
    schema::CollectionSchema,
    transaction::DocumentTransaction,
};
use serde::{Deserialize, Serialize};
use std::io;

const MAX_STEP: usize = 128;
const SCHEMA_PROGRESS_DOMAIN: &[u8] = b"EMDA guarded schema migration v1\0";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seal: Option<Seal>,
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
        self.start_schema_migration_using(collection, schema, None)
    }

    /// Starts a migration with progress authenticated by an independently kept key.
    pub fn start_schema_migration_guarded(
        &mut self,
        collection: &str,
        schema: CollectionSchema,
        maintenance_key: &MaintenanceKey,
    ) -> io::Result<()> {
        self.start_schema_migration_using(collection, schema, Some(maintenance_key))
    }

    fn start_schema_migration_using(
        &mut self,
        collection: &str,
        schema: CollectionSchema,
        maintenance_key: Option<&MaintenanceKey>,
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
            let seal = maintenance_key::seal(
                maintenance_key,
                SCHEMA_PROGRESS_DOMAIN,
                (collection, &schema, Option::<&str>::None),
            )?;
            metadata.schema_job = Some(SchemaJob {
                schema,
                after: None,
                seal,
            });
            this.save_metadata(collection, &metadata)
        })
    }

    /// Protects an unfinished unguarded migration with a fresh, separately
    /// saved key. The pending schema must match the expected definition.
    /// The next step validates from the beginning of the collection.
    pub fn restart_schema_migration_guarded(
        &mut self,
        collection: &str,
        expected: &CollectionSchema,
        maintenance_key: &MaintenanceKey,
    ) -> io::Result<()> {
        self.mutate(|this| {
            let mut metadata = this.metadata(collection)?;
            let job = metadata.schema_job.as_mut().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "schema migration does not exist")
            })?;
            if job.seal.is_some() {
                return Err(key::invalid("schema migration is already guarded"));
            }
            if &job.schema != expected {
                return Err(key::invalid(
                    "pending schema does not match expected schema",
                ));
            }
            job.seal = maintenance_key::seal(
                Some(maintenance_key),
                SCHEMA_PROGRESS_DOMAIN,
                (collection, expected, Option::<&str>::None),
            )?;
            job.after = None;
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
        self.advance_schema_migration_using(collection, limit, None)
    }

    /// Verifies the guarded schema and cursor before validating or publishing.
    pub fn advance_schema_migration_guarded(
        &mut self,
        collection: &str,
        limit: usize,
        maintenance_key: &MaintenanceKey,
    ) -> io::Result<SchemaStep> {
        self.advance_schema_migration_using(collection, limit, Some(maintenance_key))
    }

    fn advance_schema_migration_using(
        &mut self,
        collection: &str,
        limit: usize,
        maintenance_key: Option<&MaintenanceKey>,
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
            maintenance_key::verify(
                maintenance_key,
                job.seal.as_ref(),
                SCHEMA_PROGRESS_DOMAIN,
                (collection, &job.schema, job.after.as_deref()),
            )?;
            let prefix = key::collection(b'D', collection)?;
            let end = key::end(&prefix);
            let cursor = match &job.after {
                Some(id) => key::document(collection, id)?,
                None => prefix.clone(),
            };
            let rows = this.scan_document_page(&cursor, &end, limit)?;
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
            let seal = maintenance_key::seal(
                maintenance_key,
                SCHEMA_PROGRESS_DOMAIN,
                (collection, &job.schema, last.as_deref()),
            )?;
            metadata.schema_job = Some(SchemaJob {
                schema: job.schema,
                after: last,
                seal,
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
