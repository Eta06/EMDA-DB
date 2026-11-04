//! Resumable, bounded index creation and deletion with concurrent writers.
use super::{
    index::{self, IndexDefinition},
    key,
    transaction::{DocumentTransaction, Metadata},
};
use serde::{Deserialize, Serialize};
use std::io;

const MAX_STEP: usize = 128;

/// The key namespace currently being removed by an incremental drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexDropPhase {
    /// Remove ordinary secondary index entries.
    Entries,
    /// Remove unique-value ownership entries.
    Unique,
}

/// A durable index maintenance job. Building indexes are not queryable;
/// dropping indexes stop being queryable as soon as the drop starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexJobStatus {
    /// A build can be resumed after the last scanned document ID.
    Building {
        definition: IndexDefinition,
        after: Option<String>,
    },
    /// A drop is removing the indicated key namespace.
    Dropping { name: String, phase: IndexDropPhase },
}

/// Result of one bounded index maintenance transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexStep {
    /// Number of documents scanned or index keys removed in this step.
    pub processed: usize,
    /// Whether this step published the index or finished the drop.
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(super) enum IndexJob {
    Building {
        definition: IndexDefinition,
        after: Option<String>,
    },
    Dropping {
        name: String,
        phase: IndexDropPhase,
        after: Option<Vec<u8>>,
    },
}

impl IndexJob {
    pub(super) fn name(&self) -> &str {
        match self {
            Self::Building { definition, .. } => &definition.name,
            Self::Dropping { name, .. } => name,
        }
    }

    pub(super) fn building_definition(&self) -> Option<&IndexDefinition> {
        match self {
            Self::Building { definition, .. } => Some(definition),
            Self::Dropping { .. } => None,
        }
    }

    pub(super) fn validate(&self, collection: &str) -> io::Result<()> {
        match self {
            Self::Building { definition, after } => {
                definition.validate()?;
                if let Some(id) = after {
                    key::id(id)?;
                }
            }
            Self::Dropping { name, phase, after } => {
                key::name(name)?;
                if let Some(cursor) = after {
                    let prefix = key::index(
                        match phase {
                            IndexDropPhase::Entries => b'I',
                            IndexDropPhase::Unique => b'U',
                        },
                        collection,
                        name,
                    )?;
                    crate::record::validate_key(cursor)?;
                    if !cursor.starts_with(&prefix) || cursor.len() <= prefix.len() {
                        return Err(key::invalid("invalid index drop cursor"));
                    }
                }
            }
        }
        Ok(())
    }

    fn status(&self) -> IndexJobStatus {
        match self {
            Self::Building { definition, after } => IndexJobStatus::Building {
                definition: definition.clone(),
                after: after.clone(),
            },
            Self::Dropping { name, phase, .. } => IndexJobStatus::Dropping {
                name: name.clone(),
                phase: *phase,
            },
        }
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn job_position(metadata: &Metadata, name: &str) -> io::Result<usize> {
    metadata
        .jobs
        .iter()
        .position(|job| job.name() == name)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "index maintenance job does not exist",
            )
        })
}
fn validate_step(limit: usize) -> io::Result<()> {
    if limit == 0 || limit > MAX_STEP {
        return Err(key::invalid(
            "index maintenance step must be between 1 and 128",
        ));
    }
    Ok(())
}

impl DocumentTransaction<'_, '_> {
    /// Starts a durable build. Existing documents are scanned in later steps;
    /// new document writes maintain the pending index from this commit onward.
    /// A unique index is not guaranteed until the build completes.
    pub fn start_index_build(
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
                .any(|index| index.name == definition.name)
                || metadata
                    .jobs
                    .iter()
                    .any(|job| job.name() == definition.name)
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "index or index maintenance job already exists",
                ));
            }
            if metadata.indexes.len() + metadata.jobs.len() >= 16 {
                return Err(key::invalid("collection exceeds 16 indexes and jobs"));
            }
            for kind in *b"IU" {
                let prefix = key::index(kind, collection, &definition.name)?;
                if !this.scan(&prefix, &key::end(&prefix), 1)?.is_empty() {
                    return Err(invalid_data("unexpected pre-existing index keys"));
                }
            }
            metadata.version = 2;
            metadata.jobs.push(IndexJob::Building {
                definition,
                after: None,
            });
            this.save_metadata(collection, &metadata)
        })
    }

    /// Scans at most `limit` documents and persists progress atomically. An
    /// empty page publishes the completed index. Conflicting writes can make
    /// the transaction return `WouldBlock`; retry the entire step.
    pub fn advance_index_build(
        &mut self,
        collection: &str,
        name: &str,
        limit: usize,
    ) -> io::Result<IndexStep> {
        self.mutate(|this| {
            validate_step(limit)?;
            let mut metadata = this.metadata(collection)?;
            let position = job_position(&metadata, name)?;
            let (definition, after) = match &metadata.jobs[position] {
                IndexJob::Building { definition, after } => (definition.clone(), after.clone()),
                _ => return Err(key::invalid("index job is not a build")),
            };
            let prefix = key::collection(b'D', collection)?;
            let end = key::end(&prefix);
            let cursor = match after {
                Some(id) => key::document(collection, &id)?,
                None => prefix.clone(),
            };
            let rows = this.scan(&cursor, &end, limit)?;
            if rows.is_empty() {
                metadata.jobs.remove(position);
                metadata.indexes.push(definition);
                this.save_metadata(collection, &metadata)?;
                return Ok(IndexStep {
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
                if let Some(keys) = index::keys(collection, &definition, &document, id)? {
                    match this.read(&keys.entry)? {
                        Some(value) if value.is_empty() => {
                            if let Some(unique) = &keys.unique {
                                if this.read(unique)?.as_deref() != Some(id.as_bytes()) {
                                    return Err(invalid_data("invalid pending unique index entry"));
                                }
                            }
                        }
                        Some(_) => return Err(invalid_data("invalid pending index entry")),
                        None => {
                            if let Some(unique) = &keys.unique {
                                if let Some(owner) = this.read(unique)? {
                                    if owner == id.as_bytes() {
                                        return Err(invalid_data(
                                            "unique entry has no document index entry",
                                        ));
                                    }
                                    return Err(io::Error::new(
                                        io::ErrorKind::AlreadyExists,
                                        "unique index build found a duplicate",
                                    ));
                                }
                            }
                            this.write(&keys.entry, &[])?;
                            if let Some(unique) = keys.unique {
                                this.write(&unique, id.as_bytes())?;
                            }
                        }
                    }
                }
                last = Some(id.to_owned());
            }
            metadata.jobs[position] = IndexJob::Building {
                definition,
                after: last,
            };
            this.save_metadata(collection, &metadata)?;
            Ok(IndexStep {
                processed: rows.len(),
                complete: false,
            })
        })
    }

    /// Cancels a build and changes its job into an incremental cleanup.
    pub fn cancel_index_build(&mut self, collection: &str, name: &str) -> io::Result<()> {
        self.mutate(|this| {
            let mut metadata = this.metadata(collection)?;
            let position = job_position(&metadata, name)?;
            if !matches!(metadata.jobs[position], IndexJob::Building { .. }) {
                return Err(key::invalid("index job is not a build"));
            }
            metadata.jobs[position] = IndexJob::Dropping {
                name: name.to_owned(),
                phase: IndexDropPhase::Entries,
                after: None,
            };
            this.save_metadata(collection, &metadata)
        })
    }

    /// Makes an active index unavailable and starts durable incremental removal.
    pub fn start_index_drop(&mut self, collection: &str, name: &str) -> io::Result<()> {
        self.mutate(|this| {
            let mut metadata = this.metadata(collection)?;
            let position = metadata
                .indexes
                .iter()
                .position(|index| index.name == name)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index does not exist"))?;
            metadata.indexes.remove(position);
            metadata.version = 2;
            metadata.jobs.push(IndexJob::Dropping {
                name: name.to_owned(),
                phase: IndexDropPhase::Entries,
                after: None,
            });
            this.save_metadata(collection, &metadata)
        })
    }

    /// Removes at most `limit` index keys and persists its cursor atomically.
    /// An empty entries page advances to unique keys; an empty unique page
    /// completes the job and releases the index name.
    pub fn advance_index_drop(
        &mut self,
        collection: &str,
        name: &str,
        limit: usize,
    ) -> io::Result<IndexStep> {
        self.mutate(|this| {
            validate_step(limit)?;
            let mut metadata = this.metadata(collection)?;
            let position = job_position(&metadata, name)?;
            let (phase, after) = match &metadata.jobs[position] {
                IndexJob::Dropping { phase, after, .. } => (*phase, after.clone()),
                _ => return Err(key::invalid("index job is not a drop")),
            };
            let prefix = key::index(
                match phase {
                    IndexDropPhase::Entries => b'I',
                    IndexDropPhase::Unique => b'U',
                },
                collection,
                name,
            )?;
            let end = key::end(&prefix);
            let cursor = after.unwrap_or(prefix);
            let rows = this.scan(&cursor, &end, limit)?;
            if rows.is_empty() {
                if phase == IndexDropPhase::Entries {
                    metadata.jobs[position] = IndexJob::Dropping {
                        name: name.to_owned(),
                        phase: IndexDropPhase::Unique,
                        after: None,
                    };
                    this.save_metadata(collection, &metadata)?;
                    return Ok(IndexStep {
                        processed: 0,
                        complete: false,
                    });
                }
                metadata.jobs.remove(position);
                this.save_metadata(collection, &metadata)?;
                return Ok(IndexStep {
                    processed: 0,
                    complete: true,
                });
            }
            for (key, _) in &rows {
                this.remove(key)?;
            }
            let after = rows.last().map(|(key, _)| key.clone());
            metadata.jobs[position] = IndexJob::Dropping {
                name: name.to_owned(),
                phase,
                after,
            };
            this.save_metadata(collection, &metadata)?;
            Ok(IndexStep {
                processed: rows.len(),
                complete: false,
            })
        })
    }

    /// Lists resumable index jobs without exposing partially built indexes.
    pub fn index_jobs(&self, collection: &str) -> io::Result<Vec<IndexJobStatus>> {
        Ok(self
            .metadata(collection)?
            .jobs
            .iter()
            .map(IndexJob::status)
            .collect())
    }
}
