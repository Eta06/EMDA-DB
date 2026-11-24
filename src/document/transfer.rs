use super::{
    CollectionSchema, DocumentDatabase, DocumentTransaction, DocumentTransactionMode,
    IndexDefinition, key,
};
use crate::TransactionFailure;
use serde::de::{self, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{self, Write};
use std::marker::PhantomData;

/// Maximum encoded bytes in a portable document collection archive.
pub const MAX_COLLECTION_ARCHIVE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum documents in one atomic collection archive.
pub const MAX_COLLECTION_ARCHIVE_DOCUMENTS: usize = 256;
const LEGACY_VERSION: u32 = 1;
const BOUNDED_SCHEMA_VERSION: u32 = 2;

fn archive_version(schema: &CollectionSchema) -> u32 {
    if schema.has_value_bounds() {
        BOUNDED_SCHEMA_VERSION
    } else {
        LEGACY_VERSION
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectionArchive {
    version: u32,
    schema: CollectionSchema,
    #[serde(deserialize_with = "archive_indexes")]
    indexes: Vec<IndexDefinition>,
    #[serde(deserialize_with = "archive_documents")]
    documents: Vec<ArchiveDocument>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveDocument {
    id: String,
    document: Value,
}

fn bounded_sequence<'de, D, T, const MAX: usize>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T, const MAX: usize>(PhantomData<T>);

    impl<'de, T: Deserialize<'de>, const MAX: usize> Visitor<'de> for Bounded<T, MAX> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "an array of at most {MAX} items")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<T>, A::Error> {
            let mut items = Vec::new();
            while items.len() < MAX {
                let Some(item) = sequence.next_element()? else {
                    return Ok(items);
                };
                items.push(item);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(de::Error::invalid_length(MAX + 1, &self));
            }
            Ok(items)
        }
    }

    deserializer.deserialize_seq(Bounded::<T, MAX>(PhantomData))
}

fn archive_indexes<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<IndexDefinition>, D::Error> {
    bounded_sequence::<_, _, 16>(deserializer)
}

fn archive_documents<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ArchiveDocument>, D::Error> {
    bounded_sequence::<_, _, MAX_COLLECTION_ARCHIVE_DOCUMENTS>(deserializer)
}

#[derive(Default)]
struct ArchiveOutput(Vec<u8>);

impl Write for ArchiveOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_COLLECTION_ARCHIVE_BYTES - self.0.len() {
            return Err(key::invalid("collection archive exceeds its byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl DocumentTransaction<'_, '_> {
    /// Exports a bounded logical archive from this transaction's view.
    /// A pending index or schema job must finish or be cancelled first.
    pub fn export_collection(&self, collection: &str) -> io::Result<Vec<u8>> {
        let metadata = self.metadata(collection)?;
        if !metadata.jobs.is_empty() || metadata.schema_job.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "collection maintenance is in progress",
            ));
        }
        let mut documents = Vec::new();
        self.visit_document_rows(
            collection,
            &metadata.schema,
            MAX_COLLECTION_ARCHIVE_DOCUMENTS,
            MAX_COLLECTION_ARCHIVE_BYTES,
            |id, document| documents.push(ArchiveDocument { id, document }),
        )?;
        let archive = CollectionArchive {
            version: archive_version(&metadata.schema),
            schema: metadata.schema,
            indexes: metadata.indexes,
            documents,
        };
        let mut output = ArchiveOutput::default();
        serde_json::to_writer(&mut output, &archive).map_err(io::Error::from)?;
        Ok(output.0)
    }

    /// Stages a collection archive under a new name. Indexes are rebuilt
    /// through the normal schema and index checks. Any failure aborts the
    /// whole transaction, including earlier staged changes from the caller.
    pub fn import_collection(&mut self, collection: &str, bytes: &[u8]) -> io::Result<()> {
        self.mutate(|this| {
            if bytes.len() > MAX_COLLECTION_ARCHIVE_BYTES {
                return Err(key::invalid("collection archive exceeds its byte limit"));
            }
            let archive: CollectionArchive = serde_json::from_slice(bytes)
                .map_err(|_| key::invalid("invalid document collection archive"))?;
            if archive.version != archive_version(&archive.schema) {
                return Err(key::invalid(
                    "unsupported document collection archive version",
                ));
            }
            if archive
                .documents
                .windows(2)
                .any(|pair| pair[0].id >= pair[1].id)
            {
                return Err(key::invalid("invalid collection archive document order"));
            }
            this.create_collection(collection, archive.schema)?;
            for kind in *b"DIU" {
                let prefix = key::collection(kind, collection)?;
                let mut occupied = false;
                this.scan_admitted(&prefix, &key::end(&prefix), 1, |_, _| {
                    occupied = true;
                    false
                })?;
                if occupied {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "destination has orphan document or index keys",
                    ));
                }
            }
            for index in archive.indexes {
                this.create_index(collection, index)?;
            }
            for row in archive.documents {
                this.insert(collection, &row.id, &row.document)?;
            }
            Ok(())
        })
    }
}

impl DocumentDatabase<'_> {
    /// Exports one bounded logical collection from a consistent read.
    pub fn export_collection(&self, collection: &str) -> io::Result<Vec<u8>> {
        self.transaction(|tx| tx.export_collection(collection))
    }

    /// Imports one archive atomically into a new collection. An unknown
    /// commit outcome requires reconciliation before retry.
    pub fn import_collection(
        &self,
        collection: &str,
        archive: &[u8],
    ) -> Result<(), TransactionFailure> {
        self.transaction_detailed(DocumentTransactionMode::Conservative, None, None, |tx| {
            tx.import_collection(collection, archive)
        })
    }
}
