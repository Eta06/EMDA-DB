//! Bounded one-to-many relationships through a named non-unique index.
//!
//! Each selected source row supplies a complete typed tuple, one value per
//! field of the foreign index in order. Every foreign document whose index
//! entry carries exactly that tuple is returned, in ascending foreign ID
//! order. Index entries are read through the validated index visitor, so
//! each entry is checked against its document, and in read-set transactions
//! the scanned tuple ranges are validated at commit like any other range.
use super::{
    filter::{DocumentQuery, QueryPlan},
    index::{IndexValue, MAX_INDEX_FIELDS},
    key,
    planner::{DocumentQueryAccess, PlannedDocumentQueryResult},
    query::{DocumentEntry, IndexQuery, IndexScanOptions, MAX_QUERY_DOCUMENTS, index_definition},
    schema,
    transaction::DocumentTransaction,
};
use std::io;

/// Most related documents one source row may have.
pub const MAX_LOOKUP_MANY_PER_SOURCE: usize = 1024;
/// Most related documents one lookup may return in total.
pub const MAX_LOOKUP_MANY_RELATED: usize = 4096;
/// Largest related-document response, in ID plus stored document bytes.
pub const MAX_LOOKUP_MANY_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Required limits for a one-to-many lookup. Exceeding any limit fails the
/// whole lookup; no partial result is returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct LookupManyLimits {
    /// Most related documents for one source row, 1 to 1,024.
    pub max_per_source: usize,
    /// Most related documents across all source rows, 1 to 4,096. A
    /// document related to several source rows counts once per row.
    pub max_related: usize,
    /// Most related-document bytes across all source rows: each returned
    /// document's ID length plus its stored length, counted per row. 1 byte
    /// to 16 MiB. Each document is admitted before its value is read.
    pub max_response_bytes: usize,
}

impl LookupManyLimits {
    /// Limits with every field set explicitly.
    pub fn new(max_per_source: usize, max_related: usize, max_response_bytes: usize) -> Self {
        Self {
            max_per_source,
            max_related,
            max_response_bytes,
        }
    }

    fn validate(&self) -> io::Result<()> {
        if !(1..=MAX_LOOKUP_MANY_PER_SOURCE).contains(&self.max_per_source)
            || !(1..=MAX_LOOKUP_MANY_RELATED).contains(&self.max_related)
            || !(1..=MAX_LOOKUP_MANY_RESPONSE_BYTES).contains(&self.max_response_bytes)
        {
            return Err(key::invalid(
                "lookup limits must be 1 to 1,024 per source, 1 to 4,096 related and 1 byte to 16 MiB",
            ));
        }
        Ok(())
    }
}

/// A source row and all of its related foreign documents.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct LookupManyEntry {
    /// The source document.
    pub source: DocumentEntry,
    /// Related foreign documents in ascending ID order; empty when nothing
    /// matches or a local field is missing.
    pub related: Vec<DocumentEntry>,
}

/// Complete one-to-many lookup results and the work they took.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct LookupManyResult {
    /// Source rows in source query order, each with its related documents.
    pub documents: Vec<LookupManyEntry>,
    /// Actual work on the source collection.
    pub plan: QueryPlan,
    /// Related documents returned across all source rows.
    pub related: usize,
    /// Related-document bytes admitted against `max_response_bytes`.
    pub response_bytes: usize,
    /// Foreign index entries read.
    pub index_entries_read: usize,
}

/// One-to-many lookup results with the chosen source access path.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct PlannedLookupManyResult {
    /// Rows, related documents and work.
    pub result: LookupManyResult,
    /// Access path used to select source rows.
    pub access: DocumentQueryAccess,
}

impl DocumentTransaction<'_, '_> {
    /// Resolves every foreign document whose entry in the named non-unique
    /// index carries the source row's complete tuple. `local_paths` holds one
    /// source path per indexed field, in the index's field order.
    ///
    /// Tuple semantics match [Self::lookup_unique_index]: a missing local
    /// field gives no related documents; null matches an indexed null; other
    /// values must be indexable scalars (null, boolean, signed 64-bit integer,
    /// or string up to 256 bytes) and compare by type, so the integer 1 never
    /// matches the string "1". A partial index relates only the documents it
    /// covers. A tuple too long to be stored in any entry matches nothing.
    ///
    /// The source query is a bounded collection scan and must not be
    /// projected. Source and foreign reads share this transaction and its
    /// read budget. Any exceeded limit, invalid input or inconsistent index
    /// entry fails the whole call.
    pub fn lookup_many(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_paths: &[Vec<String>],
        (foreign_collection, foreign_index): (&str, &str),
        limits: LookupManyLimits,
    ) -> io::Result<LookupManyResult> {
        self.lookup_many_using(
            (collection, query, false),
            local_paths,
            (foreign_collection, foreign_index),
            limits,
        )
        .map(|lookup| lookup.result)
    }

    /// Like [Self::lookup_many], selecting source rows with the bounded query
    /// planner, which trusts declared index maintenance.
    pub fn lookup_many_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
        local_paths: &[Vec<String>],
        (foreign_collection, foreign_index): (&str, &str),
        limits: LookupManyLimits,
    ) -> io::Result<PlannedLookupManyResult> {
        self.lookup_many_using(
            (collection, query, true),
            local_paths,
            (foreign_collection, foreign_index),
            limits,
        )
    }

    fn lookup_many_using(
        &self,
        (collection, query, planned): (&str, &DocumentQuery, bool),
        local_paths: &[Vec<String>],
        (foreign_collection, foreign_index): (&str, &str),
        limits: LookupManyLimits,
    ) -> io::Result<PlannedLookupManyResult> {
        // Validate every input before reading anything.
        limits.validate()?;
        if query.projection.is_some() {
            return Err(key::invalid(
                "lookup does not accept a projected source query",
            ));
        }
        for path in local_paths {
            schema::validate_path(path)?;
        }
        let foreign = self.metadata(foreign_collection)?;
        let definition = index_definition(&foreign, foreign_index)?;
        if definition.unique || local_paths.len() != definition.fields.len() {
            return Err(key::invalid(
                "lookup_many requires a non-unique index and one local path per indexed field",
            ));
        }
        let index_prefix = key::index(b'I', foreign_collection, foreign_index)?;
        let PlannedDocumentQueryResult { result, access } = if planned {
            self.query_planned(collection, query)?
        } else {
            PlannedDocumentQueryResult {
                result: self.query(collection, query)?,
                access: DocumentQueryAccess::CollectionScan,
            }
        };
        let mut documents = Vec::with_capacity(result.documents.len());
        let (mut related_total, mut response_bytes, mut entries_read) = (0, 0, 0);
        for source in result.documents {
            let mut related = Vec::new();
            if let Some(tuple) = source_tuple(&source.document, local_paths)? {
                // An entry holds the tuple and an ID of at least one byte.
                let mut encoded = index_prefix.clone();
                for value in &tuple {
                    value.encode(&mut encoded)?;
                }
                if encoded.len() < crate::record::MAX_KEY {
                    let per_source_cap = limits
                        .max_per_source
                        .min(limits.max_related - related_total);
                    let mut after = None;
                    loop {
                        // Ask for one more than allowed to detect overflow.
                        let page = (per_source_cap + 1 - related.len()).min(MAX_QUERY_DOCUMENTS);
                        let scan = self.visit_index_with_metadata(
                            foreign_collection,
                            definition,
                            &IndexQuery {
                                equal: tuple.clone(),
                                limit: page,
                                ..Default::default()
                            },
                            &foreign,
                            IndexScanOptions {
                                after: after.as_deref(),
                                byte_limit: None,
                                response_byte_limit: Some(
                                    limits.max_response_bytes - response_bytes,
                                ),
                                candidate_limit: None,
                                unique_equality: false,
                                reject_response_overflow: true,
                            },
                            |entry| related.push(entry),
                        )?;
                        // Every entry read must be returned; a shorter page
                        // would present an incomplete relationship.
                        if scan.plan.returned != scan.plan.entries_read {
                            return Err(key::invalid("lookup response exceeds its byte limit"));
                        }
                        entries_read += scan.plan.entries_read;
                        response_bytes += scan.response_bytes;
                        if related.len() > per_source_cap {
                            return Err(key::invalid(if per_source_cap == limits.max_per_source {
                                "lookup source exceeds max_per_source related documents"
                            } else {
                                "lookup exceeds max_related related documents"
                            }));
                        }
                        match scan.next {
                            Some(next) if scan.plan.returned == page => after = Some(next),
                            _ => break,
                        }
                    }
                }
            }
            related_total += related.len();
            documents.push(LookupManyEntry { source, related });
        }
        Ok(PlannedLookupManyResult {
            result: LookupManyResult {
                documents,
                plan: result.plan,
                related: related_total,
                response_bytes,
                index_entries_read: entries_read,
            },
            access,
        })
    }
}

// The complete typed tuple of a source row, or `None` if a field is missing.
fn source_tuple(
    document: &serde_json::Value,
    local_paths: &[Vec<String>],
) -> io::Result<Option<Vec<IndexValue>>> {
    debug_assert!(local_paths.len() <= MAX_INDEX_FIELDS);
    let mut tuple = Vec::with_capacity(local_paths.len());
    for path in local_paths {
        let Some(value) = schema::field(document, path) else {
            return Ok(None);
        };
        tuple.push(IndexValue::from_json(value)?);
    }
    Ok(Some(tuple))
}
