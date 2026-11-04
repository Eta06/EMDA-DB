//! Conservative equality-index selection for bounded document queries.
use super::{
    filter::{self, DocumentFilter, DocumentQuery, DocumentQueryResult},
    index::{IndexDefinition, IndexValue},
    key,
    query::IndexQuery,
    transaction::DocumentTransaction,
};
use std::io;

/// The access path selected for a planned document query.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DocumentQueryAccess {
    /// No eligible equality index was found; scan the complete collection.
    CollectionScan,
    /// Probe a non-partial unique single-field index using exact scalar equality.
    UniqueEqualityIndex { index: String },
    /// Scan a non-partial, nonunique single-field index for exact scalar equality.
    EqualityIndex { index: String },
}

/// Filtered query output, including the chosen access path and actual work.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedDocumentQueryResult {
    /// Filtered, sorted, paginated and projected results.
    pub result: DocumentQueryResult,
    /// The access path used by this execution.
    pub access: DocumentQueryAccess,
}

// Only Equal nodes that are required by the entire filter are eligible. Never
// descend into Or or Not: their children need not match every result.
fn required_equality(filter: &DocumentFilter, path: &[String]) -> Option<IndexValue> {
    let mut pending = vec![filter];
    while let Some(filter) = pending.pop() {
        match filter {
            DocumentFilter::Equal { path: field, value } if field == path => {
                if let Ok(value) = IndexValue::from_json(value) {
                    return Some(value);
                }
            }
            DocumentFilter::And(filters) => pending.extend(filters.iter().rev()),
            _ => {}
        }
    }
    None
}

fn choose_index<'a>(
    filter: &DocumentFilter,
    indexes: &'a [IndexDefinition],
) -> Option<(&'a IndexDefinition, IndexValue)> {
    // A unique probe has at most one valid result. Prefer it regardless of
    // declaration order, then consider a nonunique single-field index.
    indexes
        .iter()
        .filter(|index| index.unique)
        .chain(indexes.iter().filter(|index| !index.unique))
        .find_map(|index| {
            if index.partial.is_some() || index.fields.len() != 1 {
                return None;
            }
            required_equality(filter, &index.fields[0]).map(|value| (index, value))
        })
}

impl DocumentTransaction<'_, '_> {
    /// Executes a bounded filtered query using a qualifying single-field
    /// equality index, or falls back to the complete collection scan. This opt-in path
    /// assumes declared indexes are maintained; corruption that removes index
    /// entries can cause omissions. Neither path is a cost-based optimizer.
    pub fn query_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<PlannedDocumentQueryResult> {
        filter::validate_query(query)?;
        let metadata = self.metadata(collection)?;
        let Some((index, value)) = choose_index(&query.filter, &metadata.indexes) else {
            return Ok(PlannedDocumentQueryResult {
                result: self.query(collection, query)?,
                access: DocumentQueryAccess::CollectionScan,
            });
        };
        let mut rows = Vec::new();
        let mut bytes = 0_usize;
        let mut after = None;
        loop {
            // Probe one entry beyond the candidate limit, never silently
            // returning a prefix. Unique indexes probe two to catch a corrupt
            // duplicate rather than accepting it as one result.
            let limit = if index.unique {
                2
            } else {
                (query.max_scanned - rows.len() + 1).min(256)
            };
            let indexed = self.query_index_with_metadata(
                collection,
                &index.name,
                &IndexQuery {
                    equal: vec![value.clone()],
                    after,
                    limit,
                    ..Default::default()
                },
                &metadata,
            )?;
            let page = indexed.page;
            if index.unique && page.documents.len() > 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unique index contains multiple matches",
                ));
            }
            bytes = bytes
                .checked_add(indexed.document_key_value_bytes)
                .ok_or_else(|| key::invalid("scan byte count overflow"))?;
            if bytes > filter::MAX_SCAN_BYTES
                || page.documents.len() > query.max_scanned - rows.len()
            {
                return Err(key::invalid("document scan exceeds the operation limit"));
            }
            if page.documents.is_empty() {
                break;
            }
            rows.extend(
                page.documents
                    .into_iter()
                    .map(|entry| (entry.id, entry.document)),
            );
            if index.unique {
                break;
            }
            after = Some(page.next.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index page lost its continuation cursor",
                )
            })?);
        }
        let access = if index.unique {
            DocumentQueryAccess::UniqueEqualityIndex {
                index: index.name.clone(),
            }
        } else {
            DocumentQueryAccess::EqualityIndex {
                index: index.name.clone(),
            }
        };
        Ok(PlannedDocumentQueryResult {
            result: filter::finish_query(rows, query)?,
            access,
        })
    }
}
