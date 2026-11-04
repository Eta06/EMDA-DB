//! Conservative equality-index selection for bounded document queries.
use super::{
    filter::{self, DocumentFilter, DocumentQuery, DocumentQueryResult},
    index::{IndexDefinition, IndexValue},
    key,
    query::IndexQuery,
    transaction::{DocumentTransaction, MAX_DOCUMENT_BYTES},
};
use std::io;

/// The access path selected for a planned document query.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DocumentQueryAccess {
    /// No eligible unique equality index was found; scan the complete collection.
    CollectionScan,
    /// Probe a non-partial unique single-field index using exact scalar equality.
    UniqueEqualityIndex { index: String },
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
    indexes.iter().find_map(|index| {
        if !index.unique || index.partial.is_some() || index.fields.len() != 1 {
            return None;
        }
        required_equality(filter, &index.fields[0]).map(|value| (index, value))
    })
}

impl DocumentTransaction<'_, '_> {
    /// Executes a bounded filtered query using a qualifying unique equality
    /// index, or falls back to the complete collection scan. This opt-in path
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
        // A valid unique index has at most one match. Fetch two so a corrupt
        // duplicate cannot silently produce a truncated query result.
        let page = self.query_index_with_metadata(
            collection,
            &index.name,
            &IndexQuery {
                equal: vec![value],
                limit: 2,
                ..Default::default()
            },
            &metadata,
        )?;
        if page.documents.len() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unique index contains multiple matches",
            ));
        }
        let mut rows = Vec::with_capacity(page.documents.len());
        for entry in page.documents {
            // Indexed reads already reject stored documents over this bound.
            // Conservatively check the maximum, avoiding an allocation and
            // re-encoding while preserving the scan's logical byte limit.
            let max_bytes = key::document(collection, &entry.id)?
                .len()
                .checked_add(MAX_DOCUMENT_BYTES)
                .ok_or_else(|| key::invalid("scan byte count overflow"))?;
            if rows.len() >= query.max_scanned || max_bytes > filter::MAX_SCAN_BYTES {
                return Err(key::invalid("document scan exceeds the operation limit"));
            }
            rows.push((entry.id, entry.document));
        }
        Ok(PlannedDocumentQueryResult {
            result: filter::finish_query(rows, query)?,
            access: DocumentQueryAccess::UniqueEqualityIndex {
                index: index.name.clone(),
            },
        })
    }
}
