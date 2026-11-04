//! Conservative equality and scalar range selection for bounded queries.
use super::{
    filter::{self, Comparison, DocumentFilter, DocumentQuery, DocumentQueryResult},
    index::{IndexDefinition, IndexValue, PartialFilter},
    key,
    query::{IndexBound, IndexQuery},
    transaction::DocumentTransaction,
};
use std::cmp::Ordering;
use std::io;

/// The access path selected for a planned document query.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DocumentQueryAccess {
    /// No eligible index was found; scan the complete collection.
    CollectionScan,
    /// Probe a non-partial unique single-field index using exact scalar equality.
    UniqueEqualityIndex { index: String },
    /// Scan a non-partial, nonunique single-field index for exact scalar equality.
    EqualityIndex { index: String },
    /// Probe a non-partial unique compound index using equality on every field.
    UniqueCompoundEqualityIndex { index: String, fields: usize },
    /// Scan a non-partial nonunique compound index using equality on every field.
    CompoundEqualityIndex { index: String, fields: usize },
    /// Scan a non-partial single-field index over a required scalar range.
    RangeIndex { index: String },
    /// Scan a compound index with equality on a prefix and a range on the next field.
    CompoundRangeIndex {
        index: String,
        equality_fields: usize,
    },
    /// Probe an implied partial unique index using equality on every field.
    PartialUniqueEqualityIndex { index: String, fields: usize },
    /// Scan an implied partial nonunique index using equality on every field.
    PartialEqualityIndex { index: String, fields: usize },
    /// Scan an implied partial single-field index over a required scalar range.
    PartialRangeIndex { index: String },
    /// Scan an implied partial compound index with a prefix and scalar range.
    PartialCompoundRangeIndex {
        index: String,
        equality_fields: usize,
    },
}

/// Filtered query output, including the chosen access path and actual work.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedDocumentQueryResult {
    /// Filtered, sorted, paginated and projected results.
    pub result: DocumentQueryResult,
    /// The access path used by this execution.
    pub access: DocumentQueryAccess,
}

// Only predicates required by the entire filter are eligible. Never descend
// into Or or Not: their children need not match every result.
fn required_equality(filter: &DocumentFilter, path: &[String]) -> Option<IndexValue> {
    let mut pending = vec![filter];
    while let Some(filter) = pending.pop() {
        match filter {
            DocumentFilter::Equal { path: field, value } if field == path => {
                if let Ok(value) = IndexValue::from_json(value) {
                    return Some(value);
                }
            }
            DocumentFilter::In {
                path: field,
                values,
            } if field == path && values.len() == 1 => {
                if let Ok(value) = IndexValue::from_json(&values[0]) {
                    return Some(value);
                }
            }
            DocumentFilter::And(filters) => pending.extend(filters.iter().rev()),
            _ => {}
        }
    }
    None
}

// A successful equality, comparison or array predicate requires the path to
// exist. Explicit Exists is the only currently recognized proof of absence.
// Children of Or/Not are never individually required by the whole filter.
fn required_presence(filter: &DocumentFilter, path: &[String], exists: bool) -> bool {
    let mut pending = vec![filter];
    while let Some(filter) = pending.pop() {
        match filter {
            DocumentFilter::Exists {
                path: field,
                exists: actual,
            } if field == path && *actual == exists => return true,
            DocumentFilter::Equal { path: field, .. }
            | DocumentFilter::In { path: field, .. }
            | DocumentFilter::Compare { path: field, .. }
            | DocumentFilter::ArrayContains { path: field, .. }
            | DocumentFilter::ArrayContainsAny { path: field, .. }
            | DocumentFilter::ArrayContainsAll { path: field, .. }
            | DocumentFilter::ArrayLength { path: field, .. }
                if exists && field == path =>
            {
                return true;
            }
            DocumentFilter::And(filters) => pending.extend(filters.iter().rev()),
            _ => {}
        }
    }
    false
}

fn partial_is_implied(filter: &DocumentFilter, partial: &PartialFilter) -> bool {
    match partial {
        PartialFilter::Exists { path, exists } => required_presence(filter, path, *exists),
        PartialFilter::Equals { path, value } => {
            required_equality(filter, path).as_ref() == Some(value)
        }
    }
}

fn eligible(index: &IndexDefinition, filter: &DocumentFilter) -> bool {
    index
        .partial
        .as_ref()
        .is_none_or(|partial| partial_is_implied(filter, partial))
}

#[derive(Clone)]
enum Selection {
    Equality(Vec<IndexValue>),
    Range {
        equal: Vec<IndexValue>,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
    },
}

// Keep the tighter bound in the index's actual encoded ordering. Equal bounds
// are exclusive if any corresponding predicate is exclusive.
fn tighten(
    current: &mut Option<(IndexBound, Vec<u8>)>,
    value: &IndexValue,
    inclusive: bool,
    lower: bool,
) -> io::Result<()> {
    let mut encoded = Vec::new();
    value.encode(&mut encoded)?;
    if let Some((bound, old)) = current {
        match encoded.cmp(old) {
            Ordering::Equal => bound.inclusive &= inclusive,
            Ordering::Greater if lower => {
                *current = Some((
                    IndexBound {
                        value: value.clone(),
                        inclusive,
                    },
                    encoded,
                ));
            }
            Ordering::Less if !lower => {
                *current = Some((
                    IndexBound {
                        value: value.clone(),
                        inclusive,
                    },
                    encoded,
                ));
            }
            _ => {}
        }
    } else {
        *current = Some((
            IndexBound {
                value: value.clone(),
                inclusive,
            },
            encoded,
        ));
    }
    Ok(())
}

fn required_range(
    filter: &DocumentFilter,
    path: &[String],
) -> io::Result<Option<(Option<IndexBound>, Option<IndexBound>)>> {
    let mut lower = None;
    let mut upper = None;
    let mut pending = vec![filter];
    while let Some(filter) = pending.pop() {
        match filter {
            DocumentFilter::Compare {
                path: field,
                comparison,
                value,
            } if field == path => match comparison {
                Comparison::Greater => tighten(&mut lower, value, false, true)?,
                Comparison::GreaterOrEqual => tighten(&mut lower, value, true, true)?,
                Comparison::Less => tighten(&mut upper, value, false, false)?,
                Comparison::LessOrEqual => tighten(&mut upper, value, true, false)?,
            },
            DocumentFilter::And(filters) => pending.extend(filters.iter().rev()),
            _ => {}
        }
    }
    if lower.is_none() && upper.is_none() {
        return Ok(None);
    }
    Ok(Some((
        lower.map(|(bound, _)| bound),
        upper.map(|(bound, _)| bound),
    )))
}

fn choose_index<'a>(
    filter: &DocumentFilter,
    indexes: &'a [IndexDefinition],
) -> io::Result<Option<(&'a IndexDefinition, Selection)>> {
    // Prefer a unique exact tuple, then a nonunique one, regardless of range.
    // Complete tuples require equality on every field, so omitted missing
    // fields cannot produce an incorrect result.
    if let Some(selected) = indexes
        .iter()
        .filter(|index| index.unique)
        .chain(indexes.iter().filter(|index| !index.unique))
        .filter(|index| eligible(index, filter))
        .find_map(|index| {
            index
                .fields
                .iter()
                .map(|field| required_equality(filter, field))
                .collect::<Option<Vec<_>>>()
                .map(|tuple| (index, Selection::Equality(tuple)))
        })
    {
        return Ok(Some(selected));
    }
    // Keep the existing single-field range preference so adding compound
    // planning does not displace an already eligible single-field range.
    for index in indexes
        .iter()
        .filter(|index| index.fields.len() == 1)
        .chain(indexes.iter().filter(|index| index.fields.len() > 1))
        .filter(|index| eligible(index, filter))
    {
        let mut equal = Vec::new();
        for (position, field) in index.fields.iter().enumerate() {
            if let Some(value) = required_equality(filter, field) {
                equal.push(value);
                continue;
            }
            // Compound entries omit a document if any indexed field is
            // missing. The required range proves this field is present;
            // every later field must be independently required as well.
            if index.fields[position + 1..]
                .iter()
                .all(|later| required_presence(filter, later, true))
            {
                if let Some((lower, upper)) = required_range(filter, field)? {
                    return Ok(Some((
                        index,
                        Selection::Range {
                            equal,
                            lower,
                            upper,
                        },
                    )));
                }
            }
            break;
        }
    }
    Ok(None)
}

impl DocumentTransaction<'_, '_> {
    /// Executes a bounded filtered query using a qualifying exact equality
    /// tuple or qualifying scalar range, or falls back to the collection scan.
    /// A partial index is eligible only when the filter proves its condition.
    /// This opt-in path assumes declared indexes are maintained; corruption
    /// that removes index entries can cause omissions. It is not cost-based.
    pub fn query_planned(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<PlannedDocumentQueryResult> {
        filter::validate_query(query)?;
        let metadata = self.metadata(collection)?;
        let Some((index, selection)) = choose_index(&query.filter, &metadata.indexes)? else {
            return Ok(PlannedDocumentQueryResult {
                result: self.query(collection, query)?,
                access: DocumentQueryAccess::CollectionScan,
            });
        };
        let unique_equality = index.unique && matches!(selection, Selection::Equality(_));
        let (equal, lower, upper) = match &selection {
            Selection::Equality(values) => (values.clone(), None, None),
            Selection::Range {
                equal,
                lower,
                upper,
            } => (equal.clone(), lower.clone(), upper.clone()),
        };
        let mut rows = Vec::new();
        let mut bytes = 0_usize;
        let mut after = None;
        loop {
            // Probe one entry beyond the candidate limit, never silently
            // returning a prefix. Unique equality probes two to detect a
            // corrupt duplicate rather than accepting it as one result.
            let limit = if unique_equality {
                2
            } else {
                (query.max_scanned - rows.len() + 1).min(256)
            };
            let indexed = self.query_index_with_metadata(
                collection,
                &index.name,
                &IndexQuery {
                    equal: equal.clone(),
                    lower: lower.clone(),
                    upper: upper.clone(),
                    after,
                    limit,
                },
                &metadata,
            )?;
            let page = indexed.page;
            if unique_equality && page.documents.len() > 1 {
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
            if unique_equality {
                break;
            }
            after = Some(page.next.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index page lost its continuation cursor",
                )
            })?);
        }
        let partial = index.partial.is_some();
        let access = match selection {
            Selection::Equality(values) if partial && index.unique => {
                DocumentQueryAccess::PartialUniqueEqualityIndex {
                    index: index.name.clone(),
                    fields: values.len(),
                }
            }
            Selection::Equality(values) if partial => DocumentQueryAccess::PartialEqualityIndex {
                index: index.name.clone(),
                fields: values.len(),
            },
            Selection::Equality(values) if index.unique && values.len() > 1 => {
                DocumentQueryAccess::UniqueCompoundEqualityIndex {
                    index: index.name.clone(),
                    fields: values.len(),
                }
            }
            Selection::Equality(values) if values.len() > 1 => {
                DocumentQueryAccess::CompoundEqualityIndex {
                    index: index.name.clone(),
                    fields: values.len(),
                }
            }
            Selection::Equality(_) if index.unique => DocumentQueryAccess::UniqueEqualityIndex {
                index: index.name.clone(),
            },
            Selection::Equality(_) => DocumentQueryAccess::EqualityIndex {
                index: index.name.clone(),
            },
            Selection::Range { equal, .. } => {
                // Index ranges are ordered by value then ID; collection scans
                // use ID order. Restore ID order before filtering and paging.
                rows.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
                if partial && index.fields.len() > 1 {
                    DocumentQueryAccess::PartialCompoundRangeIndex {
                        index: index.name.clone(),
                        equality_fields: equal.len(),
                    }
                } else if partial {
                    DocumentQueryAccess::PartialRangeIndex {
                        index: index.name.clone(),
                    }
                } else if index.fields.len() > 1 {
                    DocumentQueryAccess::CompoundRangeIndex {
                        index: index.name.clone(),
                        equality_fields: equal.len(),
                    }
                } else {
                    DocumentQueryAccess::RangeIndex {
                        index: index.name.clone(),
                    }
                }
            }
        };
        Ok(PlannedDocumentQueryResult {
            result: filter::finish_query(rows, query)?,
            access,
        })
    }
}
