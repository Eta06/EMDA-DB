//! Explicitly bounded collection scans, filters, sorting and projection.
use super::{
    index::IndexValue, key, query::DocumentEntry, schema, transaction::DocumentTransaction,
};
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::io;

const MAX_SCAN_DOCUMENTS: usize = 1024;
const MAX_SCAN_BYTES: usize = 32 * 1024 * 1024;

/// A filter evaluated against object paths. Missing never equals null.
#[derive(Clone, Debug, PartialEq)]
pub enum DocumentFilter {
    /// Match all documents.
    All,
    /// Test whether a path exists, including explicit null.
    Exists { path: Vec<String>, exists: bool },
    /// Compare any JSON value with exact serde_json equality.
    Equal { path: Vec<String>, value: Value },
    /// Compare indexable scalars by the documented index ordering.
    Compare {
        path: Vec<String>,
        comparison: Comparison,
        value: IndexValue,
    },
    /// Match every nested filter; an empty list matches all documents.
    And(Vec<DocumentFilter>),
    /// Match any nested filter; an empty list matches no documents.
    Or(Vec<DocumentFilter>),
    /// Negate a nested filter.
    Not(Box<DocumentFilter>),
}

/// Scalar comparison used by a filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

/// Sort by an object path. Missing values sort before null in ascending order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentSort {
    /// An object path containing an indexable scalar when present.
    pub path: Vec<String>,
    /// Reverse scalar ordering when true; IDs break ties in ascending order.
    pub descending: bool,
}

/// A bounded complete collection scan. Queries fail instead of silently
/// truncating when the collection exceeds `max_scanned` or the byte budget.
#[derive(Clone, Debug)]
pub struct DocumentQuery {
    /// Predicate applied before projection and pagination.
    pub filter: DocumentFilter,
    /// Optional scalar sort; without it, order is ascending ID bytes.
    pub sort: Option<DocumentSort>,
    /// Optional object paths to retain; missing paths are omitted.
    pub projection: Option<Vec<Vec<String>>>,
    /// Matching rows skipped after sorting.
    pub offset: usize,
    /// Maximum rows returned, between 1 and 256.
    pub limit: usize,
    /// Maximum documents examined, between 1 and 1,024.
    pub max_scanned: usize,
}

impl Default for DocumentQuery {
    fn default() -> Self {
        Self {
            filter: DocumentFilter::All,
            sort: None,
            projection: None,
            offset: 0,
            limit: 100,
            max_scanned: 256,
        }
    }
}

/// Actual work performed by a collection-scan query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryPlan {
    /// Documents examined by the complete collection scan.
    pub scanned: usize,
    /// Documents matching before offset and limit.
    pub matched: usize,
    /// Whether a sort was requested.
    pub sorted: bool,
    /// Number of rows returned.
    pub returned: usize,
}

/// Query results and their actual collection-scan statistics.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentQueryResult {
    /// Results after filter, sort, offset, limit and projection.
    pub documents: Vec<DocumentEntry>,
    /// Execution statistics; this initial plan always uses a collection scan.
    pub plan: QueryPlan,
}

pub(super) fn validate_filter(filter: &DocumentFilter) -> io::Result<()> {
    let mut pending = vec![(filter, 0_usize)];
    let mut count = 0_usize;
    while let Some((filter, depth)) = pending.pop() {
        count += 1;
        if count > 128 || depth > 16 || pending.len() + count > 128 {
            return Err(key::invalid("filter exceeds depth or node limit"));
        }
        match filter {
            DocumentFilter::All => {}
            DocumentFilter::Exists { path, .. } => schema::validate_path(path)?,
            DocumentFilter::Equal { path, value } => {
                schema::validate_path(path)?;
                schema::validate_tree(value)?;
            }
            DocumentFilter::Compare { path, value, .. } => {
                schema::validate_path(path)?;
                value.encode(&mut Vec::new())?;
            }
            DocumentFilter::And(filters) | DocumentFilter::Or(filters) => {
                if pending.len() + count + filters.len() > 128 {
                    return Err(key::invalid("filter exceeds node limit"));
                }
                pending.extend(filters.iter().map(|filter| (filter, depth + 1)));
            }
            DocumentFilter::Not(filter) => pending.push((filter, depth + 1)),
        }
    }
    Ok(())
}

fn encoded(value: &IndexValue) -> io::Result<Vec<u8>> {
    let mut result = Vec::new();
    value.encode(&mut result)?;
    Ok(result)
}

fn matches(filter: &DocumentFilter, document: &Value) -> io::Result<bool> {
    Ok(match filter {
        DocumentFilter::All => true,
        DocumentFilter::Exists { path, exists } => {
            schema::field(document, path).is_some() == *exists
        }
        DocumentFilter::Equal { path, value } => schema::field(document, path) == Some(value),
        DocumentFilter::Compare {
            path,
            comparison,
            value,
        } => {
            let actual =
                schema::field(document, path).and_then(|value| IndexValue::from_json(value).ok());
            match actual {
                None => false,
                Some(actual) => {
                    let order = encoded(&actual)?.cmp(&encoded(value)?);
                    match comparison {
                        Comparison::Less => order == Ordering::Less,
                        Comparison::LessOrEqual => order != Ordering::Greater,
                        Comparison::Greater => order == Ordering::Greater,
                        Comparison::GreaterOrEqual => order != Ordering::Less,
                    }
                }
            }
        }
        DocumentFilter::And(filters) => {
            let mut matched = true;
            for filter in filters {
                if !matches(filter, document)? {
                    matched = false;
                    break;
                }
            }
            matched
        }
        DocumentFilter::Or(filters) => {
            let mut matched = false;
            for filter in filters {
                if matches(filter, document)? {
                    matched = true;
                    break;
                }
            }
            matched
        }
        DocumentFilter::Not(filter) => !matches(filter, document)?,
    })
}

fn project(document: &Value, paths: &[Vec<String>]) -> io::Result<Value> {
    let mut result = Value::Object(Map::new());
    for path in paths {
        let Some(value) = schema::field(document, path) else {
            continue;
        };
        let mut current = &mut result;
        for component in &path[..path.len() - 1] {
            let object = current.as_object_mut().expect("projection objects");
            current = object
                .entry(component.clone())
                .or_insert_with(|| Value::Object(Map::new()));
        }
        current
            .as_object_mut()
            .expect("non-overlapping projection paths")
            .insert(path.last().expect("validated path").clone(), value.clone());
    }
    Ok(result)
}

impl DocumentTransaction<'_, '_> {
    /// Executes a complete bounded scan with filter, sort, pagination and
    /// optional projection. Statistics describe the work actually performed.
    pub fn query(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<DocumentQueryResult> {
        if query.limit == 0
            || query.limit > 256
            || query.max_scanned == 0
            || query.max_scanned > MAX_SCAN_DOCUMENTS
            || query.offset > MAX_SCAN_DOCUMENTS
        {
            return Err(key::invalid("invalid query limit, offset or scan budget"));
        }
        validate_filter(&query.filter)?;
        if let Some(sort) = &query.sort {
            schema::validate_path(&sort.path)?;
        }
        if let Some(paths) = &query.projection {
            if paths.len() > 64 {
                return Err(key::invalid("projection exceeds 64 paths"));
            }
            for path in paths {
                schema::validate_path(path)?;
            }
            for (position, path) in paths.iter().enumerate() {
                if paths
                    .iter()
                    .skip(position + 1)
                    .any(|other| path.starts_with(other) || other.starts_with(path))
                {
                    return Err(key::invalid("projection paths overlap"));
                }
            }
        }
        let metadata = self.metadata(collection)?;
        let rows = self.document_rows(
            collection,
            &metadata.schema,
            query.max_scanned,
            MAX_SCAN_BYTES,
        )?;
        let scanned = rows.len();
        let mut matched = Vec::new();
        for (id, document) in rows {
            if matches(&query.filter, &document)? {
                let sort_key = match &query.sort {
                    None => None,
                    Some(sort) => match schema::field(&document, &sort.path) {
                        None => Some(Vec::new()),
                        Some(value) => Some(encoded(&IndexValue::from_json(value)?)?),
                    },
                };
                matched.push((sort_key, id, document));
            }
        }
        let matched_count = matched.len();
        if let Some(sort) = &query.sort {
            matched.sort_by(|left, right| {
                let order = left.0.cmp(&right.0);
                (if sort.descending {
                    order.reverse()
                } else {
                    order
                })
                .then_with(|| left.1.as_bytes().cmp(right.1.as_bytes()))
            });
        }
        let mut documents = Vec::new();
        for (_, id, document) in matched.into_iter().skip(query.offset).take(query.limit) {
            let document = match &query.projection {
                Some(paths) => project(&document, paths)?,
                None => document,
            };
            documents.push(DocumentEntry { id, document });
        }
        let plan = QueryPlan {
            scanned,
            matched: matched_count,
            sorted: query.sort.is_some(),
            returned: documents.len(),
        };
        Ok(DocumentQueryResult { documents, plan })
    }
}
