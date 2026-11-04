//! Explicitly bounded collection scans, filters, sorting and projection.
use super::{
    index::IndexValue, key, query::DocumentEntry, schema, transaction::DocumentTransaction,
};
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::io::{self, Write};

const MAX_SCAN_DOCUMENTS: usize = 1024;
pub(super) const MAX_SCAN_BYTES: usize = 32 * 1024 * 1024;
const MAX_ARRAY_FILTER_VALUES: usize = 256;
const MAX_ARRAY_FILTER_INPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_ARRAY_FILTER_COMPARISONS: usize = 1_000_000;

/// A filter evaluated against object paths. Missing never equals null. Paths
/// traverse objects only. Array predicates match only an array at the path;
/// values use exact `serde_json` equality, including nested values. Across one
/// filter, array predicates accept at most 256 values and 4 MiB of their
/// encoded JSON. A query fails after 1,000,000 array-element comparisons
/// across its evaluated documents; these are not total memory or time bounds.
#[derive(Clone, Debug, PartialEq)]
pub enum DocumentFilter {
    /// Match all documents.
    All,
    /// Test whether a path exists, including explicit null.
    Exists { path: Vec<String>, exists: bool },
    /// Compare any JSON value with exact serde_json equality.
    Equal { path: Vec<String>, value: Value },
    /// Match an array containing an element equal to `value`.
    ArrayContains { path: Vec<String>, value: Value },
    /// Match an array containing at least one of `values`. An empty list never matches.
    ArrayContainsAny {
        path: Vec<String>,
        values: Vec<Value>,
    },
    /// Match an array containing each of `values`. An empty list matches every array.
    /// Duplicate requested values do not require duplicate array elements.
    ArrayContainsAll {
        path: Vec<String>,
        values: Vec<Value>,
    },
    /// Match an array with exactly this number of elements.
    ArrayLength { path: Vec<String>, length: usize },
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

/// A bounded filtered query. `query` scans the complete collection;
/// `query_planned` can instead probe a qualifying unique equality index.
/// Both fail rather than silently truncating when their work limits are exceeded.
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
    /// Maximum documents examined by the access path, between 1 and 1,024.
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

/// Actual work performed by a bounded document query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryPlan {
    /// Documents examined by the selected access path.
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
    /// Actual work performed by the selected access path.
    pub plan: QueryPlan,
}

// These are logical work limits, not total memory or time bounds. Complex
// equality comparisons can traverse nested JSON values.
#[derive(Default)]
struct ArrayFilterBudget {
    values: usize,
    bytes: usize,
    comparisons: usize,
}

impl Write for ArrayFilterBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| key::invalid("array filter inputs exceed the encoded byte limit"))?;
        if self.bytes > MAX_ARRAY_FILTER_INPUT_BYTES {
            return Err(key::invalid(
                "array filter inputs exceed the encoded byte limit",
            ));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ArrayFilterBudget {
    fn add_values<'a>(&mut self, values: impl IntoIterator<Item = &'a Value>) -> io::Result<()> {
        for value in values {
            self.values = self
                .values
                .checked_add(1)
                .ok_or_else(|| key::invalid("array filter exceeds the value limit"))?;
            if self.values > MAX_ARRAY_FILTER_VALUES {
                return Err(key::invalid("array filter exceeds the value limit"));
            }
            schema::validate_tree(value)?;
            serde_json::to_writer(&mut *self, value).map_err(|_| {
                key::invalid(
                    "array filter inputs exceed the encoded byte limit or cannot be encoded",
                )
            })?;
        }
        Ok(())
    }

    fn contains(&mut self, array: &[Value], value: &Value) -> io::Result<bool> {
        for entry in array {
            self.comparisons = self
                .comparisons
                .checked_add(1)
                .ok_or_else(|| key::invalid("array filter comparison limit exceeded"))?;
            if self.comparisons > MAX_ARRAY_FILTER_COMPARISONS {
                return Err(key::invalid("array filter comparison limit exceeded"));
            }
            if entry == value {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(super) fn validate_filter(filter: &DocumentFilter) -> io::Result<()> {
    let mut pending = vec![(filter, 0_usize)];
    let mut count = 0_usize;
    let mut array_budget = ArrayFilterBudget::default();
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
            DocumentFilter::ArrayContains { path, value } => {
                schema::validate_path(path)?;
                array_budget.add_values(std::iter::once(value))?;
            }
            DocumentFilter::ArrayContainsAny { path, values }
            | DocumentFilter::ArrayContainsAll { path, values } => {
                schema::validate_path(path)?;
                array_budget.add_values(values)?;
            }
            DocumentFilter::ArrayLength { path, .. } => schema::validate_path(path)?,
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

fn matches(
    filter: &DocumentFilter,
    document: &Value,
    array_budget: &mut ArrayFilterBudget,
) -> io::Result<bool> {
    Ok(match filter {
        DocumentFilter::All => true,
        DocumentFilter::Exists { path, exists } => {
            schema::field(document, path).is_some() == *exists
        }
        DocumentFilter::Equal { path, value } => schema::field(document, path) == Some(value),
        DocumentFilter::ArrayContains { path, value } => {
            match schema::field(document, path).and_then(Value::as_array) {
                Some(array) => array_budget.contains(array, value)?,
                None => false,
            }
        }
        DocumentFilter::ArrayContainsAny { path, values } => {
            let mut found = false;
            if let Some(array) = schema::field(document, path).and_then(Value::as_array) {
                for value in values {
                    if array_budget.contains(array, value)? {
                        found = true;
                        break;
                    }
                }
            }
            found
        }
        DocumentFilter::ArrayContainsAll { path, values } => {
            match schema::field(document, path).and_then(Value::as_array) {
                Some(array) => {
                    let mut found = true;
                    for value in values {
                        if !array_budget.contains(array, value)? {
                            found = false;
                            break;
                        }
                    }
                    found
                }
                None => false,
            }
        }
        DocumentFilter::ArrayLength { path, length } => schema::field(document, path)
            .and_then(Value::as_array)
            .is_some_and(|array| array.len() == *length),
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
                if !matches(filter, document, array_budget)? {
                    matched = false;
                    break;
                }
            }
            matched
        }
        DocumentFilter::Or(filters) => {
            let mut matched = false;
            for filter in filters {
                if matches(filter, document, array_budget)? {
                    matched = true;
                    break;
                }
            }
            matched
        }
        DocumentFilter::Not(filter) => !matches(filter, document, array_budget)?,
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

pub(super) fn validate_query(query: &DocumentQuery) -> io::Result<()> {
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
    Ok(())
}

pub(super) fn finish_query(
    rows: Vec<(String, Value)>,
    query: &DocumentQuery,
) -> io::Result<DocumentQueryResult> {
    let scanned = rows.len();
    let mut matched = Vec::new();
    let mut array_budget = ArrayFilterBudget::default();
    for (id, document) in rows {
        if matches(&query.filter, &document, &mut array_budget)? {
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

impl DocumentTransaction<'_, '_> {
    /// Executes a complete bounded scan with filter, sort, pagination and
    /// optional projection. Statistics describe the work actually performed.
    pub fn query(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<DocumentQueryResult> {
        validate_query(query)?;
        let metadata = self.metadata(collection)?;
        let rows = self.document_rows(
            collection,
            &metadata.schema,
            query.max_scanned,
            MAX_SCAN_BYTES,
        )?;
        finish_query(rows, query)
    }
}
