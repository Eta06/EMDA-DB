//! Explicitly bounded collection scans, filters, sorting and projection.
use super::{
    index::{IndexValue, MAX_INDEX_STRING_BYTES},
    key,
    query::DocumentEntry,
    schema,
    transaction::DocumentTransaction,
};
use serde_json::{Map, Value};
use std::cmp::{Ordering, Reverse};
use std::collections::BTreeMap;
use std::io::{self, Write};

const MAX_SCAN_DOCUMENTS: usize = 1024;
pub(super) const MAX_SCAN_BYTES: usize = 32 * 1024 * 1024;
const MAX_FILTER_NODES: usize = 128;
const MAX_FILTER_VALUES: usize = 256;
const MAX_FILTER_INPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_FILTER_COMPARISONS: usize = 1_000_000;
const MAX_FILTER_ARRAY_ELEMENT_VISITS: usize = 1_000_000;

/// A filter evaluated against object paths. Missing never equals null. Paths
/// traverse objects only. Array predicates match only an array at the path;
/// values use exact `serde_json` equality, including nested values. Across one
/// filter, equality, membership and array predicates accept at most 256 values
/// and 4 MiB of their encoded JSON. A query fails after 1,000,000 comparisons
/// across its evaluated documents. Array element matching additionally accepts
/// at most 1,000,000 element visits per query, including non-object elements.
/// These are not total memory or time bounds.
#[derive(Clone, Debug, PartialEq)]
pub enum DocumentFilter {
    /// Match all documents.
    All,
    /// Test whether a path exists, including explicit null.
    Exists { path: Vec<String>, exists: bool },
    /// Compare any JSON value with exact serde_json equality.
    Equal { path: Vec<String>, value: Value },
    /// Match a string beginning with the given UTF-8 prefix. An empty prefix
    /// matches every string. The prefix may contain up to 256 bytes.
    StartsWith { path: Vec<String>, prefix: String },
    /// Match a present path equal to any listed JSON value. Empty never matches.
    In {
        path: Vec<String>,
        values: Vec<Value>,
    },
    /// Negate `In`, including for missing paths. Empty matches every document.
    NotIn {
        path: Vec<String>,
        values: Vec<Value>,
    },
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
    /// Match when one object element satisfies `filter` relative to that object.
    /// Missing, null, non-array and empty values never match. Non-object array
    /// elements are skipped, including when the nested filter is negated.
    ArrayElementMatch {
        path: Vec<String>,
        filter: Box<DocumentFilter>,
    },
    /// Match when every array element is an object satisfying the relative
    /// filter. An empty array matches. Missing and non-array paths fail.
    ArrayAllElementsMatch {
        path: Vec<String>,
        filter: Box<DocumentFilter>,
    },
    /// Compare indexable scalars by the documented index ordering.
    Compare {
        path: Vec<String>,
        comparison: Comparison,
        value: IndexValue,
    },
    /// Compare two indexable scalars in the same object using index ordering.
    /// A missing or non-indexable operand never matches.
    CompareFields {
        left: Vec<String>,
        comparison: Comparison,
        right: Vec<String>,
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

impl Comparison {
    fn accepts(self, order: Ordering) -> bool {
        match self {
            Self::Less => order == Ordering::Less,
            Self::LessOrEqual => order != Ordering::Greater,
            Self::Greater => order == Ordering::Greater,
            Self::GreaterOrEqual => order != Ordering::Less,
        }
    }
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
/// `query_planned` can instead use qualifying index probes or a bounded union.
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
    /// Maximum document visits by the access path, between 1 and 1,024.
    /// A visit through each branch of an indexed disjunction counts separately.
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
    /// Documents examined by the selected access path, including repeat visits.
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
struct FilterValueBudget {
    values: usize,
    bytes: usize,
    comparisons: usize,
    array_element_visits: usize,
}

impl Write for FilterValueBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(|| {
            key::invalid("membership filter inputs exceed the encoded byte limit")
        })?;
        if self.bytes > MAX_FILTER_INPUT_BYTES {
            return Err(key::invalid(
                "membership filter inputs exceed the encoded byte limit",
            ));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl FilterValueBudget {
    fn visit_array_element(&mut self) -> io::Result<()> {
        self.array_element_visits = self
            .array_element_visits
            .checked_add(1)
            .ok_or_else(|| key::invalid("array element visit limit exceeded"))?;
        if self.array_element_visits > MAX_FILTER_ARRAY_ELEMENT_VISITS {
            return Err(key::invalid("array element visit limit exceeded"));
        }
        Ok(())
    }

    fn charge_value(&mut self) -> io::Result<()> {
        if self.values == MAX_FILTER_VALUES {
            return Err(key::invalid("membership filter exceeds the value limit"));
        }
        self.values += 1;
        Ok(())
    }

    fn charge_encoded_value(&mut self, value: &impl serde::Serialize) -> io::Result<()> {
        serde_json::to_writer(self, value).map_err(|_| {
            key::invalid(
                "membership filter inputs exceed the encoded byte limit or cannot be encoded",
            )
        })
    }

    fn add_values<'a>(&mut self, values: impl IntoIterator<Item = &'a Value>) -> io::Result<()> {
        for value in values {
            self.charge_value()?;
            schema::validate_tree(value)?;
            self.charge_encoded_value(value)?;
        }
        Ok(())
    }

    fn charge_comparison(&mut self) -> io::Result<()> {
        self.comparisons = self
            .comparisons
            .checked_add(1)
            .ok_or_else(|| key::invalid("membership filter comparison limit exceeded"))?;
        if self.comparisons > MAX_FILTER_COMPARISONS {
            return Err(key::invalid("membership filter comparison limit exceeded"));
        }
        Ok(())
    }

    fn equals(&mut self, left: &Value, right: &Value) -> io::Result<bool> {
        self.charge_comparison()?;
        Ok(left == right)
    }

    fn contains(&mut self, array: &[Value], value: &Value) -> io::Result<bool> {
        for entry in array {
            if self.equals(entry, value)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(super) fn validate_filter(filter: &DocumentFilter) -> io::Result<()> {
    let mut pending = vec![(filter, 0_usize)];
    let mut count = 0_usize;
    let mut value_budget = FilterValueBudget::default();
    while let Some((filter, depth)) = pending.pop() {
        count += 1;
        if count > MAX_FILTER_NODES || depth > 16 || pending.len() + count > MAX_FILTER_NODES {
            return Err(key::invalid("filter exceeds depth or node limit"));
        }
        match filter {
            DocumentFilter::All => {}
            DocumentFilter::Exists { path, .. } => schema::validate_path(path)?,
            DocumentFilter::Equal { path, value } => {
                schema::validate_path(path)?;
                value_budget.add_values(std::iter::once(value))?;
            }
            DocumentFilter::StartsWith { path, prefix } => {
                schema::validate_path(path)?;
                if prefix.len() > MAX_INDEX_STRING_BYTES {
                    return Err(key::invalid("string prefix exceeds 256 bytes"));
                }
                value_budget.charge_value()?;
                value_budget.charge_encoded_value(prefix)?;
            }
            DocumentFilter::In { path, values } | DocumentFilter::NotIn { path, values } => {
                schema::validate_path(path)?;
                value_budget.add_values(values)?;
            }
            DocumentFilter::ArrayContains { path, value } => {
                schema::validate_path(path)?;
                value_budget.add_values(std::iter::once(value))?;
            }
            DocumentFilter::ArrayContainsAny { path, values }
            | DocumentFilter::ArrayContainsAll { path, values } => {
                schema::validate_path(path)?;
                value_budget.add_values(values)?;
            }
            DocumentFilter::ArrayLength { path, .. } => schema::validate_path(path)?,
            DocumentFilter::ArrayElementMatch { path, filter }
            | DocumentFilter::ArrayAllElementsMatch { path, filter } => {
                schema::validate_path(path)?;
                pending.push((filter, depth + 1));
            }
            DocumentFilter::Compare { path, value, .. } => {
                schema::validate_path(path)?;
                value.encode(&mut Vec::new())?;
            }
            DocumentFilter::CompareFields { left, right, .. } => {
                schema::validate_path(left)?;
                schema::validate_path(right)?;
            }
            DocumentFilter::And(filters) | DocumentFilter::Or(filters) => {
                if pending.len() + count + filters.len() > MAX_FILTER_NODES {
                    return Err(key::invalid("filter exceeds node limit"));
                }
                pending.extend(filters.iter().map(|filter| (filter, depth + 1)));
            }
            DocumentFilter::Not(filter) => pending.push((filter, depth + 1)),
        }
    }
    Ok(())
}

// Without an array walk, each candidate can compare at most the validated
// number of predicate values plus one comparison per field-comparison node.
// Even one retry in ID order stays within the comparison budget.
pub(super) fn may_retry_in_id_order(filter: &DocumentFilter) -> bool {
    // An over-limit page can validate two extra rows for a unique probe.
    const {
        assert!(
            2 * (MAX_SCAN_DOCUMENTS + 2) * (MAX_FILTER_VALUES + MAX_FILTER_NODES)
                < MAX_FILTER_COMPARISONS
        )
    };
    match filter {
        DocumentFilter::ArrayContains { .. }
        | DocumentFilter::ArrayContainsAny { .. }
        | DocumentFilter::ArrayContainsAll { .. }
        | DocumentFilter::ArrayElementMatch { .. }
        | DocumentFilter::ArrayAllElementsMatch { .. } => false,
        DocumentFilter::And(children) | DocumentFilter::Or(children) => {
            children.iter().all(may_retry_in_id_order)
        }
        DocumentFilter::Not(child) => may_retry_in_id_order(child),
        DocumentFilter::All
        | DocumentFilter::Exists { .. }
        | DocumentFilter::Equal { .. }
        | DocumentFilter::StartsWith { .. }
        | DocumentFilter::In { .. }
        | DocumentFilter::NotIn { .. }
        | DocumentFilter::ArrayLength { .. }
        | DocumentFilter::Compare { .. }
        | DocumentFilter::CompareFields { .. } => true,
    }
}

fn encoded(value: &IndexValue) -> io::Result<Vec<u8>> {
    let mut result = Vec::new();
    value.encode(&mut result)?;
    Ok(result)
}

fn matches_array_elements(
    field: Option<&Value>,
    filter: &DocumentFilter,
    budget: &mut FilterValueBudget,
    require_all: bool,
) -> io::Result<bool> {
    let Some(array) = field.and_then(Value::as_array) else {
        return Ok(false);
    };
    for element in array {
        budget.visit_array_element()?;
        let matched = element.is_object() && matches(filter, element, budget)?;
        if matched != require_all {
            return Ok(matched);
        }
    }
    Ok(require_all)
}

fn matches(
    filter: &DocumentFilter,
    document: &Value,
    value_budget: &mut FilterValueBudget,
) -> io::Result<bool> {
    Ok(match filter {
        DocumentFilter::All => true,
        DocumentFilter::Exists { path, exists } => {
            schema::field(document, path).is_some() == *exists
        }
        DocumentFilter::Equal { path, value } => match schema::field(document, path) {
            Some(actual) => value_budget.equals(actual, value)?,
            None => false,
        },
        DocumentFilter::StartsWith { path, prefix } => {
            if let Some(actual) = schema::field(document, path).and_then(Value::as_str) {
                value_budget.charge_comparison()?;
                actual.starts_with(prefix)
            } else {
                false
            }
        }
        DocumentFilter::In { path, values } => match schema::field(document, path) {
            Some(value) => value_budget.contains(values, value)?,
            None => false,
        },
        DocumentFilter::NotIn { path, values } => match schema::field(document, path) {
            Some(value) => !value_budget.contains(values, value)?,
            None => true,
        },
        DocumentFilter::ArrayContains { path, value } => {
            match schema::field(document, path).and_then(Value::as_array) {
                Some(array) => value_budget.contains(array, value)?,
                None => false,
            }
        }
        DocumentFilter::ArrayContainsAny { path, values } => {
            let mut found = false;
            if let Some(array) = schema::field(document, path).and_then(Value::as_array) {
                for value in values {
                    if value_budget.contains(array, value)? {
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
                        if !value_budget.contains(array, value)? {
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
        DocumentFilter::ArrayElementMatch { path, filter } => {
            matches_array_elements(schema::field(document, path), filter, value_budget, false)?
        }
        DocumentFilter::ArrayAllElementsMatch { path, filter } => {
            matches_array_elements(schema::field(document, path), filter, value_budget, true)?
        }
        DocumentFilter::Compare {
            path,
            comparison,
            value,
        } => schema::field(document, path)
            .and_then(|actual| value.compare_json(actual))
            .is_some_and(|order| comparison.accepts(order)),
        DocumentFilter::CompareFields {
            left,
            comparison,
            right,
        } => {
            let order = schema::field(document, left).and_then(|left| {
                let right = IndexValue::from_json(schema::field(document, right)?).ok()?;
                right.compare_json(left)
            });
            if let Some(order) = order {
                value_budget.charge_comparison()?;
                comparison.accepts(order)
            } else {
                false
            }
        }
        DocumentFilter::And(filters) => {
            let mut matched = true;
            for filter in filters {
                if !matches(filter, document, value_budget)? {
                    matched = false;
                    break;
                }
            }
            matched
        }
        DocumentFilter::Or(filters) => {
            let mut matched = false;
            for filter in filters {
                if matches(filter, document, value_budget)? {
                    matched = true;
                    break;
                }
            }
            matched
        }
        DocumentFilter::Not(filter) => !matches(filter, document, value_budget)?,
    })
}

fn project(mut document: Value, paths: &[Vec<String>]) -> Value {
    let mut result = Value::Object(Map::new());
    for path in paths {
        let (field, parents) = path.split_last().expect("validated path");
        let parent = parents.iter().try_fold(&mut document, |value, component| {
            value.as_object_mut()?.get_mut(component)
        });
        let Some(value) = parent
            .and_then(Value::as_object_mut)
            .and_then(|object| object.remove(field))
        else {
            continue;
        };
        let mut current = &mut result;
        for component in parents {
            let object = current.as_object_mut().expect("projection objects");
            current = object
                .entry(component.clone())
                .or_insert_with(|| Value::Object(Map::new()));
        }
        current
            .as_object_mut()
            .expect("non-overlapping projection paths")
            .insert(field.clone(), value);
    }
    result
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

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum SortKey {
    Ascending(Vec<u8>),
    Descending(Reverse<Vec<u8>>),
}

pub(super) struct QueryCollector<'a> {
    query: &'a DocumentQuery,
    id_order: bool,
    scanned: usize,
    matched_count: usize,
    matched: Vec<DocumentEntry>,
    // IDs are unique in both collection scans and deduplicated index queries.
    sorted: BTreeMap<(SortKey, String), Value>,
    window_size: usize,
    value_budget: FilterValueBudget,
    error: Option<io::Error>,
}

impl<'a> QueryCollector<'a> {
    pub(super) fn new(query: &'a DocumentQuery, id_order: bool) -> Self {
        Self {
            query,
            id_order,
            scanned: 0,
            matched_count: 0,
            matched: Vec::new(),
            sorted: BTreeMap::new(),
            window_size: if query.offset < query.max_scanned {
                query
                    .offset
                    .saturating_add(query.limit)
                    .min(query.max_scanned)
            } else {
                0
            },
            value_budget: FilterValueBudget::default(),
            error: None,
        }
    }

    pub(super) fn push(&mut self, id: String, document: Value) {
        self.scanned += 1;
        if self.error.is_some() {
            return;
        }
        // Defer a filter/sort error until the complete scan has checked all
        // document records and limits, preserving their previous precedence.
        let result = (|| -> io::Result<()> {
            if !matches(&self.query.filter, &document, &mut self.value_budget)? {
                return Ok(());
            }
            let position = self.matched_count;
            self.matched_count += 1;
            if self.id_order && self.query.sort.is_none() {
                if position >= self.query.offset && position - self.query.offset < self.query.limit
                {
                    let document = self.result_document(document);
                    self.matched.push(DocumentEntry { id, document });
                }
                return Ok(());
            }
            let sort_key = self
                .query
                .sort
                .as_ref()
                .and_then(|sort| schema::field(&document, &sort.path))
                .map(|value| encoded(&IndexValue::from_json(value)?))
                .transpose()?
                .unwrap_or_default();
            // Validate every match before deciding whether it belongs in the
            // window. Reversing only the scalar key leaves ID ties ascending.
            let rank = if self.query.sort.as_ref().is_some_and(|sort| sort.descending) {
                SortKey::Descending(Reverse(sort_key))
            } else {
                SortKey::Ascending(sort_key)
            };
            let key = (rank, id);
            if self.window_size == 0 {
                return Ok(());
            }
            if self.sorted.len() == self.window_size {
                let worst = self.sorted.last_entry().expect("nonempty sort window");
                if &key >= worst.key() {
                    return Ok(());
                }
                worst.remove();
            }
            let document = self.result_document(document);
            self.sorted.insert(key, document);
            Ok(())
        })();
        if let Err(error) = result {
            self.error = Some(error);
        }
    }

    pub(super) fn has_failed(&self) -> bool {
        self.error.is_some()
    }

    fn result_document(&self, document: Value) -> Value {
        match &self.query.projection {
            Some(paths) => project(document, paths),
            None => document,
        }
    }

    pub(super) fn finish(self) -> io::Result<DocumentQueryResult> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let documents = if !self.id_order || self.query.sort.is_some() {
            self.sorted
                .into_iter()
                .skip(self.query.offset)
                .take(self.query.limit)
                .map(|((_, id), document)| DocumentEntry { id, document })
                .collect::<Vec<_>>()
        } else {
            self.matched
        };
        let plan = QueryPlan {
            scanned: self.scanned,
            matched: self.matched_count,
            sorted: self.query.sort.is_some(),
            returned: documents.len(),
        };
        Ok(DocumentQueryResult { documents, plan })
    }
}

// Stream all matches while retaining query error precedence: scan and
// record-validation errors come first, then filter errors, then the first
// callback error. A callback error must not hide a later filter error.
pub(super) struct FilteredVisitor<'a, F> {
    filter: &'a DocumentFilter,
    budget: FilterValueBudget,
    matched: usize,
    filter_error: Option<io::Error>,
    callback_error: Option<io::Error>,
    visit: F,
}

impl<'a, F: FnMut(&Value) -> io::Result<()>> FilteredVisitor<'a, F> {
    pub(super) fn new(filter: &'a DocumentFilter, visit: F) -> Self {
        Self {
            filter,
            budget: FilterValueBudget::default(),
            matched: 0,
            filter_error: None,
            callback_error: None,
            visit,
        }
    }

    pub(super) fn push(&mut self, document: &Value) {
        if self.filter_error.is_some() {
            return;
        }
        match matches(self.filter, document, &mut self.budget) {
            Ok(true) => {
                self.matched += 1;
                if self.callback_error.is_none() {
                    self.callback_error = (self.visit)(document).err();
                }
            }
            Ok(false) => {}
            Err(error) => self.filter_error = Some(error),
        }
    }

    pub(super) fn has_failed(&self) -> bool {
        self.filter_error.is_some() || self.callback_error.is_some()
    }

    pub(super) fn finish(self, scanned: usize) -> io::Result<QueryPlan> {
        if let Some(error) = self.filter_error.or(self.callback_error) {
            return Err(error);
        }
        Ok(QueryPlan {
            scanned,
            matched: self.matched,
            sorted: false,
            returned: self.matched,
        })
    }
}

impl DocumentTransaction<'_, '_> {
    pub(super) fn visit_filtered_documents(
        &self,
        collection: &str,
        filter: &DocumentFilter,
        max_scanned: usize,
        visit: impl FnMut(&Value) -> io::Result<()>,
    ) -> io::Result<QueryPlan> {
        if max_scanned == 0 || max_scanned > MAX_SCAN_DOCUMENTS {
            return Err(key::invalid("query scan limit must be between 1 and 1024"));
        }
        validate_filter(filter)?;
        let metadata = self.metadata(collection)?;
        self.visit_filtered_documents_with_schema(
            collection,
            &metadata.schema,
            filter,
            max_scanned,
            visit,
        )
    }

    // The planner has already validated the filter and obtained the schema.
    pub(super) fn visit_filtered_documents_with_schema(
        &self,
        collection: &str,
        schema: &schema::CollectionSchema,
        filter: &DocumentFilter,
        max_scanned: usize,
        visit: impl FnMut(&Value) -> io::Result<()>,
    ) -> io::Result<QueryPlan> {
        let mut visitor = FilteredVisitor::new(filter, visit);
        let mut scanned = 0;
        self.visit_document_rows(
            collection,
            schema,
            max_scanned,
            MAX_SCAN_BYTES,
            |_, document| {
                scanned += 1;
                visitor.push(&document);
            },
        )?;
        visitor.finish(scanned)
    }

    /// Executes a complete bounded scan with filter, sort, pagination and
    /// optional projection. Statistics describe the work actually performed.
    pub fn query(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<DocumentQueryResult> {
        validate_query(query)?;
        let metadata = self.metadata(collection)?;
        self.query_with_schema(collection, &metadata.schema, query)
    }

    pub(super) fn query_with_schema(
        &self,
        collection: &str,
        schema: &schema::CollectionSchema,
        query: &DocumentQuery,
    ) -> io::Result<DocumentQueryResult> {
        let mut collector = QueryCollector::new(query, true);
        self.visit_document_rows(
            collection,
            schema,
            query.max_scanned,
            MAX_SCAN_BYTES,
            |id, document| collector.push(id, document),
        )?;
        collector.finish()
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unsorted_result_window_does_not_retain_other_matching_documents() {
        let query = DocumentQuery {
            offset: 80,
            limit: 3,
            ..Default::default()
        };
        let mut collector = QueryCollector::new(&query, true);
        for index in 0..100 {
            collector.push(format!("id{index:03}"), json!({"value":index}));
        }
        // The resource bound is independent of the complete match count.
        assert_eq!(collector.matched.len(), 3);
        let result = collector.finish().unwrap();
        assert_eq!(result.plan.scanned, 100);
        assert_eq!(result.plan.matched, 100);
        assert_eq!(result.plan.returned, 3);
        assert_eq!(
            result
                .documents
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["id080", "id081", "id082"]
        );
    }

    #[test]
    fn ranked_window_keeps_only_selected_fields_including_nested_siblings() {
        let query = DocumentQuery {
            sort: Some(DocumentSort {
                path: vec!["rank".into()],
                descending: true,
            }),
            projection: Some(vec![
                vec!["nested".into(), "empty".into()],
                vec!["nested".into(), "value".into()],
                vec!["nested".into(), "missing".into()],
            ]),
            limit: 1,
            ..Default::default()
        };
        validate_query(&query).unwrap();
        let mut collector = QueryCollector::new(&query, true);
        collector.push("a".into(), json!({"rank":0, "nested":{"value":0}}));
        collector.push(
            "b".into(),
            json!({
                "rank":1, "nested":{"empty":null,"value":[1,2],"unused":true}, "payload":[4,5]
            }),
        );
        let expected = json!({"nested":{"empty":null,"value":[1,2]}});
        assert_eq!(collector.sorted.values().next(), Some(&expected));
        let rows = collector.finish().unwrap().documents;
        assert_eq!(rows[0].id, "b");
        assert_eq!(rows[0].document, expected);
    }

    #[test]
    fn sorted_window_matches_full_sort_and_checks_discarded_matches() {
        let rows: Vec<_> = (0..100)
            .rev()
            .map(|n| {
                (
                    format!("id{n:03}"),
                    match n % 7 {
                        0 => None,
                        1 => Some(None),
                        score => Some(Some(score as i64 - 4)),
                    },
                )
            })
            .collect();
        for descending in [false, true] {
            let mut expected = rows.clone();
            expected.sort_by(|left, right| {
                let scalar = left.1.cmp(&right.1);
                (if descending { scalar.reverse() } else { scalar })
                    .then_with(|| left.0.cmp(&right.0))
            });
            for offset in [0, 3, 98, 100] {
                let query = DocumentQuery {
                    sort: Some(DocumentSort {
                        path: vec!["score".into()],
                        descending,
                    }),
                    offset,
                    limit: 5,
                    max_scanned: rows.len(),
                    ..Default::default()
                };
                let mut collector = QueryCollector::new(&query, true);
                for (id, score) in &rows {
                    let document = match score {
                        None => json!({}),
                        Some(score) => json!({"score":score}),
                    };
                    collector.push(id.clone(), document);
                    assert!(
                        collector.sorted.len() <= if offset == rows.len() { 0 } else { offset + 5 }
                    );
                }
                let result = collector.finish().unwrap();
                assert_eq!(result.plan.matched, rows.len());
                let actual = result.documents.iter().map(|row| row.id.as_str());
                let expected = expected
                    .iter()
                    .skip(offset)
                    .take(5)
                    .map(|row| row.0.as_str());
                assert!(actual.eq(expected));
                // Even an offset that guarantees no output must validate sort values.
                let mut invalid = QueryCollector::new(&query, true);
                invalid.push("a".into(), json!({"score":0}));
                invalid.push("z".into(), json!({"score":[]}));
                assert_eq!(
                    invalid.finish().unwrap_err().kind(),
                    io::ErrorKind::InvalidInput
                );
            }
        }
    }
}
