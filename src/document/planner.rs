//! Conservative index selection for bounded queries.
use super::{
    filter::{self, Comparison, DocumentFilter, DocumentQuery, DocumentQueryResult},
    index::{IndexDefinition, IndexValue, PartialFilter},
    key,
    query::{IndexBound, IndexQuery},
    transaction::{DocumentTransaction, Metadata},
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::io;

// Bound the Cartesian product before allocating tuple probes.
const MAX_INDEX_PROBES: usize = 256;

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
    /// Probe a single-field index for each distinct value of a required `In`.
    MembershipIndex { index: String, probes: usize },
    /// Probe an implied partial single-field index for required `In` values.
    PartialMembershipIndex { index: String, probes: usize },
    /// Probe a compound index for complete required equality/membership tuples.
    CompoundMembershipIndex {
        index: String,
        fields: usize,
        probes: usize,
    },
    /// Probe an implied partial compound index for complete required tuples.
    PartialCompoundMembershipIndex {
        index: String,
        fields: usize,
        probes: usize,
    },
    /// Union the indexed candidates of every branch of a required disjunction.
    DisjunctionIndex {
        /// Distinct index names in ascending order.
        indexes: Vec<String>,
        /// Total equality or range probes across all branches.
        probes: usize,
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

// Each supplied filter must hold for the current plan. Within it, descend
// only through And: Or and Not do not require any particular child.
fn required_equality(required: &[&DocumentFilter], path: &[String]) -> Option<IndexValue> {
    let mut pending = required.to_vec();
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

// Select one required, wholly indexable list. Deduplication uses the exact
// scalar index encoding so repeated values cannot duplicate rows or work.
// Non-indexable lists fall back rather than omitting potential matches.
fn required_membership(required: &[&DocumentFilter], path: &[String]) -> Option<Vec<IndexValue>> {
    let mut pending = required.to_vec();
    while let Some(filter) = pending.pop() {
        match filter {
            DocumentFilter::In {
                path: field,
                values,
            } if field == path && values.len() > 1 => {
                let mut distinct = BTreeMap::new();
                let mut usable = true;
                for value in values {
                    let Ok(value) = IndexValue::from_json(value) else {
                        usable = false;
                        break;
                    };
                    let mut encoded = Vec::new();
                    if value.encode(&mut encoded).is_err() {
                        usable = false;
                        break;
                    }
                    distinct.entry(encoded).or_insert(value);
                }
                if usable {
                    return Some(distinct.into_values().collect());
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
// An Or proves the condition only if every nonempty branch proves it.
// Not and nested array-element filters do not establish root-path presence.
fn required_presence(required: &[&DocumentFilter], path: &[String], exists: bool) -> bool {
    let mut pending = required.to_vec();
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
            | DocumentFilter::ArrayElementMatch { path: field, .. }
                if exists && field == path =>
            {
                return true;
            }
            DocumentFilter::And(filters) => pending.extend(filters.iter().rev()),
            DocumentFilter::Or(filters)
                if !filters.is_empty()
                    && filters
                        .iter()
                        .all(|branch| required_presence(&[branch], path, exists)) =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

fn partial_is_implied(required: &[&DocumentFilter], partial: &PartialFilter) -> bool {
    match partial {
        PartialFilter::Exists { path, exists } => required_presence(required, path, *exists),
        PartialFilter::Equals { path, value } => {
            required_equality(required, path).as_ref() == Some(value)
        }
    }
}

fn eligible(index: &IndexDefinition, required: &[&DocumentFilter]) -> bool {
    index
        .partial
        .as_ref()
        .is_none_or(|partial| partial_is_implied(required, partial))
}

enum Selection {
    Equality(Vec<IndexValue>),
    Membership(Vec<IndexValue>),
    CompoundMembership(Vec<Vec<IndexValue>>),
    Range {
        equal: Vec<IndexValue>,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
    },
}

impl Selection {
    fn probe_count(&self) -> usize {
        match self {
            Self::Membership(values) => values.len(),
            Self::CompoundMembership(tuples) => tuples.len(),
            _ => 1,
        }
    }
}

// A compound entry omits any document missing one of its fields. Require an
// indexable equality or membership predicate for every field; never infer one
// from Or or Not. Distinct values make tuple probes disjoint. Reject an
// excessive product before building it, then let the caller use a safe scan.
fn compound_membership(
    required: &[&DocumentFilter],
    index: &IndexDefinition,
) -> Option<Vec<Vec<IndexValue>>> {
    let mut choices = Vec::with_capacity(index.fields.len());
    let mut probe_count = 1_usize;
    let mut has_membership = false;
    for field in &index.fields {
        let values = if let Some(value) = required_equality(required, field) {
            vec![value]
        } else {
            has_membership = true;
            required_membership(required, field)?
        };
        probe_count = probe_count.checked_mul(values.len())?;
        if probe_count > MAX_INDEX_PROBES {
            return None;
        }
        choices.push(values);
    }
    if !has_membership {
        return None;
    }
    let mut tuples = vec![Vec::new()];
    for values in choices {
        let mut next = Vec::with_capacity(tuples.len() * values.len());
        for tuple in tuples {
            for value in &values {
                let mut next_tuple = tuple.clone();
                next_tuple.push(value.clone());
                next.push(next_tuple);
            }
        }
        tuples = next;
    }
    Some(tuples)
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
    required: &[&DocumentFilter],
    path: &[String],
) -> io::Result<Option<(Option<IndexBound>, Option<IndexBound>)>> {
    let mut lower = None;
    let mut upper = None;
    let mut pending = required.to_vec();
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
    required: &[&DocumentFilter],
    indexes: &'a [IndexDefinition],
) -> io::Result<Option<(&'a IndexDefinition, Selection)>> {
    // Prefer a unique exact tuple, then a nonunique one, regardless of range.
    // Complete tuples require equality on every field, so omitted missing
    // fields cannot produce an incorrect result.
    if let Some(selected) = indexes
        .iter()
        .filter(|index| index.unique)
        .chain(indexes.iter().filter(|index| !index.unique))
        .filter(|index| eligible(index, required))
        .find_map(|index| {
            index
                .fields
                .iter()
                .map(|field| required_equality(required, field))
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
        .filter(|index| eligible(index, required))
    {
        let mut equal = Vec::new();
        for (position, field) in index.fields.iter().enumerate() {
            if let Some(value) = required_equality(required, field) {
                equal.push(value);
                continue;
            }
            // Compound entries omit a document if any indexed field is
            // missing. The required range proves this field is present;
            // every later field must be independently required as well.
            if index.fields[position + 1..]
                .iter()
                .all(|later| required_presence(required, later, true))
            {
                if let Some((lower, upper)) = required_range(required, field)? {
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
    // Preserve existing equality and range choices. A union is safe for a
    // single field because every matching present scalar has exactly one
    // index entry; a partial index additionally requires its guard to follow
    // from the full filter.
    if let Some(selected) = indexes
        .iter()
        .filter(|index| index.unique)
        .chain(indexes.iter().filter(|index| !index.unique))
        .filter(|index| index.fields.len() == 1 && eligible(index, required))
        .find_map(|index| {
            required_membership(required, &index.fields[0])
                .map(|values| (index, Selection::Membership(values)))
        })
    {
        return Ok(Some(selected));
    }
    // Complete compound tuples can also be probed as a bounded union. Keep
    // existing exact, range and single-field membership choices first.
    if let Some(selected) = indexes
        .iter()
        .filter(|index| index.unique)
        .chain(indexes.iter().filter(|index| !index.unique))
        .filter(|index| index.fields.len() > 1 && eligible(index, required))
        .find_map(|index| {
            compound_membership(required, index)
                .map(|tuples| (index, Selection::CompoundMembership(tuples)))
        })
    {
        return Ok(Some(selected));
    }
    Ok(None)
}

type Branch<'a> = (&'a IndexDefinition, Selection);

// Find a required OR under zero or more ANDs. The original filter and
// each selected ancestor branch remain required within a branch. Sibling
// alternatives are never added to that context.
fn choose_disjunction<'a, 'filter>(
    filter: &'filter DocumentFilter,
    context: &mut Vec<&'filter DocumentFilter>,
    indexes: &'a [IndexDefinition],
    branches: &mut Vec<Branch<'a>>,
    remaining: &mut usize,
) -> io::Result<bool> {
    let mut required = vec![filter];
    while let Some(filter) = required.pop() {
        match filter {
            DocumentFilter::And(filters) => required.extend(filters.iter().rev()),
            DocumentFilter::Or(filters) if !filters.is_empty() => {
                let start = branches.len();
                let available = *remaining;
                let mut complete = true;
                for branch in filters {
                    context.push(branch);
                    let planned = match choose_index(context, indexes)? {
                        Some((index, selection)) if selection.probe_count() <= *remaining => {
                            *remaining -= selection.probe_count();
                            branches.push((index, selection));
                            true
                        }
                        _ => choose_disjunction(branch, context, indexes, branches, remaining)?,
                    };
                    context.pop();
                    if !planned {
                        complete = false;
                        break;
                    }
                }
                if complete {
                    return Ok(true);
                }
                branches.truncate(start);
                *remaining = available;
            }
            _ => {}
        }
    }
    Ok(false)
}

#[derive(Default)]
struct Candidates {
    rows: BTreeMap<String, serde_json::Value>,
    visits: usize,
    bytes: usize,
}

impl DocumentTransaction<'_, '_> {
    /// Executes a bounded filtered query using a qualifying exact equality
    /// tuple, scalar range, bounded membership union or a union of indexed
    /// disjuncts. Falls back to the collection scan when none qualifies.
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
        let mut branches = Vec::new();
        let access =
            if let Some((index, selection)) = choose_index(&[&query.filter], &metadata.indexes)? {
                let access = describe_selection(index, &selection);
                branches.push((index, selection));
                access
            } else {
                let mut remaining = MAX_INDEX_PROBES;
                if !choose_disjunction(
                    &query.filter,
                    &mut vec![&query.filter],
                    &metadata.indexes,
                    &mut branches,
                    &mut remaining,
                )? {
                    return Ok(PlannedDocumentQueryResult {
                        result: self.query(collection, query)?,
                        access: DocumentQueryAccess::CollectionScan,
                    });
                }
                DocumentQueryAccess::DisjunctionIndex {
                    indexes: branches
                        .iter()
                        .map(|(index, _)| index.name.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                    probes: MAX_INDEX_PROBES - remaining,
                }
            };

        let mut candidates = Candidates::default();
        for (index, selection) in branches {
            self.collect_index_candidates(
                collection,
                &metadata,
                index,
                selection,
                query.max_scanned,
                &mut candidates,
            )?;
        }
        let mut result = filter::finish_query(candidates.rows, query)?;
        result.plan.scanned = candidates.visits;
        Ok(PlannedDocumentQueryResult { result, access })
    }

    fn collect_index_candidates(
        &self,
        collection: &str,
        metadata: &Metadata,
        index: &IndexDefinition,
        selection: Selection,
        max_scanned: usize,
        candidates: &mut Candidates,
    ) -> io::Result<()> {
        let unique_equality = index.unique && !matches!(selection, Selection::Range { .. });
        let probes = match selection {
            Selection::Equality(values) => vec![(values, None, None)],
            Selection::Membership(values) => values
                .into_iter()
                .map(|value| (vec![value], None, None))
                .collect(),
            Selection::CompoundMembership(tuples) => tuples
                .into_iter()
                .map(|tuple| (tuple, None, None))
                .collect(),
            Selection::Range {
                equal,
                lower,
                upper,
            } => vec![(equal, lower, upper)],
        };
        for (equal, lower, upper) in probes {
            let mut after = None;
            loop {
                // Probe one entry beyond the candidate limit, never silently
                // returning a prefix. Unique equality probes two to detect a
                // corrupt duplicate rather than accepting it as one result.
                let limit = if unique_equality {
                    2
                } else {
                    (max_scanned - candidates.visits + 1).min(256)
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
                    metadata,
                )?;
                let page = indexed.page;
                if unique_equality && page.documents.len() > 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unique index contains multiple matches",
                    ));
                }
                candidates.bytes = candidates
                    .bytes
                    .checked_add(indexed.document_key_value_bytes)
                    .ok_or_else(|| key::invalid("scan byte count overflow"))?;
                if candidates.bytes > filter::MAX_SCAN_BYTES
                    || page.documents.len() > max_scanned - candidates.visits
                {
                    return Err(key::invalid("document scan exceeds the operation limit"));
                }
                if page.documents.is_empty() {
                    break;
                }
                // Count every visit, but retain one document per ID in
                // collection order for filtering and pagination.
                candidates.visits += page.documents.len();
                for entry in page.documents {
                    candidates.rows.entry(entry.id).or_insert(entry.document);
                }
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
        }
        Ok(())
    }
}

fn describe_selection(index: &IndexDefinition, selection: &Selection) -> DocumentQueryAccess {
    let partial = index.partial.is_some();
    match selection {
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
        Selection::Membership(values) => {
            if partial {
                DocumentQueryAccess::PartialMembershipIndex {
                    index: index.name.clone(),
                    probes: values.len(),
                }
            } else {
                DocumentQueryAccess::MembershipIndex {
                    index: index.name.clone(),
                    probes: values.len(),
                }
            }
        }
        Selection::CompoundMembership(tuples) => {
            if partial {
                DocumentQueryAccess::PartialCompoundMembershipIndex {
                    index: index.name.clone(),
                    fields: index.fields.len(),
                    probes: tuples.len(),
                }
            } else {
                DocumentQueryAccess::CompoundMembershipIndex {
                    index: index.name.clone(),
                    fields: index.fields.len(),
                    probes: tuples.len(),
                }
            }
        }
        Selection::Range { equal, .. } => {
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
    }
}
