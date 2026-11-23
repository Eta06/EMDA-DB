//! Conservative index selection for bounded queries.
use super::{
    filter::{self, Comparison, DocumentFilter, DocumentQuery, DocumentQueryResult},
    index::{IndexDefinition, IndexValue, PartialFilter},
    key,
    query::{IndexBound, IndexQuery, IndexScanOptions, index_definition},
    transaction::{DocumentTransaction, Metadata},
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, hash_map::Entry};
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
    /// Scan a compound range for each distinct equality/membership prefix.
    CompoundMembershipRangeIndex {
        index: String,
        prefix_fields: usize,
        probes: usize,
    },
    /// Scan an implied partial compound range for each distinct prefix.
    PartialCompoundMembershipRangeIndex {
        index: String,
        prefix_fields: usize,
        probes: usize,
    },
    /// Probe consecutive integer ranges and individual values from a required
    /// membership list intersected with scalar bounds.
    IntegerMembershipIndex {
        index: String,
        prefix_fields: usize,
        probes: usize,
        partial: bool,
    },
    /// Scan a compound index for distinct equality or membership prefixes.
    CompoundPrefixIndex {
        index: String,
        prefix_fields: usize,
        probes: usize,
        partial: bool,
    },
    /// Scan an index whose fields are all required to be present.
    PresenceIndex { index: String, partial: bool },
    /// Union the indexed candidates of every branch of a required disjunction.
    DisjunctionIndex {
        /// Distinct index names in ascending order.
        indexes: Vec<String>,
        /// Distinct equality and range probes across all branches.
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

type Predicate<'a> = (&'a DocumentFilter, bool);

// Each supplied predicate must hold with the indicated polarity. Descend
// through positive And and negated Or without entering alternatives.
fn required_predicates<'a>(
    required: &[Predicate<'a>],
) -> impl Iterator<Item = Predicate<'a>> + use<'a> {
    let mut pending = required.to_vec();
    std::iter::from_fn(move || {
        while let Some((filter, negated)) = pending.pop() {
            match (filter, negated) {
                (DocumentFilter::Not(child), _) => pending.push((child, !negated)),
                (DocumentFilter::And(children), false) | (DocumentFilter::Or(children), true) => {
                    pending.extend(children.iter().rev().map(|child| (child, negated)));
                }
                _ => return Some((filter, negated)),
            }
        }
        None
    })
}

fn required_equality(
    required: &[Predicate<'_>],
    path: &[String],
) -> io::Result<Option<IndexValue>> {
    let (mut lower, mut upper) = (None, None);
    for (filter, negated) in required_predicates(required) {
        match (filter, negated) {
            (DocumentFilter::Equal { path: field, value }, false) if field == path => {
                if let Ok(value) = IndexValue::from_json(value) {
                    return Ok(Some(value));
                }
            }
            (
                DocumentFilter::In {
                    path: field,
                    values,
                },
                false,
            )
            | (
                DocumentFilter::NotIn {
                    path: field,
                    values,
                },
                true,
            ) if field == path && values.len() == 1 => {
                if let Ok(value) = IndexValue::from_json(&values[0]) {
                    return Ok(Some(value));
                }
            }
            (
                DocumentFilter::Compare {
                    path: field,
                    comparison,
                    value,
                },
                false,
            ) if field == path => tighten_range(&mut lower, &mut upper, value, *comparison)?,
            _ => {}
        }
    }
    match (lower, upper) {
        (Some((lower, _)), Some((upper, _)))
            if lower.inclusive && upper.inclusive && lower.value == upper.value =>
        {
            Ok(Some(lower.value))
        }
        _ => Ok(None),
    }
}

// Start from a required, wholly indexable list. Once its alternatives cover
// every possible match, further required lists can prune them even if those
// lists also contain non-indexable values.
fn required_membership(
    required: &[Predicate<'_>],
    path: &[String],
    accepts: impl Fn(&serde_json::Value) -> bool,
) -> Option<Vec<IndexValue>> {
    let mut lists = required_predicates(required)
        .filter_map(|(filter, negated)| match (filter, negated) {
            (
                DocumentFilter::In {
                    path: field,
                    values,
                },
                false,
            )
            | (
                DocumentFilter::NotIn {
                    path: field,
                    values,
                },
                true,
            ) if field == path && values.len() > 1 => Some(values),
            _ => None,
        })
        .enumerate();
    let mut mixed = Vec::new();
    let mut intersection = BTreeMap::<Vec<u8>, (IndexValue, usize)>::new();
    let mut encoded = Vec::new();
    loop {
        let (ordinal, values) = lists.next()?;
        let indexable = values.iter().filter(|value| accepts(value)).all(|value| {
            let Ok(value) = IndexValue::from_json(value) else {
                return false;
            };
            encoded.clear();
            if value.encode(&mut encoded).is_err() {
                return false;
            }
            intersection
                .entry(std::mem::take(&mut encoded))
                .or_insert((value, ordinal));
            true
        });
        if indexable && !intersection.is_empty() {
            break;
        }
        intersection.clear();
        if !indexable {
            mixed.push((ordinal, values));
        }
    }
    for (ordinal, values) in mixed.into_iter().chain(lists) {
        let mut matched = 0;
        for value in values.iter().filter(|value| accepts(value)) {
            let Ok(value) = IndexValue::from_json(value) else {
                continue;
            };
            encoded.clear();
            if value.encode(&mut encoded).is_err() {
                continue;
            }
            if let Some((_, seen)) = intersection.get_mut(encoded.as_slice()) {
                if *seen != ordinal {
                    *seen = ordinal;
                    matched += 1;
                }
            }
        }
        // Keep a usable access path if no alternatives intersect. The full
        // filter still checks all requirements against each visited row.
        if matched != 0 && matched < intersection.len() {
            intersection.retain(|_, (_, seen)| *seen == ordinal);
        }
    }
    Some(intersection.into_values().map(|(value, _)| value).collect())
}

fn path_implies_presence(field: &[String], actual: bool, path: &[String], exists: bool) -> bool {
    match (actual, exists) {
        // Field traversal visits each parent object before reaching the child.
        (true, true) => field.starts_with(path),
        (false, false) => path.starts_with(field),
        _ => false,
    }
}

// Query validation bounds recursion to 16 levels. Under negation, And and Or
// swap roles. A conjunction needs one proof and a disjunction needs every
// branch to prove the condition. Empty disjunctions are left to the filter.
fn required_presence(required: &[Predicate<'_>], path: &[String], exists: bool) -> bool {
    required
        .iter()
        .any(|&(filter, negated)| proves_presence(filter, path, exists, negated, false))
}

fn proves_presence(
    filter: &DocumentFilter,
    path: &[String],
    exists: bool,
    negated: bool,
    explicit_only: bool,
) -> bool {
    let proves = |child| proves_presence(child, path, exists, negated, explicit_only);
    match filter {
        DocumentFilter::Not(child) => proves_presence(child, path, exists, !negated, explicit_only),
        DocumentFilter::And(children) if !negated => children.iter().any(proves),
        DocumentFilter::Or(children) if negated => children.iter().any(proves),
        DocumentFilter::And(children) | DocumentFilter::Or(children) => {
            !children.is_empty() && children.iter().all(proves)
        }
        DocumentFilter::Exists {
            path: field,
            exists: actual,
        } => path_implies_presence(field, *actual != negated, path, exists),
        DocumentFilter::NotIn {
            path: field,
            values,
        } if !explicit_only && negated && !values.is_empty() => {
            path_implies_presence(field, true, path, exists)
        }
        DocumentFilter::Equal { path: field, .. }
        | DocumentFilter::In { path: field, .. }
        | DocumentFilter::Compare { path: field, .. }
        | DocumentFilter::ArrayContains { path: field, .. }
        | DocumentFilter::ArrayContainsAny { path: field, .. }
        | DocumentFilter::ArrayContainsAll { path: field, .. }
        | DocumentFilter::ArrayLength { path: field, .. }
        | DocumentFilter::ArrayElementMatch { path: field, .. }
        | DocumentFilter::ArrayAllElementsMatch { path: field, .. }
            if !explicit_only && !negated =>
        {
            path_implies_presence(field, true, path, exists)
        }
        _ => false,
    }
}

// A typed equality follows when the filter requires both sides of the same
// scalar bound. Comparisons use the same type-sensitive order as index keys.
fn requires_guard_bound(
    filter: &DocumentFilter,
    path: &[String],
    expected: &IndexValue,
    lower: bool,
    negated: bool,
) -> bool {
    let within = |value| {
        expected.compare_json(value).is_some_and(|order| {
            order
                != if lower {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
        })
    };
    let proves = |child| requires_guard_bound(child, path, expected, lower, negated);
    match filter {
        DocumentFilter::Not(child) => requires_guard_bound(child, path, expected, lower, !negated),
        DocumentFilter::And(children) if !negated => children.iter().any(proves),
        DocumentFilter::Or(children) if negated => children.iter().any(proves),
        DocumentFilter::And(children) | DocumentFilter::Or(children) => {
            !children.is_empty() && children.iter().all(proves)
        }
        DocumentFilter::Equal { path: field, value } if !negated && field == path => within(value),
        DocumentFilter::In {
            path: field,
            values,
        } if !negated && field == path => !values.is_empty() && values.iter().all(within),
        // NotIn includes missing fields, so its negation requires a present
        // field in the list. A negated comparison has no such guarantee.
        DocumentFilter::NotIn {
            path: field,
            values,
        } if negated && field == path => !values.is_empty() && values.iter().all(within),
        DocumentFilter::Compare {
            path: field,
            comparison,
            value,
        } if !negated && field == path && value == expected => {
            matches!(comparison, Comparison::Greater | Comparison::GreaterOrEqual) == lower
        }
        _ => false,
    }
}

fn partial_is_implied(required: &[Predicate<'_>], partial: &PartialFilter) -> bool {
    match partial {
        PartialFilter::Exists { path, exists } => required_presence(required, path, *exists),
        PartialFilter::Equals { path, value } => {
            let requires = |lower| {
                required.iter().any(|&(filter, negated)| {
                    requires_guard_bound(filter, path, value, lower, negated)
                })
            };
            requires(true) && requires(false)
        }
    }
}

fn eligible(index: &IndexDefinition, required: &[Predicate<'_>]) -> bool {
    index
        .partial
        .as_ref()
        .is_none_or(|partial| partial_is_implied(required, partial))
}

enum Selection {
    Equality(Vec<IndexValue>),
    Membership(Vec<IndexValue>),
    CompoundMembership(Vec<Vec<IndexValue>>),
    CompoundPrefix(Vec<Vec<IndexValue>>),
    IntegerMembership {
        prefixes: Vec<Vec<IndexValue>>,
        runs: Vec<(i64, i64)>,
    },
    Range {
        equal: Vec<IndexValue>,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
    },
    MembershipRange {
        prefixes: Vec<Vec<IndexValue>>,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
    },
}

#[derive(PartialEq, Eq, Hash)]
struct IndexProbe<'a> {
    index: &'a str,
    equal: Vec<IndexValue>,
    lower: Option<IndexBound>,
    upper: Option<IndexBound>,
    unique_equality: bool,
}

impl Selection {
    fn into_probes(self, index: &IndexDefinition) -> Vec<IndexProbe<'_>> {
        let (prefixes, lower, upper) = match self {
            Self::Equality(values) => (vec![values], None, None),
            Self::Membership(values) => (
                values.into_iter().map(|value| vec![value]).collect(),
                None,
                None,
            ),
            Self::CompoundMembership(tuples) | Self::CompoundPrefix(tuples) => (tuples, None, None),
            Self::IntegerMembership { prefixes, runs } => {
                let runs = &runs;
                return prefixes
                    .into_iter()
                    .flat_map(|prefix| {
                        runs.iter().map(move |&(first, last)| {
                            let mut equal = prefix.clone();
                            let bound = |value| IndexBound {
                                value: IndexValue::Integer(value),
                                inclusive: true,
                            };
                            let (lower, upper) = if first == last {
                                equal.push(IndexValue::Integer(first));
                                (None, None)
                            } else {
                                (Some(bound(first)), Some(bound(last)))
                            };
                            IndexProbe {
                                index: &index.name,
                                unique_equality: index.unique && equal.len() == index.fields.len(),
                                equal,
                                lower,
                                upper,
                            }
                        })
                    })
                    .collect();
            }
            Self::Range {
                equal,
                lower,
                upper,
            } => (vec![equal], lower, upper),
            Self::MembershipRange {
                prefixes,
                lower,
                upper,
            } => (prefixes, lower, upper),
        };
        let unbounded_unique = index.unique && lower.is_none() && upper.is_none();
        prefixes
            .into_iter()
            .map(|equal| IndexProbe {
                index: &index.name,
                unique_equality: unbounded_unique && equal.len() == index.fields.len(),
                equal,
                lower: lower.clone(),
                upper: upper.clone(),
            })
            .collect()
    }
}

fn compound_membership(
    required: &[Predicate<'_>],
    fields: &[Vec<String>],
) -> io::Result<Option<Vec<Vec<IndexValue>>>> {
    let (choices, has_membership) = compound_choices(required, fields)?;
    Ok((has_membership
        && choices.len() == fields.len()
        && fits_probe_product(&choices, MAX_INDEX_PROBES))
    .then(|| expand_tuples(choices)))
}

// Gather factors without expanding them. Callers check the probe product
// after choosing exact tuples or grouped integer ranges.
fn compound_choices(
    required: &[Predicate<'_>],
    fields: &[Vec<String>],
) -> io::Result<(Vec<Vec<IndexValue>>, bool)> {
    let mut choices = Vec::with_capacity(fields.len());
    let mut has_membership = false;
    for field in fields {
        let values = if let Some(value) = required_equality(required, field)? {
            vec![value]
        } else {
            let narrowed = required_range(required, field)?.and_then(|(lower, upper)| {
                membership_within_bounds(required, field, lower.as_ref(), upper.as_ref())
            });
            let Some(values) = narrowed.or_else(|| required_membership(required, field, |_| true))
            else {
                break;
            };
            has_membership = true;
            values
        };
        choices.push(values);
    }
    Ok((choices, has_membership))
}

fn fits_probe_product(choices: &[Vec<IndexValue>], max: usize) -> bool {
    choices
        .iter()
        .try_fold(1_usize, |count, values| {
            count
                .checked_mul(values.len())
                .filter(|&count| count <= max)
        })
        .is_some()
}

// The distinct alternatives within each field yield disjoint prefixes.
fn expand_tuples(choices: Vec<Vec<IndexValue>>) -> Vec<Vec<IndexValue>> {
    let mut tuples = vec![Vec::new()];
    for values in choices {
        tuples = extend_tuples(tuples, &values);
    }
    tuples
}

fn extend_tuples(tuples: Vec<Vec<IndexValue>>, values: &[IndexValue]) -> Vec<Vec<IndexValue>> {
    let mut next = Vec::with_capacity(tuples.len() * values.len());
    for tuple in tuples {
        for value in values {
            let mut next_tuple = tuple.clone();
            next_tuple.push(value.clone());
            next.push(next_tuple);
        }
    }
    next
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

type EncodedBound = Option<(IndexBound, Vec<u8>)>;

fn tighten_range(
    lower: &mut EncodedBound,
    upper: &mut EncodedBound,
    value: &IndexValue,
    comparison: Comparison,
) -> io::Result<()> {
    match comparison {
        Comparison::Greater => tighten(lower, value, false, true),
        Comparison::GreaterOrEqual => tighten(lower, value, true, true),
        Comparison::Less => tighten(upper, value, false, false),
        Comparison::LessOrEqual => tighten(upper, value, true, false),
    }
}

fn required_range(
    required: &[Predicate<'_>],
    path: &[String],
) -> io::Result<Option<(Option<IndexBound>, Option<IndexBound>)>> {
    let mut lower = None;
    let mut upper = None;
    for (filter, negated) in required_predicates(required) {
        match (filter, negated) {
            (
                DocumentFilter::Compare {
                    path: field,
                    comparison,
                    value,
                },
                false,
            ) if field == path => {
                tighten_range(&mut lower, &mut upper, value, *comparison)?;
            }
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

fn compound_bounds(
    required: &[Predicate<'_>],
    fields: &[Vec<String>],
    position: usize,
) -> io::Result<Option<(Option<IndexBound>, Option<IndexBound>)>> {
    // Compound entries omit a document if any indexed field is missing.
    // The range requires this field and the prefix predicates require the
    // earlier fields. Every later field needs its own proof of presence.
    if fields[position + 1..]
        .iter()
        .all(|later| required_presence(required, later, true))
    {
        required_range(required, &fields[position])
    } else {
        Ok(None)
    }
}

fn membership_within_bounds(
    required: &[Predicate<'_>],
    field: &[String],
    lower: Option<&IndexBound>,
    upper: Option<&IndexBound>,
) -> Option<Vec<IndexValue>> {
    required_membership(required, field, |actual| {
        lower.is_none_or(|bound| {
            bound.value.compare_json(actual).is_some_and(|order| {
                order == Ordering::Greater || (bound.inclusive && order == Ordering::Equal)
            })
        }) && upper.is_none_or(|bound| {
            bound.value.compare_json(actual).is_some_and(|order| {
                order == Ordering::Less || (bound.inclusive && order == Ordering::Equal)
            })
        })
    })
}

// Values are distinct and in encoded index order. Consecutive signed
// integers are adjacent in that order, with no other scalar type in between.
fn consecutive_integer_runs(values: &[IndexValue]) -> Option<Vec<(i64, i64)>> {
    if !values.windows(2).any(|pair| {
        matches!(pair, [IndexValue::Integer(a), IndexValue::Integer(b)]
            if a.checked_add(1) == Some(*b))
    }) {
        return None;
    }
    let mut runs: Vec<(i64, i64)> = Vec::new();
    for value in values {
        let IndexValue::Integer(value) = *value else {
            return None;
        };
        match runs.last_mut() {
            Some((_, last)) if last.checked_add(1) == Some(value) => *last = value,
            _ => runs.push((value, value)),
        }
    }
    Some(runs)
}

fn choose_index<'a>(
    required: &[Predicate<'_>],
    indexes: &'a [IndexDefinition],
    allow_integer_runs: bool,
) -> io::Result<Option<(&'a IndexDefinition, Selection)>> {
    // Prefer a unique exact tuple, then a nonunique one, regardless of range.
    // Complete tuples require equality on every field, so omitted missing
    // fields cannot produce an incorrect result.
    for index in indexes
        .iter()
        .filter(|index| index.unique)
        .chain(indexes.iter().filter(|index| !index.unique))
        .filter(|index| eligible(index, required))
    {
        if let Some(tuple) = index
            .fields
            .iter()
            .map(|field| required_equality(required, field))
            .collect::<io::Result<Option<Vec<_>>>>()?
        {
            return Ok(Some((index, Selection::Equality(tuple))));
        }
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
            if let Some(value) = required_equality(required, field)? {
                equal.push(value);
                continue;
            }
            if let Some((lower, upper)) = compound_bounds(required, &index.fields, position)? {
                // The range and membership are both required. Exact probes of
                // the surviving values can skip all other rows in the range.
                // compound_bounds already proved presence of trailing fields.
                if let Some(values) =
                    membership_within_bounds(required, field, lower.as_ref(), upper.as_ref())
                {
                    let runs = if allow_integer_runs {
                        consecutive_integer_runs(&values)
                    } else {
                        None
                    };
                    let selection = if let Some(runs) = runs {
                        Selection::IntegerMembership {
                            prefixes: vec![equal],
                            runs,
                        }
                    } else if index.fields.len() == 1 {
                        Selection::Membership(values)
                    } else {
                        let prefixes = values
                            .into_iter()
                            .map(|value| {
                                let mut prefix = equal.clone();
                                prefix.push(value);
                                prefix
                            })
                            .collect();
                        if position + 1 == index.fields.len() {
                            Selection::CompoundMembership(prefixes)
                        } else {
                            Selection::CompoundPrefix(prefixes)
                        }
                    };
                    return Ok(Some((index, selection)));
                }
                return Ok(Some((
                    index,
                    Selection::Range {
                        equal,
                        lower,
                        upper,
                    },
                )));
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
            required_membership(required, &index.fields[0], |_| true)
                .map(|values| (index, Selection::Membership(values)))
        })
    {
        return Ok(Some(selected));
    }
    // Complete compound tuples can also be probed as a bounded union. Keep
    // existing exact, range and single-field membership choices first.
    for index in indexes
        .iter()
        .filter(|index| index.unique)
        .chain(indexes.iter().filter(|index| !index.unique))
        .filter(|index| index.fields.len() > 1 && eligible(index, required))
    {
        let (mut choices, has_membership) = compound_choices(required, &index.fields)?;
        if !has_membership || choices.len() != index.fields.len() {
            continue;
        }
        let last = choices.len() - 1;
        let exact_fits = fits_probe_product(&choices, MAX_INDEX_PROBES);
        if allow_integer_runs && choices[..last].iter().any(|values| values.len() > 1) {
            if let Some(runs) = consecutive_integer_runs(&choices[last])
                .filter(|runs| fits_probe_product(&choices[..last], MAX_INDEX_PROBES / runs.len()))
            {
                if !exact_fits || required_range(required, &index.fields[last])?.is_some() {
                    choices.pop();
                    return Ok(Some((
                        index,
                        Selection::IntegerMembership {
                            prefixes: expand_tuples(choices),
                            runs,
                        },
                    )));
                }
            }
        }
        if !exact_fits {
            continue;
        }
        return Ok(Some((
            index,
            Selection::CompoundMembership(expand_tuples(choices)),
        )));
    }
    for index in indexes
        .iter()
        .filter(|index| index.fields.len() > 1 && eligible(index, required))
    {
        for position in 1..index.fields.len() {
            let Some((lower, upper)) = compound_bounds(required, &index.fields, position)? else {
                continue;
            };
            if let Some(prefixes) = compound_membership(required, &index.fields[..position])? {
                if let Some(values) = membership_within_bounds(
                    required,
                    &index.fields[position],
                    lower.as_ref(),
                    upper.as_ref(),
                ) {
                    if allow_integer_runs {
                        if let Some(runs) = consecutive_integer_runs(&values)
                            .filter(|runs| prefixes.len() <= MAX_INDEX_PROBES / runs.len())
                        {
                            return Ok(Some((
                                index,
                                Selection::IntegerMembership { prefixes, runs },
                            )));
                        }
                    }
                    if position + 1 < index.fields.len()
                        && prefixes.len() <= MAX_INDEX_PROBES / values.len()
                    {
                        return Ok(Some((
                            index,
                            Selection::CompoundPrefix(extend_tuples(prefixes, &values)),
                        )));
                    }
                }
                return Ok(Some((
                    index,
                    Selection::MembershipRange {
                        prefixes,
                        lower,
                        upper,
                    },
                )));
            }
        }
    }
    Ok(None)
}

fn choose_prefix<'a>(
    required: &[Predicate<'_>],
    indexes: &'a [IndexDefinition],
) -> io::Result<Option<(&'a IndexDefinition, Selection)>> {
    for index in indexes
        .iter()
        .filter(|index| index.fields.len() > 1 && eligible(index, required))
    {
        let (choices, _) = compound_choices(required, &index.fields)?;
        let prefix_fields = choices.len();
        if prefix_fields > 0
            && prefix_fields < index.fields.len()
            && fits_probe_product(&choices, MAX_INDEX_PROBES)
            && index.fields[prefix_fields..]
                .iter()
                .all(|field| required_presence(required, field, true))
        {
            return Ok(Some((
                index,
                Selection::CompoundPrefix(expand_tuples(choices)),
            )));
        }
    }
    Ok(None)
}

// Keep the first occurrence of each exact probe, including its index and
// bound inclusivity. Its ordinal preserves scan order and supports rollback.
fn admit_probes<'a>(
    planned: &mut HashMap<IndexProbe<'a>, usize>,
    probes: impl Iterator<Item = IndexProbe<'a>>,
) -> bool {
    let start = planned.len();
    for probe in probes {
        let ordinal = planned.len();
        if let Entry::Vacant(entry) = planned.entry(probe) {
            if ordinal == MAX_INDEX_PROBES {
                planned.retain(|_, ordinal| *ordinal < start);
                return false;
            }
            entry.insert(ordinal);
        }
    }
    true
}

// Both a positive Or and a negated And are unions. A branch can use
// the other required predicates in its context, but never a sibling branch.
fn disjunction(filter: &DocumentFilter, negated: bool) -> Option<&[DocumentFilter]> {
    match (filter, negated) {
        (DocumentFilter::Or(branches), false) | (DocumentFilter::And(branches), true)
            if !branches.is_empty() =>
        {
            Some(branches)
        }
        _ => None,
    }
}

fn choose_disjunction<'a, 'filter>(
    predicate: Predicate<'filter>,
    context: &mut Vec<Predicate<'filter>>,
    indexes: &'a [IndexDefinition],
    planned: &mut HashMap<IndexProbe<'a>, usize>,
    allow_prefix: bool,
) -> io::Result<bool> {
    for (filter, negated) in required_predicates(&[predicate]) {
        let Some(branches) = disjunction(filter, negated) else {
            continue;
        };
        let start = planned.len();
        let mut complete = true;
        for branch in branches {
            let branch = (branch, negated);
            context.push(branch);
            // Keep exact probes here so common values across branches still
            // deduplicate before consuming the candidate budget.
            let mut selection = choose_index(context, indexes, false)?;
            if selection.is_none() && allow_prefix {
                selection = choose_prefix(context, indexes)?;
            }
            let covered = selection.is_some_and(|(index, selection)| {
                admit_probes(planned, selection.into_probes(index).into_iter())
            });
            let covered =
                covered || choose_disjunction(branch, context, indexes, planned, allow_prefix)?;
            context.pop();
            if !covered {
                complete = false;
                break;
            }
        }
        if complete {
            return Ok(true);
        }
        planned.retain(|_, ordinal| *ordinal < start);
    }
    Ok(false)
}

// Only choices on one field with indexable scalar values are equivalent to
// a membership predicate. Keep correlated alternatives as branch plans.
fn literal_disjunction(branches: &[DocumentFilter], negated: bool) -> Option<DocumentFilter> {
    let mut pending: Vec<_> = branches.iter().map(|branch| (branch, negated)).collect();
    let mut path = None;
    let mut distinct = HashSet::new();
    let mut values = Vec::new();
    while let Some((branch, negated)) = pending.pop() {
        if let Some(children) = disjunction(branch, negated) {
            pending.extend(children.iter().map(|child| (child, negated)));
            continue;
        }
        let (field, alternatives) = match (branch, negated) {
            (DocumentFilter::Equal { path, value }, false) => (path, std::slice::from_ref(value)),
            (DocumentFilter::In { path, values }, false)
            | (DocumentFilter::NotIn { path, values }, true)
                if !values.is_empty() =>
            {
                (path, values.as_slice())
            }
            (DocumentFilter::Not(child), _) => {
                pending.push((child, !negated));
                continue;
            }
            _ => return None,
        };
        if *path.get_or_insert(field) != field {
            return None;
        }
        for value in alternatives {
            if distinct.insert(IndexValue::from_json(value).ok()?) {
                if values.len() == MAX_INDEX_PROBES {
                    return None;
                }
                values.push(value);
            }
        }
    }
    Some(DocumentFilter::In {
        path: path?.clone(),
        values: values.into_iter().cloned().collect(),
    })
}

fn choose_literal_disjunctions<'a>(
    filter: &DocumentFilter,
    indexes: &'a [IndexDefinition],
) -> io::Result<Option<(&'a IndexDefinition, Selection)>> {
    let memberships: Vec<_> = required_predicates(&[(filter, false)])
        .filter_map(|(filter, negated)| literal_disjunction(disjunction(filter, negated)?, negated))
        .collect();
    if memberships.is_empty() {
        return Ok(None);
    }
    let mut required = vec![(filter, false)];
    required.extend(memberships.iter().map(|membership| (membership, false)));
    choose_index(&required, indexes, true)
}

fn plan_query<'a>(
    filter: &DocumentFilter,
    indexes: &'a [IndexDefinition],
) -> io::Result<Option<(DocumentQueryAccess, Vec<IndexProbe<'a>>)>> {
    if let Some((index, selection)) = choose_index(&[(filter, false)], indexes, true)? {
        return Ok(Some((
            describe_selection(index, &selection),
            selection.into_probes(index),
        )));
    }
    let mut planned = HashMap::new();
    for allow_prefix in [false, true] {
        if allow_prefix {
            // Try compound constraints from independent disjunctions before
            // falling back to prefix scans. Complete branch plans have priority.
            let selected = match choose_literal_disjunctions(filter, indexes)? {
                some @ Some(_) => some,
                None => choose_prefix(&[(filter, false)], indexes)?,
            };
            if let Some((index, selection)) = selected {
                return Ok(Some((
                    describe_selection(index, &selection),
                    selection.into_probes(index),
                )));
            }
        }
        if choose_disjunction(
            (filter, false),
            &mut vec![(filter, false)],
            indexes,
            &mut planned,
            allow_prefix,
        )? {
            let access = DocumentQueryAccess::DisjunctionIndex {
                indexes: planned
                    .keys()
                    .map(|probe| probe.index)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                probes: planned.len(),
            };
            let mut probes: Vec<_> = planned.into_iter().collect();
            probes.sort_unstable_by_key(|(_, ordinal)| *ordinal);
            return Ok(Some((
                access,
                probes.into_iter().map(|(probe, _)| probe).collect(),
            )));
        }
    }
    // This scans the whole index, so keep every narrower plan above it.
    // Opt in through explicit presence tests. An unsupported scalar probe
    // alone should keep its existing collection-scan fallback.
    let presence = indexes.iter().find(|index| {
        eligible(index, &[(filter, false)])
            && index
                .fields
                .iter()
                .all(|field| proves_presence(filter, field, true, false, true))
    });
    Ok(presence.map(|index| {
        (
            DocumentQueryAccess::PresenceIndex {
                index: index.name.clone(),
                partial: index.partial.is_some(),
            },
            vec![IndexProbe {
                index: &index.name,
                equal: Vec::new(),
                lower: None,
                upper: None,
                unique_equality: false,
            }],
        )
    }))
}

struct Candidates<'a> {
    ids: Option<BTreeSet<String>>,
    collector: Option<filter::QueryCollector<'a>>,
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
        let Some((access, probes)) = plan_query(&query.filter, &metadata.indexes)? else {
            return Ok(PlannedDocumentQueryResult {
                result: self.query(collection, query)?,
                access: DocumentQueryAccess::CollectionScan,
            });
        };

        // A complete equality tuple visits its matching documents in ID
        // order. An index range or a union does not generally do so.
        let id_order = matches!(probes.as_slice(), [probe]
        if metadata.indexes.iter().any(|index|
            index.name == probe.index && index.fields.len() == probe.equal.len()
        ));
        let mut candidates = Candidates {
            // A validated entry in a single complete tuple has a unique ID
            // suffix. Only unordered probes need deduplication or a retry.
            ids: (!id_order).then(BTreeSet::new),
            collector: (id_order || filter::may_retry_in_id_order(&query.filter))
                .then(|| filter::QueryCollector::new(query, id_order)),
            visits: 0,
            bytes: 0,
        };
        for probe in probes {
            self.collect_index_candidates(
                collection,
                &metadata,
                probe,
                query.max_scanned,
                &mut candidates,
            )?;
        }
        let mut result =
            if let Some(collector) = candidates.collector.filter(|c| id_order || !c.has_failed()) {
                collector.finish()?
            } else {
                // Array walks consume their budget in collection order. Other
                // filters retry only if needed to report the first error by ID.
                let mut collector = filter::QueryCollector::new(query, true);
                for id in candidates
                    .ids
                    .expect("unordered probes retain candidate IDs")
                {
                    let encoded = self
                        .read_document_bytes(&key::document(collection, &id)?, |_| Ok(()))?
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "index references a missing document",
                            )
                        })?;
                    collector.push(id, Self::decode_document(&encoded, &metadata.schema)?);
                    if collector.has_failed() {
                        break;
                    }
                }
                collector.finish()?
            };
        result.plan.scanned = candidates.visits;
        Ok(PlannedDocumentQueryResult { result, access })
    }

    fn collect_index_candidates(
        &self,
        collection: &str,
        metadata: &Metadata,
        probe: IndexProbe<'_>,
        max_scanned: usize,
        candidates: &mut Candidates<'_>,
    ) -> io::Result<()> {
        let IndexProbe {
            index,
            equal,
            lower,
            upper,
            unique_equality,
        } = probe;
        let definition = index_definition(metadata, index)?;
        let mut after: Option<Vec<u8>> = None;
        loop {
            // Probe one entry beyond the candidate limit, never silently
            // returning a prefix. Unique equality probes two to detect a
            // corrupt duplicate rather than accepting it as one result.
            let limit = if unique_equality {
                2
            } else {
                (max_scanned - candidates.visits + 1).min(256)
            };
            let indexed = self.visit_index_with_metadata(
                collection,
                definition,
                &IndexQuery {
                    equal: equal.clone(),
                    lower: lower.clone(),
                    upper: upper.clone(),
                    after: None,
                    limit,
                },
                metadata,
                IndexScanOptions {
                    after: after.as_deref(),
                    byte_limit: Some(filter::MAX_SCAN_BYTES - candidates.bytes),
                },
                |entry| {
                    if candidates
                        .ids
                        .as_mut()
                        .is_none_or(|ids| ids.insert(entry.id.clone()))
                    {
                        if let Some(collector) = &mut candidates.collector {
                            collector.push(entry.id, entry.document);
                        }
                    }
                },
            )?;
            let returned = indexed.plan.returned;
            if unique_equality && returned > 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unique index contains multiple matches",
                ));
            }
            candidates.bytes = candidates
                .bytes
                .checked_add(indexed.document_key_value_bytes)
                .ok_or_else(|| key::invalid("scan byte count overflow"))?;
            if returned > max_scanned - candidates.visits {
                return Err(key::invalid("document scan exceeds the operation limit"));
            }
            if returned == 0 {
                break;
            }
            candidates.visits += returned;
            if unique_equality {
                break;
            }
            after = Some(indexed.next.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index page lost its continuation cursor",
                )
            })?);
        }
        Ok(())
    }
}

fn describe_membership_ranges(
    index: &IndexDefinition,
    prefix_fields: usize,
    probes: usize,
) -> DocumentQueryAccess {
    if index.partial.is_some() {
        DocumentQueryAccess::PartialCompoundMembershipRangeIndex {
            index: index.name.clone(),
            prefix_fields,
            probes,
        }
    } else {
        DocumentQueryAccess::CompoundMembershipRangeIndex {
            index: index.name.clone(),
            prefix_fields,
            probes,
        }
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
        Selection::CompoundPrefix(prefixes) => DocumentQueryAccess::CompoundPrefixIndex {
            index: index.name.clone(),
            prefix_fields: prefixes[0].len(),
            probes: prefixes.len(),
            partial,
        },
        Selection::IntegerMembership { prefixes, runs } if prefixes.len() == 1 => {
            DocumentQueryAccess::IntegerMembershipIndex {
                index: index.name.clone(),
                prefix_fields: prefixes[0].len(),
                probes: runs.len(),
                partial,
            }
        }
        Selection::IntegerMembership { prefixes, runs } => {
            describe_membership_ranges(index, prefixes[0].len(), prefixes.len() * runs.len())
        }
        Selection::MembershipRange { prefixes, .. } => {
            describe_membership_ranges(index, prefixes[0].len(), prefixes.len())
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
