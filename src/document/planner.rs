//! Conservative index selection for bounded queries.
use super::{
    filter::{self, Comparison, DocumentFilter, DocumentQuery, DocumentQueryResult, QueryPlan},
    index::{IndexDefinition, IndexValue, MAX_INDEX_STRING_BYTES, PartialFilter},
    key,
    query::{IndexBound, IndexQuery, IndexScanOptions, index_bounds, index_definition},
    transaction::{DocumentTransaction, Metadata},
};
use serde_json::Value;
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

/// A planned query's access path and ordered index probes before execution.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DocumentQueryExplanation {
    /// The path selected from the current collection metadata.
    pub access: DocumentQueryAccess,
    /// The probes in execution order. Empty for a collection scan.
    pub probes: Vec<DocumentQueryProbe>,
}

/// Index-entry counts made by an opt-in cost-based planned query.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DocumentQueryEstimates {
    /// One count per eligible range-class candidate, in fallback order.
    /// Empty when fewer than two such candidates existed.
    pub candidates: Vec<DocumentQueryEstimate>,
    /// Index entries read by every estimate probe. This is never more than
    /// the supplied entry budget and excludes the executed query's scan.
    pub entries_read: usize,
    /// Index scan calls made by every estimate probe, including calls that
    /// returned no entries. This is never more than the supplied scan budget.
    pub scans: usize,
}

/// Total work allowed for the estimates of one opt-in cost-based query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocumentEstimateLimits {
    /// Index entries the estimate probes may read across all candidates.
    pub max_entries: usize,
    /// Index scan calls the estimate probes may make across all candidates.
    /// Each call can register a range in the transaction's read set, even
    /// when it finds no entries. An empty key range needs no call.
    pub max_scans: usize,
}

impl Default for DocumentEstimateLimits {
    fn default() -> Self {
        Self {
            max_entries: 2 * ESTIMATE_CAP + 2,
            max_scans: 16,
        }
    }
}

/// One candidate's index-entry count.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DocumentQueryEstimate {
    /// Name of the candidate index.
    pub index: String,
    /// Index entries the candidate would read, or `None` when it would read
    /// more than needed to beat the best candidate so far, or when the entry
    /// or scan budget ran out before its count was complete.
    pub entries: Option<usize>,
}

/// One probe in a document query explanation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DocumentQueryProbe {
    /// Name of the index being probed.
    pub index: String,
    /// Leading fields constrained to these exact values, in index order.
    pub equal: Vec<IndexValue>,
    /// Optional lower bound on the next indexed field.
    pub lower: Option<IndexBound>,
    /// Optional upper bound on the next indexed field.
    pub upper: Option<IndexBound>,
    /// Whether the probe constrains every field of a unique index to equality.
    pub unique_equality: bool,
}

type Predicate<'a> = (&'a DocumentFilter, bool);

// A chosen access path and its probes, or `None` for a collection scan.
type Plan<'a> = Option<(DocumentQueryAccess, Vec<IndexProbe<'a>>)>;

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
    let mut bounds = RequiredBounds::default();
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
                negated,
            ) if field == path => bounds.compare(value, *comparison, negated)?,
            _ => {}
        }
    }
    let (Some((lower, _)), Some((upper, _))) = bounds.finish()? else {
        return Ok(None);
    };
    match (&lower.value, &upper.value) {
        // The interval between two integer bounds excludes every other
        // scalar type. Only inside that domain is adjacency sufficient.
        (IndexValue::Integer(first), IndexValue::Integer(last)) => {
            let first = inclusive_integer_endpoint(*first, lower.inclusive, true);
            let last = inclusive_integer_endpoint(*last, upper.inclusive, false);
            Ok(first
                .filter(|first| Some(*first) == last)
                .map(IndexValue::Integer))
        }
        _ if lower.inclusive && upper.inclusive && lower.value == upper.value => {
            Ok(Some(lower.value))
        }
        _ => Ok(None),
    }
}

fn inclusive_integer_endpoint(value: i64, inclusive: bool, lower: bool) -> Option<i64> {
    if inclusive {
        Some(value)
    } else if lower {
        value.checked_add(1)
    } else {
        value.checked_sub(1)
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
        DocumentFilter::CompareFields { left, right, .. } if !explicit_only && !negated => {
            path_implies_presence(left, true, path, exists)
                || path_implies_presence(right, true, path, exists)
        }
        DocumentFilter::Equal { path: field, .. }
        | DocumentFilter::StartsWith { path: field, .. }
        | DocumentFilter::EndsWith { path: field, .. }
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
        } if !negated && field == path => {
            let is_lower = matches!(comparison, Comparison::Greater | Comparison::GreaterOrEqual);
            let inclusive = matches!(
                comparison,
                Comparison::GreaterOrEqual | Comparison::LessOrEqual
            );
            is_lower == lower
                && (value == expected
                    || match (value, expected) {
                        (IndexValue::Integer(value), IndexValue::Integer(expected)) => {
                            inclusive_integer_endpoint(*value, inclusive, lower) == Some(*expected)
                        }
                        _ => false,
                    })
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

#[derive(Clone)]
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

#[derive(Default)]
struct RequiredBounds<'a> {
    lower: EncodedBound,
    upper: EncodedBound,
    comparable: bool,
    negated: Vec<(&'a IndexValue, Comparison)>,
}

impl<'a> RequiredBounds<'a> {
    fn compare(
        &mut self,
        value: &'a IndexValue,
        comparison: Comparison,
        negated: bool,
    ) -> io::Result<()> {
        if negated {
            self.negated.push((value, comparison));
            Ok(())
        } else {
            self.comparable = true;
            tighten_range(&mut self.lower, &mut self.upper, value, comparison)
        }
    }

    fn finish(mut self) -> io::Result<(EncodedBound, EncodedBound)> {
        // A positive comparison on this path rules out missing fields and
        // non-indexable values. On that domain negation reverses the bound.
        if self.comparable {
            for (value, comparison) in self.negated {
                let opposite = match comparison {
                    Comparison::Greater => Comparison::LessOrEqual,
                    Comparison::GreaterOrEqual => Comparison::Less,
                    Comparison::Less => Comparison::GreaterOrEqual,
                    Comparison::LessOrEqual => Comparison::Greater,
                };
                tighten_range(&mut self.lower, &mut self.upper, value, opposite)?;
            }
        }
        Ok((self.lower, self.upper))
    }
}

// UTF-8 preserves scalar order. Advance the last scalar that has a
// representable successor and discard everything after it. If a wider
// successor would exceed 256 bytes, no indexed string can use it there.
fn string_prefix_end(prefix: &str) -> Option<String> {
    for (offset, last) in prefix.char_indices().rev() {
        let next = u32::from(last) + 1;
        let next = if next == 0xd800 { 0xe000 } else { next };
        if let Some(next) =
            char::from_u32(next).filter(|next| offset + next.len_utf8() <= MAX_INDEX_STRING_BYTES)
        {
            let mut end = prefix[..offset].to_owned();
            end.push(next);
            return Some(end);
        }
    }
    None
}

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
    let mut bounds = RequiredBounds::default();
    for (filter, negated) in required_predicates(required) {
        match (filter, negated) {
            (
                DocumentFilter::Compare {
                    path: field,
                    comparison,
                    value,
                },
                negated,
            ) if field == path => bounds.compare(value, *comparison, negated)?,
            (
                DocumentFilter::StartsWith {
                    path: field,
                    prefix,
                },
                false,
            ) if field == path => {
                tighten(
                    &mut bounds.lower,
                    &IndexValue::String(prefix.clone()),
                    true,
                    true,
                )?;
                if let Some(end) = string_prefix_end(prefix) {
                    tighten(&mut bounds.upper, &IndexValue::String(end), false, false)?;
                }
            }
            _ => {}
        }
    }
    let (lower, upper) = bounds.finish()?;
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
    choose_index_with(required, indexes, allow_integer_runs, &mut None)
}

fn choose_index_with<'a>(
    required: &[Predicate<'_>],
    indexes: &'a [IndexDefinition],
    allow_integer_runs: bool,
    estimate: &mut Option<&mut Estimation<'_>>,
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
    // Range-class choices keep the single-field-first order as their
    // deterministic fallback. With an estimator, the eligible choice with the
    // fewest index entries, counted up to a bounded cap, is taken instead.
    let ranges = indexes
        .iter()
        .filter(|index| index.fields.len() == 1)
        .chain(indexes.iter().filter(|index| index.fields.len() > 1))
        .filter(|index| eligible(index, required));
    if estimate.is_none() {
        // Without estimation keep the lazy first eligible choice exactly.
        for index in ranges {
            if let Some(selection) = range_selection(required, index, allow_integer_runs)? {
                return Ok(Some((index, selection)));
            }
        }
    } else {
        let mut choices = Vec::new();
        for index in ranges {
            if let Some(selection) = range_selection(required, index, allow_integer_runs)? {
                choices.push((index, selection));
            }
        }
        if let Some(choice) = pick_estimated(choices, estimate)? {
            return Ok(Some(choice));
        }
    }
    // Preserve existing equality and range choices. A union is safe for a    // Preserve existing equality and range choices. A union is safe for a
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
        for position in (1..index.fields.len()).rev() {
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
                    let runs = allow_integer_runs
                        .then(|| {
                            consecutive_integer_runs(&values)
                                .filter(|runs| prefixes.len() <= MAX_INDEX_PROBES / runs.len())
                        })
                        .flatten();
                    let fits_prefix = prefixes.len() <= MAX_INDEX_PROBES / values.len();
                    if position + 1 < index.fields.len() && (runs.is_some() || fits_prefix) {
                        continue;
                    }
                    if let Some(runs) = runs {
                        return Ok(Some((
                            index,
                            Selection::IntegerMembership { prefixes, runs },
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

// The range-class selection the range pass makes for one eligible index.
fn range_selection(
    required: &[Predicate<'_>],
    index: &IndexDefinition,
    allow_integer_runs: bool,
) -> io::Result<Option<Selection>> {
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
                // When trailing fields remain, defer an incomplete prefix
                // scan so complete compound tuples, deeper ranges, complete
                // disjunctions and longer prefixes can narrow them first.
                if position + 1 < index.fields.len() {
                    return Ok(None);
                }
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
                    Selection::CompoundMembership(prefixes)
                };
                return Ok(Some(selection));
            }
            return Ok(Some(Selection::Range {
                equal,
                lower,
                upper,
            }));
        }
        return Ok(None);
    }
    Ok(None)
}

/// Most index entries one candidate is counted to before it is reported as
/// exceeding the cap.
const ESTIMATE_CAP: usize = 256;

// Counts the index entries a selection would read, up to `cap`. Returns the
// count, or `None` for more than `cap`, and the index entries it read, which
// is at most `cap + 1`.
type Estimator<'e> =
    &'e mut dyn FnMut(&IndexDefinition, &Selection, usize) -> io::Result<(Option<usize>, usize)>;

// Opt-in estimation: the estimator, the remaining total entry budget across
// every candidate, and the estimates recorded in candidate order.
struct Estimation<'e> {
    estimator: Estimator<'e>,
    remaining: usize,
    recorded: Vec<(String, Option<usize>)>,
}

fn pick_estimated<'a>(
    choices: Vec<(&'a IndexDefinition, Selection)>,
    estimation: &mut Option<&mut Estimation<'_>>,
) -> io::Result<Option<(&'a IndexDefinition, Selection)>> {
    let Some(estimation) = estimation.as_mut().filter(|_| choices.len() > 1) else {
        return Ok(choices.into_iter().next());
    };
    // A later candidate is counted only to one less than the best count so
    // far, and never past the remaining total budget, so the estimate reads
    // at most the caller's total across all candidates. Ties keep the
    // deterministic fallback order.
    let mut best: Option<(usize, usize)> = None;
    for (position, (index, selection)) in choices.iter().enumerate() {
        let wanted = best.map_or(ESTIMATE_CAP, |(count, _)| count.saturating_sub(1));
        let count = match (best, estimation.remaining.checked_sub(1)) {
            (Some((0, _)), _) | (_, None) => None,
            (_, Some(spare)) => {
                let (count, read) = (estimation.estimator)(index, selection, wanted.min(spare))?;
                estimation.remaining = estimation
                    .remaining
                    .checked_sub(read)
                    .ok_or_else(|| io::Error::other("estimate exceeded its entry budget"))?;
                count
            }
        };
        estimation.recorded.push((index.name.clone(), count));
        if let Some(count) = count {
            if best.is_none_or(|(best_count, _)| count < best_count) {
                best = Some((count, position));
            }
        }
    }
    let chosen = best.map_or(0, |(_, position)| position);
    Ok(choices.into_iter().nth(chosen))
}

fn choose_prefix<'a>(
    required: &[Predicate<'_>],
    indexes: &'a [IndexDefinition],
    allow_integer_runs: bool,
) -> io::Result<Option<(&'a IndexDefinition, Selection)>> {
    for index in indexes
        .iter()
        .filter(|index| index.fields.len() > 1 && eligible(index, required))
    {
        let (mut choices, _) = compound_choices(required, &index.fields)?;
        let full_len = choices.len();
        if full_len == 0 {
            continue;
        }
        let mut prefix_fields = full_len.min(index.fields.len() - 1);
        while prefix_fields > 0 {
            let narrowed = prefix_fields < full_len;
            if narrowed
                && !index.fields[..prefix_fields]
                    .iter()
                    .try_fold(false, |found, field| {
                        Ok::<_, io::Error>(found || required_range(required, field)?.is_some())
                    })?
            {
                break;
            }
            if index.fields[prefix_fields..]
                .iter()
                .all(|field| required_presence(required, field, true))
            {
                let last = prefix_fields - 1;
                if allow_integer_runs && required_range(required, &index.fields[last])?.is_some() {
                    if let Some(runs) = consecutive_integer_runs(&choices[last]).filter(|runs| {
                        fits_probe_product(&choices[..last], MAX_INDEX_PROBES / runs.len())
                    }) {
                        choices.truncate(last);
                        return Ok(Some((
                            index,
                            Selection::IntegerMembership {
                                prefixes: expand_tuples(choices),
                                runs,
                            },
                        )));
                    }
                }
                if fits_probe_product(&choices[..prefix_fields], MAX_INDEX_PROBES) {
                    choices.truncate(prefix_fields);
                    return Ok(Some((
                        index,
                        Selection::CompoundPrefix(expand_tuples(choices)),
                    )));
                }
            }
            prefix_fields -= 1;
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
                selection = choose_prefix(context, indexes, false)?;
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
    collection: &str,
    filter: &DocumentFilter,
    indexes: &'a [IndexDefinition],
    estimate: &mut Option<&mut Estimation<'_>>,
) -> io::Result<Option<(DocumentQueryAccess, Vec<IndexProbe<'a>>)>> {
    if let Some((index, selection)) =
        choose_index_with(&[(filter, false)], indexes, true, estimate)?
    {
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
                None => choose_prefix(&[(filter, false)], indexes, true)?,
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
            let mut ordered: Vec<_> = planned.into_iter().collect();
            ordered.sort_unstable_by_key(|(_, ordinal)| *ordinal);
            let mut probes: Vec<_> = ordered.into_iter().map(|(probe, _)| probe).collect();
            coalesce_disjunction_ranges(collection, &mut probes)?;
            let access = DocumentQueryAccess::DisjunctionIndex {
                indexes: probes
                    .iter()
                    .map(|probe| probe.index)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                probes: probes.len(),
            };
            return Ok(Some((access, probes)));
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

fn coalesce_disjunction_ranges(
    collection: &str,
    probes: &mut Vec<IndexProbe<'_>>,
) -> io::Result<()> {
    let ranges = probes
        .iter()
        .enumerate()
        .filter(|(_, probe)| probe.lower.is_some() || probe.upper.is_some());
    // Exact probes retain their existing unique-equality and cursor checks.
    if ranges.clone().take(2).count() < 2 {
        return Ok(());
    }
    let mut intervals = Vec::new();
    for (ordinal, probe) in ranges {
        let prefix = key::index(b'I', collection, probe.index)?;
        let bounds = index_bounds(
            &prefix,
            &probe.equal,
            probe.lower.as_ref(),
            probe.upper.as_ref(),
        )?;
        // Overlong endpoints go through the execution path's clipping rules.
        // Leave those and empty intervals out of this optimization.
        if bounds.after.len() <= crate::record::MAX_KEY
            && bounds.before.len() <= crate::record::MAX_KEY
            && bounds.after < bounds.before
        {
            intervals.push((
                probe.index,
                bounds.after,
                std::cmp::Reverse(bounds.before),
                ordinal,
            ));
        }
    }
    intervals.sort_unstable();
    let mut keep = vec![true; probes.len()];
    let mut covering: Option<(&str, Vec<u8>, usize)> = None;
    // Starts ascend and ties put the widest interval first. Containment only
    // needs the largest end. A crossing interval can extend that probe when
    // both bounds constrain the same field after the same equality prefix.
    for (index, after, std::cmp::Reverse(end), mut ordinal) in intervals {
        if let Some((_, upper, previous)) = covering.as_ref().filter(|(name, _, _)| *name == index)
        {
            if end <= *upper {
                keep[ordinal] = false;
                continue;
            }
            // The scan excludes both endpoints. Equality could leave a gap
            // at the shared boundary, so only join strict intersections.
            if after < *upper && probes[ordinal].equal == probes[*previous].equal {
                if ordinal < *previous {
                    probes[ordinal].lower = probes[*previous].lower.take();
                    keep[*previous] = false;
                } else {
                    probes[*previous].upper = probes[ordinal].upper.take();
                    keep[ordinal] = false;
                    ordinal = *previous;
                }
            }
        }
        covering = Some((index, end, ordinal));
    }
    let mut keep = keep.into_iter();
    probes.retain(|_| keep.next().expect("one decision per probe"));
    Ok(())
}

#[derive(Default)]
struct CandidateBudget {
    visits: usize,
    bytes: usize,
}

// Only a single complete equality tuple returns rows in document ID order.
fn in_id_order(probes: &[IndexProbe<'_>], metadata: &Metadata) -> bool {
    matches!(probes, [probe] if metadata.indexes.iter().any(|index|
        index.name == probe.index && index.fields.len() == probe.equal.len()
    ))
}

impl DocumentTransaction<'_, '_> {
    /// Validates the query and explains the access path using this transaction's
    /// collection metadata, including staged index changes. Execution reports
    /// the actual scan work and enforces the candidate limits.
    pub fn explain_query(
        &self,
        collection: &str,
        query: &DocumentQuery,
    ) -> io::Result<DocumentQueryExplanation> {
        filter::validate_query(query)?;
        let metadata = self.metadata(collection)?;
        let plan = plan_query(collection, &query.filter, &metadata.indexes, &mut None)?;
        Ok(explain_plan(plan))
    }

    /// Like [`Self::explain_query`], but chooses among eligible range-class
    /// paths by counting each one's index entries. Across all candidates the
    /// counts read at most `limits.max_entries` index entries and make at
    /// most `limits.max_scans` index scan calls.
    /// These reads see staged writes, join this transaction's read set and
    /// count against its read limits, so they can add conflicts on an index
    /// that is not chosen. Results do not depend on the estimate: every
    /// candidate is a semantically eligible path. Ties and exhausted budgets
    /// keep the deterministic order used by [`Self::explain_query`].
    pub fn explain_query_estimated(
        &self,
        collection: &str,
        query: &DocumentQuery,
        limits: DocumentEstimateLimits,
    ) -> io::Result<(DocumentQueryExplanation, DocumentQueryEstimates)> {
        filter::validate_query(query)?;
        let metadata = self.metadata(collection)?;
        let (plan, estimates) =
            self.plan_estimated(collection, &query.filter, &metadata, limits)?;
        Ok((explain_plan(plan), estimates))
    }

    fn plan_estimated<'m>(
        &self,
        collection: &str,
        filter: &DocumentFilter,
        metadata: &'m Metadata,
        limits: DocumentEstimateLimits,
    ) -> io::Result<(Plan<'m>, DocumentQueryEstimates)> {
        let (mut entries_read, mut scans) = (0, 0);
        let mut estimator = |definition: &IndexDefinition, selection: &Selection, cap: usize| {
            // Count one entry past the cap to tell "more than cap" apart. A
            // count cut short by the scan budget is inconclusive and cannot win.
            let (mut counted, mut conclusive) = (0, true);
            for probe in selection.clone().into_probes(definition) {
                let (count, used, finished) = self.count_index_entries(
                    collection,
                    definition,
                    &IndexQuery {
                        equal: probe.equal,
                        lower: probe.lower,
                        upper: probe.upper,
                        after: None,
                        limit: 1,
                    },
                    cap + 1 - counted,
                    limits.max_scans - scans,
                )?;
                counted += count;
                scans += used;
                if !finished {
                    conclusive = false;
                    break;
                }
                if counted > cap {
                    break;
                }
            }
            entries_read += counted;
            Ok(((conclusive && counted <= cap).then_some(counted), counted))
        };
        let mut estimation = Estimation {
            estimator: &mut estimator,
            remaining: limits.max_entries,
            recorded: Vec::new(),
        };
        let plan = plan_query(
            collection,
            filter,
            &metadata.indexes,
            &mut Some(&mut estimation),
        )?;
        let candidates = estimation
            .recorded
            .into_iter()
            .map(|(index, entries)| DocumentQueryEstimate { index, entries })
            .collect();
        Ok((
            plan,
            DocumentQueryEstimates {
                candidates,
                entries_read,
                scans,
            },
        ))
    }
}

fn explain_plan(
    plan: Option<(DocumentQueryAccess, Vec<IndexProbe<'_>>)>,
) -> DocumentQueryExplanation {
    let Some((access, probes)) = plan else {
        return DocumentQueryExplanation {
            access: DocumentQueryAccess::CollectionScan,
            probes: Vec::new(),
        };
    };
    {
        DocumentQueryExplanation {
            access,
            probes: probes
                .into_iter()
                .map(|probe| DocumentQueryProbe {
                    index: probe.index.to_owned(),
                    equal: probe.equal,
                    lower: probe.lower,
                    upper: probe.upper,
                    unique_equality: probe.unique_equality,
                })
                .collect(),
        }
    }
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
        let plan = plan_query(collection, &query.filter, &metadata.indexes, &mut None)?;
        self.execute_plan(collection, query, &metadata, plan)
    }

    /// Like [`Self::query_planned`], choosing the path as described for
    /// [`Self::explain_query_estimated`]. The returned estimates report the
    /// index entries the estimate probes read, separately from the scan work
    /// reported in the query plan; both count against this transaction.
    pub fn query_planned_estimated(
        &self,
        collection: &str,
        query: &DocumentQuery,
        limits: DocumentEstimateLimits,
    ) -> io::Result<(PlannedDocumentQueryResult, DocumentQueryEstimates)> {
        filter::validate_query(query)?;
        let metadata = self.metadata(collection)?;
        let (plan, estimates) =
            self.plan_estimated(collection, &query.filter, &metadata, limits)?;
        Ok((
            self.execute_plan(collection, query, &metadata, plan)?,
            estimates,
        ))
    }

    fn execute_plan(
        &self,
        collection: &str,
        query: &DocumentQuery,
        metadata: &Metadata,
        plan: Option<(DocumentQueryAccess, Vec<IndexProbe<'_>>)>,
    ) -> io::Result<PlannedDocumentQueryResult> {
        let Some((access, probes)) = plan else {
            return Ok(PlannedDocumentQueryResult {
                result: self.query_with_schema(collection, &metadata.schema, query)?,
                access: DocumentQueryAccess::CollectionScan,
            });
        };

        let id_order = in_id_order(&probes, metadata);
        // Only unordered probes need deduplication or a retry.
        let mut ids = (!id_order).then(BTreeSet::new);
        let mut collector = (id_order || filter::may_retry_in_id_order(&query.filter))
            .then(|| filter::QueryCollector::new(query, id_order));
        let mut budget = CandidateBudget::default();
        for probe in probes {
            self.collect_index_candidates(
                collection,
                metadata,
                probe,
                query.max_scanned,
                &mut budget,
                |entry| {
                    if ids.as_mut().is_none_or(|ids| ids.insert(entry.id.clone())) {
                        if let Some(collector) = &mut collector {
                            collector.push(entry.id, entry.document);
                        }
                    }
                },
            )?;
        }
        let mut result = if let Some(collector) = collector.filter(|c| id_order || !c.has_failed())
        {
            collector.finish()?
        } else {
            // Array walks consume their budget in collection order. Other
            // filters retry only if needed to report the first error by ID.
            let mut collector = filter::QueryCollector::new(query, true);
            for id in ids.expect("unordered probes retain candidate IDs") {
                let document = self.read_index_candidate(collection, metadata, &id)?;
                collector.push(id, document);
                if collector.has_failed() {
                    break;
                }
            }
            collector.finish()?
        };
        result.plan.scanned = budget.visits;
        Ok(PlannedDocumentQueryResult { result, access })
    }

    pub(super) fn visit_planned_documents<T: Default>(
        &self,
        collection: &str,
        predicate: &DocumentFilter,
        max_scanned: usize,
        visit: impl Fn(&mut T, &Value) -> io::Result<()>,
    ) -> io::Result<(QueryPlan, DocumentQueryAccess, T)> {
        filter::validate_filter(predicate)?;
        let metadata = self.metadata(collection)?;
        let mut state = T::default();
        let Some((access, probes)) =
            plan_query(collection, predicate, &metadata.indexes, &mut None)?
        else {
            let plan = self.visit_filtered_documents_with_schema(
                collection,
                &metadata.schema,
                predicate,
                max_scanned,
                |doc| visit(&mut state, doc),
            )?;
            return Ok((plan, DocumentQueryAccess::CollectionScan, state));
        };
        let id_order = in_id_order(&probes, &metadata);
        let stream = id_order || filter::may_retry_in_id_order(predicate);
        let mut ids = (!id_order).then(BTreeSet::new);
        let mut visitor = filter::FilteredVisitor::new(predicate, |doc| visit(&mut state, doc));
        let mut budget = CandidateBudget::default();
        for probe in probes {
            self.collect_index_candidates(
                collection,
                &metadata,
                probe,
                max_scanned,
                &mut budget,
                |entry| {
                    let first = ids.as_mut().is_none_or(|ids| ids.insert(entry.id));
                    if first && stream {
                        visitor.push(&entry.document);
                    }
                },
            )?;
        }
        if id_order || (stream && !visitor.has_failed()) {
            return Ok((visitor.finish(budget.visits)?, access, state));
        }
        // Array walks must consume the filter budget in ID order. For
        // other filters, replay only on error to preserve error ordering.
        state = T::default();
        let mut visitor = filter::FilteredVisitor::new(predicate, |doc| visit(&mut state, doc));
        for id in ids.expect("unordered probes retain candidate IDs") {
            visitor.push(&self.read_index_candidate(collection, &metadata, &id)?);
        }
        Ok((visitor.finish(budget.visits)?, access, state))
    }

    fn read_index_candidate(
        &self,
        collection: &str,
        metadata: &Metadata,
        id: &str,
    ) -> io::Result<Value> {
        let encoded = self
            .read_document_bytes(&key::document(collection, id)?, |_| Ok(()))?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index references a missing document",
                )
            })?;
        Self::decode_document(&encoded, &metadata.schema)
    }

    fn collect_index_candidates(
        &self,
        collection: &str,
        metadata: &Metadata,
        probe: IndexProbe<'_>,
        max_scanned: usize,
        budget: &mut CandidateBudget,
        mut visit: impl FnMut(super::query::DocumentEntry),
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
            // Check one extra index row for exhaustion without reading its
            // document. A unique equality probe also checks for a duplicate.
            let remaining = max_scanned - budget.visits;
            let limit = if unique_equality {
                2
            } else {
                (remaining + 1).min(256)
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
                    byte_limit: Some(filter::MAX_SCAN_BYTES - budget.bytes),
                    response_byte_limit: None,
                    candidate_limit: Some(remaining),
                    unique_equality,
                },
                &mut visit,
            )?;
            let returned = indexed.plan.returned;
            budget.bytes = budget
                .bytes
                .checked_add(indexed.document_key_value_bytes)
                .ok_or_else(|| key::invalid("scan byte count overflow"))?;
            if returned == 0 {
                break;
            }
            budget.visits += returned;
            // A short index scan proves exhaustion. This private visitor
            // consumes all admitted candidates without building a response page.
            if unique_equality || returned < limit {
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
