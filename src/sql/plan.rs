//! Primary-key and secondary-index access path planning for SQL `SELECT`, `UPDATE`, and `DELETE`.
use super::catalog::{IndexDef, TableDef};
use super::exec::{BoundExpr, BoundOrderBy};
use super::parser::CompareOp;
use super::value::{
    MAX_SQL_INDEX_TEXT_BYTES, MAX_SQL_TEXT_PK_BYTES, SqlType, SqlValue, encode_i64_pk,
};
use crate::document::{IndexBound, IndexValue};

const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
const SMALL_PK_RANGE_THRESHOLD: usize = 8;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UpperBound {
    Unbounded,
    Included(String),
    Excluded(String),
}

impl UpperBound {
    pub(crate) fn is_past(&self, doc_id: &str) -> bool {
        match self {
            Self::Unbounded => false,
            Self::Included(hi) => doc_id.as_bytes() > hi.as_bytes(),
            Self::Excluded(hi) => doc_id.as_bytes() >= hi.as_bytes(),
        }
    }

    pub(crate) fn is_at_inclusive(&self, doc_id: &str) -> bool {
        matches!(self, Self::Included(hi) if doc_id.as_bytes() == hi.as_bytes())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AccessPlan {
    Empty,
    PkPoint {
        doc_id: String,
    },
    PkRange {
        after_id: Option<String>,
        upper: UpperBound,
        max_exact_rows: Option<usize>,
    },
    Index {
        index_idx: usize,
        equal: Vec<IndexValue>,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
        unique_equality: bool,
        preserves_pk_asc: bool,
        satisfies_order_by: bool,
    },
    FullScan,
}

#[derive(Clone)]
enum ColBounds {
    Int {
        lo: i64,
        hi: i64,
        has_bound: bool,
    },
    Text {
        lower: TextLower,
        upper: TextUpper,
        has_eq: Option<String>,
    },
    Bool {
        can_false: bool,
        can_true: bool,
    },
    Real,
}

impl ColBounds {
    fn for_type(ty: SqlType) -> Self {
        match ty {
            SqlType::Integer => Self::Int {
                lo: i64::MIN,
                hi: i64::MAX,
                has_bound: false,
            },
            SqlType::Text { .. } => Self::Text {
                lower: TextLower::Unbounded,
                upper: TextUpper::Unbounded,
                has_eq: None,
            },
            SqlType::Boolean => Self::Bool {
                can_false: true,
                can_true: true,
            },
            SqlType::Real => Self::Real,
        }
    }
}

pub(crate) fn plan_access(
    filter: Option<&BoundExpr>,
    table: &TableDef,
    order_by: &[BoundOrderBy],
    has_limit: bool,
    params: &[SqlValue],
) -> AccessPlan {
    let col_count = table.columns.len();
    let mut bounds: Vec<ColBounds> = table
        .columns
        .iter()
        .map(|c| ColBounds::for_type(c.col_type))
        .collect();
    let mut proven_non_null: Vec<bool> = table.columns.iter().map(|c| c.not_null).collect();
    let mut proven_null: Vec<bool> = vec![false; col_count];

    if let Some(expr) = filter {
        let single;
        let mut multi;
        let conjuncts: &[&BoundExpr] = if matches!(expr, BoundExpr::And(_)) {
            multi = Vec::new();
            collect_conjuncts(expr, &mut multi);
            &multi
        } else {
            single = [expr];
            &single
        };

        for &conj in conjuncts {
            match conj {
                BoundExpr::IsNull {
                    expr: inner,
                    negated,
                } => {
                    if let BoundExpr::Column(col_idx) = inner.as_ref() {
                        if *negated {
                            proven_non_null[*col_idx] = true;
                        } else {
                            proven_null[*col_idx] = true;
                        }
                        if proven_null[*col_idx] && proven_non_null[*col_idx] {
                            return AccessPlan::Empty;
                        }
                    }
                }
                BoundExpr::Column(col_idx) => {
                    proven_non_null[*col_idx] = true;
                    if proven_null[*col_idx] {
                        return AccessPlan::Empty;
                    }
                    if let ColBounds::Bool { can_false, .. } = &mut bounds[*col_idx] {
                        *can_false = false;
                    }
                    if matches!(
                        bounds[*col_idx],
                        ColBounds::Bool {
                            can_false: false,
                            can_true: false
                        }
                    ) {
                        return AccessPlan::Empty;
                    }
                }
                BoundExpr::Not(inner) => {
                    if let BoundExpr::Column(col_idx) = inner.as_ref() {
                        proven_non_null[*col_idx] = true;
                        if proven_null[*col_idx] {
                            return AccessPlan::Empty;
                        }
                        if let ColBounds::Bool { can_true, .. } = &mut bounds[*col_idx] {
                            *can_true = false;
                        }
                        if matches!(
                            bounds[*col_idx],
                            ColBounds::Bool {
                                can_false: false,
                                can_true: false
                            }
                        ) {
                            return AccessPlan::Empty;
                        }
                    }
                }
                BoundExpr::Compare { op, left, right } => {
                    let Some((col_idx, op, val)) = extract_col_predicate(*op, left, right, params)
                    else {
                        continue;
                    };
                    if matches!(val, SqlValue::Null) {
                        return AccessPlan::Empty;
                    }
                    proven_non_null[col_idx] = true;
                    if proven_null[col_idx] {
                        return AccessPlan::Empty;
                    }
                    if op == CompareOp::Ne {
                        continue;
                    }
                    if !tighten_col_bound(
                        &mut bounds[col_idx],
                        col_idx == table.primary_key_index,
                        op,
                        val,
                    ) {
                        return AccessPlan::Empty;
                    }
                }
                _ => {}
            }
        }
    }

    let pk_idx = table.primary_key_index;
    let pk_plan = build_pk_plan(&bounds[pk_idx], table.columns[pk_idx].col_type);
    if matches!(pk_plan, AccessPlan::PkPoint { .. }) {
        return pk_plan;
    }

    let mut candidates: Vec<IndexCandidate> = Vec::new();
    for (index_idx, idx_def) in table.indexes.iter().enumerate() {
        if let Some(cand) = evaluate_index_candidate(
            index_idx,
            idx_def,
            table,
            &bounds,
            &proven_non_null,
            order_by,
        ) {
            if cand.unique_equality {
                return cand.into_plan();
            }
            candidates.push(cand);
        }
    }

    let best_where_idx = candidates
        .iter()
        .filter(|c| c.where_useful)
        .max_by_key(|c| c.rank_key());

    match (&pk_plan, best_where_idx) {
        (
            AccessPlan::PkRange {
                max_exact_rows: pk_exact,
                after_id,
                upper,
            },
            Some(idx_cand),
        ) => {
            let pk_two_sided = after_id.is_some() && !matches!(upper, UpperBound::Unbounded);
            if pk_exact.is_some_and(|r| r <= SMALL_PK_RANGE_THRESHOLD) {
                pk_plan
            } else if (idx_cand.equal_len > 0 && idx_cand.has_non_bool_equality)
                || (!pk_two_sided && (idx_cand.two_sided_range || idx_cand.equal_len > 0))
            {
                idx_cand.clone().into_plan()
            } else {
                pk_plan
            }
        }
        (AccessPlan::PkRange { .. }, None) => pk_plan,
        (_, Some(idx_cand)) => idx_cand.clone().into_plan(),
        (_, None) => {
            // If WHERE didn't select an index or PK range, check if an index can stream
            // ORDER BY ... LIMIT directly without sorting.
            if has_limit && !order_by.is_empty() {
                if let Some(order_cand) = candidates.into_iter().find(|c| c.satisfies_order_by) {
                    return order_cand.into_plan();
                }
            }
            AccessPlan::FullScan
        }
    }
}

#[derive(Clone)]
struct IndexCandidate {
    index_idx: usize,
    equal: Vec<IndexValue>,
    lower: Option<IndexBound>,
    upper: Option<IndexBound>,
    equal_len: usize,
    is_full_eq: bool,
    has_range: bool,
    two_sided_range: bool,
    has_non_bool_equality: bool,
    unique: bool,
    unique_equality: bool,
    preserves_pk_asc: bool,
    satisfies_order_by: bool,
    where_useful: bool,
}

impl IndexCandidate {
    fn rank_key(&self) -> (bool, bool, bool, usize, bool, bool, bool, bool) {
        (
            self.is_full_eq && self.has_non_bool_equality,
            self.equal_len > 0 && self.has_non_bool_equality,
            self.is_full_eq,
            self.equal_len,
            self.has_range,
            self.two_sided_range,
            self.unique,
            self.satisfies_order_by,
        )
    }

    fn into_plan(self) -> AccessPlan {
        AccessPlan::Index {
            index_idx: self.index_idx,
            equal: self.equal,
            lower: self.lower,
            upper: self.upper,
            unique_equality: self.unique_equality,
            preserves_pk_asc: self.preserves_pk_asc,
            satisfies_order_by: self.satisfies_order_by,
        }
    }
}

fn evaluate_index_candidate(
    index_idx: usize,
    idx_def: &IndexDef,
    table: &TableDef,
    bounds: &[ColBounds],
    proven_non_null: &[bool],
    order_by: &[BoundOrderBy],
) -> Option<IndexCandidate> {
    let mut equal = Vec::new();
    let mut has_non_bool_equality = false;

    for &col_idx in &idx_def.columns {
        match extract_exact_index_value(&bounds[col_idx])? {
            Some(val) => {
                if !matches!(val, IndexValue::Boolean(_)) {
                    has_non_bool_equality = true;
                }
                equal.push(val);
            }
            None => break,
        }
    }

    let equal_len = equal.len();
    let (lower, upper) = if equal_len < idx_def.columns.len() {
        extract_index_range(&bounds[idx_def.columns[equal_len]])
    } else {
        (None, None)
    };
    let has_range = lower.is_some() || upper.is_some();
    let two_sided_range = lower.is_some() && upper.is_some();

    let covered_prefix = if has_range { equal_len + 1 } else { equal_len };
    for &trailing_col in &idx_def.columns[covered_prefix..] {
        if !proven_non_null[trailing_col] {
            return None;
        }
    }

    let is_full_eq = equal_len == idx_def.columns.len();
    let unique_equality = idx_def.unique && is_full_eq;
    let preserves_pk_asc = is_full_eq;
    let where_useful = equal_len > 0 || has_range;
    let satisfies_order_by = check_index_satisfies_order_by(idx_def, table, equal_len, order_by);

    Some(IndexCandidate {
        index_idx,
        equal,
        lower,
        upper,
        equal_len,
        is_full_eq,
        has_range,
        two_sided_range,
        has_non_bool_equality,
        unique: idx_def.unique,
        unique_equality,
        preserves_pk_asc,
        satisfies_order_by,
        where_useful,
    })
}

/// Returns `Ok`-like `Option<Option<IndexValue>>`:
/// `None` means impossible (> 256 byte text eq), `Some(None)` means not exact equality,
/// `Some(Some(val))` means exact equality.
fn extract_exact_index_value(col_bounds: &ColBounds) -> Option<Option<IndexValue>> {
    match col_bounds {
        ColBounds::Int { lo, hi, .. } => {
            if lo == hi {
                Some(Some(IndexValue::Integer(*lo)))
            } else {
                Some(None)
            }
        }
        ColBounds::Text {
            lower,
            upper,
            has_eq,
        } => {
            if let Some(s) = has_eq {
                if s.len() > MAX_SQL_INDEX_TEXT_BYTES {
                    return None;
                }
                return Some(Some(IndexValue::String(s.clone())));
            }
            if let (TextLower::Included(lo), TextUpper::Included(hi)) = (lower, upper) {
                if lo == hi {
                    if lo.len() > MAX_SQL_INDEX_TEXT_BYTES {
                        return None;
                    }
                    return Some(Some(IndexValue::String(lo.clone())));
                }
            }
            Some(None)
        }
        ColBounds::Bool {
            can_false,
            can_true,
        } => match (*can_false, *can_true) {
            (false, true) => Some(Some(IndexValue::Boolean(true))),
            (true, false) => Some(Some(IndexValue::Boolean(false))),
            _ => Some(None),
        },
        ColBounds::Real => Some(None),
    }
}

fn extract_index_range(col_bounds: &ColBounds) -> (Option<IndexBound>, Option<IndexBound>) {
    match col_bounds {
        ColBounds::Int {
            lo,
            hi,
            has_bound: true,
        } => {
            let lower = (*lo > i64::MIN).then_some(IndexBound {
                value: IndexValue::Integer(*lo),
                inclusive: true,
            });
            let upper = (*hi < i64::MAX).then_some(IndexBound {
                value: IndexValue::Integer(*hi),
                inclusive: true,
            });
            (lower, upper)
        }
        ColBounds::Text { lower, upper, .. } => {
            let lo_bound = match lower {
                TextLower::Unbounded => None,
                TextLower::Included(s) => {
                    if s.len() <= MAX_SQL_INDEX_TEXT_BYTES {
                        Some(IndexBound {
                            value: IndexValue::String(s.clone()),
                            inclusive: true,
                        })
                    } else {
                        let prefix = truncate_utf8_prefix(s, MAX_SQL_INDEX_TEXT_BYTES);
                        Some(IndexBound {
                            value: IndexValue::String(prefix.to_owned()),
                            inclusive: false,
                        })
                    }
                }
                TextLower::Excluded(s) => {
                    if s.len() <= MAX_SQL_INDEX_TEXT_BYTES {
                        Some(IndexBound {
                            value: IndexValue::String(s.clone()),
                            inclusive: false,
                        })
                    } else {
                        let prefix = truncate_utf8_prefix(s, MAX_SQL_INDEX_TEXT_BYTES);
                        Some(IndexBound {
                            value: IndexValue::String(prefix.to_owned()),
                            inclusive: false,
                        })
                    }
                }
            };
            let hi_bound = match upper {
                TextUpper::Unbounded => None,
                TextUpper::Included(s) => {
                    if s.len() <= MAX_SQL_INDEX_TEXT_BYTES {
                        Some(IndexBound {
                            value: IndexValue::String(s.clone()),
                            inclusive: true,
                        })
                    } else {
                        let prefix = truncate_utf8_prefix(s, MAX_SQL_INDEX_TEXT_BYTES);
                        (prefix.len() == MAX_SQL_INDEX_TEXT_BYTES).then(|| IndexBound {
                            value: IndexValue::String(prefix.to_owned()),
                            inclusive: true,
                        })
                    }
                }
                TextUpper::Excluded(s) => {
                    if s.len() <= MAX_SQL_INDEX_TEXT_BYTES {
                        Some(IndexBound {
                            value: IndexValue::String(s.clone()),
                            inclusive: false,
                        })
                    } else {
                        let prefix = truncate_utf8_prefix(s, MAX_SQL_INDEX_TEXT_BYTES);
                        (prefix.len() == MAX_SQL_INDEX_TEXT_BYTES).then(|| IndexBound {
                            value: IndexValue::String(prefix.to_owned()),
                            inclusive: true,
                        })
                    }
                }
            };
            (lo_bound, hi_bound)
        }
        _ => (None, None),
    }
}

fn check_index_satisfies_order_by(
    idx_def: &IndexDef,
    table: &TableDef,
    equal_len: usize,
    order_by: &[BoundOrderBy],
) -> bool {
    let eq_cols = &idx_def.columns[..equal_len];
    let mut pos = 0;
    while pos < order_by.len() && eq_cols.contains(&order_by[pos].col_idx) {
        pos += 1;
    }
    let rem_order = &order_by[pos..];
    let rem_cols = &idx_def.columns[equal_len..];

    if rem_order.is_empty() {
        return rem_cols.is_empty();
    }
    if rem_order.iter().any(|o| o.desc) {
        return false;
    }
    if rem_order.len() == rem_cols.len() {
        return rem_order.iter().zip(rem_cols).all(|(o, &c)| o.col_idx == c);
    }
    if rem_order.len() == rem_cols.len() + 1
        && rem_order[rem_cols.len()].col_idx == table.primary_key_index
    {
        return rem_order[..rem_cols.len()]
            .iter()
            .zip(rem_cols)
            .all(|(o, &c)| o.col_idx == c);
    }
    false
}

fn build_pk_plan(pk_bounds: &ColBounds, pk_type: SqlType) -> AccessPlan {
    match (pk_type, pk_bounds) {
        (SqlType::Integer, ColBounds::Int { lo, hi, has_bound }) => {
            if !has_bound {
                return AccessPlan::FullScan;
            }
            if lo == hi {
                return AccessPlan::PkPoint {
                    doc_id: encode_i64_pk(*lo),
                };
            }
            if *lo == i64::MIN && *hi == i64::MAX {
                return AccessPlan::FullScan;
            }
            let after_id = (*lo > i64::MIN).then(|| encode_i64_pk(*lo - 1));
            let upper = if *hi == i64::MAX {
                UpperBound::Unbounded
            } else {
                UpperBound::Included(encode_i64_pk(*hi))
            };
            let max_exact_rows = usize::try_from(*hi as i128 - *lo as i128 + 1).ok();
            AccessPlan::PkRange {
                after_id,
                upper,
                max_exact_rows,
            }
        }
        (
            SqlType::Text { .. },
            ColBounds::Text {
                lower,
                upper,
                has_eq,
            },
        ) => {
            if has_eq.is_some() {
                if let TextLower::Included(s) = lower {
                    return AccessPlan::PkPoint {
                        doc_id: format!("t:{s}"),
                    };
                }
            }
            if let (TextLower::Included(lo), TextUpper::Included(hi)) = (lower, upper) {
                if lo == hi && lo.len() <= MAX_SQL_TEXT_PK_BYTES {
                    return AccessPlan::PkPoint {
                        doc_id: format!("t:{lo}"),
                    };
                }
            }
            if matches!(lower, TextLower::Unbounded) && matches!(upper, TextUpper::Unbounded) {
                return AccessPlan::FullScan;
            }
            let after_id = match lower {
                TextLower::Unbounded => None,
                TextLower::Excluded(s) => {
                    let bound = truncate_utf8_prefix(s, MAX_SQL_TEXT_PK_BYTES);
                    Some(format!("t:{bound}"))
                }
                TextLower::Included(s) => {
                    if s.is_empty() {
                        Some("t".to_owned())
                    } else if s.len() <= MAX_SQL_TEXT_PK_BYTES {
                        Some(format!("t:{}", predecessor_text_pk(s)))
                    } else {
                        let bound = truncate_utf8_prefix(s, MAX_SQL_TEXT_PK_BYTES);
                        Some(format!("t:{bound}"))
                    }
                }
            };
            let upper_bound = match upper {
                TextUpper::Unbounded => UpperBound::Unbounded,
                TextUpper::Included(s) => UpperBound::Included(format!("t:{s}")),
                TextUpper::Excluded(s) => UpperBound::Excluded(format!("t:{s}")),
            };
            AccessPlan::PkRange {
                after_id,
                upper: upper_bound,
                max_exact_rows: None,
            }
        }
        _ => AccessPlan::FullScan,
    }
}

fn tighten_col_bound(bounds: &mut ColBounds, is_pk: bool, op: CompareOp, val: &SqlValue) -> bool {
    match bounds {
        ColBounds::Int { lo, hi, has_bound } => {
            let Some((cond_lo, cond_hi)) = integer_bound_for_op(op, val) else {
                return false;
            };
            *lo = (*lo).max(cond_lo);
            *hi = (*hi).min(cond_hi);
            *has_bound = true;
            *lo <= *hi
        }
        ColBounds::Text {
            lower,
            upper,
            has_eq,
        } => {
            let SqlValue::Text(s) = val else {
                return false;
            };
            let max_eq_len = if is_pk {
                MAX_SQL_TEXT_PK_BYTES
            } else {
                MAX_SQL_INDEX_TEXT_BYTES
            };
            match op {
                CompareOp::Eq => {
                    if s.len() > max_eq_len {
                        return false;
                    }
                    lower.tighten(TextLower::Included(s.clone()));
                    upper.tighten(TextUpper::Included(s.clone()));
                    *has_eq = Some(s.clone());
                }
                CompareOp::Ge => lower.tighten(TextLower::Included(s.clone())),
                CompareOp::Gt => lower.tighten(TextLower::Excluded(s.clone())),
                CompareOp::Le => upper.tighten(TextUpper::Included(s.clone())),
                CompareOp::Lt => upper.tighten(TextUpper::Excluded(s.clone())),
                CompareOp::Ne => {}
            }
            !text_bounds_empty(lower, upper, is_pk)
        }
        ColBounds::Bool {
            can_false,
            can_true,
        } => {
            let SqlValue::Boolean(b) = val else {
                return false;
            };
            match (op, *b) {
                (CompareOp::Eq, true) | (CompareOp::Gt, false) | (CompareOp::Ge, true) => {
                    *can_false = false;
                }
                (CompareOp::Eq, false) | (CompareOp::Lt, true) | (CompareOp::Le, false) => {
                    *can_true = false;
                }
                (CompareOp::Gt, true) | (CompareOp::Lt, false) => {
                    *can_false = false;
                    *can_true = false;
                }
                (CompareOp::Ge, false) | (CompareOp::Le, true) | (CompareOp::Ne, _) => {}
            }
            *can_false || *can_true
        }
        ColBounds::Real => true,
    }
}

fn collect_conjuncts<'a>(expr: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
    match expr {
        BoundExpr::And(items) => {
            for item in items {
                collect_conjuncts(item, out);
            }
        }
        other => out.push(other),
    }
}

fn flip_op(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
    }
}

fn extract_col_predicate<'a>(
    op: CompareOp,
    left: &'a BoundExpr,
    right: &'a BoundExpr,
    params: &'a [SqlValue],
) -> Option<(usize, CompareOp, &'a SqlValue)> {
    match (left, right) {
        (BoundExpr::Column(idx), BoundExpr::Literal(val)) => Some((*idx, op, val)),
        (BoundExpr::Column(idx), BoundExpr::Param(p)) => Some((*idx, op, &params[*p])),
        (BoundExpr::Literal(val), BoundExpr::Column(idx)) => Some((*idx, flip_op(op), val)),
        (BoundExpr::Param(p), BoundExpr::Column(idx)) => Some((*idx, flip_op(op), &params[*p])),
        _ => None,
    }
}

fn integer_bound_for_op(op: CompareOp, val: &SqlValue) -> Option<(i64, i64)> {
    match val {
        SqlValue::Integer(n) => {
            let n = *n;
            match op {
                CompareOp::Eq => Some((n, n)),
                CompareOp::Ge => Some((n, i64::MAX)),
                CompareOp::Gt => n.checked_add(1).map(|lo| (lo, i64::MAX)),
                CompareOp::Le => Some((i64::MIN, n)),
                CompareOp::Lt => n.checked_sub(1).map(|hi| (i64::MIN, hi)),
                CompareOp::Ne => None,
            }
        }
        SqlValue::Real(f) => {
            let f = *f;
            debug_assert!(f.is_finite());
            match op {
                CompareOp::Eq => {
                    if !(-TWO_POW_63..TWO_POW_63).contains(&f) || f.fract() != 0.0 {
                        None
                    } else {
                        let n = f as i64;
                        Some((n, n))
                    }
                }
                CompareOp::Ge => {
                    if f >= TWO_POW_63 {
                        None
                    } else if f <= -TWO_POW_63 {
                        Some((i64::MIN, i64::MAX))
                    } else {
                        Some((f.ceil() as i64, i64::MAX))
                    }
                }
                CompareOp::Gt => {
                    if f >= TWO_POW_63 {
                        None
                    } else if f < -TWO_POW_63 {
                        Some((i64::MIN, i64::MAX))
                    } else {
                        // -2^63 <= f < 2^63, so f.floor() <= 2^63 - 1024 < i64::MAX.
                        let fi = f.floor() as i64;
                        fi.checked_add(1).map(|lo| (lo, i64::MAX))
                    }
                }
                CompareOp::Le => {
                    if f < -TWO_POW_63 {
                        None
                    } else if f >= TWO_POW_63 {
                        Some((i64::MIN, i64::MAX))
                    } else {
                        Some((i64::MIN, f.floor() as i64))
                    }
                }
                CompareOp::Lt => {
                    if f <= -TWO_POW_63 {
                        None
                    } else if f >= TWO_POW_63 {
                        Some((i64::MIN, i64::MAX))
                    } else {
                        let ci = f.ceil() as i64;
                        ci.checked_sub(1).map(|hi| (i64::MIN, hi))
                    }
                }
                CompareOp::Ne => None,
            }
        }
        _ => None,
    }
}

#[derive(Clone)]
enum TextLower {
    Unbounded,
    Included(String),
    Excluded(String),
}

#[derive(Clone)]
enum TextUpper {
    Unbounded,
    Included(String),
    Excluded(String),
}

impl TextLower {
    fn tighten(&mut self, incoming: Self) {
        match (&*self, &incoming) {
            (Self::Unbounded, _) => *self = incoming,
            (_, Self::Unbounded) => {}
            (Self::Included(a), Self::Included(b)) => {
                if b.as_bytes() > a.as_bytes() {
                    *self = incoming;
                }
            }
            (Self::Excluded(a), Self::Excluded(b)) => {
                if b.as_bytes() > a.as_bytes() {
                    *self = incoming;
                }
            }
            (Self::Included(a), Self::Excluded(b)) => {
                if a.as_bytes() <= b.as_bytes() {
                    *self = incoming;
                }
            }
            (Self::Excluded(a), Self::Included(b)) => {
                if b.as_bytes() > a.as_bytes() {
                    *self = incoming;
                }
            }
        }
    }
}

impl TextUpper {
    fn tighten(&mut self, incoming: Self) {
        match (&*self, &incoming) {
            (Self::Unbounded, _) => *self = incoming,
            (_, Self::Unbounded) => {}
            (Self::Included(a), Self::Included(b)) => {
                if b.as_bytes() < a.as_bytes() {
                    *self = incoming;
                }
            }
            (Self::Excluded(a), Self::Excluded(b)) => {
                if b.as_bytes() < a.as_bytes() {
                    *self = incoming;
                }
            }
            (Self::Included(a), Self::Excluded(b)) => {
                if a.as_bytes() >= b.as_bytes() {
                    *self = incoming;
                }
            }
            (Self::Excluded(a), Self::Included(b)) => {
                if b.as_bytes() < a.as_bytes() {
                    *self = incoming;
                }
            }
        }
    }
}

fn text_bounds_empty(lower: &TextLower, upper: &TextUpper, is_pk: bool) -> bool {
    if matches!(upper, TextUpper::Excluded(s) if s.is_empty()) {
        return true;
    }
    let max_pk = MAX_SQL_TEXT_PK_BYTES;
    match (lower, upper) {
        (TextLower::Included(lo), TextUpper::Included(hi)) => {
            lo.as_bytes() > hi.as_bytes() || (is_pk && lo == hi && lo.len() > max_pk)
        }
        (TextLower::Included(lo), TextUpper::Excluded(hi))
        | (TextLower::Excluded(lo), TextUpper::Included(hi))
        | (TextLower::Excluded(lo), TextUpper::Excluded(hi)) => lo.as_bytes() >= hi.as_bytes(),
        _ => false,
    }
}

fn truncate_utf8_prefix(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn prev_scalar(c: char) -> char {
    let code = c as u32;
    if code == 0xE000 {
        '\u{D7FF}'
    } else {
        char::from_u32(code - 1).expect("non-zero scalar has valid predecessor")
    }
}

fn predecessor_text_pk(s: &str) -> String {
    let mut chars = s.char_indices();
    let (last_idx, last_char) = chars.next_back().expect("non-empty string");
    let prefix = &s[..last_idx];
    if last_char == '\0' {
        return prefix.to_owned();
    }
    let prev = prev_scalar(last_char);
    let mut out = String::with_capacity(MAX_SQL_TEXT_PK_BYTES);
    out.push_str(prefix);
    out.push(prev);
    while out.len() + 4 <= MAX_SQL_TEXT_PK_BYTES {
        out.push('\u{10ffff}');
    }
    match MAX_SQL_TEXT_PK_BYTES - out.len() {
        0 => {}
        1 => out.push('\u{7f}'),
        2 => out.push('\u{7ff}'),
        3 => out.push('\u{ffff}'),
        _ => unreachable!(),
    }
    out
}
