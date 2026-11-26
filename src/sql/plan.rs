//! Primary-key access path planning for SQL `SELECT`, `UPDATE`, and `DELETE`.
use super::catalog::TableDef;
use super::exec::BoundExpr;
use super::parser::CompareOp;
use super::value::{MAX_SQL_TEXT_PK_BYTES, SqlType, SqlValue, encode_i64_pk};

const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

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
    Point {
        doc_id: String,
    },
    Range {
        after_id: Option<String>,
        upper: UpperBound,
        max_exact_rows: Option<usize>,
    },
    FullScan,
}

pub(crate) fn plan_access(
    filter: Option<&BoundExpr>,
    table: &TableDef,
    params: &[SqlValue],
) -> AccessPlan {
    let Some(expr) = filter else {
        return AccessPlan::FullScan;
    };
    let mut conjuncts = Vec::new();
    collect_conjuncts(expr, &mut conjuncts);

    let pk_idx = table.primary_key_index;
    let pk_type = table.columns[pk_idx].col_type;

    match pk_type {
        SqlType::Integer => plan_integer_pk(&conjuncts, pk_idx, params),
        SqlType::Text { .. } => plan_text_pk(&conjuncts, pk_idx, params),
        _ => AccessPlan::FullScan,
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

fn extract_pk_predicate<'a>(
    expr: &'a BoundExpr,
    pk_idx: usize,
    params: &'a [SqlValue],
) -> Option<(CompareOp, &'a SqlValue)> {
    let BoundExpr::Compare { op, left, right } = expr else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (BoundExpr::Column(idx), BoundExpr::Literal(val)) if *idx == pk_idx => Some((*op, val)),
        (BoundExpr::Column(idx), BoundExpr::Param(p)) if *idx == pk_idx => Some((*op, &params[*p])),
        (BoundExpr::Literal(val), BoundExpr::Column(idx)) if *idx == pk_idx => {
            Some((flip_op(*op), val))
        }
        (BoundExpr::Param(p), BoundExpr::Column(idx)) if *idx == pk_idx => {
            Some((flip_op(*op), &params[*p]))
        }
        _ => None,
    }
}

fn plan_integer_pk(conjuncts: &[&BoundExpr], pk_idx: usize, params: &[SqlValue]) -> AccessPlan {
    let mut lo = i64::MIN;
    let mut hi = i64::MAX;
    let mut has_eq = false;
    let mut has_bound = false;

    for expr in conjuncts {
        let Some((op, val)) = extract_pk_predicate(expr, pk_idx, params) else {
            continue;
        };
        if matches!(val, SqlValue::Null) {
            return AccessPlan::Empty;
        }
        if op == CompareOp::Ne {
            continue;
        }
        let Some((cond_lo, cond_hi)) = integer_bound_for_op(op, val) else {
            return AccessPlan::Empty;
        };
        lo = lo.max(cond_lo);
        hi = hi.min(cond_hi);
        has_bound = true;
        if op == CompareOp::Eq {
            has_eq = true;
        }
        if lo > hi {
            return AccessPlan::Empty;
        }
    }

    if has_eq {
        debug_assert_eq!(lo, hi);
        return AccessPlan::Point {
            doc_id: encode_i64_pk(lo),
        };
    }
    if !has_bound || (lo == i64::MIN && hi == i64::MAX) {
        return AccessPlan::FullScan;
    }

    let after_id = (lo > i64::MIN).then(|| encode_i64_pk(lo - 1));
    let upper = if hi == i64::MAX {
        UpperBound::Unbounded
    } else {
        UpperBound::Included(encode_i64_pk(hi))
    };
    let max_exact_rows = usize::try_from(hi as i128 - lo as i128 + 1).ok();
    AccessPlan::Range {
        after_id,
        upper,
        max_exact_rows,
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

fn text_bounds_empty(lower: &TextLower, upper: &TextUpper) -> bool {
    if matches!(upper, TextUpper::Excluded(s) if s.is_empty()) {
        return true;
    }
    match (lower, upper) {
        (TextLower::Included(lo), TextUpper::Included(hi)) => {
            lo.as_bytes() > hi.as_bytes() || (lo == hi && lo.len() > MAX_SQL_TEXT_PK_BYTES)
        }
        (TextLower::Included(lo), TextUpper::Excluded(hi))
        | (TextLower::Excluded(lo), TextUpper::Included(hi))
        | (TextLower::Excluded(lo), TextUpper::Excluded(hi)) => lo.as_bytes() >= hi.as_bytes(),
        _ => false,
    }
}

fn plan_text_pk(conjuncts: &[&BoundExpr], pk_idx: usize, params: &[SqlValue]) -> AccessPlan {
    let mut lower = TextLower::Unbounded;
    let mut upper = TextUpper::Unbounded;
    let mut has_eq = false;

    for expr in conjuncts {
        let Some((op, val)) = extract_pk_predicate(expr, pk_idx, params) else {
            continue;
        };
        if matches!(val, SqlValue::Null) {
            return AccessPlan::Empty;
        }
        if op == CompareOp::Ne {
            continue;
        }
        let SqlValue::Text(s) = val else {
            return AccessPlan::Empty;
        };
        match op {
            CompareOp::Eq => {
                if s.len() > MAX_SQL_TEXT_PK_BYTES {
                    return AccessPlan::Empty;
                }
                lower.tighten(TextLower::Included(s.clone()));
                upper.tighten(TextUpper::Included(s.clone()));
                has_eq = true;
            }
            CompareOp::Ge => lower.tighten(TextLower::Included(s.clone())),
            CompareOp::Gt => lower.tighten(TextLower::Excluded(s.clone())),
            CompareOp::Le => upper.tighten(TextUpper::Included(s.clone())),
            CompareOp::Lt => upper.tighten(TextUpper::Excluded(s.clone())),
            CompareOp::Ne => {}
        }
        if text_bounds_empty(&lower, &upper) {
            return AccessPlan::Empty;
        }
    }

    if has_eq {
        if let TextLower::Included(s) = lower {
            return AccessPlan::Point {
                doc_id: format!("t:{s}"),
            };
        }
        unreachable!("has_eq guarantees TextLower::Included");
    }

    if matches!(lower, TextLower::Unbounded) && matches!(upper, TextUpper::Unbounded) {
        return AccessPlan::FullScan;
    }

    let after_id = match &lower {
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

    AccessPlan::Range {
        after_id,
        upper: upper_bound,
        max_exact_rows: None,
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
