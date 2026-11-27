//! Aggregates, `GROUP BY` and `HAVING` over a join.
//!
//! The join runs as an ordinary join that returns only the columns the
//! aggregate needs, with no ordering or limit, so it keeps every access
//! path, bound and isolation property of a join. Its rows, at most
//! `max_sort_rows` of them, are then grouped and aggregated with the same
//! accumulators as single-table aggregates; `HAVING`, `ORDER BY`, `OFFSET`
//! and `LIMIT` apply to the groups.
use super::{
    AggCall, ColRef, JExpr, JoinSelect, OrderItem, SelectItem, limit_value, run as run_join,
};
use crate::document::DocumentTransaction;
use crate::sql::catalog::{self, ColumnDef};
use crate::sql::exec::{AggAcc, BoundAggregate, SqlCache, SqlValueRef, compare_order_cell_ref};
use crate::sql::parser::{AggregateKind, CompareOp};
use crate::sql::value::{SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;

/// Whether the statement aggregates: it has an aggregate item or `GROUP BY`.
pub(super) fn is_aggregate(statement: &JoinSelect) -> bool {
    !statement.group_by.is_empty()
        || statement.having.is_some()
        || statement
            .items
            .iter()
            .any(|item| matches!(item, SelectItem::Aggregate(..)))
}

/// One resolved column: table position and column position.
type Resolved = (usize, usize);

struct Resolver<'a> {
    names: Vec<String>,
    aliases: Vec<String>,
    defs: Vec<&'a catalog::TableDef>,
}

impl Resolver<'_> {
    fn resolve(&self, column: &ColRef) -> Result<(Resolved, &ColumnDef), SqlError> {
        let found: Vec<Resolved> = match &column.qualifier {
            Some(qualifier) => {
                let table = self
                    .aliases
                    .iter()
                    .position(|alias| alias == qualifier)
                    .ok_or_else(|| SqlError::Syntax {
                        offset: column.offset,
                        message: format!("unknown table or alias '{qualifier}'"),
                    })?;
                self.defs[table]
                    .column_index(&column.column)
                    .map(|c| vec![(table, c)])
                    .unwrap_or_default()
            }
            None => self
                .defs
                .iter()
                .enumerate()
                .filter_map(|(t, def)| def.column_index(&column.column).map(|c| (t, c)))
                .collect(),
        };
        match found.as_slice() {
            [one] => Ok((*one, &self.defs[one.0].columns[one.1])),
            [] => Err(SqlError::NoSuchColumn {
                table: column
                    .qualifier
                    .as_ref()
                    .and_then(|q| self.aliases.iter().position(|a| a == q))
                    .map_or_else(|| self.names.join(", "), |t| self.names[t].clone()),
                column: column.column.clone(),
            }),
            _ => Err(SqlError::Syntax {
                offset: column.offset,
                message: format!("column name '{}' is ambiguous", column.column),
            }),
        }
    }
}

/// A group key cell, hashable and exact; `-0.0` and `0.0` are one key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Key {
    Null,
    Integer(i64),
    Real(u64),
    Text(String),
    Boolean(bool),
}

impl Key {
    fn of(value: &SqlValue) -> Self {
        match value {
            SqlValue::Null => Self::Null,
            SqlValue::Integer(n) => Self::Integer(*n),
            SqlValue::Real(f) => Self::Real(if *f == 0.0 {
                0.0_f64.to_bits()
            } else {
                f.to_bits()
            }),
            SqlValue::Text(s) => Self::Text(s.clone()),
            SqlValue::Boolean(b) => Self::Boolean(*b),
        }
    }
}

fn value_ref(value: &SqlValue) -> SqlValueRef<'_> {
    match value {
        SqlValue::Null => SqlValueRef::Null,
        SqlValue::Integer(n) => SqlValueRef::Integer(*n),
        SqlValue::Real(f) => SqlValueRef::Real(*f),
        SqlValue::Text(s) => SqlValueRef::Text(s.as_str()),
        SqlValue::Boolean(b) => SqlValueRef::Boolean(*b),
    }
}

/// What each output column of a group is.
enum Output {
    Group(usize),
    Agg(usize),
}

struct Bound {
    group_cols: Vec<usize>,
    aggs: Vec<(AggCall, BoundAggregate)>,
    outputs: Vec<Output>,
    names: Vec<String>,
    /// Output aliases, for `ORDER BY` and `HAVING`.
    aliases: Vec<Option<String>>,
    /// Columns the inner join returns, in order.
    needed: Vec<Resolved>,
}

fn aggregate_name(call: &AggCall) -> String {
    let arg = call
        .arg
        .as_ref()
        .map_or(String::new(), |c| c.column.clone());
    match call.kind {
        AggregateKind::CountStar => "count(*)".into(),
        AggregateKind::CountDistinct => format!("count(distinct {arg})"),
        kind => format!("{}({arg})", kind.sql_name()),
    }
}

fn bind(statement: &JoinSelect, resolver: &Resolver<'_>) -> Result<Bound, SqlError> {
    let mut needed: Vec<Resolved> = Vec::new();
    let mut need = |r: Resolved| match needed.iter().position(|x| *x == r) {
        Some(i) => i,
        None => {
            needed.push(r);
            needed.len() - 1
        }
    };
    let mut group_keys: Vec<Resolved> = Vec::new();
    let mut group_cols = Vec::new();
    for column in &statement.group_by {
        let (r, _) = resolver.resolve(column)?;
        if !group_keys.contains(&r) {
            group_keys.push(r);
            group_cols.push(need(r));
        }
    }
    let mut aggs: Vec<(AggCall, BoundAggregate)> = Vec::new();
    let mut bind_agg =
        |call: &AggCall, need: &mut dyn FnMut(Resolved) -> usize| -> Result<usize, SqlError> {
            if let Some(i) = aggs
                .iter()
                .position(|(c, _)| c.kind == call.kind && c.arg == call.arg)
            {
                return Ok(i);
            }
            let (col_idx, col_type) = match &call.arg {
                None => (None, None),
                Some(column) => {
                    let (r, def) = resolver.resolve(column)?;
                    if matches!(call.kind, AggregateKind::Sum | AggregateKind::Avg)
                        && !matches!(def.col_type, SqlType::Integer | SqlType::Real)
                    {
                        return Err(SqlError::Type(format!(
                            "{} requires an INTEGER or REAL column, got {}",
                            call.kind.sql_name(),
                            def.col_type.display_name()
                        )));
                    }
                    (Some(need(r)), Some(def.col_type))
                }
            };
            aggs.push((
                call.clone(),
                BoundAggregate {
                    kind: call.kind,
                    col_idx,
                    col_type,
                },
            ));
            Ok(aggs.len() - 1)
        };
    let mut outputs = Vec::new();
    let mut names = Vec::new();
    let mut aliases = Vec::new();
    for item in &statement.items {
        match item {
            SelectItem::Column(column) => {
                let (r, def) = resolver.resolve(column)?;
                let position =
                    group_keys
                        .iter()
                        .position(|k| *k == r)
                        .ok_or_else(|| SqlError::Syntax {
                            offset: column.offset,
                            message: format!(
                                "column '{}' must appear in GROUP BY or inside an aggregate",
                                column.column
                            ),
                        })?;
                outputs.push(Output::Group(position));
                names.push(def.name.clone());
                aliases.push(None);
            }
            SelectItem::Aggregate(call, alias) => {
                outputs.push(Output::Agg(bind_agg(call, &mut need)?));
                names.push(alias.clone().unwrap_or_else(|| aggregate_name(call)));
                aliases.push(alias.clone());
            }
            SelectItem::All | SelectItem::TableAll(..) => {
                return Err(SqlError::Unsupported {
                    feature: "* in an aggregate join".into(),
                    offset: None,
                });
            }
        }
    }
    // Aggregates used only in HAVING are computed too.
    if let Some(having) = &statement.having {
        collect_having_aggs(having, &mut |call| bind_agg(call, &mut need).map(|_| ()))?;
    }
    Ok(Bound {
        group_cols,
        aggs,
        outputs,
        names,
        aliases,
        needed,
    })
}

fn collect_having_aggs(
    expr: &JExpr,
    f: &mut dyn FnMut(&AggCall) -> Result<(), SqlError>,
) -> Result<(), SqlError> {
    match expr {
        JExpr::Agg(call) => f(call),
        JExpr::IsNull(inner, _) | JExpr::Not(inner) => collect_having_aggs(inner, f),
        JExpr::Compare(_, left, right) => {
            collect_having_aggs(left, f)?;
            collect_having_aggs(right, f)
        }
        JExpr::And(items) | JExpr::Or(items) => items
            .iter()
            .try_for_each(|item| collect_having_aggs(item, f)),
        JExpr::Literal(_) | JExpr::Param(_) | JExpr::Column(_) => Ok(()),
    }
}

/// One finished group: its key cells and aggregate results.
struct Group {
    keys: Vec<SqlValue>,
    aggs: Vec<SqlValue>,
}

/// Evaluates `HAVING` over a group in three-valued logic.
fn eval_having(
    expr: &JExpr,
    group: &Group,
    bound: &Bound,
    resolver: &Resolver<'_>,
    group_keys: &[Resolved],
    params: &[SqlValue],
) -> Result<Option<bool>, SqlError> {
    let value =
        |e: &JExpr| -> Result<SqlValue, SqlError> {
            Ok(match e {
                JExpr::Literal(v) => v.clone(),
                JExpr::Param(i) => params.get(*i).cloned().unwrap_or(SqlValue::Null),
                JExpr::Agg(call) => {
                    let i = bound
                        .aggs
                        .iter()
                        .position(|(c, _)| c.kind == call.kind && c.arg == call.arg)
                        .expect("HAVING aggregates are bound");
                    group.aggs[i].clone()
                }
                JExpr::Column(column) => {
                    if column.qualifier.is_none() {
                        if let Some(i) = bound
                            .aliases
                            .iter()
                            .position(|a| a.as_deref() == Some(column.column.as_str()))
                        {
                            return Ok(output_value(&bound.outputs[i], group));
                        }
                    }
                    let (r, _) = resolver.resolve(column)?;
                    let i = group_keys.iter().position(|k| *k == r).ok_or_else(|| {
                        SqlError::Syntax {
                            offset: column.offset,
                            message: format!(
                                "HAVING column '{}' must appear in GROUP BY or inside an aggregate",
                                column.column
                            ),
                        }
                    })?;
                    group.keys[i].clone()
                }
                _ => unreachable!("only operands are valued"),
            })
        };
    Ok(match expr {
        JExpr::And(items) => {
            let mut result = Some(true);
            for item in items {
                match eval_having(item, group, bound, resolver, group_keys, params)? {
                    Some(false) => return Ok(Some(false)),
                    None => result = None,
                    Some(true) => {}
                }
            }
            result
        }
        JExpr::Or(items) => {
            let mut result = Some(false);
            for item in items {
                match eval_having(item, group, bound, resolver, group_keys, params)? {
                    Some(true) => return Ok(Some(true)),
                    None => result = None,
                    Some(false) => {}
                }
            }
            result
        }
        JExpr::Not(inner) => {
            eval_having(inner, group, bound, resolver, group_keys, params)?.map(|b| !b)
        }
        JExpr::IsNull(inner, negated) => Some(matches!(value(inner)?, SqlValue::Null) != *negated),
        JExpr::Compare(op, left, right) => {
            let (left, right) = (value(left)?, value(right)?);
            compare(&left, &right)?.map(|ordering| match op {
                CompareOp::Eq => ordering == Ordering::Equal,
                CompareOp::Ne => ordering != Ordering::Equal,
                CompareOp::Lt => ordering == Ordering::Less,
                CompareOp::Le => ordering != Ordering::Greater,
                CompareOp::Gt => ordering == Ordering::Greater,
                CompareOp::Ge => ordering != Ordering::Less,
            })
        }
        other => match value(other)? {
            SqlValue::Boolean(b) => Some(b),
            SqlValue::Null => None,
            _ => {
                return Err(SqlError::Type("HAVING requires a boolean condition".into()));
            }
        },
    })
}

/// SQL comparison: `None` when either side is NULL; numbers compare exactly
/// across INTEGER and REAL; other kinds must match.
fn compare(left: &SqlValue, right: &SqlValue) -> Result<Option<Ordering>, SqlError> {
    Ok(match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => None,
        (SqlValue::Integer(a), SqlValue::Integer(b)) => Some(a.cmp(b)),
        (SqlValue::Real(a), SqlValue::Real(b)) => a.partial_cmp(b),
        (SqlValue::Integer(a), SqlValue::Real(b)) => compare_int_real(*a, *b),
        (SqlValue::Real(a), SqlValue::Integer(b)) => {
            compare_int_real(*b, *a).map(Ordering::reverse)
        }
        (SqlValue::Text(a), SqlValue::Text(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
        (SqlValue::Boolean(a), SqlValue::Boolean(b)) => Some(a.cmp(b)),
        _ => {
            return Err(SqlError::Type(format!(
                "cannot compare {} with {}",
                left.kind_name(),
                right.kind_name()
            )));
        }
    })
}

fn compare_int_real(int: i64, real: f64) -> Option<Ordering> {
    if real.is_nan() {
        return None;
    }
    if real >= 9_223_372_036_854_775_808.0 {
        return Some(Ordering::Less);
    }
    if real < -9_223_372_036_854_775_808.0 {
        return Some(Ordering::Greater);
    }
    let truncated = real.trunc();
    let whole = truncated as i64;
    Some(int.cmp(&whole).then_with(|| {
        if real > truncated {
            Ordering::Less
        } else if real < truncated {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }))
}

fn output_value(output: &Output, group: &Group) -> SqlValue {
    match output {
        Output::Group(i) => group.keys[*i].clone(),
        Output::Agg(i) => group.aggs[*i].clone(),
    }
}

/// The bound aggregate and the inner join that feeds it.
pub(super) struct Prepared {
    names: Vec<String>,
    aliases: Vec<String>,
    tables: Vec<std::sync::Arc<catalog::CachedTable>>,
    bound: Bound,
    pub(super) inner: JoinSelect,
}

impl Prepared {
    fn resolver(&self) -> Resolver<'_> {
        Resolver {
            names: self.names.clone(),
            aliases: self.aliases.clone(),
            defs: self.tables.iter().map(|t| &*t.def).collect(),
        }
    }

    /// One line for `EXPLAIN`.
    pub(super) fn describe(&self) -> String {
        format!(
            "{} group column(s), {} aggregate(s) over at most max_sort_rows joined rows",
            self.bound.group_cols.len(),
            self.bound.aggs.len()
        )
    }
}

pub(super) fn prepare(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    cache: &SqlCache,
) -> Result<Prepared, SqlError> {
    let mut tables = Vec::with_capacity(statement.tables.len());
    for table in &statement.tables {
        tables.push(catalog::load_table_cached(tx, &table.name, cache.tables())?);
    }
    let mut prepared = Prepared {
        names: statement.tables.iter().map(|t| t.name.clone()).collect(),
        aliases: statement.tables.iter().map(|t| t.alias.clone()).collect(),
        tables,
        bound: Bound {
            group_cols: Vec::new(),
            aggs: Vec::new(),
            outputs: Vec::new(),
            names: Vec::new(),
            aliases: Vec::new(),
            needed: Vec::new(),
        },
        inner: JoinSelect {
            items: Vec::new(),
            tables: statement.tables.clone(),
            joins: statement.joins.clone(),
            filter: statement.filter.clone(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            group_by: Vec::new(),
            having: None,
            cached_plan: RwLock::new(None),
        },
    };
    let resolver = prepared.resolver();
    let bound = bind(statement, &resolver)?;
    // An aggregate with no column needs one row per joined row, so the inner
    // join then returns the first table's key.
    let mut needed = bound.needed.clone();
    if needed.is_empty() {
        needed.push((0, resolver.defs[0].primary_key_index));
    }
    let items = needed
        .iter()
        .map(|&(t, c)| {
            SelectItem::Column(ColRef {
                qualifier: Some(resolver.aliases[t].clone()),
                column: resolver.defs[t].columns[c].name.clone(),
                offset: 0,
            })
        })
        .collect();
    drop(resolver);
    prepared.inner.items = items;
    prepared.bound = bound;
    Ok(prepared)
}

/// Runs an aggregate join.
pub(super) fn run(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let prepared = prepare(tx, statement, cache)?;
    let (resolver, bound, inner) = (&prepared.resolver(), &prepared.bound, &prepared.inner);
    let group_keys: Vec<Resolved> = bound.group_cols.iter().map(|&i| bound.needed[i]).collect();
    let order = order_plan(&statement.order_by, bound, resolver, &group_keys)?;
    let inner_limits = SqlLimits {
        max_select_rows: limits.max_sort_rows,
        ..*limits
    };
    let rows = match run_join(tx, inner, params, &inner_limits, cache) {
        Ok(SqlOutcome::Rows { rows, .. }) => rows,
        Ok(_) => unreachable!("a join returns rows"),
        Err(SqlError::Bounds(message)) if message.contains("max_select_rows") => {
            return Err(SqlError::Bounds(format!(
                "an aggregate join reads at most max_sort_rows ({}) joined rows",
                limits.max_sort_rows
            )));
        }
        Err(error) => return Err(error),
    };

    let hash_builder = RandomState::new();
    let mut total_distinct = 0usize;
    let mut index: HashMap<Vec<Key>, usize> = HashMap::new();
    let mut groups: Vec<(Vec<SqlValue>, Vec<AggAcc>)> = Vec::new();
    if bound.group_cols.is_empty() {
        groups.push((
            Vec::new(),
            bound.aggs.iter().map(|(_, b)| AggAcc::new(b)).collect(),
        ));
        index.insert(Vec::new(), 0);
    }
    for row in &rows {
        let keys: Vec<SqlValue> = bound.group_cols.iter().map(|&i| row[i].clone()).collect();
        let key: Vec<Key> = keys.iter().map(Key::of).collect();
        let slot = match index.get(&key) {
            Some(&slot) => slot,
            None => {
                if groups.len() >= limits.max_groups {
                    return Err(SqlError::Bounds(format!(
                        "GROUP BY group count exceeds max_groups ({})",
                        limits.max_groups
                    )));
                }
                groups.push((
                    keys,
                    bound.aggs.iter().map(|(_, b)| AggAcc::new(b)).collect(),
                ));
                index.insert(key, groups.len() - 1);
                groups.len() - 1
            }
        };
        for (acc, (_, spec)) in groups[slot].1.iter_mut().zip(&bound.aggs) {
            let value = spec.col_idx.map(|i| value_ref(&row[i]));
            acc.update(
                value,
                &hash_builder,
                &mut total_distinct,
                limits.max_distinct_values,
            )?;
        }
    }

    let mut finished = Vec::with_capacity(groups.len());
    for (keys, accs) in groups {
        let aggs = accs
            .into_iter()
            .map(AggAcc::finalize)
            .collect::<Result<Vec<_>, _>>()?;
        let group = Group { keys, aggs };
        if let Some(having) = &statement.having {
            if eval_having(having, &group, bound, resolver, &group_keys, params)? != Some(true) {
                continue;
            }
        }
        finished.push(group);
    }
    if !order.is_empty() {
        finished.sort_by(|a, b| {
            for (target, descending, nulls_first) in &order {
                let (x, y) = (output_value(target, a), output_value(target, b));
                let ordering =
                    compare_order_cell_ref(value_ref(&x), value_ref(&y), *descending, *nulls_first);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });
    }
    let limit = limit_value(statement.limit, params)?;
    let offset = limit_value(statement.offset, params)?.unwrap_or(0);
    let mut out = Vec::new();
    for group in finished.into_iter().skip(offset) {
        if limit.is_some_and(|l| out.len() >= l) {
            break;
        }
        if out.len() >= limits.max_select_rows {
            return Err(SqlError::Bounds(format!(
                "SELECT result exceeds max_select_rows ({})",
                limits.max_select_rows
            )));
        }
        out.push(
            bound
                .outputs
                .iter()
                .map(|o| output_value(o, &group))
                .collect(),
        );
    }
    Ok(SqlOutcome::Rows {
        columns: bound.names.clone(),
        rows: out,
    })
}

/// `ORDER BY` targets: an output alias, a grouped column, or an aggregate
/// spelled as in the select list by its default name.
fn order_plan(
    order_by: &[OrderItem],
    bound: &Bound,
    resolver: &Resolver<'_>,
    group_keys: &[Resolved],
) -> Result<Vec<(Output, bool, bool)>, SqlError> {
    let mut plan = Vec::new();
    for item in order_by {
        let column = &item.column;
        let target = if column.qualifier.is_none() {
            bound
                .aliases
                .iter()
                .position(|a| a.as_deref() == Some(column.column.as_str()))
                .map(|i| match bound.outputs[i] {
                    Output::Group(g) => Output::Group(g),
                    Output::Agg(a) => Output::Agg(a),
                })
        } else {
            None
        };
        let target =
            match target {
                Some(target) => target,
                None => {
                    let (r, _) = resolver.resolve(column)?;
                    let g = group_keys.iter().position(|k| *k == r).ok_or_else(|| SqlError::Syntax {
                    offset: column.offset,
                    message: format!(
                        "ORDER BY column '{}' must appear in GROUP BY or be an output alias",
                        column.column
                    ),
                })?;
                    Output::Group(g)
                }
            };
        // NULLs first for ascending and last for descending unless stated.
        let nulls_first = item.nulls_first.unwrap_or(!item.descending);
        plan.push((target, item.descending, nulls_first));
    }
    Ok(plan)
}
