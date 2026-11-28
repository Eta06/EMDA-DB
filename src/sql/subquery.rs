//! Bounded, uncorrelated subquery binding, materialization, and EXPLAIN support.
use super::catalog::{self, CachedTable, TableDef};
use super::exec::{self, BoundExpr, SelectPlan, SqlCache, SqlValueRef, StaticExprType};
use super::join::JoinSelect;
use super::parser::{
    CompareOp, Expr, LimitValue, SelectColumns, SelectExpr, SelectItem, SelectStmt, Subquery,
    SubqueryBody,
};
use super::value::{
    MAX_SQL_SELECT_ROWS, MAX_SQL_TEXT_PK_BYTES, SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue,
};
use crate::document::DocumentTransaction;
use std::cell::Cell;
use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

/// Rewrites a correlated equality in EXISTS to a bounded membership probe.
/// The caller converts an unknown membership result to false, as required
/// when a NULL correlation key makes the inner predicate fail.
pub(crate) fn decorrelate_exists(sub: &Subquery, outer: &TableDef) -> Option<Expr> {
    let SubqueryBody::Select(select) = &sub.body else {
        return None;
    };
    let projects_literals = match &select.columns {
        SelectColumns::All => true,
        SelectColumns::Items(items) => items
            .iter()
            .all(|item| matches!(item.expr, SelectExpr::Literal { .. })),
    };
    if select.table == outer.name
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.offset.is_some()
        || !matches!(select.limit, None | Some(LimitValue::Literal(1..)))
        || !projects_literals
    {
        return None;
    }
    let mut predicates = match select.filter.as_ref()? {
        Expr::And(items) => items.clone(),
        predicate => vec![predicate.clone()],
    };
    let mut equality = None;
    for (pos, predicate) in predicates.iter().enumerate() {
        let Expr::Compare {
            op: CompareOp::Eq,
            left,
            right,
        } = predicate
        else {
            continue;
        };
        for (lhs, rhs) in [
            (left.as_ref(), right.as_ref()),
            (right.as_ref(), left.as_ref()),
        ] {
            let (
                Expr::QualifiedColumn {
                    table: lhs_table,
                    column: lhs,
                },
                Expr::QualifiedColumn {
                    table: rhs_table,
                    column: rhs,
                },
            ) = (lhs, rhs)
            else {
                continue;
            };
            if lhs_table == &outer.name && rhs_table == &select.table {
                if equality.is_some() {
                    return None;
                }
                equality = Some((pos, lhs.clone(), rhs.clone()));
                break;
            }
        }
    }
    let (pos, outer_column, inner_column) = equality?;
    predicates.remove(pos);
    let mut inner = (**select).clone();
    inner.limit = None;
    inner.columns = SelectColumns::Items(vec![SelectItem {
        expr: SelectExpr::Column {
            name: inner_column,
            offset: sub.offset,
        },
        alias: None,
    }]);
    inner.filter = (!predicates.is_empty()).then_some(Expr::And(predicates));
    Some(Expr::InSubquery {
        expr: Box::new(Expr::Column(outer_column)),
        subquery: Box::new(Subquery {
            body: SubqueryBody::Select(Box::new(inner)),
            ..sub.clone()
        }),
        negated: false,
    })
}

thread_local! {
    static ACTIVE_VISIT_COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Records `n` visited rows into the active subquery visit-count scope, if any.
pub(crate) fn record_visited_rows(n: usize) {
    if n == 0 {
        return;
    }
    ACTIVE_VISIT_COUNT.with(|cell| {
        if let Some(cur) = cell.get() {
            cell.set(Some(cur.saturating_add(n)));
        }
    });
}

pub(super) struct VisitScopeGuard {
    prev: Option<usize>,
}

impl VisitScopeGuard {
    pub(super) fn enter() -> Self {
        let prev = ACTIVE_VISIT_COUNT.with(|cell| cell.replace(Some(0)));
        Self { prev }
    }

    pub(super) fn take_and_restore(self) -> usize {
        let count = ACTIVE_VISIT_COUNT.with(|cell| cell.replace(self.prev));
        std::mem::forget(self);
        count.unwrap_or(0)
    }
}

impl Drop for VisitScopeGuard {
    fn drop(&mut self) {
        ACTIVE_VISIT_COUNT.with(|cell| cell.set(self.prev));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubqueryRole {
    Exists,
    In,
    Scalar,
}

impl SubqueryRole {
    fn label(self) -> &'static str {
        match self {
            Self::Exists => "EXISTS subquery",
            Self::In => "IN subquery",
            Self::Scalar => "scalar subquery",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SubquerySlot {
    pub(crate) role: SubqueryRole,
    pub(crate) context: &'static str,
    pub(crate) subquery: Subquery,
}

#[derive(Clone, Debug)]
pub(crate) enum SubqueryTypeCheck {
    Boolean {
        sub_id: usize,
        context: &'static str,
    },
    CompatibleWithType {
        sub_id: usize,
        expected: SqlType,
    },
    CompatibleWithParam {
        sub_id: usize,
        param_idx: usize,
    },
    CompatiblePair {
        left_sub_id: usize,
        right_sub_id: usize,
    },
    AssignToColumn {
        sub_id: usize,
        col_name: String,
        col_type: SqlType,
    },
}

/// Materialized set of single-column values produced by an `IN (SELECT ...)` subquery.
#[derive(Clone, Debug, Default)]
pub(crate) struct SubqueryInSet {
    pub(crate) has_null: bool,
    pub(crate) ints: HashSet<i64>,
    pub(crate) reals: HashSet<u64>,
    pub(crate) texts: HashSet<String>,
    pub(crate) has_false: bool,
    pub(crate) has_true: bool,
}

impl SubqueryInSet {
    pub(crate) fn is_empty(&self) -> bool {
        !self.has_null
            && self.ints.is_empty()
            && self.reals.is_empty()
            && self.texts.is_empty()
            && !self.has_false
            && !self.has_true
    }

    pub(crate) fn non_null_len(&self) -> usize {
        self.ints.len()
            + self.reals.len()
            + self.texts.len()
            + usize::from(self.has_false)
            + usize::from(self.has_true)
    }

    pub(crate) fn insert(
        &mut self,
        value: SqlValue,
        budget: &mut SubqueryBudget,
    ) -> Result<(), SqlError> {
        match value {
            SqlValue::Null => {
                if !self.has_null {
                    budget.record_materialized_value()?;
                    self.has_null = true;
                }
            }
            SqlValue::Integer(n) => {
                if !self.ints.contains(&n) {
                    budget.record_materialized_value()?;
                    self.ints.insert(n);
                }
            }
            SqlValue::Real(f) => {
                if !f.is_finite() {
                    return Err(SqlError::Type(
                        "subquery returned non-finite REAL value".into(),
                    ));
                }
                if (-TWO_POW_63..TWO_POW_63).contains(&f) && f.fract() == 0.0 {
                    let n = f as i64;
                    if !self.ints.contains(&n) {
                        budget.record_materialized_value()?;
                        self.ints.insert(n);
                    }
                } else {
                    let bits = if f == 0.0 {
                        0.0_f64.to_bits()
                    } else {
                        f.to_bits()
                    };
                    if !self.reals.contains(&bits) {
                        budget.record_materialized_value()?;
                        self.reals.insert(bits);
                    }
                }
            }
            SqlValue::Text(s) => {
                if !self.texts.contains(&s) {
                    budget.record_materialized_value()?;
                    self.texts.insert(s);
                }
            }
            SqlValue::Boolean(b) => {
                let slot = if b {
                    &mut self.has_true
                } else {
                    &mut self.has_false
                };
                if !*slot {
                    budget.record_materialized_value()?;
                    *slot = true;
                }
            }
        }
        Ok(())
    }

    fn contains_ref(&self, value: SqlValueRef<'_>) -> bool {
        match value {
            SqlValueRef::Null => false,
            SqlValueRef::Integer(n) => self.ints.contains(&n),
            SqlValueRef::Real(f) => {
                if (-TWO_POW_63..TWO_POW_63).contains(&f) && f.fract() == 0.0 {
                    self.ints.contains(&(f as i64))
                } else {
                    let bits = if f == 0.0 {
                        0.0_f64.to_bits()
                    } else {
                        f.to_bits()
                    };
                    self.reals.contains(&bits)
                }
            }
            SqlValueRef::Text(s) => self.texts.contains(s),
            SqlValueRef::Boolean(b) => {
                if b {
                    self.has_true
                } else {
                    self.has_false
                }
            }
        }
    }

    /// Evaluates `lhs [NOT] IN (self)` under SQL three-valued logic.
    pub(crate) fn evaluate_in(&self, lhs: SqlValueRef<'_>, negated: bool) -> SqlValueRef<'static> {
        if self.is_empty() {
            return SqlValueRef::Boolean(negated);
        }
        if matches!(lhs, SqlValueRef::Null) {
            return SqlValueRef::Null;
        }
        if self.contains_ref(lhs) {
            return SqlValueRef::Boolean(!negated);
        }
        if self.has_null {
            return SqlValueRef::Null;
        }
        SqlValueRef::Boolean(negated)
    }

    pub(crate) fn pk_int_candidates(&self) -> Vec<i64> {
        let mut keys: Vec<i64> = self.ints.iter().copied().collect();
        keys.sort_unstable();
        keys
    }

    pub(crate) fn pk_text_candidates(&self) -> Vec<&str> {
        let mut keys: Vec<&str> = self
            .texts
            .iter()
            .map(String::as_str)
            .filter(|s| s.len() <= MAX_SQL_TEXT_PK_BYTES)
            .collect();
        keys.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        keys
    }
}

pub(crate) struct SubqueryBudget {
    pub(crate) visited_rows: usize,
    pub(crate) max_visited_rows: usize,
    pub(crate) materialized_values: usize,
    pub(crate) max_subquery_values: usize,
}

impl SubqueryBudget {
    pub(crate) fn new(limits: &SqlLimits) -> Self {
        Self {
            visited_rows: 0,
            max_visited_rows: limits.max_visited_rows,
            materialized_values: 0,
            max_subquery_values: limits.max_subquery_values,
        }
    }

    pub(crate) fn remaining_visited(&self) -> usize {
        self.max_visited_rows.saturating_sub(self.visited_rows)
    }

    pub(crate) fn charge_visited(&mut self, rows: usize) -> Result<(), SqlError> {
        self.visited_rows = self.visited_rows.saturating_add(rows);
        if self.visited_rows > self.max_visited_rows {
            return Err(SqlError::Bounds(format!(
                "visited row count exceeds max_visited_rows ({})",
                self.max_visited_rows
            )));
        }
        Ok(())
    }

    pub(crate) fn record_materialized_value(&mut self) -> Result<(), SqlError> {
        if self.materialized_values >= self.max_subquery_values {
            return Err(SqlError::Bounds(format!(
                "subquery materialized value count exceeds max_subquery_values ({})",
                self.max_subquery_values
            )));
        }
        self.materialized_values += 1;
        Ok(())
    }

    pub(crate) fn sub_limits(&self, base: &SqlLimits) -> SqlLimits {
        SqlLimits {
            max_visited_rows: self.remaining_visited(),
            ..*base
        }
    }
}

pub(crate) enum SelectSink<'a> {
    Client {
        rows: Vec<Vec<SqlValue>>,
        max_rows: usize,
    },
    Exists {
        found: bool,
    },
    Scalar {
        value: Option<SqlValue>,
    },
    InSet {
        set: SubqueryInSet,
        budget: &'a mut SubqueryBudget,
    },
}

impl SelectSink<'_> {
    pub(crate) fn is_exists(&self) -> bool {
        matches!(self, Self::Exists { .. })
    }

    pub(crate) fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar { .. })
    }

    pub(crate) fn max_output_rows(&self) -> Option<usize> {
        match self {
            Self::Client { max_rows, .. } => Some(*max_rows),
            Self::Exists { .. } | Self::Scalar { .. } | Self::InSet { .. } => None,
        }
    }

    pub(crate) fn push_row(&mut self, mut row: Vec<SqlValue>) -> Result<ControlFlow<()>, SqlError> {
        match self {
            Self::Client { rows, max_rows } => {
                if rows.len() >= *max_rows {
                    return Err(SqlError::Bounds(format!(
                        "SELECT result exceeds max_select_rows ({max_rows})"
                    )));
                }
                rows.push(row);
                Ok(ControlFlow::Continue(()))
            }
            Self::Exists { found } => {
                *found = true;
                Ok(ControlFlow::Break(()))
            }
            Self::Scalar { value } => {
                if value.is_some() {
                    return Err(SqlError::Constraint(
                        "scalar subquery returned more than one row".into(),
                    ));
                }
                *value = Some(row.pop().unwrap_or(SqlValue::Null));
                Ok(ControlFlow::Continue(()))
            }
            Self::InSet { set, budget } => {
                let val = row.pop().unwrap_or(SqlValue::Null);
                set.insert(val, budget)?;
                Ok(ControlFlow::Continue(()))
            }
        }
    }
}

pub(crate) enum PreparedBody {
    Select {
        cached_table: Arc<CachedTable>,
        plan: Arc<SelectPlan>,
        stmt: SelectStmt,
        nested: Vec<PreparedSubquery>,
    },
    Join {
        join: JoinSelect,
        param_start: usize,
        param_count: usize,
        outer_tables: Vec<String>,
        outer_cols: Vec<String>,
    },
}

pub(crate) struct PreparedSubquery {
    pub(crate) role: SubqueryRole,
    pub(crate) context: &'static str,
    pub(crate) offset: usize,
    pub(crate) output_type: Option<StaticExprType>,
    pub(super) outer_bindings: Vec<usize>,
    pub(crate) body: PreparedBody,
}

impl PreparedSubquery {
    pub(super) fn empty_value(&self) -> SubqueryValue {
        match self.role {
            SubqueryRole::Exists => SubqueryValue::Exists(false),
            SubqueryRole::Scalar => SubqueryValue::Scalar(SqlValue::Null),
            SubqueryRole::In => SubqueryValue::InSet(Arc::new(SubqueryInSet::default())),
        }
    }
}

#[derive(Clone)]
pub(crate) enum SubqueryValue {
    Exists(bool),
    InSet(Arc<SubqueryInSet>),
    Scalar(SqlValue),
}

pub(crate) fn missing_column_error(
    table: &TableDef,
    column: &str,
    offset: usize,
    outer_scopes: &[&TableDef],
) -> SqlError {
    if outer_scopes
        .iter()
        .any(|outer| outer.column_index(column).is_some())
    {
        SqlError::Unsupported {
            feature: format!("correlated subquery reference to outer column '{column}'"),
            offset: Some(offset),
        }
    } else {
        SqlError::NoSuchColumn {
            table: table.name.clone(),
            column: column.to_owned(),
        }
    }
}

fn check_single_column_select(
    stmt: &SelectStmt,
    table: &TableDef,
    offset: usize,
) -> Result<(), SqlError> {
    let col_count = match &stmt.columns {
        SelectColumns::All => table.columns.len(),
        SelectColumns::Items(items) => items.len(),
    };
    if col_count != 1 {
        return Err(SqlError::Syntax {
            offset,
            message: format!("subquery must return 1 column, got {col_count}"),
        });
    }
    Ok(())
}

fn select_output_type(
    stmt: &SelectStmt,
    table: &TableDef,
    plan: &SelectPlan,
    nested: &[PreparedSubquery],
) -> Option<StaticExprType> {
    match &stmt.columns {
        SelectColumns::All => {
            if table.columns.len() == 1 {
                Some(StaticExprType::Typed(table.columns[0].col_type))
            } else {
                None
            }
        }
        SelectColumns::Items(items) => {
            if items.len() != 1 {
                return None;
            }
            match &items[0].expr {
                SelectExpr::Column { name, .. } => table
                    .column_index(name)
                    .map(|idx| StaticExprType::Typed(table.columns[idx].col_type)),
                SelectExpr::Aggregate(_) => plan.single_aggregate_output_type(),
                SelectExpr::Literal { value, .. } => Some(exec::literal_static_type(value)),
                SelectExpr::Param { idx } => Some(StaticExprType::Param(*idx)),
                SelectExpr::ScalarSubquery(_) => plan
                    .first_select_subquery_id()
                    .and_then(|sub_id| nested.get(sub_id).and_then(|s| s.output_type)),
                SelectExpr::Exists { .. } | SelectExpr::InSubquery { .. } => {
                    Some(StaticExprType::Typed(SqlType::Boolean))
                }
            }
        }
    }
}

pub(crate) fn bind_subqueries(
    tx: &DocumentTransaction<'_, '_>,
    slots: &[SubquerySlot],
    checks: &[SubqueryTypeCheck],
    outer_scopes: &[&TableDef],
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Vec<PreparedSubquery>, SqlError> {
    let mut prepared = Vec::with_capacity(slots.len());
    for slot in slots {
        prepared.push(bind_one_subquery(
            tx,
            slot,
            outer_scopes,
            params,
            limits,
            cache,
        )?);
    }
    check_subquery_types(checks, &prepared, params)?;
    Ok(prepared)
}

/// Lift references to the immediate outer row into private parameters.
fn lift_outer_columns(
    select: &mut SelectStmt,
    local: &TableDef,
    outer: &TableDef,
    param_base: usize,
) -> Vec<usize> {
    fn visit(
        expr: &mut Expr,
        local: &TableDef,
        outer: &TableDef,
        param_base: usize,
        bindings: &mut Vec<usize>,
    ) {
        let index = match expr {
            Expr::Column(name) if local.column_index(name).is_none() => outer.column_index(name),
            Expr::QualifiedColumn { table, column }
                if *table == outer.name && *table != local.name =>
            {
                outer.column_index(column)
            }
            Expr::Compare { left, right, .. } => {
                visit(left, local, outer, param_base, bindings);
                visit(right, local, outer, param_base, bindings);
                None
            }
            Expr::And(items) | Expr::Or(items) => {
                for item in items {
                    visit(item, local, outer, param_base, bindings);
                }
                None
            }
            Expr::Not(inner)
            | Expr::IsNull { expr: inner, .. }
            | Expr::InSubquery { expr: inner, .. } => {
                visit(inner, local, outer, param_base, bindings);
                None
            }
            _ => None,
        };
        if let Some(index) = index {
            let slot = bindings
                .iter()
                .position(|&col| col == index)
                .unwrap_or_else(|| {
                    bindings.push(index);
                    bindings.len() - 1
                });
            *expr = Expr::Param(param_base + slot);
        }
    }

    let mut bindings = Vec::new();
    for expression in [&mut select.filter, &mut select.having]
        .into_iter()
        .flatten()
    {
        visit(expression, local, outer, param_base, &mut bindings);
    }
    if let SelectColumns::Items(items) = &mut select.columns {
        for item in items {
            match &mut item.expr {
                SelectExpr::InSubquery { expr, .. } => {
                    visit(expr, local, outer, param_base, &mut bindings)
                }
                SelectExpr::Column { name, .. } if local.column_index(name).is_none() => {
                    let mut expr = Expr::Column(name.clone());
                    visit(&mut expr, local, outer, param_base, &mut bindings);
                    if let Expr::Param(idx) = expr {
                        if item.alias.is_none() {
                            item.alias = Some(name.clone());
                        }
                        item.expr = SelectExpr::Param { idx };
                    }
                }
                _ => {}
            }
        }
    }
    bindings
}

fn bind_one_subquery(
    tx: &DocumentTransaction<'_, '_>,
    slot: &SubquerySlot,
    outer_scopes: &[&TableDef],
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<PreparedSubquery, SqlError> {
    match &slot.subquery.body {
        SubqueryBody::Select(select) => {
            let cached_table = catalog::load_table_cached(tx, &select.table, cache.tables())?;
            let mut stmt = (**select).clone();
            let bindings = outer_scopes.last().map_or_else(Vec::new, |outer| {
                lift_outer_columns(&mut stmt, &cached_table.def, outer, params.len())
            });
            let param_base = params.len();
            let mut typed_params = Vec::new();
            let params = if bindings.is_empty() {
                params
            } else {
                typed_params.extend_from_slice(params);
                let outer = *outer_scopes.last().expect("outer columns were bound");
                typed_params.extend(bindings.iter().map(
                    |&col| match outer.columns[col].col_type {
                        SqlType::Integer => SqlValue::Integer(0),
                        SqlType::Real => SqlValue::Real(0.0),
                        SqlType::Text { .. } => SqlValue::Text(String::new()),
                        SqlType::Boolean => SqlValue::Boolean(false),
                    },
                ));
                typed_params.as_slice()
            };
            let select = &stmt;

            let plan = if bindings.is_empty() {
                exec::get_or_build_select_plan_with_outer(
                    slot.subquery.id,
                    select,
                    &cached_table,
                    outer_scopes,
                    limits,
                    cache,
                )?
            } else {
                // The lifted parameter positions depend on both scopes.
                exec::build_select_plan(select, &cached_table, outer_scopes, limits)?
            };
            let table = &cached_table.def;
            if matches!(slot.role, SubqueryRole::In | SubqueryRole::Scalar) {
                check_single_column_select(select, table, slot.subquery.offset)?;
            }
            let mut inner_scopes = Vec::with_capacity(outer_scopes.len() + 1);
            inner_scopes.extend_from_slice(outer_scopes);
            inner_scopes.push(table.as_ref());

            let (sub_slots, sub_checks) = plan.subquery_metadata();
            let nested = bind_subqueries(
                tx,
                sub_slots,
                sub_checks,
                &inner_scopes,
                params,
                limits,
                cache,
            )?;
            if nested.iter().any(|sub| !sub.outer_bindings.is_empty()) {
                return Err(SqlError::Unsupported {
                    feature: "nested correlated subqueries".into(),
                    offset: Some(slot.subquery.offset),
                });
            }
            plan.validate_params(params)?;
            let _ = exec::resolve_limit_value(select.limit, params, "LIMIT")?;
            let _ = exec::resolve_limit_value(select.offset, params, "OFFSET")?;

            let output_type = if matches!(slot.role, SubqueryRole::In | SubqueryRole::Scalar) {
                match select_output_type(select, table, &plan, &nested) {
                    Some(StaticExprType::Param(idx)) if idx >= param_base => {
                        let outer = outer_scopes.last().expect("outer parameter has a scope");
                        Some(StaticExprType::Typed(
                            outer.columns[bindings[idx - param_base]].col_type,
                        ))
                    }
                    output => output,
                }
            } else {
                Some(StaticExprType::Typed(SqlType::Boolean))
            };
            Ok(PreparedSubquery {
                role: slot.role,
                context: slot.context,
                offset: slot.subquery.offset,
                output_type,
                outer_bindings: bindings,
                body: PreparedBody::Select {
                    cached_table,
                    plan,
                    stmt,
                    nested,
                },
            })
        }
        SubqueryBody::Join {
            join,
            param_start,
            param_count,
        } => {
            let outer_tables: Vec<String> = outer_scopes.iter().map(|t| t.name.clone()).collect();
            let outer_cols: Vec<String> = outer_scopes
                .iter()
                .flat_map(|t| t.columns.iter().map(|c| c.name.clone()))
                .collect();
            let sub_params = &params[*param_start..*param_start + *param_count];
            super::join::validate_subquery(tx, join, sub_params, limits, cache).map_err(|err| {
                map_join_correlated_error(err, &outer_tables, &outer_cols, slot.subquery.offset)
            })?;
            Ok(PreparedSubquery {
                role: slot.role,
                context: slot.context,
                offset: slot.subquery.offset,
                output_type: None,
                outer_bindings: Vec::new(),
                body: PreparedBody::Join {
                    join: (**join).clone(),
                    param_start: *param_start,
                    param_count: *param_count,
                    outer_tables,
                    outer_cols,
                },
            })
        }
    }
}

fn map_join_correlated_error(
    err: SqlError,
    outer_tables: &[String],
    outer_cols: &[String],
    offset: usize,
) -> SqlError {
    match &err {
        SqlError::Syntax {
            message,
            offset: err_off,
        } => {
            if let Some(rest) = message.strip_prefix("unknown table or alias '") {
                if let Some(alias) = rest.split('\'').next() {
                    if outer_tables.iter().any(|t| t == alias) {
                        return SqlError::Unsupported {
                            feature: format!(
                                "correlated subquery reference to outer table '{alias}'"
                            ),
                            offset: Some(*err_off),
                        };
                    }
                }
            }
            err
        }
        SqlError::NoSuchColumn { column, .. } => {
            if outer_cols.iter().any(|c| c == column) {
                SqlError::Unsupported {
                    feature: format!("correlated subquery reference to outer column '{column}'"),
                    offset: Some(offset),
                }
            } else {
                err
            }
        }
        _ => err,
    }
}

fn resolve_runtime_static_type(ty: StaticExprType, params: &[SqlValue]) -> Option<SqlType> {
    match ty {
        StaticExprType::Null => None,
        StaticExprType::Typed(t) => Some(t),
        StaticExprType::Param(idx) => exec::param_sql_type(&params[idx]),
        StaticExprType::Subquery(_) => None,
    }
}

fn types_compatible(a: SqlType, b: SqlType) -> bool {
    a.matches_base(b) || (a.is_numeric() && b.is_numeric())
}

pub(crate) fn check_subquery_types(
    checks: &[SubqueryTypeCheck],
    prepared: &[PreparedSubquery],
    params: &[SqlValue],
) -> Result<(), SqlError> {
    for check in checks {
        match check {
            SubqueryTypeCheck::Boolean { sub_id, context } => {
                if let Some(out_ty) = prepared[*sub_id].output_type {
                    if let Some(actual) = resolve_runtime_static_type(out_ty, params) {
                        if !matches!(actual, SqlType::Boolean) {
                            return Err(SqlError::Type(format!(
                                "{context} requires BOOLEAN operand, got {}",
                                actual.display_name()
                            )));
                        }
                    }
                }
            }
            SubqueryTypeCheck::CompatibleWithType { sub_id, expected } => {
                if let Some(out_ty) = prepared[*sub_id].output_type {
                    if let Some(actual) = resolve_runtime_static_type(out_ty, params) {
                        if !types_compatible(*expected, actual) {
                            return Err(SqlError::Type(format!(
                                "cannot compare {} and {}",
                                expected.display_name(),
                                actual.display_name()
                            )));
                        }
                    }
                }
            }
            SubqueryTypeCheck::CompatibleWithParam { sub_id, param_idx } => {
                if let (Some(out_ty), Some(p_ty)) = (
                    prepared[*sub_id].output_type,
                    exec::param_sql_type(&params[*param_idx]),
                ) {
                    if let Some(sub_ty) = resolve_runtime_static_type(out_ty, params) {
                        if !types_compatible(p_ty, sub_ty) {
                            return Err(SqlError::Type(format!(
                                "cannot compare {} and {}",
                                p_ty.display_name(),
                                sub_ty.display_name()
                            )));
                        }
                    }
                }
            }
            SubqueryTypeCheck::CompatiblePair {
                left_sub_id,
                right_sub_id,
            } => {
                if let (Some(lt), Some(rt)) = (
                    prepared[*left_sub_id].output_type,
                    prepared[*right_sub_id].output_type,
                ) {
                    if let (Some(a), Some(b)) = (
                        resolve_runtime_static_type(lt, params),
                        resolve_runtime_static_type(rt, params),
                    ) {
                        if !types_compatible(a, b) {
                            return Err(SqlError::Type(format!(
                                "cannot compare {} and {}",
                                a.display_name(),
                                b.display_name()
                            )));
                        }
                    }
                }
            }
            SubqueryTypeCheck::AssignToColumn {
                sub_id,
                col_name,
                col_type,
            } => {
                if let Some(out_ty) = prepared[*sub_id].output_type {
                    if let Some(actual) = resolve_runtime_static_type(out_ty, params) {
                        if !col_type.matches_base(actual) {
                            return Err(SqlError::Type(format!(
                                "cannot assign {} to column '{col_name}' of type {}",
                                actual.display_name(),
                                col_type.display_name()
                            )));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn check_runtime_subquery_values(
    checks: &[SubqueryTypeCheck],
    values: &[SubqueryValue],
    params: &[SqlValue],
) -> Result<(), SqlError> {
    let check_val_type = |val: &SqlValue, expected: SqlType| -> Result<(), SqlError> {
        let Some(actual) = exec::param_sql_type(val) else {
            return Ok(());
        };
        if !types_compatible(expected, actual) {
            return Err(SqlError::Type(format!(
                "cannot compare {} and {}",
                expected.display_name(),
                actual.display_name()
            )));
        }
        Ok(())
    };
    let check_set_type = |set: &SubqueryInSet, expected: SqlType| -> Result<(), SqlError> {
        match expected {
            SqlType::Integer | SqlType::Real => {
                if !set.texts.is_empty() {
                    return Err(SqlError::Type(format!(
                        "cannot compare {} and TEXT",
                        expected.display_name()
                    )));
                }
                if set.has_false || set.has_true {
                    return Err(SqlError::Type(format!(
                        "cannot compare {} and BOOLEAN",
                        expected.display_name()
                    )));
                }
            }
            SqlType::Text { .. } => {
                if !set.ints.is_empty() || !set.reals.is_empty() {
                    return Err(SqlError::Type("cannot compare TEXT and numeric".into()));
                }
                if set.has_false || set.has_true {
                    return Err(SqlError::Type("cannot compare TEXT and BOOLEAN".into()));
                }
            }
            SqlType::Boolean => {
                if !set.ints.is_empty() || !set.reals.is_empty() {
                    return Err(SqlError::Type("cannot compare BOOLEAN and numeric".into()));
                }
                if !set.texts.is_empty() {
                    return Err(SqlError::Type("cannot compare BOOLEAN and TEXT".into()));
                }
            }
        }
        Ok(())
    };
    for check in checks {
        match check {
            SubqueryTypeCheck::Boolean { sub_id, context } => {
                if let SubqueryValue::Scalar(v) = &values[*sub_id] {
                    if !matches!(v, SqlValue::Null | SqlValue::Boolean(_)) {
                        return Err(SqlError::Type(format!(
                            "{context} requires BOOLEAN operand, got {}",
                            v.kind_name()
                        )));
                    }
                }
            }
            SubqueryTypeCheck::CompatibleWithType { sub_id, expected } => match &values[*sub_id] {
                SubqueryValue::Scalar(v) => check_val_type(v, *expected)?,
                SubqueryValue::InSet(set) => check_set_type(set, *expected)?,
                SubqueryValue::Exists(_) => check_val_type(&SqlValue::Boolean(true), *expected)?,
            },
            SubqueryTypeCheck::CompatibleWithParam { sub_id, param_idx } => {
                if let Some(expected) = exec::param_sql_type(&params[*param_idx]) {
                    match &values[*sub_id] {
                        SubqueryValue::Scalar(v) => check_val_type(v, expected)?,
                        SubqueryValue::InSet(set) => check_set_type(set, expected)?,
                        SubqueryValue::Exists(_) => {}
                    }
                }
            }
            SubqueryTypeCheck::CompatiblePair {
                left_sub_id,
                right_sub_id,
            } => {
                if let SubqueryValue::Scalar(lv) = &values[*left_sub_id] {
                    if let Some(expected) = exec::param_sql_type(lv) {
                        match &values[*right_sub_id] {
                            SubqueryValue::Scalar(rv) => check_val_type(rv, expected)?,
                            SubqueryValue::InSet(set) => check_set_type(set, expected)?,
                            SubqueryValue::Exists(_) => {}
                        }
                    }
                }
            }
            SubqueryTypeCheck::AssignToColumn {
                sub_id,
                col_name,
                col_type,
            } => {
                if let SubqueryValue::Scalar(v) = &values[*sub_id] {
                    if let Some(actual) = exec::param_sql_type(v) {
                        if !col_type.matches_base(actual) {
                            return Err(SqlError::Type(format!(
                                "cannot assign {} to column '{col_name}' of type {}",
                                actual.display_name(),
                                col_type.display_name()
                            )));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_subqueries(
    tx: &DocumentTransaction<'_, '_>,
    prepared: &[PreparedSubquery],
    checks: &[SubqueryTypeCheck],
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    budget: &mut SubqueryBudget,
    force_full_scan: bool,
) -> Result<Vec<SubqueryValue>, SqlError> {
    if prepared.iter().any(|sub| !sub.outer_bindings.is_empty()) {
        return Err(SqlError::Unsupported {
            feature: "correlated subquery outside a simple SELECT".into(),
            offset: None,
        });
    }
    let mut values = Vec::with_capacity(prepared.len());
    for sub in prepared {
        values.push(execute_one_subquery(
            tx,
            sub,
            params,
            limits,
            cache,
            budget,
            force_full_scan,
        )?);
    }
    check_runtime_subquery_values(checks, &values, params)?;
    Ok(values)
}

pub(super) fn execute_one_subquery(
    tx: &DocumentTransaction<'_, '_>,
    sub: &PreparedSubquery,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    budget: &mut SubqueryBudget,
    force_full_scan: bool,
) -> Result<SubqueryValue, SqlError> {
    match &sub.body {
        PreparedBody::Select {
            cached_table,
            plan,
            stmt,
            nested,
        } => {
            if !sub.outer_bindings.is_empty() {
                plan.validate_params(params)?;
            }
            let limit = exec::resolve_limit_value(stmt.limit, params, "LIMIT")?;
            if limit == Some(0) {
                return Ok(match sub.role {
                    SubqueryRole::Exists => SubqueryValue::Exists(false),
                    SubqueryRole::Scalar => SubqueryValue::Scalar(SqlValue::Null),
                    SubqueryRole::In => SubqueryValue::InSet(Arc::new(SubqueryInSet::default())),
                });
            }
            let (sub_slots, sub_checks) = plan.subquery_metadata();
            let _ = sub_slots;
            let nested_values = if limit == Some(0) || nested.is_empty() {
                Vec::new()
            } else {
                execute_subqueries(
                    tx,
                    nested,
                    sub_checks,
                    params,
                    limits,
                    cache,
                    budget,
                    force_full_scan,
                )?
            };
            let sub_limits = budget.sub_limits(limits);
            let mut sink = match sub.role {
                SubqueryRole::Exists => SelectSink::Exists { found: false },
                SubqueryRole::Scalar => SelectSink::Scalar { value: None },
                SubqueryRole::In => SelectSink::InSet {
                    set: SubqueryInSet::default(),
                    budget,
                },
            };
            let visit_guard = VisitScopeGuard::enter();
            let exec_res = exec::exec_select_plan_into(
                tx,
                &cached_table.def,
                plan,
                stmt,
                params,
                &nested_values,
                &sub_limits,
                force_full_scan,
                &mut sink,
            );
            let visited = visit_guard.take_and_restore();
            exec_res.map_err(|err| remap_visited_bounds(err, limits.max_visited_rows))?;
            let value = match sink {
                SelectSink::Exists { found } => SubqueryValue::Exists(found),
                SelectSink::Scalar { value } => {
                    SubqueryValue::Scalar(value.unwrap_or(SqlValue::Null))
                }
                SelectSink::InSet { set, .. } => SubqueryValue::InSet(Arc::new(set)),
                SelectSink::Client { .. } => unreachable!(),
            };
            budget.charge_visited(visited)?;

            Ok(value)
        }
        PreparedBody::Join {
            join,
            param_start,
            param_count,
            outer_tables,
            outer_cols,
        } => {
            let sub_params = &params[*param_start..*param_start + *param_count];
            let mut sub_limits = budget.sub_limits(limits);
            sub_limits.max_select_rows = MAX_SQL_SELECT_ROWS;
            let outcome = super::join::execute_in_tx(tx, join, sub_params, &sub_limits, cache)
                .map_err(|err| {
                    let err = map_join_correlated_error(err, outer_tables, outer_cols, sub.offset);
                    remap_visited_bounds(err, limits.max_visited_rows)
                })?;
            let SqlOutcome::Rows { columns, rows } = outcome else {
                unreachable!("join always returns Rows");
            };
            budget.charge_visited(rows.len())?;
            if matches!(sub.role, SubqueryRole::In | SubqueryRole::Scalar) && columns.len() != 1 {
                return Err(SqlError::Syntax {
                    offset: sub.offset,
                    message: format!("subquery must return 1 column, got {}", columns.len()),
                });
            }
            match sub.role {
                SubqueryRole::Exists => Ok(SubqueryValue::Exists(!rows.is_empty())),
                SubqueryRole::Scalar => {
                    if rows.len() > 1 {
                        return Err(SqlError::Constraint(
                            "scalar subquery returned more than one row".into(),
                        ));
                    }
                    Ok(SubqueryValue::Scalar(
                        rows.into_iter()
                            .next()
                            .and_then(|mut r| r.pop())
                            .unwrap_or(SqlValue::Null),
                    ))
                }
                SubqueryRole::In => {
                    let mut set = SubqueryInSet::default();
                    for mut row in rows {
                        let val = row.pop().unwrap_or(SqlValue::Null);
                        set.insert(val, budget)?;
                    }
                    Ok(SubqueryValue::InSet(Arc::new(set)))
                }
            }
        }
    }
}

pub(crate) fn remap_visited_bounds(err: SqlError, total_max: usize) -> SqlError {
    match err {
        SqlError::Bounds(msg) if msg.contains("max_visited_rows") => SqlError::Bounds(format!(
            "visited row count exceeds max_visited_rows ({total_max})"
        )),
        other => other,
    }
}

pub(crate) fn explain_subqueries(
    tx: &DocumentTransaction<'_, '_>,
    prepared: &[PreparedSubquery],
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    out: &mut Vec<Vec<SqlValue>>,
) -> Result<(), SqlError> {
    for sub in prepared {
        if !sub.outer_bindings.is_empty() {
            out.push(exec::explain_row(
                "subquery",
                "",
                sub.role.label().into(),
                format!(
                    "correlated: bind {} outer columns per row",
                    sub.outer_bindings.len()
                ),
            ));
            continue;
        }
        match &sub.body {
            PreparedBody::Select {
                cached_table,
                plan,
                stmt,
                nested,
            } => {
                explain_subqueries(tx, nested, params, limits, cache, out)?;
                let inner_rows =
                    exec::explain_select_plan(tx, &cached_table.def, plan, stmt, params, limits)?;
                let table_name = cached_table.def.name.clone();
                let access_desc = inner_rows
                    .first()
                    .and_then(|r| r.get(2))
                    .and_then(|v| match v {
                        SqlValue::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "scan".to_owned());
                let strategy = match sub.role {
                    SubqueryRole::Exists => "stops at first matching row",
                    SubqueryRole::In => "materialized into bounded set once",
                    SubqueryRole::Scalar => "expects at most 1 row",
                };
                let extra = if inner_rows.len() > 1 {
                    "; grouped/aggregated"
                } else {
                    ""
                };
                let detail = match &inner_rows[0][3] {
                    SqlValue::Text(detail) => detail,
                    _ => unreachable!("EXPLAIN scan detail is text"),
                };
                out.push(exec::explain_row(
                    "subquery",
                    &table_name,
                    access_desc,
                    format!(
                        "{} ({}); {strategy}{extra}; {detail}",
                        sub.role.label(),
                        sub.context
                    ),
                ));
            }
            PreparedBody::Join {
                join,
                param_start,
                param_count,
                ..
            } => {
                let sub_params = &params[*param_start..*param_start + *param_count];
                let join_rows = super::join::explain(tx, join, sub_params, limits, cache)?;
                let tables: Vec<String> = join_rows
                    .iter()
                    .filter(|r| matches!(r.first(), Some(SqlValue::Text(s)) if s == "scan" || s == "join"))
                    .filter_map(|r| match r.get(1) {
                        Some(SqlValue::Text(t)) => Some(t.clone()),
                        _ => None,
                    })
                    .collect();
                let strategy = match sub.role {
                    SubqueryRole::Exists => "stops when non-empty",
                    SubqueryRole::In => "materialized into bounded set once",
                    SubqueryRole::Scalar => "expects at most 1 row",
                };
                out.push(exec::explain_row(
                    "subquery",
                    &tables.join(", "),
                    "join subquery".to_owned(),
                    format!("{} ({}); {strategy}", sub.role.label(), sub.context),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn resolve_bound_expr(expr: &BoundExpr, values: &[SubqueryValue]) -> BoundExpr {
    match expr {
        BoundExpr::Literal(v) => BoundExpr::Literal(v.clone()),
        BoundExpr::ScalarValue(v) => BoundExpr::ScalarValue(v.clone()),
        BoundExpr::Param(idx) => BoundExpr::Param(*idx),
        BoundExpr::Column(idx) => BoundExpr::Column(*idx),
        BoundExpr::Aggregate(idx) => BoundExpr::Aggregate(*idx),
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(resolve_bound_expr(expr, values)),
            negated: *negated,
        },
        BoundExpr::Not(inner) => BoundExpr::Not(Box::new(resolve_bound_expr(inner, values))),
        BoundExpr::Compare { op, left, right } => BoundExpr::Compare {
            op: *op,
            left: Box::new(resolve_bound_expr(left, values)),
            right: Box::new(resolve_bound_expr(right, values)),
        },
        BoundExpr::And(items) => BoundExpr::And(
            items
                .iter()
                .map(|item| resolve_bound_expr(item, values))
                .collect(),
        ),
        BoundExpr::Or(items) => BoundExpr::Or(
            items
                .iter()
                .map(|item| resolve_bound_expr(item, values))
                .collect(),
        ),
        BoundExpr::Exists(sub_id) => match &values[*sub_id] {
            SubqueryValue::Exists(b) => BoundExpr::Literal(SqlValue::Boolean(*b)),
            _ => unreachable!("expected Exists subquery value"),
        },
        BoundExpr::ScalarSubquery(sub_id) => match &values[*sub_id] {
            SubqueryValue::Scalar(v) => BoundExpr::ScalarValue(v.clone()),
            _ => unreachable!("expected Scalar subquery value"),
        },
        BoundExpr::InSubquery {
            expr,
            sub_id,
            negated,
        } => match &values[*sub_id] {
            SubqueryValue::InSet(set) => BoundExpr::InSet {
                expr: Box::new(resolve_bound_expr(expr, values)),
                set: Arc::clone(set),
                negated: *negated,
            },
            _ => unreachable!("expected InSet subquery value"),
        },
        BoundExpr::InSet { expr, set, negated } => BoundExpr::InSet {
            expr: Box::new(resolve_bound_expr(expr, values)),
            set: Arc::clone(set),
            negated: *negated,
        },
    }
}

pub(crate) fn plan_in_pk_candidates(set: &SubqueryInSet, pk_type: SqlType) -> Option<Vec<String>> {
    match pk_type {
        SqlType::Integer => {
            let ints = set.pk_int_candidates();
            if ints.is_empty() {
                return Some(Vec::new());
            }
            if ints.len() > super::plan::SMALL_PK_IN_THRESHOLD {
                return None;
            }
            let mut out: Vec<String> = ints.into_iter().map(super::value::encode_i64_pk).collect();
            out.sort_unstable();
            out.dedup();
            Some(out)
        }
        SqlType::Text { .. } => {
            let texts = set.pk_text_candidates();
            if texts.is_empty() {
                return Some(Vec::new());
            }
            if texts.len() > super::plan::SMALL_PK_IN_THRESHOLD {
                return None;
            }
            let mut out: Vec<String> = texts.into_iter().map(|s| format!("t:{s}")).collect();
            out.sort_unstable();
            out.dedup();
            Some(out)
        }
        _ => None,
    }
}
