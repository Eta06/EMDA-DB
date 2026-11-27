//! `a FULL [OUTER] JOIN b ON c` as the only join of a statement.
//!
//! The result is `a LEFT JOIN b ON c`, then every `b` row that matched no
//! `a` row with `a`'s columns `NULL`: the mirrored `b LEFT JOIN a ON c`
//! keeping only rows where `a`'s primary key is `NULL`. `WHERE` applies to
//! both halves, as it applies after the join. Columns named in `ORDER BY`
//! are carried as hidden trailing outputs so the union can be sorted, then
//! limited, then trimmed. Each half is an ordinary bounded join; together
//! they return at most `max_select_rows` rows.
use super::{ColRef, JExpr, JoinSelect, SelectItem, limit_value, run as run_join};
use crate::document::DocumentTransaction;
use crate::sql::catalog;
use crate::sql::exec::{SqlCache, SqlValueRef, compare_order_cell_ref};
use crate::sql::value::{SqlError, SqlLimits, SqlOutcome, SqlValue};
use parking_lot::RwLock;
use std::cmp::Ordering;

fn value_ref(value: &SqlValue) -> SqlValueRef<'_> {
    match value {
        SqlValue::Null => SqlValueRef::Null,
        SqlValue::Integer(n) => SqlValueRef::Integer(*n),
        SqlValue::Real(f) => SqlValueRef::Real(*f),
        SqlValue::Text(s) => SqlValueRef::Text(s.as_str()),
        SqlValue::Boolean(b) => SqlValueRef::Boolean(*b),
    }
}

fn half(
    statement: &JoinSelect,
    items: Vec<SelectItem>,
    mirrored: bool,
    filter: Option<JExpr>,
) -> JoinSelect {
    let mut tables = statement.tables.clone();
    if mirrored {
        tables.swap(0, 1);
    }
    JoinSelect {
        items,
        tables,
        joins: statement.joins.clone(),
        filter,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        group_by: Vec::new(),
        having: None,
        full: false,
        source: None,
        cached_plan: RwLock::new(None),
    }
}

pub(super) fn run(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let left = &statement.tables[0];
    let left_def = catalog::load_table_cached(tx, &left.name, cache.tables())?;
    // `*` keeps the written table order in both halves.
    let mut items = Vec::with_capacity(statement.items.len() + statement.order_by.len());
    for item in &statement.items {
        match item {
            SelectItem::All => {
                items.push(SelectItem::TableAll(statement.tables[0].alias.clone(), 0));
                items.push(SelectItem::TableAll(statement.tables[1].alias.clone(), 0));
            }
            other => items.push(other.clone()),
        }
    }
    let hidden = statement.order_by.len();
    for order in &statement.order_by {
        items.push(SelectItem::Column(order.column.clone()));
    }
    let left_key_is_null = JExpr::IsNull(
        Box::new(JExpr::Column(ColRef {
            qualifier: Some(left.alias.clone()),
            column: left_def.def.columns[left_def.def.primary_key_index]
                .name
                .clone(),
            offset: 0,
        })),
        false,
    );
    let unmatched_filter = Some(match &statement.filter {
        Some(filter) => JExpr::And(vec![left_key_is_null, filter.clone()]),
        None => left_key_is_null,
    });
    let first = half(statement, items.clone(), false, statement.filter.clone());
    let second = half(statement, items, true, unmatched_filter);
    let (columns, mut rows) = match run_join(tx, &first, params, limits, cache)? {
        SqlOutcome::Rows { columns, rows } => (columns, rows),
        _ => unreachable!("a join returns rows"),
    };
    match run_join(tx, &second, params, limits, cache)? {
        SqlOutcome::Rows { rows: more, .. } => rows.extend(more),
        _ => unreachable!("a join returns rows"),
    }
    let width = columns.len() - hidden;
    if hidden > 0 {
        rows.sort_by(|a, b| {
            for (i, order) in statement.order_by.iter().enumerate() {
                let nulls_first = order.nulls_first.unwrap_or(!order.descending);
                let ordering = compare_order_cell_ref(
                    value_ref(&a[width + i]),
                    value_ref(&b[width + i]),
                    order.descending,
                    nulls_first,
                );
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
    for mut row in rows.into_iter().skip(offset) {
        if limit.is_some_and(|l| out.len() >= l) {
            break;
        }
        if out.len() >= limits.max_select_rows {
            return Err(SqlError::Bounds(format!(
                "SELECT result exceeds max_select_rows ({})",
                limits.max_select_rows
            )));
        }
        row.truncate(width);
        out.push(row);
    }
    let mut columns = columns;
    columns.truncate(width);
    Ok(SqlOutcome::Rows { columns, rows: out })
}
