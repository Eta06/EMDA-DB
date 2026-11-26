//! Statement binding, expression evaluation, and transactional execution over `DocumentDatabase`.
use super::catalog::{self, ColumnDef, TableDef};
use super::parser::{
    Assignment, CompareOp, CreateTableStmt, DeleteStmt, Expr, InsertStmt, LiteralValue,
    SelectColumns, SelectStmt, SqlStatement, Statement, UpdateStmt,
};
use super::value::{
    SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue, coerce_for_column, compare_i64_f64,
    encode_primary_key,
};
use crate::document::{DocumentDatabase, DocumentTransaction, DocumentTransactionMode};
use crate::{MAX_SCAN_ENTRIES, TransactionFailure};
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::io;

const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;

pub(crate) fn execute_statement(
    db: &DocumentDatabase<'_>,
    stmt: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
) -> Result<SqlOutcome, SqlError> {
    if params.len() > limits.max_params {
        return Err(SqlError::Bounds(format!(
            "parameter count {} exceeds max_params ({})",
            params.len(),
            limits.max_params
        )));
    }
    if params.len() != stmt.param_count {
        return Err(SqlError::ParameterCount {
            expected: stmt.param_count,
            actual: params.len(),
        });
    }
    for (idx, param) in params.iter().enumerate() {
        match param {
            SqlValue::Real(f) if !f.is_finite() => {
                return Err(SqlError::Type(format!(
                    "parameter {idx} has non-finite REAL value"
                )));
            }
            SqlValue::Text(s) if s.len() > limits.max_text_bytes => {
                return Err(SqlError::Bounds(format!(
                    "parameter {idx} text length {} exceeds max_text_bytes ({})",
                    s.len(),
                    limits.max_text_bytes
                )));
            }
            _ => {}
        }
    }

    run_tx(db, |tx| match &stmt.inner {
        Statement::CreateTable(create) => exec_create_table(tx, create, limits),
        Statement::Insert(insert) => exec_insert(tx, insert, params, limits),
        Statement::Select(select) => exec_select(tx, select, params, limits),
        Statement::Update(update) => exec_update(tx, update, params, limits),
        Statement::Delete(delete) => exec_delete(tx, delete, params, limits),
    })
}

fn run_tx<T>(
    db: &DocumentDatabase<'_>,
    action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    let mut sql_error: Option<SqlError> = None;
    let outcome = db.transaction_detailed(
        DocumentTransactionMode::Conservative,
        None,
        None,
        |tx| match action(tx) {
            Ok(value) => Ok(value),
            Err(err) => {
                sql_error = Some(err);
                Err(io::Error::other("sql statement aborted"))
            }
        },
    );
    match outcome {
        Ok(value) => Ok(value),
        Err(TransactionFailure::Callback(io_err)) => {
            if let Some(err) = sql_error {
                Err(err)
            } else {
                Err(classify_io_error(io_err))
            }
        }
        Err(TransactionFailure::Rejected(io_err)) => Err(classify_io_error(io_err)),
        Err(TransactionFailure::OutcomeUnknown(io_err)) => Err(SqlError::OutcomeUnknown(io_err)),
        Err(TransactionFailure::Cancelled) => Err(SqlError::Io(io::Error::new(
            io::ErrorKind::Interrupted,
            "transaction cancelled",
        ))),
        Err(TransactionFailure::DeadlineExceeded) => Err(SqlError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "transaction deadline exceeded",
        ))),
    }
}

pub(crate) fn classify_io_error(err: io::Error) -> SqlError {
    if crate::is_transaction_conflict(&err) {
        return SqlError::Conflict(err);
    }
    if err.kind() == io::ErrorKind::InvalidInput || is_engine_bounds_error(&err) {
        return SqlError::Bounds(err.to_string());
    }
    SqlError::Io(err)
}

fn is_engine_bounds_error(err: &io::Error) -> bool {
    let msg = err.to_string();
    msg.contains("snapshot key count limit exceeded")
        || msg.contains("snapshot key byte limit exceeded")
        || msg.contains("aggregate transaction staged byte limit exceeded")
        || msg.contains("transaction write count limit exceeded")
        || msg.contains("transaction encoded write limit exceeded")
        || msg.contains("atomic batch exceeds")
        || msg.contains("atomic batch must contain")
}

fn exec_create_table(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt: &CreateTableStmt,
    limits: &SqlLimits,
) -> Result<SqlOutcome, SqlError> {
    if stmt.columns.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "column count {} exceeds max_columns ({})",
            stmt.columns.len(),
            limits.max_columns
        )));
    }
    let columns = stmt
        .columns
        .iter()
        .map(|col| ColumnDef {
            name: col.name.clone(),
            col_type: col.col_type,
            not_null: col.not_null,
        })
        .collect();
    let def = TableDef::new(stmt.name.clone(), columns, stmt.primary_key_index)?;
    catalog::create_table(tx, &def, stmt.if_not_exists)?;
    Ok(SqlOutcome::TableCreated)
}

fn exec_insert(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt: &InsertStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
) -> Result<SqlOutcome, SqlError> {
    if stmt.rows.len() > limits.max_insert_rows {
        return Err(SqlError::Bounds(format!(
            "INSERT row count {} exceeds max_insert_rows ({})",
            stmt.rows.len(),
            limits.max_insert_rows
        )));
    }
    let table = catalog::load_table(tx, &stmt.table)?;

    let target_indices: Vec<usize> = match &stmt.columns {
        Some(cols) => {
            let mut indices = Vec::with_capacity(cols.len());
            for col_name in cols {
                let idx = table
                    .column_index(col_name)
                    .ok_or_else(|| SqlError::NoSuchColumn {
                        table: table.name.clone(),
                        column: col_name.clone(),
                    })?;
                indices.push(idx);
            }
            for (idx, col) in table.columns.iter().enumerate() {
                if !indices.contains(&idx) && col.not_null {
                    if idx == table.primary_key_index {
                        return Err(SqlError::Constraint(format!(
                            "PRIMARY KEY column '{}' cannot be omitted",
                            col.name
                        )));
                    }
                    return Err(SqlError::Constraint(format!(
                        "NOT NULL constraint failed on omitted column '{}.{}'",
                        table.name, col.name
                    )));
                }
            }
            indices
        }
        None => (0..table.columns.len()).collect(),
    };

    let mut prepared_rows = Vec::with_capacity(stmt.rows.len());
    let mut seen_pks = HashSet::with_capacity(stmt.rows.len());

    for tuple in &stmt.rows {
        if tuple.len() != target_indices.len() {
            return Err(SqlError::Syntax {
                offset: 0,
                message: format!(
                    "INSERT expected {} values for table '{}', got {}",
                    target_indices.len(),
                    table.name,
                    tuple.len()
                ),
            });
        }
        let mut row_values = vec![SqlValue::Null; table.columns.len()];
        for (expr, &col_idx) in tuple.iter().zip(&target_indices) {
            let bound = bind_expr(expr, None, params, &table.name)?;
            let col = &table.columns[col_idx];
            let val = eval_expr(&bound, &[], params)?;
            let allow_int_to_real = bound.is_int_literal_or_param();
            let coerced = coerce_for_column(
                &val,
                &col.name,
                col.col_type,
                col.not_null,
                allow_int_to_real,
                limits.max_text_bytes,
            )?;
            row_values[col_idx] = coerced;
        }

        let pk_col = &table.columns[table.primary_key_index];
        let pk_val = &row_values[table.primary_key_index];
        let doc_id = encode_primary_key(pk_val, pk_col.col_type)?;
        if !seen_pks.insert(doc_id.clone()) {
            return Err(SqlError::Constraint(format!(
                "duplicate primary key in INSERT on table '{}'",
                table.name
            )));
        }
        let doc = encode_row_document(&table, &row_values)?;
        prepared_rows.push((doc_id, doc));
    }

    let inserted = prepared_rows.len();
    for (doc_id, doc) in prepared_rows {
        tx.insert(&table.name, &doc_id, &doc).map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                SqlError::Constraint(format!("duplicate primary key on table '{}'", table.name))
            } else {
                classify_io_error(err)
            }
        })?;
    }
    Ok(SqlOutcome::Inserted { rows: inserted })
}

fn exec_select(
    tx: &DocumentTransaction<'_, '_>,
    stmt: &SelectStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
) -> Result<SqlOutcome, SqlError> {
    let table = catalog::load_table(tx, &stmt.table)?;
    let (proj_indices, col_names) = match &stmt.columns {
        SelectColumns::All => (
            (0..table.columns.len()).collect::<Vec<_>>(),
            table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        ),
        SelectColumns::List(cols) => {
            if cols.len() > limits.max_columns {
                return Err(SqlError::Bounds(format!(
                    "SELECT column list exceeds max_columns ({})",
                    limits.max_columns
                )));
            }
            let mut indices = Vec::with_capacity(cols.len());
            for name in cols {
                let idx = table
                    .column_index(name)
                    .ok_or_else(|| SqlError::NoSuchColumn {
                        table: table.name.clone(),
                        column: name.clone(),
                    })?;
                indices.push(idx);
            }
            (indices, cols.clone())
        }
    };

    let bound_filter = bind_where(stmt.filter.as_ref(), &table, params)?;
    let mut rows = Vec::new();
    scan_matching_rows(
        tx,
        &table,
        bound_filter.as_ref(),
        params,
        limits,
        |_doc_id, row| {
            if rows.len() >= limits.max_select_rows {
                return Err(SqlError::Bounds(format!(
                    "SELECT result exceeds max_select_rows ({})",
                    limits.max_select_rows
                )));
            }
            let projected = proj_indices.iter().map(|&idx| row[idx].clone()).collect();
            rows.push(projected);
            Ok(())
        },
    )?;

    Ok(SqlOutcome::Rows {
        columns: col_names,
        rows,
    })
}

fn exec_update(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt: &UpdateStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
) -> Result<SqlOutcome, SqlError> {
    let table = catalog::load_table(tx, &stmt.table)?;
    let mut bound_assignments = Vec::with_capacity(stmt.assignments.len());
    for Assignment {
        column,
        offset,
        value,
    } in &stmt.assignments
    {
        let idx = table
            .column_index(column)
            .ok_or_else(|| SqlError::NoSuchColumn {
                table: table.name.clone(),
                column: column.clone(),
            })?;
        if idx == table.primary_key_index {
            return Err(SqlError::Unsupported {
                feature: "updating PRIMARY KEY column".into(),
                offset: Some(*offset),
            });
        }
        let col = &table.columns[idx];
        let (bound, ty) = bind_expr_with_type(value, Some(&table), params, &table.name)?;
        check_assignment_type(&bound, ty, col)?;
        bound_assignments.push((idx, col, bound));
    }

    let bound_filter = bind_where(stmt.filter.as_ref(), &table, params)?;
    let mut matched = Vec::new();
    scan_matching_rows(
        tx,
        &table,
        bound_filter.as_ref(),
        params,
        limits,
        |doc_id, old_row| {
            if matched.len() >= limits.max_mutation_rows {
                return Err(SqlError::Bounds(format!(
                    "UPDATE row count exceeds max_mutation_rows ({})",
                    limits.max_mutation_rows
                )));
            }
            let mut new_row = old_row.clone();
            for &(col_idx, col, ref bound) in &bound_assignments {
                let val = eval_expr(bound, &old_row, params)?;
                let allow_int_to_real = bound.is_int_literal_or_param();
                let coerced = coerce_for_column(
                    &val,
                    &col.name,
                    col.col_type,
                    col.not_null,
                    allow_int_to_real,
                    limits.max_text_bytes,
                )?;
                new_row[col_idx] = coerced;
            }
            let doc = encode_row_document(&table, &new_row)?;
            matched.push((doc_id, doc));
            Ok(())
        },
    )?;

    let updated = matched.len();
    for (doc_id, doc) in matched {
        tx.replace(&table.name, &doc_id, &doc, false)
            .map_err(classify_io_error)?;
    }
    Ok(SqlOutcome::Updated { rows: updated })
}

fn exec_delete(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt: &DeleteStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
) -> Result<SqlOutcome, SqlError> {
    let table = catalog::load_table(tx, &stmt.table)?;
    let bound_filter = bind_where(stmt.filter.as_ref(), &table, params)?;
    let mut to_delete = Vec::new();
    scan_matching_rows(
        tx,
        &table,
        bound_filter.as_ref(),
        params,
        limits,
        |doc_id, _row| {
            if to_delete.len() >= limits.max_mutation_rows {
                return Err(SqlError::Bounds(format!(
                    "DELETE row count exceeds max_mutation_rows ({})",
                    limits.max_mutation_rows
                )));
            }
            to_delete.push(doc_id);
            Ok(())
        },
    )?;

    let deleted = to_delete.len();
    for doc_id in to_delete {
        tx.delete(&table.name, &doc_id).map_err(classify_io_error)?;
    }
    Ok(SqlOutcome::Deleted { rows: deleted })
}

fn scan_matching_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    mut on_match: impl FnMut(String, Vec<SqlValue>) -> Result<(), SqlError>,
) -> Result<(), SqlError> {
    let mut visited = 0usize;
    let mut after_id: Option<String> = None;

    loop {
        let remaining_visit = limits.max_visited_rows.saturating_sub(visited);
        let page_limit = remaining_visit.saturating_add(1).clamp(1, MAX_SCAN_ENTRIES);
        let page = tx
            .scan_documents(&table.name, after_id.as_deref(), page_limit)
            .map_err(classify_io_error)?;
        if page.is_empty() {
            return Ok(());
        }
        after_id = page.last().map(|entry| entry.id.clone());

        for entry in page {
            visited += 1;
            if visited > limits.max_visited_rows {
                return Err(SqlError::Bounds(format!(
                    "visited row count exceeds max_visited_rows ({})",
                    limits.max_visited_rows
                )));
            }
            let row = decode_row_document(table, &entry.document)?;
            if eval_where(filter, &row, params)? {
                on_match(entry.id, row)?;
            }
        }
    }
}

fn encode_row_document(table: &TableDef, values: &[SqlValue]) -> Result<Value, SqlError> {
    let mut obj = Map::with_capacity(table.columns.len());
    for (col, val) in table.columns.iter().zip(values) {
        let json_val = match val {
            SqlValue::Null => Value::Null,
            SqlValue::Integer(n) => Value::Number(Number::from(*n)),
            SqlValue::Real(f) => {
                let num = Number::from_f64(*f).ok_or_else(|| {
                    SqlError::Type(format!("non-finite REAL value on column '{}'", col.name))
                })?;
                Value::Number(num)
            }
            SqlValue::Text(s) => Value::String(s.clone()),
            SqlValue::Boolean(b) => Value::Bool(*b),
        };
        obj.insert(col.name.clone(), json_val);
    }
    let doc = Value::Object(obj);
    let encoded_len = serde_json::to_vec(&doc)
        .map_err(|err| SqlError::Io(io::Error::other(err)))?
        .len();
    if encoded_len > MAX_DOCUMENT_BYTES {
        return Err(SqlError::Bounds(format!(
            "serialized row size {encoded_len} exceeds 1 MiB document limit"
        )));
    }
    Ok(doc)
}

fn decode_row_document(table: &TableDef, doc: &Value) -> Result<Vec<SqlValue>, SqlError> {
    let obj = doc.as_object().ok_or_else(|| {
        SqlError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "stored row is not a JSON object",
        ))
    })?;
    let mut row = Vec::with_capacity(table.columns.len());
    for col in &table.columns {
        let val = match obj.get(&col.name) {
            None | Some(Value::Null) => {
                if col.not_null {
                    return Err(SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("stored NOT NULL column '{}' is null or missing", col.name),
                    )));
                }
                SqlValue::Null
            }
            Some(v) => match col.col_type {
                SqlType::Integer => SqlValue::Integer(v.as_i64().ok_or_else(|| {
                    SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("stored column '{}' is not an INTEGER", col.name),
                    ))
                })?),
                SqlType::Real => {
                    let f = v.as_f64().filter(|f| f.is_finite()).ok_or_else(|| {
                        SqlError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("stored column '{}' is not a finite REAL", col.name),
                        ))
                    })?;
                    SqlValue::Real(f)
                }
                SqlType::Text { .. } => SqlValue::Text(
                    v.as_str()
                        .ok_or_else(|| {
                            SqlError::Io(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("stored column '{}' is not TEXT", col.name),
                            ))
                        })?
                        .to_owned(),
                ),
                SqlType::Boolean => SqlValue::Boolean(v.as_bool().ok_or_else(|| {
                    SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("stored column '{}' is not BOOLEAN", col.name),
                    ))
                })?),
            },
        };
        row.push(val);
    }
    Ok(row)
}

#[derive(Clone, Copy, Debug)]
enum ExprType {
    Null,
    Typed(SqlType),
}

enum BoundExpr {
    Literal(SqlValue),
    Param(usize),
    Column(usize),
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
    Not(Box<BoundExpr>),
    Compare {
        op: CompareOp,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
    },
    And(Vec<BoundExpr>),
    Or(Vec<BoundExpr>),
}

impl BoundExpr {
    fn is_int_literal_or_param(&self) -> bool {
        matches!(self, Self::Literal(SqlValue::Integer(_)) | Self::Param(_))
    }
}

fn bind_where(
    filter: Option<&Expr>,
    table: &TableDef,
    params: &[SqlValue],
) -> Result<Option<BoundExpr>, SqlError> {
    let Some(expr) = filter else {
        return Ok(None);
    };
    let (bound, ty) = bind_expr_with_type(expr, Some(table), params, &table.name)?;
    match ty {
        ExprType::Null | ExprType::Typed(SqlType::Boolean) => Ok(Some(bound)),
        ExprType::Typed(other) => Err(SqlError::Type(format!(
            "WHERE clause must evaluate to BOOLEAN, got {}",
            other.display_name()
        ))),
    }
}

fn bind_expr(
    expr: &Expr,
    table: Option<&TableDef>,
    params: &[SqlValue],
    table_name: &str,
) -> Result<BoundExpr, SqlError> {
    Ok(bind_expr_with_type(expr, table, params, table_name)?.0)
}

fn check_assignment_type(bound: &BoundExpr, ty: ExprType, col: &ColumnDef) -> Result<(), SqlError> {
    match ty {
        ExprType::Null => Ok(()),
        ExprType::Typed(actual_ty) => {
            if col.col_type.matches_base(actual_ty) {
                return Ok(());
            }
            if matches!(col.col_type, SqlType::Real)
                && matches!(actual_ty, SqlType::Integer)
                && bound.is_int_literal_or_param()
            {
                if let BoundExpr::Literal(SqlValue::Integer(n)) = bound {
                    super::value::exact_i64_to_f64(*n)?;
                }
                return Ok(());
            }
            Err(SqlError::Type(format!(
                "cannot assign {} to column '{}' of type {}",
                actual_ty.display_name(),
                col.name,
                col.col_type.display_name()
            )))
        }
    }
}

fn bind_expr_with_type(
    expr: &Expr,
    table: Option<&TableDef>,
    params: &[SqlValue],
    table_name: &str,
) -> Result<(BoundExpr, ExprType), SqlError> {
    match expr {
        Expr::Literal(lit) => {
            let (val, ty) = match lit {
                LiteralValue::Null => (SqlValue::Null, ExprType::Null),
                LiteralValue::Integer(n) => {
                    (SqlValue::Integer(*n), ExprType::Typed(SqlType::Integer))
                }
                LiteralValue::Real(f) => (SqlValue::Real(*f), ExprType::Typed(SqlType::Real)),
                LiteralValue::Text(s) => (
                    SqlValue::Text(s.clone()),
                    ExprType::Typed(SqlType::Text { max_chars: None }),
                ),
                LiteralValue::Boolean(b) => {
                    (SqlValue::Boolean(*b), ExprType::Typed(SqlType::Boolean))
                }
            };
            Ok((BoundExpr::Literal(val), ty))
        }
        Expr::Param(idx) => {
            let ty = match &params[*idx] {
                SqlValue::Null => ExprType::Null,
                SqlValue::Integer(_) => ExprType::Typed(SqlType::Integer),
                SqlValue::Real(_) => ExprType::Typed(SqlType::Real),
                SqlValue::Text(_) => ExprType::Typed(SqlType::Text { max_chars: None }),
                SqlValue::Boolean(_) => ExprType::Typed(SqlType::Boolean),
            };
            Ok((BoundExpr::Param(*idx), ty))
        }
        Expr::Column(name) => {
            let Some(table) = table else {
                return Err(SqlError::NoSuchColumn {
                    table: table_name.to_owned(),
                    column: name.clone(),
                });
            };
            let idx = table
                .column_index(name)
                .ok_or_else(|| SqlError::NoSuchColumn {
                    table: table.name.clone(),
                    column: name.clone(),
                })?;
            Ok((
                BoundExpr::Column(idx),
                ExprType::Typed(table.columns[idx].col_type),
            ))
        }
        Expr::IsNull { expr, negated } => {
            let (bound, _) = bind_expr_with_type(expr, table, params, table_name)?;
            Ok((
                BoundExpr::IsNull {
                    expr: Box::new(bound),
                    negated: *negated,
                },
                ExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::Not(inner) => {
            let (bound, ty) = bind_expr_with_type(inner, table, params, table_name)?;
            match ty {
                ExprType::Null | ExprType::Typed(SqlType::Boolean) => Ok((
                    BoundExpr::Not(Box::new(bound)),
                    ExprType::Typed(SqlType::Boolean),
                )),
                ExprType::Typed(other) => Err(SqlError::Type(format!(
                    "NOT requires BOOLEAN operand, got {}",
                    other.display_name()
                ))),
            }
        }
        Expr::And(items) => {
            let mut bound_items = Vec::with_capacity(items.len());
            for item in items {
                let (bound, ty) = bind_expr_with_type(item, table, params, table_name)?;
                match ty {
                    ExprType::Null | ExprType::Typed(SqlType::Boolean) => bound_items.push(bound),
                    ExprType::Typed(other) => {
                        return Err(SqlError::Type(format!(
                            "AND requires BOOLEAN operands, got {}",
                            other.display_name()
                        )));
                    }
                }
            }
            Ok((
                BoundExpr::And(bound_items),
                ExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::Or(items) => {
            let mut bound_items = Vec::with_capacity(items.len());
            for item in items {
                let (bound, ty) = bind_expr_with_type(item, table, params, table_name)?;
                match ty {
                    ExprType::Null | ExprType::Typed(SqlType::Boolean) => bound_items.push(bound),
                    ExprType::Typed(other) => {
                        return Err(SqlError::Type(format!(
                            "OR requires BOOLEAN operands, got {}",
                            other.display_name()
                        )));
                    }
                }
            }
            Ok((
                BoundExpr::Or(bound_items),
                ExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::Compare { op, left, right } => {
            let (l_bound, l_ty) = bind_expr_with_type(left, table, params, table_name)?;
            let (r_bound, r_ty) = bind_expr_with_type(right, table, params, table_name)?;
            match (l_ty, r_ty) {
                (ExprType::Null, _) | (_, ExprType::Null) => {}
                (ExprType::Typed(lt), ExprType::Typed(rt)) => {
                    let compatible = lt.matches_base(rt) || (lt.is_numeric() && rt.is_numeric());
                    if !compatible {
                        return Err(SqlError::Type(format!(
                            "cannot compare {} and {}",
                            lt.display_name(),
                            rt.display_name()
                        )));
                    }
                }
            }
            Ok((
                BoundExpr::Compare {
                    op: *op,
                    left: Box::new(l_bound),
                    right: Box::new(r_bound),
                },
                ExprType::Typed(SqlType::Boolean),
            ))
        }
    }
}

fn eval_where(
    filter: Option<&BoundExpr>,
    row: &[SqlValue],
    params: &[SqlValue],
) -> Result<bool, SqlError> {
    let Some(expr) = filter else {
        return Ok(true);
    };
    match eval_expr(expr, row, params)? {
        SqlValue::Boolean(b) => Ok(b),
        SqlValue::Null => Ok(false),
        other => Err(SqlError::Type(format!(
            "WHERE clause must evaluate to BOOLEAN, got {}",
            other.kind_name()
        ))),
    }
}

fn eval_expr(
    expr: &BoundExpr,
    row: &[SqlValue],
    params: &[SqlValue],
) -> Result<SqlValue, SqlError> {
    match expr {
        BoundExpr::Literal(v) => Ok(v.clone()),
        BoundExpr::Param(idx) => Ok(params[*idx].clone()),
        BoundExpr::Column(idx) => Ok(row[*idx].clone()),
        BoundExpr::IsNull { expr, negated } => {
            let val = eval_expr(expr, row, params)?;
            let is_null = matches!(val, SqlValue::Null);
            Ok(SqlValue::Boolean(if *negated { !is_null } else { is_null }))
        }
        BoundExpr::Not(inner) => match eval_expr(inner, row, params)? {
            SqlValue::Boolean(b) => Ok(SqlValue::Boolean(!b)),
            SqlValue::Null => Ok(SqlValue::Null),
            other => Err(SqlError::Type(format!(
                "NOT requires BOOLEAN operand, got {}",
                other.kind_name()
            ))),
        },
        BoundExpr::And(items) => {
            let mut has_unknown = false;
            for item in items {
                match eval_expr(item, row, params)? {
                    SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                    SqlValue::Boolean(true) => {}
                    SqlValue::Null => has_unknown = true,
                    other => {
                        return Err(SqlError::Type(format!(
                            "AND requires BOOLEAN operands, got {}",
                            other.kind_name()
                        )));
                    }
                }
            }
            if has_unknown {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Boolean(true))
            }
        }
        BoundExpr::Or(items) => {
            let mut has_unknown = false;
            for item in items {
                match eval_expr(item, row, params)? {
                    SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                    SqlValue::Boolean(false) => {}
                    SqlValue::Null => has_unknown = true,
                    other => {
                        return Err(SqlError::Type(format!(
                            "OR requires BOOLEAN operands, got {}",
                            other.kind_name()
                        )));
                    }
                }
            }
            if has_unknown {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Boolean(false))
            }
        }
        BoundExpr::Compare { op, left, right } => {
            let lv = eval_expr(left, row, params)?;
            let rv = eval_expr(right, row, params)?;
            if matches!(lv, SqlValue::Null) || matches!(rv, SqlValue::Null) {
                return Ok(SqlValue::Null);
            }
            let ord = compare_non_null(&lv, &rv)?;
            let matched = match op {
                CompareOp::Eq => ord == Ordering::Equal,
                CompareOp::Ne => ord != Ordering::Equal,
                CompareOp::Lt => ord == Ordering::Less,
                CompareOp::Le => ord != Ordering::Greater,
                CompareOp::Gt => ord == Ordering::Greater,
                CompareOp::Ge => ord != Ordering::Less,
            };
            Ok(SqlValue::Boolean(matched))
        }
    }
}

fn compare_non_null(left: &SqlValue, right: &SqlValue) -> Result<Ordering, SqlError> {
    match (left, right) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => Ok(a.cmp(b)),
        (SqlValue::Real(a), SqlValue::Real(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| SqlError::Type("cannot compare non-finite REAL values".into())),
        (SqlValue::Integer(a), SqlValue::Real(b)) => Ok(compare_i64_f64(*a, *b)),
        (SqlValue::Real(a), SqlValue::Integer(b)) => Ok(compare_i64_f64(*b, *a).reverse()),
        (SqlValue::Text(a), SqlValue::Text(b)) => Ok(a.as_bytes().cmp(b.as_bytes())),
        (SqlValue::Boolean(a), SqlValue::Boolean(b)) => Ok(a.cmp(b)),
        _ => Err(SqlError::Type(format!(
            "cannot compare {} and {}",
            left.kind_name(),
            right.kind_name()
        ))),
    }
}
