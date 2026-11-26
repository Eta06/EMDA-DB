//! Statement binding, expression evaluation, and transactional execution over `DocumentDatabase`.
use super::catalog::{self, ColumnDef, TableDef};
use super::parser::{
    Assignment, CompareOp, CreateTableStmt, DeleteStmt, Expr, InsertStmt, LimitValue,
    LiteralValue, NullsOrder, SelectColumns, SelectStmt, SqlStatement, Statement, UpdateStmt,
};
use super::plan::{self, AccessPlan, UpperBound};
use super::value::{
    MAX_SQL_TEXT_PK_BYTES, SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue, coerce_for_column,
    compare_i64_f64, encode_i64_pk_bytes, encode_primary_key,
};
use crate::document::{DocumentDatabase, DocumentTransaction, DocumentTransactionMode};
use crate::{MAX_SCAN_ENTRIES, TransactionFailure};
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::io;
use std::ops::ControlFlow;
use std::rc::Rc;

const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;

pub(crate) fn execute_statement(
    db: &DocumentDatabase<'_>,
    stmt: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
    force_full_scan: bool,
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
        Statement::Select(select) => exec_select(tx, select, params, limits, force_full_scan),
        Statement::Update(update) => exec_update(tx, update, params, limits, force_full_scan),
        Statement::Delete(delete) => exec_delete(tx, delete, params, limits, force_full_scan),
    })
}

fn run_tx<T>(
    db: &DocumentDatabase<'_>,
    action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    let mut sql_error: Option<SqlError> = None;
    let outcome =
        db.transaction_detailed(
            DocumentTransactionMode::ReadSet,
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
    if is_engine_bounds_error(&err) {
        return SqlError::Bounds(err.to_string());
    }
    SqlError::Io(err)
}

fn is_engine_bounds_error(err: &io::Error) -> bool {
    const BOUNDS_PATTERNS: &[&str] = &[
        "snapshot key count limit exceeded",
        "snapshot key byte limit exceeded",
        "aggregate snapshot limit exceeded",
        "snapshot limits exceed supported maximum",
        "aggregate transaction staged byte limit exceeded",
        "transaction write count limit exceeded",
        "transaction encoded write limit exceeded",
        "transaction read-set limit exceeded",
        "transaction validation work limit exceeded",
        "transaction cumulative validation limit exceeded",
        "checkpoint transaction read-set limit exceeded",
        "checkpoint transaction range observation limit exceeded",
        "checkpoint transaction cumulative validation limit exceeded",
        "checkpoint suffix byte limit would be exceeded",
        "checkpoint suffix operation limit exceeded",
        "checkpoint suffix key limit would be exceeded",
        "checkpoint suffix key byte limit would be exceeded",
        "configured log size limit would be exceeded",
        "configured log byte limit would be exceeded",
        "configured live key limit would be exceeded",
        "configured live key byte limit would be exceeded",
        "atomic batch exceeds",
        "atomic batch must contain",
        "batch length overflow",
        "document exceeds 1 MiB",
        "document string data exceeds 1 MiB",
        "document exceeds nesting or node limits",
        "document exceeds node limit",
        "document transaction read limit exceeded",
        "document transaction write limit exceeded",
        "document snapshot read limit exceeded",
        "document scan exceeds the operation limit",
        "key exceeds 1024 bytes",
        "value exceeds 16 MiB",
        "suffix byte limit exceeded",
        "suffix operation limit exceeded",
        "suffix work limit exceeded",
        "value limit exceeded",
        "live set limit exceeded",
    ];
    let msg = err.to_string();
    BOUNDS_PATTERNS.iter().any(|pattern| msg.contains(pattern))
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
            let val = eval_expr_ref(&bound, &table, None, params)?.into_owned();
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

#[derive(Clone, Copy, Debug)]
struct BoundOrderBy {
    col_idx: usize,
    desc: bool,
    nulls_first: bool,
}

struct SortRow {
    keys: Vec<SqlValue>,
    scan_seq: usize,
    obj: Map<String, Value>,
}

struct HeapSortRow {
    row: SortRow,
    specs: Rc<[BoundOrderBy]>,
}

impl PartialEq for HeapSortRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapSortRow {}

impl PartialOrd for HeapSortRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapSortRow {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_sort_rows(&self.row, &other.row, &self.specs)
    }
}

fn compare_order_cell(a: &SqlValue, b: &SqlValue, desc: bool, nulls_first: bool) -> Ordering {
    match (a, b) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, SqlValue::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {
            let ord = compare_non_null(SqlValueRef::from_value(a), SqlValueRef::from_value(b))
                .expect("ORDER BY values have matching column types");
            if desc { ord.reverse() } else { ord }
        }
    }
}

fn compare_sort_rows(a: &SortRow, b: &SortRow, specs: &[BoundOrderBy]) -> Ordering {
    for (idx, spec) in specs.iter().enumerate() {
        let ord = compare_order_cell(&a.keys[idx], &b.keys[idx], spec.desc, spec.nulls_first);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.scan_seq.cmp(&b.scan_seq)
}

fn compare_sort_keys_to_row(
    keys: &[SqlValue],
    scan_seq: usize,
    other: &SortRow,
    specs: &[BoundOrderBy],
) -> Ordering {
    for (idx, spec) in specs.iter().enumerate() {
        let ord = compare_order_cell(&keys[idx], &other.keys[idx], spec.desc, spec.nulls_first);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    scan_seq.cmp(&other.scan_seq)
}

fn decode_order_keys(
    obj: &Map<String, Value>,
    table: &TableDef,
    specs: &[BoundOrderBy],
) -> Result<Vec<SqlValue>, SqlError> {
    let mut keys = Vec::with_capacity(specs.len());
    for spec in specs {
        keys.push(decode_col_ref(obj, &table.columns[spec.col_idx])?.into_owned());
    }
    Ok(keys)
}

fn project_row(
    mut obj: Map<String, Value>,
    table: &TableDef,
    proj_indices: &[usize],
) -> Result<Vec<SqlValue>, SqlError> {
    let mut projected = Vec::with_capacity(proj_indices.len());
    for (pos, &idx) in proj_indices.iter().enumerate() {
        let col = &table.columns[idx];
        let val = if proj_indices[pos + 1..].contains(&idx) {
            decode_col_ref(&obj, col)?.into_owned()
        } else {
            take_col_value(&mut obj, col)?
        };
        projected.push(val);
    }
    Ok(projected)
}

fn resolve_limit_value(
    val: Option<LimitValue>,
    params: &[SqlValue],
    clause: &str,
) -> Result<Option<usize>, SqlError> {
    let Some(val) = val else {
        return Ok(None);
    };
    match val {
        LimitValue::Literal(n) => Ok(Some(n)),
        LimitValue::Param(idx) => match &params[idx] {
            SqlValue::Integer(n) => {
                if *n < 0 {
                    Err(SqlError::Bounds(format!(
                        "{clause} must be non-negative, got {n}"
                    )))
                } else {
                    Ok(Some(*n as usize))
                }
            }
            other => Err(SqlError::Type(format!(
                "{clause} requires an INTEGER value, got {}",
                other.kind_name()
            ))),
        },
    }
}

fn exec_select(
    tx: &DocumentTransaction<'_, '_>,
    stmt: &SelectStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
    force_full_scan: bool,
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

    if stmt.order_by.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "ORDER BY item count exceeds max_columns ({})",
            limits.max_columns
        )));
    }
    let mut bound_order = Vec::with_capacity(stmt.order_by.len());
    for item in &stmt.order_by {
        let col_idx = table
            .column_index(&item.column)
            .ok_or_else(|| SqlError::NoSuchColumn {
                table: table.name.clone(),
                column: item.column.clone(),
            })?;
        let nulls_first = match item.nulls {
            Some(NullsOrder::First) => true,
            Some(NullsOrder::Last) => false,
            None => !item.desc,
        };
        bound_order.push(BoundOrderBy {
            col_idx,
            desc: item.desc,
            nulls_first,
        });
    }

    let limit = resolve_limit_value(stmt.limit, params, "LIMIT")?;
    let offset = resolve_limit_value(stmt.offset, params, "OFFSET")?.unwrap_or(0);

    if limit == Some(0) {
        return Ok(SqlOutcome::Rows {
            columns: col_names,
            rows: Vec::new(),
        });
    }

    let pk_first = bound_order
        .first()
        .is_some_and(|first| first.col_idx == table.primary_key_index);

    let rows = if bound_order.is_empty() || (pk_first && !bound_order[0].desc) {
        // Streaming path in ascending primary-key order: stops after OFFSET + LIMIT matches.
        let mut skipped = 0usize;
        let mut rows = Vec::new();
        let stream_cap = if bound_filter.is_none() {
            limit.map(|l| offset.saturating_add(l))
        } else {
            None
        };
        visit_matching_rows(
            tx,
            &table,
            bound_filter.as_ref(),
            params,
            limits,
            force_full_scan,
            stream_cap,
            |_doc_id, obj| {
                if skipped < offset {
                    skipped += 1;
                    return Ok(ControlFlow::Continue(()));
                }
                if rows.len() >= limits.max_select_rows {
                    return Err(SqlError::Bounds(format!(
                        "SELECT result exceeds max_select_rows ({})",
                        limits.max_select_rows
                    )));
                }
                rows.push(project_row(obj, &table, &proj_indices)?);
                if Some(rows.len()) == limit {
                    Ok(ControlFlow::Break(()))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            },
        )?;
        rows
    } else if pk_first && bound_order[0].desc {
        // Descending primary-key order over ascending scan.
        if let Some(lim) = limit {
            let keep_cap = (offset as u128).saturating_add(lim as u128);
            let mut ring: VecDeque<Map<String, Value>> = VecDeque::new();
            visit_matching_rows(
                tx,
                &table,
                bound_filter.as_ref(),
                params,
                limits,
                force_full_scan,
                None,
                |_doc_id, obj| {
                    if (ring.len() as u128) == keep_cap {
                        ring.pop_front();
                        ring.push_back(obj);
                    } else {
                        if ring.len() >= limits.max_sort_rows {
                            return Err(SqlError::Bounds(format!(
                                "sort row count exceeds max_sort_rows ({})",
                                limits.max_sort_rows
                            )));
                        }
                        if offset == 0 && ring.len() >= limits.max_select_rows {
                            return Err(SqlError::Bounds(format!(
                                "SELECT result exceeds max_select_rows ({})",
                                limits.max_select_rows
                            )));
                        }
                        ring.push_back(obj);
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
            if ring.len() <= offset {
                Vec::new()
            } else {
                let result_len = ring.len() - offset;
                if result_len > limits.max_select_rows {
                    return Err(SqlError::Bounds(format!(
                        "SELECT result exceeds max_select_rows ({})",
                        limits.max_select_rows
                    )));
                }
                for _ in 0..offset {
                    ring.pop_back();
                }
                let mut rows = Vec::with_capacity(result_len);
                while let Some(obj) = ring.pop_back() {
                    rows.push(project_row(obj, &table, &proj_indices)?);
                }
                rows
            }
        } else {
            let mut rows = Vec::new();
            visit_matching_rows(
                tx,
                &table,
                bound_filter.as_ref(),
                params,
                limits,
                force_full_scan,
                None,
                |_doc_id, obj| {
                    if rows.len() >= limits.max_select_rows {
                        return Err(SqlError::Bounds(format!(
                            "SELECT result exceeds max_select_rows ({})",
                            limits.max_select_rows
                        )));
                    }
                    if rows.len() >= limits.max_sort_rows {
                        return Err(SqlError::Bounds(format!(
                            "sort row count exceeds max_sort_rows ({})",
                            limits.max_sort_rows
                        )));
                    }
                    rows.push(project_row(obj, &table, &proj_indices)?);
                    Ok(ControlFlow::Continue(()))
                },
            )?;
            rows.reverse();
            rows
        }
    } else if let Some(lim) = limit {
        // Bounded top-K max-heap of at most OFFSET + LIMIT rows.
        let keep_cap = (offset as u128).saturating_add(lim as u128);
        let specs: Rc<[BoundOrderBy]> = Rc::from(bound_order);
        let mut heap: BinaryHeap<HeapSortRow> = BinaryHeap::new();
        let mut scan_seq = 0usize;
        visit_matching_rows(
            tx,
            &table,
            bound_filter.as_ref(),
            params,
            limits,
            force_full_scan,
            None,
            |_doc_id, obj| {
                let keys = decode_order_keys(&obj, &table, &specs)?;
                let seq = scan_seq;
                scan_seq += 1;
                if (heap.len() as u128) < keep_cap {
                    if heap.len() >= limits.max_sort_rows {
                        return Err(SqlError::Bounds(format!(
                            "sort row count exceeds max_sort_rows ({})",
                            limits.max_sort_rows
                        )));
                    }
                    if offset == 0 && heap.len() >= limits.max_select_rows {
                        return Err(SqlError::Bounds(format!(
                            "SELECT result exceeds max_select_rows ({})",
                            limits.max_select_rows
                        )));
                    }
                    heap.push(HeapSortRow {
                        row: SortRow {
                            keys,
                            scan_seq: seq,
                            obj,
                        },
                        specs: Rc::clone(&specs),
                    });
                } else if let Some(mut top) = heap.peek_mut()
                    && compare_sort_keys_to_row(&keys, seq, &top.row, &specs) == Ordering::Less
                {
                    top.row = SortRow {
                        keys,
                        scan_seq: seq,
                        obj,
                    };
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if heap.len() <= offset {
            Vec::new()
        } else {
            let result_len = heap.len() - offset;
            if result_len > limits.max_select_rows {
                return Err(SqlError::Bounds(format!(
                    "SELECT result exceeds max_select_rows ({})",
                    limits.max_select_rows
                )));
            }
            let sorted = heap.into_sorted_vec();
            let mut rows = Vec::with_capacity(result_len);
            for entry in sorted.into_iter().skip(offset) {
                rows.push(project_row(entry.row.obj, &table, &proj_indices)?);
            }
            rows
        }
    } else {
        // Full bounded sort without LIMIT.
        let mut collected = Vec::new();
        let mut scan_seq = 0usize;
        visit_matching_rows(
            tx,
            &table,
            bound_filter.as_ref(),
            params,
            limits,
            force_full_scan,
            None,
            |_doc_id, obj| {
                if collected.len() >= limits.max_select_rows {
                    return Err(SqlError::Bounds(format!(
                        "SELECT result exceeds max_select_rows ({})",
                        limits.max_select_rows
                    )));
                }
                if collected.len() >= limits.max_sort_rows {
                    return Err(SqlError::Bounds(format!(
                        "sort row count exceeds max_sort_rows ({})",
                        limits.max_sort_rows
                    )));
                }
                let keys = decode_order_keys(&obj, &table, &bound_order)?;
                let seq = scan_seq;
                scan_seq += 1;
                collected.push(SortRow {
                    keys,
                    scan_seq: seq,
                    obj,
                });
                Ok(ControlFlow::Continue(()))
            },
        )?;
        collected.sort_unstable_by(|a, b| compare_sort_rows(a, b, &bound_order));
        let mut rows = Vec::with_capacity(collected.len());
        for entry in collected {
            rows.push(project_row(entry.obj, &table, &proj_indices)?);
        }
        rows
    };

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
    force_full_scan: bool,
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
        bound_assignments.push((col, bound));
    }

    let bound_filter = bind_where(stmt.filter.as_ref(), &table, params)?;
    let mut matched = Vec::new();
    visit_matching_rows(
        tx,
        &table,
        bound_filter.as_ref(),
        params,
        limits,
        force_full_scan,
        None,
        |doc_id, mut obj| {
            if matched.len() >= limits.max_mutation_rows {
                return Err(SqlError::Bounds(format!(
                    "UPDATE row count exceeds max_mutation_rows ({})",
                    limits.max_mutation_rows
                )));
            }
            let mut updates = Vec::with_capacity(bound_assignments.len());
            for &(col, ref bound) in &bound_assignments {
                let val = eval_expr_ref(bound, &table, Some(&obj), params)?.into_owned();
                let allow_int_to_real = bound.is_int_literal_or_param();
                let coerced = coerce_for_column(
                    &val,
                    &col.name,
                    col.col_type,
                    col.not_null,
                    allow_int_to_real,
                    limits.max_text_bytes,
                )?;
                updates.push((col, coerced));
            }
            for (col, val) in updates {
                let json_val = sql_value_to_json(&val, &col.name)?;
                obj.insert(col.name.clone(), json_val);
            }
            let doc = Value::Object(obj);
            check_encoded_doc_size(&doc)?;
            matched.push((doc_id, doc));
            Ok(ControlFlow::Continue(()))
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
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    let table = catalog::load_table(tx, &stmt.table)?;
    let bound_filter = bind_where(stmt.filter.as_ref(), &table, params)?;
    let mut to_delete = Vec::new();
    visit_matching_rows(
        tx,
        &table,
        bound_filter.as_ref(),
        params,
        limits,
        force_full_scan,
        None,
        |doc_id, _obj| {
            if to_delete.len() >= limits.max_mutation_rows {
                return Err(SqlError::Bounds(format!(
                    "DELETE row count exceeds max_mutation_rows ({})",
                    limits.max_mutation_rows
                )));
            }
            to_delete.push(doc_id);
            Ok(ControlFlow::Continue(()))
        },
    )?;

    let deleted = to_delete.len();
    for doc_id in to_delete {
        tx.delete(&table.name, &doc_id).map_err(classify_io_error)?;
    }
    Ok(SqlOutcome::Deleted { rows: deleted })
}

#[allow(clippy::too_many_arguments)]
fn visit_matching_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    force_full_scan: bool,
    stream_cap: Option<usize>,
    mut on_match: impl FnMut(String, Map<String, Value>) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    let plan = if force_full_scan {
        AccessPlan::FullScan
    } else {
        plan::plan_access(filter, table, params)
    };

    match plan {
        AccessPlan::Empty => Ok(()),
        AccessPlan::Point { doc_id } => {
            let Some(doc) = tx.get(&table.name, &doc_id).map_err(classify_io_error)? else {
                return Ok(());
            };
            if 1 > limits.max_visited_rows {
                return Err(SqlError::Bounds(format!(
                    "visited row count exceeds max_visited_rows ({})",
                    limits.max_visited_rows
                )));
            }
            let obj = doc_into_object(doc)?;
            verify_doc_pk_id(table, &doc_id, &obj)?;
            if eval_where_doc(filter, table, &obj, params)? {
                let _ = on_match(doc_id, obj)?;
            }
            Ok(())
        }
        AccessPlan::Range {
            after_id,
            upper,
            max_exact_rows,
        } => scan_range_rows(
            tx,
            table,
            filter,
            params,
            limits,
            after_id,
            &upper,
            max_exact_rows,
            stream_cap,
            on_match,
        ),
        AccessPlan::FullScan => scan_range_rows(
            tx,
            table,
            filter,
            params,
            limits,
            None,
            &UpperBound::Unbounded,
            None,
            stream_cap,
            on_match,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_range_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    mut after_id: Option<String>,
    upper: &UpperBound,
    max_exact_rows: Option<usize>,
    mut stream_cap: Option<usize>,
    mut on_match: impl FnMut(String, Map<String, Value>) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    let mut visited = 0usize;

    loop {
        let remaining_visit = limits.max_visited_rows.saturating_sub(visited);
        let mut page_limit = remaining_visit.saturating_add(1).clamp(1, MAX_SCAN_ENTRIES);
        if let Some(exact) = max_exact_rows {
            page_limit = page_limit.min(exact.max(1));
        }
        if let Some(cap) = stream_cap {
            if cap == 0 {
                return Ok(());
            }
            page_limit = page_limit.min(cap);
        }
        let page = tx
            .scan_documents(&table.name, after_id.as_deref(), page_limit)
            .map_err(classify_io_error)?;
        if page.is_empty() {
            return Ok(());
        }
        after_id = page.last().map(|entry| entry.id.clone());

        for entry in page {
            if upper.is_past(&entry.id) {
                return Ok(());
            }
            visited += 1;
            if visited > limits.max_visited_rows {
                return Err(SqlError::Bounds(format!(
                    "visited row count exceeds max_visited_rows ({})",
                    limits.max_visited_rows
                )));
            }
            let at_inclusive_end = upper.is_at_inclusive(&entry.id);
            let obj = doc_into_object(entry.document)?;
            verify_doc_pk_id(table, &entry.id, &obj)?;
            if eval_where_doc(filter, table, &obj, params)? {
                if let Some(cap) = &mut stream_cap {
                    *cap = cap.saturating_sub(1);
                }
                if on_match(entry.id, obj)?.is_break() {
                    return Ok(());
                }
            }
            if at_inclusive_end {
                return Ok(());
            }
        }
    }
}

struct ByteCounter(usize);

impl io::Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn check_encoded_doc_size(doc: &Value) -> Result<(), SqlError> {
    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, doc).map_err(|err| SqlError::Io(io::Error::other(err)))?;
    if counter.0 > MAX_DOCUMENT_BYTES {
        return Err(SqlError::Bounds(format!(
            "serialized row size {} exceeds 1 MiB document limit",
            counter.0
        )));
    }
    Ok(())
}

fn sql_value_to_json(val: &SqlValue, col_name: &str) -> Result<Value, SqlError> {
    match val {
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Integer(n) => Ok(Value::Number(Number::from(*n))),
        SqlValue::Real(f) => {
            let num = Number::from_f64(*f).ok_or_else(|| {
                SqlError::Type(format!("non-finite REAL value on column '{col_name}'"))
            })?;
            Ok(Value::Number(num))
        }
        SqlValue::Text(s) => Ok(Value::String(s.clone())),
        SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
    }
}

fn encode_row_document(table: &TableDef, values: &[SqlValue]) -> Result<Value, SqlError> {
    let mut obj = Map::with_capacity(table.columns.len());
    for (col, val) in table.columns.iter().zip(values) {
        obj.insert(col.name.clone(), sql_value_to_json(val, &col.name)?);
    }
    let doc = Value::Object(obj);
    check_encoded_doc_size(&doc)?;
    Ok(doc)
}

fn doc_into_object(doc: Value) -> Result<Map<String, Value>, SqlError> {
    match doc {
        Value::Object(obj) => Ok(obj),
        _ => Err(SqlError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "stored row is not a JSON object",
        ))),
    }
}

fn verify_doc_pk_id(
    table: &TableDef,
    doc_id: &str,
    obj: &Map<String, Value>,
) -> Result<(), SqlError> {
    let pk_col = &table.columns[table.primary_key_index];
    let pk_val = decode_col_ref(obj, pk_col)?;
    let matches = match (pk_col.col_type, pk_val) {
        (SqlType::Integer, SqlValueRef::Integer(n)) => {
            let mut buf = [0u8; 18];
            encode_i64_pk_bytes(n, &mut buf);
            doc_id.as_bytes() == buf
        }
        (SqlType::Text { .. }, SqlValueRef::Text(s)) => {
            s.len() <= MAX_SQL_TEXT_PK_BYTES
                && doc_id.len() == 2 + s.len()
                && doc_id.starts_with("t:")
                && &doc_id[2..] == s
        }
        _ => false,
    };
    if !matches {
        return Err(SqlError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stored row in table '{}' has document ID '{}' that does not match primary key",
                table.name, doc_id
            ),
        )));
    }
    Ok(())
}

fn decode_col_ref<'a>(
    obj: &'a Map<String, Value>,
    col: &ColumnDef,
) -> Result<SqlValueRef<'a>, SqlError> {
    match obj.get(&col.name) {
        None | Some(Value::Null) => {
            if col.not_null {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stored NOT NULL column '{}' is null or missing", col.name),
                )));
            }
            Ok(SqlValueRef::Null)
        }
        Some(v) => match col.col_type {
            SqlType::Integer => Ok(SqlValueRef::Integer(v.as_i64().ok_or_else(|| {
                SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stored column '{}' is not an INTEGER", col.name),
                ))
            })?)),
            SqlType::Real => {
                let f = v.as_f64().filter(|f| f.is_finite()).ok_or_else(|| {
                    SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("stored column '{}' is not a finite REAL", col.name),
                    ))
                })?;
                Ok(SqlValueRef::Real(f))
            }
            SqlType::Text { .. } => Ok(SqlValueRef::Text(v.as_str().ok_or_else(|| {
                SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stored column '{}' is not TEXT", col.name),
                ))
            })?)),
            SqlType::Boolean => Ok(SqlValueRef::Boolean(v.as_bool().ok_or_else(|| {
                SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stored column '{}' is not BOOLEAN", col.name),
                ))
            })?)),
        },
    }
}

fn take_col_value(obj: &mut Map<String, Value>, col: &ColumnDef) -> Result<SqlValue, SqlError> {
    match obj.get_mut(&col.name) {
        None | Some(Value::Null) => {
            if col.not_null {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stored NOT NULL column '{}' is null or missing", col.name),
                )));
            }
            Ok(SqlValue::Null)
        }
        Some(v) => match col.col_type {
            SqlType::Integer => Ok(SqlValue::Integer(v.as_i64().ok_or_else(|| {
                SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stored column '{}' is not an INTEGER", col.name),
                ))
            })?)),
            SqlType::Real => {
                let f = v.as_f64().filter(|f| f.is_finite()).ok_or_else(|| {
                    SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("stored column '{}' is not a finite REAL", col.name),
                    ))
                })?;
                Ok(SqlValue::Real(f))
            }
            SqlType::Text { .. } => {
                let Value::String(s) = v else {
                    return Err(SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("stored column '{}' is not TEXT", col.name),
                    )));
                };
                Ok(SqlValue::Text(std::mem::take(s)))
            }
            SqlType::Boolean => Ok(SqlValue::Boolean(v.as_bool().ok_or_else(|| {
                SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stored column '{}' is not BOOLEAN", col.name),
                ))
            })?)),
        },
    }
}

#[derive(Clone, Copy)]
enum SqlValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Boolean(bool),
}

impl SqlValueRef<'_> {
    fn from_value(v: &SqlValue) -> SqlValueRef<'_> {
        match v {
            SqlValue::Null => SqlValueRef::Null,
            SqlValue::Integer(n) => SqlValueRef::Integer(*n),
            SqlValue::Real(f) => SqlValueRef::Real(*f),
            SqlValue::Text(s) => SqlValueRef::Text(s.as_str()),
            SqlValue::Boolean(b) => SqlValueRef::Boolean(*b),
        }
    }

    fn into_owned(self) -> SqlValue {
        match self {
            Self::Null => SqlValue::Null,
            Self::Integer(n) => SqlValue::Integer(n),
            Self::Real(f) => SqlValue::Real(f),
            Self::Text(s) => SqlValue::Text(s.to_owned()),
            Self::Boolean(b) => SqlValue::Boolean(b),
        }
    }

    fn kind_name(self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Integer(_) => "INTEGER",
            Self::Real(_) => "REAL",
            Self::Text(_) => "TEXT",
            Self::Boolean(_) => "BOOLEAN",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ExprType {
    Null,
    Typed(SqlType),
}

pub(crate) enum BoundExpr {
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

fn eval_where_doc(
    filter: Option<&BoundExpr>,
    table: &TableDef,
    obj: &Map<String, Value>,
    params: &[SqlValue],
) -> Result<bool, SqlError> {
    let Some(expr) = filter else {
        return Ok(true);
    };
    match eval_expr_ref(expr, table, Some(obj), params)? {
        SqlValueRef::Boolean(b) => Ok(b),
        SqlValueRef::Null => Ok(false),
        other => Err(SqlError::Type(format!(
            "WHERE clause must evaluate to BOOLEAN, got {}",
            other.kind_name()
        ))),
    }
}

fn eval_expr_ref<'a>(
    expr: &'a BoundExpr,
    table: &TableDef,
    obj: Option<&'a Map<String, Value>>,
    params: &'a [SqlValue],
) -> Result<SqlValueRef<'a>, SqlError> {
    match expr {
        BoundExpr::Literal(v) => Ok(SqlValueRef::from_value(v)),
        BoundExpr::Param(idx) => Ok(SqlValueRef::from_value(&params[*idx])),
        BoundExpr::Column(idx) => {
            let map = obj.expect("column expression requires a row object");
            decode_col_ref(map, &table.columns[*idx])
        }
        BoundExpr::IsNull { expr, negated } => {
            let val = eval_expr_ref(expr, table, obj, params)?;
            let is_null = matches!(val, SqlValueRef::Null);
            Ok(SqlValueRef::Boolean(if *negated {
                !is_null
            } else {
                is_null
            }))
        }
        BoundExpr::Not(inner) => match eval_expr_ref(inner, table, obj, params)? {
            SqlValueRef::Boolean(b) => Ok(SqlValueRef::Boolean(!b)),
            SqlValueRef::Null => Ok(SqlValueRef::Null),
            other => Err(SqlError::Type(format!(
                "NOT requires BOOLEAN operand, got {}",
                other.kind_name()
            ))),
        },
        BoundExpr::And(items) => {
            let mut has_unknown = false;
            for item in items {
                match eval_expr_ref(item, table, obj, params)? {
                    SqlValueRef::Boolean(false) => return Ok(SqlValueRef::Boolean(false)),
                    SqlValueRef::Boolean(true) => {}
                    SqlValueRef::Null => has_unknown = true,
                    other => {
                        return Err(SqlError::Type(format!(
                            "AND requires BOOLEAN operands, got {}",
                            other.kind_name()
                        )));
                    }
                }
            }
            if has_unknown {
                Ok(SqlValueRef::Null)
            } else {
                Ok(SqlValueRef::Boolean(true))
            }
        }
        BoundExpr::Or(items) => {
            let mut has_unknown = false;
            for item in items {
                match eval_expr_ref(item, table, obj, params)? {
                    SqlValueRef::Boolean(true) => return Ok(SqlValueRef::Boolean(true)),
                    SqlValueRef::Boolean(false) => {}
                    SqlValueRef::Null => has_unknown = true,
                    other => {
                        return Err(SqlError::Type(format!(
                            "OR requires BOOLEAN operands, got {}",
                            other.kind_name()
                        )));
                    }
                }
            }
            if has_unknown {
                Ok(SqlValueRef::Null)
            } else {
                Ok(SqlValueRef::Boolean(false))
            }
        }
        BoundExpr::Compare { op, left, right } => {
            let lv = eval_expr_ref(left, table, obj, params)?;
            let rv = eval_expr_ref(right, table, obj, params)?;
            if matches!(lv, SqlValueRef::Null) || matches!(rv, SqlValueRef::Null) {
                return Ok(SqlValueRef::Null);
            }
            let ord = compare_non_null(lv, rv)?;
            let matched = match op {
                CompareOp::Eq => ord == Ordering::Equal,
                CompareOp::Ne => ord != Ordering::Equal,
                CompareOp::Lt => ord == Ordering::Less,
                CompareOp::Le => ord != Ordering::Greater,
                CompareOp::Gt => ord == Ordering::Greater,
                CompareOp::Ge => ord != Ordering::Less,
            };
            Ok(SqlValueRef::Boolean(matched))
        }
    }
}

fn compare_non_null(left: SqlValueRef<'_>, right: SqlValueRef<'_>) -> Result<Ordering, SqlError> {
    match (left, right) {
        (SqlValueRef::Integer(a), SqlValueRef::Integer(b)) => Ok(a.cmp(&b)),
        (SqlValueRef::Real(a), SqlValueRef::Real(b)) => a
            .partial_cmp(&b)
            .ok_or_else(|| SqlError::Type("cannot compare non-finite REAL values".into())),
        (SqlValueRef::Integer(a), SqlValueRef::Real(b)) => Ok(compare_i64_f64(a, b)),
        (SqlValueRef::Real(a), SqlValueRef::Integer(b)) => Ok(compare_i64_f64(b, a).reverse()),
        (SqlValueRef::Text(a), SqlValueRef::Text(b)) => Ok(a.as_bytes().cmp(b.as_bytes())),
        (SqlValueRef::Boolean(a), SqlValueRef::Boolean(b)) => Ok(a.cmp(&b)),
        _ => Err(SqlError::Type(format!(
            "cannot compare {} and {}",
            left.kind_name(),
            right.kind_name()
        ))),
    }
}
