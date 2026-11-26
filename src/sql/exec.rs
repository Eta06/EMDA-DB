//! Statement binding, expression evaluation, and transactional execution over `DocumentDatabase`.
use super::catalog::{self, CachedTable, ColumnDef, IndexDef, TableCache, TableDef};
use super::parser::{
    self, Assignment, CompareOp, CreateIndexStmt, CreateTableStmt, DeleteStmt, Expr, InsertStmt,
    LimitValue, LiteralValue, NullsOrder, SelectColumns, SelectStmt, SqlStatement, Statement,
    UpdateStmt,
};
use super::plan::{self, AccessPlan, UpperBound};
use super::value::{
    MAX_SQL_TEXT_PK_BYTES, SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue, coerce_for_column,
    compare_i64_f64, encode_i64_pk_bytes, exact_i64_to_f64,
};
use crate::document::{
    DocumentDatabase, DocumentTransaction, DocumentTransactionMode, IndexBound, IndexCursor,
    IndexQuery, IndexValue,
};
use crate::{MAX_SCAN_ENTRIES, TransactionFailure};
use parking_lot::RwLock;
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::io;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::Arc;

const MAX_CACHED_STATEMENTS: usize = 128;
const MAX_CACHED_PLANS: usize = 256;
const MAX_CACHED_SQL_BYTES: usize = 4096;

type DeletePlanMap = RwLock<HashMap<(u64, u64), Arc<Option<BoundFilter>>>>;

pub(crate) struct SqlCache {
    tables: TableCache,
    stmts: RwLock<HashMap<String, Arc<SqlStatement>>>,
    select_plans: RwLock<HashMap<(u64, u64), Arc<BoundSelectPlan>>>,
    insert_plans: RwLock<HashMap<(u64, u64), Arc<BoundInsertPlan>>>,
    update_plans: RwLock<HashMap<(u64, u64), Arc<BoundUpdatePlan>>>,
    delete_plans: DeletePlanMap,
}

impl SqlCache {
    pub(crate) fn new() -> Self {
        Self {
            tables: TableCache::new(),
            stmts: RwLock::new(HashMap::new()),
            select_plans: RwLock::new(HashMap::new()),
            insert_plans: RwLock::new(HashMap::new()),
            update_plans: RwLock::new(HashMap::new()),
            delete_plans: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn clear_statements_and_plans(&self) {
        self.stmts.write().clear();
        self.select_plans.write().clear();
        self.insert_plans.write().clear();
        self.update_plans.write().clear();
        self.delete_plans.write().clear();
    }

    pub(crate) fn get_cached_statement(
        &self,
        sql: &str,
        limits: &SqlLimits,
    ) -> Result<Option<Arc<SqlStatement>>, SqlError> {
        if sql.len() > MAX_CACHED_SQL_BYTES {
            return Ok(None);
        }
        if let Some(stmt) = self.stmts.read().get(sql) {
            return Ok(Some(Arc::clone(stmt)));
        }
        let parsed = Arc::new(parser::parse_statement(sql, limits)?);
        let mut guard = self.stmts.write();
        if guard.len() >= MAX_CACHED_STATEMENTS && !guard.contains_key(sql) {
            guard.clear();
        }
        guard.insert(sql.to_owned(), Arc::clone(&parsed));
        Ok(Some(parsed))
    }
}

pub(crate) fn execute_statement(
    db: &DocumentDatabase<'_>,
    stmt: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
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
        Statement::CreateIndex(create_idx) => exec_create_index(tx, create_idx, cache),
        Statement::Insert(insert) => exec_insert(tx, stmt.stmt_id, insert, params, limits, cache),
        Statement::Select(select) => exec_select(
            tx,
            stmt.stmt_id,
            select,
            params,
            limits,
            cache,
            force_full_scan,
        ),
        Statement::Update(update) => exec_update(
            tx,
            stmt.stmt_id,
            update,
            params,
            limits,
            cache,
            force_full_scan,
        ),
        Statement::Delete(delete) => exec_delete(
            tx,
            stmt.stmt_id,
            delete,
            params,
            limits,
            cache,
            force_full_scan,
        ),
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
        "indexed string exceeds 256 bytes",
        "collection exceeds 16 indexes",
        "an index must have between 1 and 4 fields",
        "lookup response exceeds its byte limit",
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
    let indexes = stmt
        .unique_constraints
        .iter()
        .enumerate()
        .map(|(i, cols)| IndexDef {
            name: format!("__sql_u_{i}"),
            columns: cols.clone(),
            unique: true,
        })
        .collect();
    let def = TableDef::new(stmt.name.clone(), columns, stmt.primary_key_index, indexes)?;
    catalog::create_table(tx, &def, stmt.if_not_exists)?;
    Ok(SqlOutcome::TableCreated)
}

fn exec_create_index(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt: &CreateIndexStmt,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
    let table = &cached_table.def;
    let mut col_indices = Vec::with_capacity(stmt.columns.len());
    for col_name in &stmt.columns {
        let idx = table
            .column_index(col_name)
            .ok_or_else(|| SqlError::NoSuchColumn {
                table: table.name.clone(),
                column: col_name.clone(),
            })?;
        let col = &table.columns[idx];
        if !matches!(
            col.col_type,
            SqlType::Integer | SqlType::Text { .. } | SqlType::Boolean
        ) {
            return Err(SqlError::Unsupported {
                feature: format!(
                    "secondary index on {} column '{}'",
                    col.col_type.display_name(),
                    col.name
                ),
                offset: None,
            });
        }
        col_indices.push(idx);
    }
    let index_def = IndexDef {
        name: stmt.name.clone(),
        columns: col_indices,
        unique: stmt.unique,
    };
    catalog::create_index(tx, &cached_table, index_def, stmt.if_not_exists)?;
    Ok(SqlOutcome::IndexCreated)
}

struct BoundInsertPlan {
    target_indices: Vec<usize>,
}

fn get_or_build_insert_plan(
    stmt_id: u64,
    stmt: &InsertStmt,
    cached_table: &CachedTable,
    cache: &SqlCache,
) -> Result<Arc<BoundInsertPlan>, SqlError> {
    let key = (stmt_id, cached_table.generation);
    if let Some(plan) = cache.insert_plans.read().get(&key) {
        return Ok(Arc::clone(plan));
    }
    let table = &cached_table.def;
    let target_indices = match &stmt.columns {
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
    let plan = Arc::new(BoundInsertPlan { target_indices });
    let mut guard = cache.insert_plans.write();
    if guard.len() >= MAX_CACHED_PLANS {
        guard.clear();
    }
    guard.insert(key, Arc::clone(&plan));
    Ok(plan)
}

enum InsertPk {
    Int(i64),
    Text(String),
}

fn eval_insert_expr_to_json(
    expr: &Expr,
    col: &ColumnDef,
    table: &TableDef,
    params: &[SqlValue],
    max_text_bytes: usize,
) -> Result<(Value, Option<InsertPk>), SqlError> {
    enum RawVal<'a> {
        Ref(SqlValueRef<'a>, bool),
        Owned(SqlValue, bool),
    }
    let raw = match expr {
        Expr::Literal(lit) => match lit {
            LiteralValue::Null => RawVal::Ref(SqlValueRef::Null, true),
            LiteralValue::Integer(n) => RawVal::Ref(SqlValueRef::Integer(*n), true),
            LiteralValue::Real(f) => RawVal::Ref(SqlValueRef::Real(*f), true),
            LiteralValue::Text(s) => RawVal::Ref(SqlValueRef::Text(s.as_str()), true),
            LiteralValue::Boolean(b) => RawVal::Ref(SqlValueRef::Boolean(*b), true),
        },
        Expr::Param(idx) => RawVal::Ref(SqlValueRef::from_value(&params[*idx]), true),
        other => {
            let bound = bind_expr(other, None, params, &table.name)?;
            let allow_int_to_real = bound.is_int_literal_or_param();
            let val = eval_expr_ref(&bound, table, None, params)?.into_owned();
            RawVal::Owned(val, allow_int_to_real)
        }
    };
    let (val_ref, allow_int_to_real) = match &raw {
        RawVal::Ref(r, allow) => (*r, *allow),
        RawVal::Owned(v, allow) => (SqlValueRef::from_value(v), *allow),
    };
    match val_ref {
        SqlValueRef::Null => {
            if col.not_null {
                Err(SqlError::Constraint(format!(
                    "NOT NULL constraint failed on column '{}'",
                    col.name
                )))
            } else {
                Ok((Value::Null, None))
            }
        }
        SqlValueRef::Integer(n) => match col.col_type {
            SqlType::Integer => Ok((Value::Number(Number::from(n)), Some(InsertPk::Int(n)))),
            SqlType::Real if allow_int_to_real => {
                let f = exact_i64_to_f64(n)?;
                let num = Number::from_f64(f).ok_or_else(|| {
                    SqlError::Type(format!("non-finite REAL value on column '{}'", col.name))
                })?;
                Ok((Value::Number(num), None))
            }
            _ => Err(SqlError::Type(format!(
                "column '{}' expects {}, got INTEGER",
                col.name,
                col.col_type.display_name()
            ))),
        },
        SqlValueRef::Real(f) => match col.col_type {
            SqlType::Real => {
                if !f.is_finite() {
                    return Err(SqlError::Type(format!(
                        "column '{}' rejects non-finite REAL value",
                        col.name
                    )));
                }
                let num = Number::from_f64(f).ok_or_else(|| {
                    SqlError::Type(format!("non-finite REAL value on column '{}'", col.name))
                })?;
                Ok((Value::Number(num), None))
            }
            _ => Err(SqlError::Type(format!(
                "column '{}' expects {}, got REAL",
                col.name,
                col.col_type.display_name()
            ))),
        },
        SqlValueRef::Text(s) => match col.col_type {
            SqlType::Text { max_chars } => {
                if s.len() > max_text_bytes {
                    return Err(SqlError::Bounds(format!(
                        "TEXT value of {} bytes on column '{}' exceeds max_text_bytes ({max_text_bytes})",
                        s.len(),
                        col.name
                    )));
                }
                if let Some(limit) = max_chars {
                    let chars = s.chars().count();
                    if chars > limit {
                        return Err(SqlError::Constraint(format!(
                            "VARCHAR({limit}) constraint failed on column '{}': got {chars} characters",
                            col.name
                        )));
                    }
                }
                Ok((
                    Value::String(s.to_owned()),
                    Some(InsertPk::Text(s.to_owned())),
                ))
            }
            _ => Err(SqlError::Type(format!(
                "column '{}' expects {}, got TEXT",
                col.name,
                col.col_type.display_name()
            ))),
        },
        SqlValueRef::Boolean(b) => match col.col_type {
            SqlType::Boolean => Ok((Value::Bool(b), None)),
            _ => Err(SqlError::Type(format!(
                "column '{}' expects {}, got BOOLEAN",
                col.name,
                col.col_type.display_name()
            ))),
        },
    }
}

fn encode_insert_pk_stack<'a>(
    pk: &InsertPk,
    pk_type: SqlType,
    i64_buf: &'a mut [u8; 18],
    txt_buf: &'a mut [u8; 256],
) -> Result<&'a str, SqlError> {
    match (pk_type, pk) {
        (SqlType::Integer, InsertPk::Int(n)) => {
            encode_i64_pk_bytes(*n, i64_buf);
            Ok(std::str::from_utf8(i64_buf).expect("ASCII hex PK"))
        }
        (SqlType::Text { .. }, InsertPk::Text(s)) => {
            if s.len() > MAX_SQL_TEXT_PK_BYTES {
                return Err(SqlError::Bounds(format!(
                    "TEXT primary key of {} bytes exceeds the {MAX_SQL_TEXT_PK_BYTES}-byte limit (256-byte document ID cap)",
                    s.len()
                )));
            }
            txt_buf[0] = b't';
            txt_buf[1] = b':';
            txt_buf[2..2 + s.len()].copy_from_slice(s.as_bytes());
            Ok(std::str::from_utf8(&txt_buf[..2 + s.len()]).expect("valid UTF-8 PK"))
        }
        _ => Err(SqlError::Constraint(
            "PRIMARY KEY value cannot be NULL".into(),
        )),
    }
}

fn exec_insert(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt_id: u64,
    stmt: &InsertStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    if stmt.rows.len() > limits.max_insert_rows {
        return Err(SqlError::Bounds(format!(
            "INSERT row count {} exceeds max_insert_rows ({})",
            stmt.rows.len(),
            limits.max_insert_rows
        )));
    }
    let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
    let plan = get_or_build_insert_plan(stmt_id, stmt, &cached_table, cache)?;
    let table = &cached_table.def;
    let pk_idx = table.primary_key_index;
    let pk_type = table.columns[pk_idx].col_type;

    if stmt.rows.len() == 1 {
        let tuple = &stmt.rows[0];
        if tuple.len() != plan.target_indices.len() {
            return Err(SqlError::Syntax {
                offset: 0,
                message: format!(
                    "INSERT expected {} values for table '{}', got {}",
                    plan.target_indices.len(),
                    table.name,
                    tuple.len()
                ),
            });
        }
        let mut obj = Map::with_capacity(table.columns.len());
        let mut pk_val: Option<InsertPk> = None;
        for (expr, &col_idx) in tuple.iter().zip(&plan.target_indices) {
            let col = &table.columns[col_idx];
            let (json_val, maybe_pk) =
                eval_insert_expr_to_json(expr, col, table, params, limits.max_text_bytes)?;
            if col_idx == pk_idx {
                pk_val = maybe_pk;
            }
            if !json_val.is_null() {
                obj.insert(col.name.clone(), json_val);
            }
        }
        let pk = pk_val
            .ok_or_else(|| SqlError::Constraint("PRIMARY KEY value cannot be NULL".into()))?;
        let mut i64_buf = [0u8; 18];
        let mut txt_buf = [0u8; 256];
        let doc_id = encode_insert_pk_stack(&pk, pk_type, &mut i64_buf, &mut txt_buf)?;
        let doc = Value::Object(obj);
        tx.insert(&table.name, doc_id, &doc)
            .map_err(|err| map_write_constraint_error(err, &table.name))?;
        return Ok(SqlOutcome::Inserted { rows: 1 });
    }

    let mut prepared_rows = Vec::with_capacity(stmt.rows.len());
    let mut seen_pks = HashSet::with_capacity(stmt.rows.len());

    for tuple in &stmt.rows {
        if tuple.len() != plan.target_indices.len() {
            return Err(SqlError::Syntax {
                offset: 0,
                message: format!(
                    "INSERT expected {} values for table '{}', got {}",
                    plan.target_indices.len(),
                    table.name,
                    tuple.len()
                ),
            });
        }
        let mut obj = Map::with_capacity(table.columns.len());
        let mut pk_val: Option<InsertPk> = None;
        for (expr, &col_idx) in tuple.iter().zip(&plan.target_indices) {
            let col = &table.columns[col_idx];
            let (json_val, maybe_pk) =
                eval_insert_expr_to_json(expr, col, table, params, limits.max_text_bytes)?;
            if col_idx == pk_idx {
                pk_val = maybe_pk;
            }
            if !json_val.is_null() {
                obj.insert(col.name.clone(), json_val);
            }
        }
        let pk = pk_val
            .ok_or_else(|| SqlError::Constraint("PRIMARY KEY value cannot be NULL".into()))?;
        let doc_id = match (pk_type, &pk) {
            (SqlType::Integer, InsertPk::Int(n)) => {
                let mut buf = [0u8; 18];
                encode_i64_pk_bytes(*n, &mut buf);
                String::from_utf8(buf.to_vec()).expect("ASCII hex PK")
            }
            (SqlType::Text { .. }, InsertPk::Text(s)) => {
                if s.len() > MAX_SQL_TEXT_PK_BYTES {
                    return Err(SqlError::Bounds(format!(
                        "TEXT primary key of {} bytes exceeds the {MAX_SQL_TEXT_PK_BYTES}-byte limit (256-byte document ID cap)",
                        s.len()
                    )));
                }
                format!("t:{s}")
            }
            _ => {
                return Err(SqlError::Constraint(
                    "PRIMARY KEY value cannot be NULL".into(),
                ));
            }
        };
        if !seen_pks.insert(doc_id.clone()) {
            return Err(SqlError::Constraint(format!(
                "duplicate primary key in INSERT on table '{}'",
                table.name
            )));
        }
        prepared_rows.push((doc_id, Value::Object(obj)));
    }

    let inserted = prepared_rows.len();
    for (doc_id, doc) in prepared_rows {
        tx.insert(&table.name, &doc_id, &doc)
            .map_err(|err| map_write_constraint_error(err, &table.name))?;
    }
    Ok(SqlOutcome::Inserted { rows: inserted })
}

fn map_write_constraint_error(err: io::Error, table_name: &str) -> SqlError {
    if err.kind() == io::ErrorKind::AlreadyExists {
        if err.to_string().contains("unique") {
            return SqlError::Constraint(format!(
                "UNIQUE constraint failed on table '{table_name}'"
            ));
        }
        return SqlError::Constraint(format!("duplicate primary key on table '{table_name}'"));
    }
    classify_io_error(err)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundOrderBy {
    pub(crate) col_idx: usize,
    pub(crate) desc: bool,
    pub(crate) nulls_first: bool,
}

struct SortRow {
    keys: Vec<SqlValue>,
    doc_id: String,
    verified_pk: Option<i64>,
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

struct PkSortRow {
    doc_id: String,
    verified_pk: Option<i64>,
    obj: Map<String, Value>,
    desc: bool,
}

impl PartialEq for PkSortRow {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id
    }
}

impl Eq for PkSortRow {}

impl PartialOrd for PkSortRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PkSortRow {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.doc_id.as_bytes().cmp(other.doc_id.as_bytes());
        if self.desc { ord.reverse() } else { ord }
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
    a.doc_id.as_bytes().cmp(b.doc_id.as_bytes())
}

fn compare_sort_keys_to_row(
    keys: &[SqlValue],
    doc_id: &str,
    other: &SortRow,
    specs: &[BoundOrderBy],
) -> Ordering {
    for (idx, spec) in specs.iter().enumerate() {
        let ord = compare_order_cell(&keys[idx], &other.keys[idx], spec.desc, spec.nulls_first);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    doc_id.as_bytes().cmp(other.doc_id.as_bytes())
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

enum OrderStrategy {
    PkAsc,
    PkDesc,
    General,
}

enum PkEqSource {
    Param(usize),
    IntLit(i64),
    TextLit(String),
}

struct BoundSelectPlan {
    proj_indices: Vec<usize>,
    proj_is_last: Vec<bool>,
    col_names: Vec<String>,
    filter: Option<BoundFilter>,
    order_by: Vec<BoundOrderBy>,
    order_strategy: OrderStrategy,
    simple_pk_eq: Option<PkEqSource>,
}

struct BoundUpdatePlan {
    assignments: Vec<(usize, BoundExpr)>,
    assign_param_checks: Vec<(usize, SqlType)>,
    filter: Option<BoundFilter>,
    extra_param_constraints: Vec<ParamConstraint>,
}

enum ParamConstraint {
    Boolean(usize),
    Compatible { idx: usize, expected: SqlType },
    Pair { left: usize, right: usize },
}

struct BoundFilter {
    expr: BoundExpr,
    param_constraints: Vec<ParamConstraint>,
}

fn param_sql_type(v: &SqlValue) -> Option<SqlType> {
    match v {
        SqlValue::Null => None,
        SqlValue::Integer(_) => Some(SqlType::Integer),
        SqlValue::Real(_) => Some(SqlType::Real),
        SqlValue::Text(_) => Some(SqlType::Text { max_chars: None }),
        SqlValue::Boolean(_) => Some(SqlType::Boolean),
    }
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

#[derive(Clone, Copy)]
enum StaticExprType {
    Null,
    Typed(SqlType),
    Param(usize),
}

fn validate_constraints(
    constraints: &[ParamConstraint],
    params: &[SqlValue],
) -> Result<(), SqlError> {
    for c in constraints {
        match *c {
            ParamConstraint::Boolean(idx) => match &params[idx] {
                SqlValue::Null | SqlValue::Boolean(_) => {}
                other => {
                    return Err(SqlError::Type(format!(
                        "boolean expression requires BOOLEAN operand, got {}",
                        other.kind_name()
                    )));
                }
            },
            ParamConstraint::Compatible { idx, expected } => match &params[idx] {
                SqlValue::Null => {}
                other => {
                    let actual = param_sql_type(other).expect("non-null");
                    let ok = expected.matches_base(actual)
                        || (expected.is_numeric() && actual.is_numeric());
                    if !ok {
                        return Err(SqlError::Type(format!(
                            "cannot compare {} and {}",
                            expected.display_name(),
                            actual.display_name()
                        )));
                    }
                }
            },
            ParamConstraint::Pair { left, right } => {
                if let (Some(lt), Some(rt)) = (
                    param_sql_type(&params[left]),
                    param_sql_type(&params[right]),
                ) {
                    let ok = lt.matches_base(rt) || (lt.is_numeric() && rt.is_numeric());
                    if !ok {
                        return Err(SqlError::Type(format!(
                            "cannot compare {} and {}",
                            lt.display_name(),
                            rt.display_name()
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_filter_params(
    filter: Option<&BoundFilter>,
    params: &[SqlValue],
) -> Result<(), SqlError> {
    let Some(filter) = filter else {
        return Ok(());
    };
    validate_constraints(&filter.param_constraints, params)
}

fn bind_where_static(
    filter: Option<&Expr>,
    table: &TableDef,
) -> Result<Option<BoundFilter>, SqlError> {
    let Some(expr) = filter else {
        return Ok(None);
    };
    let mut param_constraints = Vec::new();
    let (bound, ty) = bind_expr_static(expr, Some(table), &table.name, &mut param_constraints)?;
    match ty {
        StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
        StaticExprType::Param(p) => param_constraints.push(ParamConstraint::Boolean(p)),
        StaticExprType::Typed(other) => {
            return Err(SqlError::Type(format!(
                "WHERE clause must evaluate to BOOLEAN, got {}",
                other.display_name()
            )));
        }
    }
    Ok(Some(BoundFilter {
        expr: bound,
        param_constraints,
    }))
}

fn bind_expr_static(
    expr: &Expr,
    table: Option<&TableDef>,
    table_name: &str,
    constraints: &mut Vec<ParamConstraint>,
) -> Result<(BoundExpr, StaticExprType), SqlError> {
    match expr {
        Expr::Literal(lit) => {
            let (val, ty) = match lit {
                LiteralValue::Null => (SqlValue::Null, StaticExprType::Null),
                LiteralValue::Integer(n) => (
                    SqlValue::Integer(*n),
                    StaticExprType::Typed(SqlType::Integer),
                ),
                LiteralValue::Real(f) => (SqlValue::Real(*f), StaticExprType::Typed(SqlType::Real)),
                LiteralValue::Text(s) => (
                    SqlValue::Text(s.clone()),
                    StaticExprType::Typed(SqlType::Text { max_chars: None }),
                ),
                LiteralValue::Boolean(b) => (
                    SqlValue::Boolean(*b),
                    StaticExprType::Typed(SqlType::Boolean),
                ),
            };
            Ok((BoundExpr::Literal(val), ty))
        }
        Expr::Param(idx) => Ok((BoundExpr::Param(*idx), StaticExprType::Param(*idx))),
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
                StaticExprType::Typed(table.columns[idx].col_type),
            ))
        }
        Expr::IsNull { expr, negated } => {
            let (bound, _) = bind_expr_static(expr, table, table_name, constraints)?;
            Ok((
                BoundExpr::IsNull {
                    expr: Box::new(bound),
                    negated: *negated,
                },
                StaticExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::Not(inner) => {
            let (bound, ty) = bind_expr_static(inner, table, table_name, constraints)?;
            match ty {
                StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
                StaticExprType::Param(p) => constraints.push(ParamConstraint::Boolean(p)),
                StaticExprType::Typed(other) => {
                    return Err(SqlError::Type(format!(
                        "NOT requires BOOLEAN operand, got {}",
                        other.display_name()
                    )));
                }
            }
            Ok((
                BoundExpr::Not(Box::new(bound)),
                StaticExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::And(items) => {
            let mut bound_items = Vec::with_capacity(items.len());
            for item in items {
                let (bound, ty) = bind_expr_static(item, table, table_name, constraints)?;
                match ty {
                    StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
                    StaticExprType::Param(p) => constraints.push(ParamConstraint::Boolean(p)),
                    StaticExprType::Typed(other) => {
                        return Err(SqlError::Type(format!(
                            "AND requires BOOLEAN operands, got {}",
                            other.display_name()
                        )));
                    }
                }
                bound_items.push(bound);
            }
            Ok((
                BoundExpr::And(bound_items),
                StaticExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::Or(items) => {
            let mut bound_items = Vec::with_capacity(items.len());
            for item in items {
                let (bound, ty) = bind_expr_static(item, table, table_name, constraints)?;
                match ty {
                    StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
                    StaticExprType::Param(p) => constraints.push(ParamConstraint::Boolean(p)),
                    StaticExprType::Typed(other) => {
                        return Err(SqlError::Type(format!(
                            "OR requires BOOLEAN operands, got {}",
                            other.display_name()
                        )));
                    }
                }
                bound_items.push(bound);
            }
            Ok((
                BoundExpr::Or(bound_items),
                StaticExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::Compare { op, left, right } => {
            let (l_bound, l_ty) = bind_expr_static(left, table, table_name, constraints)?;
            let (r_bound, r_ty) = bind_expr_static(right, table, table_name, constraints)?;
            match (l_ty, r_ty) {
                (StaticExprType::Null, _) | (_, StaticExprType::Null) => {}
                (StaticExprType::Typed(lt), StaticExprType::Typed(rt)) => {
                    let compatible = lt.matches_base(rt) || (lt.is_numeric() && rt.is_numeric());
                    if !compatible {
                        return Err(SqlError::Type(format!(
                            "cannot compare {} and {}",
                            lt.display_name(),
                            rt.display_name()
                        )));
                    }
                }
                (StaticExprType::Typed(t), StaticExprType::Param(p))
                | (StaticExprType::Param(p), StaticExprType::Typed(t)) => {
                    constraints.push(ParamConstraint::Compatible {
                        idx: p,
                        expected: t,
                    });
                }
                (StaticExprType::Param(lp), StaticExprType::Param(rp)) => {
                    constraints.push(ParamConstraint::Pair {
                        left: lp,
                        right: rp,
                    });
                }
            }
            Ok((
                BoundExpr::Compare {
                    op: *op,
                    left: Box::new(l_bound),
                    right: Box::new(r_bound),
                },
                StaticExprType::Typed(SqlType::Boolean),
            ))
        }
    }
}

fn detect_simple_pk_eq(expr: &BoundExpr, pk_idx: usize) -> Option<PkEqSource> {
    let BoundExpr::Compare {
        op: CompareOp::Eq,
        left,
        right,
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (BoundExpr::Column(c), BoundExpr::Param(p))
        | (BoundExpr::Param(p), BoundExpr::Column(c))
            if *c == pk_idx =>
        {
            Some(PkEqSource::Param(*p))
        }
        (BoundExpr::Column(c), BoundExpr::Literal(SqlValue::Integer(n)))
        | (BoundExpr::Literal(SqlValue::Integer(n)), BoundExpr::Column(c))
            if *c == pk_idx =>
        {
            Some(PkEqSource::IntLit(*n))
        }
        (BoundExpr::Column(c), BoundExpr::Literal(SqlValue::Text(s)))
        | (BoundExpr::Literal(SqlValue::Text(s)), BoundExpr::Column(c))
            if *c == pk_idx && s.len() <= MAX_SQL_TEXT_PK_BYTES =>
        {
            Some(PkEqSource::TextLit(s.clone()))
        }
        _ => None,
    }
}

fn project_row_with_pk(
    mut obj: Map<String, Value>,
    table: &TableDef,
    proj_indices: &[usize],
    proj_is_last: &[bool],
    verified_int_pk: Option<i64>,
) -> Result<Vec<SqlValue>, SqlError> {
    let pk_idx = table.primary_key_index;
    let mut projected = Vec::with_capacity(proj_indices.len());
    for (&idx, &is_last) in proj_indices.iter().zip(proj_is_last) {
        if let Some(n) = verified_int_pk.filter(|_| idx == pk_idx) {
            projected.push(SqlValue::Integer(n));
            continue;
        }
        let col = &table.columns[idx];
        let val = if is_last {
            take_col_value(&mut obj, col)?
        } else {
            decode_col_ref(&obj, col)?.into_owned()
        };
        projected.push(val);
    }
    Ok(projected)
}

fn get_or_build_select_plan(
    stmt_id: u64,
    stmt: &SelectStmt,
    cached_table: &CachedTable,
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Arc<BoundSelectPlan>, SqlError> {
    let key = (stmt_id, cached_table.generation);
    if let Some(plan) = cache.select_plans.read().get(&key) {
        return Ok(Arc::clone(plan));
    }
    let table = &cached_table.def;
    let (proj_indices, col_names) = match &stmt.columns {
        SelectColumns::All => (
            (0..table.columns.len()).collect::<Vec<_>>(),
            cached_table.all_column_names.clone(),
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
    let mut proj_is_last = Vec::with_capacity(proj_indices.len());
    for (pos, &idx) in proj_indices.iter().enumerate() {
        proj_is_last.push(!proj_indices[pos + 1..].contains(&idx));
    }

    let filter = bind_where_static(stmt.filter.as_ref(), table)?;

    if stmt.order_by.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "ORDER BY item count exceeds max_columns ({})",
            limits.max_columns
        )));
    }
    let mut order_by = Vec::with_capacity(stmt.order_by.len());
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
        order_by.push(BoundOrderBy {
            col_idx,
            desc: item.desc,
            nulls_first,
        });
    }

    let pk_first = order_by
        .first()
        .is_some_and(|first| first.col_idx == table.primary_key_index);
    let order_strategy = if order_by.is_empty() || (pk_first && !order_by[0].desc) {
        OrderStrategy::PkAsc
    } else if pk_first && order_by[0].desc {
        OrderStrategy::PkDesc
    } else {
        OrderStrategy::General
    };
    let simple_pk_eq = filter
        .as_ref()
        .and_then(|f| detect_simple_pk_eq(&f.expr, table.primary_key_index));

    let plan = Arc::new(BoundSelectPlan {
        proj_indices,
        proj_is_last,
        col_names,
        filter,
        order_by,
        order_strategy,
        simple_pk_eq,
    });
    let mut guard = cache.select_plans.write();
    if guard.len() >= MAX_CACHED_PLANS {
        guard.clear();
    }
    guard.insert(key, Arc::clone(&plan));
    Ok(plan)
}

#[allow(clippy::too_many_arguments)]
fn exec_select(
    tx: &DocumentTransaction<'_, '_>,
    stmt_id: u64,
    stmt: &SelectStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
    let plan = get_or_build_select_plan(stmt_id, stmt, &cached_table, limits, cache)?;
    let table = &cached_table.def;

    validate_filter_params(plan.filter.as_ref(), params)?;

    let limit = resolve_limit_value(stmt.limit, params, "LIMIT")?;
    let offset = resolve_limit_value(stmt.offset, params, "OFFSET")?.unwrap_or(0);

    if limit == Some(0) {
        return Ok(SqlOutcome::Rows {
            columns: plan.col_names.clone(),
            rows: Vec::new(),
        });
    }

    // Fast path for single-row PK equality point lookups (`WHERE pk = <lit|param>`).
    if let Some(pk_src) = plan.simple_pk_eq.as_ref().filter(|_| !force_full_scan) {
        let pk_col = &table.columns[table.primary_key_index];
        let pk_val = match pk_src {
            PkEqSource::IntLit(n) => SqlValueRef::Integer(*n),
            PkEqSource::TextLit(s) => SqlValueRef::Text(s.as_str()),
            PkEqSource::Param(p) => SqlValueRef::from_value(&params[*p]),
        };
        match (pk_col.col_type, pk_val) {
            (SqlType::Integer, SqlValueRef::Integer(n)) => {
                let mut id_buf = [0u8; 18];
                encode_i64_pk_bytes(n, &mut id_buf);
                let doc_id = std::str::from_utf8(&id_buf).expect("ASCII hex PK");
                let Some(doc) = tx.get(&table.name, doc_id).map_err(classify_io_error)? else {
                    return Ok(SqlOutcome::Rows {
                        columns: plan.col_names.clone(),
                        rows: Vec::new(),
                    });
                };
                if 1 > limits.max_visited_rows {
                    return Err(SqlError::Bounds(format!(
                        "visited row count exceeds max_visited_rows ({})",
                        limits.max_visited_rows
                    )));
                }
                let obj = doc_into_object(doc)?;
                let verified_pk = verify_doc_pk_id(table, doc_id, &obj)?;
                if offset >= 1 {
                    return Ok(SqlOutcome::Rows {
                        columns: plan.col_names.clone(),
                        rows: Vec::new(),
                    });
                }
                let rows = vec![project_row_with_pk(
                    obj,
                    table,
                    &plan.proj_indices,
                    &plan.proj_is_last,
                    verified_pk,
                )?];
                return Ok(SqlOutcome::Rows {
                    columns: plan.col_names.clone(),
                    rows,
                });
            }
            (SqlType::Text { .. }, SqlValueRef::Text(s)) if s.len() <= MAX_SQL_TEXT_PK_BYTES => {
                let mut txt_buf = [0u8; 256];
                txt_buf[0] = b't';
                txt_buf[1] = b':';
                txt_buf[2..2 + s.len()].copy_from_slice(s.as_bytes());
                let doc_id = std::str::from_utf8(&txt_buf[..2 + s.len()]).expect("valid UTF-8 PK");
                let Some(doc) = tx.get(&table.name, doc_id).map_err(classify_io_error)? else {
                    return Ok(SqlOutcome::Rows {
                        columns: plan.col_names.clone(),
                        rows: Vec::new(),
                    });
                };
                if 1 > limits.max_visited_rows {
                    return Err(SqlError::Bounds(format!(
                        "visited row count exceeds max_visited_rows ({})",
                        limits.max_visited_rows
                    )));
                }
                let obj = doc_into_object(doc)?;
                let verified_pk = verify_doc_pk_id(table, doc_id, &obj)?;
                if offset >= 1 {
                    return Ok(SqlOutcome::Rows {
                        columns: plan.col_names.clone(),
                        rows: Vec::new(),
                    });
                }
                let rows = vec![project_row_with_pk(
                    obj,
                    table,
                    &plan.proj_indices,
                    &plan.proj_is_last,
                    verified_pk,
                )?];
                return Ok(SqlOutcome::Rows {
                    columns: plan.col_names.clone(),
                    rows,
                });
            }
            _ => {}
        }
    }

    let bound_filter = plan.filter.as_ref().map(|f| &f.expr);
    let access = if force_full_scan {
        AccessPlan::FullScan
    } else {
        plan::plan_access(bound_filter, table, &plan.order_by, limit.is_some(), params)
    };
    if matches!(access, AccessPlan::Empty) {
        return Ok(SqlOutcome::Rows {
            columns: plan.col_names.clone(),
            rows: Vec::new(),
        });
    }

    let rows = match plan.order_strategy {
        OrderStrategy::PkAsc => {
            let can_stream = match &access {
                AccessPlan::Index {
                    preserves_pk_asc,
                    satisfies_order_by,
                    ..
                } => *preserves_pk_asc || *satisfies_order_by,
                _ => true,
            };
            if can_stream {
                exec_streaming_select(
                    tx,
                    table,
                    &plan,
                    &access,
                    bound_filter,
                    params,
                    limits,
                    limit,
                    offset,
                )?
            } else {
                exec_pk_sorted_select(
                    tx,
                    table,
                    &plan,
                    &access,
                    bound_filter,
                    params,
                    limits,
                    limit,
                    offset,
                    false,
                )?
            }
        }
        OrderStrategy::PkDesc => {
            let preserves_pk_asc = match &access {
                AccessPlan::Index {
                    preserves_pk_asc, ..
                } => *preserves_pk_asc,
                _ => true,
            };
            if !preserves_pk_asc {
                exec_pk_sorted_select(
                    tx,
                    table,
                    &plan,
                    &access,
                    bound_filter,
                    params,
                    limits,
                    limit,
                    offset,
                    true,
                )?
            } else if let Some(lim) = limit {
                let keep_cap = (offset as u128).saturating_add(lim as u128);
                let mut ring: VecDeque<(Option<i64>, Map<String, Value>)> = VecDeque::new();
                visit_access_rows(
                    tx,
                    table,
                    &access,
                    bound_filter,
                    params,
                    limits,
                    None,
                    |_doc_id, verified_pk, obj| {
                        if (ring.len() as u128) == keep_cap {
                            ring.pop_front();
                            ring.push_back((verified_pk, obj));
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
                            ring.push_back((verified_pk, obj));
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
                    while let Some((verified_pk, obj)) = ring.pop_back() {
                        rows.push(project_row_with_pk(
                            obj,
                            table,
                            &plan.proj_indices,
                            &plan.proj_is_last,
                            verified_pk,
                        )?);
                    }
                    rows
                }
            } else {
                let mut rows = Vec::new();
                visit_access_rows(
                    tx,
                    table,
                    &access,
                    bound_filter,
                    params,
                    limits,
                    None,
                    |_doc_id, verified_pk, obj| {
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
                        rows.push(project_row_with_pk(
                            obj,
                            table,
                            &plan.proj_indices,
                            &plan.proj_is_last,
                            verified_pk,
                        )?);
                        Ok(ControlFlow::Continue(()))
                    },
                )?;
                rows.reverse();
                rows
            }
        }
        OrderStrategy::General => {
            if matches!(
                access,
                AccessPlan::Index {
                    satisfies_order_by: true,
                    ..
                }
            ) {
                exec_streaming_select(
                    tx,
                    table,
                    &plan,
                    &access,
                    bound_filter,
                    params,
                    limits,
                    limit,
                    offset,
                )?
            } else if let Some(lim) = limit {
                let keep_cap = (offset as u128).saturating_add(lim as u128);
                let specs: Rc<[BoundOrderBy]> = Rc::from(plan.order_by.as_slice());
                let mut heap: BinaryHeap<HeapSortRow> = BinaryHeap::new();
                visit_access_rows(
                    tx,
                    table,
                    &access,
                    bound_filter,
                    params,
                    limits,
                    None,
                    |doc_id, verified_pk, obj| {
                        let keys = decode_order_keys(&obj, table, &specs)?;
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
                                    doc_id,
                                    verified_pk,
                                    obj,
                                },
                                specs: Rc::clone(&specs),
                            });
                        } else if let Some(mut top) = heap.peek_mut() {
                            if compare_sort_keys_to_row(&keys, &doc_id, &top.row, &specs)
                                == Ordering::Less
                            {
                                top.row = SortRow {
                                    keys,
                                    doc_id,
                                    verified_pk,
                                    obj,
                                };
                            }
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
                        rows.push(project_row_with_pk(
                            entry.row.obj,
                            table,
                            &plan.proj_indices,
                            &plan.proj_is_last,
                            entry.row.verified_pk,
                        )?);
                    }
                    rows
                }
            } else {
                let mut collected = Vec::new();
                visit_access_rows(
                    tx,
                    table,
                    &access,
                    bound_filter,
                    params,
                    limits,
                    None,
                    |doc_id, verified_pk, obj| {
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
                        let keys = decode_order_keys(&obj, table, &plan.order_by)?;
                        collected.push(SortRow {
                            keys,
                            doc_id,
                            verified_pk,
                            obj,
                        });
                        Ok(ControlFlow::Continue(()))
                    },
                )?;
                collected.sort_unstable_by(|a, b| compare_sort_rows(a, b, &plan.order_by));
                let mut rows = Vec::with_capacity(collected.len());
                for entry in collected {
                    rows.push(project_row_with_pk(
                        entry.obj,
                        table,
                        &plan.proj_indices,
                        &plan.proj_is_last,
                        entry.verified_pk,
                    )?);
                }
                rows
            }
        }
    };

    Ok(SqlOutcome::Rows {
        columns: plan.col_names.clone(),
        rows,
    })
}

#[allow(clippy::too_many_arguments)]
fn exec_streaming_select(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &BoundSelectPlan,
    access: &AccessPlan,
    bound_filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    limit: Option<usize>,
    offset: usize,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    let mut skipped = 0usize;
    let mut rows = Vec::new();
    let stream_cap = if bound_filter.is_none() {
        limit.map(|l| offset.saturating_add(l))
    } else {
        None
    };
    visit_access_rows(
        tx,
        table,
        access,
        bound_filter,
        params,
        limits,
        stream_cap,
        |_doc_id, verified_pk, obj| {
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
            rows.push(project_row_with_pk(
                obj,
                table,
                &plan.proj_indices,
                &plan.proj_is_last,
                verified_pk,
            )?);
            if Some(rows.len()) == limit {
                Ok(ControlFlow::Break(()))
            } else {
                Ok(ControlFlow::Continue(()))
            }
        },
    )?;
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn exec_pk_sorted_select(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &BoundSelectPlan,
    access: &AccessPlan,
    bound_filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    limit: Option<usize>,
    offset: usize,
    desc: bool,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    if let Some(lim) = limit {
        let keep_cap = (offset as u128).saturating_add(lim as u128);
        let mut heap: BinaryHeap<PkSortRow> = BinaryHeap::new();
        visit_access_rows(
            tx,
            table,
            access,
            bound_filter,
            params,
            limits,
            None,
            |doc_id, verified_pk, obj| {
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
                    heap.push(PkSortRow {
                        doc_id,
                        verified_pk,
                        obj,
                        desc,
                    });
                } else if let Some(mut top) = heap.peek_mut() {
                    let ord = doc_id.as_bytes().cmp(top.doc_id.as_bytes());
                    let is_better = if desc {
                        ord == Ordering::Greater
                    } else {
                        ord == Ordering::Less
                    };
                    if is_better {
                        *top = PkSortRow {
                            doc_id,
                            verified_pk,
                            obj,
                            desc,
                        };
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if heap.len() <= offset {
            return Ok(Vec::new());
        }
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
            rows.push(project_row_with_pk(
                entry.obj,
                table,
                &plan.proj_indices,
                &plan.proj_is_last,
                entry.verified_pk,
            )?);
        }
        Ok(rows)
    } else {
        let mut collected = Vec::new();
        visit_access_rows(
            tx,
            table,
            access,
            bound_filter,
            params,
            limits,
            None,
            |doc_id, verified_pk, obj| {
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
                collected.push((doc_id, verified_pk, obj));
                Ok(ControlFlow::Continue(()))
            },
        )?;
        collected.sort_unstable_by(|a, b| {
            let ord = a.0.as_bytes().cmp(b.0.as_bytes());
            if desc { ord.reverse() } else { ord }
        });
        let mut rows = Vec::with_capacity(collected.len());
        for (_doc_id, verified_pk, obj) in collected {
            rows.push(project_row_with_pk(
                obj,
                table,
                &plan.proj_indices,
                &plan.proj_is_last,
                verified_pk,
            )?);
        }
        Ok(rows)
    }
}

fn get_or_build_update_plan(
    stmt_id: u64,
    stmt: &UpdateStmt,
    cached_table: &CachedTable,
    cache: &SqlCache,
) -> Result<Arc<BoundUpdatePlan>, SqlError> {
    let key = (stmt_id, cached_table.generation);
    if let Some(plan) = cache.update_plans.read().get(&key) {
        return Ok(Arc::clone(plan));
    }
    let table = &cached_table.def;
    let mut assignments = Vec::with_capacity(stmt.assignments.len());
    let mut assign_param_checks = Vec::new();
    let mut param_constraints = Vec::new();
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
        let (bound, ty) =
            bind_expr_static(value, Some(table), &table.name, &mut param_constraints)?;
        match ty {
            StaticExprType::Null => {}
            StaticExprType::Typed(actual_ty) => {
                check_assignment_type(&bound, ExprType::Typed(actual_ty), col)?;
            }
            StaticExprType::Param(p) => {
                assign_param_checks.push((p, col.col_type));
            }
        }
        assignments.push((idx, bound));
    }
    let mut filter = bind_where_static(stmt.filter.as_ref(), table)?;
    let extra_param_constraints = if let Some(ref mut f) = filter {
        f.param_constraints.extend(param_constraints);
        Vec::new()
    } else {
        param_constraints
    };
    let plan = Arc::new(BoundUpdatePlan {
        assignments,
        assign_param_checks,
        filter,
        extra_param_constraints,
    });
    let mut guard = cache.update_plans.write();
    if guard.len() >= MAX_CACHED_PLANS {
        guard.clear();
    }
    guard.insert(key, Arc::clone(&plan));
    Ok(plan)
}

fn exec_update(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt_id: u64,
    stmt: &UpdateStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
    let plan = get_or_build_update_plan(stmt_id, stmt, &cached_table, cache)?;
    let table = &cached_table.def;

    for &(param_idx, expected_ty) in &plan.assign_param_checks {
        match &params[param_idx] {
            SqlValue::Null => {}
            SqlValue::Integer(_) if matches!(expected_ty, SqlType::Integer | SqlType::Real) => {}
            SqlValue::Real(_) if matches!(expected_ty, SqlType::Real) => {}
            SqlValue::Text(_) if matches!(expected_ty, SqlType::Text { .. }) => {}
            SqlValue::Boolean(_) if matches!(expected_ty, SqlType::Boolean) => {}
            other => {
                return Err(SqlError::Type(format!(
                    "cannot assign {} to column of type {}",
                    other.kind_name(),
                    expected_ty.display_name()
                )));
            }
        }
    }
    if !plan.extra_param_constraints.is_empty() {
        validate_constraints(&plan.extra_param_constraints, params)?;
    }
    validate_filter_params(plan.filter.as_ref(), params)?;

    let bound_filter = plan.filter.as_ref().map(|f| &f.expr);
    let mut matched = Vec::new();
    visit_matching_rows(
        tx,
        table,
        bound_filter,
        params,
        limits,
        force_full_scan,
        None,
        |doc_id, _verified_pk, mut obj| {
            if matched.len() >= limits.max_mutation_rows {
                return Err(SqlError::Bounds(format!(
                    "UPDATE row count exceeds max_mutation_rows ({})",
                    limits.max_mutation_rows
                )));
            }
            let mut updates = Vec::with_capacity(plan.assignments.len());
            for (col_idx, bound) in &plan.assignments {
                let col = &table.columns[*col_idx];
                let val = eval_expr_ref(bound, table, Some(&obj), params)?.into_owned();
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
                if matches!(val, SqlValue::Null) {
                    obj.remove(&col.name);
                } else {
                    let json_val = sql_value_to_json(&val, &col.name)?;
                    obj.insert(col.name.clone(), json_val);
                }
            }
            matched.push((doc_id, Value::Object(obj)));
            Ok(ControlFlow::Continue(()))
        },
    )?;

    let updated = matched.len();
    for (doc_id, doc) in matched {
        tx.replace(&table.name, &doc_id, &doc, false)
            .map_err(|err| map_write_constraint_error(err, &table.name))?;
    }
    Ok(SqlOutcome::Updated { rows: updated })
}

fn get_or_build_delete_plan(
    stmt_id: u64,
    stmt: &DeleteStmt,
    cached_table: &CachedTable,
    cache: &SqlCache,
) -> Result<Arc<Option<BoundFilter>>, SqlError> {
    let key = (stmt_id, cached_table.generation);
    if let Some(plan) = cache.delete_plans.read().get(&key) {
        return Ok(Arc::clone(plan));
    }
    let filter = bind_where_static(stmt.filter.as_ref(), &cached_table.def)?;
    let plan = Arc::new(filter);
    let mut guard = cache.delete_plans.write();
    if guard.len() >= MAX_CACHED_PLANS {
        guard.clear();
    }
    guard.insert(key, Arc::clone(&plan));
    Ok(plan)
}

fn exec_delete(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt_id: u64,
    stmt: &DeleteStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
    let plan = get_or_build_delete_plan(stmt_id, stmt, &cached_table, cache)?;
    let table = &cached_table.def;
    validate_filter_params(plan.as_ref().as_ref(), params)?;
    let bound_filter = plan.as_ref().as_ref().map(|f| &f.expr);
    let mut to_delete = Vec::new();
    visit_matching_rows(
        tx,
        table,
        bound_filter,
        params,
        limits,
        force_full_scan,
        None,
        |doc_id, _verified_pk, _obj| {
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
    on_match: impl FnMut(String, Option<i64>, Map<String, Value>) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    let plan = if force_full_scan {
        AccessPlan::FullScan
    } else {
        plan::plan_access(filter, table, &[], false, params)
    };
    visit_access_rows(
        tx, table, &plan, filter, params, limits, stream_cap, on_match,
    )
}

#[allow(clippy::too_many_arguments)]
fn visit_access_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &AccessPlan,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    stream_cap: Option<usize>,
    mut on_match: impl FnMut(
        String,
        Option<i64>,
        Map<String, Value>,
    ) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    match plan {
        AccessPlan::Empty => Ok(()),
        AccessPlan::PkPoint { doc_id } => {
            let Some(doc) = tx.get(&table.name, doc_id).map_err(classify_io_error)? else {
                return Ok(());
            };
            if 1 > limits.max_visited_rows {
                return Err(SqlError::Bounds(format!(
                    "visited row count exceeds max_visited_rows ({})",
                    limits.max_visited_rows
                )));
            }
            let obj = doc_into_object(doc)?;
            let verified_pk = verify_doc_pk_id(table, doc_id, &obj)?;
            if eval_where_doc(filter, table, &obj, params)? {
                let _ = on_match(doc_id.clone(), verified_pk, obj)?;
            }
            Ok(())
        }
        AccessPlan::PkRange {
            after_id,
            upper,
            max_exact_rows,
        } => scan_range_rows(
            tx,
            table,
            filter,
            params,
            limits,
            after_id.clone(),
            upper,
            *max_exact_rows,
            stream_cap,
            on_match,
        ),
        AccessPlan::Index {
            index_idx,
            equal,
            lower,
            upper,
            unique_equality,
            ..
        } => visit_index_rows(
            tx,
            table,
            &table.indexes[*index_idx].name,
            equal,
            lower.as_ref(),
            upper.as_ref(),
            *unique_equality,
            filter,
            params,
            limits,
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
fn visit_index_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    index_name: &str,
    equal: &[IndexValue],
    lower: Option<&IndexBound>,
    upper: Option<&IndexBound>,
    unique_equality: bool,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    mut stream_cap: Option<usize>,
    mut on_match: impl FnMut(
        String,
        Option<i64>,
        Map<String, Value>,
    ) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    const MAX_INDEX_QUERY_DOCUMENTS: usize = 256;
    let mut visited = 0usize;
    let mut after: Option<IndexCursor> = None;

    loop {
        let remaining_visit = limits.max_visited_rows.saturating_sub(visited);
        let mut page_limit = remaining_visit
            .saturating_add(1)
            .clamp(1, MAX_INDEX_QUERY_DOCUMENTS);
        if unique_equality {
            page_limit = page_limit.min(2);
        }
        if let Some(cap) = stream_cap {
            if cap == 0 {
                return Ok(());
            }
            page_limit = page_limit.min(cap);
        }
        let query = IndexQuery {
            equal: equal.to_vec(),
            lower: lower.cloned(),
            upper: upper.cloned(),
            after: after.take(),
            limit: page_limit,
        };
        let page = tx
            .query_index(&table.name, index_name, &query)
            .map_err(classify_io_error)?;
        if page.documents.is_empty() {
            return Ok(());
        }
        let done = unique_equality
            || (page.plan.entries_read < page_limit
                && page.plan.returned == page.plan.entries_read);
        after = page.next;

        for entry in page.documents {
            visited += 1;
            if visited > limits.max_visited_rows {
                return Err(SqlError::Bounds(format!(
                    "visited row count exceeds max_visited_rows ({})",
                    limits.max_visited_rows
                )));
            }
            let obj = doc_into_object(entry.document)?;
            let verified_pk = verify_doc_pk_id(table, &entry.id, &obj)?;
            if eval_where_doc(filter, table, &obj, params)? {
                if let Some(cap) = &mut stream_cap {
                    *cap = cap.saturating_sub(1);
                }
                if on_match(entry.id, verified_pk, obj)?.is_break() {
                    return Ok(());
                }
            }
        }
        if done {
            return Ok(());
        }
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
    mut on_match: impl FnMut(
        String,
        Option<i64>,
        Map<String, Value>,
    ) -> Result<ControlFlow<()>, SqlError>,
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
            let verified_pk = verify_doc_pk_id(table, &entry.id, &obj)?;
            if eval_where_doc(filter, table, &obj, params)? {
                if let Some(cap) = &mut stream_cap {
                    *cap = cap.saturating_sub(1);
                }
                if on_match(entry.id, verified_pk, obj)?.is_break() {
                    return Ok(());
                }
            }
            if at_inclusive_end {
                return Ok(());
            }
        }
    }
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
) -> Result<Option<i64>, SqlError> {
    let pk_col = &table.columns[table.primary_key_index];
    let pk_val = decode_col_ref(obj, pk_col)?;
    let (matches, int_pk) = match (pk_col.col_type, pk_val) {
        (SqlType::Integer, SqlValueRef::Integer(n)) => {
            let mut buf = [0u8; 18];
            encode_i64_pk_bytes(n, &mut buf);
            (doc_id.as_bytes() == buf, Some(n))
        }
        (SqlType::Text { .. }, SqlValueRef::Text(s)) => (
            s.len() <= MAX_SQL_TEXT_PK_BYTES
                && doc_id.len() == 2 + s.len()
                && doc_id.starts_with("t:")
                && &doc_id[2..] == s,
            None,
        ),
        _ => (false, None),
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
    Ok(int_pk)
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
