//! Statement binding, expression evaluation, and transactional execution over `DocumentDatabase`.
use super::catalog::{self, CachedTable, ColumnDef, IndexDef, IndexState, TableCache, TableDef};
use super::parser::{
    self, AggregateCall, AggregateKind, Assignment, CompareOp, CreateIndexStmt, CreateTableStmt,
    DeleteStmt, DropIndexStmt, Expr, InsertStmt, LimitValue, LiteralValue, NullsOrder,
    OrderByTarget, SelectColumns, SelectExpr, SelectItem, SelectStmt, SqlStatement, Statement,
    UpdateStmt,
};
use super::plan::{self, AccessPlan, CompiledSelectAccess, ResolvedTemplatePlan, UpperBound};
use super::subquery::{
    self, SelectSink, SubqueryBudget, SubqueryInSet, SubqueryRole, SubquerySlot, SubqueryTypeCheck,
    SubqueryValue,
};
use super::value::{
    MAX_SQL_INDEXES_PER_TABLE, MAX_SQL_TEXT_PK_BYTES, SqlError, SqlLimits, SqlOutcome, SqlType,
    SqlValue, coerce_for_column, compare_i64_f64, encode_i64_pk_bytes, exact_i64_to_f64,
};
use crate::document::flat::{FlatDocument, FlatScalar};
use crate::document::{
    DocumentDatabase, DocumentTransaction, DocumentTransactionMode, IndexBound, IndexQuery,
    IndexValue, StoredDocument,
};
use crate::{MAX_SCAN_ENTRIES, TransactionFailure};
use parking_lot::RwLock;
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
use std::collections::hash_map::RandomState;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io;
use std::ops::Bound;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::Arc;

const MAX_CACHED_STATEMENTS: usize = 128;
const MAX_CACHED_PLANS: usize = 256;
const MAX_CACHED_SQL_BYTES: usize = 4096;

type DeletePlanMap = RwLock<HashMap<(u64, u64), Arc<BoundDeletePlan>>>;

pub(crate) struct SqlCache {
    tables: TableCache,
    stmts: RwLock<HashMap<String, Arc<SqlStatement>>>,
    select_plans: RwLock<HashMap<(u64, u64), Arc<SelectPlan>>>,
    insert_plans: RwLock<HashMap<(u64, u64), Arc<BoundInsertPlan>>>,
    update_plans: RwLock<HashMap<(u64, u64), Arc<BoundUpdatePlan>>>,
    delete_plans: DeletePlanMap,
}

impl SqlCache {
    pub(crate) fn tables(&self) -> &TableCache {
        &self.tables
    }

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

fn validate_params(
    stmt: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
) -> Result<(), SqlError> {
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
    Ok(())
}

pub(crate) fn execute_statement(
    db: &DocumentDatabase<'_>,
    stmt: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    validate_params(stmt, params, limits)?;

    match &stmt.inner {
        Statement::CreateIndex(create_idx) => return exec_create_index(db, create_idx, cache),
        Statement::DropIndex(drop_idx) => return exec_drop_index(db, drop_idx, cache),
        Statement::Join(join) => return super::join::execute(db, join, params, limits, cache),
        Statement::DropTable(drop) => return super::ddl::execute(db, drop, cache),
        Statement::Explain(inner) => {
            return run_tx(db, |tx| explain(tx, inner, params, limits, cache));
        }
        _ => {}
    }

    run_tx(db, |tx| {
        execute_statement_in_validated_tx(tx, stmt, params, limits, cache, force_full_scan)
    })
}

pub(crate) fn execute_statement_in_tx(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    validate_params(stmt, params, limits)?;
    execute_statement_in_validated_tx(tx, stmt, params, limits, cache, force_full_scan)
}

/// Runs a `SELECT` or join statement without parameters inside `tx`, for a
/// derived table or CTE body.
pub(crate) fn run_read_in_tx(
    tx: &DocumentTransaction<'_, '_>,
    stmt: &SqlStatement,
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    match &stmt.inner {
        Statement::Select(select) => {
            exec_select(tx, stmt.stmt_id, select, &[], limits, cache, false)
        }
        Statement::Join(join) => super::join::execute_in_tx(tx, join, &[], limits, cache),
        _ => Err(SqlError::Unsupported {
            feature: "a derived table or CTE that is not a SELECT".into(),
            offset: None,
        }),
    }
}

pub(super) fn read_columns(
    tx: &DocumentTransaction<'_, '_>,
    stmt: &SqlStatement,
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Vec<ColumnDef>, SqlError> {
    let (table, plan) = match &stmt.inner {
        Statement::Select(select) => {
            let table = catalog::load_table_cached(tx, &select.table, &cache.tables)?;
            let plan = get_or_build_select_plan(stmt.stmt_id, select, &table, limits, cache)?;
            (table, plan)
        }
        Statement::Join(join) => return super::join::read_columns(tx, join, limits, cache),
        _ => {
            return Err(SqlError::Unsupported {
                feature: "a derived table or CTE that is not a SELECT".into(),
                offset: None,
            });
        }
    };
    let expression_type = |expr: &BoundExpr| match expr {
        BoundExpr::Literal(SqlValue::Integer(_)) => Some(SqlType::Integer),
        BoundExpr::Literal(SqlValue::Real(_)) => Some(SqlType::Real),
        BoundExpr::Literal(SqlValue::Text(_)) => Some(SqlType::Text { max_chars: None }),
        BoundExpr::Literal(SqlValue::Boolean(_))
        | BoundExpr::Exists(_)
        | BoundExpr::InSubquery { .. }
        | BoundExpr::Not(_)
        | BoundExpr::And(_)
        | BoundExpr::Or(_)
        | BoundExpr::Compare { .. }
        | BoundExpr::IsNull { .. } => Some(SqlType::Boolean),
        _ => None,
    };
    let types: Vec<_> = match plan.as_ref() {
        SelectPlan::Simple(simple) => simple
            .outputs
            .iter()
            .map(|output| match output {
                SimpleSelectOutput::Column { col_idx, .. } => {
                    Some(table.def.columns[*col_idx].col_type)
                }
                SimpleSelectOutput::Expr(expr) => expression_type(expr),
            })
            .collect(),
        SelectPlan::Aggregate(agg) => agg
            .outputs
            .iter()
            .map(|output| match output {
                BoundSelectOutput::GroupCol(pos) => {
                    Some(table.def.columns[agg.group_cols[*pos]].col_type)
                }
                BoundSelectOutput::Aggregate(pos) => agg.aggregates[*pos].output_type(),
                BoundSelectOutput::Expr(expr) => expression_type(expr),
            })
            .collect(),
    };
    plan.col_names()
        .iter()
        .zip(types)
        .map(|(name, col_type)| {
            let col_type = col_type.ok_or_else(|| SqlError::Unsupported {
                feature: format!("joined derived column '{name}' without a static type"),
                offset: None,
            })?;
            Ok(ColumnDef {
                name: name.clone(),
                col_type,
                not_null: false,
                default: None,
            })
        })
        .collect()
}

fn execute_statement_in_validated_tx(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    match &stmt.inner {
        Statement::CreateTable(create) => exec_create_table(tx, create, limits, cache),
        Statement::CreateIndex(_) => Err(SqlError::Unsupported {
            feature: "CREATE INDEX inside an explicit transaction (online index DDL must run outside a transaction)".into(),
            offset: None,
        }),
        Statement::DropIndex(_) => Err(SqlError::Unsupported {
            feature: "DROP INDEX inside an explicit transaction (online index DDL must run outside a transaction)".into(),
            offset: None,
        }),
        Statement::DropTable(_) => Err(SqlError::Unsupported {
            feature: "DROP TABLE inside an explicit transaction (a large table is dropped in several transactions)".into(),
            offset: None,
        }),
        Statement::AlterTable(alter) => super::ddl::alter_in_tx(tx, alter, limits, cache),
        Statement::Explain(inner) => explain(tx, inner, params, limits, cache),
        Statement::Join(join) => super::join::execute_in_tx(tx, join, params, limits, cache),
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
    }
}

pub(crate) fn run_tx<T>(
    db: &DocumentDatabase<'_>,
    action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    run_tx_with_mode(db, DocumentTransactionMode::ReadSet, action)
}

fn run_tx_with_mode<T>(
    db: &DocumentDatabase<'_>,
    mode: DocumentTransactionMode,
    action: impl FnOnce(&mut DocumentTransaction<'_, '_>) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    let mut sql_error: Option<SqlError> = None;
    let outcome = db.transaction_detailed(mode, None, None, |tx| match action(tx) {
        Ok(value) => Ok(value),
        Err(err) => {
            sql_error = Some(err);
            Err(io::Error::other("sql statement aborted"))
        }
    });
    map_transaction_outcome(outcome, sql_error)
}

pub(crate) fn map_transaction_outcome<T>(
    outcome: Result<T, TransactionFailure>,
    sql_error: Option<SqlError>,
) -> Result<T, SqlError> {
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
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    if stmt.columns.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "column count {} exceeds max_columns ({})",
            stmt.columns.len(),
            limits.max_columns
        )));
    }
    let mut columns: Vec<ColumnDef> = Vec::with_capacity(stmt.columns.len());
    for (idx, col) in stmt.columns.iter().enumerate() {
        columns.push(ColumnDef {
            name: col.name.clone(),
            col_type: col.col_type,
            not_null: col.not_null,
            default: column_default(col, stmt.primary_key_columns.contains(&idx), limits)?,
        });
    }
    let indexes: Vec<IndexDef> = stmt
        .unique_constraints
        .iter()
        .enumerate()
        .map(|(i, cols)| IndexDef {
            name: format!("__sql_u_{i}"),
            columns: cols.clone(),
            unique: true,
            state: IndexState::Ready,
            is_constraint: true,
        })
        .collect();
    if catalog::table_exists(tx, &stmt.name)? {
        if stmt.if_not_exists {
            return Ok(SqlOutcome::TableCreated);
        }
        return Err(SqlError::TableExists(stmt.name.clone()));
    }
    let prepared_fks = super::foreign_key::prepare_create_table_foreign_keys(
        tx,
        &stmt.name,
        &columns,
        (stmt.primary_key_columns.len() == 1).then_some(stmt.primary_key_columns[0]),
        &indexes,
        &stmt.foreign_keys,
        &cache.tables,
    )?;
    let mut def = TableDef::with_foreign_keys(
        stmt.name.clone(),
        columns,
        stmt.primary_key_columns[0],
        stmt.primary_key_columns[1..].to_vec(),
        indexes,
        prepared_fks.foreign_keys,
        prepared_fks.self_referenced_by,
    )?;
    def.checks = stmt.checks.clone();
    def.validate()?;
    // Binding checks each expression's columns and types now, so a stored
    // check always binds later.
    bind_table_checks(&def, limits)?;
    catalog::create_table(tx, &def, stmt.if_not_exists)?;
    for (parent_key, parent_def) in prepared_fks.parent_updates {
        parent_def.validate()?;
        catalog::save_table_def(tx, &parent_key, &parent_def)?;
    }
    Ok(SqlOutcome::TableCreated)
}

const ONLINE_STEP_LIMIT: usize = 128;
const MAX_STEP_CONFLICTS: usize = 256;

fn run_maintenance_step<T>(
    db: &DocumentDatabase<'_>,
    mut action: impl FnMut(&mut DocumentTransaction<'_, '_>) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    let mut conflicts = 0usize;
    loop {
        match run_tx_with_mode(db, DocumentTransactionMode::Conservative, |tx| action(tx)) {
            Ok(value) => return Ok(value),
            Err(SqlError::Conflict(err)) => {
                conflicts += 1;
                if conflicts >= MAX_STEP_CONFLICTS {
                    return Err(SqlError::Conflict(err));
                }
                std::thread::yield_now();
            }
            Err(err) => return Err(err),
        }
    }
}

enum CreateIndexAction {
    AlreadyReady,
    FinishDropFirst {
        col_indices: Vec<usize>,
        col_names: Vec<String>,
    },
    ResumeBuild,
    StartFresh {
        col_indices: Vec<usize>,
        col_names: Vec<String>,
    },
}

enum BuildStartState {
    AlreadyReady,
    StartedOrResume,
    NeedsDropFirst,
}

enum BuildProgress {
    AlreadyReady,
    InProgress,
    Complete,
}

fn exec_create_index(
    db: &DocumentDatabase<'_>,
    stmt: &CreateIndexStmt,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let action = run_tx(db, |tx| {
        let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
        let table = &cached_table.def;
        let mut col_indices = Vec::with_capacity(stmt.columns.len());
        let mut col_names = Vec::with_capacity(stmt.columns.len());
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
            col_names.push(col.name.clone());
        }

        if let Some(existing) = table.indexes.iter().find(|i| i.name == stmt.name) {
            return match existing.state {
                IndexState::Ready => {
                    if stmt.if_not_exists {
                        Ok(CreateIndexAction::AlreadyReady)
                    } else {
                        Err(SqlError::IndexExists(stmt.name.clone()))
                    }
                }
                IndexState::Building => {
                    if existing.columns == col_indices && existing.unique == stmt.unique {
                        Ok(CreateIndexAction::ResumeBuild)
                    } else {
                        Err(SqlError::IndexExists(stmt.name.clone()))
                    }
                }
                IndexState::Dropping => Ok(CreateIndexAction::FinishDropFirst {
                    col_indices,
                    col_names,
                }),
            };
        }

        if table.indexes.len() >= MAX_SQL_INDEXES_PER_TABLE {
            return Err(SqlError::Bounds(format!(
                "table '{}' cannot have more than {MAX_SQL_INDEXES_PER_TABLE} indexes",
                table.name
            )));
        }

        Ok(CreateIndexAction::StartFresh {
            col_indices,
            col_names,
        })
    })?;

    let (col_indices, col_names) = match action {
        CreateIndexAction::AlreadyReady => return Ok(SqlOutcome::IndexCreated),
        CreateIndexAction::ResumeBuild => {
            return drive_index_build(db, &stmt.table, &stmt.name, stmt.if_not_exists, cache);
        }
        CreateIndexAction::FinishDropFirst {
            col_indices,
            col_names,
        } => {
            drive_index_drop(db, &stmt.table, &stmt.name, cache)?;
            (col_indices, col_names)
        }
        CreateIndexAction::StartFresh {
            col_indices,
            col_names,
        } => (col_indices, col_names),
    };

    loop {
        omit_explicit_nulls_bounded(db, &stmt.table, &col_names)?;
        let start_state = run_maintenance_step(db, |tx| {
            let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
            if let Some(existing) = cached_table
                .def
                .indexes
                .iter()
                .find(|i| i.name == stmt.name)
            {
                return match existing.state {
                    IndexState::Ready => {
                        if stmt.if_not_exists {
                            Ok(BuildStartState::AlreadyReady)
                        } else {
                            Err(SqlError::IndexExists(stmt.name.clone()))
                        }
                    }
                    IndexState::Building => {
                        if existing.columns == col_indices && existing.unique == stmt.unique {
                            Ok(BuildStartState::StartedOrResume)
                        } else {
                            Err(SqlError::IndexExists(stmt.name.clone()))
                        }
                    }
                    IndexState::Dropping => Ok(BuildStartState::NeedsDropFirst),
                };
            }
            let index_def = IndexDef {
                name: stmt.name.clone(),
                columns: col_indices.clone(),
                unique: stmt.unique,
                state: IndexState::Building,
                is_constraint: false,
            };
            catalog::start_index_build(tx, &cached_table, index_def)?;
            Ok(BuildStartState::StartedOrResume)
        })?;
        match start_state {
            BuildStartState::AlreadyReady => return Ok(SqlOutcome::IndexCreated),
            BuildStartState::StartedOrResume => break,
            BuildStartState::NeedsDropFirst => {
                drive_index_drop(db, &stmt.table, &stmt.name, cache)?;
            }
        }
    }

    drive_index_build(db, &stmt.table, &stmt.name, stmt.if_not_exists, cache)
}

fn drive_index_build(
    db: &DocumentDatabase<'_>,
    table_name: &str,
    index_name: &str,
    if_not_exists: bool,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    loop {
        let step_res = run_maintenance_step(db, |tx| {
            let cached_table = catalog::load_table_cached(tx, table_name, &cache.tables)?;
            match cached_table
                .def
                .indexes
                .iter()
                .find(|i| i.name == index_name)
            {
                Some(i) if i.state == IndexState::Ready => {
                    return Ok(BuildProgress::AlreadyReady);
                }
                Some(i) if i.state == IndexState::Building => {}
                _ => {
                    return Err(SqlError::Conflict(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "index build was cancelled or dropped concurrently",
                    )));
                }
            }
            let step = tx
                .advance_index_build(table_name, index_name, ONLINE_STEP_LIMIT)
                .map_err(|err| catalog::map_index_build_error(err, table_name, index_name))?;
            if step.complete {
                catalog::set_index_state(
                    tx,
                    table_name,
                    index_name,
                    IndexState::Ready,
                    &cache.tables,
                )?;
                Ok(BuildProgress::Complete)
            } else {
                Ok(BuildProgress::InProgress)
            }
        });
        match step_res {
            Ok(BuildProgress::Complete) => return Ok(SqlOutcome::IndexCreated),
            Ok(BuildProgress::AlreadyReady) => {
                if if_not_exists {
                    return Ok(SqlOutcome::IndexCreated);
                }
                return Err(SqlError::IndexExists(index_name.to_owned()));
            }
            Ok(BuildProgress::InProgress) => {}
            Err(err) => {
                if !matches!(err, SqlError::OutcomeUnknown(_) | SqlError::Conflict(_)) {
                    if let Err(cleanup_err) =
                        cleanup_failed_index_build(db, table_name, index_name, cache)
                    {
                        if matches!(cleanup_err, SqlError::OutcomeUnknown(_)) {
                            return Err(cleanup_err);
                        }
                    }
                }
                return Err(err);
            }
        }
    }
}

fn cleanup_failed_index_build(
    db: &DocumentDatabase<'_>,
    table_name: &str,
    index_name: &str,
    cache: &SqlCache,
) -> Result<(), SqlError> {
    let needs_drop = run_maintenance_step(db, |tx| {
        let cached_table = catalog::load_table_cached(tx, table_name, &cache.tables)?;
        let Some(existing) = cached_table
            .def
            .indexes
            .iter()
            .find(|i| i.name == index_name)
        else {
            return Ok(false);
        };
        match existing.state {
            IndexState::Building => {
                tx.cancel_index_build(table_name, index_name)
                    .map_err(classify_io_error)?;
                catalog::set_index_state(
                    tx,
                    table_name,
                    index_name,
                    IndexState::Dropping,
                    &cache.tables,
                )?;
                Ok(true)
            }
            IndexState::Dropping => Ok(true),
            IndexState::Ready => Ok(false),
        }
    })?;
    if needs_drop {
        drive_index_drop(db, table_name, index_name, cache)?;
    }
    Ok(())
}

fn drive_index_drop(
    db: &DocumentDatabase<'_>,
    table_name: &str,
    index_name: &str,
    cache: &SqlCache,
) -> Result<(), SqlError> {
    loop {
        let done = run_maintenance_step(db, |tx| {
            let cached_table = catalog::load_table_cached(tx, table_name, &cache.tables)?;
            match cached_table
                .def
                .indexes
                .iter()
                .find(|i| i.name == index_name)
            {
                Some(i) if i.state == IndexState::Dropping => {}
                None => return Ok(true),
                Some(_) => {
                    return Err(SqlError::Conflict(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "index state changed during drop",
                    )));
                }
            }
            let step = tx
                .advance_index_drop(table_name, index_name, ONLINE_STEP_LIMIT)
                .map_err(classify_io_error)?;
            if step.complete {
                catalog::remove_index(tx, table_name, index_name, &cache.tables)?;
                Ok(true)
            } else {
                Ok(false)
            }
        })?;
        if done {
            return Ok(());
        }
    }
}

enum DropAction {
    Done,
    CancelBuilding,
    FinishDropping,
    StartAndDriveDrop,
}

fn exec_drop_index(
    db: &DocumentDatabase<'_>,
    stmt: &DropIndexStmt,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let action_res = run_tx(db, |tx| {
        let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
        let Some(existing) = cached_table
            .def
            .indexes
            .iter()
            .find(|i| i.name == stmt.name)
        else {
            if stmt.if_exists {
                return Ok(DropAction::Done);
            }
            return Err(SqlError::NoSuchIndex {
                table: stmt.table.clone(),
                index: stmt.name.clone(),
            });
        };
        if existing.is_constraint {
            return Err(SqlError::Constraint(format!(
                "cannot drop index '{}' backing a UNIQUE constraint on table '{}'",
                existing.name, stmt.table
            )));
        }
        super::foreign_key::ensure_index_not_fk_target(&cached_table.def, existing)?;
        match existing.state {
            IndexState::Building => Ok(DropAction::CancelBuilding),
            IndexState::Dropping => Ok(DropAction::FinishDropping),
            IndexState::Ready => {
                tx.drop_index(&stmt.table, &stmt.name)
                    .map_err(classify_io_error)?;
                catalog::remove_index(tx, &stmt.table, &stmt.name, &cache.tables)?;
                Ok(DropAction::Done)
            }
        }
    });

    let action = match action_res {
        Ok(action) => action,
        Err(SqlError::Io(err))
            if err
                .to_string()
                .contains("index drop exceeds key or byte limits") =>
        {
            DropAction::StartAndDriveDrop
        }
        Err(SqlError::Bounds(msg)) if msg.contains("index drop exceeds key or byte limits") => {
            DropAction::StartAndDriveDrop
        }
        Err(err) => return Err(err),
    };

    match action {
        DropAction::Done => Ok(SqlOutcome::IndexDropped),
        DropAction::CancelBuilding => {
            cleanup_failed_index_build(db, &stmt.table, &stmt.name, cache)?;
            Ok(SqlOutcome::IndexDropped)
        }
        DropAction::FinishDropping => {
            drive_index_drop(db, &stmt.table, &stmt.name, cache)?;
            Ok(SqlOutcome::IndexDropped)
        }
        DropAction::StartAndDriveDrop => {
            let needs_drive = run_maintenance_step(db, |tx| {
                let cached_table = catalog::load_table_cached(tx, &stmt.table, &cache.tables)?;
                let Some(existing) = cached_table
                    .def
                    .indexes
                    .iter()
                    .find(|i| i.name == stmt.name)
                else {
                    if stmt.if_exists {
                        return Ok(None);
                    }
                    return Err(SqlError::NoSuchIndex {
                        table: stmt.table.clone(),
                        index: stmt.name.clone(),
                    });
                };
                if existing.is_constraint {
                    return Err(SqlError::Constraint(format!(
                        "cannot drop index '{}' backing a UNIQUE constraint on table '{}'",
                        existing.name, stmt.table
                    )));
                }
                super::foreign_key::ensure_index_not_fk_target(&cached_table.def, existing)?;
                match existing.state {
                    IndexState::Ready => {
                        tx.start_index_drop(&stmt.table, &stmt.name)
                            .map_err(classify_io_error)?;
                        catalog::set_index_state(
                            tx,
                            &stmt.table,
                            &stmt.name,
                            IndexState::Dropping,
                            &cache.tables,
                        )?;
                        Ok(Some(false))
                    }
                    IndexState::Dropping => Ok(Some(false)),
                    IndexState::Building => Ok(Some(true)),
                }
            })?;
            match needs_drive {
                None => Ok(SqlOutcome::IndexDropped),
                Some(true) => {
                    cleanup_failed_index_build(db, &stmt.table, &stmt.name, cache)?;
                    Ok(SqlOutcome::IndexDropped)
                }
                Some(false) => {
                    drive_index_drop(db, &stmt.table, &stmt.name, cache)?;
                    Ok(SqlOutcome::IndexDropped)
                }
            }
        }
    }
}

/// Rows written before NULL was stored by omission may hold explicit JSON
/// `null` in an indexed column. Document indexes treat `null` as a value but
/// skip missing fields, so rewrite those rows to omit the field in bounded
/// steps before the index is built. SQL reads both forms as NULL.
fn omit_explicit_nulls_bounded(
    db: &DocumentDatabase<'_>,
    table_name: &str,
    col_names: &[String],
) -> Result<(), SqlError> {
    let mut after: Option<String> = None;
    loop {
        let next_after = run_maintenance_step(db, |tx| {
            let page = tx
                .scan_documents(table_name, after.as_deref(), ONLINE_STEP_LIMIT)
                .map_err(classify_io_error)?;
            let Some(last) = page.last() else {
                return Ok(None);
            };
            let next_cursor = last.id.clone();
            let mut rewrites = Vec::new();
            for entry in page {
                let Value::Object(mut fields) = entry.document else {
                    continue;
                };
                let mut changed = false;
                for name in col_names {
                    if matches!(fields.get(name.as_str()), Some(Value::Null)) {
                        fields.remove(name.as_str());
                        changed = true;
                    }
                }
                if changed {
                    rewrites.push((entry.id, Value::Object(fields)));
                }
            }
            for (id, document) in rewrites {
                tx.replace(table_name, &id, &document, false)
                    .map_err(classify_io_error)?;
            }
            Ok(Some(next_cursor))
        })?;
        match next_after {
            Some(cursor) => after = Some(cursor),
            None => return Ok(()),
        }
    }
}

struct BoundInsertPlan {
    target_indices: Vec<usize>,
    /// Omitted columns that take their `DEFAULT`.
    defaulted: Vec<usize>,
    checks: Arc<Vec<(String, BoundExpr)>>,
}

fn get_or_build_insert_plan(
    stmt_id: u64,
    stmt: &InsertStmt,
    cached_table: &CachedTable,
    cache: &SqlCache,
    limits: &SqlLimits,
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
                if !indices.contains(&idx) && col.not_null && col.default.is_none() {
                    if table.primary_key_columns().any(|key| key == idx) {
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
    let defaulted = (0..table.columns.len())
        .filter(|idx| !target_indices.contains(idx) && table.columns[*idx].default.is_some())
        .collect();
    let plan = Arc::new(BoundInsertPlan {
        target_indices,
        defaulted,
        checks: bind_table_checks(table, limits)?,
    });
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

#[allow(clippy::too_many_arguments)]
fn eval_insert_expr_to_json(
    tx: &DocumentTransaction<'_, '_>,
    expr: &Expr,
    col: &ColumnDef,
    table: &TableDef,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    budget: &mut SubqueryBudget,
) -> Result<(Value, Option<InsertPk>), SqlError> {
    let max_text_bytes = limits.max_text_bytes;
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
            let mut constraints = Vec::new();
            let mut subqueries = Vec::new();
            let mut subquery_checks = Vec::new();
            let mut sub_ctx = SubqueryBindCtx {
                outer_scopes: &[],
                slots: &mut subqueries,
                checks: &mut subquery_checks,
                context_label: "VALUES",
            };
            let (bound, ty) = bind_expr_static_with(
                other,
                None,
                &table.name,
                &mut constraints,
                &mut sub_ctx,
                None,
            )?;
            validate_constraints(&constraints, params)?;
            let allow_int_to_real = bound.is_int_literal_or_param();
            let resolved = if !subqueries.is_empty() {
                if let StaticExprType::Subquery(sub_id) = ty {
                    subquery_checks.push(SubqueryTypeCheck::AssignToColumn {
                        sub_id,
                        col_name: col.name.clone(),
                        col_type: col.col_type,
                    });
                }
                let prepared = subquery::bind_subqueries(
                    tx,
                    &subqueries,
                    &subquery_checks,
                    &[],
                    params,
                    limits,
                    cache,
                )?;
                let values = subquery::execute_subqueries(
                    tx,
                    &prepared,
                    &subquery_checks,
                    params,
                    limits,
                    cache,
                    budget,
                    false,
                )?;
                subquery::resolve_bound_expr(&bound, &values)
            } else {
                bound
            };
            let val = eval_expr_ref(&resolved, EvalContext::None, params)?.into_owned();
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
    let plan = get_or_build_insert_plan(stmt_id, stmt, &cached_table, cache, limits)?;
    let table = &cached_table.def;
    let pk_idx = table.primary_key_index;
    let pk_type = table.columns[pk_idx].col_type;
    let mut sub_budget = SubqueryBudget::new(limits);

    if stmt.rows.len() == 1 && table.has_scalar_pk() {
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
        for &col_idx in &plan.defaulted {
            let col = &table.columns[col_idx];
            if let Some(default) = &col.default {
                obj.insert(col.name.clone(), default.clone());
            }
        }
        let mut pk_val: Option<InsertPk> = None;
        for (expr, &col_idx) in tuple.iter().zip(&plan.target_indices) {
            let col = &table.columns[col_idx];
            let (json_val, maybe_pk) = eval_insert_expr_to_json(
                tx,
                expr,
                col,
                table,
                params,
                limits,
                cache,
                &mut sub_budget,
            )?;
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
        enforce_checks(&plan.checks, table, &obj)?;
        let doc = Value::Object(obj);
        tx.insert(&table.name, doc_id, &doc)
            .map_err(|err| map_write_constraint_error(err, &table.name))?;
        super::foreign_key::check_inserted_docs(tx, table, &[&doc], &cache.tables)?;
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
        for &col_idx in &plan.defaulted {
            let col = &table.columns[col_idx];
            if let Some(default) = &col.default {
                obj.insert(col.name.clone(), default.clone());
            }
        }
        let mut pk_val: Option<InsertPk> = None;
        for (expr, &col_idx) in tuple.iter().zip(&plan.target_indices) {
            let col = &table.columns[col_idx];
            let (json_val, maybe_pk) = eval_insert_expr_to_json(
                tx,
                expr,
                col,
                table,
                params,
                limits,
                cache,
                &mut sub_budget,
            )?;
            if col_idx == pk_idx {
                pk_val = maybe_pk;
            }
            if !json_val.is_null() {
                obj.insert(col.name.clone(), json_val);
            }
        }
        let doc_id = if table.has_scalar_pk() {
            let pk = pk_val
                .ok_or_else(|| SqlError::Constraint("PRIMARY KEY value cannot be NULL".into()))?;
            let mut integer = [0; 18];
            let mut text = [0; 256];
            encode_insert_pk_stack(&pk, pk_type, &mut integer, &mut text)?.to_owned()
        } else {
            compound_row_id(table, |index| decode_col_ref(&obj, &table.columns[index]))?
        };
        if !seen_pks.insert(doc_id.clone()) {
            return Err(SqlError::Constraint(format!(
                "duplicate primary key in INSERT on table '{}'",
                table.name
            )));
        }
        enforce_checks(&plan.checks, table, &obj)?;
        prepared_rows.push((doc_id, Value::Object(obj)));
    }

    let inserted = prepared_rows.len();
    for (doc_id, doc) in &prepared_rows {
        tx.insert(&table.name, doc_id, doc)
            .map_err(|err| map_write_constraint_error(err, &table.name))?;
    }
    super::foreign_key::check_inserted_prepared_rows(tx, table, &prepared_rows, &cache.tables)?;
    Ok(SqlOutcome::Inserted { rows: inserted })
}

pub(crate) fn map_write_constraint_error(err: io::Error, table_name: &str) -> SqlError {
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
    #[allow(dead_code)]
    pub(crate) nulls_first: bool,
}

#[derive(Clone, Debug)]
struct SortRow {
    keys: Vec<SqlValue>,
    doc_id: String,
    verified_pk: Option<i64>,
    obj: Map<String, Value>,
}

#[derive(Clone)]
struct HeapSortRow {
    row: SortRow,
    specs: Rc<[SimpleOrderSpec]>,
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

fn compare_sort_rows(a: &SortRow, b: &SortRow, specs: &[SimpleOrderSpec]) -> Ordering {
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
    specs: &[SimpleOrderSpec],
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
    specs: &[SimpleOrderSpec],
    params: &[SqlValue],
) -> Result<Vec<SqlValue>, SqlError> {
    let mut keys = Vec::with_capacity(specs.len());
    for spec in specs {
        match &spec.target {
            SimpleOrderTarget::Column(col_idx) => {
                keys.push(decode_col_ref(obj, &table.columns[*col_idx])?.into_owned());
            }
            SimpleOrderTarget::Expr(expr) => {
                keys.push(
                    eval_expr_ref(expr, EvalContext::Doc { table, obj }, params)?.into_owned(),
                );
            }
        }
    }
    Ok(keys)
}

#[derive(Clone, Copy)]
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

pub(crate) enum SelectPlan {
    Simple(BoundSelectPlan),
    Aggregate(BoundAggregatePlan),
}

impl SelectPlan {
    pub(crate) fn subquery_metadata(&self) -> (&[SubquerySlot], &[SubqueryTypeCheck]) {
        match self {
            Self::Simple(simple) => (&simple.subqueries, &simple.subquery_checks),
            Self::Aggregate(agg) => (&agg.subqueries, &agg.subquery_checks),
        }
    }

    pub(crate) fn validate_params(&self, params: &[SqlValue]) -> Result<(), SqlError> {
        match self {
            Self::Simple(simple) => {
                validate_constraints(&simple.output_param_constraints, params)?;
                validate_filter_params(simple.filter.as_ref(), params)
            }
            Self::Aggregate(agg) => {
                validate_constraints(&agg.output_param_constraints, params)?;
                validate_filter_params(agg.filter.as_ref(), params)?;
                validate_filter_params(agg.having.as_ref(), params)
            }
        }
    }

    pub(crate) fn col_names(&self) -> &[String] {
        match self {
            Self::Simple(simple) => &simple.col_names,
            Self::Aggregate(agg) => &agg.col_names,
        }
    }

    pub(crate) fn single_aggregate_output_type(&self) -> Option<StaticExprType> {
        let Self::Aggregate(agg) = self else {
            return None;
        };
        if agg.outputs.len() != 1 {
            return None;
        }
        let BoundSelectOutput::Aggregate(idx) = &agg.outputs[0] else {
            return None;
        };
        let spec = &agg.aggregates[*idx];
        spec.output_type().map(StaticExprType::Typed)
    }

    pub(crate) fn first_select_subquery_id(&self) -> Option<usize> {
        match self {
            Self::Simple(simple) => match simple.outputs.first() {
                Some(SimpleSelectOutput::Expr(BoundExpr::ScalarSubquery(id))) => Some(*id),
                _ => None,
            },
            Self::Aggregate(agg) => match agg.outputs.first() {
                Some(BoundSelectOutput::Expr(BoundExpr::ScalarSubquery(id))) => Some(*id),
                _ => None,
            },
        }
    }
}

#[derive(Clone)]
enum SimpleSelectOutput {
    Column { col_idx: usize, take: bool },
    Expr(BoundExpr),
}

#[derive(Clone)]
enum SimpleOrderTarget {
    Column(usize),
    Expr(BoundExpr),
}

#[derive(Clone)]
struct SimpleOrderSpec {
    target: SimpleOrderTarget,
    desc: bool,
    nulls_first: bool,
}

pub(crate) struct BoundSelectPlan {
    /// Columns a row needs after filtering: projection, ordering and key.
    wanted_columns: Vec<bool>,
    outputs: Vec<SimpleSelectOutput>,
    col_names: Vec<String>,
    output_param_constraints: Vec<ParamConstraint>,
    filter: Option<BoundFilter>,
    order_specs: Vec<SimpleOrderSpec>,
    order_by: Vec<BoundOrderBy>,
    order_strategy: OrderStrategy,
    simple_pk_eq: Option<PkEqSource>,
    compiled_access: CompiledSelectAccess,
    subqueries: Vec<SubquerySlot>,
    subquery_checks: Vec<SubqueryTypeCheck>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BoundAggregate {
    pub(crate) kind: AggregateKind,
    pub(crate) col_idx: Option<usize>,
    pub(crate) col_type: Option<SqlType>,
}

impl BoundAggregate {
    pub(super) fn output_type(self) -> Option<SqlType> {
        match self.kind {
            AggregateKind::CountStar | AggregateKind::Count | AggregateKind::CountDistinct => {
                Some(SqlType::Integer)
            }
            AggregateKind::Avg => Some(SqlType::Real),
            AggregateKind::Sum | AggregateKind::Min | AggregateKind::Max => self.col_type,
        }
    }
}

#[derive(Clone, Debug)]
enum BoundSelectOutput {
    GroupCol(usize),
    Aggregate(usize),
    Expr(BoundExpr),
}

#[derive(Clone, Copy, Debug)]
enum BoundGroupOrderTarget {
    GroupCol(usize),
    Aggregate(usize),
    Output(usize),
}

#[derive(Clone, Copy, Debug)]
struct BoundGroupOrderBy {
    target: BoundGroupOrderTarget,
    desc: bool,
    nulls_first: bool,
}

pub(crate) struct BoundAggregatePlan {
    col_names: Vec<String>,
    outputs: Vec<BoundSelectOutput>,
    output_param_constraints: Vec<ParamConstraint>,
    group_cols: Vec<usize>,
    aggregates: Vec<BoundAggregate>,
    filter: Option<BoundFilter>,
    having: Option<BoundFilter>,
    order_by: Vec<BoundGroupOrderBy>,
    ungrouped_min_col: Option<usize>,
    subqueries: Vec<SubquerySlot>,
    subquery_checks: Vec<SubqueryTypeCheck>,
}

struct BoundUpdatePlan {
    assignments: Vec<(usize, BoundExpr)>,
    assign_param_checks: Vec<(usize, SqlType)>,
    filter: Option<BoundFilter>,
    extra_param_constraints: Vec<ParamConstraint>,
    subqueries: Vec<SubquerySlot>,
    subquery_checks: Vec<SubqueryTypeCheck>,
    checks: Arc<Vec<(String, BoundExpr)>>,
}

struct BoundDeletePlan {
    filter: Option<BoundFilter>,
    subqueries: Vec<SubquerySlot>,
    subquery_checks: Vec<SubqueryTypeCheck>,
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

pub(crate) fn param_sql_type(v: &SqlValue) -> Option<SqlType> {
    match v {
        SqlValue::Null => None,
        SqlValue::Integer(_) => Some(SqlType::Integer),
        SqlValue::Real(_) => Some(SqlType::Real),
        SqlValue::Text(_) => Some(SqlType::Text { max_chars: None }),
        SqlValue::Boolean(_) => Some(SqlType::Boolean),
    }
}

pub(crate) fn resolve_limit_value(
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
pub(crate) enum StaticExprType {
    Null,
    Typed(SqlType),
    Param(usize),
    Subquery(usize),
}

pub(crate) fn literal_static_type(lit: &LiteralValue) -> StaticExprType {
    match lit {
        LiteralValue::Null => StaticExprType::Null,
        LiteralValue::Integer(_) => StaticExprType::Typed(SqlType::Integer),
        LiteralValue::Real(_) => StaticExprType::Typed(SqlType::Real),
        LiteralValue::Text(_) => StaticExprType::Typed(SqlType::Text { max_chars: None }),
        LiteralValue::Boolean(_) => StaticExprType::Typed(SqlType::Boolean),
    }
}

fn literal_to_sql_value(lit: &LiteralValue) -> SqlValue {
    match lit {
        LiteralValue::Null => SqlValue::Null,
        LiteralValue::Integer(n) => SqlValue::Integer(*n),
        LiteralValue::Real(f) => SqlValue::Real(*f),
        LiteralValue::Text(s) => SqlValue::Text(s.clone()),
        LiteralValue::Boolean(b) => SqlValue::Boolean(*b),
    }
}

pub(crate) struct SubqueryBindCtx<'a> {
    pub(crate) outer_scopes: &'a [&'a TableDef],
    pub(crate) slots: &'a mut Vec<SubquerySlot>,
    pub(crate) checks: &'a mut Vec<SubqueryTypeCheck>,
    pub(crate) context_label: &'static str,
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

struct AggBindContext<'a> {
    group_cols: &'a [usize],
    aggregates: &'a mut Vec<BoundAggregate>,
}

fn intern_aggregate(
    table: &TableDef,
    agg: &AggregateCall,
    aggregates: &mut Vec<BoundAggregate>,
    outer_scopes: &[&TableDef],
) -> Result<(usize, SqlType), SqlError> {
    let (col_idx, col_type, result_type) = match agg.kind {
        AggregateKind::CountStar => (None, None, SqlType::Integer),
        AggregateKind::Count | AggregateKind::CountDistinct => {
            let name = agg.column.as_deref().expect("COUNT column");
            let idx = table.column_index(name).ok_or_else(|| {
                subquery::missing_column_error(table, name, agg.offset, outer_scopes)
            })?;
            (
                Some(idx),
                Some(table.columns[idx].col_type),
                SqlType::Integer,
            )
        }
        AggregateKind::Sum => {
            let name = agg.column.as_deref().expect("SUM column");
            let idx = table.column_index(name).ok_or_else(|| {
                subquery::missing_column_error(table, name, agg.offset, outer_scopes)
            })?;
            let ty = table.columns[idx].col_type;
            if !ty.is_numeric() {
                return Err(SqlError::Type(format!(
                    "SUM cannot be applied to {} column '{}'",
                    ty.display_name(),
                    name
                )));
            }
            (Some(idx), Some(ty), ty)
        }
        AggregateKind::Avg => {
            let name = agg.column.as_deref().expect("AVG column");
            let idx = table.column_index(name).ok_or_else(|| {
                subquery::missing_column_error(table, name, agg.offset, outer_scopes)
            })?;
            let ty = table.columns[idx].col_type;
            if !ty.is_numeric() {
                return Err(SqlError::Type(format!(
                    "AVG cannot be applied to {} column '{}'",
                    ty.display_name(),
                    name
                )));
            }
            (Some(idx), Some(ty), SqlType::Real)
        }
        AggregateKind::Min | AggregateKind::Max => {
            let name = agg.column.as_deref().expect("MIN/MAX column");
            let idx = table.column_index(name).ok_or_else(|| {
                subquery::missing_column_error(table, name, agg.offset, outer_scopes)
            })?;
            let ty = table.columns[idx].col_type;
            (Some(idx), Some(ty), ty)
        }
    };

    let bound = BoundAggregate {
        kind: agg.kind,
        col_idx,
        col_type,
    };
    let pos = if let Some(existing) = aggregates.iter().position(|a| *a == bound) {
        existing
    } else {
        let idx = aggregates.len();
        aggregates.push(bound);
        idx
    };
    Ok((pos, result_type))
}

fn bind_where_static(
    filter: Option<&Expr>,
    table: &TableDef,
    sub_ctx: &mut SubqueryBindCtx<'_>,
) -> Result<Option<BoundFilter>, SqlError> {
    bind_boolean_clause_static(filter, table, "WHERE", sub_ctx, None)
}

fn bind_having_static(
    having: Option<&Expr>,
    table: &TableDef,
    group_cols: &[usize],
    aggregates: &mut Vec<BoundAggregate>,
    sub_ctx: &mut SubqueryBindCtx<'_>,
) -> Result<Option<BoundFilter>, SqlError> {
    let mut ctx = AggBindContext {
        group_cols,
        aggregates,
    };
    bind_boolean_clause_static(having, table, "HAVING", sub_ctx, Some(&mut ctx))
}

fn bind_boolean_clause_static(
    expr: Option<&Expr>,
    table: &TableDef,
    clause_name: &'static str,
    sub_ctx: &mut SubqueryBindCtx<'_>,
    agg_ctx: Option<&mut AggBindContext<'_>>,
) -> Result<Option<BoundFilter>, SqlError> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    let mut param_constraints = Vec::new();
    let (bound, ty) = bind_expr_static_with(
        expr,
        Some(table),
        &table.name,
        &mut param_constraints,
        sub_ctx,
        agg_ctx,
    )?;
    match ty {
        StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
        StaticExprType::Param(p) => param_constraints.push(ParamConstraint::Boolean(p)),
        StaticExprType::Subquery(sub_id) => {
            sub_ctx.checks.push(SubqueryTypeCheck::Boolean {
                sub_id,
                context: clause_name,
            });
        }
        StaticExprType::Typed(other) => {
            return Err(SqlError::Type(format!(
                "{clause_name} clause must evaluate to BOOLEAN, got {}",
                other.display_name()
            )));
        }
    }
    Ok(Some(BoundFilter {
        expr: bound,
        param_constraints,
    }))
}

fn check_compare_static_types(
    l_ty: StaticExprType,
    r_ty: StaticExprType,
    constraints: &mut Vec<ParamConstraint>,
    sub_ctx: &mut SubqueryBindCtx<'_>,
) -> Result<(), SqlError> {
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
        (StaticExprType::Typed(t), StaticExprType::Subquery(sub_id))
        | (StaticExprType::Subquery(sub_id), StaticExprType::Typed(t)) => {
            sub_ctx.checks.push(SubqueryTypeCheck::CompatibleWithType {
                sub_id,
                expected: t,
            });
        }
        (StaticExprType::Param(param_idx), StaticExprType::Subquery(sub_id))
        | (StaticExprType::Subquery(sub_id), StaticExprType::Param(param_idx)) => {
            sub_ctx
                .checks
                .push(SubqueryTypeCheck::CompatibleWithParam { sub_id, param_idx });
        }
        (StaticExprType::Subquery(left_sub_id), StaticExprType::Subquery(right_sub_id)) => {
            sub_ctx.checks.push(SubqueryTypeCheck::CompatiblePair {
                left_sub_id,
                right_sub_id,
            });
        }
    }
    Ok(())
}

fn bind_expr_static_with(
    expr: &Expr,
    table: Option<&TableDef>,
    table_name: &str,
    constraints: &mut Vec<ParamConstraint>,
    sub_ctx: &mut SubqueryBindCtx<'_>,
    mut agg_ctx: Option<&mut AggBindContext<'_>>,
) -> Result<(BoundExpr, StaticExprType), SqlError> {
    match expr {
        Expr::Literal(lit) => Ok((
            BoundExpr::Literal(literal_to_sql_value(lit)),
            literal_static_type(lit),
        )),
        Expr::Param(idx) => Ok((BoundExpr::Param(*idx), StaticExprType::Param(*idx))),
        Expr::QualifiedColumn {
            table: qualifier,
            column,
        } => {
            if table.is_some_and(|table| table.name == *qualifier) {
                return bind_expr_static_with(
                    &Expr::Column(column.clone()),
                    table,
                    table_name,
                    constraints,
                    sub_ctx,
                    agg_ctx,
                );
            }
            if sub_ctx
                .outer_scopes
                .iter()
                .any(|table| table.name == *qualifier)
            {
                return Err(SqlError::Unsupported {
                    feature: format!("correlated subquery reference to '{qualifier}.{column}'"),
                    offset: None,
                });
            }
            Err(SqlError::NoSuchColumn {
                table: qualifier.clone(),
                column: column.clone(),
            })
        }
        Expr::Column(name) => {
            let Some(table) = table else {
                if sub_ctx
                    .outer_scopes
                    .iter()
                    .any(|outer| outer.column_index(name).is_some())
                {
                    return Err(SqlError::Unsupported {
                        feature: format!("correlated subquery reference to outer column '{name}'"),
                        offset: None,
                    });
                }
                return Err(SqlError::NoSuchColumn {
                    table: table_name.to_owned(),
                    column: name.clone(),
                });
            };
            let idx = table.column_index(name).ok_or_else(|| {
                subquery::missing_column_error(table, name, 0, sub_ctx.outer_scopes)
            })?;
            let ty = StaticExprType::Typed(table.columns[idx].col_type);
            if let Some(ctx) = agg_ctx.as_deref() {
                let group_pos = ctx
                    .group_cols
                    .iter()
                    .position(|&g| g == idx)
                    .ok_or_else(|| SqlError::Syntax {
                        offset: 0,
                        message: format!(
                            "column '{}.{}' must appear in the GROUP BY clause or be used in an aggregate function",
                            table.name, name
                        ),
                    })?;
                Ok((BoundExpr::Column(group_pos), ty))
            } else {
                Ok((BoundExpr::Column(idx), ty))
            }
        }
        Expr::Aggregate(agg) => {
            let Some(table) = table else {
                return Err(SqlError::Unsupported {
                    feature: format!(
                        "aggregate function '{}'",
                        agg.kind.sql_name().to_ascii_uppercase()
                    ),
                    offset: Some(agg.offset),
                });
            };
            let Some(ctx) = agg_ctx.as_deref_mut() else {
                return Err(SqlError::Unsupported {
                    feature: format!(
                        "aggregate function '{}'",
                        agg.kind.sql_name().to_ascii_uppercase()
                    ),
                    offset: Some(agg.offset),
                });
            };
            let (agg_idx, ty) = intern_aggregate(table, agg, ctx.aggregates, sub_ctx.outer_scopes)?;
            Ok((BoundExpr::Aggregate(agg_idx), StaticExprType::Typed(ty)))
        }
        Expr::IsNull { expr, negated } => {
            let (bound, _) =
                bind_expr_static_with(expr, table, table_name, constraints, sub_ctx, agg_ctx)?;
            Ok((
                BoundExpr::IsNull {
                    expr: Box::new(bound),
                    negated: *negated,
                },
                StaticExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::Not(inner) => {
            let (bound, ty) =
                bind_expr_static_with(inner, table, table_name, constraints, sub_ctx, agg_ctx)?;
            match ty {
                StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
                StaticExprType::Param(p) => constraints.push(ParamConstraint::Boolean(p)),
                StaticExprType::Subquery(sub_id) => {
                    sub_ctx.checks.push(SubqueryTypeCheck::Boolean {
                        sub_id,
                        context: "NOT",
                    });
                }
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
                let (bound, ty) = bind_expr_static_with(
                    item,
                    table,
                    table_name,
                    constraints,
                    sub_ctx,
                    agg_ctx.as_deref_mut(),
                )?;
                match ty {
                    StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
                    StaticExprType::Param(p) => constraints.push(ParamConstraint::Boolean(p)),
                    StaticExprType::Subquery(sub_id) => {
                        sub_ctx.checks.push(SubqueryTypeCheck::Boolean {
                            sub_id,
                            context: "AND",
                        });
                    }
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
                let (bound, ty) = bind_expr_static_with(
                    item,
                    table,
                    table_name,
                    constraints,
                    sub_ctx,
                    agg_ctx.as_deref_mut(),
                )?;
                match ty {
                    StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
                    StaticExprType::Param(p) => constraints.push(ParamConstraint::Boolean(p)),
                    StaticExprType::Subquery(sub_id) => {
                        sub_ctx.checks.push(SubqueryTypeCheck::Boolean {
                            sub_id,
                            context: "OR",
                        });
                    }
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
            let (l_bound, l_ty) = bind_expr_static_with(
                left,
                table,
                table_name,
                constraints,
                sub_ctx,
                agg_ctx.as_deref_mut(),
            )?;
            let (r_bound, r_ty) =
                bind_expr_static_with(right, table, table_name, constraints, sub_ctx, agg_ctx)?;
            check_compare_static_types(l_ty, r_ty, constraints, sub_ctx)?;
            Ok((
                BoundExpr::Compare {
                    op: *op,
                    left: Box::new(l_bound),
                    right: Box::new(r_bound),
                },
                StaticExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::Exists(sub) => {
            if let Some(membership) =
                table.and_then(|table| subquery::decorrelate_exists(sub, table))
            {
                let (expr, ty) = bind_expr_static_with(
                    &membership,
                    table,
                    table_name,
                    constraints,
                    sub_ctx,
                    agg_ctx,
                )?;
                // Unlike IN, EXISTS returns false when the correlation key
                // or matching inner values are NULL.
                return Ok((
                    BoundExpr::And(vec![
                        BoundExpr::IsNull {
                            expr: Box::new(expr.clone()),
                            negated: true,
                        },
                        expr,
                    ]),
                    ty,
                ));
            }
            let sub_id = sub_ctx.slots.len();
            sub_ctx.slots.push(SubquerySlot {
                role: SubqueryRole::Exists,
                context: sub_ctx.context_label,
                subquery: (**sub).clone(),
            });
            Ok((
                BoundExpr::Exists(sub_id),
                StaticExprType::Typed(SqlType::Boolean),
            ))
        }
        Expr::ScalarSubquery(sub) => {
            let sub_id = sub_ctx.slots.len();
            sub_ctx.slots.push(SubquerySlot {
                role: SubqueryRole::Scalar,
                context: sub_ctx.context_label,
                subquery: (**sub).clone(),
            });
            Ok((
                BoundExpr::ScalarSubquery(sub_id),
                StaticExprType::Subquery(sub_id),
            ))
        }
        Expr::InSubquery {
            expr,
            subquery: sub,
            negated,
        } => {
            let (bound_lhs, lhs_ty) =
                bind_expr_static_with(expr, table, table_name, constraints, sub_ctx, agg_ctx)?;
            let sub_id = sub_ctx.slots.len();
            sub_ctx.slots.push(SubquerySlot {
                role: SubqueryRole::In,
                context: sub_ctx.context_label,
                subquery: (**sub).clone(),
            });
            check_compare_static_types(
                lhs_ty,
                StaticExprType::Subquery(sub_id),
                constraints,
                sub_ctx,
            )?;
            Ok((
                BoundExpr::InSubquery {
                    expr: Box::new(bound_lhs),
                    sub_id,
                    negated: *negated,
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

fn collect_bound_expr_cols(expr: &BoundExpr, used: &mut [bool]) {
    match expr {
        BoundExpr::Column(idx) => {
            if let Some(slot) = used.get_mut(*idx) {
                *slot = true;
            }
        }
        BoundExpr::IsNull { expr, .. }
        | BoundExpr::Not(expr)
        | BoundExpr::InSubquery { expr, .. }
        | BoundExpr::InSet { expr, .. } => collect_bound_expr_cols(expr, used),
        BoundExpr::Compare { left, right, .. } => {
            collect_bound_expr_cols(left, used);
            collect_bound_expr_cols(right, used);
        }
        BoundExpr::And(items) | BoundExpr::Or(items) => {
            for item in items {
                collect_bound_expr_cols(item, used);
            }
        }
        BoundExpr::Literal(_)
        | BoundExpr::ScalarValue(_)
        | BoundExpr::Param(_)
        | BoundExpr::Aggregate(_)
        | BoundExpr::Exists(_)
        | BoundExpr::ScalarSubquery(_) => {}
    }
}

fn bound_expr_uses_col(expr: &BoundExpr, col_idx: usize) -> bool {
    match expr {
        BoundExpr::Column(idx) => *idx == col_idx,
        BoundExpr::IsNull { expr, .. }
        | BoundExpr::Not(expr)
        | BoundExpr::InSubquery { expr, .. }
        | BoundExpr::InSet { expr, .. } => bound_expr_uses_col(expr, col_idx),
        BoundExpr::Compare { left, right, .. } => {
            bound_expr_uses_col(left, col_idx) || bound_expr_uses_col(right, col_idx)
        }
        BoundExpr::And(items) | BoundExpr::Or(items) => {
            items.iter().any(|item| bound_expr_uses_col(item, col_idx))
        }
        BoundExpr::Literal(_)
        | BoundExpr::ScalarValue(_)
        | BoundExpr::Param(_)
        | BoundExpr::Aggregate(_)
        | BoundExpr::Exists(_)
        | BoundExpr::ScalarSubquery(_) => false,
    }
}

fn bind_select_expr_item(
    item_expr: &SelectExpr,
    table: &TableDef,
    constraints: &mut Vec<ParamConstraint>,
    sub_ctx: &mut SubqueryBindCtx<'_>,
    agg_ctx: Option<&mut AggBindContext<'_>>,
) -> Result<BoundExpr, SqlError> {
    match item_expr {
        SelectExpr::Literal { value, .. } => Ok(BoundExpr::Literal(literal_to_sql_value(value))),
        SelectExpr::Param { idx } => Ok(BoundExpr::Param(*idx)),
        SelectExpr::ScalarSubquery(sub) => {
            let sub_id = sub_ctx.slots.len();
            sub_ctx.slots.push(SubquerySlot {
                role: SubqueryRole::Scalar,
                context: sub_ctx.context_label,
                subquery: (**sub).clone(),
            });
            Ok(BoundExpr::ScalarSubquery(sub_id))
        }
        SelectExpr::Exists { subquery, negated } => {
            let (inner, _) = bind_expr_static_with(
                &Expr::Exists(subquery.clone()),
                Some(table),
                &table.name,
                constraints,
                sub_ctx,
                agg_ctx,
            )?;
            Ok(if *negated {
                BoundExpr::Not(Box::new(inner))
            } else {
                inner
            })
        }
        SelectExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let (bound_lhs, lhs_ty) = bind_expr_static_with(
                expr,
                Some(table),
                &table.name,
                constraints,
                sub_ctx,
                agg_ctx,
            )?;
            let sub_id = sub_ctx.slots.len();
            sub_ctx.slots.push(SubquerySlot {
                role: SubqueryRole::In,
                context: sub_ctx.context_label,
                subquery: (**subquery).clone(),
            });
            check_compare_static_types(
                lhs_ty,
                StaticExprType::Subquery(sub_id),
                constraints,
                sub_ctx,
            )?;
            Ok(BoundExpr::InSubquery {
                expr: Box::new(bound_lhs),
                sub_id,
                negated: *negated,
            })
        }
        SelectExpr::Column { .. } | SelectExpr::Aggregate(_) => {
            unreachable!("column and aggregate items handled separately")
        }
    }
}

fn default_select_item_name(expr: &SelectExpr) -> String {
    match expr {
        SelectExpr::Column { name, .. } => name.clone(),
        SelectExpr::Aggregate(agg) => format_aggregate_name(agg),
        SelectExpr::Literal { .. } => "literal".to_owned(),
        SelectExpr::Param { .. } => "param".to_owned(),
        SelectExpr::ScalarSubquery(_) => "subquery".to_owned(),
        SelectExpr::Exists { negated: false, .. } => "exists".to_owned(),
        SelectExpr::Exists { negated: true, .. } => "not exists".to_owned(),
        SelectExpr::InSubquery { negated: false, .. } => "in_subquery".to_owned(),
        SelectExpr::InSubquery { negated: true, .. } => "not_in_subquery".to_owned(),
    }
}

fn project_simple_row(
    mut obj: Map<String, Value>,
    table: &TableDef,
    outputs: &[SimpleSelectOutput],
    params: &[SqlValue],
    verified_int_pk: Option<i64>,
) -> Result<Vec<SqlValue>, SqlError> {
    let pk_idx = table.primary_key_index;
    let mut projected = Vec::with_capacity(outputs.len());
    for output in outputs {
        match output {
            SimpleSelectOutput::Column { col_idx, take } => {
                if let Some(n) = verified_int_pk.filter(|_| *col_idx == pk_idx) {
                    projected.push(SqlValue::Integer(n));
                    continue;
                }
                let col = &table.columns[*col_idx];
                let val = if *take {
                    take_col_value(&mut obj, col)?
                } else {
                    decode_col_ref(&obj, col)?.into_owned()
                };
                projected.push(val);
            }
            SimpleSelectOutput::Expr(expr) => {
                let val = eval_expr_ref(expr, EvalContext::Doc { table, obj: &obj }, params)?
                    .into_owned();
                projected.push(val);
            }
        }
    }
    Ok(projected)
}

fn format_aggregate_name(agg: &AggregateCall) -> String {
    match agg.kind {
        AggregateKind::CountStar => "count(*)".to_owned(),
        AggregateKind::CountDistinct => {
            format!("count(distinct {})", agg.column.as_deref().unwrap_or(""))
        }
        AggregateKind::Count
        | AggregateKind::Sum
        | AggregateKind::Avg
        | AggregateKind::Min
        | AggregateKind::Max => format!(
            "{}({})",
            agg.kind.sql_name(),
            agg.column.as_deref().unwrap_or("")
        ),
    }
}

fn find_unique_select_alias(
    items: &[SelectItem],
    name: &str,
    offset: usize,
) -> Result<Option<usize>, SqlError> {
    let mut matched: Option<usize> = None;
    for (idx, item) in items.iter().enumerate() {
        if item.alias.as_deref() == Some(name) {
            if matched.is_some() {
                return Err(SqlError::Syntax {
                    offset,
                    message: format!("ORDER BY '{name}' is ambiguous"),
                });
            }
            matched = Some(idx);
        }
    }
    Ok(matched)
}

fn get_or_build_select_plan(
    stmt_id: u64,
    stmt: &SelectStmt,
    cached_table: &CachedTable,
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Arc<SelectPlan>, SqlError> {
    get_or_build_select_plan_with_outer(stmt_id, stmt, cached_table, &[], limits, cache)
}

pub(crate) fn get_or_build_select_plan_with_outer(
    stmt_id: u64,
    stmt: &SelectStmt,
    cached_table: &CachedTable,
    outer_scopes: &[&TableDef],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Arc<SelectPlan>, SqlError> {
    let key = (stmt_id, cached_table.generation);
    if let Some(plan) = cache.select_plans.read().get(&key) {
        return Ok(Arc::clone(plan));
    }

    let select_has_aggregate = match &stmt.columns {
        SelectColumns::All => false,
        SelectColumns::Items(items) => items
            .iter()
            .any(|item| matches!(item.expr, SelectExpr::Aggregate(_))),
    };
    let order_has_aggregate = stmt
        .order_by
        .iter()
        .any(|item| matches!(item.target, OrderByTarget::Aggregate(_)));
    let is_aggregate_query = !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || select_has_aggregate
        || order_has_aggregate;

    let plan = if is_aggregate_query {
        build_aggregate_select_plan(stmt, cached_table, outer_scopes, limits)?
    } else {
        build_simple_select_plan(stmt, cached_table, outer_scopes, limits)?
    };

    let mut guard = cache.select_plans.write();
    if guard.len() >= MAX_CACHED_PLANS {
        guard.clear();
    }
    guard.insert(key, Arc::clone(&plan));
    Ok(plan)
}

fn build_simple_select_plan(
    stmt: &SelectStmt,
    cached_table: &CachedTable,
    outer_scopes: &[&TableDef],
    limits: &SqlLimits,
) -> Result<Arc<SelectPlan>, SqlError> {
    let table = &cached_table.def;
    let mut subqueries = Vec::new();
    let mut subquery_checks = Vec::new();
    let mut output_param_constraints = Vec::new();
    let mut sub_ctx = SubqueryBindCtx {
        outer_scopes,
        slots: &mut subqueries,
        checks: &mut subquery_checks,
        context_label: "SELECT",
    };

    let (mut outputs, col_names, select_items_for_alias) = match &stmt.columns {
        SelectColumns::All => (
            (0..table.columns.len())
                .map(|col_idx| SimpleSelectOutput::Column {
                    col_idx,
                    take: true,
                })
                .collect::<Vec<_>>(),
            cached_table.all_column_names.clone(),
            None,
        ),
        SelectColumns::Items(items) => {
            if items.len() > limits.max_columns {
                return Err(SqlError::Bounds(format!(
                    "SELECT column list exceeds max_columns ({})",
                    limits.max_columns
                )));
            }
            let mut bound_outputs = Vec::with_capacity(items.len());
            let mut names = Vec::with_capacity(items.len());
            for item in items {
                match &item.expr {
                    SelectExpr::Column { name, offset } => {
                        let idx = table.column_index(name).ok_or_else(|| {
                            subquery::missing_column_error(
                                table,
                                name,
                                *offset,
                                sub_ctx.outer_scopes,
                            )
                        })?;
                        bound_outputs.push(SimpleSelectOutput::Column {
                            col_idx: idx,
                            take: false,
                        });
                    }
                    SelectExpr::Aggregate(_) => {
                        unreachable!("non-aggregate plan has no aggregate items")
                    }
                    other => {
                        let bound = bind_select_expr_item(
                            other,
                            table,
                            &mut output_param_constraints,
                            &mut sub_ctx,
                            None,
                        )?;
                        bound_outputs.push(SimpleSelectOutput::Expr(bound));
                    }
                }
                names.push(
                    item.alias
                        .clone()
                        .unwrap_or_else(|| default_select_item_name(&item.expr)),
                );
            }
            (bound_outputs, names, Some(items.as_slice()))
        }
    };

    for pos in 0..outputs.len() {
        if let SimpleSelectOutput::Column { col_idx, .. } = outputs[pos] {
            let used_later = outputs[pos + 1..].iter().any(|later| match later {
                SimpleSelectOutput::Column { col_idx: c, .. } => *c == col_idx,
                SimpleSelectOutput::Expr(expr) => bound_expr_uses_col(expr, col_idx),
            });
            outputs[pos] = SimpleSelectOutput::Column {
                col_idx,
                take: !used_later,
            };
        }
    }

    sub_ctx.context_label = "WHERE";
    let filter = bind_where_static(stmt.filter.as_ref(), table, &mut sub_ctx)?;

    if stmt.order_by.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "ORDER BY item count exceeds max_columns ({})",
            limits.max_columns
        )));
    }
    let mut order_specs = Vec::with_capacity(stmt.order_by.len());
    for item in &stmt.order_by {
        let OrderByTarget::Column(col_name) = &item.target else {
            unreachable!("non-aggregate plan has only column ORDER BY items")
        };
        let target = if let Some(items) = select_items_for_alias {
            if let Some(pos) = find_unique_select_alias(items, col_name, item.offset)? {
                match &outputs[pos] {
                    SimpleSelectOutput::Column { col_idx, .. } => {
                        SimpleOrderTarget::Column(*col_idx)
                    }
                    SimpleSelectOutput::Expr(expr) => SimpleOrderTarget::Expr(expr.clone()),
                }
            } else {
                let col_idx = table.column_index(col_name).ok_or_else(|| {
                    subquery::missing_column_error(table, col_name, item.offset, outer_scopes)
                })?;
                SimpleOrderTarget::Column(col_idx)
            }
        } else {
            let col_idx = table.column_index(col_name).ok_or_else(|| {
                subquery::missing_column_error(table, col_name, item.offset, outer_scopes)
            })?;
            SimpleOrderTarget::Column(col_idx)
        };
        let nulls_first = match item.nulls {
            Some(NullsOrder::First) => true,
            Some(NullsOrder::Last) => false,
            None => !item.desc,
        };
        order_specs.push(SimpleOrderSpec {
            target,
            desc: item.desc,
            nulls_first,
        });
    }

    let all_order_are_cols = order_specs
        .iter()
        .all(|s| matches!(s.target, SimpleOrderTarget::Column(_)));
    let order_by: Vec<BoundOrderBy> = if all_order_are_cols {
        order_specs
            .iter()
            .map(|s| {
                let SimpleOrderTarget::Column(col_idx) = s.target else {
                    unreachable!()
                };
                BoundOrderBy {
                    col_idx,
                    desc: s.desc,
                    nulls_first: s.nulls_first,
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    let pk_first = table.has_scalar_pk()
        && matches!(
            order_specs.first(),
            Some(SimpleOrderSpec {
                target: SimpleOrderTarget::Column(col_idx),
                ..
            }) if *col_idx == table.primary_key_index
        );
    let order_strategy = if order_specs.is_empty() || (pk_first && !order_specs[0].desc) {
        OrderStrategy::PkAsc
    } else if pk_first && order_specs[0].desc {
        OrderStrategy::PkDesc
    } else {
        OrderStrategy::General
    };
    let simple_pk_eq = filter.as_ref().and_then(|f| {
        table
            .has_scalar_pk()
            .then(|| detect_simple_pk_eq(&f.expr, table.primary_key_index))
            .flatten()
    });
    let compiled_access = plan::compile_select_access(
        filter.as_ref().map(|f| &f.expr),
        table,
        &order_by,
        stmt.limit.is_some(),
    );

    let mut wanted_columns = vec![false; table.columns.len()];
    for key in table.primary_key_columns() {
        wanted_columns[key] = true;
    }
    for output in &outputs {
        match output {
            SimpleSelectOutput::Column { col_idx, .. } => wanted_columns[*col_idx] = true,
            SimpleSelectOutput::Expr(expr) => collect_bound_expr_cols(expr, &mut wanted_columns),
        }
    }
    for spec in &order_specs {
        match &spec.target {
            SimpleOrderTarget::Column(col_idx) => wanted_columns[*col_idx] = true,
            SimpleOrderTarget::Expr(expr) => collect_bound_expr_cols(expr, &mut wanted_columns),
        }
    }
    Ok(Arc::new(SelectPlan::Simple(BoundSelectPlan {
        wanted_columns,
        outputs,
        col_names,
        output_param_constraints,
        filter,
        order_specs,
        order_by,
        order_strategy,
        simple_pk_eq,
        compiled_access,
        subqueries,
        subquery_checks,
    })))
}

fn build_aggregate_select_plan(
    stmt: &SelectStmt,
    cached_table: &CachedTable,
    outer_scopes: &[&TableDef],
    limits: &SqlLimits,
) -> Result<Arc<SelectPlan>, SqlError> {
    let table = &cached_table.def;
    if stmt.group_by.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "GROUP BY column count exceeds max_columns ({})",
            limits.max_columns
        )));
    }
    let mut group_cols = Vec::with_capacity(stmt.group_by.len());
    for item in &stmt.group_by {
        let col_idx = table.column_index(&item.column).ok_or_else(|| {
            subquery::missing_column_error(table, &item.column, item.offset, outer_scopes)
        })?;
        if !group_cols.contains(&col_idx) {
            group_cols.push(col_idx);
        }
    }

    let mut aggregates = Vec::new();
    let expanded_all_items;
    let select_items: &[SelectItem] = match &stmt.columns {
        SelectColumns::All => {
            if table.columns.len() > limits.max_columns {
                return Err(SqlError::Bounds(format!(
                    "SELECT column list exceeds max_columns ({})",
                    limits.max_columns
                )));
            }
            expanded_all_items = table
                .columns
                .iter()
                .map(|c| SelectItem {
                    expr: SelectExpr::Column {
                        name: c.name.clone(),
                        offset: 0,
                    },
                    alias: None,
                })
                .collect::<Vec<_>>();
            &expanded_all_items
        }
        SelectColumns::Items(items) => {
            if items.len() > limits.max_columns {
                return Err(SqlError::Bounds(format!(
                    "SELECT column list exceeds max_columns ({})",
                    limits.max_columns
                )));
            }
            items
        }
    };

    let mut subqueries = Vec::new();
    let mut subquery_checks = Vec::new();
    let mut output_param_constraints = Vec::new();
    let mut sub_ctx = SubqueryBindCtx {
        outer_scopes,
        slots: &mut subqueries,
        checks: &mut subquery_checks,
        context_label: "SELECT",
    };

    let mut outputs = Vec::with_capacity(select_items.len());
    let mut col_names = Vec::with_capacity(select_items.len());
    for item in select_items {
        match &item.expr {
            SelectExpr::Column { name, offset } => {
                let col_idx = table.column_index(name).ok_or_else(|| {
                    subquery::missing_column_error(table, name, *offset, sub_ctx.outer_scopes)
                })?;
                let group_pos = group_cols
                    .iter()
                    .position(|&g| g == col_idx)
                    .ok_or_else(|| SqlError::Syntax {
                        offset: *offset,
                        message: format!(
                            "column '{}.{}' must appear in the GROUP BY clause or be used in an aggregate function",
                            table.name, name
                        ),
                    })?;
                outputs.push(BoundSelectOutput::GroupCol(group_pos));
            }
            SelectExpr::Aggregate(agg) => {
                let (agg_idx, _) =
                    intern_aggregate(table, agg, &mut aggregates, sub_ctx.outer_scopes)?;
                outputs.push(BoundSelectOutput::Aggregate(agg_idx));
            }
            other => {
                let mut agg_ctx = AggBindContext {
                    group_cols: &group_cols,
                    aggregates: &mut aggregates,
                };
                let bound = bind_select_expr_item(
                    other,
                    table,
                    &mut output_param_constraints,
                    &mut sub_ctx,
                    Some(&mut agg_ctx),
                )?;
                outputs.push(BoundSelectOutput::Expr(bound));
            }
        }
        col_names.push(
            item.alias
                .clone()
                .unwrap_or_else(|| default_select_item_name(&item.expr)),
        );
    }

    sub_ctx.context_label = "WHERE";
    let filter = bind_where_static(stmt.filter.as_ref(), table, &mut sub_ctx)?;
    sub_ctx.context_label = "HAVING";
    let having = bind_having_static(
        stmt.having.as_ref(),
        table,
        &group_cols,
        &mut aggregates,
        &mut sub_ctx,
    )?;

    if stmt.order_by.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "ORDER BY item count exceeds max_columns ({})",
            limits.max_columns
        )));
    }
    let mut order_by = Vec::with_capacity(stmt.order_by.len());
    for item in &stmt.order_by {
        let target = match &item.target {
            OrderByTarget::Column(name) => {
                if let Some(pos) = find_unique_select_alias(select_items, name, item.offset)? {
                    match &outputs[pos] {
                        BoundSelectOutput::GroupCol(g) => BoundGroupOrderTarget::GroupCol(*g),
                        BoundSelectOutput::Aggregate(a) => BoundGroupOrderTarget::Aggregate(*a),
                        BoundSelectOutput::Expr(_) => BoundGroupOrderTarget::Output(pos),
                    }
                } else {
                    let col_idx = table.column_index(name).ok_or_else(|| {
                        subquery::missing_column_error(table, name, item.offset, outer_scopes)
                    })?;
                    let group_pos = group_cols
                        .iter()
                        .position(|&g| g == col_idx)
                        .ok_or_else(|| SqlError::Syntax {
                            offset: item.offset,
                            message: format!(
                                "column '{}.{}' must appear in the GROUP BY clause or be used in an aggregate function",
                                table.name, name
                            ),
                        })?;
                    BoundGroupOrderTarget::GroupCol(group_pos)
                }
            }
            OrderByTarget::Aggregate(agg) => {
                let (agg_idx, _) = intern_aggregate(table, agg, &mut aggregates, outer_scopes)?;
                BoundGroupOrderTarget::Aggregate(agg_idx)
            }
        };
        let nulls_first = match item.nulls {
            Some(NullsOrder::First) => true,
            Some(NullsOrder::Last) => false,
            None => !item.desc,
        };
        order_by.push(BoundGroupOrderBy {
            target,
            desc: item.desc,
            nulls_first,
        });
    }

    let ungrouped_min_col = if group_cols.is_empty()
        && aggregates.len() == 1
        && aggregates[0].kind == AggregateKind::Min
    {
        aggregates[0].col_idx
    } else {
        None
    };

    Ok(Arc::new(SelectPlan::Aggregate(BoundAggregatePlan {
        col_names,
        outputs,
        output_param_constraints,
        group_cols,
        aggregates,
        filter,
        having,
        order_by,
        ungrouped_min_col,
        subqueries,
        subquery_checks,
    })))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn exec_select_plan_into(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &SelectPlan,
    stmt: &SelectStmt,
    params: &[SqlValue],
    subquery_values: &[SubqueryValue],
    limits: &SqlLimits,
    force_full_scan: bool,
    sink: &mut SelectSink<'_>,
) -> Result<(), SqlError> {
    match plan {
        SelectPlan::Simple(simple) => exec_simple_select_into(
            tx,
            table,
            simple,
            stmt,
            params,
            subquery_values,
            limits,
            force_full_scan,
            sink,
        ),
        SelectPlan::Aggregate(agg) => exec_aggregate_select_into(
            tx,
            table,
            agg,
            stmt,
            params,
            subquery_values,
            limits,
            force_full_scan,
            sink,
        ),
    }
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
    plan.validate_params(params)?;
    let limit = resolve_limit_value(stmt.limit, params, "LIMIT")?;
    let _ = resolve_limit_value(stmt.offset, params, "OFFSET")?;

    let (sub_slots, sub_checks) = plan.subquery_metadata();
    let prepared = if sub_slots.is_empty() {
        Vec::new()
    } else {
        subquery::bind_subqueries(
            tx,
            sub_slots,
            sub_checks,
            &[cached_table.def.as_ref()],
            params,
            limits,
            cache,
        )?
    };
    if limit == Some(0) {
        return Ok(SqlOutcome::Rows {
            columns: plan.col_names().to_vec(),
            rows: Vec::new(),
        });
    }

    let mut budget = SubqueryBudget::new(limits);
    let sub_values = if prepared.is_empty() {
        Vec::new()
    } else {
        subquery::execute_subqueries(
            tx,
            &prepared,
            sub_checks,
            params,
            limits,
            cache,
            &mut budget,
            force_full_scan,
        )?
    };
    let outer_limits = budget.sub_limits(limits);
    let mut sink = SelectSink::Client {
        rows: Vec::new(),
        max_rows: outer_limits.max_select_rows,
    };
    let res = exec_select_plan_into(
        tx,
        &cached_table.def,
        &plan,
        stmt,
        params,
        &sub_values,
        &outer_limits,
        force_full_scan,
        &mut sink,
    );
    if !sub_values.is_empty() {
        res.map_err(|err| subquery::remap_visited_bounds(err, limits.max_visited_rows))?;
    } else {
        res?;
    }
    let SelectSink::Client { rows, .. } = sink else {
        unreachable!();
    };
    Ok(SqlOutcome::Rows {
        columns: plan.col_names().to_vec(),
        rows,
    })
}

/// The access path a simple SELECT reads through, and the WHERE expression
/// still to be checked on each row (`None` when the index proves it).
/// Execution and EXPLAIN share it, so EXPLAIN reports the path used.
fn simple_select_access<'p>(
    plan: &'p BoundSelectPlan,
    table: &TableDef,
    params: &[SqlValue],
    has_limit: bool,
    force_full_scan: bool,
) -> (AccessPlan, Option<&'p BoundExpr>) {
    let mut bound_filter = plan.filter.as_ref().map(|f| &f.expr);
    let access = if force_full_scan {
        AccessPlan::FullScan
    } else {
        match &plan.compiled_access {
            CompiledSelectAccess::Static(cached) => cached.clone(),
            CompiledSelectAccess::Index(tmpl) => {
                match plan::resolve_compiled_index_template(tmpl, params) {
                    ResolvedTemplatePlan::Plan {
                        access,
                        skip_filter,
                    } => {
                        if skip_filter {
                            bound_filter = None;
                        }
                        access
                    }
                    ResolvedTemplatePlan::Fallback => {
                        plan::plan_access(bound_filter, table, &plan.order_by, has_limit, params)
                    }
                }
            }
            CompiledSelectAccess::Dynamic => {
                plan::plan_access(bound_filter, table, &plan.order_by, has_limit, params)
            }
        }
    };
    (access, bound_filter)
}

#[allow(clippy::too_many_arguments)]
fn exec_simple_select_into(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &BoundSelectPlan,
    stmt: &SelectStmt,
    params: &[SqlValue],
    subquery_values: &[SubqueryValue],
    limits: &SqlLimits,
    force_full_scan: bool,
    sink: &mut SelectSink<'_>,
) -> Result<(), SqlError> {
    let limit = resolve_limit_value(stmt.limit, params, "LIMIT")?;
    let offset = resolve_limit_value(stmt.offset, params, "OFFSET")?.unwrap_or(0);

    if limit == Some(0) {
        return Ok(());
    }

    let resolved_outputs: Vec<SimpleSelectOutput>;
    let outputs: &[SimpleSelectOutput] = if subquery_values.is_empty() {
        &plan.outputs
    } else {
        resolved_outputs = plan
            .outputs
            .iter()
            .map(|o| match o {
                SimpleSelectOutput::Column { col_idx, take } => SimpleSelectOutput::Column {
                    col_idx: *col_idx,
                    take: *take,
                },
                SimpleSelectOutput::Expr(expr) => {
                    SimpleSelectOutput::Expr(subquery::resolve_bound_expr(expr, subquery_values))
                }
            })
            .collect();
        &resolved_outputs
    };

    // Fast path for single-row PK equality point lookups (`WHERE pk = <lit|param>`).
    if subquery_values.is_empty() && !force_full_scan {
        if let Some(pk_src) = plan.simple_pk_eq.as_ref() {
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
                        return Ok(());
                    };
                    if 1 > limits.max_visited_rows {
                        return Err(SqlError::Bounds(format!(
                            "visited row count exceeds max_visited_rows ({})",
                            limits.max_visited_rows
                        )));
                    }
                    subquery::record_visited_rows(1);
                    let obj = doc_into_object(doc)?;
                    let verified_pk = verify_doc_pk_id(table, doc_id, &obj)?;
                    if offset < 1 {
                        let _ = sink.push_row(project_simple_row(
                            obj,
                            table,
                            outputs,
                            params,
                            verified_pk,
                        )?)?;
                    }
                    return Ok(());
                }
                (SqlType::Text { .. }, SqlValueRef::Text(s))
                    if s.len() <= MAX_SQL_TEXT_PK_BYTES =>
                {
                    let mut txt_buf = [0u8; 256];
                    txt_buf[0] = b't';
                    txt_buf[1] = b':';
                    txt_buf[2..2 + s.len()].copy_from_slice(s.as_bytes());
                    let doc_id =
                        std::str::from_utf8(&txt_buf[..2 + s.len()]).expect("valid UTF-8 PK");
                    let Some(doc) = tx.get(&table.name, doc_id).map_err(classify_io_error)? else {
                        return Ok(());
                    };
                    if 1 > limits.max_visited_rows {
                        return Err(SqlError::Bounds(format!(
                            "visited row count exceeds max_visited_rows ({})",
                            limits.max_visited_rows
                        )));
                    }
                    subquery::record_visited_rows(1);
                    let obj = doc_into_object(doc)?;
                    let verified_pk = verify_doc_pk_id(table, doc_id, &obj)?;
                    if offset < 1 {
                        let _ = sink.push_row(project_simple_row(
                            obj,
                            table,
                            outputs,
                            params,
                            verified_pk,
                        )?)?;
                    }
                    return Ok(());
                }
                _ => {}
            }
        }
    }

    let resolved_filter: Option<BoundExpr>;
    let (access, bound_filter) = if subquery_values.is_empty() {
        simple_select_access(plan, table, params, limit.is_some(), force_full_scan)
    } else {
        resolved_filter = plan
            .filter
            .as_ref()
            .map(|f| subquery::resolve_bound_expr(&f.expr, subquery_values));
        let f_ref = resolved_filter.as_ref();
        let access = if force_full_scan {
            AccessPlan::FullScan
        } else {
            plan::plan_access(f_ref, table, &plan.order_by, limit.is_some(), params)
        };
        (access, f_ref)
    };
    if matches!(access, AccessPlan::Empty) {
        return Ok(());
    }

    let resolved_specs: Vec<SimpleOrderSpec>;
    let order_specs: &[SimpleOrderSpec] = if subquery_values.is_empty() {
        &plan.order_specs
    } else {
        resolved_specs = plan
            .order_specs
            .iter()
            .map(|s| SimpleOrderSpec {
                target: match &s.target {
                    SimpleOrderTarget::Column(c) => SimpleOrderTarget::Column(*c),
                    SimpleOrderTarget::Expr(e) => {
                        SimpleOrderTarget::Expr(subquery::resolve_bound_expr(e, subquery_values))
                    }
                },
                desc: s.desc,
                nulls_first: s.nulls_first,
            })
            .collect();
        &resolved_specs
    };

    let unordered_shortcut = sink.is_exists() || (sink.is_scalar() && limit.is_none());
    let effective_strategy = if unordered_shortcut {
        OrderStrategy::PkAsc
    } else {
        plan.order_strategy
    };

    match effective_strategy {
        OrderStrategy::PkAsc => {
            let can_stream = unordered_shortcut
                || match &access {
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
                    plan,
                    outputs,
                    access,
                    bound_filter,
                    params,
                    limits,
                    limit,
                    offset,
                    sink,
                )?;
            } else {
                exec_pk_sorted_select(
                    tx,
                    table,
                    plan,
                    outputs,
                    access,
                    bound_filter,
                    params,
                    limits,
                    limit,
                    offset,
                    false,
                    sink,
                )?;
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
                    plan,
                    outputs,
                    access,
                    bound_filter,
                    params,
                    limits,
                    limit,
                    offset,
                    true,
                    sink,
                )?;
            } else if let Some(lim) = limit {
                let keep_cap = (offset as u128).saturating_add(lim as u128);
                let mut ring: VecDeque<(Option<i64>, Map<String, Value>)> = VecDeque::new();
                visit_access_rows(
                    tx,
                    table,
                    access,
                    bound_filter,
                    params,
                    limits,
                    Some(&plan.wanted_columns),
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
                            if offset == 0 {
                                if let Some(max_rows) = sink.max_output_rows() {
                                    if ring.len() >= max_rows {
                                        return Err(SqlError::Bounds(format!(
                                            "SELECT result exceeds max_select_rows ({max_rows})"
                                        )));
                                    }
                                }
                            }
                            ring.push_back((verified_pk, obj));
                        }
                        Ok(ControlFlow::Continue(()))
                    },
                )?;
                if ring.len() > offset {
                    let result_len = ring.len() - offset;
                    if let Some(max_rows) = sink.max_output_rows() {
                        if result_len > max_rows {
                            return Err(SqlError::Bounds(format!(
                                "SELECT result exceeds max_select_rows ({max_rows})"
                            )));
                        }
                    }
                    for _ in 0..offset {
                        ring.pop_back();
                    }
                    while let Some((verified_pk, obj)) = ring.pop_back() {
                        let row = project_simple_row(obj, table, outputs, params, verified_pk)?;
                        if matches!(sink.push_row(row)?, ControlFlow::Break(())) {
                            break;
                        }
                    }
                }
            } else {
                let mut rows = Vec::new();
                visit_access_rows(
                    tx,
                    table,
                    access,
                    bound_filter,
                    params,
                    limits,
                    Some(&plan.wanted_columns),
                    None,
                    |_doc_id, verified_pk, obj| {
                        if let Some(max_rows) = sink.max_output_rows() {
                            if rows.len() >= max_rows {
                                return Err(SqlError::Bounds(format!(
                                    "SELECT result exceeds max_select_rows ({max_rows})"
                                )));
                            }
                        }
                        if rows.len() >= limits.max_sort_rows {
                            return Err(SqlError::Bounds(format!(
                                "sort row count exceeds max_sort_rows ({})",
                                limits.max_sort_rows
                            )));
                        }
                        rows.push(project_simple_row(
                            obj,
                            table,
                            outputs,
                            params,
                            verified_pk,
                        )?);
                        Ok(ControlFlow::Continue(()))
                    },
                )?;
                rows.reverse();
                for row in rows.into_iter().skip(offset) {
                    if matches!(sink.push_row(row)?, ControlFlow::Break(())) {
                        break;
                    }
                }
            }
        }
        OrderStrategy::General => {
            if !plan.order_by.is_empty()
                && matches!(
                    access,
                    AccessPlan::Index {
                        satisfies_order_by: true,
                        ..
                    }
                )
            {
                exec_streaming_select(
                    tx,
                    table,
                    plan,
                    outputs,
                    access,
                    bound_filter,
                    params,
                    limits,
                    limit,
                    offset,
                    sink,
                )?;
            } else if let Some(lim) = limit {
                let keep_cap = (offset as u128).saturating_add(lim as u128);
                let specs: Rc<[SimpleOrderSpec]> = Rc::from(order_specs);
                let mut heap: BinaryHeap<HeapSortRow> = BinaryHeap::new();
                visit_access_rows(
                    tx,
                    table,
                    access,
                    bound_filter,
                    params,
                    limits,
                    Some(&plan.wanted_columns),
                    None,
                    |doc_id, verified_pk, obj| {
                        let keys = decode_order_keys(&obj, table, &specs, params)?;
                        if (heap.len() as u128) < keep_cap {
                            if heap.len() >= limits.max_sort_rows {
                                return Err(SqlError::Bounds(format!(
                                    "sort row count exceeds max_sort_rows ({})",
                                    limits.max_sort_rows
                                )));
                            }
                            if offset == 0 {
                                if let Some(max_rows) = sink.max_output_rows() {
                                    if heap.len() >= max_rows {
                                        return Err(SqlError::Bounds(format!(
                                            "SELECT result exceeds max_select_rows ({max_rows})"
                                        )));
                                    }
                                }
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
                if heap.len() > offset {
                    let result_len = heap.len() - offset;
                    if let Some(max_rows) = sink.max_output_rows() {
                        if result_len > max_rows {
                            return Err(SqlError::Bounds(format!(
                                "SELECT result exceeds max_select_rows ({max_rows})"
                            )));
                        }
                    }
                    let sorted = heap.into_sorted_vec();
                    for entry in sorted.into_iter().skip(offset) {
                        let row = project_simple_row(
                            entry.row.obj,
                            table,
                            outputs,
                            params,
                            entry.row.verified_pk,
                        )?;
                        if matches!(sink.push_row(row)?, ControlFlow::Break(())) {
                            break;
                        }
                    }
                }
            } else {
                let mut collected = Vec::new();
                visit_access_rows(
                    tx,
                    table,
                    access,
                    bound_filter,
                    params,
                    limits,
                    Some(&plan.wanted_columns),
                    None,
                    |doc_id, verified_pk, obj| {
                        if let Some(max_rows) = sink.max_output_rows() {
                            if collected.len() >= max_rows {
                                return Err(SqlError::Bounds(format!(
                                    "SELECT result exceeds max_select_rows ({max_rows})"
                                )));
                            }
                        }
                        if collected.len() >= limits.max_sort_rows {
                            return Err(SqlError::Bounds(format!(
                                "sort row count exceeds max_sort_rows ({})",
                                limits.max_sort_rows
                            )));
                        }
                        let keys = decode_order_keys(&obj, table, order_specs, params)?;
                        collected.push(SortRow {
                            keys,
                            doc_id,
                            verified_pk,
                            obj,
                        });
                        Ok(ControlFlow::Continue(()))
                    },
                )?;
                collected.sort_unstable_by(|a, b| compare_sort_rows(a, b, order_specs));
                for entry in collected.into_iter().skip(offset) {
                    let row =
                        project_simple_row(entry.obj, table, outputs, params, entry.verified_pk)?;
                    if matches!(sink.push_row(row)?, ControlFlow::Break(())) {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ScalarKey {
    Null,
    Integer(i64),
    Real(u64),
    Text(String),
    Boolean(bool),
}

fn canonical_f64_bits(f: f64) -> u64 {
    if f == 0.0 {
        0.0_f64.to_bits()
    } else {
        f.to_bits()
    }
}

impl ScalarKey {
    pub(super) fn from_value_ref(v: SqlValueRef<'_>) -> Self {
        match v {
            SqlValueRef::Null => Self::Null,
            SqlValueRef::Integer(n) => Self::Integer(n),
            SqlValueRef::Real(f) => Self::Real(canonical_f64_bits(f)),
            SqlValueRef::Text(s) => Self::Text(s.to_owned()),
            SqlValueRef::Boolean(b) => Self::Boolean(b),
        }
    }

    fn as_value_ref(&self) -> SqlValueRef<'_> {
        match self {
            Self::Null => SqlValueRef::Null,
            Self::Integer(n) => SqlValueRef::Integer(*n),
            Self::Real(bits) => SqlValueRef::Real(f64::from_bits(*bits)),
            Self::Text(s) => SqlValueRef::Text(s.as_str()),
            Self::Boolean(b) => SqlValueRef::Boolean(*b),
        }
    }

    fn into_sql_value(self) -> SqlValue {
        match self {
            Self::Null => SqlValue::Null,
            Self::Integer(n) => SqlValue::Integer(n),
            Self::Real(bits) => SqlValue::Real(f64::from_bits(bits)),
            Self::Text(s) => SqlValue::Text(s),
            Self::Boolean(b) => SqlValue::Boolean(b),
        }
    }

    fn cmp_asc_nulls_first(&self, other: &Self) -> Ordering {
        compare_order_cell_ref(self.as_value_ref(), other.as_value_ref(), false, true)
    }
}

fn hash_value_ref(v: SqlValueRef<'_>, hasher: &mut impl Hasher) {
    match v {
        SqlValueRef::Null => 0_u8.hash(hasher),
        SqlValueRef::Integer(n) => {
            1_u8.hash(hasher);
            n.hash(hasher);
        }
        SqlValueRef::Real(f) => {
            2_u8.hash(hasher);
            canonical_f64_bits(f).hash(hasher);
        }
        SqlValueRef::Text(s) => {
            3_u8.hash(hasher);
            s.hash(hasher);
        }
        SqlValueRef::Boolean(b) => {
            4_u8.hash(hasher);
            b.hash(hasher);
        }
    }
}

fn value_ref_eq_scalar(v: SqlValueRef<'_>, k: &ScalarKey) -> bool {
    match (v, k) {
        (SqlValueRef::Null, ScalarKey::Null) => true,
        (SqlValueRef::Integer(a), ScalarKey::Integer(b)) => a == *b,
        (SqlValueRef::Real(a), ScalarKey::Real(b)) => canonical_f64_bits(a) == *b,
        (SqlValueRef::Text(a), ScalarKey::Text(b)) => a == b.as_str(),
        (SqlValueRef::Boolean(a), ScalarKey::Boolean(b)) => a == *b,
        _ => false,
    }
}

fn compare_value_ref_to_scalar(v: SqlValueRef<'_>, k: &ScalarKey) -> Ordering {
    compare_non_null(v, k.as_value_ref()).expect("aggregate column has uniform type")
}

pub(crate) fn compare_order_cell_ref(
    a: SqlValueRef<'_>,
    b: SqlValueRef<'_>,
    desc: bool,
    nulls_first: bool,
) -> Ordering {
    match (a, b) {
        (SqlValueRef::Null, SqlValueRef::Null) => Ordering::Equal,
        (SqlValueRef::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, SqlValueRef::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {
            let ord = compare_non_null(a, b).expect("ORDER BY values have matching column types");
            if desc { ord.reverse() } else { ord }
        }
    }
}

pub(crate) struct DistinctSet {
    values: Vec<ScalarKey>,
    buckets: HashMap<u64, Vec<usize>>,
}

impl DistinctSet {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            buckets: HashMap::new(),
        }
    }

    fn insert(
        &mut self,
        val: SqlValueRef<'_>,
        hash_builder: &RandomState,
        total_distinct: &mut usize,
        max_distinct: usize,
    ) -> Result<(), SqlError> {
        let mut hasher = hash_builder.build_hasher();
        hash_value_ref(val, &mut hasher);
        let h = hasher.finish();
        if let Some(bucket) = self.buckets.get(&h) {
            if bucket
                .iter()
                .any(|&idx| value_ref_eq_scalar(val, &self.values[idx]))
            {
                return Ok(());
            }
        }
        if *total_distinct >= max_distinct {
            return Err(SqlError::Bounds(format!(
                "COUNT(DISTINCT) distinct value count exceeds max_distinct_values ({max_distinct})"
            )));
        }
        *total_distinct += 1;
        let idx = self.values.len();
        self.values.push(ScalarKey::from_value_ref(val));
        self.buckets.entry(h).or_default().push(idx);
        Ok(())
    }
}

pub(crate) enum AggAcc {
    Count(u64),
    CountDistinct(DistinctSet),
    SumInt { sum: i128, has_value: bool },
    SumReal { sum: f64, has_value: bool },
    AvgInt { sum: i128, count: u64 },
    AvgReal { sum: f64, count: u64 },
    Min(Option<ScalarKey>),
    Max(Option<ScalarKey>),
}

impl AggAcc {
    pub(crate) fn new(spec: &BoundAggregate) -> Self {
        match (spec.kind, spec.col_type) {
            (AggregateKind::CountStar | AggregateKind::Count, _) => Self::Count(0),
            (AggregateKind::CountDistinct, _) => Self::CountDistinct(DistinctSet::new()),
            (AggregateKind::Sum, Some(SqlType::Integer)) => Self::SumInt {
                sum: 0,
                has_value: false,
            },
            (AggregateKind::Sum, Some(SqlType::Real)) => Self::SumReal {
                sum: 0.0,
                has_value: false,
            },
            (AggregateKind::Avg, Some(SqlType::Integer)) => Self::AvgInt { sum: 0, count: 0 },
            (AggregateKind::Avg, Some(SqlType::Real)) => Self::AvgReal { sum: 0.0, count: 0 },
            (AggregateKind::Min, _) => Self::Min(None),
            (AggregateKind::Max, _) => Self::Max(None),
            _ => unreachable!("validated aggregate type"),
        }
    }

    pub(crate) fn update(
        &mut self,
        val: Option<SqlValueRef<'_>>,
        hash_builder: &RandomState,
        total_distinct: &mut usize,
        max_distinct: usize,
    ) -> Result<(), SqlError> {
        let Some(v) = val else {
            if let Self::Count(c) = self {
                *c += 1;
            }
            return Ok(());
        };
        if matches!(v, SqlValueRef::Null) {
            return Ok(());
        }
        match self {
            Self::Count(c) => *c += 1,
            Self::CountDistinct(set) => {
                set.insert(v, hash_builder, total_distinct, max_distinct)?;
            }
            Self::SumInt { sum, has_value } => {
                let SqlValueRef::Integer(n) = v else {
                    unreachable!("checked integer column")
                };
                *sum += i128::from(n);
                *has_value = true;
            }
            Self::SumReal { sum, has_value } => {
                let SqlValueRef::Real(f) = v else {
                    unreachable!("checked real column")
                };
                *sum += f;
                *has_value = true;
            }
            Self::AvgInt { sum, count } => {
                let SqlValueRef::Integer(n) = v else {
                    unreachable!("checked integer column")
                };
                *sum += i128::from(n);
                *count += 1;
            }
            Self::AvgReal { sum, count } => {
                let SqlValueRef::Real(f) = v else {
                    unreachable!("checked real column")
                };
                *sum += f;
                *count += 1;
            }
            Self::Min(cur) => match cur {
                None => *cur = Some(ScalarKey::from_value_ref(v)),
                Some(existing) => {
                    if compare_value_ref_to_scalar(v, existing) == Ordering::Less {
                        *cur = Some(ScalarKey::from_value_ref(v));
                    }
                }
            },
            Self::Max(cur) => match cur {
                None => *cur = Some(ScalarKey::from_value_ref(v)),
                Some(existing) => {
                    if compare_value_ref_to_scalar(v, existing) == Ordering::Greater {
                        *cur = Some(ScalarKey::from_value_ref(v));
                    }
                }
            },
        }
        Ok(())
    }

    pub(crate) fn finalize(self) -> Result<SqlValue, SqlError> {
        match self {
            Self::Count(c) => Ok(SqlValue::Integer(c as i64)),
            Self::CountDistinct(set) => Ok(SqlValue::Integer(set.values.len() as i64)),
            Self::SumInt { sum, has_value } => {
                if !has_value {
                    Ok(SqlValue::Null)
                } else {
                    i64::try_from(sum).map(SqlValue::Integer).map_err(|_| {
                        SqlError::Bounds(
                            "SUM(INTEGER) overflow: sum exceeds signed 64-bit integer range".into(),
                        )
                    })
                }
            }
            Self::SumReal { sum, has_value } => {
                if !has_value {
                    Ok(SqlValue::Null)
                } else if !sum.is_finite() {
                    Err(SqlError::Bounds(
                        "SUM(REAL) overflow: sum is not finite".into(),
                    ))
                } else {
                    Ok(SqlValue::Real(if sum == 0.0 { 0.0 } else { sum }))
                }
            }
            Self::AvgInt { sum, count } => {
                if count == 0 {
                    Ok(SqlValue::Null)
                } else {
                    let avg = (sum as f64) / (count as f64);
                    Ok(SqlValue::Real(if avg == 0.0 { 0.0 } else { avg }))
                }
            }
            Self::AvgReal { sum, count } => {
                if count == 0 {
                    Ok(SqlValue::Null)
                } else {
                    let avg = sum / (count as f64);
                    if !avg.is_finite() {
                        Err(SqlError::Bounds(
                            "AVG(REAL) overflow: result is not finite".into(),
                        ))
                    } else {
                        Ok(SqlValue::Real(if avg == 0.0 { 0.0 } else { avg }))
                    }
                }
            }
            Self::Min(val) | Self::Max(val) => {
                Ok(val.map_or(SqlValue::Null, ScalarKey::into_sql_value))
            }
        }
    }
}

struct GroupEntry {
    keys: Vec<ScalarKey>,
    accs: Vec<AggAcc>,
}

struct GroupTable {
    entries: Vec<GroupEntry>,
    buckets: HashMap<u64, Vec<usize>>,
    hash_builder: RandomState,
    max_groups: usize,
}

impl GroupTable {
    fn new(max_groups: usize, hash_builder: RandomState) -> Self {
        Self {
            entries: Vec::new(),
            buckets: HashMap::new(),
            hash_builder,
            max_groups,
        }
    }

    fn get_or_insert(
        &mut self,
        row_keys: &[SqlValueRef<'_>],
        agg_specs: &[BoundAggregate],
    ) -> Result<&mut Vec<AggAcc>, SqlError> {
        let mut hasher = self.hash_builder.build_hasher();
        for &v in row_keys {
            hash_value_ref(v, &mut hasher);
        }
        let h = hasher.finish();
        if let Some(bucket) = self.buckets.get(&h) {
            for &idx in bucket {
                if self.entries[idx]
                    .keys
                    .iter()
                    .zip(row_keys.iter())
                    .all(|(k, &v)| value_ref_eq_scalar(v, k))
                {
                    return Ok(&mut self.entries[idx].accs);
                }
            }
        }
        if self.entries.len() >= self.max_groups {
            return Err(SqlError::Bounds(format!(
                "group count exceeds max_groups ({})",
                self.max_groups
            )));
        }
        let idx = self.entries.len();
        let keys = row_keys
            .iter()
            .map(|&v| ScalarKey::from_value_ref(v))
            .collect();
        let accs = agg_specs.iter().map(AggAcc::new).collect();
        self.entries.push(GroupEntry { keys, accs });
        self.buckets.entry(h).or_default().push(idx);
        Ok(&mut self.entries[idx].accs)
    }
}

struct FinalGroup {
    keys: Vec<ScalarKey>,
    aggs: Vec<SqlValue>,
    preprojected: Vec<SqlValue>,
}

struct HeapGroupRow {
    group: FinalGroup,
    specs: Rc<[BoundGroupOrderBy]>,
}

impl PartialEq for HeapGroupRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapGroupRow {}

impl PartialOrd for HeapGroupRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapGroupRow {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_final_groups(&self.group, &other.group, &self.specs)
    }
}

fn group_order_val(group: &FinalGroup, target: BoundGroupOrderTarget) -> SqlValueRef<'_> {
    match target {
        BoundGroupOrderTarget::GroupCol(pos) => group.keys[pos].as_value_ref(),
        BoundGroupOrderTarget::Aggregate(idx) => SqlValueRef::from_value(&group.aggs[idx]),
        BoundGroupOrderTarget::Output(pos) => SqlValueRef::from_value(&group.preprojected[pos]),
    }
}

fn compare_group_keys_asc(a: &[ScalarKey], b: &[ScalarKey]) -> Ordering {
    for (ak, bk) in a.iter().zip(b.iter()) {
        let ord = ak.cmp_asc_nulls_first(bk);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn compare_final_groups(a: &FinalGroup, b: &FinalGroup, specs: &[BoundGroupOrderBy]) -> Ordering {
    for spec in specs {
        let av = group_order_val(a, spec.target);
        let bv = group_order_val(b, spec.target);
        let ord = compare_order_cell_ref(av, bv, spec.desc, spec.nulls_first);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    compare_group_keys_asc(&a.keys, &b.keys)
}

fn decode_col_with_pk<'a>(
    obj: RowRef<'a>,
    table: &TableDef,
    col_idx: usize,
    verified_pk: Option<i64>,
) -> Result<SqlValueRef<'a>, SqlError> {
    if col_idx == table.primary_key_index {
        if let Some(n) = verified_pk {
            return Ok(SqlValueRef::Integer(n));
        }
    }
    obj.col(&table.columns[col_idx])
}

struct DistinctBudget<'a> {
    hash_builder: &'a RandomState,
    total_distinct: usize,
    max_distinct: usize,
}

fn update_group_accs(
    accs: &mut [AggAcc],
    agg_specs: &[BoundAggregate],
    obj: RowRef<'_>,
    table: &TableDef,
    verified_pk: Option<i64>,
    distinct: &mut DistinctBudget<'_>,
) -> Result<(), SqlError> {
    for (acc, spec) in accs.iter_mut().zip(agg_specs.iter()) {
        let val = match spec.col_idx {
            None => None,
            Some(col_idx) => Some(decode_col_with_pk(obj, table, col_idx, verified_pk)?),
        };
        acc.update(
            val,
            distinct.hash_builder,
            &mut distinct.total_distinct,
            distinct.max_distinct,
        )?;
    }
    Ok(())
}

fn eval_aggregate_outputs(
    outputs: &[BoundSelectOutput],
    keys: &[ScalarKey],
    aggs: &[SqlValue],
    params: &[SqlValue],
) -> Result<Vec<SqlValue>, SqlError> {
    let mut row = Vec::with_capacity(outputs.len());
    for output in outputs {
        let val = match output {
            BoundSelectOutput::GroupCol(pos) => keys[*pos].clone().into_sql_value(),
            BoundSelectOutput::Aggregate(idx) => aggs[*idx].clone(),
            BoundSelectOutput::Expr(expr) => {
                eval_expr_ref(expr, EvalContext::Group { keys, aggs }, params)?.into_owned()
            }
        };
        row.push(val);
    }
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
fn exec_aggregate_select_into(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &BoundAggregatePlan,
    stmt: &SelectStmt,
    params: &[SqlValue],
    subquery_values: &[SubqueryValue],
    limits: &SqlLimits,
    force_full_scan: bool,
    sink: &mut SelectSink<'_>,
) -> Result<(), SqlError> {
    let limit = resolve_limit_value(stmt.limit, params, "LIMIT")?;
    let offset = resolve_limit_value(stmt.offset, params, "OFFSET")?.unwrap_or(0);

    if limit == Some(0) {
        return Ok(());
    }

    let resolved_outputs: Vec<BoundSelectOutput>;
    let outputs: &[BoundSelectOutput] = if subquery_values.is_empty() {
        &plan.outputs
    } else {
        resolved_outputs = plan
            .outputs
            .iter()
            .map(|o| match o {
                BoundSelectOutput::GroupCol(g) => BoundSelectOutput::GroupCol(*g),
                BoundSelectOutput::Aggregate(a) => BoundSelectOutput::Aggregate(*a),
                BoundSelectOutput::Expr(expr) => {
                    BoundSelectOutput::Expr(subquery::resolve_bound_expr(expr, subquery_values))
                }
            })
            .collect();
        &resolved_outputs
    };

    let resolved_filter: Option<BoundExpr>;
    let bound_filter = if subquery_values.is_empty() {
        plan.filter.as_ref().map(|f| &f.expr)
    } else {
        resolved_filter = plan
            .filter
            .as_ref()
            .map(|f| subquery::resolve_bound_expr(&f.expr, subquery_values));
        resolved_filter.as_ref()
    };

    let resolved_having: Option<BoundExpr>;
    let having_expr = if subquery_values.is_empty() {
        plan.having.as_ref().map(|f| &f.expr)
    } else {
        resolved_having = plan
            .having
            .as_ref()
            .map(|f| subquery::resolve_bound_expr(&f.expr, subquery_values));
        resolved_having.as_ref()
    };

    // Ungrouped COUNT(*) over the whole table counts document keys without
    // reading rows, under the same visit bound a scan would have.
    if !force_full_scan
        && bound_filter.is_none()
        && having_expr.is_none()
        && plan.group_cols.is_empty()
        && !plan.aggregates.is_empty()
        && plan
            .aggregates
            .iter()
            .all(|aggregate| matches!(aggregate.kind, AggregateKind::CountStar))
    {
        let count = tx
            .count_documents(&table.name, limits.max_visited_rows)
            .map_err(classify_io_error)?;
        if count > limits.max_visited_rows {
            return Err(SqlError::Bounds(format!(
                "visited row count exceeds max_visited_rows ({})",
                limits.max_visited_rows
            )));
        }
        subquery::record_visited_rows(count);
        if offset == 0 {
            let count_val = SqlValue::Integer(count as i64);
            let aggs = vec![count_val; plan.aggregates.len()];
            let row = eval_aggregate_outputs(outputs, &[], &aggs, params)?;
            let _ = sink.push_row(row)?;
        }
        return Ok(());
    }

    let mut access = if force_full_scan {
        AccessPlan::FullScan
    } else {
        plan::plan_access(bound_filter, table, &[], false, params)
    };

    // Provably equivalent MIN(pk) or MIN(ready_index_leading_col) first-entry optimization.
    let mut stop_on_first_non_null_min = false;
    let mut stream_cap: Option<usize> = None;
    if !force_full_scan {
        if let Some(min_col_idx) = plan.ungrouped_min_col {
            if table.has_scalar_pk() && min_col_idx == table.primary_key_index {
                let pk_ordered = matches!(
                    access,
                    AccessPlan::PkPoint { .. }
                        | AccessPlan::PkRange { .. }
                        | AccessPlan::FullScan
                        | AccessPlan::Index {
                            preserves_pk_asc: true,
                            ..
                        }
                );
                if pk_ordered {
                    stop_on_first_non_null_min = true;
                    if bound_filter.is_none() {
                        stream_cap = Some(1);
                    }
                }
            } else if bound_filter.is_none() {
                if let Some((index_idx, _)) = table
                    .indexes
                    .iter()
                    .enumerate()
                    .filter(|(_, idx)| {
                        idx.state == IndexState::Ready
                            && idx.columns.first() == Some(&min_col_idx)
                            && idx.columns[1..].iter().all(|&c| table.columns[c].not_null)
                    })
                    .min_by_key(|(_, idx)| idx.columns.len())
                {
                    access = AccessPlan::Index {
                        index_idx,
                        equal: Vec::new(),
                        lower: None,
                        upper: None,
                        unique_equality: false,
                        preserves_pk_asc: false,
                        satisfies_order_by: false,
                    };
                    stop_on_first_non_null_min = true;
                    stream_cap = Some(1);
                }
            } else if let AccessPlan::Index {
                index_idx,
                ref equal,
                ..
            } = access
            {
                if table.indexes[index_idx].columns.get(equal.len()) == Some(&min_col_idx) {
                    stop_on_first_non_null_min = true;
                }
            }
        }
    }

    let hash_builder = RandomState::new();
    let mut distinct = DistinctBudget {
        hash_builder: &hash_builder,
        total_distinct: 0,
        max_distinct: limits.max_distinct_values,
    };

    let raw_groups: Vec<GroupEntry> = if plan.group_cols.is_empty() {
        let mut accs: Vec<AggAcc> = plan.aggregates.iter().map(AggAcc::new).collect();
        if !matches!(access, AccessPlan::Empty) {
            visit_access_rows_ref(
                tx,
                table,
                access,
                bound_filter,
                params,
                limits,
                stream_cap,
                |_doc_id, verified_pk, obj| {
                    update_group_accs(
                        &mut accs,
                        &plan.aggregates,
                        obj,
                        table,
                        verified_pk,
                        &mut distinct,
                    )?;
                    if stop_on_first_non_null_min && matches!(&accs[0], AggAcc::Min(Some(_))) {
                        Ok(ControlFlow::Break(()))
                    } else {
                        Ok(ControlFlow::Continue(()))
                    }
                },
            )?;
        }
        vec![GroupEntry {
            keys: Vec::new(),
            accs,
        }]
    } else {
        let mut table_groups = GroupTable::new(limits.max_groups, hash_builder.clone());
        if !matches!(access, AccessPlan::Empty) {
            visit_access_rows_ref(
                tx,
                table,
                access,
                bound_filter,
                params,
                limits,
                None,
                |_doc_id, verified_pk, obj| {
                    let accs = if plan.group_cols.len() == 1 {
                        let key = decode_col_with_pk(obj, table, plan.group_cols[0], verified_pk)?;
                        table_groups.get_or_insert(&[key], &plan.aggregates)?
                    } else {
                        let mut keys = Vec::with_capacity(plan.group_cols.len());
                        for &col_idx in &plan.group_cols {
                            keys.push(decode_col_with_pk(obj, table, col_idx, verified_pk)?);
                        }
                        table_groups.get_or_insert(&keys, &plan.aggregates)?
                    };
                    update_group_accs(
                        accs,
                        &plan.aggregates,
                        obj,
                        table,
                        verified_pk,
                        &mut distinct,
                    )?;
                    Ok(ControlFlow::Continue(()))
                },
            )?;
        }
        table_groups.entries
    };

    let need_preprojected = plan
        .order_by
        .iter()
        .any(|o| matches!(o.target, BoundGroupOrderTarget::Output(_)));
    let mut surviving = Vec::new();
    for entry in raw_groups {
        let mut aggs = Vec::with_capacity(entry.accs.len());
        for acc in entry.accs {
            aggs.push(acc.finalize()?);
        }
        if eval_having_doc(having_expr, &entry.keys, &aggs, params)? {
            let preprojected = if need_preprojected {
                eval_aggregate_outputs(outputs, &entry.keys, &aggs, params)?
            } else {
                Vec::new()
            };
            surviving.push(FinalGroup {
                keys: entry.keys,
                aggs,
                preprojected,
            });
        }
    }

    let ordered_groups: Vec<FinalGroup> = if surviving.is_empty() {
        Vec::new()
    } else if plan.order_by.is_empty() {
        if !plan.group_cols.is_empty() && surviving.len() > 1 {
            surviving.sort_unstable_by(|a, b| compare_group_keys_asc(&a.keys, &b.keys));
        }
        let result_len = surviving
            .len()
            .saturating_sub(offset)
            .min(limit.unwrap_or(usize::MAX));
        if let Some(max_rows) = sink.max_output_rows() {
            if result_len > max_rows {
                return Err(SqlError::Bounds(format!(
                    "SELECT result exceeds max_select_rows ({max_rows})"
                )));
            }
        }
        surviving
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    } else if let Some(lim) = limit {
        let keep_cap = (offset as u128).saturating_add(lim as u128);
        let specs: Rc<[BoundGroupOrderBy]> = Rc::from(plan.order_by.as_slice());
        let mut heap: BinaryHeap<HeapGroupRow> = BinaryHeap::new();
        for group in surviving {
            if (heap.len() as u128) < keep_cap {
                if heap.len() >= limits.max_sort_rows {
                    return Err(SqlError::Bounds(format!(
                        "sort row count exceeds max_sort_rows ({})",
                        limits.max_sort_rows
                    )));
                }
                if offset == 0 {
                    if let Some(max_rows) = sink.max_output_rows() {
                        if heap.len() >= max_rows {
                            return Err(SqlError::Bounds(format!(
                                "SELECT result exceeds max_select_rows ({max_rows})"
                            )));
                        }
                    }
                }
                heap.push(HeapGroupRow {
                    group,
                    specs: Rc::clone(&specs),
                });
            } else if let Some(mut top) = heap.peek_mut() {
                if compare_final_groups(&group, &top.group, &specs) == Ordering::Less {
                    top.group = group;
                }
            }
        }
        if heap.len() <= offset {
            Vec::new()
        } else {
            let result_len = heap.len() - offset;
            if let Some(max_rows) = sink.max_output_rows() {
                if result_len > max_rows {
                    return Err(SqlError::Bounds(format!(
                        "SELECT result exceeds max_select_rows ({max_rows})"
                    )));
                }
            }
            heap.into_sorted_vec()
                .into_iter()
                .skip(offset)
                .map(|h| h.group)
                .collect()
        }
    } else {
        if surviving.len() > limits.max_sort_rows {
            return Err(SqlError::Bounds(format!(
                "sort row count exceeds max_sort_rows ({})",
                limits.max_sort_rows
            )));
        }
        let result_len = surviving.len().saturating_sub(offset);
        if let Some(max_rows) = sink.max_output_rows() {
            if result_len > max_rows {
                return Err(SqlError::Bounds(format!(
                    "SELECT result exceeds max_select_rows ({max_rows})"
                )));
            }
        }
        surviving.sort_unstable_by(|a, b| compare_final_groups(a, b, &plan.order_by));
        surviving.into_iter().skip(offset).collect()
    };

    for group in ordered_groups {
        let row = if need_preprojected {
            group.preprojected
        } else {
            eval_aggregate_outputs(outputs, &group.keys, &group.aggs, params)?
        };
        if matches!(sink.push_row(row)?, ControlFlow::Break(())) {
            break;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn exec_streaming_select(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    _plan: &BoundSelectPlan,
    outputs: &[SimpleSelectOutput],
    access: AccessPlan,
    bound_filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    limit: Option<usize>,
    offset: usize,
    sink: &mut SelectSink<'_>,
) -> Result<(), SqlError> {
    let mut skipped = 0usize;
    let mut pushed = 0usize;
    let stream_cap = if bound_filter.is_none() {
        if sink.is_exists() {
            Some(offset.saturating_add(1))
        } else {
            limit.map(|l| offset.saturating_add(l))
        }
    } else {
        None
    };
    visit_access_rows_ref(
        tx,
        table,
        access,
        bound_filter,
        params,
        limits,
        stream_cap,
        |_doc_id, verified_pk, row| {
            if skipped < offset {
                skipped += 1;
                return Ok(ControlFlow::Continue(()));
            }
            let row = project_simple_row_ref(row, table, outputs, params, verified_pk)?;
            pushed += 1;
            if matches!(sink.push_row(row)?, ControlFlow::Break(())) {
                return Ok(ControlFlow::Break(()));
            }
            if Some(pushed) == limit {
                Ok(ControlFlow::Break(()))
            } else {
                Ok(ControlFlow::Continue(()))
            }
        },
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn exec_pk_sorted_select(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &BoundSelectPlan,
    outputs: &[SimpleSelectOutput],
    access: AccessPlan,
    bound_filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    limit: Option<usize>,
    offset: usize,
    desc: bool,
    sink: &mut SelectSink<'_>,
) -> Result<(), SqlError> {
    if let Some(lim) = limit {
        let keep_cap = (offset as u128).saturating_add(lim as u128);
        let mut heap: BinaryHeap<PkSortRow> = BinaryHeap::new();
        let wanted = &plan.wanted_columns;
        // Rows are kept as owned objects holding only the needed columns.
        let to_owned_map = |row: RowRef<'_>| match row {
            RowRef::Flat(doc) => {
                doc.to_map_where(|name| table.column_index(name).is_some_and(|index| wanted[index]))
            }
            RowRef::Map(obj) => obj.clone(),
        };
        visit_access_rows_ref(
            tx,
            table,
            access,
            bound_filter,
            params,
            limits,
            None,
            |doc_id, verified_pk, row| {
                if (heap.len() as u128) < keep_cap {
                    if heap.len() >= limits.max_sort_rows {
                        return Err(SqlError::Bounds(format!(
                            "sort row count exceeds max_sort_rows ({})",
                            limits.max_sort_rows
                        )));
                    }
                    if offset == 0 {
                        if let Some(max_rows) = sink.max_output_rows() {
                            if heap.len() >= max_rows {
                                return Err(SqlError::Bounds(format!(
                                    "SELECT result exceeds max_select_rows ({max_rows})"
                                )));
                            }
                        }
                    }
                    heap.push(PkSortRow {
                        doc_id: doc_id.to_owned(),
                        verified_pk,
                        obj: to_owned_map(row),
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
                            doc_id: doc_id.to_owned(),
                            verified_pk,
                            obj: to_owned_map(row),
                            desc,
                        };
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if heap.len() <= offset {
            return Ok(());
        }
        let result_len = heap.len() - offset;
        if let Some(max_rows) = sink.max_output_rows() {
            if result_len > max_rows {
                return Err(SqlError::Bounds(format!(
                    "SELECT result exceeds max_select_rows ({max_rows})"
                )));
            }
        }
        let sorted = heap.into_sorted_vec();
        for entry in sorted.into_iter().skip(offset) {
            let row = project_simple_row(entry.obj, table, outputs, params, entry.verified_pk)?;
            if matches!(sink.push_row(row)?, ControlFlow::Break(())) {
                break;
            }
        }
        Ok(())
    } else {
        let mut first: Option<(String, Vec<SqlValue>)> = None;
        let mut rest: Vec<(String, Vec<SqlValue>)> = Vec::new();
        visit_access_rows_ref(
            tx,
            table,
            access,
            bound_filter,
            params,
            limits,
            None,
            |doc_id, verified_pk, obj| {
                let count = usize::from(first.is_some()) + rest.len();
                if let Some(max_rows) = sink.max_output_rows() {
                    if count >= max_rows {
                        return Err(SqlError::Bounds(format!(
                            "SELECT result exceeds max_select_rows ({max_rows})"
                        )));
                    }
                }
                if count >= limits.max_sort_rows {
                    return Err(SqlError::Bounds(format!(
                        "sort row count exceeds max_sort_rows ({})",
                        limits.max_sort_rows
                    )));
                }
                let row = project_simple_row_ref(obj, table, outputs, params, verified_pk)?;
                if first.is_none() && rest.is_empty() {
                    first = Some((doc_id.to_owned(), row));
                } else {
                    if let Some(first_pair) = first.take() {
                        rest.push(first_pair);
                    }
                    rest.push((doc_id.to_owned(), row));
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if rest.is_empty() {
            if offset == 0 {
                if let Some((_, row)) = first {
                    let _ = sink.push_row(row)?;
                }
            }
            return Ok(());
        }
        rest.sort_unstable_by(|a, b| {
            let ord = a.0.as_bytes().cmp(b.0.as_bytes());
            if desc { ord.reverse() } else { ord }
        });
        for (_, row) in rest.into_iter().skip(offset) {
            if matches!(sink.push_row(row)?, ControlFlow::Break(())) {
                break;
            }
        }
        Ok(())
    }
}

fn get_or_build_update_plan(
    stmt_id: u64,
    stmt: &UpdateStmt,
    cached_table: &CachedTable,
    cache: &SqlCache,
    limits: &SqlLimits,
) -> Result<Arc<BoundUpdatePlan>, SqlError> {
    let key = (stmt_id, cached_table.generation);
    if let Some(plan) = cache.update_plans.read().get(&key) {
        return Ok(Arc::clone(plan));
    }
    let table = &cached_table.def;
    let mut assignments = Vec::with_capacity(stmt.assignments.len());
    let mut assign_param_checks = Vec::new();
    let mut param_constraints = Vec::new();
    let mut subqueries = Vec::new();
    let mut subquery_checks = Vec::new();
    let mut sub_ctx = SubqueryBindCtx {
        outer_scopes: &[],
        slots: &mut subqueries,
        checks: &mut subquery_checks,
        context_label: "SET",
    };
    for Assignment { column, value, .. } in &stmt.assignments {
        let idx = table
            .column_index(column)
            .ok_or_else(|| SqlError::NoSuchColumn {
                table: table.name.clone(),
                column: column.clone(),
            })?;
        let col = &table.columns[idx];
        let (bound, ty) = bind_expr_static_with(
            value,
            Some(table),
            &table.name,
            &mut param_constraints,
            &mut sub_ctx,
            None,
        )?;
        match ty {
            StaticExprType::Null => {}
            StaticExprType::Typed(actual_ty) => {
                check_assignment_type(&bound, actual_ty, col)?;
            }
            StaticExprType::Param(p) => {
                assign_param_checks.push((p, col.col_type));
            }
            StaticExprType::Subquery(sub_id) => {
                sub_ctx.checks.push(SubqueryTypeCheck::AssignToColumn {
                    sub_id,
                    col_name: col.name.clone(),
                    col_type: col.col_type,
                });
            }
        }
        assignments.push((idx, bound));
    }
    sub_ctx.context_label = "WHERE";
    let mut filter = bind_where_static(stmt.filter.as_ref(), table, &mut sub_ctx)?;
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
        subqueries,
        subquery_checks,
        checks: bind_table_checks(table, limits)?,
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
    let plan = get_or_build_update_plan(stmt_id, stmt, &cached_table, cache, limits)?;
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

    let mut budget = SubqueryBudget::new(limits);
    let sub_values = if plan.subqueries.is_empty() {
        Vec::new()
    } else {
        let prepared = subquery::bind_subqueries(
            tx,
            &plan.subqueries,
            &plan.subquery_checks,
            &[table.as_ref()],
            params,
            limits,
            cache,
        )?;
        subquery::execute_subqueries(
            tx,
            &prepared,
            &plan.subquery_checks,
            params,
            limits,
            cache,
            &mut budget,
            force_full_scan,
        )?
    };
    let visit_limits = budget.sub_limits(limits);

    let resolved_assignments: Vec<(usize, BoundExpr, bool)>;
    let resolved_filter: Option<BoundExpr>;
    let assignments_ref: Vec<(usize, &BoundExpr, bool)>;
    let bound_filter: Option<&BoundExpr>;
    if sub_values.is_empty() {
        assignments_ref = plan
            .assignments
            .iter()
            .map(|(col_idx, expr)| (*col_idx, expr, expr.is_int_literal_or_param()))
            .collect();
        bound_filter = plan.filter.as_ref().map(|f| &f.expr);
    } else {
        resolved_assignments = plan
            .assignments
            .iter()
            .map(|(col_idx, expr)| {
                (
                    *col_idx,
                    subquery::resolve_bound_expr(expr, &sub_values),
                    expr.is_int_literal_or_param(),
                )
            })
            .collect();
        assignments_ref = resolved_assignments
            .iter()
            .map(|(col_idx, expr, allow)| (*col_idx, expr, *allow))
            .collect();
        resolved_filter = plan
            .filter
            .as_ref()
            .map(|f| subquery::resolve_bound_expr(&f.expr, &sub_values));
        bound_filter = resolved_filter.as_ref();
    }

    let assigned_cols: Vec<usize> = plan
        .assignments
        .iter()
        .map(|(col_idx, _)| *col_idx)
        .collect();
    let mut fk_checks = super::foreign_key::PendingUpdateFkChecks::new(table, &assigned_cols);
    let assigns_pk = table
        .primary_key_columns()
        .any(|key| assigned_cols.contains(&key));
    let mut matched = Vec::new();
    let visit_res = visit_matching_rows(
        tx,
        table,
        bound_filter,
        params,
        &visit_limits,
        force_full_scan,
        None,
        |doc_id, _verified_pk, mut obj| {
            if matched.len() >= limits.max_mutation_rows {
                return Err(SqlError::Bounds(format!(
                    "UPDATE row count exceeds max_mutation_rows ({})",
                    limits.max_mutation_rows
                )));
            }
            let mut updates = Vec::with_capacity(assignments_ref.len());
            for &(col_idx, bound, allow_int_to_real) in &assignments_ref {
                let col = &table.columns[col_idx];
                let val = eval_expr_ref(bound, EvalContext::Doc { table, obj: &obj }, params)?
                    .into_owned();
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
            fk_checks.record_row(table, &obj, &updates)?;
            for (col, val) in updates {
                if matches!(val, SqlValue::Null) {
                    obj.remove(&col.name);
                } else {
                    let json_val = sql_value_to_json(&val, &col.name)?;
                    obj.insert(col.name.clone(), json_val);
                }
            }
            enforce_checks(&plan.checks, table, &obj)?;
            matched.push((doc_id, obj));
            Ok(ControlFlow::Continue(()))
        },
    );
    if !sub_values.is_empty() {
        visit_res.map_err(|err| subquery::remap_visited_bounds(err, limits.max_visited_rows))?;
    } else {
        visit_res?;
    }

    let updated = matched.len();
    let mut updated_ids = HashSet::new();
    for (doc_id, obj) in matched {
        let id = stage_updated_row(tx, table, doc_id, obj, assigns_pk)?;
        if fk_checks.checks_children {
            updated_ids.insert(id);
        }
    }
    fk_checks.verify(tx, table, updated_ids, limits, &cache.tables)?;
    Ok(SqlOutcome::Updated { rows: updated })
}

pub(super) fn stage_updated_row(
    tx: &mut DocumentTransaction<'_, '_>,
    table: &TableDef,
    old_doc_id: String,
    row: Map<String, Value>,
    assigns_pk: bool,
) -> Result<String, SqlError> {
    let on_error = |error| map_write_constraint_error(error, &table.name);
    if assigns_pk {
        let new_doc_id = if table.has_scalar_pk() {
            let pk = &table.columns[table.primary_key_index];
            let value = match decode_col_ref(&row, pk)? {
                SqlValueRef::Integer(n) => InsertPk::Int(n),
                SqlValueRef::Text(s) => InsertPk::Text(s.to_owned()),
                _ => return Err(SqlError::Constraint("invalid PRIMARY KEY value".into())),
            };
            let mut integer = [0; 18];
            let mut text = [0; 256];
            encode_insert_pk_stack(&value, pk.col_type, &mut integer, &mut text)?.to_owned()
        } else {
            compound_row_id(table, |index| decode_col_ref(&row, &table.columns[index]))?
        };
        if new_doc_id != old_doc_id {
            tx.delete(&table.name, &old_doc_id).map_err(on_error)?;
            tx.insert(&table.name, &new_doc_id, &Value::Object(row))
                .map_err(on_error)?;
            return Ok(new_doc_id);
        }
    }
    tx.replace(&table.name, &old_doc_id, &Value::Object(row), false)
        .map_err(on_error)?;
    Ok(old_doc_id)
}

fn get_or_build_delete_plan(
    stmt_id: u64,
    stmt: &DeleteStmt,
    cached_table: &CachedTable,
    cache: &SqlCache,
) -> Result<Arc<BoundDeletePlan>, SqlError> {
    let key = (stmt_id, cached_table.generation);
    if let Some(plan) = cache.delete_plans.read().get(&key) {
        return Ok(Arc::clone(plan));
    }
    let mut subqueries = Vec::new();
    let mut subquery_checks = Vec::new();
    let mut sub_ctx = SubqueryBindCtx {
        outer_scopes: &[],
        slots: &mut subqueries,
        checks: &mut subquery_checks,
        context_label: "WHERE",
    };
    let filter = bind_where_static(stmt.filter.as_ref(), &cached_table.def, &mut sub_ctx)?;
    let plan = Arc::new(BoundDeletePlan {
        filter,
        subqueries,
        subquery_checks,
    });
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
    validate_filter_params(plan.filter.as_ref(), params)?;

    let mut budget = SubqueryBudget::new(limits);
    let sub_values = if plan.subqueries.is_empty() {
        Vec::new()
    } else {
        let prepared = subquery::bind_subqueries(
            tx,
            &plan.subqueries,
            &plan.subquery_checks,
            &[table.as_ref()],
            params,
            limits,
            cache,
        )?;
        subquery::execute_subqueries(
            tx,
            &prepared,
            &plan.subquery_checks,
            params,
            limits,
            cache,
            &mut budget,
            force_full_scan,
        )?
    };
    let visit_limits = budget.sub_limits(limits);

    let resolved_filter: Option<BoundExpr>;
    let bound_filter = if sub_values.is_empty() {
        plan.filter.as_ref().map(|f| &f.expr)
    } else {
        resolved_filter = plan
            .filter
            .as_ref()
            .map(|f| subquery::resolve_bound_expr(&f.expr, &sub_values));
        resolved_filter.as_ref()
    };

    let mut fk_checks = super::foreign_key::PendingDeleteFkChecks::new(table)?;
    let mut to_delete = Vec::new();
    let visit_res = visit_matching_rows(
        tx,
        table,
        bound_filter,
        params,
        &visit_limits,
        force_full_scan,
        None,
        |doc_id, _verified_pk, obj| {
            if to_delete.len() >= limits.max_mutation_rows {
                return Err(SqlError::Bounds(format!(
                    "DELETE row count exceeds max_mutation_rows ({})",
                    limits.max_mutation_rows
                )));
            }
            fk_checks.record_row(table, &obj)?;
            to_delete.push(doc_id);
            Ok(ControlFlow::Continue(()))
        },
    );
    if !sub_values.is_empty() {
        visit_res.map_err(|err| subquery::remap_visited_bounds(err, limits.max_visited_rows))?;
    } else {
        visit_res?;
    }

    let deleted = to_delete.len();
    for doc_id in to_delete {
        tx.delete(&table.name, &doc_id).map_err(classify_io_error)?;
    }
    fk_checks.verify(tx, table, limits, &cache.tables)?;
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
        tx, table, plan, filter, params, limits, None, stream_cap, on_match,
    )
}

#[allow(clippy::too_many_arguments)]
fn visit_access_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: AccessPlan,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    wanted: Option<&[bool]>,
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
            let verified_pk = verify_doc_pk_id(table, &doc_id, &obj)?;
            if eval_where_doc(filter, table, &obj, params)? {
                let _ = on_match(doc_id, verified_pk, obj)?;
            }
            Ok(())
        }
        AccessPlan::PkPoints { doc_ids } => {
            let mut visited = 0usize;
            for doc_id in doc_ids {
                let Some(doc) = tx.get(&table.name, &doc_id).map_err(classify_io_error)? else {
                    continue;
                };
                visited += 1;
                if visited > limits.max_visited_rows {
                    return Err(SqlError::Bounds(format!(
                        "visited row count exceeds max_visited_rows ({})",
                        limits.max_visited_rows
                    )));
                }
                let obj = doc_into_object(doc)?;
                let verified_pk = verify_doc_pk_id(table, &doc_id, &obj)?;
                if eval_where_doc(filter, table, &obj, params)?
                    && matches!(on_match(doc_id, verified_pk, obj)?, ControlFlow::Break(()))
                {
                    break;
                }
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
            wanted,
            after_id,
            &upper,
            max_exact_rows,
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
            &table.indexes[index_idx].name,
            equal,
            lower,
            upper,
            unique_equality,
            filter,
            params,
            limits,
            wanted,
            stream_cap,
            on_match,
        ),
        AccessPlan::FullScan => scan_range_rows(
            tx,
            table,
            filter,
            params,
            limits,
            wanted,
            None,
            &UpperBound::Unbounded,
            None,
            stream_cap,
            on_match,
        ),
    }
}

/// `visit_access_rows`, passing borrowed rows: flat rows from range, index, and
/// full scans are never turned into owned objects.
#[allow(clippy::too_many_arguments)]
fn visit_access_rows_ref(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: AccessPlan,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    stream_cap: Option<usize>,
    mut on_row: impl FnMut(&str, Option<i64>, RowRef<'_>) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    match plan {
        AccessPlan::PkRange {
            after_id,
            upper,
            max_exact_rows,
        } => scan_range_rows_ref(
            tx,
            table,
            filter,
            params,
            limits,
            after_id,
            &upper,
            max_exact_rows,
            stream_cap,
            on_row,
        ),
        AccessPlan::Index {
            index_idx,
            equal,
            lower,
            upper,
            unique_equality,
            ..
        } => visit_index_rows_ref(
            tx,
            table,
            &table.indexes[index_idx].name,
            equal,
            lower,
            upper,
            unique_equality,
            filter,
            params,
            limits,
            stream_cap,
            on_row,
        ),
        AccessPlan::FullScan => scan_range_rows_ref(
            tx,
            table,
            filter,
            params,
            limits,
            None,
            &UpperBound::Unbounded,
            None,
            stream_cap,
            on_row,
        ),
        other => visit_access_rows(
            tx,
            table,
            other,
            filter,
            params,
            limits,
            None,
            stream_cap,
            |id, verified_pk, obj| on_row(&id, verified_pk, RowRef::Map(&obj)),
        ),
    }
}

#[inline]
/// `project_simple_row` for a borrowed row: columns are decoded in place,
/// and the row becomes an owned object only when an output is an
/// expression that needs one.
fn project_simple_row_ref(
    row: RowRef<'_>,
    table: &TableDef,
    outputs: &[SimpleSelectOutput],
    params: &[SqlValue],
    verified_int_pk: Option<i64>,
) -> Result<Vec<SqlValue>, SqlError> {
    let all_columns = outputs
        .iter()
        .all(|output| matches!(output, SimpleSelectOutput::Column { .. }));
    if !all_columns {
        let obj = match row {
            RowRef::Flat(doc) => doc.to_map(),
            RowRef::Map(obj) => obj.clone(),
        };
        return project_simple_row(obj, table, outputs, params, verified_int_pk);
    }
    let pk_idx = table.primary_key_index;
    let mut projected = Vec::with_capacity(outputs.len());
    for output in outputs {
        if let SimpleSelectOutput::Column { col_idx, .. } = output {
            if let Some(n) = verified_int_pk.filter(|_| *col_idx == pk_idx) {
                projected.push(SqlValue::Integer(n));
                continue;
            }
            projected.push(row.col(&table.columns[*col_idx])?.into_owned());
        }
    }
    Ok(projected)
}

#[allow(clippy::too_many_arguments)]
fn visit_index_rows_ref(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    index_name: &str,
    equal: Vec<IndexValue>,
    lower: Option<IndexBound>,
    upper: Option<IndexBound>,
    unique_equality: bool,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    stream_cap: Option<usize>,
    mut on_match: impl FnMut(&str, Option<i64>, RowRef<'_>) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    const MAX_INDEX_QUERY_DOCUMENTS: usize = 256;
    let visited = std::cell::Cell::new(0usize);
    let stream_cap = std::cell::Cell::new(stream_cap);

    tx.visit_index_stored(
        &table.name,
        index_name,
        &equal,
        lower.as_ref(),
        upper.as_ref(),
        unique_equality,
        || {
            let remaining_visit = limits.max_visited_rows.saturating_sub(visited.get());
            let mut page_limit = remaining_visit
                .saturating_add(1)
                .clamp(1, MAX_INDEX_QUERY_DOCUMENTS);
            if unique_equality {
                page_limit = page_limit.min(2);
            }
            if let Some(cap) = stream_cap.get() {
                if cap == 0 {
                    return 0;
                }
                page_limit = page_limit.min(cap);
            }
            page_limit
        },
        |id, stored| {
            let next_visited = visited.get() + 1;
            visited.set(next_visited);
            if next_visited > limits.max_visited_rows {
                return Err(SqlError::Bounds(format!(
                    "visited row count exceeds max_visited_rows ({})",
                    limits.max_visited_rows
                )));
            }
            match stored {
                Ok(doc) => {
                    let verified_pk = verify_row_key(table, id, |index| {
                        decode_flat_col(doc, &table.columns[index])
                    })?;
                    if eval_where_flat(filter, table, doc, params)? {
                        if let Some(cap) = stream_cap.get() {
                            stream_cap.set(Some(cap.saturating_sub(1)));
                        }
                        on_match(id, verified_pk, RowRef::Flat(doc))
                    } else {
                        Ok(ControlFlow::Continue(()))
                    }
                }
                Err(document) => {
                    let obj = document.as_object().ok_or_else(|| {
                        SqlError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "stored row is not a JSON object",
                        ))
                    })?;
                    let verified_pk = verify_doc_pk_id(table, id, obj)?;
                    if eval_where_doc(filter, table, obj, params)? {
                        if let Some(cap) = stream_cap.get() {
                            stream_cap.set(Some(cap.saturating_sub(1)));
                        }
                        on_match(id, verified_pk, RowRef::Map(obj))
                    } else {
                        Ok(ControlFlow::Continue(()))
                    }
                }
            }
        },
    )
    .map_err(classify_io_error)?
}

#[allow(clippy::too_many_arguments)]
fn visit_index_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    index_name: &str,
    equal: Vec<IndexValue>,
    lower: Option<IndexBound>,
    upper: Option<IndexBound>,
    unique_equality: bool,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    wanted: Option<&[bool]>,
    stream_cap: Option<usize>,
    mut on_match: impl FnMut(
        String,
        Option<i64>,
        Map<String, Value>,
    ) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    visit_index_rows_ref(
        tx,
        table,
        index_name,
        equal,
        lower,
        upper,
        unique_equality,
        filter,
        params,
        limits,
        stream_cap,
        |id, verified_pk, row| {
            let obj = match row {
                RowRef::Flat(doc) => match wanted {
                    Some(wanted) => doc.to_map_where(|name| {
                        table.column_index(name).is_some_and(|index| wanted[index])
                    }),
                    None => doc.to_map(),
                },
                RowRef::Map(obj) => obj.clone(),
            };
            on_match(id.to_owned(), verified_pk, obj)
        },
    )
}

/// Reads rows of a primary-key range in place, page by page. `on_match` runs
/// while the page is read from the store, so it must not use `tx`; callers
/// collect what they need and act after the scan.
#[allow(clippy::too_many_arguments)]
fn scan_range_rows_ref(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    mut after_id: Option<String>,
    upper: &UpperBound,
    max_exact_rows: Option<usize>,
    mut stream_cap: Option<usize>,
    mut on_match: impl FnMut(&str, Option<i64>, RowRef<'_>) -> Result<ControlFlow<()>, SqlError>,
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
        // Rows are read in place from the page; the visitor records why it
        // stopped, and an error, since it can only return whether to go on.
        let mut outcome: Result<bool, SqlError> = Ok(false);
        let mut row = |schema: &crate::CollectionSchema, id: &str, bytes: &[u8]| {
            outcome = scan_row(
                table,
                filter,
                params,
                limits,
                upper,
                &mut visited,
                &mut stream_cap,
                &mut on_match,
                (schema, id, bytes),
            );
            matches!(outcome, Ok(false))
        };
        let (count, last_id) = tx
            .visit_stored_documents(&table.name, after_id.as_deref(), page_limit, &mut row)
            .map_err(classify_io_error)?;
        if outcome? || count == 0 {
            return Ok(());
        }
        after_id = last_id;
    }
}

/// Checks and filters one stored row of a range scan and passes a match to
/// `on_match`. Returns whether the scan ends with this row.
#[allow(clippy::too_many_arguments)]
fn scan_row(
    table: &TableDef,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    upper: &UpperBound,
    visited: &mut usize,
    stream_cap: &mut Option<usize>,
    on_match: &mut impl FnMut(&str, Option<i64>, RowRef<'_>) -> Result<ControlFlow<()>, SqlError>,
    (schema, id, bytes): (&crate::CollectionSchema, &str, &[u8]),
) -> Result<bool, SqlError> {
    if upper.is_past(id) {
        return Ok(true);
    }
    *visited += 1;
    if *visited > limits.max_visited_rows {
        return Err(SqlError::Bounds(format!(
            "visited row count exceeds max_visited_rows ({})",
            limits.max_visited_rows
        )));
    }
    let at_inclusive_end = upper.is_at_inclusive(id);
    // A flat row is checked and filtered from borrowed members; only a
    // matching row is turned into an owned object.
    let decoded = DocumentTransaction::decode_stored(bytes, schema).map_err(classify_io_error)?;
    let flow = match decoded {
        StoredDocument::Flat(doc) => {
            let verified_pk = verify_row_key(table, id, |index| {
                decode_flat_col(&doc, &table.columns[index])
            })?;
            if eval_where_flat(filter, table, &doc, params)? {
                if let Some(cap) = stream_cap {
                    *cap = cap.saturating_sub(1);
                }
                Some(on_match(id, verified_pk, RowRef::Flat(&doc))?)
            } else {
                None
            }
        }
        StoredDocument::Full(document) => {
            let obj = doc_into_object(document)?;
            let verified_pk = verify_doc_pk_id(table, id, &obj)?;
            if eval_where_doc(filter, table, &obj, params)? {
                if let Some(cap) = stream_cap {
                    *cap = cap.saturating_sub(1);
                }
                Some(on_match(id, verified_pk, RowRef::Map(&obj))?)
            } else {
                None
            }
        }
    };
    Ok(flow.is_some_and(|flow| flow.is_break()) || at_inclusive_end)
}

#[allow(clippy::too_many_arguments)]
fn scan_range_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    filter: Option<&BoundExpr>,
    params: &[SqlValue],
    limits: &SqlLimits,
    wanted: Option<&[bool]>,
    after_id: Option<String>,
    upper: &UpperBound,
    max_exact_rows: Option<usize>,
    stream_cap: Option<usize>,
    mut on_match: impl FnMut(
        String,
        Option<i64>,
        Map<String, Value>,
    ) -> Result<ControlFlow<()>, SqlError>,
) -> Result<(), SqlError> {
    scan_range_rows_ref(
        tx,
        table,
        filter,
        params,
        limits,
        after_id,
        upper,
        max_exact_rows,
        stream_cap,
        |id, verified_pk, row| {
            // Only rows that matched WHERE become owned objects, holding the
            // columns the statement still needs.
            let obj = match row {
                RowRef::Flat(doc) => match wanted {
                    Some(wanted) => doc.to_map_where(|name| {
                        table.column_index(name).is_some_and(|index| wanted[index])
                    }),
                    None => doc.to_map(),
                },
                RowRef::Map(obj) => obj.clone(),
            };
            on_match(id.to_owned(), verified_pk, obj)
        },
    )
}

/// A row being read: an owned object, or the borrowed members of a flat
/// stored document. Both decode columns with the same rules and errors.
#[derive(Clone, Copy)]
enum RowRef<'r> {
    Map(&'r Map<String, Value>),
    Flat(&'r FlatDocument<'r>),
}

impl<'r> RowRef<'r> {
    fn col(self, col: &ColumnDef) -> Result<SqlValueRef<'r>, SqlError> {
        match self {
            Self::Map(obj) => decode_col_ref(obj, col),
            Self::Flat(doc) => decode_flat_col(doc, col),
        }
    }
}

/// The table's `CHECK` constraints, parsed from their stored text and bound
/// to its columns. Subqueries are not allowed in a check.
pub(crate) fn bind_table_checks(
    table: &TableDef,
    limits: &SqlLimits,
) -> Result<Arc<Vec<(String, BoundExpr)>>, SqlError> {
    let mut bound = Vec::with_capacity(table.checks.len());
    for text in &table.checks {
        let expr = super::parser::parse_check_expression(text, limits)?;
        let mut slots = Vec::new();
        let mut sub_checks = Vec::new();
        let mut sub_ctx = SubqueryBindCtx {
            outer_scopes: &[],
            slots: &mut slots,
            checks: &mut sub_checks,
            context_label: "CHECK",
        };
        let filter = bind_where_static(Some(&expr), table, &mut sub_ctx)?;
        if !slots.is_empty() {
            return Err(SqlError::Unsupported {
                feature: "subqueries in CHECK".into(),
                offset: None,
            });
        }
        if let Some(filter) = filter {
            bound.push((text.clone(), filter.expr));
        }
    }
    Ok(Arc::new(bound))
}

/// Fails when any check evaluates to FALSE for the row; TRUE and NULL pass,
/// as in SQL.
pub(crate) fn enforce_checks(
    checks: &[(String, BoundExpr)],
    table: &TableDef,
    obj: &Map<String, Value>,
) -> Result<(), SqlError> {
    for (text, expr) in checks {
        if let SqlValueRef::Boolean(false) =
            eval_expr_ref(expr, EvalContext::Doc { table, obj }, &[])?
        {
            return Err(SqlError::Constraint(format!(
                "CHECK constraint failed on table '{}': {text}",
                table.name
            )));
        }
    }
    Ok(())
}

/// Checks a column's `DEFAULT` literal against its type and constraints and
/// returns its stored form. `DEFAULT NULL` is the same as no default.
fn column_default(
    col: &super::parser::ColumnSpec,
    is_primary_key: bool,
    limits: &SqlLimits,
) -> Result<Option<Value>, SqlError> {
    let Some((literal, offset)) = &col.default else {
        return Ok(None);
    };
    if is_primary_key {
        return Err(SqlError::Unsupported {
            feature: "DEFAULT on a PRIMARY KEY column".into(),
            offset: Some(*offset),
        });
    }
    let value = match literal {
        LiteralValue::Null => SqlValue::Null,
        LiteralValue::Integer(n) => SqlValue::Integer(*n),
        LiteralValue::Real(f) => SqlValue::Real(*f),
        LiteralValue::Text(s) => SqlValue::Text(s.clone()),
        LiteralValue::Boolean(b) => SqlValue::Boolean(*b),
    };
    let coerced = coerce_for_column(
        &value,
        &col.name,
        col.col_type,
        col.not_null,
        true,
        limits.max_text_bytes,
    )?;
    match coerced {
        SqlValue::Null => Ok(None),
        other => sql_value_to_json(&other, &col.name).map(Some),
    }
}

pub(super) fn sql_value_to_json(val: &SqlValue, col_name: &str) -> Result<Value, SqlError> {
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

pub(super) fn verify_doc_pk_id(
    table: &TableDef,
    doc_id: &str,
    obj: &Map<String, Value>,
) -> Result<Option<i64>, SqlError> {
    verify_row_key(table, doc_id, |index| {
        decode_col_ref(obj, &table.columns[index])
    })
}

fn verify_row_key<'a>(
    table: &TableDef,
    id: &str,
    mut value: impl FnMut(usize) -> Result<SqlValueRef<'a>, SqlError>,
) -> Result<Option<i64>, SqlError> {
    if table.has_scalar_pk() {
        verify_pk_value(table, id, value(table.primary_key_index)?)
    } else if compound_row_id(table, value).is_ok_and(|key| key == id) {
        Ok(None)
    } else {
        Err(SqlError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "row ID does not match its primary key in table '{}'",
                table.name
            ),
        )))
    }
}

/// Ordered UTF-8 document ID for a typed SQL tuple. A text terminator sorts
/// before the hexadecimal alphabet, keeping shorter prefixes first.
pub(super) fn append_compound_key(id: &mut String, value: SqlValueRef<'_>) -> Result<(), SqlError> {
    const MAX_ID_BYTES: usize = 256;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let exceeds = || SqlError::Bounds("encoded composite PRIMARY KEY exceeds 256 bytes".into());
    match value {
        SqlValueRef::Integer(n) => {
            if id.len() + 17 > MAX_ID_BYTES {
                return Err(exceeds());
            }
            let mut buf = [0; 18];
            encode_i64_pk_bytes(n, &mut buf);
            id.push('i');
            id.push_str(std::str::from_utf8(&buf[2..]).expect("ASCII integer key"));
        }
        SqlValueRef::Text(text) => {
            if id.len().saturating_add(2 + text.len().saturating_mul(2)) > MAX_ID_BYTES {
                return Err(exceeds());
            }
            id.push('t');
            for &byte in text.as_bytes() {
                id.push(char::from(HEX[(byte >> 4) as usize]));
                id.push(char::from(HEX[(byte & 15) as usize]));
            }
            id.push('!');
        }
        _ => {
            return Err(SqlError::Constraint(
                "PRIMARY KEY requires non-NULL INTEGER or TEXT values".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn compound_row_id<'a>(
    table: &TableDef,
    mut value: impl FnMut(usize) -> Result<SqlValueRef<'a>, SqlError>,
) -> Result<String, SqlError> {
    let mut id = String::from("c:");
    for index in table.primary_key_columns() {
        append_compound_key(&mut id, value(index)?)?;
    }
    Ok(id)
}

/// Checks that a row's primary-key value encodes to its document ID.
fn verify_pk_value(
    table: &TableDef,
    doc_id: &str,
    pk_val: SqlValueRef<'_>,
) -> Result<Option<i64>, SqlError> {
    let pk_col = &table.columns[table.primary_key_index];
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

/// `decode_col_ref` over a flat document: the same type rules and errors,
/// applied to borrowed members.
fn decode_flat_col<'a>(
    doc: &FlatDocument<'a>,
    col: &ColumnDef,
) -> Result<SqlValueRef<'a>, SqlError> {
    let invalid =
        |message: String| SqlError::Io(io::Error::new(io::ErrorKind::InvalidData, message));
    match doc.get(&col.name) {
        None | Some(FlatScalar::Null) => {
            if col.not_null {
                return Err(invalid(format!(
                    "stored NOT NULL column '{}' is null or missing",
                    col.name
                )));
            }
            Ok(SqlValueRef::Null)
        }
        Some(value) => match col.col_type {
            SqlType::Integer => value
                .as_i64()
                .map(SqlValueRef::Integer)
                .ok_or_else(|| invalid(format!("stored column '{}' is not an INTEGER", col.name))),
            SqlType::Real => value
                .as_f64()
                .filter(|f| f.is_finite())
                .map(SqlValueRef::Real)
                .ok_or_else(|| {
                    invalid(format!("stored column '{}' is not a finite REAL", col.name))
                }),
            SqlType::Text { .. } => match value {
                FlatScalar::Str(text) => Ok(SqlValueRef::Text(text)),
                _ => Err(invalid(format!("stored column '{}' is not TEXT", col.name))),
            },
            SqlType::Boolean => match value {
                FlatScalar::Bool(b) => Ok(SqlValueRef::Boolean(b)),
                _ => Err(invalid(format!(
                    "stored column '{}' is not BOOLEAN",
                    col.name
                ))),
            },
        },
    }
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
pub(crate) enum SqlValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Boolean(bool),
}

impl SqlValueRef<'_> {
    pub(crate) fn from_value(v: &SqlValue) -> SqlValueRef<'_> {
        match v {
            SqlValue::Null => SqlValueRef::Null,
            SqlValue::Integer(n) => SqlValueRef::Integer(*n),
            SqlValue::Real(f) => SqlValueRef::Real(*f),
            SqlValue::Text(s) => SqlValueRef::Text(s.as_str()),
            SqlValue::Boolean(b) => SqlValueRef::Boolean(*b),
        }
    }

    pub(crate) fn into_owned(self) -> SqlValue {
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

#[derive(Clone, Debug)]
pub(crate) enum BoundExpr {
    Literal(SqlValue),
    ScalarValue(SqlValue),
    Param(usize),
    Column(usize),
    Aggregate(usize),
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
    Exists(usize),
    InSubquery {
        expr: Box<BoundExpr>,
        sub_id: usize,
        negated: bool,
    },
    InSet {
        expr: Box<BoundExpr>,
        set: Arc<SubqueryInSet>,
        negated: bool,
    },
    ScalarSubquery(usize),
}

#[derive(Clone, Copy)]
enum EvalContext<'a> {
    None,
    Doc {
        table: &'a TableDef,
        obj: &'a Map<String, Value>,
    },
    Flat {
        table: &'a TableDef,
        doc: &'a FlatDocument<'a>,
    },
    Group {
        keys: &'a [ScalarKey],
        aggs: &'a [SqlValue],
    },
}

impl BoundExpr {
    fn is_int_literal_or_param(&self) -> bool {
        matches!(self, Self::Literal(SqlValue::Integer(_)) | Self::Param(_))
    }
}

fn check_assignment_type(
    bound: &BoundExpr,
    actual_ty: SqlType,
    col: &ColumnDef,
) -> Result<(), SqlError> {
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

fn eval_where_doc(
    filter: Option<&BoundExpr>,
    table: &TableDef,
    obj: &Map<String, Value>,
    params: &[SqlValue],
) -> Result<bool, SqlError> {
    subquery::record_visited_rows(1);
    let Some(expr) = filter else {
        return Ok(true);
    };
    match eval_expr_ref(expr, EvalContext::Doc { table, obj }, params)? {
        SqlValueRef::Boolean(b) => Ok(b),
        SqlValueRef::Null => Ok(false),
        other => Err(SqlError::Type(format!(
            "WHERE clause must evaluate to BOOLEAN, got {}",
            other.kind_name()
        ))),
    }
}

fn eval_where_flat(
    filter: Option<&BoundExpr>,
    table: &TableDef,
    doc: &FlatDocument<'_>,
    params: &[SqlValue],
) -> Result<bool, SqlError> {
    subquery::record_visited_rows(1);
    let Some(expr) = filter else {
        return Ok(true);
    };
    match eval_expr_ref(expr, EvalContext::Flat { table, doc }, params)? {
        SqlValueRef::Boolean(b) => Ok(b),
        SqlValueRef::Null => Ok(false),
        other => Err(SqlError::Type(format!(
            "WHERE clause must evaluate to BOOLEAN, got {}",
            other.kind_name()
        ))),
    }
}

fn eval_having_doc(
    having: Option<&BoundExpr>,
    keys: &[ScalarKey],
    aggs: &[SqlValue],
    params: &[SqlValue],
) -> Result<bool, SqlError> {
    let Some(expr) = having else {
        return Ok(true);
    };
    match eval_expr_ref(expr, EvalContext::Group { keys, aggs }, params)? {
        SqlValueRef::Boolean(b) => Ok(b),
        SqlValueRef::Null => Ok(false),
        other => Err(SqlError::Type(format!(
            "HAVING clause must evaluate to BOOLEAN, got {}",
            other.kind_name()
        ))),
    }
}

fn eval_expr_ref<'a>(
    expr: &'a BoundExpr,
    ctx: EvalContext<'a>,
    params: &'a [SqlValue],
) -> Result<SqlValueRef<'a>, SqlError> {
    match expr {
        BoundExpr::Literal(v) | BoundExpr::ScalarValue(v) => Ok(SqlValueRef::from_value(v)),
        BoundExpr::Param(idx) => Ok(SqlValueRef::from_value(&params[*idx])),
        BoundExpr::Column(idx) => match ctx {
            EvalContext::Doc { table, obj } => decode_col_ref(obj, &table.columns[*idx]),
            EvalContext::Flat { table, doc } => decode_flat_col(doc, &table.columns[*idx]),
            EvalContext::Group { keys, .. } => Ok(keys[*idx].as_value_ref()),
            EvalContext::None => unreachable!("column expression requires a row or group context"),
        },
        BoundExpr::Aggregate(idx) => match ctx {
            EvalContext::Group { aggs, .. } => Ok(SqlValueRef::from_value(&aggs[*idx])),
            _ => unreachable!("aggregate expression requires a group context"),
        },
        BoundExpr::IsNull { expr, negated } => {
            let val = eval_expr_ref(expr, ctx, params)?;
            let is_null = matches!(val, SqlValueRef::Null);
            Ok(SqlValueRef::Boolean(if *negated {
                !is_null
            } else {
                is_null
            }))
        }
        BoundExpr::Not(inner) => match eval_expr_ref(inner, ctx, params)? {
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
                match eval_expr_ref(item, ctx, params)? {
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
                match eval_expr_ref(item, ctx, params)? {
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
            let lv = eval_expr_ref(left, ctx, params)?;
            let rv = eval_expr_ref(right, ctx, params)?;
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
        BoundExpr::InSet { expr, set, negated } => {
            let val = eval_expr_ref(expr, ctx, params)?;
            Ok(set.evaluate_in(val, *negated))
        }
        BoundExpr::Exists(_) | BoundExpr::InSubquery { .. } | BoundExpr::ScalarSubquery(_) => {
            unreachable!("subquery must be resolved before expression evaluation")
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

/// Columns of an EXPLAIN result.
const EXPLAIN_COLUMNS: [&str; 4] = ["step", "table", "access", "detail"];

pub(crate) fn explain_row(
    step: &str,
    table: &str,
    access: String,
    detail: String,
) -> Vec<SqlValue> {
    vec![
        SqlValue::Text(step.into()),
        SqlValue::Text(table.into()),
        SqlValue::Text(access),
        SqlValue::Text(detail),
    ]
}

fn describe_access(access: &AccessPlan, table: &TableDef) -> String {
    match access {
        AccessPlan::Empty => "none: the WHERE clause cannot match a row".into(),
        AccessPlan::PkPoint { .. } => "primary-key lookup".into(),
        AccessPlan::PkPoints { doc_ids } => format!("primary-key lookup ({} keys)", doc_ids.len()),
        AccessPlan::PkRange {
            max_exact_rows: Some(rows),
            ..
        } => format!("primary-key range of at most {rows} keys"),
        AccessPlan::PkRange { .. } => "primary-key range".into(),
        AccessPlan::Index {
            index_idx,
            equal,
            lower,
            upper,
            unique_equality,
            ..
        } => {
            let name = &table.indexes[*index_idx].name;
            if *unique_equality {
                format!("unique index {name} lookup")
            } else {
                let range = if lower.is_some() || upper.is_some() {
                    " and a range on the next column"
                } else {
                    ""
                };
                format!("index {name}: equality on {} column(s){range}", equal.len())
            }
        }
        AccessPlan::FullScan => "full scan in primary-key order".into(),
    }
}

const EXPLAIN_COUNT_CAP: usize = 256;

fn scan_estimate(count: usize, cap: usize) -> String {
    if count > cap {
        format!("candidate rows: at least {count} (count capped)")
    } else {
        format!("candidate rows: {count}")
    }
}

pub(crate) fn explain_table_count(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    limits: &SqlLimits,
) -> Result<String, SqlError> {
    let cap = EXPLAIN_COUNT_CAP.min(limits.max_visited_rows);
    let count = tx
        .count_documents(&table.name, cap)
        .map_err(classify_io_error)?;
    Ok(scan_estimate(count, cap))
}

pub(super) fn explain_access_count(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    access: &AccessPlan,
    limits: &SqlLimits,
) -> Result<String, SqlError> {
    let cap = EXPLAIN_COUNT_CAP.min(limits.max_visited_rows);
    match access {
        AccessPlan::Empty => Ok(scan_estimate(0, cap)),
        AccessPlan::PkPoint { .. }
        | AccessPlan::Index {
            unique_equality: true,
            ..
        } => Ok("candidate rows: at most 1".into()),
        AccessPlan::PkPoints { doc_ids } => {
            Ok(format!("candidate rows: at most {}", doc_ids.len()))
        }
        AccessPlan::FullScan => explain_table_count(tx, table, limits),
        AccessPlan::PkRange {
            after_id, upper, ..
        } => {
            let upper = match upper {
                UpperBound::Unbounded => Bound::Unbounded,
                UpperBound::Included(id) => Bound::Included(id.as_str()),
                UpperBound::Excluded(id) => Bound::Excluded(id.as_str()),
            };
            let count = tx
                .count_document_range(&table.name, after_id.as_deref(), upper, cap)
                .map_err(classify_io_error)?;
            Ok(scan_estimate(count, cap))
        }
        AccessPlan::Index {
            index_idx,
            equal,
            lower,
            upper,
            ..
        } => {
            let query = IndexQuery {
                equal: equal.clone(),
                lower: lower.clone(),
                upper: upper.clone(),
                after: None,
                limit: 1,
            };
            let count = tx
                .count_named_index_entries(
                    &table.name,
                    &table.indexes[*index_idx].name,
                    &query,
                    cap + 1,
                )
                .map_err(classify_io_error)?;
            Ok(scan_estimate(count, cap))
        }
    }
}

pub(crate) fn explain_select_plan(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &SelectPlan,
    stmt: &SelectStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    let limit = resolve_limit_value(stmt.limit, params, "LIMIT")?;
    let _ = resolve_limit_value(stmt.offset, params, "OFFSET")?;
    match plan {
        SelectPlan::Simple(simple) => {
            let (access, residual) =
                simple_select_access(simple, table, params, limit.is_some(), false);
            let access_text = if simple.simple_pk_eq.is_some() {
                "primary-key lookup".to_owned()
            } else {
                describe_access(&access, table)
            };
            let filter = match (&simple.filter, residual) {
                (None, _) => "no WHERE",
                (Some(_), None) => "WHERE answered by the index",
                (Some(_), Some(_)) => "WHERE checked on each row read",
            };
            let streams = !matches!(
                access,
                AccessPlan::Index {
                    preserves_pk_asc: false,
                    satisfies_order_by: false,
                    ..
                }
            );
            let order = match (&simple.order_strategy, limit.is_some()) {
                (OrderStrategy::PkAsc, true) if streams => {
                    "rows stream in key order and reading stops at OFFSET + LIMIT"
                }
                (OrderStrategy::PkAsc, false) if streams => "rows stream in key order",
                (OrderStrategy::PkAsc, _) => "rows are sorted by primary key",
                (OrderStrategy::PkDesc, _) => "rows are returned in descending key order",
                (OrderStrategy::General, true) => "top OFFSET + LIMIT rows kept while reading",
                (OrderStrategy::General, false) => "rows are sorted by ORDER BY",
            };
            let estimate = explain_access_count(tx, table, &access, limits)?;
            Ok(vec![explain_row(
                "scan",
                &table.name,
                access_text,
                format!("{filter}; {order}; {estimate}"),
            )])
        }
        SelectPlan::Aggregate(aggregate) => {
            let access = plan::plan_access(
                aggregate.filter.as_ref().map(|f| &f.expr),
                table,
                &[],
                false,
                params,
            );
            let grouping = if aggregate.group_cols.is_empty() {
                "one group".to_owned()
            } else {
                format!("groups by {} column(s)", aggregate.group_cols.len())
            };
            let shortcut = if aggregate.ungrouped_min_col.is_some() {
                "; MIN may stop at the first ordered row"
            } else {
                ""
            };
            let counts_keys = aggregate.filter.is_none()
                && aggregate.having.is_none()
                && aggregate.group_cols.is_empty()
                && !aggregate.aggregates.is_empty()
                && aggregate
                    .aggregates
                    .iter()
                    .all(|a| matches!(a.kind, AggregateKind::CountStar));
            let access_text = if counts_keys {
                "count of row keys without reading rows".to_owned()
            } else {
                describe_access(&access, table)
            };
            let filter = if aggregate.filter.is_some() {
                "WHERE checked on each row read"
            } else {
                "no WHERE"
            };
            let estimate = explain_access_count(tx, table, &access, limits)?;
            Ok(vec![
                explain_row(
                    "scan",
                    &table.name,
                    access_text,
                    format!("{filter}; {estimate}"),
                ),
                explain_row(
                    "aggregate",
                    &table.name,
                    grouping,
                    format!(
                        "{} aggregate(s){}{shortcut}",
                        aggregate.aggregates.len(),
                        if aggregate.having.is_some() {
                            ", HAVING after grouping"
                        } else {
                            ""
                        }
                    ),
                ),
            ])
        }
    }
}

/// Describes how a SELECT would run, without reading rows. The access path
/// comes from the same planning functions execution uses.
fn explain(
    tx: &DocumentTransaction<'_, '_>,
    inner: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let rows = match &inner.inner {
        Statement::Join(join) => super::join::explain(tx, join, params, limits, cache)?,
        Statement::Select(select) => {
            let cached_table = catalog::load_table_cached(tx, &select.table, &cache.tables)?;
            let plan =
                get_or_build_select_plan(inner.stmt_id, select, &cached_table, limits, cache)?;
            plan.validate_params(params)?;
            let mut rows = Vec::new();
            let (sub_slots, sub_checks) = plan.subquery_metadata();
            if !sub_slots.is_empty() {
                let prepared = subquery::bind_subqueries(
                    tx,
                    sub_slots,
                    sub_checks,
                    &[cached_table.def.as_ref()],
                    params,
                    limits,
                    cache,
                )?;
                subquery::explain_subqueries(tx, &prepared, params, limits, cache, &mut rows)?;
            }
            rows.extend(explain_select_plan(
                tx,
                &cached_table.def,
                &plan,
                select,
                params,
                limits,
            )?);
            rows
        }
        _ => {
            return Err(SqlError::Unsupported {
                feature: "EXPLAIN of a statement other than SELECT".into(),
                offset: None,
            });
        }
    };
    Ok(SqlOutcome::Rows {
        columns: EXPLAIN_COLUMNS.iter().map(|c| (*c).to_owned()).collect(),
        rows,
    })
}
