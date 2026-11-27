//! Statement binding, expression evaluation, and transactional execution over `DocumentDatabase`.
use super::catalog::{self, CachedTable, ColumnDef, IndexDef, IndexState, TableCache, TableDef};
use super::parser::{
    self, AggregateCall, AggregateKind, Assignment, CompareOp, CreateIndexStmt, CreateTableStmt,
    DeleteStmt, DropIndexStmt, Expr, InsertStmt, LimitValue, LiteralValue, NullsOrder,
    OrderByTarget, SelectColumns, SelectExpr, SelectItem, SelectStmt, SqlStatement, Statement,
    UpdateStmt,
};
use super::plan::{self, AccessPlan, CompiledSelectAccess, ResolvedTemplatePlan, UpperBound};
use super::value::{
    MAX_SQL_INDEXES_PER_TABLE, MAX_SQL_TEXT_PK_BYTES, SqlError, SqlLimits, SqlOutcome, SqlType,
    SqlValue, coerce_for_column, compare_i64_f64, encode_i64_pk_bytes, exact_i64_to_f64,
};
use crate::document::{
    DocumentDatabase, DocumentTransaction, DocumentTransactionMode, IndexBound, IndexQuery,
    IndexValue,
};
use crate::{MAX_SCAN_ENTRIES, TransactionFailure};
use parking_lot::RwLock;
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
use std::collections::hash_map::RandomState;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hash, Hasher};
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
        Statement::AddColumn(add) => {
            return super::ddl::execute_add_column(db, add, limits, cache);
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

fn execute_statement_in_validated_tx(
    tx: &mut DocumentTransaction<'_, '_>,
    stmt: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    match &stmt.inner {
        Statement::CreateTable(create) => exec_create_table(tx, create, limits),
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
        Statement::AddColumn(add) => super::ddl::add_column_in_tx(tx, add, limits, cache),
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
            state: IndexState::Ready,
            is_constraint: true,
        })
        .collect();
    let def = TableDef::new(stmt.name.clone(), columns, stmt.primary_key_index, indexes)?;
    catalog::create_table(tx, &def, stmt.if_not_exists)?;
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
            let val = eval_expr_ref(&bound, EvalContext::None, params)?.into_owned();
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

enum SelectPlan {
    Simple(BoundSelectPlan),
    Aggregate(BoundAggregatePlan),
}

struct BoundSelectPlan {
    proj_indices: Vec<usize>,
    proj_is_last: Vec<bool>,
    col_names: Vec<String>,
    filter: Option<BoundFilter>,
    order_by: Vec<BoundOrderBy>,
    order_strategy: OrderStrategy,
    simple_pk_eq: Option<PkEqSource>,
    compiled_access: CompiledSelectAccess,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BoundAggregate {
    kind: AggregateKind,
    col_idx: Option<usize>,
    col_type: Option<SqlType>,
}

#[derive(Clone, Copy, Debug)]
enum BoundSelectOutput {
    GroupCol(usize),
    Aggregate(usize),
}

#[derive(Clone, Copy, Debug)]
enum BoundGroupOrderTarget {
    GroupCol(usize),
    Aggregate(usize),
}

#[derive(Clone, Copy, Debug)]
struct BoundGroupOrderBy {
    target: BoundGroupOrderTarget,
    desc: bool,
    nulls_first: bool,
}

struct BoundAggregatePlan {
    col_names: Vec<String>,
    outputs: Vec<BoundSelectOutput>,
    group_cols: Vec<usize>,
    aggregates: Vec<BoundAggregate>,
    filter: Option<BoundFilter>,
    having: Option<BoundFilter>,
    order_by: Vec<BoundGroupOrderBy>,
    ungrouped_min_col: Option<usize>,
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

struct AggBindContext<'a> {
    group_cols: &'a [usize],
    aggregates: &'a mut Vec<BoundAggregate>,
}

fn intern_aggregate(
    table: &TableDef,
    agg: &AggregateCall,
    aggregates: &mut Vec<BoundAggregate>,
) -> Result<(usize, SqlType), SqlError> {
    let (col_idx, col_type, result_type) = match agg.kind {
        AggregateKind::CountStar => (None, None, SqlType::Integer),
        AggregateKind::Count | AggregateKind::CountDistinct => {
            let name = agg.column.as_deref().expect("COUNT column");
            let idx = table
                .column_index(name)
                .ok_or_else(|| SqlError::NoSuchColumn {
                    table: table.name.clone(),
                    column: name.to_owned(),
                })?;
            (
                Some(idx),
                Some(table.columns[idx].col_type),
                SqlType::Integer,
            )
        }
        AggregateKind::Sum => {
            let name = agg.column.as_deref().expect("SUM column");
            let idx = table
                .column_index(name)
                .ok_or_else(|| SqlError::NoSuchColumn {
                    table: table.name.clone(),
                    column: name.to_owned(),
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
            let idx = table
                .column_index(name)
                .ok_or_else(|| SqlError::NoSuchColumn {
                    table: table.name.clone(),
                    column: name.to_owned(),
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
            let idx = table
                .column_index(name)
                .ok_or_else(|| SqlError::NoSuchColumn {
                    table: table.name.clone(),
                    column: name.to_owned(),
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
) -> Result<Option<BoundFilter>, SqlError> {
    bind_boolean_clause_static(filter, table, "WHERE", None)
}

fn bind_having_static(
    having: Option<&Expr>,
    table: &TableDef,
    group_cols: &[usize],
    aggregates: &mut Vec<BoundAggregate>,
) -> Result<Option<BoundFilter>, SqlError> {
    let mut ctx = AggBindContext {
        group_cols,
        aggregates,
    };
    bind_boolean_clause_static(having, table, "HAVING", Some(&mut ctx))
}

fn bind_boolean_clause_static(
    expr: Option<&Expr>,
    table: &TableDef,
    clause_name: &str,
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
        agg_ctx,
    )?;
    match ty {
        StaticExprType::Null | StaticExprType::Typed(SqlType::Boolean) => {}
        StaticExprType::Param(p) => param_constraints.push(ParamConstraint::Boolean(p)),
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

fn bind_expr_static(
    expr: &Expr,
    table: Option<&TableDef>,
    table_name: &str,
    constraints: &mut Vec<ParamConstraint>,
) -> Result<(BoundExpr, StaticExprType), SqlError> {
    bind_expr_static_with(expr, table, table_name, constraints, None)
}

fn bind_expr_static_with(
    expr: &Expr,
    table: Option<&TableDef>,
    table_name: &str,
    constraints: &mut Vec<ParamConstraint>,
    mut agg_ctx: Option<&mut AggBindContext<'_>>,
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
            let (agg_idx, ty) = intern_aggregate(table, agg, ctx.aggregates)?;
            Ok((BoundExpr::Aggregate(agg_idx), StaticExprType::Typed(ty)))
        }
        Expr::IsNull { expr, negated } => {
            let (bound, _) = bind_expr_static_with(expr, table, table_name, constraints, agg_ctx)?;
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
                bind_expr_static_with(inner, table, table_name, constraints, agg_ctx)?;
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
                let (bound, ty) = bind_expr_static_with(
                    item,
                    table,
                    table_name,
                    constraints,
                    agg_ctx.as_deref_mut(),
                )?;
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
                let (bound, ty) = bind_expr_static_with(
                    item,
                    table,
                    table_name,
                    constraints,
                    agg_ctx.as_deref_mut(),
                )?;
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
            let (l_bound, l_ty) = bind_expr_static_with(
                left,
                table,
                table_name,
                constraints,
                agg_ctx.as_deref_mut(),
            )?;
            let (r_bound, r_ty) =
                bind_expr_static_with(right, table, table_name, constraints, agg_ctx)?;
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
        build_aggregate_select_plan(stmt, cached_table, limits)?
    } else {
        build_simple_select_plan(stmt, cached_table, limits)?
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
    limits: &SqlLimits,
) -> Result<Arc<SelectPlan>, SqlError> {
    let table = &cached_table.def;
    let (proj_indices, col_names, select_items_for_alias) = match &stmt.columns {
        SelectColumns::All => (
            (0..table.columns.len()).collect::<Vec<_>>(),
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
            let mut indices = Vec::with_capacity(items.len());
            let mut names = Vec::with_capacity(items.len());
            for item in items {
                let SelectExpr::Column { name, .. } = &item.expr else {
                    unreachable!("non-aggregate plan has only column select items")
                };
                let idx = table
                    .column_index(name)
                    .ok_or_else(|| SqlError::NoSuchColumn {
                        table: table.name.clone(),
                        column: name.clone(),
                    })?;
                indices.push(idx);
                names.push(item.alias.clone().unwrap_or_else(|| name.clone()));
            }
            (indices, names, Some(items.as_slice()))
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
        let OrderByTarget::Column(col_name) = &item.target else {
            unreachable!("non-aggregate plan has only column ORDER BY items")
        };
        let col_idx = if let Some(items) = select_items_for_alias {
            if let Some(pos) = find_unique_select_alias(items, col_name, item.offset)? {
                proj_indices[pos]
            } else {
                table
                    .column_index(col_name)
                    .ok_or_else(|| SqlError::NoSuchColumn {
                        table: table.name.clone(),
                        column: col_name.clone(),
                    })?
            }
        } else {
            table
                .column_index(col_name)
                .ok_or_else(|| SqlError::NoSuchColumn {
                    table: table.name.clone(),
                    column: col_name.clone(),
                })?
        };
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
    let compiled_access = plan::compile_select_access(
        filter.as_ref().map(|f| &f.expr),
        table,
        &order_by,
        stmt.limit.is_some(),
    );

    Ok(Arc::new(SelectPlan::Simple(BoundSelectPlan {
        proj_indices,
        proj_is_last,
        col_names,
        filter,
        order_by,
        order_strategy,
        simple_pk_eq,
        compiled_access,
    })))
}

fn build_aggregate_select_plan(
    stmt: &SelectStmt,
    cached_table: &CachedTable,
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
        let col_idx = table
            .column_index(&item.column)
            .ok_or_else(|| SqlError::NoSuchColumn {
                table: table.name.clone(),
                column: item.column.clone(),
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

    let mut outputs = Vec::with_capacity(select_items.len());
    let mut col_names = Vec::with_capacity(select_items.len());
    for item in select_items {
        match &item.expr {
            SelectExpr::Column { name, offset } => {
                let col_idx = table
                    .column_index(name)
                    .ok_or_else(|| SqlError::NoSuchColumn {
                        table: table.name.clone(),
                        column: name.clone(),
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
                col_names.push(item.alias.clone().unwrap_or_else(|| name.clone()));
            }
            SelectExpr::Aggregate(agg) => {
                let (agg_idx, _) = intern_aggregate(table, agg, &mut aggregates)?;
                outputs.push(BoundSelectOutput::Aggregate(agg_idx));
                col_names.push(
                    item.alias
                        .clone()
                        .unwrap_or_else(|| format_aggregate_name(agg)),
                );
            }
        }
    }

    let filter = bind_where_static(stmt.filter.as_ref(), table)?;
    let having = bind_having_static(stmt.having.as_ref(), table, &group_cols, &mut aggregates)?;

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
                    match outputs[pos] {
                        BoundSelectOutput::GroupCol(g) => BoundGroupOrderTarget::GroupCol(g),
                        BoundSelectOutput::Aggregate(a) => BoundGroupOrderTarget::Aggregate(a),
                    }
                } else {
                    let col_idx =
                        table
                            .column_index(name)
                            .ok_or_else(|| SqlError::NoSuchColumn {
                                table: table.name.clone(),
                                column: name.clone(),
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
                let (agg_idx, _) = intern_aggregate(table, agg, &mut aggregates)?;
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
        group_cols,
        aggregates,
        filter,
        having,
        order_by,
        ungrouped_min_col,
    })))
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
    match plan.as_ref() {
        SelectPlan::Simple(simple) => {
            exec_simple_select(tx, table, simple, stmt, params, limits, force_full_scan)
        }
        SelectPlan::Aggregate(agg) => {
            exec_aggregate_select(tx, table, agg, stmt, params, limits, force_full_scan)
        }
    }
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
fn exec_simple_select(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &BoundSelectPlan,
    stmt: &SelectStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
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

    let (access, bound_filter) =
        simple_select_access(plan, table, params, limit.is_some(), force_full_scan);
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
                    plan,
                    access,
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
                    plan,
                    access,
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
                    plan,
                    access,
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
                    access,
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
                    access,
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
                    plan,
                    access,
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
                    access,
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum ScalarKey {
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
    fn from_value_ref(v: SqlValueRef<'_>) -> Self {
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

fn compare_order_cell_ref(
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

struct DistinctSet {
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

enum AggAcc {
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
    fn new(spec: &BoundAggregate) -> Self {
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

    fn update(
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

    fn finalize(self) -> Result<SqlValue, SqlError> {
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
    obj: &'a Map<String, Value>,
    table: &TableDef,
    col_idx: usize,
    verified_pk: Option<i64>,
) -> Result<SqlValueRef<'a>, SqlError> {
    if col_idx == table.primary_key_index {
        if let Some(n) = verified_pk {
            return Ok(SqlValueRef::Integer(n));
        }
    }
    decode_col_ref(obj, &table.columns[col_idx])
}

struct DistinctBudget<'a> {
    hash_builder: &'a RandomState,
    total_distinct: usize,
    max_distinct: usize,
}

fn update_group_accs(
    accs: &mut [AggAcc],
    agg_specs: &[BoundAggregate],
    obj: &Map<String, Value>,
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

#[allow(clippy::too_many_arguments)]
fn exec_aggregate_select(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    plan: &BoundAggregatePlan,
    stmt: &SelectStmt,
    params: &[SqlValue],
    limits: &SqlLimits,
    force_full_scan: bool,
) -> Result<SqlOutcome, SqlError> {
    validate_filter_params(plan.filter.as_ref(), params)?;
    validate_filter_params(plan.having.as_ref(), params)?;

    let limit = resolve_limit_value(stmt.limit, params, "LIMIT")?;
    let offset = resolve_limit_value(stmt.offset, params, "OFFSET")?.unwrap_or(0);

    if limit == Some(0) {
        return Ok(SqlOutcome::Rows {
            columns: plan.col_names.clone(),
            rows: Vec::new(),
        });
    }

    let bound_filter = plan.filter.as_ref().map(|f| &f.expr);
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
            if min_col_idx == table.primary_key_index {
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
            visit_access_rows(
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
                        &obj,
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
            visit_access_rows(
                tx,
                table,
                access,
                bound_filter,
                params,
                limits,
                None,
                |_doc_id, verified_pk, obj| {
                    let accs = if plan.group_cols.len() == 1 {
                        let key = decode_col_with_pk(&obj, table, plan.group_cols[0], verified_pk)?;
                        table_groups.get_or_insert(&[key], &plan.aggregates)?
                    } else {
                        let mut keys = Vec::with_capacity(plan.group_cols.len());
                        for &col_idx in &plan.group_cols {
                            keys.push(decode_col_with_pk(&obj, table, col_idx, verified_pk)?);
                        }
                        table_groups.get_or_insert(&keys, &plan.aggregates)?
                    };
                    update_group_accs(
                        accs,
                        &plan.aggregates,
                        &obj,
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

    let having_expr = plan.having.as_ref().map(|f| &f.expr);
    let mut surviving = Vec::new();
    for entry in raw_groups {
        let mut aggs = Vec::with_capacity(entry.accs.len());
        for acc in entry.accs {
            aggs.push(acc.finalize()?);
        }
        if eval_having_doc(having_expr, &entry.keys, &aggs, params)? {
            surviving.push(FinalGroup {
                keys: entry.keys,
                aggs,
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
        if result_len > limits.max_select_rows {
            return Err(SqlError::Bounds(format!(
                "SELECT result exceeds max_select_rows ({})",
                limits.max_select_rows
            )));
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
                if offset == 0 && heap.len() >= limits.max_select_rows {
                    return Err(SqlError::Bounds(format!(
                        "SELECT result exceeds max_select_rows ({})",
                        limits.max_select_rows
                    )));
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
            if result_len > limits.max_select_rows {
                return Err(SqlError::Bounds(format!(
                    "SELECT result exceeds max_select_rows ({})",
                    limits.max_select_rows
                )));
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
        if result_len > limits.max_select_rows {
            return Err(SqlError::Bounds(format!(
                "SELECT result exceeds max_select_rows ({})",
                limits.max_select_rows
            )));
        }
        surviving.sort_unstable_by(|a, b| compare_final_groups(a, b, &plan.order_by));
        surviving.into_iter().skip(offset).collect()
    };

    let mut rows = Vec::with_capacity(ordered_groups.len());
    for group in ordered_groups {
        let mut row = Vec::with_capacity(plan.outputs.len());
        for output in &plan.outputs {
            let val = match *output {
                BoundSelectOutput::GroupCol(pos) => group.keys[pos].clone().into_sql_value(),
                BoundSelectOutput::Aggregate(idx) => group.aggs[idx].clone(),
            };
            row.push(val);
        }
        rows.push(row);
    }

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
    access: AccessPlan,
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
    access: AccessPlan,
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
        let mut first: Option<(String, Vec<SqlValue>)> = None;
        let mut rest: Vec<(String, Vec<SqlValue>)> = Vec::new();
        visit_access_rows(
            tx,
            table,
            access,
            bound_filter,
            params,
            limits,
            None,
            |doc_id, verified_pk, obj| {
                let count = usize::from(first.is_some()) + rest.len();
                if count >= limits.max_select_rows {
                    return Err(SqlError::Bounds(format!(
                        "SELECT result exceeds max_select_rows ({})",
                        limits.max_select_rows
                    )));
                }
                if count >= limits.max_sort_rows {
                    return Err(SqlError::Bounds(format!(
                        "sort row count exceeds max_sort_rows ({})",
                        limits.max_sort_rows
                    )));
                }
                let row = project_row_with_pk(
                    obj,
                    table,
                    &plan.proj_indices,
                    &plan.proj_is_last,
                    verified_pk,
                )?;
                if first.is_none() && rest.is_empty() {
                    first = Some((doc_id, row));
                } else {
                    if let Some(first_pair) = first.take() {
                        rest.push(first_pair);
                    }
                    rest.push((doc_id, row));
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if rest.is_empty() {
            return Ok(first.map_or_else(Vec::new, |(_, row)| vec![row]));
        }
        rest.sort_unstable_by(|a, b| {
            let ord = a.0.as_bytes().cmp(b.0.as_bytes());
            if desc { ord.reverse() } else { ord }
        });
        Ok(rest.into_iter().map(|(_, row)| row).collect())
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
                let val = eval_expr_ref(bound, EvalContext::Doc { table, obj: &obj }, params)?
                    .into_owned();
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
        tx, table, plan, filter, params, limits, stream_cap, on_match,
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
    equal: Vec<IndexValue>,
    lower: Option<IndexBound>,
    upper: Option<IndexBound>,
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
    let mut query = IndexQuery {
        equal,
        lower,
        upper,
        after: None,
        limit: 1,
    };

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
        query.limit = page_limit;
        let page = tx
            .query_index(&table.name, index_name, &query)
            .map_err(classify_io_error)?;
        if page.documents.is_empty() {
            return Ok(());
        }
        let done = unique_equality
            || (page.plan.entries_read < page_limit
                && page.plan.returned == page.plan.entries_read);
        query.after = page.next;

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
}

#[derive(Clone, Copy)]
enum EvalContext<'a> {
    None,
    Doc {
        table: &'a TableDef,
        obj: &'a Map<String, Value>,
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
        Expr::Aggregate(agg) => Err(SqlError::Unsupported {
            feature: format!(
                "aggregate function '{}'",
                agg.kind.sql_name().to_ascii_uppercase()
            ),
            offset: Some(agg.offset),
        }),
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
    match eval_expr_ref(expr, EvalContext::Doc { table, obj }, params)? {
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
        BoundExpr::Literal(v) => Ok(SqlValueRef::from_value(v)),
        BoundExpr::Param(idx) => Ok(SqlValueRef::from_value(&params[*idx])),
        BoundExpr::Column(idx) => match ctx {
            EvalContext::Doc { table, obj } => decode_col_ref(obj, &table.columns[*idx]),
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

fn explain_row(step: &str, table: &str, access: String, detail: String) -> Vec<SqlValue> {
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
            let table = &cached_table.def;
            let limit = resolve_limit_value(select.limit, params, "LIMIT")?;
            match plan.as_ref() {
                SelectPlan::Simple(simple) => {
                    validate_filter_params(simple.filter.as_ref(), params)?;
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
                        (OrderStrategy::General, true) => {
                            "top OFFSET + LIMIT rows kept while reading"
                        }
                        (OrderStrategy::General, false) => "rows are sorted by ORDER BY",
                    };
                    vec![explain_row(
                        "scan",
                        &table.name,
                        access_text,
                        format!("{filter}; {order}"),
                    )]
                }
                SelectPlan::Aggregate(aggregate) => {
                    validate_filter_params(aggregate.filter.as_ref(), params)?;
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
                    vec![
                        explain_row(
                            "scan",
                            &table.name,
                            describe_access(&access, table),
                            if aggregate.filter.is_some() {
                                "WHERE checked on each row read".into()
                            } else {
                                "no WHERE".into()
                            },
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
                    ]
                }
            }
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
