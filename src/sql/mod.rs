//! Embedded SQL subset executed atomically over `DocumentDatabase`.
//!
//! This module provides a bounded, transactional SQL subset (`CREATE TABLE`,
//! `INSERT`, `SELECT`, `UPDATE`, `DELETE`) compiled onto `DocumentDatabase`.
//! It is NOT PostgreSQL- or wire-compatible. See `docs/SQL_SUBSET.md` for the
//! exact grammar, bounds, ordering guarantee, and backend behavior.

mod catalog;
mod ddl;
mod exec;
mod foreign_key;
mod join;
mod lexer;
mod parser;
mod plan;
mod subquery;
mod transaction;
mod value;

#[cfg(test)]
mod tests;

pub use parser::SqlStatement;
pub use transaction::SqlTransaction;
pub use value::{
    DEFAULT_SQL_DISTINCT_VALUES, DEFAULT_SQL_GROUPS, DEFAULT_SQL_SORT_ROWS,
    DEFAULT_SQL_SUBQUERY_VALUES, DEFAULT_SQL_VISITED_ROWS, MAX_SQL_BYTES, MAX_SQL_COLUMNS,
    MAX_SQL_DISTINCT_VALUES, MAX_SQL_EXPR_DEPTH, MAX_SQL_GROUPS, MAX_SQL_IDENTIFIER_BYTES,
    MAX_SQL_INSERT_ROWS, MAX_SQL_MUTATION_ROWS, MAX_SQL_PARAMS, MAX_SQL_SELECT_ROWS,
    MAX_SQL_SORT_ROWS, MAX_SQL_SUBQUERY_DEPTH, MAX_SQL_SUBQUERY_VALUES, MAX_SQL_TEXT_BYTES,
    MAX_SQL_TEXT_PK_BYTES, MAX_SQL_TOKENS, MAX_SQL_VISITED_ROWS, SqlError, SqlLimits, SqlOutcome,
    SqlValue,
};

use crate::document::DocumentDatabase;

/// Embedded SQL entry point wrapping a [`DocumentDatabase`].
///
/// Each statement runs inside a single `DocumentDatabase` transaction
/// (`transaction_detailed` in `ReadSet` mode), propagating pre-commit conflicts
/// and unknown commit outcomes unchanged without automatic retries.
pub struct SqlDatabase<'a> {
    db: DocumentDatabase<'a>,
    limits: SqlLimits,
    cache: exec::SqlCache,
}

impl<'a> SqlDatabase<'a> {
    /// Creates a SQL view over `db` with [`SqlLimits::default()`].
    pub fn new(db: DocumentDatabase<'a>) -> Self {
        Self {
            db,
            limits: SqlLimits::default(),
            cache: exec::SqlCache::new(),
        }
    }

    /// Creates a SQL view over `db` with validated custom bounds.
    pub fn with_limits(db: DocumentDatabase<'a>, limits: SqlLimits) -> Result<Self, SqlError> {
        limits.validate()?;
        Ok(Self {
            db,
            limits,
            cache: exec::SqlCache::new(),
        })
    }

    /// Replaces the configured execution bounds after validating against hard maxima.
    pub fn set_limits(&mut self, limits: SqlLimits) -> Result<(), SqlError> {
        limits.validate()?;
        self.limits = limits;
        self.cache.clear_statements_and_plans();
        Ok(())
    }

    /// Returns the active execution bounds.
    pub fn limits(&self) -> &SqlLimits {
        &self.limits
    }

    /// Returns a reference to the underlying [`DocumentDatabase`].
    pub fn document_database(&self) -> &DocumentDatabase<'a> {
        &self.db
    }

    /// Parses and validates a single SQL statement without executing it.
    pub fn prepare(&self, sql: &str) -> Result<SqlStatement, SqlError> {
        parser::parse_statement(sql, &self.limits)
    }

    /// Executes a previously prepared [`SqlStatement`] in one atomic transaction.
    pub fn execute_prepared(
        &self,
        stmt: &SqlStatement,
        params: &[SqlValue],
    ) -> Result<SqlOutcome, SqlError> {
        exec::execute_statement(&self.db, stmt, params, &self.limits, &self.cache, false)
    }

    /// Parses and executes a single SQL statement in one atomic transaction.
    pub fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError> {
        if let Some(stmt) = self.cache.get_cached_statement(sql, &self.limits)? {
            exec::execute_statement(&self.db, &stmt, params, &self.limits, &self.cache, false)
        } else {
            let stmt = self.prepare(sql)?;
            exec::execute_statement(&self.db, &stmt, params, &self.limits, &self.cache, false)
        }
    }

    /// Runs a multi-statement serializable transaction in `DocumentTransactionMode::ReadSet`.
    ///
    /// Statements executed inside `f` share one `DocumentTransaction`, see each other's
    /// staged writes, and commit atomically only when `f` returns `Ok` without any prior
    /// poisoned statement error. Returning `Err` or unwinding on panic rolls back all
    /// staged SQL and catalog changes.
    ///
    /// Confirmed pre-commit conflicts return [`SqlError::Conflict`], uncertain commit
    /// failures return [`SqlError::OutcomeUnknown`], and exceeding the transaction-wide
    /// staged-write (`1,024` keys / `16 MiB`) or read-set limits returns [`SqlError::Bounds`].
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&mut SqlTransaction<'_>) -> Result<T, SqlError>,
    ) -> Result<T, SqlError> {
        transaction::run_sql_transaction(&self.db, &self.limits, &self.cache, f)
    }

    /// Runs [`Self::transaction`] up to `max_attempts` times, retrying **only** confirmed
    /// pre-commit conflicts ([`SqlError::Conflict`]).
    ///
    /// Never retries [`SqlError::OutcomeUnknown`] or non-conflict errors. Because `f` may
    /// execute multiple times before committing, callers must ensure `f` has no unrolled-back
    /// external side effects.
    pub fn transaction_with_retry<T>(
        &self,
        max_attempts: usize,
        f: impl FnMut(&mut SqlTransaction<'_>) -> Result<T, SqlError>,
    ) -> Result<T, SqlError> {
        transaction::run_sql_transaction_with_retry(
            &self.db,
            &self.limits,
            &self.cache,
            max_attempts,
            f,
        )
    }

    #[doc(hidden)]
    pub fn execute_force_full_scan_for_test(
        &self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<SqlOutcome, SqlError> {
        let stmt = self.prepare(sql)?;
        exec::execute_statement(&self.db, &stmt, params, &self.limits, &self.cache, true)
    }
}
