//! Embedded SQL subset executed atomically over `DocumentDatabase`.
//!
//! This module provides a bounded, transactional SQL subset (`CREATE TABLE`,
//! `INSERT`, `SELECT`, `UPDATE`, `DELETE`) compiled onto `DocumentDatabase`.
//! It is NOT PostgreSQL- or wire-compatible. See `docs/SQL_SUBSET.md` for the
//! exact grammar, bounds, ordering guarantee, and backend behavior.

mod catalog;
mod exec;
mod lexer;
mod parser;
mod value;

#[cfg(test)]
mod tests;

pub use parser::SqlStatement;
pub use value::{
    DEFAULT_SQL_VISITED_ROWS, MAX_SQL_BYTES, MAX_SQL_COLUMNS, MAX_SQL_EXPR_DEPTH,
    MAX_SQL_IDENTIFIER_BYTES, MAX_SQL_INSERT_ROWS, MAX_SQL_MUTATION_ROWS, MAX_SQL_PARAMS,
    MAX_SQL_SELECT_ROWS, MAX_SQL_TEXT_BYTES, MAX_SQL_TEXT_PK_BYTES, MAX_SQL_TOKENS,
    MAX_SQL_VISITED_ROWS, SqlError, SqlLimits, SqlOutcome, SqlValue,
};

use crate::document::DocumentDatabase;

/// Embedded SQL entry point wrapping a [`DocumentDatabase`].
///
/// Each statement runs inside a single conservative `DocumentDatabase`
/// transaction (`transaction_detailed`), propagating pre-commit conflicts and
/// unknown commit outcomes unchanged without automatic retries.
pub struct SqlDatabase<'a> {
    db: DocumentDatabase<'a>,
    limits: SqlLimits,
}

impl<'a> SqlDatabase<'a> {
    /// Creates a SQL view over `db` with [`SqlLimits::default()`].
    pub fn new(db: DocumentDatabase<'a>) -> Self {
        Self {
            db,
            limits: SqlLimits::default(),
        }
    }

    /// Creates a SQL view over `db` with validated custom bounds.
    pub fn with_limits(db: DocumentDatabase<'a>, limits: SqlLimits) -> Result<Self, SqlError> {
        limits.validate()?;
        Ok(Self { db, limits })
    }

    /// Replaces the configured execution bounds after validating against hard maxima.
    pub fn set_limits(&mut self, limits: SqlLimits) -> Result<(), SqlError> {
        limits.validate()?;
        self.limits = limits;
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
        exec::execute_statement(&self.db, stmt, params, &self.limits)
    }

    /// Parses and executes a single SQL statement in one atomic transaction.
    pub fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError> {
        let stmt = self.prepare(sql)?;
        self.execute_prepared(&stmt, params)
    }
}
