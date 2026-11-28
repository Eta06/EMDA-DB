//! Multi-statement serializable SQL transactions over `DocumentDatabase`.
use super::exec::{self, SqlCache};
use super::parser::{self, SqlStatement};
use super::value::{SqlError, SqlLimits, SqlOutcome, SqlValue};
use crate::document::{DocumentDatabase, DocumentTransaction, DocumentTransactionMode};
use std::io;
use std::time::Instant;

trait DocTxDispatch {
    fn execute_stmt(
        &mut self,
        stmt: &SqlStatement,
        params: &[SqlValue],
        limits: &SqlLimits,
        cache: &SqlCache,
        force_full_scan: bool,
    ) -> Result<SqlOutcome, SqlError>;
}

impl DocTxDispatch for DocumentTransaction<'_, '_> {
    fn execute_stmt(
        &mut self,
        stmt: &SqlStatement,
        params: &[SqlValue],
        limits: &SqlLimits,
        cache: &SqlCache,
        force_full_scan: bool,
    ) -> Result<SqlOutcome, SqlError> {
        exec::execute_statement_in_tx(self, stmt, params, limits, cache, force_full_scan)
    }
}

/// An active multi-statement SQL transaction backed by one `DocumentTransaction`
/// in `DocumentTransactionMode::ReadSet`.
///
/// Statements executed through `SqlTransaction` observe earlier staged writes
/// in the same transaction. Commit happens once when the enclosing
/// [`super::SqlDatabase::transaction`] closure returns `Ok`; returning `Err` or
/// unwinding on panic discards all staged changes.
///
/// If any statement executed on `SqlTransaction` returns an error, the
/// transaction is marked poisoned: subsequent statement calls immediately
/// return [`SqlError::TransactionAborted`], and returning `Ok` from the
/// closure fails commit with [`SqlError::TransactionAborted`] rather than
/// committing partial work.
pub struct SqlTransaction<'a> {
    tx: &'a mut dyn DocTxDispatch,
    limits: &'a SqlLimits,
    cache: &'a SqlCache,
    poisoned: Option<String>,
}

impl SqlTransaction<'_> {
    /// Returns the active execution bounds inherited from the parent `SqlDatabase`.
    pub fn limits(&self) -> &SqlLimits {
        self.limits
    }

    /// Executes a previously prepared [`SqlStatement`] within this transaction.
    pub fn execute_prepared(
        &mut self,
        stmt: &SqlStatement,
        params: &[SqlValue],
    ) -> Result<SqlOutcome, SqlError> {
        self.run_stmt(stmt, params, false)
    }

    /// Parses and executes a single SQL statement within this transaction.
    pub fn execute(&mut self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError> {
        if let Some(reason) = &self.poisoned {
            return Err(SqlError::TransactionAborted(format!(
                "transaction already aborted by earlier statement error: {reason}"
            )));
        }
        match self.cache.get_cached_statement(sql, self.limits) {
            Ok(Some(stmt)) => self.run_stmt(&stmt, params, false),
            Ok(None) => match parser::parse_statement(sql, self.limits) {
                Ok(stmt) => self.run_stmt(&stmt, params, false),
                Err(err) => {
                    self.poisoned = Some(err.to_string());
                    Err(err)
                }
            },
            Err(err) => {
                self.poisoned = Some(err.to_string());
                Err(err)
            }
        }
    }

    #[doc(hidden)]
    pub fn execute_force_full_scan_for_test(
        &mut self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<SqlOutcome, SqlError> {
        if let Some(reason) = &self.poisoned {
            return Err(SqlError::TransactionAborted(format!(
                "transaction already aborted by earlier statement error: {reason}"
            )));
        }
        match parser::parse_statement(sql, self.limits) {
            Ok(stmt) => self.run_stmt(&stmt, params, true),
            Err(err) => {
                self.poisoned = Some(err.to_string());
                Err(err)
            }
        }
    }

    fn run_stmt(
        &mut self,
        stmt: &SqlStatement,
        params: &[SqlValue],
        force_full_scan: bool,
    ) -> Result<SqlOutcome, SqlError> {
        if let Some(reason) = &self.poisoned {
            return Err(SqlError::TransactionAborted(format!(
                "transaction already aborted by earlier statement error: {reason}"
            )));
        }
        match self
            .tx
            .execute_stmt(stmt, params, self.limits, self.cache, force_full_scan)
        {
            Ok(outcome) => Ok(outcome),
            Err(err) => {
                self.poisoned = Some(err.to_string());
                Err(err)
            }
        }
    }
}

pub(crate) fn run_sql_transaction<T>(
    db: &DocumentDatabase<'_>,
    limits: &SqlLimits,
    cache: &SqlCache,
    action: impl FnOnce(&mut SqlTransaction<'_>) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    run_sql_transaction_until(db, limits, cache, None, action)
}

pub(super) fn run_sql_transaction_until<T>(
    db: &DocumentDatabase<'_>,
    limits: &SqlLimits,
    cache: &SqlCache,
    deadline: Option<Instant>,
    action: impl FnOnce(&mut SqlTransaction<'_>) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    let mut sql_error: Option<SqlError> = None;
    let outcome =
        db.transaction_detailed(DocumentTransactionMode::ReadSet, deadline, None, |doc_tx| {
            let mut sql_tx = SqlTransaction {
                tx: doc_tx,
                limits,
                cache,
                poisoned: None,
            };
            match action(&mut sql_tx) {
                Ok(value) => {
                    if let Some(reason) = sql_tx.poisoned.take() {
                        sql_error = Some(SqlError::TransactionAborted(format!(
                            "cannot commit after earlier statement error: {reason}"
                        )));
                        Err(io::Error::other("sql transaction poisoned"))
                    } else {
                        Ok(value)
                    }
                }
                Err(err) => {
                    sql_error = Some(err);
                    Err(io::Error::other("sql transaction aborted"))
                }
            }
        });
    exec::map_transaction_outcome(outcome, sql_error)
}

pub(crate) fn run_sql_transaction_with_retry<T>(
    db: &DocumentDatabase<'_>,
    limits: &SqlLimits,
    cache: &SqlCache,
    max_attempts: usize,
    mut action: impl FnMut(&mut SqlTransaction<'_>) -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    if max_attempts == 0 {
        return Err(SqlError::Bounds(
            "transaction_with_retry max_attempts must be at least 1".into(),
        ));
    }
    for attempt in 0..max_attempts {
        match run_sql_transaction(db, limits, cache, &mut action) {
            Ok(value) => return Ok(value),
            Err(SqlError::Conflict(err)) if attempt + 1 < max_attempts => {
                let _ = err;
                std::thread::yield_now();
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("loop always returns on final attempt")
}
