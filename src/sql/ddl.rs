//! `DROP TABLE [IF EXISTS] name`.
//!
//! A table small enough for one maintenance transaction is dropped
//! atomically: its catalog entry, rows and indexes disappear together. A
//! larger table first loses its catalog entry while the document collection
//! gets a durable incremental-deletion marker, in one transaction, so SQL no
//! longer sees the table. Bounded steps then remove the rows and indexes. If
//! a crash interrupts those steps, the next `DROP TABLE [IF EXISTS]` of the
//! same name finishes them; until then, creating a table of that name fails.
use super::catalog::{self, IndexState};
use super::exec::{self, SqlCache};
use super::lexer::{Token, TokenKind};
use super::value::{SqlError, SqlOutcome};
use crate::document::DocumentDatabase;
use std::io;

/// Keys removed per incremental step; the document layer's maximum.
const DROP_STEP_KEYS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DropTable {
    name: String,
    if_exists: bool,
}

/// Whether a tokenized statement starts with `DROP TABLE`.
pub(crate) fn is_drop_table(tokens: &[Token]) -> bool {
    let keyword = |index: usize, word: &str| {
        tokens.get(index).is_some_and(
            |token| matches!(&token.kind, TokenKind::Ident { text, quoted: false } if text == word),
        )
    };
    keyword(0, "drop") && keyword(1, "table")
}

pub(crate) fn parse_drop_table(tokens: &[Token]) -> Result<DropTable, SqlError> {
    let mut pos = 2;
    let keyword = |pos: usize, word: &str| matches!(&tokens[pos].kind, TokenKind::Ident { text, quoted: false } if text == word);
    let if_exists = keyword(pos, "if");
    if if_exists {
        if !keyword(pos + 1, "exists") {
            return Err(SqlError::Syntax {
                offset: tokens[pos + 1].offset,
                message: "expected EXISTS".into(),
            });
        }
        pos += 2;
    }
    let name = match &tokens[pos].kind {
        TokenKind::Ident { text, .. } => text.clone(),
        _ => {
            return Err(SqlError::Syntax {
                offset: tokens[pos].offset,
                message: "expected table name".into(),
            });
        }
    };
    pos += 1;
    if keyword(pos, "cascade") || keyword(pos, "restrict") {
        return Err(SqlError::Unsupported {
            feature: "DROP TABLE CASCADE or RESTRICT".into(),
            offset: Some(tokens[pos].offset),
        });
    }
    if matches!(tokens[pos].kind, TokenKind::Comma) {
        return Err(SqlError::Unsupported {
            feature: "dropping several tables in one statement".into(),
            offset: Some(tokens[pos].offset),
        });
    }
    if matches!(tokens[pos].kind, TokenKind::Semicolon) {
        pos += 1;
    }
    if !matches!(tokens[pos].kind, TokenKind::Eof) {
        return Err(SqlError::Syntax {
            offset: tokens[pos].offset,
            message: "multiple statements or trailing tokens are not supported".into(),
        });
    }
    Ok(DropTable { name, if_exists })
}

enum Plan {
    Done,
    /// The catalog entry is gone and the collection is marked for
    /// incremental deletion.
    Drive,
}

fn too_large(error: &SqlError) -> bool {
    const MESSAGE: &str = "collection drop exceeds maintenance limits";
    match error {
        SqlError::Io(error) => error.to_string().contains(MESSAGE),
        SqlError::Bounds(message) => message.contains(MESSAGE),
        _ => false,
    }
}

pub(crate) fn execute(
    db: &DocumentDatabase<'_>,
    statement: &DropTable,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let name = &statement.name;
    let first = exec::run_tx(db, |tx| {
        let table = match catalog::load_table_cached(tx, name, cache.tables()) {
            Ok(table) => table,
            Err(SqlError::NoSuchTable(_)) => {
                // A drop interrupted after its catalog entry was removed.
                if tx.collection_drop_in_progress(name).unwrap_or(false) {
                    return Ok(Plan::Drive);
                }
                if statement.if_exists {
                    return Ok(Plan::Done);
                }
                return Err(SqlError::NoSuchTable(name.clone()));
            }
            Err(error) => return Err(error),
        };
        if let Some(index) = table
            .def
            .indexes
            .iter()
            .find(|index| index.state != IndexState::Ready)
        {
            return Err(SqlError::Constraint(format!(
                "index '{}' on table '{name}' is still being built or dropped; \
                 run CREATE INDEX or DROP INDEX for it first",
                index.name
            )));
        }
        tx.drop_collection(name).map_err(exec::classify_io_error)?;
        catalog::remove_table(tx, name)?;
        Ok(Plan::Done)
    });
    let plan = match first {
        Ok(plan) => plan,
        Err(error) if too_large(&error) => exec::run_tx(db, |tx| {
            // Re-check inside the new transaction; another statement may
            // have changed the table.
            catalog::load_table_cached(tx, name, cache.tables())?;
            catalog::remove_table(tx, name)?;
            tx.start_collection_drop(name)
                .map_err(exec::classify_io_error)?;
            Ok(Plan::Drive)
        })?,
        Err(error) => return Err(error),
    };
    if let Plan::Drive = plan {
        // A step can remove nothing while it verifies that namespaces are
        // empty, so only a run of such steps means no progress.
        let mut idle = 0;
        loop {
            let step = exec::run_tx(db, |tx| {
                tx.advance_collection_drop(name, DROP_STEP_KEYS)
                    .map_err(exec::classify_io_error)
            })?;
            if step.complete {
                break;
            }
            idle = if step.removed == 0 { idle + 1 } else { 0 };
            if idle > 16 {
                return Err(SqlError::Io(io::Error::other(format!(
                    "incremental drop of table '{name}' made no progress"
                ))));
            }
        }
    }
    Ok(SqlOutcome::TableDropped)
}
