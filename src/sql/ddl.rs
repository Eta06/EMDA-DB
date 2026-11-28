//! `DROP TABLE [IF EXISTS] name` and bounded column migrations.
//!
//! A table small enough for one maintenance transaction is dropped
//! atomically: its catalog entry, rows and indexes disappear together. A
//! larger table first loses its catalog entry while the document collection
//! gets a durable incremental-deletion marker, in one transaction, so SQL no
//! longer sees the table. Bounded steps then remove the rows and indexes. If
//! a crash interrupts those steps, the next `DROP TABLE [IF EXISTS]` of the
//! same name finishes them; until then, creating a table of that name fails.
use super::catalog::{self, ColumnDef, IndexState};
use super::exec::{self, SqlCache};
use super::foreign_key;
use super::lexer::{Token, TokenKind};
use super::value::{SqlError, SqlLimits, SqlOutcome, SqlType};
use crate::document::{DocumentDatabase, DocumentTransaction};
use serde_json::Value;
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
    let keyword = |pos: usize, word: &str| matches!(&at(tokens, pos).kind, TokenKind::Ident { text, quoted: false } if text == word);
    let if_exists = keyword(pos, "if");
    if if_exists {
        if !keyword(pos + 1, "exists") {
            return Err(SqlError::Syntax {
                offset: at(tokens, pos + 1).offset,
                message: "expected EXISTS".into(),
            });
        }
        pos += 2;
    }
    let name = match &at(tokens, pos).kind {
        TokenKind::Ident { text, .. } => text.clone(),
        _ => {
            return Err(SqlError::Syntax {
                offset: at(tokens, pos).offset,
                message: "expected table name".into(),
            });
        }
    };
    pos += 1;
    if keyword(pos, "cascade") || keyword(pos, "restrict") {
        return Err(SqlError::Unsupported {
            feature: "DROP TABLE CASCADE or RESTRICT".into(),
            offset: Some(at(tokens, pos).offset),
        });
    }
    if matches!(at(tokens, pos).kind, TokenKind::Comma) {
        return Err(SqlError::Unsupported {
            feature: "dropping several tables in one statement".into(),
            offset: Some(at(tokens, pos).offset),
        });
    }
    if matches!(at(tokens, pos).kind, TokenKind::Semicolon) {
        pos += 1;
    }
    if !matches!(at(tokens, pos).kind, TokenKind::Eof) {
        return Err(SqlError::Syntax {
            offset: at(tokens, pos).offset,
            message: "multiple statements or trailing tokens are not supported".into(),
        });
    }
    Ok(DropTable { name, if_exists })
}

/// The token at `pos`, or the final end-of-input token past the end, so
/// truncated statements are syntax errors rather than out-of-bounds reads.
fn at(tokens: &[Token], pos: usize) -> &Token {
    &tokens[pos.min(tokens.len() - 1)]
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

fn check_droppable(table: &catalog::TableDef, name: &str) -> Result<(), SqlError> {
    if let Some(index) = table
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
    foreign_key::ensure_table_droppable(table)
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
        check_droppable(&table.def, name)?;
        tx.drop_collection(name).map_err(exec::classify_io_error)?;
        foreign_key::detach_parent_references(tx, &table.def, cache.tables())?;
        catalog::remove_table(tx, name)?;
        Ok(Plan::Done)
    });
    let plan = match first {
        Ok(plan) => plan,
        Err(error) if too_large(&error) => exec::run_tx(db, |tx| {
            // Re-check inside the new transaction; another statement may
            // have changed the table.
            let table = catalog::load_table_cached(tx, name, cache.tables())?;
            check_droppable(&table.def, name)?;
            foreign_key::detach_parent_references(tx, &table.def, cache.tables())?;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AlterTable {
    table: String,
    action: AlterAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AlterAction {
    Column { name: String, change: ColumnAction },
    RenameTable(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ColumnAction {
    Add {
        column_type: SqlType,
        not_null: bool,
    },
    Drop,
    Rename(String),
}

/// Whether a tokenized statement starts with `ALTER TABLE`.
pub(crate) fn is_alter_table(tokens: &[Token]) -> bool {
    let keyword = |index: usize, word: &str| {
        tokens.get(index).is_some_and(
            |token| matches!(&token.kind, TokenKind::Ident { text, quoted: false } if text == word),
        )
    };
    keyword(0, "alter") && keyword(1, "table")
}

/// Parses one table or column migration.
pub(crate) fn parse_alter_table(
    tokens: &[Token],
    limits: &SqlLimits,
) -> Result<AlterTable, SqlError> {
    let kw = |pos: usize, word: &str| matches!(&at(tokens, pos).kind, TokenKind::Ident { text, quoted: false } if text == word);
    let syntax = |pos: usize, message: &str| SqlError::Syntax {
        offset: at(tokens, pos).offset,
        message: message.into(),
    };
    let unsupported = |pos: usize, feature: &str| SqlError::Unsupported {
        feature: feature.into(),
        offset: Some(at(tokens, pos).offset),
    };
    let ident = |pos: usize, what: &str| match &at(tokens, pos).kind {
        TokenKind::Ident { text, .. } => Ok(text.clone()),
        _ => Err(syntax(pos, &format!("expected {what}"))),
    };
    let mut pos = 2;
    let table = ident(pos, "table name")?;
    pos += 1;
    if kw(pos, "rename") && kw(pos + 1, "to") {
        let name = ident(pos + 2, "new table name")?;
        finish_alter(tokens, pos + 3)?;
        return Ok(AlterTable {
            table,
            action: AlterAction::RenameTable(name),
        });
    }
    if kw(pos, "drop") && kw(pos + 1, "constraint") {
        return Err(unsupported(pos, "ALTER TABLE ... DROP CONSTRAINT"));
    }
    if kw(pos, "drop") || (kw(pos, "rename") && kw(pos + 1, "column")) {
        let rename = kw(pos, "rename");
        pos += 1 + usize::from(kw(pos + 1, "column"));
        let column = ident(pos, "column name")?;
        pos += 1;
        let action = if rename {
            if !kw(pos, "to") {
                return Err(syntax(pos, "expected TO"));
            }
            let name = ident(pos + 1, "new column name")?;
            pos += 2;
            ColumnAction::Rename(name)
        } else {
            ColumnAction::Drop
        };
        finish_alter(tokens, pos)?;
        return Ok(AlterTable {
            table,
            action: AlterAction::Column {
                name: column,
                change: action,
            },
        });
    }
    if !kw(pos, "add") {
        return Err(match &at(tokens, pos).kind {
            TokenKind::Ident {
                text,
                quoted: false,
            } => unsupported(
                pos,
                &format!("ALTER TABLE ... {}", text.to_ascii_uppercase()),
            ),
            _ => syntax(pos, "expected ADD, DROP, or RENAME COLUMN"),
        });
    }
    pos += 1;
    if kw(pos, "constraint") || kw(pos, "primary") || kw(pos, "unique") || kw(pos, "foreign") {
        return Err(unsupported(pos, "ALTER TABLE ... ADD CONSTRAINT"));
    }
    if kw(pos, "column") {
        pos += 1;
    }
    let column = ident(pos, "column name")?;
    pos += 1;
    let type_name = match &at(tokens, pos).kind {
        TokenKind::Ident {
            text,
            quoted: false,
        } => text.clone(),
        _ => return Err(syntax(pos, "expected column data type")),
    };
    let column_type = match type_name.as_str() {
        "integer" | "int" | "bigint" => SqlType::Integer,
        "real" | "double" | "float" => SqlType::Real,
        "boolean" | "bool" => SqlType::Boolean,
        "text" => SqlType::Text { max_chars: None },
        "varchar" => {
            if !matches!(at(tokens, pos + 1).kind, TokenKind::LParen) {
                SqlType::Text { max_chars: None }
            } else {
                let n = match &at(tokens, pos + 2).kind {
                    TokenKind::Integer(raw) => raw
                        .parse::<usize>()
                        .map_err(|_| SqlError::Bounds("VARCHAR length out of range".into()))?,
                    _ => {
                        return Err(syntax(
                            pos + 2,
                            "expected positive integer length in VARCHAR(n)",
                        ));
                    }
                };
                if n == 0 {
                    return Err(syntax(pos + 2, "VARCHAR(n) length must be at least 1"));
                }
                if n > limits.max_text_bytes {
                    return Err(SqlError::Bounds(format!(
                        "VARCHAR({n}) exceeds max_text_bytes ({})",
                        limits.max_text_bytes
                    )));
                }
                if !matches!(at(tokens, pos + 3).kind, TokenKind::RParen) {
                    return Err(syntax(pos + 3, "expected ')'"));
                }
                pos += 3;
                SqlType::Text { max_chars: Some(n) }
            }
        }
        _ => {
            return Err(syntax(
                pos,
                &format!("unknown column data type '{type_name}'"),
            ));
        }
    };
    pos += 1;
    let mut not_null = false;
    loop {
        if kw(pos, "not") && kw(pos + 1, "null") {
            not_null = true;
            pos += 2;
        } else if kw(pos, "null") {
            pos += 1;
        } else if kw(pos, "primary") || kw(pos, "unique") {
            return Err(unsupported(
                pos,
                "PRIMARY KEY or UNIQUE in ALTER TABLE ... ADD COLUMN",
            ));
        } else if kw(pos, "default") || kw(pos, "check") || kw(pos, "references") {
            return Err(unsupported(pos, "DEFAULT, CHECK or REFERENCES"));
        } else {
            break;
        }
    }
    finish_alter(tokens, pos)?;
    Ok(AlterTable {
        table,
        action: AlterAction::Column {
            name: column,
            change: ColumnAction::Add {
                column_type,
                not_null,
            },
        },
    })
}

fn finish_alter(tokens: &[Token], mut pos: usize) -> Result<(), SqlError> {
    if matches!(at(tokens, pos).kind, TokenKind::Comma) {
        return Err(SqlError::Unsupported {
            feature: "several ALTER TABLE actions in one statement".into(),
            offset: Some(at(tokens, pos).offset),
        });
    }
    if matches!(at(tokens, pos).kind, TokenKind::Semicolon) {
        pos += 1;
    }
    if !matches!(at(tokens, pos).kind, TokenKind::Eof) {
        return Err(SqlError::Syntax {
            offset: at(tokens, pos).offset,
            message: "multiple statements or trailing tokens are not supported".into(),
        });
    }
    Ok(())
}

/// Applies one table or column migration inside the caller's transaction.
pub(crate) fn alter_in_tx(
    tx: &mut DocumentTransaction<'_, '_>,
    statement: &AlterTable,
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let table = catalog::load_table_cached(tx, &statement.table, cache.tables())?;
    let (column_name, change) = match &statement.action {
        AlterAction::Column { name, change } => (name, change),
        AlterAction::RenameTable(name) => return rename_table(tx, &table.def, name, limits, cache),
    };
    let mut updated = catalog::TableDef::clone(&table.def);
    if let ColumnAction::Add {
        column_type,
        not_null,
    } = *change
    {
        if table.def.column_index(column_name).is_some() {
            return Err(SqlError::Syntax {
                offset: 0,
                message: format!(
                    "column '{}' already exists in table '{}'",
                    column_name, statement.table
                ),
            });
        }
        if table.def.columns.len() >= limits.max_columns {
            return Err(SqlError::Bounds(format!(
                "table '{}' already has max_columns ({}) columns",
                statement.table, limits.max_columns
            )));
        }
        if not_null
            && !tx
                .scan_documents(&statement.table, None, 1)
                .map_err(exec::classify_io_error)?
                .is_empty()
        {
            return Err(SqlError::Constraint(format!(
                "cannot add NOT NULL column '{}' to non-empty table '{}': existing rows would be NULL",
                column_name, statement.table
            )));
        }
        updated.columns.push(ColumnDef {
            name: column_name.clone(),
            col_type: column_type,
            not_null,
            default: None,
        });
    } else {
        let rename = match change {
            ColumnAction::Rename(name) => Some(name.as_str()),
            _ => None,
        };
        let column = column_name;
        let index = updated
            .column_index(column)
            .ok_or_else(|| SqlError::NoSuchColumn {
                table: updated.name.clone(),
                column: column.clone(),
            })?;
        if updated.primary_key_columns().any(|key| index == key)
            || updated
                .indexes
                .iter()
                .any(|def| def.columns.contains(&index))
            || updated.foreign_keys.iter().any(|def| def.column == index)
            || updated
                .referenced_by
                .iter()
                .any(|reference| reference.ref_column == *column)
            || updated
                .tuple_foreign_keys
                .iter()
                .any(|constraint| constraint.columns.contains(&index))
            || updated
                .tuple_referenced_by
                .iter()
                .any(|reference| reference.ref_columns.contains(column))
        {
            return Err(SqlError::Constraint(format!(
                "cannot alter column '{}.{column}' because a key or index depends on it",
                updated.name
            )));
        }
        if let Some(name) = rename {
            if updated.column_index(name).is_some() {
                return Err(SqlError::Constraint(format!(
                    "column '{name}' already exists in table '{}'",
                    updated.name
                )));
            }
            updated.columns[index].name = name.to_owned();
        } else {
            updated.columns.remove(index);
            let shift = |stored: &mut usize| *stored -= usize::from(*stored > index);
            shift(&mut updated.primary_key_index);
            updated
                .additional_primary_key_columns
                .iter_mut()
                .for_each(shift);
            for def in &mut updated.indexes {
                def.columns.iter_mut().for_each(shift);
            }
            for def in &mut updated.foreign_keys {
                shift(&mut def.column);
            }
            for def in &mut updated.tuple_foreign_keys {
                def.columns.iter_mut().for_each(shift);
            }
        }
        if let Err(error) = exec::bind_table_checks(&updated, limits) {
            return Err(match error {
                SqlError::NoSuchColumn { .. } => SqlError::Constraint(format!(
                    "cannot alter column '{}.{column}' because a CHECK depends on it",
                    updated.name
                )),
                other => other,
            });
        }
        updated.validate()?;
        rewrite_column(tx, &table.def, &updated, index, rename, limits)?;
    }
    updated.validate()?;
    catalog::save_table_def(tx, &table.catalog_key, &updated)?;
    Ok(SqlOutcome::TableAltered)
}

fn rewrite_column(
    tx: &mut DocumentTransaction<'_, '_>,
    old: &catalog::TableDef,
    updated: &catalog::TableDef,
    index: usize,
    rename: Option<&str>,
    limits: &SqlLimits,
) -> Result<(), SqlError> {
    let column = &old.columns[index];
    let final_schema = updated.to_collection_schema();
    let mut schema = final_schema.clone();
    if let Some(name) = rename {
        schema.fields.retain(|rule| rule.path[0] != name);
    }
    // Both row shapes can be read while the change is being staged. The
    // document schema and SQL catalog commit atomically with the row changes.
    tx.replace_schema_for_sql_migration(&old.name, schema)
        .map_err(exec::classify_io_error)?;
    let (mut changed, mut visited, mut after) = (0, 0, None);
    loop {
        let page_size = (limits.max_visited_rows - visited + 1).min(128);
        let page = tx
            .scan_documents(&old.name, after.as_deref(), page_size)
            .map_err(exec::classify_io_error)?;
        let Some(last) = page.last() else { break };
        after = Some(last.id.clone());
        visited += page.len();
        if visited > limits.max_visited_rows {
            return Err(SqlError::Bounds(
                "column migration exceeds max_visited_rows".into(),
            ));
        }
        for entry in page {
            let Value::Object(mut obj) = entry.document else {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stored row is not an object",
                )));
            };
            exec::verify_doc_pk_id(old, &entry.id, &obj)?;
            if rename.is_some_and(|name| obj.contains_key(name)) {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stored row already contains the new column",
                )));
            }
            let Some(value) = obj.remove(&column.name) else {
                if column.not_null {
                    return Err(SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "stored row is missing a required column",
                    )));
                }
                continue;
            };
            if let Some(name) = rename {
                obj.insert(name.to_owned(), value);
            }
            changed += 1;
            if changed > limits.max_mutation_rows {
                return Err(SqlError::Bounds(
                    "column migration exceeds max_mutation_rows".into(),
                ));
            }
            let document = Value::Object(obj);
            if rename.is_some() {
                final_schema
                    .validate_document(&document)
                    .map_err(exec::classify_io_error)?;
            }
            tx.replace(&old.name, &entry.id, &document, false)
                .map_err(|error| exec::map_write_constraint_error(error, &old.name))?;
        }
    }
    if rename.is_some() {
        tx.replace_schema_for_sql_migration(&old.name, final_schema)
            .map_err(exec::classify_io_error)?;
    }
    Ok(())
}

/// Copies an admitted table to its new collection and publishes the change
/// with all foreign key references in one durable transaction.
fn rename_table(
    tx: &mut DocumentTransaction<'_, '_>,
    old: &catalog::TableDef,
    new_name: &str,
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    if old
        .indexes
        .iter()
        .any(|index| index.state != IndexState::Ready)
    {
        return Err(SqlError::Constraint(
            "cannot rename a table while index maintenance is active".into(),
        ));
    }
    let mut renamed = old.clone();
    renamed.name = new_name.to_owned();
    rewrite_related_names(&mut renamed, &old.name, new_name);
    catalog::create_table(tx, &renamed, false)?;

    let row_limit = limits.max_mutation_rows.min(limits.max_visited_rows);
    let (mut count, mut after) = (0, None);
    loop {
        let page = tx
            .scan_documents(
                &old.name,
                after.as_deref(),
                (row_limit - count + 1).min(128),
            )
            .map_err(exec::classify_io_error)?;
        let Some(last) = page.last() else { break };
        after = Some(last.id.clone());
        count += page.len();
        if count > row_limit {
            return Err(SqlError::Bounds(format!(
                "table rename exceeds its {row_limit}-row migration bound"
            )));
        }
        for entry in page {
            let obj = entry.document.as_object().ok_or_else(|| {
                SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stored row is not an object",
                ))
            })?;
            exec::verify_doc_pk_id(old, &entry.id, obj)?;
            tx.insert(new_name, &entry.id, &entry.document)
                .map_err(|error| exec::map_write_constraint_error(error, new_name))?;
        }
    }

    let related = old
        .foreign_keys
        .iter()
        .map(|fk| fk.ref_table.as_str())
        .chain(old.referenced_by.iter().map(|fk| fk.child_table.as_str()))
        .chain(
            old.tuple_foreign_keys
                .iter()
                .map(|fk| fk.ref_table.as_str()),
        )
        .chain(
            old.tuple_referenced_by
                .iter()
                .map(|fk| fk.child_table.as_str()),
        )
        .filter(|name| *name != old.name)
        .collect::<std::collections::BTreeSet<_>>();
    for related in related {
        let cached = catalog::load_table_cached(tx, related, cache.tables())?;
        let mut table = (*cached.def).clone();
        rewrite_related_names(&mut table, &old.name, new_name);
        table.validate()?;
        catalog::save_table_def(tx, &cached.catalog_key, &table)?;
    }
    tx.drop_collection(&old.name)
        .map_err(exec::classify_io_error)?;
    catalog::remove_table(tx, &old.name)?;
    Ok(SqlOutcome::TableAltered)
}

fn rewrite_related_names(table: &mut catalog::TableDef, old: &str, new: &str) {
    for name in table
        .foreign_keys
        .iter_mut()
        .map(|fk| &mut fk.ref_table)
        .chain(table.referenced_by.iter_mut().map(|fk| &mut fk.child_table))
        .chain(
            table
                .tuple_foreign_keys
                .iter_mut()
                .map(|fk| &mut fk.ref_table),
        )
        .chain(
            table
                .tuple_referenced_by
                .iter_mut()
                .map(|fk| &mut fk.child_table),
        )
    {
        if name == old {
            new.clone_into(name);
        }
    }
}
