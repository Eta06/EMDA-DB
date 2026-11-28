//! Composite, statement-end foreign key enforcement using complete typed tuples.
use super::catalog::{self, IndexDef, IndexState, TableCache, TableDef};
use super::exec::{SqlValueRef, append_compound_key, classify_io_error};
use super::parser::TupleForeignKeySpec;
use super::value::{MAX_SQL_IDENTIFIER_BYTES, MAX_SQL_INDEX_COLUMNS, SqlError, SqlLimits, SqlType};
use crate::document::{DocumentTransaction, IndexQuery, IndexValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Constraint {
    pub(crate) columns: Vec<usize>,
    pub(crate) ref_table: String,
    pub(crate) ref_columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    pub(crate) child_table: String,
    pub(crate) child_columns: Vec<String>,
    pub(crate) ref_columns: Vec<String>,
}

fn invalid(message: &str) -> SqlError {
    classify_io_error(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn columns(table: &TableDef, names: &[String]) -> Result<Vec<usize>, SqlError> {
    names
        .iter()
        .map(|name| {
            table
                .column_index(name)
                .ok_or_else(|| SqlError::NoSuchColumn {
                    table: table.name.clone(),
                    column: name.clone(),
                })
        })
        .collect()
}

fn tuple_columns_valid(names: &[String]) -> bool {
    (2..=MAX_SQL_INDEX_COLUMNS).contains(&names.len())
        && names
            .iter()
            .all(|name| !name.is_empty() && name.len() <= MAX_SQL_IDENTIFIER_BYTES)
        && names.iter().collect::<HashSet<_>>().len() == names.len()
}

pub(super) fn validate(table: &TableDef) -> Result<(), SqlError> {
    for fk in &table.tuple_foreign_keys {
        if fk.columns.len() != fk.ref_columns.len()
            || !tuple_columns_valid(&fk.ref_columns)
            || fk.ref_table.is_empty()
            || fk.ref_table.len() > MAX_SQL_IDENTIFIER_BYTES
            || fk.columns.iter().collect::<HashSet<_>>().len() != fk.columns.len()
            || fk.columns.iter().any(|&col| {
                table
                    .columns
                    .get(col)
                    .is_none_or(|col| col.col_type == SqlType::Real)
            })
        {
            return Err(invalid("invalid composite foreign key catalog entry"));
        }
    }
    for reference in &table.tuple_referenced_by {
        if reference.child_columns.len() != reference.ref_columns.len()
            || reference.child_table.is_empty()
            || reference.child_table.len() > MAX_SQL_IDENTIFIER_BYTES
            || !tuple_columns_valid(&reference.child_columns)
            || !tuple_columns_valid(&reference.ref_columns)
            || reference
                .ref_columns
                .iter()
                .any(|column| table.column_index(column).is_none())
        {
            return Err(invalid("invalid composite foreign key back-reference"));
        }
    }
    Ok(())
}

fn matches_pk(table: &TableDef, indices: &[usize]) -> bool {
    table.primary_key_columns().eq(indices.iter().copied())
}

fn backing_index<'a>(
    table: &'a TableDef,
    indices: &[usize],
    except: Option<&str>,
) -> Option<&'a IndexDef> {
    table.indexes.iter().find(|index| {
        index.unique
            && index.columns == indices
            && index.state == IndexState::Ready
            && except.is_none_or(|name| index.name != name)
    })
}

pub(super) fn attach(
    tx: &mut DocumentTransaction<'_, '_>,
    child_name: &str,
    specs: &[TupleForeignKeySpec],
    cache: &TableCache,
) -> Result<(), SqlError> {
    for spec in specs {
        let current = catalog::load_table_cached(tx, child_name, cache)?;
        let mut child = TableDef::clone(&current.def);
        let parent = catalog::load_table_cached(tx, &spec.ref_table, cache)?;
        let target = if spec.ref_columns.is_empty() {
            parent.def.primary_key_columns().collect::<Vec<_>>()
        } else {
            columns(&parent.def, &spec.ref_columns)?
        };
        if spec.columns.len() != target.len() {
            return Err(SqlError::Constraint(
                "foreign key column counts differ".into(),
            ));
        }
        if !matches_pk(&parent.def, &target) && backing_index(&parent.def, &target, None).is_none()
        {
            return Err(SqlError::Constraint(format!(
                "composite foreign key requires a matching PRIMARY KEY or UNIQUE index on '{}'",
                parent.def.name
            )));
        }
        for (&from, &to) in spec.columns.iter().zip(&target) {
            let source = child.columns[from].col_type;
            let target = parent.def.columns[to].col_type;
            if source == SqlType::Real || !source.matches_base(target) {
                return Err(SqlError::Type(
                    "composite foreign key column types must match and be indexable".into(),
                ));
            }
        }
        let ref_columns = target
            .iter()
            .map(|&index| parent.def.columns[index].name.clone())
            .collect::<Vec<_>>();
        let constraint = Constraint {
            columns: spec.columns.clone(),
            ref_table: spec.ref_table.clone(),
            ref_columns: ref_columns.clone(),
        };
        if child.tuple_foreign_keys.contains(&constraint) {
            continue;
        }
        child.tuple_foreign_keys.push(constraint);
        let reference = Reference {
            child_table: child_name.to_owned(),
            child_columns: spec
                .columns
                .iter()
                .map(|&index| child.columns[index].name.clone())
                .collect(),
            ref_columns,
        };
        if child_name == parent.def.name {
            child.tuple_referenced_by.push(reference);
        } else {
            let mut other = TableDef::clone(&parent.def);
            other.tuple_referenced_by.push(reference);
            other.validate()?;
            catalog::save_table_def(tx, &parent.catalog_key, &other)?;
        }
        child.validate()?;
        catalog::save_table_def(tx, &current.catalog_key, &child)?;
    }
    Ok(())
}

pub(super) fn ensure_index_not_target(table: &TableDef, index: &IndexDef) -> Result<(), SqlError> {
    if !index.unique || index.columns.len() < 2 {
        return Ok(());
    }
    for reference in &table.tuple_referenced_by {
        let target = columns(table, &reference.ref_columns)?;
        if index.columns == target
            && !matches_pk(table, &target)
            && backing_index(table, &target, Some(&index.name)).is_none()
        {
            return Err(SqlError::Constraint(format!(
                "index '{}' backs a composite foreign key referencing '{}'",
                index.name, table.name
            )));
        }
    }
    Ok(())
}

fn extract(
    table: &TableDef,
    indices: &[usize],
    obj: &Map<String, Value>,
) -> Result<Option<Vec<IndexValue>>, SqlError> {
    let mut values = Vec::with_capacity(indices.len());
    for &index in indices {
        let column = &table.columns[index];
        let Some(value) = obj.get(&column.name).filter(|value| !value.is_null()) else {
            return Ok(None);
        };
        let value = match column.col_type {
            SqlType::Integer => value.as_i64().map(IndexValue::Integer),
            SqlType::Text { .. } => value
                .as_str()
                .map(|text| IndexValue::String(text.to_owned())),
            SqlType::Boolean => value.as_bool().map(IndexValue::Boolean),
            SqlType::Real => None,
        }
        .ok_or_else(|| invalid("stored composite foreign key value has the wrong type"))?;
        values.push(value);
    }
    Ok(Some(values))
}

fn index_has_tuple(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    index: &IndexDef,
    values: &[IndexValue],
) -> Result<bool, SqlError> {
    if values.iter().any(|value| matches!(value, IndexValue::String(text) if text.len() > super::value::MAX_SQL_INDEX_TEXT_BYTES)) {
        return Ok(false);
    }
    let page = tx
        .query_index(
            &table.name,
            &index.name,
            &IndexQuery {
                equal: values.to_vec(),
                lower: None,
                upper: None,
                after: None,
                limit: 1,
            },
        )
        .map_err(classify_io_error)?;
    Ok(!page.documents.is_empty())
}

fn parent_has_tuple(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    indices: &[usize],
    values: &[IndexValue],
) -> Result<bool, SqlError> {
    if matches_pk(table, indices) {
        let mut id = String::from("c:");
        for value in values {
            let value = match value {
                IndexValue::Integer(n) => SqlValueRef::Integer(*n),
                IndexValue::String(text) => SqlValueRef::Text(text),
                _ => return Err(invalid("invalid composite primary key value")),
            };
            if append_compound_key(&mut id, value).is_err() {
                return Ok(false);
            }
        }
        let row = tx.get(&table.name, &id).map_err(classify_io_error)?;
        if let Some(row) = row {
            let obj = row
                .as_object()
                .ok_or_else(|| invalid("stored row is not an object"))?;
            super::exec::verify_doc_pk_id(table, &id, obj)?;
            return Ok(true);
        }
        return Ok(false);
    }
    let index = backing_index(table, indices, None)
        .ok_or_else(|| invalid("referenced composite key is missing its unique index"))?;
    index_has_tuple(tx, table, index, values)
}

pub(super) fn check_children(
    tx: &DocumentTransaction<'_, '_>,
    child: &TableDef,
    rows: &[&Value],
    cache: &TableCache,
) -> Result<(), SqlError> {
    for constraint in &child.tuple_foreign_keys {
        let mut tuples = HashSet::new();
        for row in rows {
            let obj = row
                .as_object()
                .ok_or_else(|| invalid("stored row is not an object"))?;
            if let Some(tuple) = extract(child, &constraint.columns, obj)? {
                tuples.insert(tuple);
            }
        }
        if tuples.is_empty() {
            continue;
        }
        let parent = catalog::load_table_cached(tx, &constraint.ref_table, cache)?;
        let indices = columns(&parent.def, &constraint.ref_columns)?;
        for tuple in tuples {
            if !parent_has_tuple(tx, &parent.def, &indices, &tuple)? {
                return Err(SqlError::Constraint(format!(
                    "composite FOREIGN KEY constraint failed on '{}' referencing '{}'",
                    child.name, parent.def.name
                )));
            }
        }
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct Removals {
    entries: HashSet<(String, Reference, Vec<IndexValue>)>,
}

impl Removals {
    pub(super) fn record(
        &mut self,
        parent: &TableDef,
        row: &Map<String, Value>,
    ) -> Result<(), SqlError> {
        for reference in &parent.tuple_referenced_by {
            let indices = columns(parent, &reference.ref_columns)?;
            if let Some(tuple) = extract(parent, &indices, row)? {
                self.entries
                    .insert((parent.name.clone(), reference.clone(), tuple));
            }
        }
        Ok(())
    }

    pub(super) fn append(&mut self, other: Self) {
        self.entries.extend(other.entries);
    }

    pub(super) fn verify(
        self,
        tx: &DocumentTransaction<'_, '_>,
        limits: &SqlLimits,
        cache: &TableCache,
        visited: &mut usize,
    ) -> Result<(), SqlError> {
        for (parent, reference, tuple) in self.entries {
            let parent = catalog::load_table_cached(tx, &parent, cache)?;
            let target = columns(&parent.def, &reference.ref_columns)?;
            if parent_has_tuple(tx, &parent.def, &target, &tuple)? {
                continue;
            }
            let child = catalog::load_table_cached(tx, &reference.child_table, cache)?;
            let from = columns(&child.def, &reference.child_columns)?;
            if child_has_tuple(tx, &child.def, &from, &tuple, limits, visited)? {
                return Err(SqlError::Constraint(format!(
                    "composite FOREIGN KEY on '{}' still references '{}'",
                    child.def.name, parent.def.name
                )));
            }
        }
        Ok(())
    }
}

fn child_has_tuple(
    tx: &DocumentTransaction<'_, '_>,
    child: &TableDef,
    cols: &[usize],
    tuple: &[IndexValue],
    limits: &SqlLimits,
    visited: &mut usize,
) -> Result<bool, SqlError> {
    if matches_pk(child, cols) {
        return parent_has_tuple(tx, child, cols, tuple);
    }
    if let Some(index) = child.indexes.iter().find(|index| {
        index.state == IndexState::Ready
            && index.columns.starts_with(cols)
            && index.columns[cols.len()..]
                .iter()
                .all(|&trailing| child.columns[trailing].not_null)
    }) {
        return index_has_tuple(tx, child, index, tuple);
    }
    let mut after = None;
    loop {
        let limit =
            (limits.max_visited_rows.saturating_sub(*visited) + 1).min(crate::MAX_SCAN_ENTRIES);
        let page = tx
            .scan_documents(&child.name, after.as_deref(), limit)
            .map_err(classify_io_error)?;
        if page.is_empty() {
            return Ok(false);
        }
        for entry in &page {
            *visited += 1;
            if *visited > limits.max_visited_rows {
                return Err(SqlError::Bounds(
                    "composite foreign key scan exceeds max_visited_rows".into(),
                ));
            }
            let row = entry
                .document
                .as_object()
                .ok_or_else(|| invalid("stored row is not an object"))?;
            if extract(child, cols, row)?.as_deref() == Some(tuple) {
                return Ok(true);
            }
        }
        after = page.last().map(|entry| entry.id.clone());
    }
}
