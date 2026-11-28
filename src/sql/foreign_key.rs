//! Foreign key DDL validation, catalog back-reference maintenance, and runtime enforcement.
use super::catalog::{
    self, CachedTable, ForeignKeyDef, ForeignKeyRef, IndexDef, IndexState, TableCache, TableDef,
};
use super::exec::classify_io_error;
use super::parser::ForeignKeySpec;
use super::tuple_foreign_key::{self as tuple, Removals};
use super::value::{
    MAX_SQL_INDEX_TEXT_BYTES, MAX_SQL_TEXT_PK_BYTES, SqlError, SqlLimits, SqlType, SqlValue,
    coerce_for_column, encode_i64_pk_bytes,
};
use crate::MAX_SCAN_ENTRIES;
use crate::document::flat::FlatScalar;
use crate::document::{DocumentTransaction, IndexQuery, IndexValue, StoredDocument};
use crate::sql::catalog::ForeignKeyAction;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum FkKey {
    Integer(i64),
    Text(String),
    Boolean(bool),
}

enum FkKeySet {
    Integer(HashSet<i64>),
    Text(HashSet<String>),
    Boolean(HashSet<bool>),
}

impl FkKeySet {
    fn new(col_type: SqlType) -> Result<Self, SqlError> {
        match col_type {
            SqlType::Integer => Ok(Self::Integer(HashSet::new())),
            SqlType::Text { .. } => Ok(Self::Text(HashSet::new())),
            SqlType::Boolean => Ok(Self::Boolean(HashSet::new())),
            SqlType::Real => Err(SqlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "foreign key column cannot have type REAL",
            ))),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Integer(s) => s.is_empty(),
            Self::Text(s) => s.is_empty(),
            Self::Boolean(s) => s.is_empty(),
        }
    }

    fn insert(&mut self, key: FkKey) -> Result<(), SqlError> {
        match (self, key) {
            (Self::Integer(set), FkKey::Integer(n)) => {
                set.insert(n);
                Ok(())
            }
            (Self::Text(set), FkKey::Text(t)) => {
                set.insert(t);
                Ok(())
            }
            (Self::Boolean(set), FkKey::Boolean(b)) => {
                set.insert(b);
                Ok(())
            }
            _ => Err(SqlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "mismatched foreign key value type",
            ))),
        }
    }

    fn to_vec(&self) -> Vec<FkKey> {
        match self {
            Self::Integer(set) => set.iter().copied().map(FkKey::Integer).collect(),
            Self::Text(set) => set.iter().cloned().map(FkKey::Text).collect(),
            Self::Boolean(set) => set.iter().copied().map(FkKey::Boolean).collect(),
        }
    }
}

pub(crate) struct PreparedCreateTableFks {
    pub(crate) foreign_keys: Vec<ForeignKeyDef>,
    pub(crate) self_referenced_by: Vec<ForeignKeyRef>,
    pub(crate) parent_updates: Vec<(Vec<u8>, TableDef)>,
}

/// Validates declared foreign keys for a new table and prepares both the child's
/// resolved `ForeignKeyDef`s and the parent `TableDef` back-reference updates.
pub(crate) fn prepare_create_table_foreign_keys(
    tx: &DocumentTransaction<'_, '_>,
    child_name: &str,
    child_columns: &[catalog::ColumnDef],
    child_pk_index: Option<usize>,
    child_indexes: &[IndexDef],
    specs: &[ForeignKeySpec],
    cache: &TableCache,
) -> Result<PreparedCreateTableFks, SqlError> {
    if specs.is_empty() {
        return Ok(PreparedCreateTableFks {
            foreign_keys: Vec::new(),
            self_referenced_by: Vec::new(),
            parent_updates: Vec::new(),
        });
    }

    let mut foreign_keys: Vec<ForeignKeyDef> = Vec::with_capacity(specs.len());
    let mut self_referenced_by: Vec<ForeignKeyRef> = Vec::new();
    let mut parent_map: HashMap<String, (Vec<u8>, TableDef)> = HashMap::new();

    for spec in specs {
        let child_col = &child_columns[spec.column];
        if spec.on_delete == ForeignKeyAction::SetNull && child_col.not_null {
            return Err(SqlError::Constraint(format!(
                "ON DELETE SET NULL is not allowed on NOT NULL column '{child_name}.{}'",
                child_col.name
            )));
        }
        if spec.on_update == ForeignKeyAction::SetNull && child_col.not_null {
            return Err(SqlError::Constraint(format!(
                "ON UPDATE SET NULL is not allowed on NOT NULL column '{child_name}.{}'",
                child_col.name
            )));
        }
        if spec.ref_table == child_name {
            let ref_col_idx = match &spec.ref_column {
                Some(name) => child_columns
                    .iter()
                    .position(|c| c.name == *name)
                    .ok_or_else(|| SqlError::NoSuchColumn {
                        table: child_name.to_owned(),
                        column: name.clone(),
                    })?,
                None => child_pk_index.ok_or_else(|| {
                    SqlError::Constraint(
                        "single-column REFERENCES needs a single-column parent key".into(),
                    )
                })?,
            };
            validate_fk_target(
                child_name,
                child_col,
                child_name,
                &child_columns[ref_col_idx],
                Some(ref_col_idx) == child_pk_index,
                child_indexes,
                ref_col_idx,
            )?;
            let ref_col_name = child_columns[ref_col_idx].name.clone();
            let fk_def = ForeignKeyDef {
                column: spec.column,
                ref_table: spec.ref_table.clone(),
                ref_column: ref_col_name.clone(),
                on_delete: spec.on_delete,
                on_update: spec.on_update,
            };
            if !foreign_keys.iter().any(|existing| {
                existing.column == fk_def.column
                    && existing.ref_table == fk_def.ref_table
                    && existing.ref_column == fk_def.ref_column
            }) {
                foreign_keys.push(fk_def);
            }
            let back_ref = ForeignKeyRef {
                child_table: child_name.to_owned(),
                child_column: child_col.name.clone(),
                ref_column: ref_col_name,
            };
            if !self_referenced_by.contains(&back_ref) {
                self_referenced_by.push(back_ref);
            }
        } else {
            if !parent_map.contains_key(&spec.ref_table) {
                let cached_parent = catalog::load_table_cached(tx, &spec.ref_table, cache)?;
                parent_map.insert(
                    spec.ref_table.clone(),
                    (
                        cached_parent.catalog_key.clone(),
                        TableDef::clone(&cached_parent.def),
                    ),
                );
            }
            let (_, parent_def) = parent_map
                .get_mut(&spec.ref_table)
                .expect("parent loaded above");
            let ref_col_idx = match &spec.ref_column {
                Some(name) => {
                    parent_def
                        .column_index(name)
                        .ok_or_else(|| SqlError::NoSuchColumn {
                            table: parent_def.name.clone(),
                            column: name.clone(),
                        })?
                }
                None if parent_def.has_scalar_pk() => parent_def.primary_key_index,
                None => {
                    return Err(SqlError::Constraint(
                        "single-column REFERENCES needs a single-column parent key".into(),
                    ));
                }
            };
            validate_fk_target(
                child_name,
                child_col,
                &parent_def.name,
                &parent_def.columns[ref_col_idx],
                parent_def.has_scalar_pk() && ref_col_idx == parent_def.primary_key_index,
                &parent_def.indexes,
                ref_col_idx,
            )?;
            let ref_col_name = parent_def.columns[ref_col_idx].name.clone();
            let fk_def = ForeignKeyDef {
                column: spec.column,
                ref_table: spec.ref_table.clone(),
                ref_column: ref_col_name.clone(),
                on_delete: spec.on_delete,
                on_update: spec.on_update,
            };
            if !foreign_keys.iter().any(|existing| {
                existing.column == fk_def.column
                    && existing.ref_table == fk_def.ref_table
                    && existing.ref_column == fk_def.ref_column
            }) {
                foreign_keys.push(fk_def);
            }
            let back_ref = ForeignKeyRef {
                child_table: child_name.to_owned(),
                child_column: child_col.name.clone(),
                ref_column: ref_col_name,
            };
            if !parent_def.referenced_by.contains(&back_ref) {
                parent_def.referenced_by.push(back_ref);
            }
        }
    }

    let mut parent_updates: Vec<(Vec<u8>, TableDef)> = parent_map.into_values().collect();
    parent_updates.sort_by(|a, b| a.1.name.cmp(&b.1.name));

    Ok(PreparedCreateTableFks {
        foreign_keys,
        self_referenced_by,
        parent_updates,
    })
}

fn validate_fk_target(
    child_table: &str,
    child_col: &catalog::ColumnDef,
    parent_table: &str,
    parent_col: &catalog::ColumnDef,
    is_parent_pk: bool,
    parent_indexes: &[IndexDef],
    parent_col_idx: usize,
) -> Result<(), SqlError> {
    let has_unique_index = is_parent_pk
        || parent_indexes.iter().any(|idx| {
            idx.unique
                && idx.state == IndexState::Ready
                && idx.columns.as_slice() == [parent_col_idx]
        });
    if !has_unique_index {
        return Err(SqlError::Constraint(format!(
            "foreign key target '{parent_table}.{}' must be PRIMARY KEY or have a Ready UNIQUE single-column index",
            parent_col.name
        )));
    }
    if !child_col.col_type.matches_base(parent_col.col_type) {
        return Err(SqlError::Type(format!(
            "foreign key column '{child_table}.{}' ({}) is incompatible with referenced column '{parent_table}.{}' ({})",
            child_col.name,
            child_col.col_type.display_name(),
            parent_col.name,
            parent_col.col_type.display_name()
        )));
    }
    Ok(())
}

/// Refuses `DROP TABLE` if any other table holds a foreign key referencing this table.
pub(crate) fn ensure_table_droppable(table: &TableDef) -> Result<(), SqlError> {
    if let Some(fk_ref) = table
        .referenced_by
        .iter()
        .find(|r| r.child_table != table.name)
    {
        return Err(SqlError::Constraint(format!(
            "cannot drop table '{}': referenced by foreign key '{}.{}'",
            table.name, fk_ref.child_table, fk_ref.child_column
        )));
    }
    if let Some(reference) = table
        .tuple_referenced_by
        .iter()
        .find(|reference| reference.child_table != table.name)
    {
        return Err(SqlError::Constraint(format!(
            "cannot drop table '{}': referenced by composite foreign key on '{}'",
            table.name, reference.child_table
        )));
    }
    Ok(())
}

/// Removes back-references from all external parent tables when dropping `child`.
pub(crate) fn detach_parent_references(
    tx: &mut DocumentTransaction<'_, '_>,
    child: &TableDef,
    cache: &TableCache,
) -> Result<(), SqlError> {
    let mut parent_names: Vec<&str> = child
        .foreign_keys
        .iter()
        .map(|fk| fk.ref_table.as_str())
        .chain(
            child
                .tuple_foreign_keys
                .iter()
                .map(|fk| fk.ref_table.as_str()),
        )
        .filter(|name| *name != child.name)
        .collect();
    parent_names.sort_unstable();
    parent_names.dedup();

    for parent_name in parent_names {
        let cached_parent = match catalog::load_table_cached(tx, parent_name, cache) {
            Ok(p) => p,
            Err(SqlError::NoSuchTable(_)) => continue,
            Err(err) => return Err(err),
        };
        let mut updated = TableDef::clone(&cached_parent.def);
        let prev_len = updated.referenced_by.len() + updated.tuple_referenced_by.len();
        updated
            .referenced_by
            .retain(|r| r.child_table != child.name);
        updated
            .tuple_referenced_by
            .retain(|r| r.child_table != child.name);
        if updated.referenced_by.len() + updated.tuple_referenced_by.len() != prev_len {
            updated.validate()?;
            catalog::save_table_def(tx, &cached_parent.catalog_key, &updated)?;
        }
    }
    Ok(())
}

/// Refuses dropping a single-column UNIQUE index if a foreign key depends on it
/// and no other Ready UNIQUE single-column index (or PRIMARY KEY) covers that column.
pub(crate) fn ensure_index_not_fk_target(
    table: &TableDef,
    index: &IndexDef,
) -> Result<(), SqlError> {
    tuple::ensure_index_not_target(table, index)?;
    if !index.unique || index.columns.len() != 1 || table.referenced_by.is_empty() {
        return Ok(());
    }
    let col_idx = index.columns[0];
    if table.has_scalar_pk() && col_idx == table.primary_key_index {
        return Ok(());
    }
    let col_name = &table.columns[col_idx].name;
    let referenced = table
        .referenced_by
        .iter()
        .any(|r| r.ref_column == *col_name);
    if !referenced {
        return Ok(());
    }
    let has_other_unique = table.indexes.iter().any(|other| {
        other.name != index.name
            && other.unique
            && other.state == IndexState::Ready
            && other.columns.as_slice() == [col_idx]
    });
    if !has_other_unique {
        return Err(SqlError::Constraint(format!(
            "cannot drop UNIQUE index '{}' on '{}.{}': referenced by a FOREIGN KEY",
            index.name, table.name, col_name
        )));
    }
    Ok(())
}

/// Verifies all foreign key constraints after staging one or more inserted rows.
pub(crate) fn check_inserted_docs(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    docs: &[&Value],
    cache: &TableCache,
) -> Result<(), SqlError> {
    tuple::check_children(tx, table, docs, cache)?;
    if table.foreign_keys.is_empty() || docs.is_empty() {
        return Ok(());
    }
    let mut parent_cache: HashMap<&str, Arc<CachedTable>> = HashMap::new();
    for fk in &table.foreign_keys {
        let child_col = &table.columns[fk.column];
        let mut keys = FkKeySet::new(child_col.col_type)?;
        for doc in docs {
            let Value::Object(obj) = doc else {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "inserted row is not a JSON object",
                )));
            };
            if let Some(key) = extract_json_fk_key(obj, child_col)? {
                keys.insert(key)?;
            }
        }
        if keys.is_empty() {
            continue;
        }
        verify_parent_keys_exist(
            tx,
            table,
            child_col,
            &fk.ref_table,
            &fk.ref_column,
            &keys,
            cache,
            &mut parent_cache,
        )?;
    }
    Ok(())
}

pub(crate) fn check_inserted_prepared_rows(
    tx: &DocumentTransaction<'_, '_>,
    table: &TableDef,
    rows: &[(String, Value)],
    cache: &TableCache,
) -> Result<(), SqlError> {
    if (table.foreign_keys.is_empty() && table.tuple_foreign_keys.is_empty()) || rows.is_empty() {
        return Ok(());
    }
    let doc_refs: Vec<&Value> = rows.iter().map(|(_, doc)| doc).collect();
    check_inserted_docs(tx, table, &doc_refs, cache)
}

/// Tracks parent-key deletions during `DELETE` so they can be verified at statement end.
pub(crate) struct PendingDeleteFkChecks {
    /// `(ref_col_idx, FkKeySet)` for each distinct referenced column of `table`.
    deleted_by_col: Vec<(usize, FkKeySet)>,
    tuple_removed: Removals,
}

impl PendingDeleteFkChecks {
    pub(crate) fn new(table: &TableDef) -> Result<Self, SqlError> {
        if table.referenced_by.is_empty() {
            return Ok(Self {
                deleted_by_col: Vec::new(),
                tuple_removed: Removals::default(),
            });
        }
        let mut col_indices: Vec<usize> = table
            .referenced_by
            .iter()
            .filter_map(|r| table.column_index(&r.ref_column))
            .collect();
        col_indices.sort_unstable();
        col_indices.dedup();
        let mut deleted_by_col = Vec::with_capacity(col_indices.len());
        for col_idx in col_indices {
            deleted_by_col.push((col_idx, FkKeySet::new(table.columns[col_idx].col_type)?));
        }
        Ok(Self {
            deleted_by_col,
            tuple_removed: Removals::default(),
        })
    }

    pub(crate) fn record_row(
        &mut self,
        table: &TableDef,
        obj: &Map<String, Value>,
    ) -> Result<(), SqlError> {
        self.tuple_removed.record(table, obj)?;
        for (col_idx, set) in &mut self.deleted_by_col {
            if let Some(key) = extract_json_fk_key(obj, &table.columns[*col_idx])? {
                set.insert(key)?;
            }
        }
        Ok(())
    }

    /// Applies each child's `ON DELETE` action for the deleted parent keys,
    /// then checks `NO ACTION`/`RESTRICT` references. Runs inside the
    /// statement's transaction after the parent rows are deleted.
    pub(crate) fn verify(
        self,
        tx: &mut DocumentTransaction<'_, '_>,
        table: &TableDef,
        limits: &SqlLimits,
        cache: &TableCache,
    ) -> Result<(), SqlError> {
        let mut budget = CascadeBudget::default();
        let mut actions = UpdateActions::new(limits, cache, &mut budget);
        actions.tuple_removed = self.tuple_removed;
        apply_delete_actions(tx, table, self.deleted_by_col, &mut actions, 0)?;
        actions.finish(tx)
    }
}

/// Nested cascades beyond this depth fail instead of continuing.
pub(crate) const MAX_FK_CASCADE_DEPTH: usize = 16;

#[derive(Default)]
struct CascadeBudget {
    /// Rows read while looking for child rows, against `max_visited_rows`.
    visited: usize,
    /// Child rows deleted or changed by actions, against `max_mutation_rows`.
    changed: usize,
}

fn load_child(
    tx: &DocumentTransaction<'_, '_>,
    fk_ref: &ForeignKeyRef,
    cache: &TableCache,
) -> Result<(Arc<CachedTable>, usize), SqlError> {
    let child = catalog::load_table_cached(tx, &fk_ref.child_table, cache)?;
    let idx = child
        .def
        .column_index(&fk_ref.child_column)
        .ok_or_else(|| {
            SqlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "foreign key child column '{}.{}' missing from catalog",
                    fk_ref.child_table, fk_ref.child_column
                ),
            ))
        })?;
    Ok((child, idx))
}

fn apply_delete_actions(
    tx: &mut DocumentTransaction<'_, '_>,
    parent: &TableDef,
    removed_by_col: Vec<(usize, FkKeySet)>,
    actions: &mut UpdateActions<'_>,
    depth: usize,
) -> Result<(), SqlError> {
    for (ref_col_idx, keys) in removed_by_col {
        if keys.is_empty() {
            continue;
        }
        let ref_col_name = parent.columns[ref_col_idx].name.clone();
        for fk_ref in &parent.referenced_by {
            if fk_ref.ref_column != ref_col_name {
                continue;
            }
            let (child, child_col_idx) = load_child(tx, fk_ref, actions.cache)?;
            let child_def = &child.def;
            let action = child_def
                .foreign_keys
                .iter()
                .find(|fk| {
                    fk.column == child_col_idx
                        && fk.ref_table == parent.name
                        && fk.ref_column == ref_col_name
                })
                .map_or(ForeignKeyAction::NoAction, |fk| fk.on_delete);
            match action {
                ForeignKeyAction::NoAction => {}
                ForeignKeyAction::Restrict => {
                    if child_has_any_key(
                        tx,
                        child_def,
                        child_col_idx,
                        &keys,
                        actions.limits,
                        &mut actions.budget.visited,
                    )? {
                        return Err(SqlError::Constraint(format!(
                            "FOREIGN KEY constraint failed: '{}.{}' is referenced by '{}.{}'",
                            parent.name, ref_col_name, child_def.name, fk_ref.child_column
                        )));
                    }
                }
                ForeignKeyAction::Cascade | ForeignKeyAction::SetNull => {
                    if depth >= MAX_FK_CASCADE_DEPTH {
                        return Err(SqlError::Bounds(format!(
                            "foreign key actions nest deeper than {MAX_FK_CASCADE_DEPTH} levels"
                        )));
                    }
                    let rows = collect_child_rows(
                        tx,
                        child_def,
                        child_col_idx,
                        &keys,
                        actions.limits,
                        actions.budget,
                    )?;
                    if rows.is_empty() {
                        continue;
                    }
                    actions.charge_mutations(rows.len())?;
                    if action == ForeignKeyAction::Cascade {
                        let mut removed = PendingDeleteFkChecks::new(child_def)?;
                        for (doc_id, obj) in &rows {
                            super::exec::verify_doc_pk_id(child_def, doc_id, obj)?;
                            tx.delete(&child_def.name, doc_id)
                                .map_err(classify_io_error)?;
                            removed.record_row(child_def, obj)?;
                        }
                        actions
                            .tuple_removed
                            .append(removed.tuple_removed, depth + 1);
                        apply_delete_actions(
                            tx,
                            child_def,
                            removed.deleted_by_col,
                            actions,
                            depth + 1,
                        )?;
                    } else {
                        let checks = super::exec::bind_table_checks(child_def, actions.limits)?;
                        let column = &child_def.columns[child_col_idx];
                        let mut changed = PendingUpdateFkChecks::new(child_def, &[child_col_idx]);
                        for (doc_id, mut obj) in rows {
                            super::exec::verify_doc_pk_id(child_def, &doc_id, &obj)?;
                            changed.record_row(child_def, &obj, &[(column, SqlValue::Null)])?;
                            obj.remove(&column.name);
                            super::exec::enforce_checks(&checks, child_def, &obj)?;
                            tx.replace(&child_def.name, &doc_id, &Value::Object(obj), false)
                                .map_err(|error| {
                                    super::exec::map_write_constraint_error(error, &child_def.name)
                                })?;
                        }
                        // SET NULL updates a child row. Its dependents follow
                        // ON UPDATE even though the original parent was deleted.
                        actions
                            .tuple_removed
                            .append(changed.tuple_removed, depth + 1);
                        actions.apply(tx, child_def, changed.parent_changes, depth + 1)?;
                    }
                }
            }
        }
        actions
            .removed
            .entry(parent.name.clone())
            .or_default()
            .push((ref_col_idx, keys));
    }
    Ok(())
}

/// A child row's document ID and stored object.
type ChildRow = (String, Map<String, Value>);

/// Child rows whose column holds one of `keys`, with their document IDs,
/// found through the child's primary key, a usable index, or a bounded scan.
fn collect_child_rows(
    tx: &DocumentTransaction<'_, '_>,
    child: &TableDef,
    child_col_idx: usize,
    keys: &FkKeySet,
    limits: &SqlLimits,
    budget: &mut CascadeBudget,
) -> Result<Vec<ChildRow>, SqlError> {
    let mut rows = Vec::new();
    let mut take =
        |doc_id: String, doc: Value, budget: &mut CascadeBudget| -> Result<(), SqlError> {
            budget.visited += 1;
            if budget.visited > limits.max_visited_rows {
                return Err(SqlError::Bounds(format!(
                    "visited row count exceeds max_visited_rows ({})",
                    limits.max_visited_rows
                )));
            }
            let Value::Object(obj) = doc else {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stored row is not a JSON object",
                )));
            };
            if rows.len() >= limits.max_mutation_rows {
                return Err(SqlError::Bounds(format!(
                    "foreign key actions change more than max_mutation_rows ({}) rows",
                    limits.max_mutation_rows
                )));
            }
            rows.push((doc_id, obj));
            Ok(())
        };
    let column = &child.columns[child_col_idx];
    if child.has_scalar_pk() && child_col_idx == child.primary_key_index {
        for key in keys.to_vec() {
            let doc_id = match (column.col_type, &key) {
                (SqlType::Integer, FkKey::Integer(n)) => {
                    let mut buf = [0u8; 18];
                    encode_i64_pk_bytes(*n, &mut buf);
                    std::str::from_utf8(&buf).expect("ASCII hex PK").to_owned()
                }
                (SqlType::Text { .. }, FkKey::Text(s)) if s.len() <= MAX_SQL_TEXT_PK_BYTES => {
                    format!("t:{s}")
                }
                _ => continue,
            };
            if let Some(doc) = tx.get(&child.name, &doc_id).map_err(classify_io_error)? {
                take(doc_id, doc, budget)?;
            }
        }
        return Ok(rows);
    }
    if let Some(index) = child_lookup_index(child, child_col_idx) {
        for key in keys.to_vec() {
            let Some(value) = fk_key_to_index_value(&key) else {
                continue;
            };
            let mut query = IndexQuery {
                equal: vec![value],
                lower: None,
                upper: None,
                after: None,
                limit: 1,
            };
            loop {
                query.limit = (limits.max_visited_rows - budget.visited + 1).min(256);
                let page = tx
                    .query_index(&child.name, &index.name, &query)
                    .map_err(classify_io_error)?;
                for entry in page.documents {
                    take(entry.id, entry.document, budget)?;
                }
                if page.next.is_none() {
                    break;
                }
                query.after = page.next;
            }
        }
        return Ok(rows);
    }
    // Every matching row is needed, so read the whole table in bounded pages;
    // each row's column is compared with the keys.
    let mut after: Option<String> = None;
    loop {
        let page = tx
            .scan_documents(&child.name, after.as_deref(), MAX_SCAN_ENTRIES)
            .map_err(classify_io_error)?;
        let Some(last) = page.last() else {
            return Ok(rows);
        };
        after = Some(last.id.clone());
        for entry in page {
            let matches = match entry.document.get(&column.name) {
                None | Some(Value::Null) => false,
                Some(value) => match (keys, value) {
                    (FkKeySet::Integer(set), v) => v.as_i64().is_some_and(|n| set.contains(&n)),
                    (FkKeySet::Text(set), Value::String(s)) => set.contains(s.as_str()),
                    (FkKeySet::Boolean(set), Value::Bool(b)) => set.contains(b),
                    _ => false,
                },
            };
            if matches {
                take(entry.id, entry.document, budget)?;
            } else {
                budget.visited += 1;
                if budget.visited > limits.max_visited_rows {
                    return Err(SqlError::Bounds(format!(
                        "visited row count exceeds max_visited_rows ({})",
                        limits.max_visited_rows
                    )));
                }
            }
        }
    }
}

type KeyChanges = Vec<(usize, HashMap<FkKey, Option<FkKey>>)>;

/// Tracks child assignments and parent key replacements during an update.
pub(crate) struct PendingUpdateFkChecks {
    pub(crate) checks_children: bool,
    parent_changes: KeyChanges,
    track_tuple_parent: bool,
    tuple_removed: Removals,
}

impl PendingUpdateFkChecks {
    pub(crate) fn new(table: &TableDef, assigned_cols: &[usize]) -> Self {
        let checks_children = table
            .foreign_keys
            .iter()
            .any(|fk| assigned_cols.contains(&fk.column));
        let checks_children = checks_children
            || table.tuple_foreign_keys.iter().any(|constraint| {
                constraint
                    .columns
                    .iter()
                    .any(|column| assigned_cols.contains(column))
            });
        let track_tuple_parent = table.tuple_referenced_by.iter().any(|reference| {
            reference.ref_columns.iter().any(|name| {
                table
                    .column_index(name)
                    .is_some_and(|column| assigned_cols.contains(&column))
            })
        });
        let mut parents: Vec<usize> = table
            .referenced_by
            .iter()
            .filter_map(|r| table.column_index(&r.ref_column))
            .filter(|col| assigned_cols.contains(col))
            .collect();
        parents.sort_unstable();
        parents.dedup();
        Self {
            checks_children,
            track_tuple_parent,
            tuple_removed: Removals::default(),
            parent_changes: parents
                .into_iter()
                .map(|col| (col, HashMap::new()))
                .collect(),
        }
    }

    pub(crate) fn record_row(
        &mut self,
        table: &TableDef,
        old: &Map<String, Value>,
        updates: &[(&catalog::ColumnDef, SqlValue)],
    ) -> Result<(), SqlError> {
        if self.track_tuple_parent {
            self.tuple_removed.record_update(table, old, updates)?;
        }
        for (col, changes) in &mut self.parent_changes {
            let column = &table.columns[*col];
            let Some(old_key) = extract_json_fk_key(old, column)? else {
                continue;
            };
            let (_, value) = updates
                .iter()
                .find(|(c, _)| c.name == column.name)
                .expect("parent changes only track assigned columns");
            let new_key = sql_value_to_fk_key(value)?;
            if new_key.as_ref() != Some(&old_key) {
                changes.insert(old_key, new_key);
            }
        }
        Ok(())
    }

    pub(crate) fn verify(
        self,
        tx: &mut DocumentTransaction<'_, '_>,
        table: &TableDef,
        changed_ids: HashSet<String>,
        limits: &SqlLimits,
        cache: &TableCache,
    ) -> Result<(), SqlError> {
        let mut budget = CascadeBudget::default();
        let mut actions = UpdateActions::new(limits, cache, &mut budget);
        if self.checks_children && !changed_ids.is_empty() {
            actions.changed.insert(table.name.clone(), changed_ids);
        }
        actions.tuple_removed = self.tuple_removed;
        actions.apply(tx, table, self.parent_changes, 0)?;
        actions.finish(tx)
    }
}

struct UpdateActions<'a> {
    limits: &'a SqlLimits,
    cache: &'a TableCache,
    budget: &'a mut CascadeBudget,
    removed: HashMap<String, Vec<(usize, FkKeySet)>>,
    tuple_removed: Removals,
    changed: HashMap<String, HashSet<String>>,
}

impl<'a> UpdateActions<'a> {
    fn new(limits: &'a SqlLimits, cache: &'a TableCache, budget: &'a mut CascadeBudget) -> Self {
        Self {
            limits,
            cache,
            budget,
            removed: HashMap::new(),
            tuple_removed: Removals::default(),
            changed: HashMap::new(),
        }
    }

    fn apply(
        &mut self,
        tx: &mut DocumentTransaction<'_, '_>,
        parent: &TableDef,
        changes: KeyChanges,
        depth: usize,
    ) -> Result<(), SqlError> {
        for (ref_col, changes) in changes {
            if changes.is_empty() {
                continue;
            }
            let column = &parent.columns[ref_col];
            let mut old_keys = FkKeySet::new(column.col_type)?;
            for key in changes.keys() {
                old_keys.insert(key.clone())?;
            }
            for backref in parent
                .referenced_by
                .iter()
                .filter(|backref| backref.ref_column == column.name)
            {
                let (child, child_col) = load_child(tx, backref, self.cache)?;
                let child = &child.def;
                let action = child
                    .foreign_keys
                    .iter()
                    .find(|fk| {
                        fk.column == child_col
                            && fk.ref_table == parent.name
                            && fk.ref_column == column.name
                    })
                    .map_or(ForeignKeyAction::NoAction, |fk| fk.on_update);
                if !matches!(
                    action,
                    ForeignKeyAction::Cascade | ForeignKeyAction::SetNull
                ) {
                    continue;
                }
                let rows =
                    collect_child_rows(tx, child, child_col, &old_keys, self.limits, self.budget)?;
                if rows.is_empty() {
                    continue;
                }
                if depth >= MAX_FK_CASCADE_DEPTH {
                    return Err(SqlError::Bounds(format!(
                        "foreign key actions nest deeper than {MAX_FK_CASCADE_DEPTH} levels"
                    )));
                }
                self.charge_mutations(rows.len())?;
                let mut downstream = PendingUpdateFkChecks::new(child, &[child_col]);
                let checks = super::exec::bind_table_checks(child, self.limits)?;
                let child_column = &child.columns[child_col];
                for (id, mut obj) in rows {
                    super::exec::verify_doc_pk_id(child, &id, &obj)?;
                    let previous = extract_json_fk_key(&obj, child_column)?
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "matching foreign key is missing",
                            )
                        })
                        .map_err(classify_io_error)?;
                    let value = match (action, changes.get(&previous)) {
                        (ForeignKeyAction::Cascade, Some(Some(FkKey::Integer(n)))) => {
                            SqlValue::Integer(*n)
                        }
                        (ForeignKeyAction::Cascade, Some(Some(FkKey::Text(s)))) => {
                            SqlValue::Text(s.clone())
                        }
                        (ForeignKeyAction::Cascade, Some(Some(FkKey::Boolean(b)))) => {
                            SqlValue::Boolean(*b)
                        }
                        (_, Some(_)) => SqlValue::Null,
                        (_, None) => {
                            return Err(classify_io_error(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "matching foreign key has no update",
                            )));
                        }
                    };
                    let value = coerce_for_column(
                        &value,
                        &child_column.name,
                        child_column.col_type,
                        child_column.not_null,
                        false,
                        self.limits.max_text_bytes,
                    )?;
                    let stored = super::exec::sql_value_to_json(&value, &child_column.name)?;
                    downstream.record_row(child, &obj, &[(child_column, value)])?;
                    if stored.is_null() {
                        obj.remove(&child_column.name);
                    } else {
                        obj.insert(child_column.name.clone(), stored);
                    }
                    super::exec::enforce_checks(&checks, child, &obj)?;
                    let id = super::exec::stage_updated_row(
                        tx,
                        child,
                        id,
                        obj,
                        child.primary_key_columns().any(|key| key == child_col),
                    )?;
                    self.changed
                        .entry(child.name.clone())
                        .or_default()
                        .insert(id);
                }
                self.tuple_removed
                    .append(downstream.tuple_removed, depth + 1);
                self.apply(tx, child, downstream.parent_changes, depth + 1)?;
            }
            self.removed
                .entry(parent.name.clone())
                .or_default()
                .push((ref_col, old_keys));
        }
        Ok(())
    }

    fn charge_mutations(&mut self, count: usize) -> Result<(), SqlError> {
        self.budget.changed += count;
        if self.budget.changed > self.limits.max_mutation_rows {
            return Err(SqlError::Bounds(format!(
                "foreign key actions change more than max_mutation_rows ({}) rows",
                self.limits.max_mutation_rows
            )));
        }
        Ok(())
    }

    fn finish(mut self, tx: &mut DocumentTransaction<'_, '_>) -> Result<(), SqlError> {
        while let Some(event) = self.tuple_removed.actions.pop_front() {
            self.apply_tuple(tx, event)?;
        }
        self.verify(tx)
    }

    fn apply_tuple(
        &mut self,
        tx: &mut DocumentTransaction<'_, '_>,
        event: tuple::Action,
    ) -> Result<(), SqlError> {
        let child = catalog::load_table_cached(tx, &event.reference.child_table, self.cache)?;
        let child = &child.def;
        let cols = tuple::columns(child, &event.reference.child_columns)?;
        let rows = tuple::child_rows(
            tx,
            child,
            &cols,
            &event.old,
            self.limits,
            &mut self.budget.visited,
            self.limits.max_mutation_rows - self.budget.changed + 1,
        )?;
        if rows.is_empty() {
            return Ok(());
        }
        if event.depth >= MAX_FK_CASCADE_DEPTH {
            return Err(SqlError::Bounds(format!(
                "foreign key actions nest deeper than {MAX_FK_CASCADE_DEPTH} levels"
            )));
        }
        self.charge_mutations(rows.len())?;
        let depth = event.depth + 1;
        if event.new.is_none() && event.reference.on_delete == ForeignKeyAction::Cascade {
            let mut downstream = PendingDeleteFkChecks::new(child)?;
            for entry in rows {
                let Some(obj) = entry.document.as_object() else {
                    return Err(classify_io_error(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "stored row is not an object",
                    )));
                };
                super::exec::verify_doc_pk_id(child, &entry.id, obj)?;
                downstream.record_row(child, obj)?;
                tx.delete(&child.name, &entry.id)
                    .map_err(classify_io_error)?;
            }
            self.tuple_removed.append(downstream.tuple_removed, depth);
            return apply_delete_actions(tx, child, downstream.deleted_by_col, self, depth);
        }
        let values = event
            .new
            .filter(|_| event.reference.on_update == ForeignKeyAction::Cascade);
        let mut updates = Vec::with_capacity(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let column = &child.columns[*col];
            let value = match values.as_ref().map(|tuple| &tuple[i]) {
                Some(IndexValue::Integer(n)) => SqlValue::Integer(*n),
                Some(IndexValue::String(text)) => SqlValue::Text(text.clone()),
                Some(IndexValue::Boolean(value)) => SqlValue::Boolean(*value),
                Some(IndexValue::Null) | None => SqlValue::Null,
            };
            let value = coerce_for_column(
                &value,
                &column.name,
                column.col_type,
                column.not_null,
                false,
                self.limits.max_text_bytes,
            )?;
            updates.push((column, value));
        }
        let checks = super::exec::bind_table_checks(child, self.limits)?;
        let assigns_pk = child.primary_key_columns().any(|key| cols.contains(&key));
        let mut downstream = PendingUpdateFkChecks::new(child, &cols);
        for entry in rows {
            let Value::Object(mut obj) = entry.document else {
                return Err(classify_io_error(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stored row is not an object",
                )));
            };
            super::exec::verify_doc_pk_id(child, &entry.id, &obj)?;
            downstream.record_row(child, &obj, &updates)?;
            for (column, value) in &updates {
                let json = super::exec::sql_value_to_json(value, &column.name)?;
                if json.is_null() {
                    obj.remove(&column.name);
                } else {
                    obj.insert(column.name.clone(), json);
                }
            }
            super::exec::enforce_checks(&checks, child, &obj)?;
            let id = super::exec::stage_updated_row(tx, child, entry.id, obj, assigns_pk)?;
            self.changed
                .entry(child.name.clone())
                .or_default()
                .insert(id);
        }
        self.tuple_removed.append(downstream.tuple_removed, depth);
        self.apply(tx, child, downstream.parent_changes, depth)
    }

    fn verify(self, tx: &DocumentTransaction<'_, '_>) -> Result<(), SqlError> {
        for (table, ids) in self.changed {
            let table = catalog::load_table_cached(tx, &table, self.cache)?;
            let mut docs = Vec::with_capacity(ids.len());
            for id in ids {
                if let Some(doc) = tx.get(&table.def.name, &id).map_err(classify_io_error)? {
                    docs.push(doc);
                }
            }
            let refs: Vec<&Value> = docs.iter().collect();
            check_inserted_docs(tx, &table.def, &refs, self.cache)?;
        }
        let mut child_cache = HashMap::new();
        for (name, removed) in self.removed {
            let parent = catalog::load_table_cached(tx, &name, self.cache)?;
            let parent = &parent.def;
            for (ref_col, keys) in removed {
                let column = &parent.columns[ref_col];
                let mut missing = FkKeySet::new(column.col_type)?;
                for key in keys.to_vec() {
                    if !parent_has_key(tx, parent, ref_col, &key)? {
                        missing.insert(key)?;
                    }
                }
                if missing.is_empty() {
                    continue;
                }
                for fk_ref in parent
                    .referenced_by
                    .iter()
                    .filter(|fk_ref| fk_ref.ref_column == column.name)
                {
                    ensure_no_child_references(
                        tx,
                        parent,
                        &column.name,
                        fk_ref,
                        &missing,
                        self.limits,
                        self.cache,
                        &mut child_cache,
                        &mut self.budget.visited,
                    )?;
                }
            }
        }
        self.tuple_removed
            .verify(tx, self.limits, self.cache, &mut self.budget.visited)
    }
}

#[allow(clippy::too_many_arguments)]
fn verify_parent_keys_exist<'a>(
    tx: &DocumentTransaction<'_, '_>,
    child_table: &TableDef,
    child_col: &catalog::ColumnDef,
    ref_table_name: &'a str,
    ref_col_name: &str,
    keys: &FkKeySet,
    cache: &TableCache,
    parent_cache: &mut HashMap<&'a str, Arc<CachedTable>>,
) -> Result<(), SqlError> {
    let parent = if let Some(cached) = parent_cache.get(ref_table_name) {
        Arc::clone(cached)
    } else {
        let loaded = catalog::load_table_cached(tx, ref_table_name, cache)?;
        parent_cache.insert(ref_table_name, Arc::clone(&loaded));
        loaded
    };
    let parent_def = &parent.def;
    let ref_col_idx = parent_def.column_index(ref_col_name).ok_or_else(|| {
        SqlError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "foreign key target column '{}.{}' missing from catalog",
                parent_def.name, ref_col_name
            ),
        ))
    })?;
    for key in keys.to_vec() {
        if !parent_has_key(tx, parent_def, ref_col_idx, &key)? {
            return Err(SqlError::Constraint(format!(
                "FOREIGN KEY constraint failed on '{}.{}' referencing '{}.{}'",
                child_table.name, child_col.name, parent_def.name, ref_col_name
            )));
        }
    }
    Ok(())
}

fn parent_has_key(
    tx: &DocumentTransaction<'_, '_>,
    parent: &TableDef,
    ref_col_idx: usize,
    key: &FkKey,
) -> Result<bool, SqlError> {
    if parent.has_scalar_pk() && ref_col_idx == parent.primary_key_index {
        let pk_type = parent.columns[ref_col_idx].col_type;
        match (pk_type, key) {
            (SqlType::Integer, FkKey::Integer(n)) => {
                let mut buf = [0u8; 18];
                encode_i64_pk_bytes(*n, &mut buf);
                let doc_id = std::str::from_utf8(&buf).expect("ASCII hex PK");
                Ok(tx
                    .get(&parent.name, doc_id)
                    .map_err(classify_io_error)?
                    .is_some())
            }
            (SqlType::Text { .. }, FkKey::Text(s)) => {
                if s.len() > MAX_SQL_TEXT_PK_BYTES {
                    return Ok(false);
                }
                let mut buf = [0u8; 256];
                buf[0] = b't';
                buf[1] = b':';
                buf[2..2 + s.len()].copy_from_slice(s.as_bytes());
                let doc_id = std::str::from_utf8(&buf[..2 + s.len()]).expect("valid UTF-8 PK");
                Ok(tx
                    .get(&parent.name, doc_id)
                    .map_err(classify_io_error)?
                    .is_some())
            }
            _ => Ok(false),
        }
    } else {
        let idx_def = parent
            .indexes
            .iter()
            .find(|i| {
                i.unique && i.state == IndexState::Ready && i.columns.as_slice() == [ref_col_idx]
            })
            .ok_or_else(|| {
                SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "referenced column '{}.{}' lacks a Ready UNIQUE index",
                        parent.name, parent.columns[ref_col_idx].name
                    ),
                ))
            })?;
        let Some(index_val) = fk_key_to_index_value(key) else {
            return Ok(false);
        };
        let page = tx
            .query_index(
                &parent.name,
                &idx_def.name,
                &IndexQuery {
                    equal: vec![index_val],
                    lower: None,
                    upper: None,
                    after: None,
                    limit: 1,
                },
            )
            .map_err(classify_io_error)?;
        Ok(!page.documents.is_empty())
    }
}

#[allow(clippy::too_many_arguments)]
fn ensure_no_child_references(
    tx: &DocumentTransaction<'_, '_>,
    parent: &TableDef,
    ref_col_name: &str,
    fk_ref: &ForeignKeyRef,
    keys: &FkKeySet,
    limits: &SqlLimits,
    cache: &TableCache,
    child_cache: &mut HashMap<String, Arc<CachedTable>>,
    visited: &mut usize,
) -> Result<(), SqlError> {
    let child = if let Some(cached) = child_cache.get(&fk_ref.child_table) {
        Arc::clone(cached)
    } else {
        let loaded = catalog::load_table_cached(tx, &fk_ref.child_table, cache)?;
        child_cache.insert(fk_ref.child_table.clone(), Arc::clone(&loaded));
        loaded
    };
    let child_def = &child.def;
    let child_col_idx = child_def
        .column_index(&fk_ref.child_column)
        .ok_or_else(|| {
            SqlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "foreign key child column '{}.{}' missing from catalog",
                    child_def.name, fk_ref.child_column
                ),
            ))
        })?;

    if child_has_any_key(tx, child_def, child_col_idx, keys, limits, visited)? {
        return Err(SqlError::Constraint(format!(
            "FOREIGN KEY constraint failed: '{}.{}' is referenced by '{}.{}'",
            parent.name, ref_col_name, child_def.name, fk_ref.child_column
        )));
    }
    Ok(())
}

fn child_has_any_key(
    tx: &DocumentTransaction<'_, '_>,
    child: &TableDef,
    child_col_idx: usize,
    keys: &FkKeySet,
    limits: &SqlLimits,
    visited: &mut usize,
) -> Result<bool, SqlError> {
    // 1. Child column is PRIMARY KEY -> O(1) point lookups.
    if child.has_scalar_pk() && child_col_idx == child.primary_key_index {
        let pk_type = child.columns[child_col_idx].col_type;
        for key in keys.to_vec() {
            match (pk_type, &key) {
                (SqlType::Integer, FkKey::Integer(n)) => {
                    let mut buf = [0u8; 18];
                    encode_i64_pk_bytes(*n, &mut buf);
                    let doc_id = std::str::from_utf8(&buf).expect("ASCII hex PK");
                    if tx
                        .get(&child.name, doc_id)
                        .map_err(classify_io_error)?
                        .is_some()
                    {
                        return Ok(true);
                    }
                }
                (SqlType::Text { .. }, FkKey::Text(s)) => {
                    if s.len() > MAX_SQL_TEXT_PK_BYTES {
                        continue;
                    }
                    let mut buf = [0u8; 256];
                    buf[0] = b't';
                    buf[1] = b':';
                    buf[2..2 + s.len()].copy_from_slice(s.as_bytes());
                    let doc_id = std::str::from_utf8(&buf[..2 + s.len()]).expect("valid UTF-8 PK");
                    if tx
                        .get(&child.name, doc_id)
                        .map_err(classify_io_error)?
                        .is_some()
                    {
                        return Ok(true);
                    }
                }
                _ => {}
            }
        }
        return Ok(false);
    }

    if let Some(idx_def) = child_lookup_index(child, child_col_idx) {
        for key in keys.to_vec() {
            let Some(index_val) = fk_key_to_index_value(&key) else {
                // Indexed columns never store strings > MAX_SQL_INDEX_TEXT_BYTES.
                continue;
            };
            let page = tx
                .query_index(
                    &child.name,
                    &idx_def.name,
                    &IndexQuery {
                        equal: vec![index_val],
                        lower: None,
                        upper: None,
                        after: None,
                        limit: 1,
                    },
                )
                .map_err(classify_io_error)?;
            if !page.documents.is_empty() {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    // 3. No usable index -> bounded scan subject to `limits.max_visited_rows`.
    scan_child_for_keys(tx, child, child_col_idx, keys, limits, visited)
}

/// Every matching non-NULL foreign key must appear in this index.
fn child_lookup_index(child: &TableDef, child_col_idx: usize) -> Option<&IndexDef> {
    child
        .indexes
        .iter()
        .filter(|idx| {
            idx.state == IndexState::Ready
                && idx.columns.first() == Some(&child_col_idx)
                && idx.columns[1..]
                    .iter()
                    .all(|&trailing| child.columns[trailing].not_null)
        })
        .min_by_key(|idx| (idx.columns.len(), !idx.unique))
}

fn scan_child_for_keys(
    tx: &DocumentTransaction<'_, '_>,
    child: &TableDef,
    child_col_idx: usize,
    keys: &FkKeySet,
    limits: &SqlLimits,
    visited: &mut usize,
) -> Result<bool, SqlError> {
    let child_col = &child.columns[child_col_idx];
    let mut after_id: Option<String> = None;

    loop {
        let remaining_visit = limits.max_visited_rows.saturating_sub(*visited);
        let page_limit = remaining_visit.saturating_add(1).clamp(1, MAX_SCAN_ENTRIES);
        let mut outcome: Result<bool, SqlError> = Ok(false);
        let mut visit_fn = |schema: &crate::CollectionSchema, _id: &str, bytes: &[u8]| {
            *visited += 1;
            if *visited > limits.max_visited_rows {
                outcome = Err(SqlError::Bounds(format!(
                    "visited row count exceeds max_visited_rows ({})",
                    limits.max_visited_rows
                )));
                return false;
            }
            match check_stored_row_matches_fk(bytes, schema, child_col, keys) {
                Ok(true) => {
                    outcome = Ok(true);
                    false
                }
                Ok(false) => true,
                Err(err) => {
                    outcome = Err(err);
                    false
                }
            }
        };
        let (count, last_id) = tx
            .visit_stored_documents(&child.name, after_id.as_deref(), page_limit, &mut visit_fn)
            .map_err(classify_io_error)?;
        if outcome? {
            return Ok(true);
        }
        if count == 0 {
            return Ok(false);
        }
        after_id = last_id;
    }
}

fn check_stored_row_matches_fk(
    bytes: &[u8],
    schema: &crate::CollectionSchema,
    col: &catalog::ColumnDef,
    keys: &FkKeySet,
) -> Result<bool, SqlError> {
    let invalid = |msg: String| SqlError::Io(io::Error::new(io::ErrorKind::InvalidData, msg));
    let decoded = DocumentTransaction::decode_stored(bytes, schema).map_err(classify_io_error)?;
    match decoded {
        StoredDocument::Flat(doc) => match doc.get(&col.name) {
            None | Some(FlatScalar::Null) => Ok(false),
            Some(scalar) => match (col.col_type, keys) {
                (SqlType::Integer, FkKeySet::Integer(set)) => {
                    let n = scalar.as_i64().ok_or_else(|| {
                        invalid(format!("stored column '{}' is not an INTEGER", col.name))
                    })?;
                    Ok(set.contains(&n))
                }
                (SqlType::Text { .. }, FkKeySet::Text(set)) => match scalar {
                    FlatScalar::Str(txt) => Ok(set.contains(txt)),
                    _ => Err(invalid(format!("stored column '{}' is not TEXT", col.name))),
                },
                (SqlType::Boolean, FkKeySet::Boolean(set)) => match scalar {
                    FlatScalar::Bool(b) => Ok(set.contains(&b)),
                    _ => Err(invalid(format!(
                        "stored column '{}' is not BOOLEAN",
                        col.name
                    ))),
                },
                _ => Err(invalid(format!(
                    "mismatched foreign key type on column '{}'",
                    col.name
                ))),
            },
        },
        StoredDocument::Full(Value::Object(obj)) => match obj.get(&col.name) {
            None | Some(Value::Null) => Ok(false),
            Some(val) => match (col.col_type, keys) {
                (SqlType::Integer, FkKeySet::Integer(set)) => {
                    let n = val.as_i64().ok_or_else(|| {
                        invalid(format!("stored column '{}' is not an INTEGER", col.name))
                    })?;
                    Ok(set.contains(&n))
                }
                (SqlType::Text { .. }, FkKeySet::Text(set)) => {
                    let s = val.as_str().ok_or_else(|| {
                        invalid(format!("stored column '{}' is not TEXT", col.name))
                    })?;
                    Ok(set.contains(s))
                }
                (SqlType::Boolean, FkKeySet::Boolean(set)) => {
                    let b = val.as_bool().ok_or_else(|| {
                        invalid(format!("stored column '{}' is not BOOLEAN", col.name))
                    })?;
                    Ok(set.contains(&b))
                }
                _ => Err(invalid(format!(
                    "mismatched foreign key type on column '{}'",
                    col.name
                ))),
            },
        },
        StoredDocument::Full(_) => Err(invalid("stored row is not a JSON object".into())),
    }
}

fn fk_key_to_index_value(key: &FkKey) -> Option<IndexValue> {
    match key {
        FkKey::Integer(n) => Some(IndexValue::Integer(*n)),
        FkKey::Text(s) => {
            if s.len() > MAX_SQL_INDEX_TEXT_BYTES {
                None
            } else {
                Some(IndexValue::String(s.clone()))
            }
        }
        FkKey::Boolean(b) => Some(IndexValue::Boolean(*b)),
    }
}

fn extract_json_fk_key(
    obj: &Map<String, Value>,
    col: &catalog::ColumnDef,
) -> Result<Option<FkKey>, SqlError> {
    let invalid = |msg: String| SqlError::Io(io::Error::new(io::ErrorKind::InvalidData, msg));
    match obj.get(&col.name) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match col.col_type {
            SqlType::Integer => Ok(Some(FkKey::Integer(v.as_i64().ok_or_else(|| {
                invalid(format!("stored column '{}' is not an INTEGER", col.name))
            })?))),
            SqlType::Text { .. } => Ok(Some(FkKey::Text(
                v.as_str()
                    .ok_or_else(|| invalid(format!("stored column '{}' is not TEXT", col.name)))?
                    .to_owned(),
            ))),
            SqlType::Boolean => Ok(Some(FkKey::Boolean(v.as_bool().ok_or_else(|| {
                invalid(format!("stored column '{}' is not BOOLEAN", col.name))
            })?))),
            SqlType::Real => Err(invalid(format!(
                "foreign key column '{}' cannot have type REAL",
                col.name
            ))),
        },
    }
}

fn sql_value_to_fk_key(val: &SqlValue) -> Result<Option<FkKey>, SqlError> {
    match val {
        SqlValue::Null => Ok(None),
        SqlValue::Integer(n) => Ok(Some(FkKey::Integer(*n))),
        SqlValue::Text(s) => Ok(Some(FkKey::Text(s.clone()))),
        SqlValue::Boolean(b) => Ok(Some(FkKey::Boolean(*b))),
        SqlValue::Real(_) => Err(SqlError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "foreign key value cannot be REAL",
        ))),
    }
}
