//! Durable SQL catalog stored atomically in the same transaction as the table collection.
use super::exec::classify_io_error;
use super::value::{
    MAX_SQL_COLUMNS, MAX_SQL_IDENTIFIER_BYTES, MAX_SQL_INDEX_COLUMNS, MAX_SQL_INDEXES_PER_TABLE,
    SqlError, SqlType,
};
use crate::document::{CollectionSchema, DocumentTransaction, FieldRule, IndexDefinition};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_CACHED_TABLES: usize = 256;
pub(crate) const MAX_SQL_FOREIGN_KEYS: usize = MAX_SQL_COLUMNS;

#[derive(Debug)]
pub(crate) struct CachedTable {
    pub(crate) raw_bytes: Vec<u8>,
    pub(crate) catalog_key: Vec<u8>,
    pub(crate) def: Arc<TableDef>,
    pub(crate) all_column_names: Vec<String>,
    pub(crate) generation: u64,
}

#[derive(Debug, Default)]
pub(crate) struct TableCache {
    entries: RwLock<HashMap<String, Arc<CachedTable>>>,
    next_gen: AtomicU64,
}

impl TableCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            next_gen: AtomicU64::new(1),
        }
    }
}

const SQL_CATALOG_PREFIX: &[u8] = b"\0emda-sql\x01T";
const CATALOG_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColumnDef {
    pub(crate) name: String,
    pub(crate) col_type: SqlType,
    pub(crate) not_null: bool,
    /// The stored form of the column's `DEFAULT`, already checked against
    /// its type; never JSON `null`. Absent in catalogs written before
    /// defaults existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) default: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IndexState {
    #[default]
    Ready,
    Building,
    Dropping,
}

impl IndexState {
    fn is_ready(&self) -> bool {
        *self == Self::Ready
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexDef {
    pub(crate) name: String,
    pub(crate) columns: Vec<usize>,
    pub(crate) unique: bool,
    #[serde(default, skip_serializing_if = "IndexState::is_ready")]
    pub(crate) state: IndexState,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) is_constraint: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ForeignKeyAction {
    #[default]
    NoAction,
    Restrict,
    /// `ON DELETE CASCADE`: deleting a parent row deletes its child rows.
    Cascade,
    /// `ON DELETE SET NULL`: deleting a parent row sets the child column to NULL.
    SetNull,
}

impl ForeignKeyAction {
    fn is_default(&self) -> bool {
        *self == Self::NoAction
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ForeignKeyDef {
    pub(crate) column: usize,
    pub(crate) ref_table: String,
    pub(crate) ref_column: String,
    #[serde(default, skip_serializing_if = "ForeignKeyAction::is_default")]
    pub(crate) on_delete: ForeignKeyAction,
    #[serde(default, skip_serializing_if = "ForeignKeyAction::is_default")]
    pub(crate) on_update: ForeignKeyAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ForeignKeyRef {
    pub(crate) child_table: String,
    pub(crate) child_column: String,
    pub(crate) ref_column: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableDef {
    pub(crate) version: u32,
    pub(crate) name: String,
    pub(crate) columns: Vec<ColumnDef>,
    pub(crate) primary_key_index: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) additional_primary_key_columns: Vec<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) indexes: Vec<IndexDef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) foreign_keys: Vec<ForeignKeyDef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) referenced_by: Vec<ForeignKeyRef>,
    /// `CHECK` expressions as written; bound and enforced by the executor.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) checks: Vec<String>,
}

/// `CHECK` constraints per table.
pub(crate) const MAX_SQL_CHECKS: usize = 32;
/// Bytes of one `CHECK` expression's text.
pub(crate) const MAX_SQL_CHECK_BYTES: usize = 4_096;

impl TableDef {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key_index: usize,
        indexes: Vec<IndexDef>,
    ) -> Result<Self, SqlError> {
        Self::with_foreign_keys(
            name,
            columns,
            primary_key_index,
            Vec::new(),
            indexes,
            Vec::new(),
            Vec::new(),
        )
    }

    pub(crate) fn with_foreign_keys(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key_index: usize,
        additional_primary_key_columns: Vec<usize>,
        indexes: Vec<IndexDef>,
        foreign_keys: Vec<ForeignKeyDef>,
        referenced_by: Vec<ForeignKeyRef>,
    ) -> Result<Self, SqlError> {
        let def = Self {
            version: CATALOG_VERSION,
            name,
            columns,
            primary_key_index,
            additional_primary_key_columns,
            indexes,
            foreign_keys,
            referenced_by,
            checks: Vec::new(),
        };
        def.validate()?;
        Ok(def)
    }

    pub(crate) fn has_scalar_pk(&self) -> bool {
        self.additional_primary_key_columns.is_empty()
    }

    pub(crate) fn primary_key_columns(&self) -> impl Iterator<Item = usize> + '_ {
        std::iter::once(self.primary_key_index)
            .chain(self.additional_primary_key_columns.iter().copied())
    }

    pub(crate) fn validate(&self) -> Result<(), SqlError> {
        if self.version != CATALOG_VERSION {
            return Err(SqlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported SQL catalog entry version",
            )));
        }
        if self.checks.len() > MAX_SQL_CHECKS
            || self
                .checks
                .iter()
                .any(|check| check.is_empty() || check.len() > MAX_SQL_CHECK_BYTES)
        {
            return Err(SqlError::Bounds(format!(
                "a table holds at most {MAX_SQL_CHECKS} CHECK constraints of at most \
                 {MAX_SQL_CHECK_BYTES} bytes each"
            )));
        }
        if self.name.is_empty() || self.name.len() > MAX_SQL_IDENTIFIER_BYTES {
            return Err(SqlError::Bounds(format!(
                "table name must be 1..={MAX_SQL_IDENTIFIER_BYTES} UTF-8 bytes"
            )));
        }
        if self.columns.is_empty() || self.columns.len() > MAX_SQL_COLUMNS {
            return Err(SqlError::Bounds(format!(
                "table column count must be 1..={MAX_SQL_COLUMNS}"
            )));
        }
        let mut names = HashSet::new();
        for col in &self.columns {
            if col.name.is_empty() || col.name.len() > MAX_SQL_IDENTIFIER_BYTES {
                return Err(SqlError::Bounds(format!(
                    "column name must be 1..={MAX_SQL_IDENTIFIER_BYTES} UTF-8 bytes"
                )));
            }
            if !names.insert(col.name.as_str()) {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate column in SQL catalog entry",
                )));
            }
        }
        for (position, index) in self.primary_key_columns().enumerate() {
            if position >= MAX_SQL_INDEX_COLUMNS
                || self
                    .primary_key_columns()
                    .take(position)
                    .any(|previous| previous == index)
                || self.columns.get(index).is_none_or(|col| {
                    !col.not_null
                        || !matches!(col.col_type, SqlType::Integer | SqlType::Text { .. })
                })
            {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid primary key columns in SQL catalog entry",
                )));
            }
        }
        if self.indexes.len() > MAX_SQL_INDEXES_PER_TABLE {
            return Err(SqlError::Bounds(format!(
                "table '{}' cannot have more than {MAX_SQL_INDEXES_PER_TABLE} indexes",
                self.name
            )));
        }
        let mut index_names = HashSet::new();
        for idx in &self.indexes {
            if idx.name.is_empty() || idx.name.len() > MAX_SQL_IDENTIFIER_BYTES {
                return Err(SqlError::Bounds(format!(
                    "index name must be 1..={MAX_SQL_IDENTIFIER_BYTES} UTF-8 bytes"
                )));
            }
            if !index_names.insert(idx.name.as_str()) {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate index name in SQL catalog entry",
                )));
            }
            if idx.columns.is_empty() || idx.columns.len() > MAX_SQL_INDEX_COLUMNS {
                return Err(SqlError::Bounds(format!(
                    "index '{}' must have 1..={MAX_SQL_INDEX_COLUMNS} columns",
                    idx.name
                )));
            }
            let mut seen_cols = HashSet::new();
            for &col_idx in &idx.columns {
                let col = self.columns.get(col_idx).ok_or_else(|| {
                    SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid column index in SQL catalog index entry",
                    ))
                })?;
                if !seen_cols.insert(col_idx) {
                    return Err(SqlError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "duplicate column in SQL catalog index entry",
                    )));
                }
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
            }
        }
        if self.foreign_keys.len() > MAX_SQL_FOREIGN_KEYS {
            return Err(SqlError::Bounds(format!(
                "table '{}' cannot have more than {MAX_SQL_FOREIGN_KEYS} foreign keys",
                self.name
            )));
        }
        for fk in &self.foreign_keys {
            if fk.column >= self.columns.len() {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid foreign key column index in SQL catalog entry",
                )));
            }
            if fk.ref_table.is_empty()
                || fk.ref_table.len() > MAX_SQL_IDENTIFIER_BYTES
                || fk.ref_column.is_empty()
                || fk.ref_column.len() > MAX_SQL_IDENTIFIER_BYTES
            {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid foreign key target identifier in SQL catalog entry",
                )));
            }
        }
        for back_ref in &self.referenced_by {
            if back_ref.child_table.is_empty()
                || back_ref.child_table.len() > MAX_SQL_IDENTIFIER_BYTES
                || back_ref.child_column.is_empty()
                || back_ref.child_column.len() > MAX_SQL_IDENTIFIER_BYTES
                || back_ref.ref_column.is_empty()
                || back_ref.ref_column.len() > MAX_SQL_IDENTIFIER_BYTES
                || self.column_index(&back_ref.ref_column).is_none()
            {
                return Err(SqlError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid foreign key back-reference in SQL catalog entry",
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub(crate) fn to_collection_schema(&self) -> CollectionSchema {
        CollectionSchema {
            fields: self
                .columns
                .iter()
                .map(|col| FieldRule {
                    path: vec![col.name.clone()],
                    field_type: col.col_type.to_field_type(),
                    // SQL NULL is stored as absent so document unique indexes omit NULL tuples.
                    required: col.not_null,
                    nullable: !col.not_null,
                })
                .collect(),
        }
    }

    pub(crate) fn to_index_definition(&self, idx: &IndexDef) -> IndexDefinition {
        IndexDefinition {
            name: idx.name.clone(),
            fields: idx
                .columns
                .iter()
                .map(|&col_idx| vec![self.columns[col_idx].name.clone()])
                .collect(),
            unique: idx.unique,
            partial: None,
        }
    }
}

pub(crate) fn table_exists(
    tx: &DocumentTransaction<'_, '_>,
    table_name: &str,
) -> Result<bool, SqlError> {
    let key = catalog_key(table_name)?;
    Ok(tx.get_raw(&key).map_err(classify_io_error)?.is_some())
}

/// Removes a table's catalog entry in the caller's transaction.
pub(crate) fn remove_table(
    tx: &mut DocumentTransaction<'_, '_>,
    table_name: &str,
) -> Result<(), SqlError> {
    let key = catalog_key(table_name)?;
    tx.delete_raw(&key).map_err(classify_io_error)
}

fn catalog_key(table_name: &str) -> Result<Vec<u8>, SqlError> {
    if table_name.is_empty() || table_name.len() > MAX_SQL_IDENTIFIER_BYTES {
        return Err(SqlError::Bounds(format!(
            "table name must be 1..={MAX_SQL_IDENTIFIER_BYTES} UTF-8 bytes"
        )));
    }
    let mut key = Vec::with_capacity(SQL_CATALOG_PREFIX.len() + table_name.len());
    key.extend_from_slice(SQL_CATALOG_PREFIX);
    key.extend_from_slice(table_name.as_bytes());
    Ok(key)
}

pub(crate) fn load_table_cached(
    tx: &DocumentTransaction<'_, '_>,
    table_name: &str,
    cache: &TableCache,
) -> Result<Arc<CachedTable>, SqlError> {
    let cached = cache.entries.read().get(table_name).cloned();
    let key_buf;
    let key = match &cached {
        Some(entry) => entry.catalog_key.as_slice(),
        None => {
            key_buf = catalog_key(table_name)?;
            &key_buf
        }
    };
    let raw = tx.get_raw(key).map_err(classify_io_error)?;
    let Some(bytes) = raw else {
        if cached.is_some() {
            cache.entries.write().remove(table_name);
        }
        return Err(SqlError::NoSuchTable(table_name.to_owned()));
    };
    if let Some(entry) = cached.as_ref().filter(|entry| bytes == entry.raw_bytes) {
        return Ok(Arc::clone(entry));
    }
    let def: TableDef = serde_json::from_slice(&bytes).map_err(|err| {
        SqlError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("corrupt SQL catalog entry for '{table_name}': {err}"),
        ))
    })?;
    def.validate()?;
    let all_column_names = def.columns.iter().map(|c| c.name.clone()).collect();
    let generation = cache.next_gen.fetch_add(1, Ordering::Relaxed);
    let entry = Arc::new(CachedTable {
        raw_bytes: bytes,
        catalog_key: key.to_vec(),
        def: Arc::new(def),
        all_column_names,
        generation,
    });
    let mut guard = cache.entries.write();
    if guard.len() >= MAX_CACHED_TABLES && !guard.contains_key(table_name) {
        guard.clear();
    }
    guard.insert(table_name.to_owned(), Arc::clone(&entry));
    Ok(entry)
}

pub(crate) fn create_table(
    tx: &mut DocumentTransaction<'_, '_>,
    def: &TableDef,
    if_not_exists: bool,
) -> Result<(), SqlError> {
    def.validate()?;
    let key = catalog_key(&def.name)?;
    if tx.get_raw(&key).map_err(classify_io_error)?.is_some() {
        if if_not_exists {
            return Ok(());
        }
        return Err(SqlError::TableExists(def.name.clone()));
    }
    let schema = def.to_collection_schema();
    tx.create_collection(&def.name, schema).map_err(|err| {
        if err.kind() == io::ErrorKind::AlreadyExists {
            SqlError::TableExists(def.name.clone())
        } else {
            classify_io_error(err)
        }
    })?;
    for idx in &def.indexes {
        tx.create_index(&def.name, def.to_index_definition(idx))
            .map_err(|err| map_index_build_error(err, &def.name, &idx.name))?;
    }
    let bytes = serde_json::to_vec(def).map_err(|err| SqlError::Io(io::Error::other(err)))?;
    tx.put_raw(&key, &bytes).map_err(classify_io_error)?;
    Ok(())
}

pub(crate) fn start_index_build(
    tx: &mut DocumentTransaction<'_, '_>,
    cached: &CachedTable,
    idx: IndexDef,
) -> Result<(), SqlError> {
    let existing = &cached.def;
    let mut updated = TableDef::clone(existing);
    let doc_def = updated.to_index_definition(&idx);
    let idx_name = idx.name.clone();
    updated.indexes.push(idx);
    updated.validate()?;
    tx.start_index_build(&updated.name, doc_def)
        .map_err(|err| map_index_build_error(err, &updated.name, &idx_name))?;
    save_table_def(tx, &cached.catalog_key, &updated)
}

pub(crate) fn set_index_state(
    tx: &mut DocumentTransaction<'_, '_>,
    table_name: &str,
    index_name: &str,
    state: IndexState,
    cache: &TableCache,
) -> Result<(), SqlError> {
    let cached = load_table_cached(tx, table_name, cache)?;
    let mut updated = TableDef::clone(&cached.def);
    let idx = updated
        .indexes
        .iter_mut()
        .find(|i| i.name == index_name)
        .ok_or_else(|| SqlError::NoSuchIndex {
            table: table_name.to_owned(),
            index: index_name.to_owned(),
        })?;
    idx.state = state;
    updated.validate()?;
    save_table_def(tx, &cached.catalog_key, &updated)
}

pub(crate) fn remove_index(
    tx: &mut DocumentTransaction<'_, '_>,
    table_name: &str,
    index_name: &str,
    cache: &TableCache,
) -> Result<(), SqlError> {
    let cached = load_table_cached(tx, table_name, cache)?;
    let mut updated = TableDef::clone(&cached.def);
    let prev_len = updated.indexes.len();
    updated.indexes.retain(|i| i.name != index_name);
    if updated.indexes.len() == prev_len {
        return Err(SqlError::NoSuchIndex {
            table: table_name.to_owned(),
            index: index_name.to_owned(),
        });
    }
    updated.validate()?;
    save_table_def(tx, &cached.catalog_key, &updated)
}

pub(crate) fn save_table_def(
    tx: &mut DocumentTransaction<'_, '_>,
    catalog_key: &[u8],
    def: &TableDef,
) -> Result<(), SqlError> {
    let bytes = serde_json::to_vec(def).map_err(|err| SqlError::Io(io::Error::other(err)))?;
    tx.put_raw(catalog_key, &bytes).map_err(classify_io_error)
}

pub(crate) fn map_index_build_error(
    err: io::Error,
    table_name: &str,
    index_name: &str,
) -> SqlError {
    if err.kind() == io::ErrorKind::AlreadyExists {
        if err.to_string().contains("unique") {
            return SqlError::Constraint(format!(
                "UNIQUE constraint failed while building index '{index_name}' on table '{table_name}'"
            ));
        }
        return SqlError::IndexExists(index_name.to_owned());
    }
    classify_io_error(err)
}
