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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableDef {
    pub(crate) version: u32,
    pub(crate) name: String,
    pub(crate) columns: Vec<ColumnDef>,
    pub(crate) primary_key_index: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) indexes: Vec<IndexDef>,
}

impl TableDef {
    pub(crate) fn new(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key_index: usize,
        indexes: Vec<IndexDef>,
    ) -> Result<Self, SqlError> {
        let def = Self {
            version: CATALOG_VERSION,
            name,
            columns,
            primary_key_index,
            indexes,
        };
        def.validate()?;
        Ok(def)
    }

    pub(crate) fn validate(&self) -> Result<(), SqlError> {
        if self.version != CATALOG_VERSION {
            return Err(SqlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported SQL catalog entry version",
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
        let pk = self.columns.get(self.primary_key_index).ok_or_else(|| {
            SqlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid primary key index in SQL catalog entry",
            ))
        })?;
        if !pk.not_null || !matches!(pk.col_type, SqlType::Integer | SqlType::Text { .. }) {
            return Err(SqlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid primary key column in SQL catalog entry",
            )));
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

fn save_table_def(
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
