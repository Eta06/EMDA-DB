//! Core SQL value, type, bound, and error definitions.
use crate::document::FieldType;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::io;

/// Maximum SQL statement byte length (64 KiB).
pub const MAX_SQL_BYTES: usize = 64 * 1024;
/// Maximum lexical tokens in one SQL statement.
pub const MAX_SQL_TOKENS: usize = 4_096;
/// Maximum expression nesting depth.
pub const MAX_SQL_EXPR_DEPTH: usize = 64;
/// Maximum columns per table (matches `CollectionSchema` 64-field limit).
pub const MAX_SQL_COLUMNS: usize = 64;
/// Maximum rows in one `INSERT` statement (matches `MAX_ATOMIC_BATCH_OPERATIONS`).
pub const MAX_SQL_INSERT_ROWS: usize = 1_024;
/// Maximum rows modified by one `UPDATE` or `DELETE` statement.
pub const MAX_SQL_MUTATION_ROWS: usize = 1_024;
/// Default maximum rows returned by one `SELECT` statement.
pub const MAX_SQL_SELECT_ROWS: usize = 10_000;
/// Default maximum rows buffered for an `ORDER BY` sort or top-K heap.
pub const DEFAULT_SQL_SORT_ROWS: usize = 32_768;
/// Hard ceiling for `max_sort_rows`.
pub const MAX_SQL_SORT_ROWS: usize = 131_072;
/// Default maximum groups tracked by one `GROUP BY` statement.
pub const DEFAULT_SQL_GROUPS: usize = 32_768;
/// Hard ceiling for `max_groups`.
pub const MAX_SQL_GROUPS: usize = 131_072;
/// Default maximum distinct values tracked across `COUNT(DISTINCT ...)` in one statement.
pub const DEFAULT_SQL_DISTINCT_VALUES: usize = 32_768;
/// Hard ceiling for `max_distinct_values`.
pub const MAX_SQL_DISTINCT_VALUES: usize = 131_072;
/// Default maximum rows visited per statement.
pub const DEFAULT_SQL_VISITED_ROWS: usize = 32_768;
/// Hard ceiling for `max_visited_rows`.
pub const MAX_SQL_VISITED_ROWS: usize = 131_072;
/// Maximum positional `?` parameters in one statement.
pub const MAX_SQL_PARAMS: usize = 1_024;
/// Maximum UTF-8 byte length of a SQL `TEXT` value (consistent with the 1 MiB document limit).
pub const MAX_SQL_TEXT_BYTES: usize = 256 * 1024;
/// Maximum UTF-8 byte length of a SQL identifier (matches `document::key::MAX_NAME_BYTES`).
pub const MAX_SQL_IDENTIFIER_BYTES: usize = 128;
/// Maximum UTF-8 byte length of a `TEXT` primary key value (`256 - len("t:")`).
pub const MAX_SQL_TEXT_PK_BYTES: usize = 254;
/// Maximum columns per secondary index or `UNIQUE` constraint (matches document layer).
pub(crate) const MAX_SQL_INDEX_COLUMNS: usize = 4;
/// Maximum secondary indexes and `UNIQUE` constraints per table (matches document layer).
pub(crate) const MAX_SQL_INDEXES_PER_TABLE: usize = 16;
/// Maximum UTF-8 byte length of a `TEXT` value in a secondary index.
pub(crate) const MAX_SQL_INDEX_TEXT_BYTES: usize = 256;

/// Configurable per-database execution bounds, capped at documented hard maxima.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlLimits {
    /// Maximum SQL text length in bytes (`1..=65_536`).
    pub max_sql_bytes: usize,
    /// Maximum non-EOF tokens per statement (`1..=4_096`).
    pub max_tokens: usize,
    /// Maximum expression nesting depth (`1..=64`).
    pub max_expr_depth: usize,
    /// Maximum columns per table (`1..=64`).
    pub max_columns: usize,
    /// Maximum rows per `INSERT` statement (`1..=1_024`).
    pub max_insert_rows: usize,
    /// Maximum rows modified by `UPDATE` or `DELETE` (`1..=1_024`).
    pub max_mutation_rows: usize,
    /// Maximum rows returned by `SELECT` (`1..=10_000`).
    pub max_select_rows: usize,
    /// Maximum rows buffered by `ORDER BY` sort / top-K (`1..=131_072`).
    pub max_sort_rows: usize,
    /// Maximum distinct groups in a `GROUP BY` statement (`1..=131_072`).
    pub max_groups: usize,
    /// Maximum distinct values tracked across `COUNT(DISTINCT ...)` in one statement (`1..=131_072`).
    pub max_distinct_values: usize,
    /// Maximum rows visited during a single statement (`1..=131_072`).
    pub max_visited_rows: usize,
    /// Maximum `?` positional parameters (`0..=1_024`).
    pub max_params: usize,
    /// Maximum UTF-8 byte length of a `TEXT` value (`1..=262_144`).
    pub max_text_bytes: usize,
}

impl Default for SqlLimits {
    fn default() -> Self {
        Self {
            max_sql_bytes: MAX_SQL_BYTES,
            max_tokens: MAX_SQL_TOKENS,
            max_expr_depth: MAX_SQL_EXPR_DEPTH,
            max_columns: MAX_SQL_COLUMNS,
            max_insert_rows: MAX_SQL_INSERT_ROWS,
            max_mutation_rows: MAX_SQL_MUTATION_ROWS,
            max_select_rows: MAX_SQL_SELECT_ROWS,
            max_sort_rows: DEFAULT_SQL_SORT_ROWS,
            max_groups: DEFAULT_SQL_GROUPS,
            max_distinct_values: DEFAULT_SQL_DISTINCT_VALUES,
            max_visited_rows: DEFAULT_SQL_VISITED_ROWS,
            max_params: MAX_SQL_PARAMS,
            max_text_bytes: MAX_SQL_TEXT_BYTES,
        }
    }
}

impl SqlLimits {
    /// Validates that every configured bound is within `[min, hard_max]`.
    pub fn validate(&self) -> Result<(), SqlError> {
        let checks = [
            ("max_sql_bytes", self.max_sql_bytes, 1, MAX_SQL_BYTES),
            ("max_tokens", self.max_tokens, 1, MAX_SQL_TOKENS),
            ("max_expr_depth", self.max_expr_depth, 1, MAX_SQL_EXPR_DEPTH),
            ("max_columns", self.max_columns, 1, MAX_SQL_COLUMNS),
            (
                "max_insert_rows",
                self.max_insert_rows,
                1,
                MAX_SQL_INSERT_ROWS,
            ),
            (
                "max_mutation_rows",
                self.max_mutation_rows,
                1,
                MAX_SQL_MUTATION_ROWS,
            ),
            (
                "max_select_rows",
                self.max_select_rows,
                1,
                MAX_SQL_SELECT_ROWS,
            ),
            ("max_sort_rows", self.max_sort_rows, 1, MAX_SQL_SORT_ROWS),
            ("max_groups", self.max_groups, 1, MAX_SQL_GROUPS),
            (
                "max_distinct_values",
                self.max_distinct_values,
                1,
                MAX_SQL_DISTINCT_VALUES,
            ),
            (
                "max_visited_rows",
                self.max_visited_rows,
                1,
                MAX_SQL_VISITED_ROWS,
            ),
            ("max_params", self.max_params, 0, MAX_SQL_PARAMS),
            ("max_text_bytes", self.max_text_bytes, 1, MAX_SQL_TEXT_BYTES),
        ];
        for (name, val, min, max) in checks {
            if val < min || val > max {
                return Err(SqlError::Bounds(format!(
                    "configured limit {name}={val} must be between {min} and {max}"
                )));
            }
        }
        Ok(())
    }
}

/// Typed SQL runtime value.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Boolean(bool),
}

impl SqlValue {
    pub(crate) fn kind_name(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Integer(_) => "INTEGER",
            Self::Real(_) => "REAL",
            Self::Text(_) => "TEXT",
            Self::Boolean(_) => "BOOLEAN",
        }
    }
}

/// Column SQL types persisted in the SQL catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum SqlType {
    Integer,
    Real,
    Text {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_chars: Option<usize>,
    },
    Boolean,
}

impl SqlType {
    pub(crate) fn display_name(self) -> &'static str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text { .. } => "TEXT",
            Self::Boolean => "BOOLEAN",
        }
    }

    pub(crate) fn to_field_type(self) -> FieldType {
        match self {
            Self::Integer => FieldType::Integer,
            Self::Real => FieldType::Number,
            Self::Text { .. } => FieldType::StringBytes {
                min: 0,
                max: MAX_SQL_TEXT_BYTES,
            },
            Self::Boolean => FieldType::Boolean,
        }
    }

    pub(crate) fn is_numeric(self) -> bool {
        matches!(self, Self::Integer | Self::Real)
    }

    pub(crate) fn matches_base(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Integer, Self::Integer)
                | (Self::Real, Self::Real)
                | (Self::Text { .. }, Self::Text { .. })
                | (Self::Boolean, Self::Boolean)
        )
    }
}

/// Result of executing one SQL statement.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlOutcome {
    TableCreated,
    IndexCreated,
    IndexDropped,
    TableDropped,
    TableAltered,
    Inserted {
        rows: usize,
    },
    Updated {
        rows: usize,
    },
    Deleted {
        rows: usize,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
    },
}

/// Errors returned by SQL parsing, binding, or execution.
#[derive(Debug)]
pub enum SqlError {
    /// Lexical or syntactic error with the byte offset in the SQL text.
    Syntax { offset: usize, message: String },
    /// Recognized SQL feature outside this embedded subset.
    Unsupported {
        feature: String,
        offset: Option<usize>,
    },
    /// Type mismatch or non-finite/non-representable numeric conversion.
    Type(String),
    /// Constraint violation (`NOT NULL`, duplicate primary key, `UNIQUE`, `VARCHAR(n)` length, PK requirements).
    Constraint(String),
    /// Referenced table does not exist.
    NoSuchTable(String),
    /// Referenced column does not exist on the table.
    NoSuchColumn { table: String, column: String },
    /// Referenced index does not exist on the table.
    NoSuchIndex { table: String, index: String },
    /// Table or collection already exists.
    TableExists(String),
    /// Index already exists on the table.
    IndexExists(String),
    /// Documented or configured limit exceeded.
    Bounds(String),
    /// Positional parameter count mismatch.
    ParameterCount { expected: usize, actual: usize },
    /// Pre-commit transaction conflict (`database_engine::is_transaction_conflict` is true).
    Conflict(io::Error),
    /// Commit I/O failure whose persistence outcome is unknown; caller must reconcile.
    OutcomeUnknown(io::Error),
    /// Other storage or I/O failure.
    Io(io::Error),
}

impl SqlError {
    /// Returns the wrapped `io::Error` if this error originated from storage/transactions.
    pub fn as_io_error(&self) -> Option<&io::Error> {
        match self {
            Self::Conflict(err) | Self::OutcomeUnknown(err) | Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax { offset, message } => {
                write!(f, "syntax error at byte {offset}: {message}")
            }
            Self::Unsupported {
                feature,
                offset: Some(offset),
            } => write!(f, "unsupported SQL feature at byte {offset}: {feature}"),
            Self::Unsupported {
                feature,
                offset: None,
            } => write!(f, "unsupported SQL feature: {feature}"),
            Self::Type(msg) => write!(f, "type error: {msg}"),
            Self::Constraint(msg) => write!(f, "constraint violation: {msg}"),
            Self::NoSuchTable(name) => write!(f, "no such table: {name}"),
            Self::NoSuchColumn { table, column } => {
                write!(f, "no such column: {table}.{column}")
            }
            Self::NoSuchIndex { table, index } => {
                write!(f, "no such index: {table}.{index}")
            }
            Self::TableExists(name) => write!(f, "table already exists: {name}"),
            Self::IndexExists(name) => write!(f, "index already exists: {name}"),
            Self::Bounds(msg) => write!(f, "bounds exceeded: {msg}"),
            Self::ParameterCount { expected, actual } => {
                write!(
                    f,
                    "parameter count mismatch: expected {expected}, got {actual}"
                )
            }
            Self::Conflict(err) => write!(f, "transaction conflict: {err}"),
            Self::OutcomeUnknown(err) => write!(f, "transaction outcome unknown: {err}"),
            Self::Io(err) => write!(f, "storage I/O error: {err}"),
        }
    }
}

impl std::error::Error for SqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conflict(err) | Self::OutcomeUnknown(err) | Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// Converts `i64` to `f64` only when exact, without precision loss.
pub(crate) fn exact_i64_to_f64(value: i64) -> Result<f64, SqlError> {
    let float = value as f64;
    if float.is_finite() && (float as i128) == (value as i128) {
        Ok(float)
    } else {
        Err(SqlError::Type(format!(
            "integer {value} is not exactly representable as REAL"
        )))
    }
}

/// Exact mathematical comparison between `i64` and finite `f64`.
pub(crate) fn compare_i64_f64(a: i64, b: f64) -> Ordering {
    debug_assert!(b.is_finite());
    // 2^63 as f64 is 9223372036854775808.0; -2^63 as f64 is -9223372036854775808.0.
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if b >= TWO_POW_63 {
        return Ordering::Less;
    }
    if b < -TWO_POW_63 {
        return Ordering::Greater;
    }
    let floor = b.floor();
    // Since -2^63 <= b < 2^63, floor is in [-2^63, 2^63 - 1], fitting exactly in i64.
    let fi = floor as i64;
    match a.cmp(&fi) {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        Ordering::Equal => {
            if b == floor {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        }
    }
}

/// Encodes an `i64` primary key into its 18-byte `i:<16 hex>` representation without allocating.
pub(crate) fn encode_i64_pk_bytes(n: i64, buf: &mut [u8; 18]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut unsigned = (n as u64) ^ (i64::MIN as u64);
    buf[0] = b'i';
    buf[1] = b':';
    for i in (2..18).rev() {
        buf[i] = HEX[(unsigned & 0xf) as usize];
        unsigned >>= 4;
    }
}

/// Encodes an `i64` primary key into an owned document ID string.
pub(crate) fn encode_i64_pk(n: i64) -> String {
    let mut buf = [0u8; 18];
    encode_i64_pk_bytes(n, &mut buf);
    // All bytes are ASCII ('i', ':', '0'..='9', 'a'..='f').
    String::from_utf8(buf.to_vec()).expect("ASCII hex PK")
}

/// Encodes a SQL primary key into a document ID whose ascending byte order equals
/// ascending SQL key order.
#[cfg(test)]
pub(crate) fn encode_primary_key(value: &SqlValue, pk_type: SqlType) -> Result<String, SqlError> {
    match (pk_type, value) {
        (SqlType::Integer, SqlValue::Integer(n)) => Ok(encode_i64_pk(*n)),
        (SqlType::Text { .. }, SqlValue::Text(s)) => {
            if s.len() > MAX_SQL_TEXT_PK_BYTES {
                return Err(SqlError::Bounds(format!(
                    "TEXT primary key of {} bytes exceeds the {MAX_SQL_TEXT_PK_BYTES}-byte limit (256-byte document ID cap)",
                    s.len()
                )));
            }
            Ok(format!("t:{s}"))
        }
        (_, SqlValue::Null) => Err(SqlError::Constraint(
            "PRIMARY KEY value cannot be NULL".into(),
        )),
        (expected, actual) => Err(SqlError::Type(format!(
            "PRIMARY KEY expects {}, got {}",
            expected.display_name(),
            actual.kind_name()
        ))),
    }
}

/// Validates and coerces a value for assignment into a column.
pub(crate) fn coerce_for_column(
    value: &SqlValue,
    col_name: &str,
    col_type: SqlType,
    not_null: bool,
    allow_int_to_real: bool,
    max_text_bytes: usize,
) -> Result<SqlValue, SqlError> {
    match value {
        SqlValue::Null => {
            if not_null {
                Err(SqlError::Constraint(format!(
                    "NOT NULL constraint failed on column '{col_name}'"
                )))
            } else {
                Ok(SqlValue::Null)
            }
        }
        SqlValue::Integer(n) => match col_type {
            SqlType::Integer => Ok(SqlValue::Integer(*n)),
            SqlType::Real if allow_int_to_real => Ok(SqlValue::Real(exact_i64_to_f64(*n)?)),
            _ => Err(SqlError::Type(format!(
                "column '{col_name}' expects {}, got INTEGER",
                col_type.display_name()
            ))),
        },
        SqlValue::Real(f) => match col_type {
            SqlType::Real => {
                if !f.is_finite() {
                    return Err(SqlError::Type(format!(
                        "column '{col_name}' rejects non-finite REAL value"
                    )));
                }
                Ok(SqlValue::Real(*f))
            }
            _ => Err(SqlError::Type(format!(
                "column '{col_name}' expects {}, got REAL",
                col_type.display_name()
            ))),
        },
        SqlValue::Text(s) => match col_type {
            SqlType::Text { max_chars } => {
                if s.len() > max_text_bytes {
                    return Err(SqlError::Bounds(format!(
                        "TEXT value of {} bytes on column '{col_name}' exceeds max_text_bytes ({max_text_bytes})",
                        s.len()
                    )));
                }
                if let Some(limit) = max_chars {
                    let chars = s.chars().count();
                    if chars > limit {
                        return Err(SqlError::Constraint(format!(
                            "VARCHAR({limit}) constraint failed on column '{col_name}': got {chars} characters"
                        )));
                    }
                }
                Ok(SqlValue::Text(s.clone()))
            }
            _ => Err(SqlError::Type(format!(
                "column '{col_name}' expects {}, got TEXT",
                col_type.display_name()
            ))),
        },
        SqlValue::Boolean(b) => match col_type {
            SqlType::Boolean => Ok(SqlValue::Boolean(*b)),
            _ => Err(SqlError::Type(format!(
                "column '{col_name}' expects {}, got BOOLEAN",
                col_type.display_name()
            ))),
        },
    }
}
