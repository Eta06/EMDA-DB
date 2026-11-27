#![no_main]

use database_engine::sql::SqlTransaction;
use database_engine::{
    CheckpointStore, DocumentDatabase, SqlDatabase, SqlError, SqlLimits, SqlOutcome, SqlValue,
    Store,
};
use libfuzzer_sys::fuzz_target;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

const MAX_RAW_SQL_BYTES: usize = 8 * 1024;
const MAX_STRUCTURED_OPS: usize = 24;
const VARCHAR_LIMIT: usize = 8;

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl Backend {
    fn sql(&self) -> SqlDatabase<'_> {
        match self {
            Self::Ordinary(store) => SqlDatabase::new(DocumentDatabase::new(store)),
            Self::Checkpoint(store) => {
                SqlDatabase::new(DocumentDatabase::with_checkpoint_store(store))
            }
        }
    }
}

fn open_backend(
    path: &Path,
    checkpoint: &Path,
    digest: Option<&[u8; 32]>,
    use_checkpoint: bool,
) -> Backend {
    if use_checkpoint {
        Backend::Checkpoint(
            CheckpointStore::open(path, checkpoint, digest.expect("checkpoint digest"))
                .expect("open checkpoint store"),
        )
    } else {
        Backend::Ordinary(Store::open(path).expect("open ordinary store"))
    }
}

fn assert_documented_error(err: &SqlError) {
    let _ = err.to_string();
    match err {
        SqlError::Syntax { .. }
        | SqlError::Unsupported { .. }
        | SqlError::Type(_)
        | SqlError::Constraint(_)
        | SqlError::NoSuchTable(_)
        | SqlError::NoSuchColumn { .. }
        | SqlError::NoSuchIndex { .. }
        | SqlError::TableExists(_)
        | SqlError::IndexExists(_)
        | SqlError::Bounds(_)
        | SqlError::ParameterCount { .. } => {}
        SqlError::TransactionAborted(_) => {}
        SqlError::Conflict(io_err) => {
            panic!("unexpected transaction conflict in single-threaded fuzz run: {io_err}");
        }
        SqlError::OutcomeUnknown(io_err) => {
            panic!("unexpected unknown outcome in single-threaded fuzz run: {io_err}");
        }
        SqlError::Io(io_err) => {
            panic!("unexpected storage I/O error in single-threaded fuzz run: {io_err}");
        }
    }
}

fn synthetic_param(byte: u8) -> SqlValue {
    match byte % 12 {
        0 => SqlValue::Null,
        1 => SqlValue::Integer(0),
        2 => SqlValue::Integer(1),
        3 => SqlValue::Integer(-1),
        4 => SqlValue::Integer(i64::MIN),
        5 => SqlValue::Integer(i64::MAX),
        6 => SqlValue::Integer((1_i64 << 53) + 1),
        7 => SqlValue::Real(1.5),
        8 => SqlValue::Real(-2.5),
        9 => SqlValue::Text("alpha".into()),
        10 => SqlValue::Text("it's".into()),
        _ => SqlValue::Boolean(byte & 16 != 0),
    }
}

/// Part (a): Feed arbitrary bytes as UTF-8-lossy SQL text to `prepare` / `execute` against
/// a temporary store with a small pre-created table.
fn fuzz_raw_sql(backend: &Backend, data: &[u8]) {
    let mut db = backend.sql();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, r REAL, s VARCHAR(16), b BOOLEAN)",
        &[],
    )
    .expect("create raw table");
    db.execute(
        "INSERT INTO t VALUES (1, 10, 1.5, 'alpha', TRUE), (2, NULL, -2.5, NULL, FALSE)",
        &[],
    )
    .expect("seed raw table");

    if data.first().copied().unwrap_or(0) & 0x80 != 0 {
        let tight = SqlLimits {
            max_sql_bytes: 512,
            max_tokens: 128,
            max_expr_depth: 12,
            max_columns: 16,
            max_insert_rows: 16,
            max_mutation_rows: 16,
            max_select_rows: 64,
            max_sort_rows: 64,
            max_groups: 64,
            max_distinct_values: 64,
            max_visited_rows: 128,
            max_params: 16,
            max_text_bytes: 256,
        };
        db.set_limits(tight).expect("valid tight limits");
    }

    let bounded = &data[..data.len().min(MAX_RAW_SQL_BYTES)];
    let sql = String::from_utf8_lossy(bounded);

    match db.prepare(&sql) {
        Err(err) => assert_documented_error(&err),
        Ok(stmt) => {
            if let Err(err) = db.execute_prepared(&stmt, &[]) {
                assert_documented_error(&err);
            }
            let needed = stmt.param_count();
            if needed > 0 && needed <= 16 {
                let params: Vec<SqlValue> = (0..needed)
                    .map(|idx| {
                        let b = if data.is_empty() {
                            idx as u8
                        } else {
                            data[idx % data.len()]
                        };
                        synthetic_param(b)
                    })
                    .collect();
                if let Err(err) = db.execute_prepared(&stmt, &params) {
                    assert_documented_error(&err);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Part (b): Structured differential SQL fuzzer against an in-memory BTreeMap
// ---------------------------------------------------------------------------

struct ByteCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn next(&mut self) -> Option<u8> {
        let b = *self.bytes.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn next_or(&mut self, fallback: u8) -> u8 {
        self.next().unwrap_or(fallback)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Pk {
    Int(i64),
    Text(String),
}

impl Pk {
    fn to_sql_value(&self) -> SqlValue {
        match self {
            Self::Int(n) => SqlValue::Integer(*n),
            Self::Text(s) => SqlValue::Text(s.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Col {
    Id,
    N,
    R,
    S,
    B,
    Req,
}

impl Col {
    const ALL: [Col; 6] = [Col::Id, Col::N, Col::R, Col::S, Col::B, Col::Req];

    fn from_byte(byte: u8) -> Self {
        Self::ALL[(byte as usize) % Self::ALL.len()]
    }

    fn canonical_name(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::N => "n",
            Self::R => "r",
            Self::S => "s",
            Self::B => "b",
            Self::Req => "req",
        }
    }

    fn sql_ident(self, style: u8) -> &'static str {
        match (self, style % 3) {
            (Self::Id, 0) => "id",
            (Self::Id, 1) => "ID",
            (Self::Id, _) => "\"id\"",
            (Self::N, 0) => "n",
            (Self::N, 1) => "N",
            (Self::N, _) => "\"n\"",
            (Self::R, 0) => "r",
            (Self::R, 1) => "R",
            (Self::R, _) => "\"r\"",
            (Self::S, 0) => "s",
            (Self::S, 1) => "S",
            (Self::S, _) => "\"s\"",
            (Self::B, 0) => "b",
            (Self::B, 1) => "B",
            (Self::B, _) => "\"b\"",
            (Self::Req, 0) => "req",
            (Self::Req, 1) => "REQ",
            (Self::Req, _) => "\"req\"",
        }
    }

    fn kind(self, text_pk: bool) -> ValType {
        match self {
            Self::Id if text_pk => ValType::Text,
            Self::Id | Self::N | Self::Req => ValType::Int,
            Self::R => ValType::Real,
            Self::S => ValType::Text,
            Self::B => ValType::Bool,
        }
    }

    fn not_null(self) -> bool {
        matches!(self, Self::Id | Self::Req)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValType {
    Int,
    Real,
    Text,
    Bool,
}

impl ValType {
    fn is_numeric(self) -> bool {
        matches!(self, Self::Int | Self::Real)
    }
}

#[derive(Clone, Debug)]
struct Row {
    id: Pk,
    n: Option<i64>,
    r: Option<f64>,
    s: Option<String>,
    b: Option<bool>,
    req: i64,
}

impl Row {
    fn get(&self, col: Col) -> SqlValue {
        match col {
            Col::Id => self.id.to_sql_value(),
            Col::N => self.n.map_or(SqlValue::Null, SqlValue::Integer),
            Col::R => self.r.map_or(SqlValue::Null, SqlValue::Real),
            Col::S => self.s.clone().map_or(SqlValue::Null, SqlValue::Text),
            Col::B => self.b.map_or(SqlValue::Null, SqlValue::Boolean),
            Col::Req => SqlValue::Integer(self.req),
        }
    }

    fn set(&mut self, col: Col, value: SqlValue) {
        match col {
            Col::Id => unreachable!("PK updates are rejected before row mutation"),
            Col::N => {
                self.n = match value {
                    SqlValue::Null => None,
                    SqlValue::Integer(v) => Some(v),
                    other => panic!("bad model n value: {other:?}"),
                };
            }
            Col::R => {
                self.r = match value {
                    SqlValue::Null => None,
                    SqlValue::Real(v) => Some(v),
                    other => panic!("bad model r value: {other:?}"),
                };
            }
            Col::S => {
                self.s = match value {
                    SqlValue::Null => None,
                    SqlValue::Text(v) => Some(v),
                    other => panic!("bad model s value: {other:?}"),
                };
            }
            Col::B => {
                self.b = match value {
                    SqlValue::Null => None,
                    SqlValue::Boolean(v) => Some(v),
                    other => panic!("bad model b value: {other:?}"),
                };
            }
            Col::Req => {
                self.req = match value {
                    SqlValue::Integer(v) => v,
                    other => panic!("bad model req value: {other:?}"),
                };
            }
        }
    }

    fn project(&self, cols: &[Col]) -> Vec<SqlValue> {
        cols.iter().map(|&c| self.get(c)).collect()
    }
}

#[derive(Clone, Debug)]
enum ModelError {
    Type,
    Constraint,
    Unsupported,
    Bounds,
}

fn assert_expected_error(actual: Result<SqlOutcome, SqlError>, expected: ModelError, sql: &str) {
    match (actual, expected) {
        (Err(SqlError::Type(_)), ModelError::Type)
        | (Err(SqlError::Constraint(_)), ModelError::Constraint)
        | (Err(SqlError::Unsupported { .. }), ModelError::Unsupported)
        | (Err(SqlError::Bounds(_)), ModelError::Bounds) => {}
        (other, exp) => panic!("expected {exp:?} for `{sql}`, got {other:?}"),
    }
}

fn exact_i64_to_f64(value: i64) -> Result<f64, ModelError> {
    let float = value as f64;
    if float.is_finite() && (float as i128) == (value as i128) {
        Ok(float)
    } else {
        Err(ModelError::Type)
    }
}

fn compare_i64_f64(a: i64, b: f64) -> Ordering {
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if b >= TWO_POW_63 {
        return Ordering::Less;
    }
    if b < -TWO_POW_63 {
        return Ordering::Greater;
    }
    let floor = b.floor();
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

fn compare_non_null(lhs: &SqlValue, rhs: &SqlValue) -> Result<Ordering, ModelError> {
    match (lhs, rhs) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => Ok(a.cmp(b)),
        (SqlValue::Real(a), SqlValue::Real(b)) => a.partial_cmp(b).ok_or(ModelError::Type),
        (SqlValue::Integer(a), SqlValue::Real(b)) => Ok(compare_i64_f64(*a, *b)),
        (SqlValue::Real(a), SqlValue::Integer(b)) => Ok(compare_i64_f64(*b, *a).reverse()),
        (SqlValue::Text(a), SqlValue::Text(b)) => Ok(a.as_bytes().cmp(b.as_bytes())),
        (SqlValue::Boolean(a), SqlValue::Boolean(b)) => Ok(a.cmp(b)),
        _ => Err(ModelError::Type),
    }
}

fn coerce_for_col(
    value: &SqlValue,
    col: Col,
    text_pk: bool,
    allow_int_to_real: bool,
) -> Result<SqlValue, ModelError> {
    match value {
        SqlValue::Null => {
            if col.not_null() {
                Err(ModelError::Constraint)
            } else {
                Ok(SqlValue::Null)
            }
        }
        SqlValue::Integer(n) => match col.kind(text_pk) {
            ValType::Int => Ok(SqlValue::Integer(*n)),
            ValType::Real if allow_int_to_real => Ok(SqlValue::Real(exact_i64_to_f64(*n)?)),
            _ => Err(ModelError::Type),
        },
        SqlValue::Real(f) => match col.kind(text_pk) {
            ValType::Real => {
                if !f.is_finite() {
                    Err(ModelError::Type)
                } else {
                    Ok(SqlValue::Real(*f))
                }
            }
            _ => Err(ModelError::Type),
        },
        SqlValue::Text(s) => match col.kind(text_pk) {
            ValType::Text => {
                if col == Col::S && s.chars().count() > VARCHAR_LIMIT {
                    Err(ModelError::Constraint)
                } else {
                    Ok(SqlValue::Text(s.clone()))
                }
            }
            _ => Err(ModelError::Type),
        },
        SqlValue::Boolean(b) => match col.kind(text_pk) {
            ValType::Bool => Ok(SqlValue::Boolean(*b)),
            _ => Err(ModelError::Type),
        },
    }
}

fn quote_sql_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn format_real_lit(v: f64) -> String {
    let s = format!("{v}");
    if s.contains('.') || s.contains('e') || s.contains('E') {
        s
    } else {
        format!("{s}.0")
    }
}

fn pick_int(byte: u8) -> i64 {
    match byte % 16 {
        0 => -4,
        1 => -2,
        2 => -1,
        3 => 0,
        4 => 1,
        5 => 2,
        6 => 3,
        7 => 4,
        8 => 10,
        9 => i64::MIN,
        10 => i64::MAX,
        11 => 1_i64 << 53,
        12 => (1_i64 << 53) + 1,
        13 => -(1_i64 << 53),
        14 => -(1_i64 << 53) - 1,
        _ => 42,
    }
}

fn pick_pk_int(byte: u8) -> i64 {
    match byte % 12 {
        0 => i64::MIN,
        1 => -3,
        2 => -1,
        3 => 0,
        4 => 1,
        5 => 2,
        6 => 3,
        7 => 4,
        8 => 5,
        9 => 6,
        10 => 7,
        _ => i64::MAX,
    }
}

fn pick_pk_text(byte: u8) -> String {
    match byte % 10 {
        0 => String::new(),
        1 => "a".into(),
        2 => "b".into(),
        3 => "c".into(),
        4 => "d".into(),
        5 => "Z".into(),
        6 => "it's".into(),
        7 => "é".into(),
        8 => "a b".into(),
        _ => "z".into(),
    }
}

fn pick_real(byte: u8) -> f64 {
    match byte % 12 {
        0 => -2.5,
        1 => -1.0,
        2 => 0.0,
        3 => 0.25,
        4 => 1.0,
        5 => 1.5,
        6 => 2.0,
        7 => 10.0,
        8 => (1_i64 << 53) as f64,
        9 => -((1_i64 << 53) as f64),
        10 => 9_223_372_036_854_775_808.0, // 2^63
        _ => -9_223_372_036_854_775_808.0, // -2^63
    }
}

fn pick_text(byte: u8) -> String {
    match byte % 10 {
        0 => String::new(),
        1 => "a".into(),
        2 => "Z".into(),
        3 => "it's".into(),
        4 => "éééé".into(),
        5 => "NULL".into(),
        6 => "beta".into(),
        7 => "12345678".into(),  // exact VARCHAR(8) boundary
        8 => "123456789".into(), // exceeds VARCHAR(8)
        _ => "--comment".into(),
    }
}

/// Generates a literal or `?` parameter value aimed at `target_type` (with occasional NULLs
/// and occasional intentional type mismatches / edge values).
fn gen_value_expr(
    cur: &mut ByteCursor<'_>,
    target_type: ValType,
    params: &mut Vec<SqlValue>,
    allow_null: bool,
    allow_mismatch: bool,
) -> (String, SqlValue, bool) {
    let tag = cur.next_or(0);
    let val_byte = cur.next_or(0);
    let use_param = tag & 1 != 0;

    if allow_null && tag.is_multiple_of(7) {
        if use_param {
            params.push(SqlValue::Null);
            return ("?".into(), SqlValue::Null, true);
        }
        return ("NULL".into(), SqlValue::Null, true);
    }

    if allow_mismatch && tag % 11 == 1 {
        let wrong = match target_type {
            ValType::Int => SqlValue::Text("bad".into()),
            ValType::Real => {
                if val_byte & 1 != 0 {
                    // Non-finite REAL parameter must fail with SqlError::Type.
                    let non_finite = match val_byte % 3 {
                        0 => f64::NAN,
                        1 => f64::INFINITY,
                        _ => f64::NEG_INFINITY,
                    };
                    params.push(SqlValue::Real(non_finite));
                    return ("?".into(), SqlValue::Real(non_finite), true);
                }
                SqlValue::Boolean(true)
            }
            ValType::Text => SqlValue::Integer(7),
            ValType::Bool => SqlValue::Integer(1),
        };
        return emit_value(wrong, use_param, params);
    }

    let val = match target_type {
        ValType::Int => SqlValue::Integer(pick_int(val_byte)),
        ValType::Real => {
            if tag.is_multiple_of(5) {
                // Exercise exact and non-exact INTEGER -> REAL literal/param coercion.
                SqlValue::Integer(pick_int(val_byte))
            } else {
                SqlValue::Real(pick_real(val_byte))
            }
        }
        ValType::Text => SqlValue::Text(pick_text(val_byte)),
        ValType::Bool => SqlValue::Boolean(val_byte & 1 != 0),
    };
    emit_value(val, use_param, params)
}

fn emit_value(
    val: SqlValue,
    use_param: bool,
    params: &mut Vec<SqlValue>,
) -> (String, SqlValue, bool) {
    if use_param {
        params.push(val.clone());
        ("?".into(), val, true)
    } else {
        let lit = match &val {
            SqlValue::Null => "NULL".into(),
            SqlValue::Integer(n) => format!("{n}"),
            SqlValue::Real(f) => format_real_lit(*f),
            SqlValue::Text(s) => quote_sql_str(s),
            SqlValue::Boolean(true) => "TRUE".into(),
            SqlValue::Boolean(false) => "FALSE".into(),
        };
        (lit, val, true)
    }
}

#[derive(Clone, Copy, Debug)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    fn from_byte(b: u8) -> (Self, &'static str) {
        match b % 7 {
            0 => (Self::Eq, "="),
            1 => (Self::Ne, "<>"),
            2 => (Self::Ne, "!="),
            3 => (Self::Lt, "<"),
            4 => (Self::Le, "<="),
            5 => (Self::Gt, ">"),
            _ => (Self::Ge, ">="),
        }
    }

    fn matches(self, ord: Ordering) -> bool {
        match self {
            Self::Eq => ord == Ordering::Equal,
            Self::Ne => ord != Ordering::Equal,
            Self::Lt => ord == Ordering::Less,
            Self::Le => ord != Ordering::Greater,
            Self::Gt => ord == Ordering::Greater,
            Self::Ge => ord != Ordering::Less,
        }
    }
}

#[derive(Clone, Debug)]
enum Operand {
    Col(Col),
    Val(SqlValue),
}

impl Operand {
    fn val_type(&self, text_pk: bool) -> Option<ValType> {
        match self {
            Self::Col(c) => Some(c.kind(text_pk)),
            Self::Val(SqlValue::Null) => None,
            Self::Val(SqlValue::Integer(_)) => Some(ValType::Int),
            Self::Val(SqlValue::Real(_)) => Some(ValType::Real),
            Self::Val(SqlValue::Text(_)) => Some(ValType::Text),
            Self::Val(SqlValue::Boolean(_)) => Some(ValType::Bool),
        }
    }

    fn eval(&self, row: &Row) -> SqlValue {
        match self {
            Self::Col(c) => row.get(*c),
            Self::Val(v) => v.clone(),
        }
    }
}

#[derive(Clone, Debug)]
enum WhereNode {
    BoolLit(Option<bool>),
    ColBool(Col),
    IsNull {
        col: Col,
        negated: bool,
    },
    Cmp {
        lhs: Operand,
        op: CmpOp,
        rhs: Operand,
    },
    Not(Box<WhereNode>),
    And(Box<WhereNode>, Box<WhereNode>),
    Or(Box<WhereNode>, Box<WhereNode>),
}

impl WhereNode {
    fn check_types(&self, text_pk: bool) -> Result<(), ModelError> {
        match self {
            Self::BoolLit(_) => Ok(()),
            Self::ColBool(col) => {
                if col.kind(text_pk) == ValType::Bool {
                    Ok(())
                } else {
                    Err(ModelError::Type)
                }
            }
            Self::IsNull { .. } => Ok(()),
            Self::Cmp { lhs, rhs, .. } => {
                if let Operand::Val(SqlValue::Real(f)) = lhs
                    && !f.is_finite()
                {
                    return Err(ModelError::Type);
                }
                if let Operand::Val(SqlValue::Real(f)) = rhs
                    && !f.is_finite()
                {
                    return Err(ModelError::Type);
                }
                match (lhs.val_type(text_pk), rhs.val_type(text_pk)) {
                    (None, _) | (_, None) => Ok(()),
                    (Some(a), Some(b)) if a == b || (a.is_numeric() && b.is_numeric()) => Ok(()),
                    _ => Err(ModelError::Type),
                }
            }
            Self::Not(inner) => inner.check_types(text_pk),
            Self::And(a, b) | Self::Or(a, b) => {
                a.check_types(text_pk)?;
                b.check_types(text_pk)
            }
        }
    }

    fn eval(&self, row: &Row) -> Option<bool> {
        match self {
            Self::BoolLit(b) => *b,
            Self::ColBool(col) => match row.get(*col) {
                SqlValue::Boolean(b) => Some(b),
                SqlValue::Null => None,
                other => panic!("non-boolean value in checked bool col: {other:?}"),
            },
            Self::IsNull { col, negated } => {
                let is_null = matches!(row.get(*col), SqlValue::Null);
                Some(if *negated { !is_null } else { is_null })
            }
            Self::Cmp { lhs, op, rhs } => {
                let lv = lhs.eval(row);
                let rv = rhs.eval(row);
                if matches!(lv, SqlValue::Null) || matches!(rv, SqlValue::Null) {
                    return None;
                }
                let ord = compare_non_null(&lv, &rv).expect("types pre-checked");
                Some(op.matches(ord))
            }
            Self::Not(inner) => inner.eval(row).map(|v| !v),
            Self::And(a, b) => match (a.eval(row), b.eval(row)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Self::Or(a, b) => match (a.eval(row), b.eval(row)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
        }
    }
}

fn gen_pk_val(
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    params: &mut Vec<SqlValue>,
) -> (String, SqlValue) {
    let b = cur.next_or(0);
    let use_param = cur.next_or(0) & 1 != 0;
    let val = if text_pk {
        SqlValue::Text(pick_pk_text(b))
    } else {
        SqlValue::Integer(pick_pk_int(b))
    };
    let (sql, v, _) = emit_value(val, use_param, params);
    (sql, v)
}

fn gen_where_atom(
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    params: &mut Vec<SqlValue>,
) -> (String, WhereNode) {
    let shape = cur.next_or(0) % 10;
    let style = cur.next_or(0);
    match shape {
        0 | 1 => {
            // Primary-key point or range comparison: `id <op> val`
            let (op, op_str) = CmpOp::from_byte(cur.next_or(0));
            let (rhs_sql, rhs_val) = gen_pk_val(cur, text_pk, params);
            let lhs_sql = Col::Id.sql_ident(style);
            (
                format!("{lhs_sql} {op_str} {rhs_sql}"),
                WhereNode::Cmp {
                    lhs: Operand::Col(Col::Id),
                    op,
                    rhs: Operand::Val(rhs_val),
                },
            )
        }
        2 => {
            // Primary-key range conjunction: `(id >= lo AND id <= hi)`
            let (lo_op, lo_str) = if cur.next_or(0) & 1 == 0 {
                (CmpOp::Ge, ">=")
            } else {
                (CmpOp::Gt, ">")
            };
            let (hi_op, hi_str) = if cur.next_or(0) & 1 == 0 {
                (CmpOp::Le, "<=")
            } else {
                (CmpOp::Lt, "<")
            };
            let (lo_sql, lo_val) = gen_pk_val(cur, text_pk, params);
            let (hi_sql, hi_val) = gen_pk_val(cur, text_pk, params);
            let id_sql = Col::Id.sql_ident(style);
            let left = WhereNode::Cmp {
                lhs: Operand::Col(Col::Id),
                op: lo_op,
                rhs: Operand::Val(lo_val),
            };
            let right = WhereNode::Cmp {
                lhs: Operand::Col(Col::Id),
                op: hi_op,
                rhs: Operand::Val(hi_val),
            };
            (
                format!("({id_sql} {lo_str} {lo_sql} AND {id_sql} {hi_str} {hi_sql})"),
                WhereNode::And(Box::new(left), Box::new(right)),
            )
        }
        3 => {
            // Reversed PK comparison: `val <op> id`
            let (op, op_str) = CmpOp::from_byte(cur.next_or(0));
            let (lhs_sql, lhs_val) = gen_pk_val(cur, text_pk, params);
            let rhs_sql = Col::Id.sql_ident(style);
            (
                format!("{lhs_sql} {op_str} {rhs_sql}"),
                WhereNode::Cmp {
                    lhs: Operand::Val(lhs_val),
                    op,
                    rhs: Operand::Col(Col::Id),
                },
            )
        }
        4 => {
            // Column IS [NOT] NULL
            let col = Col::from_byte(cur.next_or(0));
            let negated = cur.next_or(0) & 1 != 0;
            let sql = if negated {
                format!("{} IS NOT NULL", col.sql_ident(style))
            } else {
                format!("{} IS NULL", col.sql_ident(style))
            };
            (sql, WhereNode::IsNull { col, negated })
        }
        5 | 6 => {
            // Arbitrary column compared against literal/param (including NULL and rare mismatch)
            let col = Col::from_byte(cur.next_or(0));
            let (op, op_str) = CmpOp::from_byte(cur.next_or(0));
            let (rhs_sql, rhs_val, _) = gen_value_expr(cur, col.kind(text_pk), params, true, true);
            (
                format!("{} {op_str} {rhs_sql}", col.sql_ident(style)),
                WhereNode::Cmp {
                    lhs: Operand::Col(col),
                    op,
                    rhs: Operand::Val(rhs_val),
                },
            )
        }
        7 => {
            // Cross-column exact numeric comparison `n <op> r` or `n <op> req`
            let (op, op_str) = CmpOp::from_byte(cur.next_or(0));
            let rhs_col = if cur.next_or(0) & 1 == 0 {
                Col::R
            } else {
                Col::Req
            };
            (
                format!(
                    "{} {op_str} {}",
                    Col::N.sql_ident(style),
                    rhs_col.sql_ident(style)
                ),
                WhereNode::Cmp {
                    lhs: Operand::Col(Col::N),
                    op,
                    rhs: Operand::Col(rhs_col),
                },
            )
        }
        8 => {
            // Boolean column directly in predicate (or occasional non-boolean col to test Type error)
            let col = if cur.next_or(0).is_multiple_of(5) {
                Col::N
            } else {
                Col::B
            };
            (col.sql_ident(style).to_string(), WhereNode::ColBool(col))
        }
        _ => {
            // Boolean / NULL literal for 3VL testing
            match cur.next_or(0) % 3 {
                0 => ("TRUE".into(), WhereNode::BoolLit(Some(true))),
                1 => ("FALSE".into(), WhereNode::BoolLit(Some(false))),
                _ => ("NULL".into(), WhereNode::BoolLit(None)),
            }
        }
    }
}

fn gen_where_expr(
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    params: &mut Vec<SqlValue>,
    depth: usize,
) -> (String, WhereNode) {
    if depth == 0 {
        return gen_where_atom(cur, text_pk, params);
    }
    match cur.next_or(0) % 6 {
        0 | 1 => gen_where_atom(cur, text_pk, params),
        2 => {
            let (sub_sql, sub_node) = gen_where_expr(cur, text_pk, params, depth - 1);
            (
                format!("NOT ({sub_sql})"),
                WhereNode::Not(Box::new(sub_node)),
            )
        }
        3 | 4 => {
            let (l_sql, l_node) = gen_where_expr(cur, text_pk, params, depth - 1);
            let (r_sql, r_node) = gen_where_expr(cur, text_pk, params, depth - 1);
            (
                format!("({l_sql} AND {r_sql})"),
                WhereNode::And(Box::new(l_node), Box::new(r_node)),
            )
        }
        _ => {
            let (l_sql, l_node) = gen_where_expr(cur, text_pk, params, depth - 1);
            let (r_sql, r_node) = gen_where_expr(cur, text_pk, params, depth - 1);
            (
                format!("({l_sql} OR {r_sql})"),
                WhereNode::Or(Box::new(l_node), Box::new(r_node)),
            )
        }
    }
}

fn check_full_table(db: &SqlDatabase<'_>, model: &BTreeMap<Pk, Row>) {
    let outcome = db.execute("SELECT * FROM m", &[]).expect("SELECT * FROM m");
    let expected_cols: Vec<String> = Col::ALL
        .iter()
        .map(|c| c.canonical_name().to_string())
        .collect();
    let expected_rows: Vec<Vec<SqlValue>> = model.values().map(|r| r.project(&Col::ALL)).collect();
    assert_eq!(
        outcome,
        SqlOutcome::Rows {
            columns: expected_cols,
            rows: expected_rows,
        }
    );
}

trait SqlExec {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError>;
    fn exec_forced(&mut self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError>;
}

impl SqlExec for &SqlDatabase<'_> {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError> {
        self.execute(sql, params)
    }

    fn exec_forced(&mut self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError> {
        self.execute_force_full_scan_for_test(sql, params)
    }
}

impl SqlExec for &mut SqlTransaction<'_> {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError> {
        self.execute(sql, params)
    }

    fn exec_forced(&mut self, sql: &str, params: &[SqlValue]) -> Result<SqlOutcome, SqlError> {
        self.execute_force_full_scan_for_test(sql, params)
    }
}

fn exec_insert_op_with(
    db: &mut dyn SqlExec,
    model: &mut BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    has_unique_n: bool,
) -> Result<(), SqlError> {
    let row_count = 1 + (cur.next_or(0) % 3) as usize;
    let explicit_cols = cur.next_or(0) & 1 != 0;
    let omit_req = explicit_cols && cur.next_or(0).is_multiple_of(9);

    let cols: Vec<Col> = if omit_req {
        vec![Col::Id, Col::N, Col::R, Col::S, Col::B]
    } else {
        Col::ALL.to_vec()
    };

    let mut sql = String::from("INSERT INTO m");
    if explicit_cols {
        sql.push_str(" (");
        for (i, c) in cols.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(c.sql_ident(i as u8));
        }
        sql.push(')');
    }
    sql.push_str(" VALUES ");

    let mut params = Vec::new();
    let mut tuples: Vec<Vec<(SqlValue, bool)>> = Vec::with_capacity(row_count);

    for r_idx in 0..row_count {
        if r_idx > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        let mut vals = Vec::with_capacity(cols.len());
        for (c_idx, &col) in cols.iter().enumerate() {
            if c_idx > 0 {
                sql.push_str(", ");
            }
            let (expr_sql, val, allow_int_to_real) = if col == Col::Id {
                // Occasionally emit NULL PK to test NOT NULL constraint.
                if cur.next_or(0).is_multiple_of(13) {
                    ("NULL".into(), SqlValue::Null, true)
                } else {
                    let (s, v) = gen_pk_val(cur, text_pk, &mut params);
                    (s, v, true)
                }
            } else {
                gen_value_expr(cur, col.kind(text_pk), &mut params, true, true)
            };
            sql.push_str(&expr_sql);
            vals.push((val, allow_int_to_real));
        }
        sql.push(')');
        tuples.push(vals);
    }

    // Check parameter-level non-finite REAL error first (matches execute_statement check).
    let mut expected_err: Option<ModelError> = None;
    for p in &params {
        if let SqlValue::Real(f) = p
            && !f.is_finite()
        {
            expected_err = Some(ModelError::Type);
            break;
        }
    }

    let mut staged_rows: Vec<(Pk, Row)> = Vec::new();
    if expected_err.is_none() && omit_req {
        expected_err = Some(ModelError::Constraint);
    }

    if expected_err.is_none() {
        let mut seen_pks = HashSet::new();
        'outer: for tuple in &tuples {
            let mut pk_val: Option<Pk> = None;
            let mut n = None;
            let mut r = None;
            let mut s = None;
            let mut b = None;
            let mut req = 0_i64;
            for (&col, (raw_val, allow_i2r)) in cols.iter().zip(tuple) {
                match coerce_for_col(raw_val, col, text_pk, *allow_i2r) {
                    Ok(coerced) => match col {
                        Col::Id => {
                            pk_val = Some(match coerced {
                                SqlValue::Integer(v) => Pk::Int(v),
                                SqlValue::Text(v) => Pk::Text(v),
                                other => panic!("unexpected coerced pk: {other:?}"),
                            });
                        }
                        Col::N => {
                            n = match coerced {
                                SqlValue::Null => None,
                                SqlValue::Integer(v) => Some(v),
                                _ => unreachable!(),
                            };
                        }
                        Col::R => {
                            r = match coerced {
                                SqlValue::Null => None,
                                SqlValue::Real(v) => Some(v),
                                _ => unreachable!(),
                            };
                        }
                        Col::S => {
                            s = match coerced {
                                SqlValue::Null => None,
                                SqlValue::Text(v) => Some(v),
                                _ => unreachable!(),
                            };
                        }
                        Col::B => {
                            b = match coerced {
                                SqlValue::Null => None,
                                SqlValue::Boolean(v) => Some(v),
                                _ => unreachable!(),
                            };
                        }
                        Col::Req => {
                            req = match coerced {
                                SqlValue::Integer(v) => v,
                                _ => unreachable!(),
                            };
                        }
                    },
                    Err(e) => {
                        expected_err = Some(e);
                        break 'outer;
                    }
                }
            }
            let pk = pk_val.expect("pk column present");
            if !seen_pks.insert(pk.clone()) {
                expected_err = Some(ModelError::Constraint);
                break;
            }
            staged_rows.push((
                pk.clone(),
                Row {
                    id: pk,
                    n,
                    r,
                    s,
                    b,
                    req,
                },
            ));
        }
    }

    if expected_err.is_none() && staged_rows.iter().any(|(pk, _)| model.contains_key(pk)) {
        expected_err = Some(ModelError::Constraint);
    }
    if expected_err.is_none() && has_unique_n {
        let mut seen_n = HashSet::new();
        for (_, row) in &staged_rows {
            if let Some(n_val) = row.n
                && (!seen_n.insert(n_val)
                    || model.values().any(|existing| existing.n == Some(n_val)))
            {
                expected_err = Some(ModelError::Constraint);
                break;
            }
        }
    }

    let actual = db.exec(&sql, &params);
    if let Some(exp_err) = expected_err {
        let err = actual.as_ref().err().map(|e| e.to_string());
        let is_expected = actual.is_err();
        assert_expected_error(actual, exp_err, &sql);
        if is_expected {
            return Err(SqlError::Constraint(err.unwrap_or_default()));
        }
        Ok(())
    } else {
        assert_eq!(
            actual.unwrap_or_else(|e| panic!("INSERT `{sql}` failed: {e}")),
            SqlOutcome::Inserted {
                rows: staged_rows.len(),
            }
        );
        for (pk, row) in staged_rows {
            model.insert(pk, row);
        }
        Ok(())
    }
}

fn exec_insert_op(
    db: &SqlDatabase<'_>,
    model: &mut BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    has_unique_n: bool,
) {
    let mut runner = db;
    let _ = exec_insert_op_with(&mut runner, model, cur, text_pk, has_unique_n);
}

fn exec_select_op_with(
    db: &mut dyn SqlExec,
    model: &BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    check_forced_scan: bool,
) -> Result<(), SqlError> {
    let select_all = cur.next_or(0) & 1 == 0;
    let mut cols = Vec::new();
    let mut sql = String::from("SELECT ");
    if select_all {
        sql.push('*');
        cols.extend_from_slice(&Col::ALL);
    } else {
        let count = 1 + (cur.next_or(0) % 4) as usize;
        for i in 0..count {
            if i > 0 {
                sql.push_str(", ");
            }
            let c = Col::from_byte(cur.next_or(0));
            sql.push_str(c.sql_ident(cur.next_or(0)));
            cols.push(c);
        }
    }
    sql.push_str(" FROM m");

    let mut params = Vec::new();
    let where_node = if !cur.next_or(0).is_multiple_of(5) {
        let (w_sql, w_node) = gen_where_expr(cur, text_pk, &mut params, 2);
        sql.push_str(" WHERE ");
        sql.push_str(&w_sql);
        Some(w_node)
    } else {
        None
    };

    let mut order_spec: Vec<(Col, bool, bool)> = Vec::new();
    if cur.next_or(0) & 1 != 0 {
        sql.push_str(" ORDER BY ");
        let count = 1 + (cur.next_or(0) % 3) as usize;
        for i in 0..count {
            if i > 0 {
                sql.push_str(", ");
            }
            let c = Col::from_byte(cur.next_or(0));
            let desc = cur.next_or(0) & 1 != 0;
            let nulls_opt = match cur.next_or(0) % 3 {
                0 => None,
                1 => Some(true),
                _ => Some(false),
            };
            sql.push_str(c.sql_ident(cur.next_or(0)));
            if desc {
                sql.push_str(" DESC");
            } else if cur.next_or(0) & 1 != 0 {
                sql.push_str(" ASC");
            }
            if let Some(nf) = nulls_opt {
                sql.push_str(if nf { " NULLS FIRST" } else { " NULLS LAST" });
            }
            order_spec.push((c, desc, nulls_opt.unwrap_or(!desc)));
        }
    }

    let (limit, offset) = if cur.next_or(0) & 1 != 0 {
        let lim = (cur.next_or(0) % 8) as usize;
        if cur.next_or(0) & 1 != 0 {
            sql.push_str(" LIMIT ?");
            params.push(SqlValue::Integer(lim as i64));
        } else {
            sql.push_str(&format!(" LIMIT {lim}"));
        }
        let off = if cur.next_or(0) & 1 != 0 {
            let o = (cur.next_or(0) % 6) as usize;
            if cur.next_or(0) & 1 != 0 {
                sql.push_str(" OFFSET ?");
                params.push(SqlValue::Integer(o as i64));
            } else {
                sql.push_str(&format!(" OFFSET {o}"));
            }
            o
        } else {
            0
        };
        (Some(lim), off)
    } else {
        (None, 0)
    };

    let mut bind_err = None;
    for p in &params {
        if let SqlValue::Real(f) = p
            && !f.is_finite()
        {
            bind_err = Some(ModelError::Type);
            break;
        }
    }
    if bind_err.is_none()
        && let Some(ref w) = where_node
        && let Err(e) = w.check_types(text_pk)
    {
        bind_err = Some(e);
    }

    let actual = db.exec(&sql, &params);
    if let Some(exp_err) = bind_err {
        let err = actual.as_ref().err().map(|e| e.to_string());
        assert_expected_error(actual, exp_err, &sql);
        return Err(SqlError::Type(err.unwrap_or_default()));
    }

    let expected_cols: Vec<String> = cols
        .iter()
        .map(|c| c.canonical_name().to_string())
        .collect();
    let mut matched_rows: Vec<&Row> = model
        .values()
        .filter(|row| {
            where_node
                .as_ref()
                .is_none_or(|w| w.eval(row) == Some(true))
        })
        .collect();
    if !order_spec.is_empty() {
        matched_rows.sort_by(|a, b| {
            for &(col, desc, nulls_first) in &order_spec {
                let lv = a.get(col);
                let rv = b.get(col);
                let ord = match (&lv, &rv) {
                    (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
                    (SqlValue::Null, _) => {
                        if nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (_, SqlValue::Null) => {
                        if nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    _ => {
                        let o = compare_non_null(&lv, &rv).expect("same column type");
                        if desc { o.reverse() } else { o }
                    }
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            a.id.cmp(&b.id)
        });
    }
    let expected_rows: Vec<Vec<SqlValue>> = matched_rows
        .into_iter()
        .skip(offset)
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| row.project(&cols))
        .collect();
    let outcome = actual.unwrap_or_else(|e| panic!("SELECT `{sql}` failed: {e}"));
    if check_forced_scan {
        let scanned = db
            .exec_forced(&sql, &params)
            .unwrap_or_else(|e| panic!("forced-scan SELECT `{sql}` failed: {e}"));
        assert_eq!(
            outcome, scanned,
            "indexed vs forced-scan mismatch for `{sql}` params={params:?}"
        );
    }
    assert_eq!(
        outcome,
        SqlOutcome::Rows {
            columns: expected_cols,
            rows: expected_rows,
        },
        "SELECT mismatch for `{sql}` with params={params:?}"
    );
    Ok(())
}

fn exec_select_op(
    db: &SqlDatabase<'_>,
    model: &BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    check_forced_scan: bool,
) {
    let mut runner = db;
    let _ = exec_select_op_with(&mut runner, model, cur, text_pk, check_forced_scan);
}

#[derive(Clone, Debug)]
enum AssignRhs {
    Col(Col),
    Val {
        val: SqlValue,
        allow_int_to_real: bool,
        is_param: bool,
    },
}

fn exec_update_op_with(
    db: &mut dyn SqlExec,
    model: &mut BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    has_unique_n: bool,
) -> Result<(), SqlError> {
    let mut params = Vec::new();
    let mut assigns: Vec<(Col, AssignRhs)> = Vec::new();
    let mut sql = String::from("UPDATE m SET ");

    let mode = cur.next_or(0) % 6;
    if mode == 0 {
        // Attempt PK update -> must fail with Unsupported.
        let before_params = params.len();
        let (v_sql, v_val) = gen_pk_val(cur, text_pk, &mut params);
        sql.push_str(&format!("id = {v_sql}"));
        assigns.push((
            Col::Id,
            AssignRhs::Val {
                val: v_val,
                allow_int_to_real: true,
                is_param: params.len() > before_params,
            },
        ));
    } else if mode == 1 {
        // Simultaneous column swap `n = req, req = n` (tests reading old row for all SET exprs).
        sql.push_str("n = req, req = n");
        assigns.push((Col::N, AssignRhs::Col(Col::Req)));
        assigns.push((Col::Req, AssignRhs::Col(Col::N)));
    } else {
        let count = 1 + (cur.next_or(0) % 2) as usize;
        let non_pk = [Col::N, Col::R, Col::S, Col::B, Col::Req];
        let mut used = Vec::new();
        for i in 0..count {
            let col = non_pk[(cur.next_or(0) as usize) % non_pk.len()];
            if used.contains(&col) {
                continue;
            }
            used.push(col);
            if !assigns.is_empty() {
                sql.push_str(", ");
            }
            let (rhs_sql, rhs) = if cur.next_or(0).is_multiple_of(5) {
                // Column-to-column assignment (including int col -> real col Type error probe).
                let src = Col::from_byte(cur.next_or(0));
                (src.sql_ident(i as u8).to_string(), AssignRhs::Col(src))
            } else {
                let before_params = params.len();
                let (s, v, allow_i2r) =
                    gen_value_expr(cur, col.kind(text_pk), &mut params, true, true);
                (
                    s,
                    AssignRhs::Val {
                        val: v,
                        allow_int_to_real: allow_i2r,
                        is_param: params.len() > before_params,
                    },
                )
            };
            sql.push_str(&format!("{} = {rhs_sql}", col.sql_ident(i as u8)));
            assigns.push((col, rhs));
        }
    }

    let where_node = if !cur.next_or(0).is_multiple_of(4) {
        let (w_sql, w_node) = gen_where_expr(cur, text_pk, &mut params, 2);
        sql.push_str(" WHERE ");
        sql.push_str(&w_sql);
        Some(w_node)
    } else {
        None
    };

    // 1. Parameter non-finite REAL check.
    let mut expected_err: Option<ModelError> = None;
    let mut has_non_exact_int_param_to_real = false;
    for p in &params {
        if let SqlValue::Real(f) = p
            && !f.is_finite()
        {
            expected_err = Some(ModelError::Type);
            break;
        }
    }

    // 2. Bind-time assignment checks (PK unsupported, assignment type compatibility).
    if expected_err.is_none() {
        for (col, rhs) in &assigns {
            if *col == Col::Id {
                expected_err = Some(ModelError::Unsupported);
                break;
            }
            let target_ty = col.kind(text_pk);
            match rhs {
                AssignRhs::Col(src) => {
                    if src.kind(text_pk) != target_ty {
                        expected_err = Some(ModelError::Type);
                        break;
                    }
                }
                AssignRhs::Val {
                    val,
                    allow_int_to_real,
                    is_param,
                } => match val {
                    SqlValue::Null => {}
                    SqlValue::Integer(n) => {
                        if target_ty == ValType::Int {
                            // exact match
                        } else if target_ty == ValType::Real && *allow_int_to_real {
                            if exact_i64_to_f64(*n).is_err() {
                                if *is_param {
                                    has_non_exact_int_param_to_real = true;
                                } else {
                                    expected_err = Some(ModelError::Type);
                                    break;
                                }
                            }
                        } else {
                            expected_err = Some(ModelError::Type);
                            break;
                        }
                    }
                    SqlValue::Real(_) if target_ty == ValType::Real => {}
                    SqlValue::Text(_) if target_ty == ValType::Text => {}
                    SqlValue::Boolean(_) if target_ty == ValType::Bool => {}
                    _ => {
                        expected_err = Some(ModelError::Type);
                        break;
                    }
                },
            }
        }
    }

    // 3. Bind-time WHERE type check.
    if expected_err.is_none()
        && let Some(ref w) = where_node
        && let Err(e) = w.check_types(text_pk)
    {
        expected_err = Some(e);
    }

    // 4. Per-matched-row evaluation & coercion (atomic: any failure rolls back all rows).
    let mut staged_updates: Vec<(Pk, Row)> = Vec::new();
    if expected_err.is_none() {
        'rows: for (pk, old_row) in model.iter() {
            if !where_node
                .as_ref()
                .is_none_or(|w| w.eval(old_row) == Some(true))
            {
                continue;
            }
            let mut new_row = old_row.clone();
            for (col, rhs) in &assigns {
                let (val, allow_i2r) = match rhs {
                    AssignRhs::Col(src) => (old_row.get(*src), false),
                    AssignRhs::Val {
                        val,
                        allow_int_to_real,
                        ..
                    } => (val.clone(), *allow_int_to_real),
                };
                match coerce_for_col(&val, *col, text_pk, allow_i2r) {
                    Ok(coerced) => new_row.set(*col, coerced),
                    Err(e) => {
                        expected_err = Some(e);
                        break 'rows;
                    }
                }
            }
            staged_updates.push((pk.clone(), new_row));
        }
    }

    let mut transient_unique_swap = false;
    if expected_err.is_none() && has_unique_n {
        let mut live_n: BTreeMap<Pk, Option<i64>> =
            model.iter().map(|(k, r)| (k.clone(), r.n)).collect();
        for (pk, new_row) in &staged_updates {
            if let Some(new_n) = new_row.n
                && live_n
                    .iter()
                    .any(|(other_pk, other_n)| other_pk != pk && *other_n == Some(new_n))
            {
                if assigns
                    .iter()
                    .any(|(c, rhs)| *c == Col::N && matches!(rhs, AssignRhs::Col(_)))
                {
                    transient_unique_swap = true;
                } else {
                    expected_err = Some(ModelError::Constraint);
                }
                break;
            }
            live_n.insert(pk.clone(), new_row.n);
        }
    }

    let actual = db.exec(&sql, &params);
    if let Some(exp_err) = expected_err {
        let err = actual.as_ref().err().map(|e| e.to_string());
        assert_expected_error(actual, exp_err, &sql);
        Err(SqlError::Constraint(err.unwrap_or_default()))
    } else if transient_unique_swap && matches!(actual, Err(SqlError::Constraint(_))) {
        // Row-by-row unique check may fail on transient swap; table remains unchanged.
        Err(SqlError::Constraint("transient unique swap".into()))
    } else if staged_updates.is_empty() && has_non_exact_int_param_to_real {
        // Non-exact INTEGER param assigned to REAL when 0 rows match may fail at bind time
        // or succeed with 0 updated rows; either way, model is unchanged.
        match actual {
            Ok(SqlOutcome::Updated { rows: 0 }) => Ok(()),
            Err(SqlError::Type(msg)) => Err(SqlError::Type(msg)),
            other => {
                panic!("unexpected result for 0-row non-exact int param UPDATE `{sql}`: {other:?}")
            }
        }
    } else {
        assert_eq!(
            actual.unwrap_or_else(|e| panic!("UPDATE `{sql}` failed: {e}")),
            SqlOutcome::Updated {
                rows: staged_updates.len(),
            }
        );
        for (pk, row) in staged_updates {
            model.insert(pk, row);
        }
        Ok(())
    }
}

fn exec_update_op(
    db: &SqlDatabase<'_>,
    model: &mut BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    has_unique_n: bool,
) {
    let mut runner = db;
    let _ = exec_update_op_with(&mut runner, model, cur, text_pk, has_unique_n);
}

fn exec_delete_op_with(
    db: &mut dyn SqlExec,
    model: &mut BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
) -> Result<(), SqlError> {
    let mut sql = String::from("DELETE FROM m");
    let mut params = Vec::new();
    let where_node = if !cur.next_or(0).is_multiple_of(5) {
        let (w_sql, w_node) = gen_where_expr(cur, text_pk, &mut params, 2);
        sql.push_str(" WHERE ");
        sql.push_str(&w_sql);
        Some(w_node)
    } else {
        None
    };

    let mut bind_err = None;
    for p in &params {
        if let SqlValue::Real(f) = p
            && !f.is_finite()
        {
            bind_err = Some(ModelError::Type);
            break;
        }
    }
    if bind_err.is_none()
        && let Some(ref w) = where_node
        && let Err(e) = w.check_types(text_pk)
    {
        bind_err = Some(e);
    }

    let actual = db.exec(&sql, &params);
    if let Some(exp_err) = bind_err {
        let err = actual.as_ref().err().map(|e| e.to_string());
        assert_expected_error(actual, exp_err, &sql);
        return Err(SqlError::Type(err.unwrap_or_default()));
    }

    let to_delete: Vec<Pk> = model
        .iter()
        .filter(|(_, row)| {
            where_node
                .as_ref()
                .is_none_or(|w| w.eval(row) == Some(true))
        })
        .map(|(pk, _)| pk.clone())
        .collect();
    assert_eq!(
        actual.unwrap_or_else(|e| panic!("DELETE `{sql}` failed: {e}")),
        SqlOutcome::Deleted {
            rows: to_delete.len(),
        }
    );
    for pk in to_delete {
        model.remove(&pk);
    }
    Ok(())
}

fn exec_delete_op(
    db: &SqlDatabase<'_>,
    model: &mut BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
) {
    let mut runner = db;
    let _ = exec_delete_op_with(&mut runner, model, cur, text_pk);
}

fn model_scalar_key(v: &SqlValue) -> (u8, i64, u64, String, bool) {
    match v {
        SqlValue::Null => (0, 0, 0, String::new(), false),
        SqlValue::Integer(n) => (1, *n, 0, String::new(), false),
        SqlValue::Real(f) => {
            let norm = if *f == 0.0 { 0.0 } else { *f };
            (2, 0, norm.to_bits(), String::new(), false)
        }
        SqlValue::Text(s) => (3, 0, 0, s.clone(), false),
        SqlValue::Boolean(b) => (4, 0, 0, String::new(), *b),
    }
}

fn model_cmp_values(a: &SqlValue, b: &SqlValue, desc: bool, nulls_first: bool) -> Ordering {
    match (a, b) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, SqlValue::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {
            let ord = compare_non_null(a, b).expect("same column type");
            if desc { ord.reverse() } else { ord }
        }
    }
}

fn exec_aggregate_select_op_with(
    db: &mut dyn SqlExec,
    model: &BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    check_forced_scan: bool,
) -> Result<(), SqlError> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum AggOp {
        CountStar,
        CountCol(Col),
        CountDistinct(Col),
        Sum(Col),
        Avg(Col),
        Min(Col),
        Max(Col),
    }

    impl AggOp {
        fn sql(self, style: u8) -> String {
            match self {
                Self::CountStar => "COUNT(*)".into(),
                Self::CountCol(c) => format!("COUNT({})", c.sql_ident(style)),
                Self::CountDistinct(c) => format!("COUNT(DISTINCT {})", c.sql_ident(style)),
                Self::Sum(c) => format!("SUM({})", c.sql_ident(style)),
                Self::Avg(c) => format!("AVG({})", c.sql_ident(style)),
                Self::Min(c) => format!("MIN({})", c.sql_ident(style)),
                Self::Max(c) => format!("MAX({})", c.sql_ident(style)),
            }
        }
        fn default_name(self) -> String {
            match self {
                Self::CountStar => "count(*)".into(),
                Self::CountCol(c) => format!("count({})", c.canonical_name()),
                Self::CountDistinct(c) => format!("count(distinct {})", c.canonical_name()),
                Self::Sum(c) => format!("sum({})", c.canonical_name()),
                Self::Avg(c) => format!("avg({})", c.canonical_name()),
                Self::Min(c) => format!("min({})", c.canonical_name()),
                Self::Max(c) => format!("max({})", c.canonical_name()),
            }
        }
        fn check_type(self, text_pk: bool) -> Result<(), ModelError> {
            match self {
                Self::Sum(c) | Self::Avg(c) if !c.kind(text_pk).is_numeric() => {
                    Err(ModelError::Type)
                }
                _ => Ok(()),
            }
        }
        fn eval(self, rows: &[&Row]) -> Result<SqlValue, ModelError> {
            match self {
                Self::CountStar => Ok(SqlValue::Integer(rows.len() as i64)),
                Self::CountCol(c) => {
                    let cnt = rows
                        .iter()
                        .filter(|r| !matches!(r.get(c), SqlValue::Null))
                        .count();
                    Ok(SqlValue::Integer(cnt as i64))
                }
                Self::CountDistinct(c) => {
                    let mut set = HashSet::new();
                    for r in rows {
                        let v = r.get(c);
                        if !matches!(v, SqlValue::Null) {
                            set.insert(model_scalar_key(&v));
                        }
                    }
                    Ok(SqlValue::Integer(set.len() as i64))
                }
                Self::Sum(c) => {
                    let mut int_sum: i128 = 0;
                    let mut real_sum: f64 = 0.0;
                    let mut is_real = false;
                    let mut count = 0usize;
                    for r in rows {
                        match r.get(c) {
                            SqlValue::Null => {}
                            SqlValue::Integer(n) => {
                                int_sum += i128::from(n);
                                count += 1;
                            }
                            SqlValue::Real(f) => {
                                is_real = true;
                                real_sum += f;
                                count += 1;
                            }
                            _ => return Err(ModelError::Type),
                        }
                    }
                    if count == 0 {
                        Ok(SqlValue::Null)
                    } else if is_real {
                        if !real_sum.is_finite() {
                            Err(ModelError::Bounds)
                        } else {
                            Ok(SqlValue::Real(if real_sum == 0.0 { 0.0 } else { real_sum }))
                        }
                    } else {
                        i64::try_from(int_sum)
                            .map(SqlValue::Integer)
                            .map_err(|_| ModelError::Bounds)
                    }
                }
                Self::Avg(c) => {
                    let mut int_sum: i128 = 0;
                    let mut real_sum: f64 = 0.0;
                    let mut is_real = false;
                    let mut count = 0u64;
                    for r in rows {
                        match r.get(c) {
                            SqlValue::Null => {}
                            SqlValue::Integer(n) => {
                                int_sum += i128::from(n);
                                count += 1;
                            }
                            SqlValue::Real(f) => {
                                is_real = true;
                                real_sum += f;
                                count += 1;
                            }
                            _ => return Err(ModelError::Type),
                        }
                    }
                    if count == 0 {
                        Ok(SqlValue::Null)
                    } else {
                        let avg = if is_real {
                            real_sum / (count as f64)
                        } else {
                            (int_sum as f64) / (count as f64)
                        };
                        if !avg.is_finite() {
                            Err(ModelError::Bounds)
                        } else {
                            Ok(SqlValue::Real(if avg == 0.0 { 0.0 } else { avg }))
                        }
                    }
                }
                Self::Min(c) => {
                    let mut best: Option<SqlValue> = None;
                    for r in rows {
                        let v = r.get(c);
                        if matches!(v, SqlValue::Null) {
                            continue;
                        }
                        if best
                            .as_ref()
                            .is_none_or(|b| compare_non_null(&v, b).unwrap() == Ordering::Less)
                        {
                            best = Some(if let SqlValue::Real(f) = v {
                                SqlValue::Real(if f == 0.0 { 0.0 } else { f })
                            } else {
                                v
                            });
                        }
                    }
                    Ok(best.unwrap_or(SqlValue::Null))
                }
                Self::Max(c) => {
                    let mut best: Option<SqlValue> = None;
                    for r in rows {
                        let v = r.get(c);
                        if matches!(v, SqlValue::Null) {
                            continue;
                        }
                        if best
                            .as_ref()
                            .is_none_or(|b| compare_non_null(&v, b).unwrap() == Ordering::Greater)
                        {
                            best = Some(if let SqlValue::Real(f) = v {
                                SqlValue::Real(if f == 0.0 { 0.0 } else { f })
                            } else {
                                v
                            });
                        }
                    }
                    Ok(best.unwrap_or(SqlValue::Null))
                }
            }
        }
    }

    let grp_count = (cur.next_or(0) % 3) as usize;
    let mut group_cols = Vec::new();
    for _ in 0..grp_count {
        let c = Col::from_byte(cur.next_or(0));
        if !group_cols.contains(&c) {
            group_cols.push(c);
        }
    }

    let mut select_sql_parts = Vec::new();
    let mut expected_cols = Vec::new();
    #[derive(Clone, Copy)]
    enum SelOut {
        Group(usize),
        Agg(AggOp),
    }
    let mut sel_outs = Vec::new();

    for (gi, &gc) in group_cols.iter().enumerate() {
        let style = cur.next_or(0);
        if cur.next_or(0) & 1 != 0 {
            let alias = format!("g_{gi}");
            select_sql_parts.push(format!("{} AS {alias}", gc.sql_ident(style)));
            expected_cols.push(alias);
        } else {
            select_sql_parts.push(gc.sql_ident(style).to_string());
            expected_cols.push(gc.canonical_name().to_string());
        }
        sel_outs.push(SelOut::Group(gi));
    }

    let agg_count = if group_cols.is_empty() {
        1 + (cur.next_or(0) % 3) as usize
    } else {
        (cur.next_or(0) % 3) as usize
    };
    let mut agg_ops = Vec::new();
    for ai in 0..agg_count {
        let col = Col::from_byte(cur.next_or(0));
        let op = match cur.next_or(0) % 7 {
            0 => AggOp::CountStar,
            1 => AggOp::CountCol(col),
            2 => AggOp::CountDistinct(col),
            3 => AggOp::Sum(col),
            4 => AggOp::Avg(col),
            5 => AggOp::Min(col),
            _ => AggOp::Max(col),
        };
        let style = cur.next_or(0);
        let op_sql = op.sql(style);
        if cur.next_or(0) & 1 != 0 {
            let alias = format!("a_{ai}");
            select_sql_parts.push(format!("{op_sql} AS {alias}"));
            expected_cols.push(alias);
        } else {
            select_sql_parts.push(op_sql);
            expected_cols.push(op.default_name());
        }
        agg_ops.push(op);
        sel_outs.push(SelOut::Agg(op));
    }

    let mut sql = format!("SELECT {} FROM m", select_sql_parts.join(", "));
    let mut params = Vec::new();

    let where_node = if !cur.next_or(0).is_multiple_of(4) {
        let (w_sql, w_node) = gen_where_expr(cur, text_pk, &mut params, 1);
        sql.push_str(" WHERE ");
        sql.push_str(&w_sql);
        Some(w_node)
    } else {
        None
    };

    if !group_cols.is_empty() {
        sql.push_str(" GROUP BY ");
        for (i, &c) in group_cols.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(c.sql_ident(cur.next_or(0)));
        }
    }

    let min_cnt = if cur.next_or(0) & 1 != 0 {
        let th = (cur.next_or(0) % 4) as i64;
        sql.push_str(" HAVING COUNT(*) >= ?");
        params.push(SqlValue::Integer(th));
        Some(th)
    } else {
        None
    };

    let order_by_agg = !agg_ops.is_empty() && cur.next_or(0) & 1 != 0;
    let order_desc = cur.next_or(0) & 1 != 0;
    let order_nulls = match cur.next_or(0) % 3 {
        0 => None,
        1 => Some(true),
        _ => Some(false),
    };
    if order_by_agg {
        sql.push_str(" ORDER BY ");
        sql.push_str(&agg_ops[0].sql(0));
        if order_desc {
            sql.push_str(" DESC");
        }
        if let Some(nf) = order_nulls {
            sql.push_str(if nf { " NULLS FIRST" } else { " NULLS LAST" });
        }
    }

    let (limit, offset) = if cur.next_or(0) & 1 != 0 {
        let lim = (cur.next_or(0) % 5) as usize;
        let off = (cur.next_or(0) % 4) as usize;
        sql.push_str(&format!(" LIMIT {lim} OFFSET {off}"));
        (Some(lim), off)
    } else {
        (None, 0)
    };

    let mut bind_err = None;
    for p in &params {
        if let SqlValue::Real(f) = p
            && !f.is_finite()
        {
            bind_err = Some(ModelError::Type);
            break;
        }
    }
    if bind_err.is_none()
        && let Some(ref w) = where_node
        && let Err(e) = w.check_types(text_pk)
    {
        bind_err = Some(e);
    }
    if bind_err.is_none() {
        for op in &agg_ops {
            if let Err(e) = op.check_type(text_pk) {
                bind_err = Some(e);
                break;
            }
        }
    }

    let actual = db.exec(&sql, &params);
    if let Some(exp_err) = bind_err {
        let err = actual.as_ref().err().map(|e| e.to_string());
        assert_expected_error(actual, exp_err, &sql);
        return Err(SqlError::Type(err.unwrap_or_default()));
    }

    let filtered: Vec<&Row> = model
        .values()
        .filter(|row| {
            where_node
                .as_ref()
                .is_none_or(|w| w.eval(row) == Some(true))
        })
        .collect();

    type ModelGroupKey = Vec<(u8, i64, u64, String, bool)>;
    type GroupBucket<'a> = (Vec<SqlValue>, Vec<&'a Row>);
    let mut group_map: Vec<(ModelGroupKey, GroupBucket<'_>)> = Vec::new();
    if group_cols.is_empty() {
        group_map.push((Vec::new(), (Vec::new(), filtered)));
    } else {
        for row in filtered {
            let vals: Vec<SqlValue> = group_cols.iter().map(|&c| row.get(c)).collect();
            let keys: Vec<_> = vals.iter().map(model_scalar_key).collect();
            if let Some((_, (_, bucket))) = group_map.iter_mut().find(|(k, _)| *k == keys) {
                bucket.push(row);
            } else {
                group_map.push((keys, (vals, vec![row])));
            }
        }
    }

    let mut final_groups: Vec<(Vec<SqlValue>, Vec<SqlValue>)> = Vec::new();
    let mut runtime_err: Option<ModelError> = None;
    for (_, (gvals, rows)) in group_map {
        let mut agg_vals = Vec::with_capacity(agg_ops.len());
        for op in &agg_ops {
            match op.eval(&rows) {
                Ok(v) => agg_vals.push(v),
                Err(e) => {
                    runtime_err = Some(e);
                    break;
                }
            }
        }
        if runtime_err.is_some() {
            break;
        }
        if let Some(th) = min_cnt
            && (rows.len() as i64) < th
        {
            continue;
        }
        final_groups.push((gvals, agg_vals));
    }

    if let Some(exp_err) = runtime_err {
        let err = actual.as_ref().err().map(|e| e.to_string());
        assert_expected_error(actual, exp_err, &sql);
        return Err(SqlError::Bounds(err.unwrap_or_default()));
    }

    if limit == Some(0) {
        assert_eq!(
            actual.unwrap(),
            SqlOutcome::Rows {
                columns: expected_cols,
                rows: Vec::new(),
            }
        );
        return Ok(());
    }

    final_groups.sort_by(|(ga, aa), (gb, ab)| {
        if order_by_agg {
            let nf = order_nulls.unwrap_or(!order_desc);
            let ord = model_cmp_values(&aa[0], &ab[0], order_desc, nf);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        for (va, vb) in ga.iter().zip(gb.iter()) {
            let ord = model_cmp_values(va, vb, false, true);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    let expected_rows: Vec<Vec<SqlValue>> = final_groups
        .into_iter()
        .skip(offset)
        .take(limit.unwrap_or(usize::MAX))
        .map(|(gvals, avals)| {
            sel_outs
                .iter()
                .map(|out| match out {
                    SelOut::Group(gi) => gvals[*gi].clone(),
                    SelOut::Agg(op) => {
                        let idx = agg_ops
                            .iter()
                            .position(|candidate| candidate == op)
                            .unwrap();
                        avals[idx].clone()
                    }
                })
                .collect()
        })
        .collect();

    let outcome = actual.unwrap_or_else(|e| panic!("AGG SELECT `{sql}` failed: {e}"));
    if check_forced_scan {
        let scanned = db
            .exec_forced(&sql, &params)
            .unwrap_or_else(|e| panic!("forced-scan AGG SELECT `{sql}` failed: {e}"));
        assert_eq!(
            outcome, scanned,
            "indexed vs forced-scan agg mismatch for `{sql}`"
        );
    }
    assert_eq!(
        outcome,
        SqlOutcome::Rows {
            columns: expected_cols,
            rows: expected_rows,
        },
        "AGG SELECT mismatch for `{sql}` params={params:?}"
    );
    Ok(())
}

fn exec_aggregate_select_op(
    db: &SqlDatabase<'_>,
    model: &BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    check_forced_scan: bool,
) {
    let mut runner = db;
    let _ = exec_aggregate_select_op_with(&mut runner, model, cur, text_pk, check_forced_scan);
}

fn exec_error_probe(db: &SqlDatabase<'_>, selector: u8) {
    match selector % 5 {
        0 => assert!(matches!(
            db.execute("CREATE TABLE m (id INTEGER PRIMARY KEY)", &[])
                .unwrap_err(),
            SqlError::TableExists(_)
        )),
        1 => assert_eq!(
            db.execute("CREATE TABLE IF NOT EXISTS m (id INTEGER PRIMARY KEY)", &[])
                .unwrap(),
            SqlOutcome::TableCreated
        ),
        2 => assert!(matches!(
            db.execute("SELECT * FROM missing_tbl", &[]).unwrap_err(),
            SqlError::NoSuchTable(_)
        )),
        3 => assert!(matches!(
            db.execute("SELECT nonexistent_col FROM m", &[])
                .unwrap_err(),
            SqlError::NoSuchColumn { .. }
        )),
        _ => {
            let prep = db.prepare("SELECT id FROM m WHERE n = ?").unwrap();
            assert!(matches!(
                db.execute_prepared(&prep, &[]).unwrap_err(),
                SqlError::ParameterCount {
                    expected: 1,
                    actual: 0
                }
            ));
        }
    }
}

fn exec_create_unique_n_op(
    db: &SqlDatabase<'_>,
    model: &BTreeMap<Pk, Row>,
    has_unique_n: &mut bool,
    cur: &mut ByteCursor<'_>,
) {
    let if_not_exists = cur.next_or(0) & 1 != 0;
    let sql = if if_not_exists {
        "CREATE UNIQUE INDEX IF NOT EXISTS u_n ON m (n)"
    } else {
        "CREATE UNIQUE INDEX u_n ON m (n)"
    };
    if *has_unique_n {
        let res = db.execute(sql, &[]);
        if if_not_exists {
            assert_eq!(res.unwrap(), SqlOutcome::IndexCreated);
        } else {
            assert!(matches!(res, Err(SqlError::IndexExists(_))));
        }
        return;
    }
    let mut seen = HashSet::new();
    let has_dup = model.values().filter_map(|r| r.n).any(|n| !seen.insert(n));
    let res = db.execute(sql, &[]);
    if has_dup {
        assert!(matches!(res, Err(SqlError::Constraint(_))));
    } else {
        assert_eq!(res.unwrap(), SqlOutcome::IndexCreated);
        *has_unique_n = true;
    }
}

fn exec_drop_unique_n_op(db: &SqlDatabase<'_>, has_unique_n: &mut bool, cur: &mut ByteCursor<'_>) {
    let if_exists = cur.next_or(0) & 1 != 0;
    let sql = if if_exists {
        "DROP INDEX IF EXISTS u_n ON m"
    } else {
        "DROP INDEX u_n ON m"
    };
    let res = db.execute(sql, &[]);
    if *has_unique_n || if_exists {
        assert_eq!(res.unwrap(), SqlOutcome::IndexDropped);
        *has_unique_n = false;
    } else {
        assert!(matches!(res, Err(SqlError::NoSuchIndex { .. })));
    }
    if cur.next_or(0) & 1 != 0 {
        let _ = db.execute("DROP INDEX IF EXISTS idx_n ON m", &[]);
    }
}

fn exec_multi_tx_op(
    db: &SqlDatabase<'_>,
    model: &mut BTreeMap<Pk, Row>,
    cur: &mut ByteCursor<'_>,
    text_pk: bool,
    has_unique_n: bool,
    with_indexes: bool,
) {
    let steps = 1 + (cur.next_or(0) % 4) as usize;
    let mode = cur.next_or(0) % 3;
    let mut staged_model = model.clone();
    let mut saw_stmt_err = false;
    let mut intentional_abort = false;

    let tx_res = db.transaction(|tx| {
        let mut runner = tx;
        for _ in 0..steps {
            let sub = cur.next_or(0) % 6;
            let step_res = match sub {
                0 => {
                    exec_insert_op_with(&mut runner, &mut staged_model, cur, text_pk, has_unique_n)
                }
                1 => exec_select_op_with(&mut runner, &staged_model, cur, text_pk, with_indexes),
                2 => exec_aggregate_select_op_with(
                    &mut runner,
                    &staged_model,
                    cur,
                    text_pk,
                    with_indexes,
                ),
                3 => {
                    exec_update_op_with(&mut runner, &mut staged_model, cur, text_pk, has_unique_n)
                }
                4 => exec_delete_op_with(&mut runner, &mut staged_model, cur, text_pk),
                _ => {
                    // Probe online DDL rejection inside explicit transaction.
                    let err = runner
                        .exec("CREATE INDEX fuzz_tx_idx ON m (n)", &[])
                        .unwrap_err();
                    assert!(matches!(err, SqlError::Unsupported { .. }));
                    Err(err)
                }
            };
            if let Err(err) = step_res {
                saw_stmt_err = true;
                if mode != 2 {
                    return Err(err);
                }
                // In mode 2, swallow the error and verify follow-up call + commit fail with TransactionAborted.
                let follow = runner.exec("SELECT COUNT(*) FROM m", &[]).unwrap_err();
                assert!(matches!(follow, SqlError::TransactionAborted(_)));
                break;
            }
        }
        if mode == 1 {
            intentional_abort = true;
            return Err(SqlError::Constraint("fuzz intentional rollback".into()));
        }
        Ok(())
    });

    if saw_stmt_err && mode == 2 {
        assert!(
            matches!(tx_res, Err(SqlError::TransactionAborted(_))),
            "swallowed statement error must poison tx, got {tx_res:?}"
        );
    } else if intentional_abort {
        assert!(matches!(tx_res, Err(SqlError::Constraint(_))));
    } else if saw_stmt_err {
        assert!(tx_res.is_err());
    } else {
        tx_res.expect("multi-statement transaction should commit");
        *model = staged_model;
    }
}

fn fuzz_structured_model(root: &Path, data: &[u8]) -> Backend {
    let mut cur = ByteCursor::new(data);
    let flags = cur.next_or(0);
    let use_checkpoint = flags & 1 != 0;
    let text_pk = flags & 2 != 0;
    let with_indexes = flags & 4 != 0;

    let store_path = root.join("model_store");
    let ckpt_path = root.join("model_ckpt");
    let digest = if use_checkpoint {
        let initial = Store::open(&store_path).expect("create initial model store");
        let d = initial
            .checkpoint_to(&ckpt_path)
            .expect("checkpoint initial model store");
        drop(initial);
        Some(d)
    } else {
        None
    };

    let mut backend = open_backend(&store_path, &ckpt_path, digest.as_ref(), use_checkpoint);
    let create_sql = if text_pk {
        "CREATE TABLE m (id TEXT PRIMARY KEY, n INTEGER, r REAL, s VARCHAR(8), b BOOLEAN, req INTEGER NOT NULL)"
    } else {
        "CREATE TABLE m (id INTEGER PRIMARY KEY, n INTEGER, r REAL, s VARCHAR(8), b BOOLEAN, req INTEGER NOT NULL)"
    };
    assert_eq!(
        backend.sql().execute(create_sql, &[]).expect("create m"),
        SqlOutcome::TableCreated
    );
    if with_indexes {
        for idx_sql in [
            "CREATE INDEX idx_n ON m (n)",
            "CREATE INDEX idx_s ON m (s)",
            "CREATE INDEX idx_req_b ON m (req, b)",
            "CREATE INDEX idx_req_s ON m (req, s)",
        ] {
            assert_eq!(
                backend.sql().execute(idx_sql, &[]).expect("create idx"),
                SqlOutcome::IndexCreated
            );
        }
    }

    let mut model = BTreeMap::<Pk, Row>::new();
    let mut has_unique_n = false;

    for _ in 0..MAX_STRUCTURED_OPS {
        let Some(op) = cur.next() else {
            break;
        };
        match op % 12 {
            0 | 1 => {
                exec_insert_op(&backend.sql(), &mut model, &mut cur, text_pk, has_unique_n);
                check_full_table(&backend.sql(), &model);
            }
            2 => {
                exec_select_op(&backend.sql(), &model, &mut cur, text_pk, with_indexes);
            }
            3 => {
                exec_aggregate_select_op(&backend.sql(), &model, &mut cur, text_pk, with_indexes);
            }
            4 => {
                exec_update_op(&backend.sql(), &mut model, &mut cur, text_pk, has_unique_n);
                check_full_table(&backend.sql(), &model);
            }
            5 => {
                exec_delete_op(&backend.sql(), &mut model, &mut cur, text_pk);
                check_full_table(&backend.sql(), &model);
            }
            6 => {
                drop(backend);
                backend = open_backend(&store_path, &ckpt_path, digest.as_ref(), use_checkpoint);
                check_full_table(&backend.sql(), &model);
            }
            7 => {
                exec_create_unique_n_op(&backend.sql(), &model, &mut has_unique_n, &mut cur);
            }
            8 => {
                exec_drop_unique_n_op(&backend.sql(), &mut has_unique_n, &mut cur);
            }
            9 => {
                exec_multi_tx_op(
                    &backend.sql(),
                    &mut model,
                    &mut cur,
                    text_pk,
                    has_unique_n,
                    with_indexes,
                );
                check_full_table(&backend.sql(), &model);
            }
            _ => {
                exec_error_probe(&backend.sql(), cur.next_or(0));
            }
        }
    }

    drop(backend);
    let reopened = open_backend(&store_path, &ckpt_path, digest.as_ref(), use_checkpoint);
    check_full_table(&reopened.sql(), &model);
    reopened
}

fuzz_target!(|data: &[u8]| {
    let root = tempfile::tempdir().expect("tempdir");
    let backend = fuzz_structured_model(root.path(), data);
    fuzz_raw_sql(&backend, data);
});
