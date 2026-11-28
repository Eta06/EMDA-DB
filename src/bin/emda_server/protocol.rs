use database_engine::{CollectionSchema, SqlOutcome, SqlValue};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::{self, Read, Write};

pub const MAX_FRAME: usize = 256 * 1024;
pub const VERSION: u16 = 1;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u16,
    pub project: String,
    pub credential: String,
    pub token: String,
    pub operation: Operation,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    KvGet {
        key: String,
    },
    KvPut {
        key: String,
        value_hex: String,
    },
    KvDelete {
        key: String,
    },
    DocumentCreate {
        collection: String,
        schema: CollectionSchema,
    },
    DocumentGet {
        collection: String,
        id: String,
    },
    DocumentInsert {
        collection: String,
        id: String,
        document: Value,
    },
    DocumentReplace {
        collection: String,
        id: String,
        document: Value,
    },
    DocumentDelete {
        collection: String,
        id: String,
    },
    Sql {
        statement: String,
        #[serde(default)]
        params: Vec<Value>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    InvalidRequest,
    UnsupportedVersion,
    Unauthorized,
    Forbidden,
    Busy,
    Limit,
    Timeout,
    Conflict,
    OutcomeUnknown,
    NotFound,
    InvalidOperation,
    Internal,
}

impl Failure {
    fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::UnsupportedVersion => "unsupported_version",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::Busy => "busy",
            Self::Limit => "limit",
            Self::Timeout => "timeout",
            Self::Conflict => "conflict",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::NotFound => "not_found",
            Self::InvalidOperation => "invalid_operation",
            Self::Internal => "internal",
        }
    }
}

pub fn read_request(stream: &mut impl Read) -> io::Result<Result<Request, Failure>> {
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    if len == 0 || len > MAX_FRAME {
        return Ok(Err(Failure::Limit));
    }
    let mut body = vec![0; len];
    stream.read_exact(&mut body)?;
    Ok(serde_json::from_slice(&body).map_err(|_| Failure::InvalidRequest))
}

pub fn write_response(stream: &mut impl Write, result: Result<Value, Failure>) -> io::Result<()> {
    let response = match result {
        Ok(result) => json!({"ok": true, "result": result}),
        Err(failure) => error_body(failure),
    };
    let mut body = Frame(Vec::new());
    if serde_json::to_writer(&mut body, &response).is_err() {
        body.0.clear();
        serde_json::to_writer(&mut body, &error_body(Failure::Limit))?;
    }
    stream.write_all(&(body.0.len() as u32).to_be_bytes())?;
    stream.write_all(&body.0)?;
    stream.flush()
}

fn error_body(failure: Failure) -> Value {
    json!({"ok": false, "error": failure.code()})
}

struct Frame(Vec<u8>);

impl Write for Frame {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() > MAX_FRAME.saturating_sub(self.0.len()) {
            return Err(io::Error::other("response exceeds frame limit"));
        }
        self.0.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn from_hex(input: &str) -> Option<Vec<u8>> {
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect::<Option<Vec<_>>>()
        .filter(|_| input.len() % 2 == 0)
}

fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

pub fn hex(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    const DIGITS: &[u8] = b"0123456789abcdef";
    for &byte in input {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    out
}

pub fn sql_value(value: Value) -> Result<SqlValue, Failure> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Boolean(value),
        Value::String(value) => SqlValue::Text(value),
        Value::Number(value) if value.is_i64() => SqlValue::Integer(value.as_i64().unwrap()),
        Value::Number(value) if value.is_f64() => SqlValue::Real(value.as_f64().unwrap()),
        _ => return Err(Failure::InvalidRequest),
    })
}

fn sql_cell(value: SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Boolean(value) => value.into(),
        SqlValue::Integer(value) => value.into(),
        SqlValue::Real(value) => value.into(),
        SqlValue::Text(value) => value.into(),
    }
}

pub fn sql_result(outcome: SqlOutcome) -> Result<Value, Failure> {
    Ok(match outcome {
        SqlOutcome::Rows { columns, rows } => {
            let rows: Vec<Vec<_>> = rows
                .into_iter()
                .map(|row| row.into_iter().map(sql_cell).collect())
                .collect();
            json!({"columns": columns, "rows": rows})
        }
        SqlOutcome::Inserted { rows } => json!({"status": "inserted", "rows": rows}),
        SqlOutcome::Updated { rows } => json!({"status": "updated", "rows": rows}),
        SqlOutcome::Deleted { rows } => json!({"status": "deleted", "rows": rows}),
        SqlOutcome::TableCreated => json!({"status": "table_created"}),
        SqlOutcome::IndexCreated => json!({"status": "index_created"}),
        SqlOutcome::IndexDropped => json!({"status": "index_dropped"}),
        SqlOutcome::TableDropped => json!({"status": "table_dropped"}),
        SqlOutcome::TableAltered => json!({"status": "table_altered"}),
        _ => return Err(Failure::Internal),
    })
}
