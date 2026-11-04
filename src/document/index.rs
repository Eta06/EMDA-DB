//! Deterministic scalar index keys and atomic index maintenance helpers.
use super::{key, schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io;

/// Types supported by the initial index encoding. Numbers are signed 64-bit
/// integers; floating-point and larger unsigned numbers are not indexed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexValue {
    /// Explicit JSON null. Missing fields are not indexed.
    Null,
    /// A JSON boolean.
    Boolean(bool),
    /// A signed 64-bit JSON integer.
    Integer(i64),
    /// A UTF-8 string with at most 256 bytes.
    String(String),
}

/// Restricts a partial index to documents matching one condition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartialFilter {
    /// Match presence or absence; explicit null counts as present.
    Exists { path: Vec<String>, exists: bool },
    /// Match a scalar value with exact type and value.
    Equals {
        path: Vec<String>,
        value: IndexValue,
    },
}

/// A single-field or compound index. Missing indexed fields omit a document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexDefinition {
    /// A collection-local index name.
    pub name: String,
    /// Between one and four non-repeated object paths.
    pub fields: Vec<Vec<String>>,
    /// Require each complete indexed tuple, including null, to be distinct.
    pub unique: bool,
    /// Index only documents matching this condition, if present.
    pub partial: Option<PartialFilter>,
}

impl IndexDefinition {
    pub(super) fn validate(&self) -> io::Result<()> {
        key::name(&self.name)?;
        if self.fields.is_empty() || self.fields.len() > 4 {
            return Err(key::invalid("an index must have between 1 and 4 fields"));
        }
        let mut fields = BTreeSet::new();
        for path in &self.fields {
            schema::validate_path(path)?;
            if !fields.insert(path) {
                return Err(key::invalid("index contains a repeated field"));
            }
        }
        if let Some(filter) = &self.partial {
            match filter {
                PartialFilter::Exists { path, .. } => schema::validate_path(path)?,
                PartialFilter::Equals { path, value } => {
                    schema::validate_path(path)?;
                    value.encode(&mut Vec::new())?;
                }
            }
        }
        Ok(())
    }
}

impl IndexValue {
    pub(super) fn from_json(value: &Value) -> io::Result<Self> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Bool(value) => Ok(Self::Boolean(*value)),
            Value::Number(value) => value
                .as_i64()
                .map(Self::Integer)
                .ok_or_else(|| key::invalid("indexed numbers must be signed 64-bit integers")),
            Value::String(value) if value.len() <= 256 => Ok(Self::String(value.clone())),
            Value::String(_) => Err(key::invalid("indexed string exceeds 256 bytes")),
            _ => Err(key::invalid(
                "indexed fields must be null, boolean, integer or string",
            )),
        }
    }

    pub(super) fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        match self {
            Self::Null => out.push(0x10),
            Self::Boolean(value) => {
                out.push(0x20);
                out.push(u8::from(*value));
            }
            Self::Integer(value) => {
                out.push(0x30);
                out.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes());
            }
            Self::String(value) => {
                if value.len() > 256 {
                    return Err(key::invalid("indexed string exceeds 256 bytes"));
                }
                out.push(0x40);
                for byte in value.bytes() {
                    if byte == 0 {
                        out.extend_from_slice(&[0, 0xff]);
                    } else {
                        out.push(byte);
                    }
                }
                out.extend_from_slice(&[0, 0]);
            }
        }
        Ok(())
    }
}

fn matches_partial(document: &Value, filter: &Option<PartialFilter>) -> bool {
    match filter {
        None => true,
        Some(PartialFilter::Exists { path, exists }) => {
            schema::field(document, path).is_some() == *exists
        }
        Some(PartialFilter::Equals { path, value }) => {
            schema::field(document, path)
                .and_then(|field| IndexValue::from_json(field).ok())
                .as_ref()
                == Some(value)
        }
    }
}

pub(super) struct Keys {
    pub entry: Vec<u8>,
    pub unique: Option<Vec<u8>>,
}

pub(super) fn keys(
    collection: &str,
    definition: &IndexDefinition,
    document: &Value,
    id: &str,
) -> io::Result<Option<Keys>> {
    if !matches_partial(document, &definition.partial) {
        return Ok(None);
    }
    let mut tuple = Vec::new();
    for path in &definition.fields {
        let Some(value) = schema::field(document, path) else {
            return Ok(None);
        };
        IndexValue::from_json(value)?.encode(&mut tuple)?;
    }
    let mut entry = key::index(b'I', collection, &definition.name)?;
    entry.extend_from_slice(&tuple);
    entry.extend_from_slice(id.as_bytes());
    crate::record::validate_key(&entry)?;
    let unique = if definition.unique {
        let mut unique = key::index(b'U', collection, &definition.name)?;
        unique.extend_from_slice(&tuple);
        crate::record::validate_key(&unique)?;
        Some(unique)
    } else {
        None
    };
    Ok(Some(Keys { entry, unique }))
}
