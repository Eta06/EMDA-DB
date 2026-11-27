//! Small, explicit schema rules shared by both storage backends.
use super::{key, transaction::MAX_DOCUMENT_BYTES};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io;

/// The JSON type accepted by a schema rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum FieldType {
    /// A JSON boolean.
    Boolean,
    /// A signed 64-bit JSON integer.
    Integer,
    /// A signed 64-bit JSON integer in an inclusive range.
    IntegerRange {
        /// Lowest accepted integer.
        min: i64,
        /// Highest accepted integer.
        max: i64,
    },
    /// Any JSON number representable by serde_json.
    Number,
    /// A UTF-8 JSON string.
    String,
    /// A UTF-8 string with an inclusive byte-length range.
    StringBytes {
        /// Minimum UTF-8 bytes, possibly zero.
        min: usize,
        /// Maximum UTF-8 bytes, at most 1 MiB.
        max: usize,
    },
    /// A JSON object.
    Object,
    /// A JSON array.
    Array,
}

/// A schema rule for an object path. Array elements are not traversed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldRule {
    /// One or more object member names, from the root to the field.
    pub path: Vec<String>,
    /// The type required when the field is present and non-null.
    pub field_type: FieldType,
    /// Reject a document where this path is missing.
    pub required: bool,
    /// Permit explicit JSON null independently of whether the field is required.
    pub nullable: bool,
}

/// Rules applied to every document written to a collection. Unlisted fields
/// are permitted; missing and explicit null are distinct.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionSchema {
    /// At most 64 distinct paths, each with at most 32 components.
    pub fields: Vec<FieldRule>,
}

pub(super) fn validate_path(path: &[String]) -> io::Result<()> {
    if path.is_empty() || path.len() > 32 {
        return Err(key::invalid(
            "field path must have between 1 and 32 components",
        ));
    }
    for component in path {
        key::name(component)?;
    }
    Ok(())
}

pub(super) fn field<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut current = value;
    for component in path {
        current = current.as_object()?.get(component)?;
    }
    Some(current)
}

pub(super) fn validate_tree(document: &Value) -> io::Result<()> {
    let mut pending = Vec::new();
    let mut nodes = 0_usize;
    let mut raw_bytes = 0_usize;
    let mut current = Some((document, 0_usize));
    while let Some((value, depth)) = current.take().or_else(|| pending.pop()) {
        nodes += 1;
        if depth > 64 || nodes + pending.len() > 131_072 {
            return Err(key::invalid("document exceeds nesting or node limits"));
        }
        match value {
            Value::String(value) => {
                raw_bytes = raw_bytes.saturating_add(value.len());
                if raw_bytes > 1024 * 1024 {
                    return Err(key::invalid("document string data exceeds 1 MiB"));
                }
            }
            Value::Array(values) => {
                if nodes + pending.len() + values.len() > 131_072 {
                    return Err(key::invalid("document exceeds node limit"));
                }
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(values) => {
                for name in values.keys() {
                    raw_bytes = raw_bytes.saturating_add(name.len());
                }
                if raw_bytes > 1024 * 1024 {
                    return Err(key::invalid("document string data exceeds 1 MiB"));
                }
                if nodes + pending.len() + values.len() > 131_072 {
                    return Err(key::invalid("document exceeds node limit"));
                }
                for child in values.values() {
                    match child {
                        Value::Array(_) | Value::Object(_) => {
                            pending.push((child, depth + 1));
                        }
                        Value::String(s) => {
                            if depth + 1 > 64 {
                                return Err(key::invalid(
                                    "document exceeds nesting or node limits",
                                ));
                            }
                            nodes += 1;
                            raw_bytes = raw_bytes.saturating_add(s.len());
                            if raw_bytes > 1024 * 1024 {
                                return Err(key::invalid("document string data exceeds 1 MiB"));
                            }
                        }
                        _ => {
                            if depth + 1 > 64 {
                                return Err(key::invalid(
                                    "document exceeds nesting or node limits",
                                ));
                            }
                            nodes += 1;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

// Encoded JSON byte thresholds below which `validate_tree`'s bounds are
// mathematically guaranteed for any valid JSON object:
// - Depth 65 inside a root JSON object requires at least `{"":` (4) + 64×`[`
//   (64) + `0` (1) + 64×`]` (64) + `}` (1) = 134 bytes.
// - Exceeding 131,072 nodes inside a root JSON object requires at least 262,148
//   bytes (each additional scalar element needs `,0`).
// - Decoded UTF-8 string bytes are strictly less than the encoded JSON byte
//   length, so any document within `MAX_DOCUMENT_BYTES` (1 MiB) is within the
//   1 MiB string-data limit.
const TREE_SAFE_ENCODED_BYTES: usize = 128;
const NODE_SAFE_ENCODED_BYTES: usize = 262_000;

fn validate_depth_only(root: &serde_json::Map<String, Value>) -> io::Result<()> {
    let mut pending = Vec::new();
    for child in root.values() {
        if matches!(child, Value::Array(_) | Value::Object(_)) {
            pending.push((child, 1_usize));
        }
    }
    while let Some((value, depth)) = pending.pop() {
        if depth > 64 {
            return Err(key::invalid("document exceeds nesting or node limits"));
        }
        match value {
            Value::Array(values) => {
                if !values.is_empty() && depth + 1 > 64 {
                    return Err(key::invalid("document exceeds nesting or node limits"));
                }
                for child in values {
                    if matches!(child, Value::Array(_) | Value::Object(_)) {
                        pending.push((child, depth + 1));
                    }
                }
            }
            Value::Object(values) => {
                if !values.is_empty() && depth + 1 > 64 {
                    return Err(key::invalid("document exceeds nesting or node limits"));
                }
                for child in values.values() {
                    if matches!(child, Value::Array(_) | Value::Object(_)) {
                        pending.push((child, depth + 1));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

impl CollectionSchema {
    pub(super) fn has_value_bounds(&self) -> bool {
        self.fields.iter().any(|rule| {
            matches!(
                rule.field_type,
                FieldType::IntegerRange { .. } | FieldType::StringBytes { .. }
            )
        })
    }

    pub(super) fn validate_definition(&self) -> io::Result<()> {
        if self.fields.len() > 64 {
            return Err(key::invalid("schema exceeds 64 field rules"));
        }
        let mut paths = BTreeSet::new();
        for rule in &self.fields {
            validate_path(&rule.path)?;
            match rule.field_type {
                FieldType::IntegerRange { min, max } if min > max => {
                    return Err(key::invalid("invalid schema integer range"));
                }
                FieldType::StringBytes { min, max } if min > max || max > MAX_DOCUMENT_BYTES => {
                    return Err(key::invalid("invalid schema string byte range"));
                }
                _ => {}
            }
            if !paths.insert(&rule.path) {
                return Err(key::invalid("schema contains a duplicate field path"));
            }
        }
        Ok(())
    }

    pub(super) fn validate_document(&self, document: &Value) -> io::Result<()> {
        let Some(root) = document.as_object() else {
            return Err(key::invalid("document must be a JSON object"));
        };
        validate_tree(document)?;
        self.validate_fields(document, root)
    }

    /// Equivalent to `validate_document`, skipping only tree-bound checks that
    /// are mathematically guaranteed by the encoded JSON byte length while
    /// always enforcing every schema rule.
    pub(super) fn validate_stored_document(
        &self,
        document: &Value,
        encoded_len: usize,
    ) -> io::Result<()> {
        let Some(root) = document.as_object() else {
            return Err(key::invalid("document must be a JSON object"));
        };
        if encoded_len > NODE_SAFE_ENCODED_BYTES {
            validate_tree(document)?;
        } else if encoded_len > TREE_SAFE_ENCODED_BYTES {
            validate_depth_only(root)?;
        }
        self.validate_fields(document, root)
    }

    fn validate_fields(
        &self,
        document: &Value,
        root: &serde_json::Map<String, Value>,
    ) -> io::Result<()> {
        for rule in &self.fields {
            let found = if rule.path.len() == 1 {
                root.get(&rule.path[0])
            } else {
                field(document, &rule.path)
            };
            match found {
                None if rule.required => return Err(key::invalid("required field is missing")),
                None => {}
                Some(Value::Null) if rule.nullable => {}
                Some(Value::Null) => return Err(key::invalid("field does not permit null")),
                Some(value) => {
                    let valid = match rule.field_type {
                        FieldType::Boolean => value.is_boolean(),
                        FieldType::Integer => value.as_i64().is_some(),
                        FieldType::IntegerRange { min, max } => value
                            .as_i64()
                            .is_some_and(|value| (min..=max).contains(&value)),
                        FieldType::Number => value.is_number(),
                        FieldType::String => value.is_string(),
                        FieldType::StringBytes { min, max } => value
                            .as_str()
                            .is_some_and(|value| (min..=max).contains(&value.len())),
                        FieldType::Object => value.is_object(),
                        FieldType::Array => value.is_array(),
                    };
                    if !valid {
                        return Err(key::invalid("field does not match its schema type"));
                    }
                }
            }
        }
        Ok(())
    }
}
