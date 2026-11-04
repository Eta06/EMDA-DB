//! Small, explicit schema rules shared by both storage backends.
use super::key;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io;

/// The JSON type accepted by a schema rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    /// A JSON boolean.
    Boolean,
    /// A signed 64-bit JSON integer.
    Integer,
    /// Any JSON number representable by serde_json.
    Number,
    /// A UTF-8 JSON string.
    String,
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
    let mut pending = vec![(document, 0_usize)];
    let mut nodes = 0_usize;
    let mut raw_bytes = 0_usize;
    while let Some((value, depth)) = pending.pop() {
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
                pending.extend(values.values().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}

impl CollectionSchema {
    pub(super) fn validate_definition(&self) -> io::Result<()> {
        if self.fields.len() > 64 {
            return Err(key::invalid("schema exceeds 64 field rules"));
        }
        let mut paths = BTreeSet::new();
        for rule in &self.fields {
            validate_path(&rule.path)?;
            if !paths.insert(&rule.path) {
                return Err(key::invalid("schema contains a duplicate field path"));
            }
        }
        Ok(())
    }

    pub(super) fn validate_document(&self, document: &Value) -> io::Result<()> {
        if !document.is_object() {
            return Err(key::invalid("document must be a JSON object"));
        }
        validate_tree(document)?;
        for rule in &self.fields {
            match field(document, &rule.path) {
                None if rule.required => return Err(key::invalid("required field is missing")),
                None => {}
                Some(Value::Null) if rule.nullable => {}
                Some(Value::Null) => return Err(key::invalid("field does not permit null")),
                Some(value) => {
                    let valid = match rule.field_type {
                        FieldType::Boolean => value.is_boolean(),
                        FieldType::Integer => value.as_i64().is_some(),
                        FieldType::Number => value.is_number(),
                        FieldType::String => value.is_string(),
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
