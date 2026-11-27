//! Deterministic scalar index keys and atomic index maintenance helpers.
#[cfg(test)]
use super::transaction::StoredDocument;
use super::{
    flat::{FlatDocument, FlatScalar},
    key, schema,
    transaction::StoredDocumentRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::io;

pub(super) const MAX_INDEX_STRING_BYTES: usize = 256;
pub(super) const MAX_INDEX_FIELDS: usize = 4;

/// Types supported by the initial index encoding. Numbers are signed 64-bit
/// integers; floating-point and larger unsigned numbers are not indexed.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
        if self.fields.is_empty() || self.fields.len() > MAX_INDEX_FIELDS {
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
    // Compare an indexable JSON value to this validated scalar in exactly the
    // same order as `encode`, without allocating encoded byte buffers.
    pub(super) fn compare_json(&self, actual: &Value) -> Option<Ordering> {
        match actual {
            Value::Null => Some(match self {
                Self::Null => Ordering::Equal,
                Self::Boolean(_) | Self::Integer(_) | Self::String(_) => Ordering::Less,
            }),
            Value::Bool(actual) => Some(match self {
                Self::Null => Ordering::Greater,
                Self::Boolean(expected) => actual.cmp(expected),
                Self::Integer(_) | Self::String(_) => Ordering::Less,
            }),
            Value::Number(actual) => actual.as_i64().map(|actual| match self {
                Self::Null | Self::Boolean(_) => Ordering::Greater,
                Self::Integer(expected) => actual.cmp(expected),
                Self::String(_) => Ordering::Less,
            }),
            Value::String(actual) if actual.len() <= MAX_INDEX_STRING_BYTES => Some(match self {
                Self::String(expected) => actual.as_bytes().cmp(expected.as_bytes()),
                Self::Null | Self::Boolean(_) | Self::Integer(_) => Ordering::Greater,
            }),
            Value::String(_) | Value::Array(_) | Value::Object(_) => None,
        }
    }

    // Same scalar ordering as `compare_json`, applied to a borrowed flat value.
    pub(super) fn compare_flat(&self, actual: FlatScalar<'_>) -> Option<Ordering> {
        match actual {
            FlatScalar::Null => Some(match self {
                Self::Null => Ordering::Equal,
                Self::Boolean(_) | Self::Integer(_) | Self::String(_) => Ordering::Less,
            }),
            FlatScalar::Bool(actual) => Some(match self {
                Self::Null => Ordering::Greater,
                Self::Boolean(expected) => actual.cmp(expected),
                Self::Integer(_) | Self::String(_) => Ordering::Less,
            }),
            FlatScalar::Int(actual) => Some(match self {
                Self::Null | Self::Boolean(_) => Ordering::Greater,
                Self::Integer(expected) => actual.cmp(expected),
                Self::String(_) => Ordering::Less,
            }),
            FlatScalar::Str(actual) if actual.len() <= MAX_INDEX_STRING_BYTES => Some(match self {
                Self::String(expected) => actual.as_bytes().cmp(expected.as_bytes()),
                Self::Null | Self::Boolean(_) | Self::Integer(_) => Ordering::Greater,
            }),
            FlatScalar::UInt(_) | FlatScalar::Float(_) | FlatScalar::Str(_) => None,
        }
    }

    pub(super) fn from_json(value: &Value) -> io::Result<Self> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Bool(value) => Ok(Self::Boolean(*value)),
            Value::Number(value) => value
                .as_i64()
                .map(Self::Integer)
                .ok_or_else(|| key::invalid("indexed numbers must be signed 64-bit integers")),
            Value::String(value) if value.len() <= MAX_INDEX_STRING_BYTES => {
                Ok(Self::String(value.clone()))
            }
            Value::String(_) => Err(key::invalid("indexed string exceeds 256 bytes")),
            _ => Err(key::invalid(
                "indexed fields must be null, boolean, integer or string",
            )),
        }
    }

    pub(super) fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        self.encode_into(out)
    }

    pub(super) fn encode_to_buf(&self, out: &mut TupleBuf) -> io::Result<()> {
        self.encode_into(out)
    }

    fn encode_into(&self, out: &mut impl ByteSink) -> io::Result<()> {
        match self {
            Self::Null => out.push_byte(0x10),
            Self::Boolean(value) => {
                out.push_byte(0x20);
                out.push_byte(u8::from(*value));
            }
            Self::Integer(value) => {
                out.push_byte(0x30);
                out.extend_bytes(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes());
            }
            Self::String(value) => encode_string_into(value, out)?,
        }
        Ok(())
    }
}

trait ByteSink {
    fn push_byte(&mut self, byte: u8);
    fn extend_bytes(&mut self, bytes: &[u8]);
}

impl ByteSink for Vec<u8> {
    fn push_byte(&mut self, byte: u8) {
        self.push(byte);
    }

    fn extend_bytes(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

const INLINE_TUPLE_BYTES: usize = 64;

/// Reusable scratch buffer for encoded index tuples and short keys. Short
/// buffers stay on the stack; longer ones spill to a vector.
pub(super) struct TupleBuf {
    inline: [u8; INLINE_TUPLE_BYTES],
    len: usize,
    spilled: bool,
    spill: Vec<u8>,
}

impl TupleBuf {
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            inline: [0; INLINE_TUPLE_BYTES],
            len: 0,
            spilled: false,
            spill: Vec::new(),
        }
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.len = 0;
        self.spilled = false;
        self.spill.clear();
    }

    #[inline]
    pub(super) fn as_slice(&self) -> &[u8] {
        if self.spilled {
            &self.spill
        } else {
            &self.inline[..self.len]
        }
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(super) fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.extend_bytes(bytes);
    }

    #[inline]
    pub(super) fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.len = len;
            if self.spilled {
                self.spill.truncate(len);
            }
        }
    }

    pub(super) fn end_in_place(&mut self) {
        let slice = if self.spilled {
            &mut self.spill[..self.len]
        } else {
            &mut self.inline[..self.len]
        };
        let mut new_len = slice.len();
        while new_len > 0 {
            new_len -= 1;
            if slice[new_len] != u8::MAX {
                slice[new_len] += 1;
                self.truncate(new_len + 1);
                return;
            }
        }
        unreachable!("reserved prefixes contain non-0xff bytes")
    }

    #[cold]
    fn spill_for(&mut self, extra: usize) {
        if !self.spilled {
            self.spill.clear();
            self.spill.reserve(self.len + extra);
            self.spill.extend_from_slice(&self.inline[..self.len]);
            self.spilled = true;
        }
    }
}

impl ByteSink for TupleBuf {
    #[inline]
    fn push_byte(&mut self, byte: u8) {
        if !self.spilled && self.len < INLINE_TUPLE_BYTES {
            self.inline[self.len] = byte;
            self.len += 1;
            return;
        }
        self.spill_for(1);
        self.spill.push(byte);
        self.len += 1;
    }

    #[inline]
    fn extend_bytes(&mut self, bytes: &[u8]) {
        if !self.spilled && self.len + bytes.len() <= INLINE_TUPLE_BYTES {
            self.inline[self.len..self.len + bytes.len()].copy_from_slice(bytes);
            self.len += bytes.len();
            return;
        }
        self.spill_for(bytes.len());
        self.spill.extend_from_slice(bytes);
        self.len += bytes.len();
    }
}

fn encode_string_into(value: &str, out: &mut impl ByteSink) -> io::Result<()> {
    if value.len() > MAX_INDEX_STRING_BYTES {
        return Err(key::invalid("indexed string exceeds 256 bytes"));
    }
    out.push_byte(0x40);
    for byte in value.bytes() {
        if byte == 0 {
            out.extend_bytes(&[0, 0xff]);
        } else {
            out.push_byte(byte);
        }
    }
    out.extend_bytes(&[0, 0]);
    Ok(())
}

fn encode_json_into(value: &Value, out: &mut impl ByteSink) -> io::Result<()> {
    match value {
        Value::Null => out.push_byte(0x10),
        Value::Bool(value) => {
            out.push_byte(0x20);
            out.push_byte(u8::from(*value));
        }
        Value::Number(value) => {
            let value = value
                .as_i64()
                .ok_or_else(|| key::invalid("indexed numbers must be signed 64-bit integers"))?;
            out.push_byte(0x30);
            out.extend_bytes(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        Value::String(value) => encode_string_into(value, out)?,
        _ => {
            return Err(key::invalid(
                "indexed fields must be null, boolean, integer or string",
            ));
        }
    }
    Ok(())
}

pub(super) fn encode_json(value: &Value, out: &mut Vec<u8>) -> io::Result<()> {
    encode_json_into(value, out)
}

#[inline]
fn encode_flat_scalar(value: FlatScalar<'_>, out: &mut impl ByteSink) -> io::Result<()> {
    match value {
        FlatScalar::Null => out.push_byte(0x10),
        FlatScalar::Bool(value) => {
            out.push_byte(0x20);
            out.push_byte(u8::from(value));
        }
        FlatScalar::Int(value) => {
            out.push_byte(0x30);
            out.extend_bytes(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        FlatScalar::Str(value) => encode_string_into(value, out)?,
        FlatScalar::UInt(_) | FlatScalar::Float(_) => {
            return Err(key::invalid(
                "indexed numbers must be signed 64-bit integers",
            ));
        }
    }
    Ok(())
}

#[inline]
fn flat_field<'a>(document: &FlatDocument<'a>, path: &[String]) -> Option<FlatScalar<'a>> {
    // A flat document contains only top-level scalar members, so any nested
    // path is absent, matching `schema::field`.
    if path.len() == 1 {
        document.get(&path[0])
    } else {
        None
    }
}

fn matches_partial(document: &Value, filter: &Option<PartialFilter>) -> bool {
    match filter {
        None => true,
        Some(PartialFilter::Exists { path, exists }) => {
            schema::field(document, path).is_some() == *exists
        }
        Some(PartialFilter::Equals { path, value }) => {
            schema::field(document, path).and_then(|field| value.compare_json(field))
                == Some(Ordering::Equal)
        }
    }
}

#[inline]
fn matches_partial_flat(document: &FlatDocument<'_>, filter: &Option<PartialFilter>) -> bool {
    match filter {
        None => true,
        Some(PartialFilter::Exists { path, exists }) => {
            flat_field(document, path).is_some() == *exists
        }
        Some(PartialFilter::Equals { path, value }) => {
            flat_field(document, path).and_then(|field| value.compare_flat(field))
                == Some(Ordering::Equal)
        }
    }
}

// Append the indexed fields to an existing key prefix. A missing field or
// an unmatched guard can leave partial bytes for the caller to discard.
pub(super) fn encode_indexed_tuple(
    definition: &IndexDefinition,
    document: &Value,
    out: &mut Vec<u8>,
) -> io::Result<bool> {
    encode_indexed_tuple_into(definition, document, out)
}

fn encode_indexed_tuple_into(
    definition: &IndexDefinition,
    document: &Value,
    out: &mut impl ByteSink,
) -> io::Result<bool> {
    if !matches_partial(document, &definition.partial) {
        return Ok(false);
    }
    for path in &definition.fields {
        let Some(value) = schema::field(document, path) else {
            return Ok(false);
        };
        encode_json_into(value, out)?;
    }
    Ok(true)
}

#[inline]
fn encode_indexed_tuple_flat(
    definition: &IndexDefinition,
    document: &FlatDocument<'_>,
    out: &mut impl ByteSink,
) -> io::Result<bool> {
    if !matches_partial_flat(document, &definition.partial) {
        return Ok(false);
    }
    for path in &definition.fields {
        let Some(value) = flat_field(document, path) else {
            return Ok(false);
        };
        encode_flat_scalar(value, out)?;
    }
    Ok(true)
}

/// Checks that `document` belongs to `definition` and produces `entry` under
/// `index_prefix` and `id`, leaving the encoded tuple in `tuple_buf` for any
/// unique-owner check. Enforces the same representability, membership, key-size,
/// and exact-entry checks as `keys` without allocating key buffers.
pub(super) fn verify_entry_stored_ref(
    definition: &IndexDefinition,
    document: &StoredDocumentRef<'_, '_>,
    index_prefix: &[u8],
    id: &str,
    entry: &[u8],
    tuple_buf: &mut TupleBuf,
) -> io::Result<()> {
    let invalid = |msg| io::Error::new(io::ErrorKind::InvalidData, msg);
    tuple_buf.clear();
    let belongs = match document {
        StoredDocumentRef::Flat(flat) => encode_indexed_tuple_flat(definition, flat, tuple_buf),
        StoredDocumentRef::Full(value) => encode_indexed_tuple_into(definition, value, tuple_buf),
    }
    .map_err(|_| invalid("document cannot be represented by its index"))?;
    if !belongs {
        return Err(invalid("document does not belong to its index"));
    }
    let tuple = tuple_buf.as_slice();
    // `key::index(b'U', ..)` has the exact same length as `key::index(b'I', ..)`.
    let entry_len = index_prefix
        .len()
        .checked_add(tuple.len())
        .and_then(|n| n.checked_add(id.len()));
    let unique_len = index_prefix.len().checked_add(tuple.len());
    if entry_len.is_none_or(|n| n == 0 || n > crate::record::MAX_KEY)
        || (definition.unique && unique_len.is_none_or(|n| n == 0 || n > crate::record::MAX_KEY))
    {
        return Err(invalid("document cannot be represented by its index"));
    }
    let matches = entry.len() == index_prefix.len() + tuple.len() + id.len()
        && entry.starts_with(index_prefix)
        && &entry[index_prefix.len()..index_prefix.len() + tuple.len()] == tuple
        && entry.ends_with(id.as_bytes());
    if !matches {
        return Err(invalid("document index entry does not match the document"));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn verify_entry_stored(
    definition: &IndexDefinition,
    document: &StoredDocument<'_>,
    index_prefix: &[u8],
    id: &str,
    entry: &[u8],
    tuple_buf: &mut TupleBuf,
) -> io::Result<()> {
    match document {
        StoredDocument::Flat(flat) => verify_entry_stored_ref(
            definition,
            &StoredDocumentRef::Flat(flat),
            index_prefix,
            id,
            entry,
            tuple_buf,
        ),
        StoredDocument::Full(value) => {
            let mut copy = value.clone();
            verify_entry_stored_ref(
                definition,
                &StoredDocumentRef::Full(&mut copy),
                index_prefix,
                id,
                entry,
                tuple_buf,
            )
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
    let mut tuple = Vec::new();
    if !encode_indexed_tuple(definition, document, &mut tuple)? {
        return Ok(None);
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

#[cfg(test)]
mod scalar_order_tests {
    use super::{IndexValue, encode_json};
    use serde_json::{Value, json};

    fn json_value(value: &IndexValue) -> Value {
        match value {
            IndexValue::Null => Value::Null,
            IndexValue::Boolean(value) => json!(value),
            IndexValue::Integer(value) => json!(value),
            IndexValue::String(value) => json!(value),
        }
    }

    #[test]
    fn borrowed_comparisons_match_complete_scalar_encoding_order() {
        let mut values = vec![
            IndexValue::Null,
            IndexValue::Boolean(false),
            IndexValue::Boolean(true),
        ];
        values.extend(
            [
                i64::MIN,
                i64::MIN + 1,
                -100,
                -1,
                0,
                1,
                100,
                i64::MAX - 1,
                i64::MAX,
            ]
            .into_iter()
            .map(IndexValue::Integer),
        );
        values.extend(
            [
                "", "\0", "\0\0", "\0a", "a", "a\0", "a\0a", "aa", "é", "😀", "z", "a\u{7f}",
            ]
            .into_iter()
            .map(|s| IndexValue::String(s.into())),
        );
        let alphabet = ['\0', 'a', 'z', 'é', '😀'];
        for a in alphabet {
            for b in alphabet {
                for c in alphabet {
                    values.push(IndexValue::String([a, b, c].into_iter().collect()));
                }
            }
        }
        values.push(IndexValue::String("a".repeat(256)));
        for actual in &values {
            let mut left = Vec::new();
            actual.encode(&mut left).unwrap();
            let json = json_value(actual);
            let mut borrowed = Vec::new();
            encode_json(&json, &mut borrowed).unwrap();
            assert_eq!(borrowed, left);
            for expected in &values {
                let mut right = Vec::new();
                expected.encode(&mut right).unwrap();
                assert_eq!(
                    expected.compare_json(&json),
                    Some(left.cmp(&right)),
                    "actual={actual:?} expected={expected:?}"
                );
            }
        }
        for invalid in [
            json!(1.0),
            json!(u64::MAX),
            json!({}),
            json!([]),
            json!("a".repeat(257)),
        ] {
            let mut out = vec![1, 2, 3];
            assert!(encode_json(&invalid, &mut out).is_err());
            assert_eq!(out, [1, 2, 3]);
            for expected in &values {
                assert_eq!(expected.compare_json(&invalid), None);
            }
        }
    }
}

#[cfg(test)]
mod flat_index_tests {
    use super::*;
    use crate::document::flat;
    use serde_json::{Value, json};

    fn check_against_keys(
        collection: &str,
        definition: &IndexDefinition,
        raw_json: &str,
        id: &str,
    ) -> (bool, bool) {
        let value: Value = serde_json::from_str(raw_json).unwrap();
        let flat_doc = flat::parse(raw_json.as_bytes()).expect("test document must be flat");
        assert_eq!(Value::Object(flat_doc.to_map()), value, "{raw_json}");

        let index_prefix = key::index(b'I', collection, &definition.name).unwrap();
        let expected_keys = keys(collection, definition, &value, id);

        let mut tuple_buf = TupleBuf::new();
        let flat_belongs = encode_indexed_tuple_flat(definition, &flat_doc, &mut tuple_buf);
        let mut json_tuple = Vec::new();
        let json_belongs = encode_indexed_tuple(definition, &value, &mut json_tuple);
        match (&json_belongs, &flat_belongs) {
            (Ok(a), Ok(b)) => {
                assert_eq!(a, b, "belongs mismatch for {raw_json} on {definition:?}");
                if *a {
                    assert_eq!(
                        tuple_buf.as_slice(),
                        json_tuple.as_slice(),
                        "tuple bytes mismatch for {raw_json} on {definition:?}"
                    );
                }
            }
            (Err(a), Err(b)) => {
                assert_eq!(a.kind(), b.kind(), "{raw_json}");
                assert_eq!(a.to_string(), b.to_string(), "{raw_json}");
            }
            _ => panic!(
                "encode_indexed_tuple mismatch for {raw_json} on {definition:?}: json={json_belongs:?}, flat={flat_belongs:?}"
            ),
        }

        // Also compare verify_entry_stored against the reference check built on `keys`.
        let reference_verify = |entry: &[u8]| -> io::Result<()> {
            let invalid = |m| io::Error::new(io::ErrorKind::InvalidData, m);
            let expected = keys(collection, definition, &value, id)
                .map_err(|_| invalid("document cannot be represented by its index"))?
                .ok_or_else(|| invalid("document does not belong to its index"))?;
            if expected.entry != entry {
                return Err(invalid("document index entry does not match the document"));
            }
            Ok(())
        };

        let stored_flat = StoredDocument::Flat(flat_doc);
        let probe_entry = match &expected_keys {
            Ok(Some(k)) => k.entry.clone(),
            _ => {
                let mut bogus = index_prefix.clone();
                bogus.push(0x10);
                bogus.extend_from_slice(id.as_bytes());
                bogus
            }
        };
        let r_ref = reference_verify(&probe_entry);
        let r_fast = verify_entry_stored(
            definition,
            &stored_flat,
            &index_prefix,
            id,
            &probe_entry,
            &mut tuple_buf,
        );
        match (&r_ref, &r_fast) {
            (Ok(()), Ok(())) => {
                if let Ok(Some(k)) = &expected_keys {
                    if let Some(unique) = &k.unique {
                        let mut rebuilt_unique =
                            key::index(b'U', collection, &definition.name).unwrap();
                        rebuilt_unique.extend_from_slice(tuple_buf.as_slice());
                        assert_eq!(&rebuilt_unique, unique);
                    }
                }
            }
            (Err(a), Err(b)) => {
                assert_eq!(a.kind(), b.kind(), "{raw_json}");
                assert_eq!(a.to_string(), b.to_string(), "{raw_json}");
            }
            _ => panic!("verify_entry_stored mismatch: ref={r_ref:?}, fast={r_fast:?}"),
        }

        // Also check mismatch error when entry bytes differ.
        if let Ok(Some(k)) = &expected_keys {
            let mut wrong = k.entry.clone();
            let flip = index_prefix.len();
            wrong[flip] ^= 0xff;
            let r_ref = reference_verify(&wrong);
            let r_fast = verify_entry_stored(
                definition,
                &stored_flat,
                &index_prefix,
                id,
                &wrong,
                &mut tuple_buf,
            );
            assert_eq!(
                r_ref.unwrap_err().to_string(),
                r_fast.unwrap_err().to_string()
            );
            (true, false)
        } else {
            (false, expected_keys.is_err())
        }
    }

    #[test]
    fn flat_index_verification_matches_json_keys() {
        let defs = vec![
            IndexDefinition {
                name: "i1".into(),
                fields: vec![vec!["a".into()]],
                unique: false,
                partial: None,
            },
            IndexDefinition {
                name: "u2".into(),
                fields: vec![vec!["a".into()], vec!["b".into()]],
                unique: true,
                partial: None,
            },
            IndexDefinition {
                name: "c4".into(),
                fields: vec![
                    vec!["a".into()],
                    vec!["b".into()],
                    vec!["c".into()],
                    vec!["d".into()],
                ],
                unique: true,
                partial: None,
            },
            IndexDefinition {
                name: "nested_field".into(),
                fields: vec![vec!["a".into(), "sub".into()]],
                unique: false,
                partial: None,
            },
            IndexDefinition {
                name: "p_exists_true".into(),
                fields: vec![vec!["a".into()]],
                unique: false,
                partial: Some(PartialFilter::Exists {
                    path: vec!["p".into()],
                    exists: true,
                }),
            },
            IndexDefinition {
                name: "p_exists_false".into(),
                fields: vec![vec!["a".into()]],
                unique: false,
                partial: Some(PartialFilter::Exists {
                    path: vec!["p".into()],
                    exists: false,
                }),
            },
            IndexDefinition {
                name: "p_nested_exists_false".into(),
                fields: vec![vec!["a".into()]],
                unique: false,
                partial: Some(PartialFilter::Exists {
                    path: vec!["p".into(), "nested".into()],
                    exists: false,
                }),
            },
            IndexDefinition {
                name: "p_eq_int".into(),
                fields: vec![vec!["a".into()]],
                unique: true,
                partial: Some(PartialFilter::Equals {
                    path: vec!["p".into()],
                    value: IndexValue::Integer(0),
                }),
            },
            IndexDefinition {
                name: "p_eq_bool".into(),
                fields: vec![vec!["a".into()], vec!["b".into()]],
                unique: false,
                partial: Some(PartialFilter::Equals {
                    path: vec!["p".into()],
                    value: IndexValue::Boolean(true),
                }),
            },
            IndexDefinition {
                name: "p_eq_null".into(),
                fields: vec![vec!["a".into()]],
                unique: false,
                partial: Some(PartialFilter::Equals {
                    path: vec!["p".into()],
                    value: IndexValue::Null,
                }),
            },
            IndexDefinition {
                name: "p_eq_str".into(),
                fields: vec![vec!["a".into()]],
                unique: false,
                partial: Some(PartialFilter::Equals {
                    path: vec!["p".into()],
                    value: IndexValue::String("ok".into()),
                }),
            },
        ];

        // Edge cases including -0, 1.0, i64 limits, u64, long strings (>128 and >256 bytes),
        // >8 members (spilling FlatDocument), and MAX_KEY overflow.
        let s150 = "x".repeat(150);
        let s256 = "y".repeat(256);
        let s257 = "z".repeat(257);
        let fixed_docs = vec![
            r#"{}"#.to_owned(),
            r#"{"a":null,"p":null}"#.to_owned(),
            r#"{"a":0,"p":-0}"#.to_owned(),
            r#"{"a":-0,"p":0}"#.to_owned(),
            r#"{"a":1.0,"b":2}"#.to_owned(),
            r#"{"a":9223372036854775807,"b":-9223372036854775808,"p":true}"#.to_owned(),
            r#"{"a":9223372036854775808,"b":1}"#.to_owned(),
            r#"{"a":18446744073709551615}"#.to_owned(),
            format!(r#"{{"a":"{s150}","b":"{s150}","c":"{s150}","d":"{s150}"}}"#),
            format!(r#"{{"a":"{s256}","b":"{s256}","c":"{s256}","d":"{s256}"}}"#),
            format!(r#"{{"a":"{s257}","b":1}}"#),
            r#"{"m0":0,"m1":1,"m2":2,"m3":3,"m4":4,"m5":5,"m6":6,"m7":7,"m8":8,"a":42,"b":"hi","p":"ok"}"#
                .to_owned(),
        ];
        for doc in &fixed_docs {
            for def in &defs {
                check_against_keys("items", def, doc, "doc-1");
            }
        }

        // Randomized differential test across thousands of flat documents.
        let values = [
            "null",
            "true",
            "false",
            "0",
            "-0",
            "1",
            "-1",
            "42",
            "1.5",
            "1.0",
            "9223372036854775807",
            "-9223372036854775808",
            "9223372036854775808",
            "\"\"",
            "\"ok\"",
            "\"é\"",
        ];
        let names = ["a", "b", "c", "d", "p", "e1", "e2", "e3", "e4", "e5"];
        let mut state = 0x517c_c1b7_2722_0a95_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let (mut matched, mut errored, mut omitted) = (0usize, 0usize, 0usize);
        for _ in 0..3_000 {
            let mut parts = Vec::new();
            for &name in &names {
                if next() % 3 != 0 {
                    let val = values[(next() % values.len() as u64) as usize];
                    parts.push(format!("\"{name}\":{val}"));
                }
            }
            let raw = format!("{{{}}}", parts.join(","));
            let def = &defs[(next() % defs.len() as u64) as usize];
            let (is_match, is_err) = check_against_keys("col", def, &raw, "i:8000000000000001");
            if is_match {
                matched += 1;
            } else if is_err {
                errored += 1;
            } else {
                omitted += 1;
            }
        }
        assert!(
            matched > 200 && errored > 200 && omitted > 200,
            "matched={matched} errored={errored} omitted={omitted}"
        );

        // Verify compare_flat matches compare_json on all scalar kinds including >256-byte strings.
        let candidates = [
            IndexValue::Null,
            IndexValue::Boolean(false),
            IndexValue::Boolean(true),
            IndexValue::Integer(i64::MIN),
            IndexValue::Integer(0),
            IndexValue::Integer(i64::MAX),
            IndexValue::String("".into()),
            IndexValue::String("a".into()),
            IndexValue::String("é".into()),
        ];
        let flat_vals = [
            (FlatScalar::Null, Value::Null),
            (FlatScalar::Bool(false), json!(false)),
            (FlatScalar::Bool(true), json!(true)),
            (FlatScalar::Int(i64::MIN), json!(i64::MIN)),
            (FlatScalar::Int(0), json!(0)),
            (FlatScalar::Int(i64::MAX), json!(i64::MAX)),
            (FlatScalar::UInt(u64::MAX), json!(u64::MAX)),
            (FlatScalar::Float(-0.0), json!(-0.0)),
            (FlatScalar::Float(1.0), json!(1.0)),
            (FlatScalar::Str(""), json!("")),
            (FlatScalar::Str("a"), json!("a")),
            (FlatScalar::Str(&s256), json!(s256)),
            (FlatScalar::Str(&s257), json!(s257)),
        ];
        for expected in &candidates {
            for (fs, jv) in &flat_vals {
                assert_eq!(expected.compare_flat(*fs), expected.compare_json(jv));
            }
        }
    }
}
