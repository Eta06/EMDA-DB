//! Conditional, ordered JSON updates staged in a single storage transaction.
mod array;
mod scalar;
use super::{index::IndexValue, key, schema, transaction::DocumentTransaction};
use serde_json::{Map, Value};
use std::io;

/// A precondition distinguishing missing fields from explicit null.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldCondition {
    /// An object path, without array traversal.
    pub path: Vec<String>,
    /// `None` matches a missing field; `Some(Value::Null)` matches null.
    pub expected: Option<Value>,
}

/// One update applied in order to a document object.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum DocumentUpdate {
    /// Set a value, creating missing intermediate objects.
    Set { path: Vec<String>, value: Value },
    /// Set a missing field; an existing field, including null, is unchanged.
    SetIfMissing { path: Vec<String>, value: Value },
    /// Remove a field; removing a missing field is a no-op.
    Unset { path: Vec<String> },
    /// Add to a signed 64-bit integer; a missing field starts at zero.
    Increment { path: Vec<String>, amount: i64 },
    /// Multiply a signed integer by a signed factor; a missing field starts at zero.
    Multiply { path: Vec<String>, factor: i64 },
    /// Set a missing field or replace a larger indexable scalar.
    Min {
        path: Vec<String>,
        value: IndexValue,
    },
    /// Set a missing field or replace a smaller indexable scalar.
    Max {
        path: Vec<String>,
        value: IndexValue,
    },
    /// Move a present field to a non-overlapping path, replacing its destination.
    Rename { from: Vec<String>, to: Vec<String> },
    /// Append one value, creating a missing array and intermediate objects.
    ArrayPush { path: Vec<String>, value: Value },
    /// Append one value only if no element has exact JSON equality with it.
    ArrayAddUnique { path: Vec<String>, value: Value },
    /// Remove every element with exact JSON equality; a missing field is a no-op.
    ArrayRemove { path: Vec<String>, value: Value },
    /// Remove the first or last element; a missing or empty array is a no-op.
    ArrayPop { path: Vec<String>, first: bool },
}

fn parent_mut<'a>(
    document: &'a mut Value,
    path: &[String],
    create: bool,
) -> io::Result<Option<&'a mut Map<String, Value>>> {
    let mut current = document;
    for name in &path[..path.len() - 1] {
        let object = current
            .as_object_mut()
            .ok_or_else(|| key::invalid("update path traverses a non-object"))?;
        if !object.contains_key(name) {
            if !create {
                return Ok(None);
            }
            object.insert(name.clone(), Value::Object(Map::new()));
        }
        current = object.get_mut(name).expect("member exists");
    }
    current
        .as_object_mut()
        .map(Some)
        .ok_or_else(|| key::invalid("update path traverses a non-object"))
}

fn apply(document: &mut Value, updates: &[DocumentUpdate]) -> io::Result<()> {
    let mut array_work = 0;
    for update in updates {
        match update {
            DocumentUpdate::Set { path, value } => {
                let object = parent_mut(document, path, true)?.expect("create parent");
                object.insert(path.last().expect("validated path").clone(), value.clone());
            }
            DocumentUpdate::Unset { path } => {
                if let Some(object) = parent_mut(document, path, false)? {
                    object.remove(path.last().expect("validated path"));
                }
            }
            DocumentUpdate::Increment { path, amount } => {
                let object = parent_mut(document, path, true)?.expect("create parent");
                let name = path.last().expect("validated path");
                let old = match object.get(name) {
                    None => 0,
                    Some(value) => value.as_i64().ok_or_else(|| {
                        key::invalid("increment requires a signed 64-bit integer")
                    })?,
                };
                let value = old
                    .checked_add(*amount)
                    .ok_or_else(|| key::invalid("increment overflows a signed 64-bit integer"))?;
                object.insert(name.clone(), Value::from(value));
            }
            DocumentUpdate::SetIfMissing { .. }
            | DocumentUpdate::Multiply { .. }
            | DocumentUpdate::Min { .. }
            | DocumentUpdate::Max { .. }
            | DocumentUpdate::Rename { .. } => scalar::apply(document, update)?,
            DocumentUpdate::ArrayPush { .. }
            | DocumentUpdate::ArrayAddUnique { .. }
            | DocumentUpdate::ArrayRemove { .. }
            | DocumentUpdate::ArrayPop { .. } => array::apply(document, update, &mut array_work)?,
        }
    }
    Ok(())
}

impl DocumentTransaction<'_, '_> {
    /// Conditionally applies ordered updates. All conditions must match. If
    /// `upsert` is true a missing ID starts as an empty object; otherwise it
    /// returns false. A false condition causes no write and returns false.
    pub fn update_if(
        &mut self,
        collection: &str,
        id: &str,
        conditions: &[FieldCondition],
        updates: &[DocumentUpdate],
        upsert: bool,
    ) -> io::Result<bool> {
        self.mutate(|this| {
            if conditions.len() > 64 || updates.is_empty() || updates.len() > 128 {
                return Err(key::invalid(
                    "update requires 1 to 128 operators and at most 64 conditions",
                ));
            }
            for condition in conditions {
                schema::validate_path(&condition.path)?;
                if let Some(value) = &condition.expected {
                    schema::validate_tree(value)?;
                }
            }
            for update in updates {
                let path = match update {
                    DocumentUpdate::Set { path, .. }
                    | DocumentUpdate::SetIfMissing { path, .. }
                    | DocumentUpdate::Unset { path }
                    | DocumentUpdate::Increment { path, .. }
                    | DocumentUpdate::Multiply { path, .. }
                    | DocumentUpdate::Min { path, .. }
                    | DocumentUpdate::Max { path, .. }
                    | DocumentUpdate::ArrayPush { path, .. }
                    | DocumentUpdate::ArrayAddUnique { path, .. }
                    | DocumentUpdate::ArrayRemove { path, .. }
                    | DocumentUpdate::ArrayPop { path, .. } => path,
                    DocumentUpdate::Rename { from, to } => {
                        schema::validate_path(to)?;
                        if from.starts_with(to) || to.starts_with(from) {
                            return Err(key::invalid("rename paths must not overlap"));
                        }
                        from
                    }
                };
                schema::validate_path(path)?;
                if let DocumentUpdate::Set { value, .. }
                | DocumentUpdate::SetIfMissing { value, .. } = update
                {
                    schema::validate_tree(value)?;
                }
                if let DocumentUpdate::Min { value, .. } | DocumentUpdate::Max { value, .. } =
                    update
                {
                    value.encode(&mut Vec::new())?;
                }
            }
            array::validate_inputs(updates)?;
            let Some(mut document) = this
                .get(collection, id)?
                .or_else(|| upsert.then(|| Value::Object(Map::new())))
            else {
                return Ok(false);
            };
            if conditions.iter().any(|condition| {
                schema::field(&document, &condition.path) != condition.expected.as_ref()
            }) {
                return Ok(false);
            }
            apply(&mut document, updates)?;
            this.set(collection, id, &document, false, upsert)?;
            Ok(true)
        })
    }
}
