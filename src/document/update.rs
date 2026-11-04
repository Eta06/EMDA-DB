//! Conditional, ordered JSON updates staged in a single storage transaction.
use super::{key, schema, transaction::DocumentTransaction};
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
pub enum DocumentUpdate {
    /// Set a value, creating missing intermediate objects.
    Set { path: Vec<String>, value: Value },
    /// Remove a field; removing a missing field is a no-op.
    Unset { path: Vec<String> },
    /// Add to a signed 64-bit integer; a missing field starts at zero.
    Increment { path: Vec<String>, amount: i64 },
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
                    | DocumentUpdate::Unset { path }
                    | DocumentUpdate::Increment { path, .. } => path,
                };
                schema::validate_path(path)?;
                if let DocumentUpdate::Set { value, .. } = update {
                    schema::validate_tree(value)?;
                }
            }
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
