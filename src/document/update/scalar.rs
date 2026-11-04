//! Ordered scalar and path updates on an isolated in-memory document.
use super::{DocumentUpdate, IndexValue, key, parent_mut};
use serde_json::Value;
use std::io;

fn json(value: &IndexValue) -> Value {
    match value {
        IndexValue::Null => Value::Null,
        IndexValue::Boolean(value) => Value::Bool(*value),
        IndexValue::Integer(value) => Value::from(*value),
        IndexValue::String(value) => Value::String(value.clone()),
    }
}

fn update_extreme(
    document: &mut Value,
    path: &[String],
    value: &IndexValue,
    minimum: bool,
) -> io::Result<()> {
    let object = parent_mut(document, path, true)?.expect("create parent");
    let name = path.last().expect("validated path");
    let replace = if let Some(current) = object.get(name) {
        let mut old = Vec::new();
        IndexValue::from_json(current)?.encode(&mut old)?;
        let mut new = Vec::new();
        value.encode(&mut new)?;
        if minimum { new < old } else { new > old }
    } else {
        true
    };
    if replace {
        object.insert(name.clone(), json(value));
    }
    Ok(())
}

pub(super) fn apply(document: &mut Value, update: &DocumentUpdate) -> io::Result<()> {
    match update {
        DocumentUpdate::SetIfMissing { path, value } => {
            let object = parent_mut(document, path, true)?.expect("create parent");
            object
                .entry(path.last().expect("validated path").clone())
                .or_insert_with(|| value.clone());
        }
        DocumentUpdate::Multiply { path, factor } => {
            let object = parent_mut(document, path, true)?.expect("create parent");
            let name = path.last().expect("validated path");
            let old = match object.get(name) {
                None => 0,
                Some(value) => value
                    .as_i64()
                    .ok_or_else(|| key::invalid("multiply requires a signed 64-bit integer"))?,
            };
            let value = old
                .checked_mul(*factor)
                .ok_or_else(|| key::invalid("multiply overflows a signed 64-bit integer"))?;
            object.insert(name.clone(), Value::from(value));
        }
        DocumentUpdate::Min { path, value } => update_extreme(document, path, value, true)?,
        DocumentUpdate::Max { path, value } => update_extreme(document, path, value, false)?,
        DocumentUpdate::Rename { from, to } => {
            let Some(value) = parent_mut(document, from, false)?
                .and_then(|parent| parent.remove(from.last().expect("validated path")))
            else {
                return Ok(());
            };
            let parent = parent_mut(document, to, true)?.expect("create parent");
            parent.insert(to.last().expect("validated path").clone(), value);
        }
        _ => unreachable!("scalar update variant"),
    }
    Ok(())
}
