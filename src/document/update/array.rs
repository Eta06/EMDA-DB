//! Bounded ordered array mutations on an isolated in-memory document value.
use super::{DocumentUpdate, parent_mut};
use crate::document::key;
use serde_json::{Map, Value};
use std::io::{self, Write};

// Logical work, not a wall-time or total-memory bound. Equality comparisons
// may inspect complex nested values, and each transaction has other work.
const MAX_ARRAY_ENTRIES_EXAMINED: usize = 1_000_000;
const MAX_ARRAY_INPUT_BYTES: usize = 4 * 1024 * 1024;

struct InputBudget(usize);
impl Write for InputBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| key::invalid("array update input exceeds 4 MiB"))?;
        if self.0 > MAX_ARRAY_INPUT_BYTES {
            return Err(key::invalid("array update input exceeds 4 MiB"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn validate_inputs(updates: &[DocumentUpdate]) -> io::Result<()> {
    let mut budget = InputBudget(0);
    for update in updates {
        let value = match update {
            DocumentUpdate::ArrayPush { value, .. }
            | DocumentUpdate::ArrayAddUnique { value, .. }
            | DocumentUpdate::ArrayRemove { value, .. } => value,
            _ => continue,
        };
        super::schema::validate_tree(value)?;
        serde_json::to_writer(&mut budget, value)
            .map_err(|_| key::invalid("array update input exceeds 4 MiB or cannot be encoded"))?;
    }
    Ok(())
}

fn array<'a>(
    document: &'a mut Value,
    path: &[String],
    create: bool,
) -> io::Result<Option<&'a mut Vec<Value>>> {
    let Some(parent): Option<&mut Map<String, Value>> = parent_mut(document, path, create)? else {
        return Ok(None);
    };
    let name = path.last().expect("validated path");
    if create && !parent.contains_key(name) {
        parent.insert(name.clone(), Value::Array(Vec::new()));
    }
    match parent.get_mut(name) {
        None => Ok(None),
        Some(Value::Array(values)) => Ok(Some(values)),
        Some(_) => Err(key::invalid("array update requires an array")),
    }
}

fn reserve_work(work: &mut usize, count: usize) -> io::Result<()> {
    *work = work
        .checked_add(count)
        .ok_or_else(|| key::invalid("array update work limit exceeded"))?;
    if *work > MAX_ARRAY_ENTRIES_EXAMINED {
        return Err(key::invalid("array update work limit exceeded"));
    }
    Ok(())
}

pub(super) fn apply(
    document: &mut Value,
    update: &DocumentUpdate,
    work: &mut usize,
) -> io::Result<()> {
    match update {
        DocumentUpdate::ArrayPush { path, value } => {
            let values = array(document, path, true)?.expect("created array");
            values.push(value.clone());
        }
        DocumentUpdate::ArrayAddUnique { path, value } => {
            let values = array(document, path, true)?.expect("created array");
            reserve_work(work, values.len())?;
            if !values.contains(value) {
                values.push(value.clone());
            }
        }
        DocumentUpdate::ArrayRemove { path, value } => {
            if let Some(values) = array(document, path, false)? {
                reserve_work(work, values.len())?;
                values.retain(|item| item != value);
            }
        }
        DocumentUpdate::ArrayPop { path, first } => {
            if let Some(values) = array(document, path, false)? {
                if !values.is_empty() {
                    if *first {
                        reserve_work(work, values.len())?;
                        values.remove(0);
                    } else {
                        values.pop();
                    }
                }
            }
        }
        _ => unreachable!("array update variant"),
    }
    Ok(())
}
