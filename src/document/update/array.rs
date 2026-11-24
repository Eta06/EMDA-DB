//! Bounded ordered array mutations on an isolated in-memory document value.
use super::{DocumentUpdate, equals, parent_mut};
use crate::document::{equality::EqualityWork, key};
use serde_json::{Map, Value};
use std::io;

// Array entries visited or moved. Nested equality work is counted separately.
const MAX_ARRAY_ENTRIES_EXAMINED: usize = 1_000_000;
pub(super) fn validate_inputs(
    updates: &[DocumentUpdate],
    budget: &mut super::input::InputBudget,
) -> io::Result<()> {
    for update in updates {
        let value = match update {
            DocumentUpdate::ArrayPush { value, .. }
            | DocumentUpdate::ArrayAddUnique { value, .. }
            | DocumentUpdate::ArrayRemove { value, .. } => value,
            _ => continue,
        };
        budget.value(value)?;
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
    equality: &mut EqualityWork,
) -> io::Result<()> {
    match update {
        DocumentUpdate::ArrayPush { path, value } => {
            let values = array(document, path, true)?.expect("created array");
            values.push(value.clone());
        }
        DocumentUpdate::ArrayAddUnique { path, value } => {
            let values = array(document, path, true)?.expect("created array");
            reserve_work(work, values.len())?;
            for item in values.iter() {
                if equals(equality, item, value)? {
                    return Ok(());
                }
            }
            values.push(value.clone());
        }
        DocumentUpdate::ArrayRemove { path, value } => {
            if let Some(values) = array(document, path, false)? {
                reserve_work(work, values.len())?;
                // Compact the survivors in order without allocating a second array.
                // The caller discards the private document if equality fails.
                let mut kept = 0;
                for index in 0..values.len() {
                    if !equals(equality, &values[index], value)? {
                        values.swap(kept, index);
                        kept += 1;
                    }
                }
                values.truncate(kept);
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
        // Only discarded elements count. A cap at or above the current length,
        // including `usize::MAX`, must not allocate or add work.
        DocumentUpdate::ArrayTruncate { path, max_len } => {
            if let Some(values) = array(document, path, false)? {
                let removed = values.len().saturating_sub(*max_len);
                if removed > 0 {
                    reserve_work(work, removed)?;
                    values.truncate(*max_len);
                }
            }
        }
        DocumentUpdate::ArrayKeepLast { path, max_len } => {
            if let Some(values) = array(document, path, false)? {
                let removed = values.len().saturating_sub(*max_len);
                if removed > 0 {
                    // Draining shifts the retained suffix as well as dropping the prefix.
                    reserve_work(work, values.len())?;
                    values.drain(..removed);
                }
            }
        }
        DocumentUpdate::ArrayReverse { path } => {
            if let Some(values) = array(document, path, false)? {
                reserve_work(work, values.len())?;
                values.reverse();
            }
        }
        _ => unreachable!("array update variant"),
    }
    Ok(())
}
