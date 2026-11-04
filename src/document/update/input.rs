//! Shared encoded-value admission budget for one conditional update.
use super::{IndexValue, key, schema};
use serde::Serialize;
use serde_json::Value;
use std::io::{self, Write};

/// Maximum combined encoded JSON bytes in condition expectations, set/array
/// values and minimum/maximum operands of one update.
/// This logical input limit is not a total-memory or process quota.
pub const MAX_DOCUMENT_UPDATE_INPUT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Default)]
pub(super) struct InputBudget(usize);

impl Write for InputBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| key::invalid("update input exceeds 4 MiB"))?;
        if self.0 > MAX_DOCUMENT_UPDATE_INPUT_BYTES {
            return Err(key::invalid("update input exceeds 4 MiB"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl InputBudget {
    fn count<T: Serialize + ?Sized>(&mut self, value: &T) -> io::Result<()> {
        serde_json::to_writer(self, value)
            .map_err(|_| key::invalid("update input exceeds 4 MiB or cannot be encoded"))
    }
    pub(super) fn value(&mut self, value: &Value) -> io::Result<()> {
        schema::validate_tree(value)?;
        self.count(value)
    }
    pub(super) fn scalar(&mut self, value: &IndexValue) -> io::Result<()> {
        match value {
            IndexValue::Null => self.count(&()),
            IndexValue::Boolean(value) => self.count(value),
            IndexValue::Integer(value) => self.count(value),
            IndexValue::String(value) => self.count(value),
        }
    }
}
