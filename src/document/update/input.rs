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
        self.charge(bytes.len())?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl InputBudget {
    fn charge(&mut self, bytes: usize) -> io::Result<()> {
        let total = self
            .0
            .checked_add(bytes)
            .ok_or_else(|| key::invalid("update input exceeds 4 MiB"))?;
        if total > MAX_DOCUMENT_UPDATE_INPUT_BYTES {
            return Err(key::invalid("update input exceeds 4 MiB"));
        }
        self.0 = total;
        Ok(())
    }

    fn count<T: Serialize + ?Sized>(&mut self, value: &T) -> io::Result<()> {
        serde_json::to_writer(self, value)
            .map_err(|_| key::invalid("update input exceeds 4 MiB or cannot be encoded"))
    }
    pub(super) fn value(&mut self, value: &Value) -> io::Result<()> {
        schema::validate_tree(value)?;
        self.json_value(value)
    }

    // The validated tree has depth at most 64. Count punctuation and keys
    // in serializer order while delegating numeric formatting to serde_json.
    fn json_value(&mut self, value: &Value) -> io::Result<()> {
        match value {
            Value::Null => self.charge(4),
            Value::Bool(false) => self.charge(5),
            Value::Bool(true) => self.charge(4),
            Value::Number(number) => self.count(number),
            Value::String(value) => self.string(value),
            Value::Array(values) => {
                self.charge(1)?;
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        self.charge(1)?;
                    }
                    self.json_value(value)?;
                }
                self.charge(1)
            }
            Value::Object(values) => {
                self.charge(1)?;
                for (index, (key, value)) in values.iter().enumerate() {
                    if index != 0 {
                        self.charge(1)?;
                    }
                    self.string(key)?;
                    self.charge(1)?;
                    self.json_value(value)?;
                }
                self.charge(1)
            }
        }
    }
    pub(super) fn scalar(&mut self, value: &IndexValue) -> io::Result<()> {
        match value {
            IndexValue::Null => self.count(&()),
            IndexValue::Boolean(value) => self.count(value),
            IndexValue::Integer(value) => self.count(value),
            IndexValue::String(value) => self.string(value),
        }
    }

    fn string(&mut self, value: &str) -> io::Result<()> {
        let remaining = MAX_DOCUMENT_UPDATE_INPUT_BYTES - self.0;
        self.charge(json_string_length(value, remaining)?)
    }
}

// `serde_json` emits UTF-8 directly except quotes, backslashes and ASCII
// controls. A word with no such byte needs no per-byte escape accounting.
const ONES: u128 = u128::MAX / 255;
const HIGH_BITS: u128 = ONES * 0x80;
const TOP_BITS: u128 = ONES * 0xe0;
const QUOTES: u128 = ONES * (b'"' as u128);
const BACKSLASHES: u128 = ONES * (b'\\' as u128);

fn has_zero_byte(value: u128) -> bool {
    value.wrapping_sub(ONES) & !value & HIGH_BITS != 0
}

fn json_string_length(value: &str, limit: usize) -> io::Result<usize> {
    let mut total = value
        .len()
        .checked_add(2)
        .filter(|size| *size <= limit)
        .ok_or_else(|| key::invalid("update input exceeds 4 MiB"))?;
    let mut account = |bytes: &[u8]| -> io::Result<()> {
        for &byte in bytes {
            let extra = match byte {
                b'"' | b'\\' | b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' => 1,
                0x00..=0x1f => 5,
                _ => 0,
            };
            total = total
                .checked_add(extra)
                .filter(|size| *size <= limit)
                .ok_or_else(|| key::invalid("update input exceeds 4 MiB"))?;
        }
        Ok(())
    };
    let mut chunks = value.as_bytes().chunks_exact(16);
    for chunk in &mut chunks {
        let word = u128::from_ne_bytes(chunk.try_into().expect("16-byte chunk"));
        if has_zero_byte(word & TOP_BITS)
            || has_zero_byte(word ^ QUOTES)
            || has_zero_byte(word ^ BACKSLASHES)
        {
            account(chunk)?;
        }
    }
    account(chunks.remainder())?;
    Ok(total)
}

#[cfg(test)]
#[path = "input_tests.rs"]
mod tests;
