//! Logical limits for exact JSON equality in document operations.
use super::key;
use serde_json::Value;
use std::io;

const MAX_NESTED_COMPARISONS: usize = 1_000_000;
const MAX_EQUALITY_TEXT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub(super) enum EqualityLimit {
    Nodes,
    Text,
}

impl EqualityLimit {
    pub(super) fn into_io_error(self, text_error: &'static str) -> io::Error {
        key::invalid(match self {
            Self::Nodes => "nested equality comparison limit exceeded",
            Self::Text => text_error,
        })
    }
}

#[derive(Default)]
pub(super) struct EqualityWork {
    nested_comparisons: usize,
    equality_text_bytes: usize,
}

impl EqualityWork {
    fn charge_equality_text(&mut self, bytes: usize) -> Result<(), EqualityLimit> {
        if bytes > MAX_EQUALITY_TEXT_BYTES - self.equality_text_bytes {
            return Err(EqualityLimit::Text);
        }
        self.equality_text_bytes += bytes;
        Ok(())
    }

    fn equal_child(&mut self, left: &Value, right: &Value) -> Result<bool, EqualityLimit> {
        if self.nested_comparisons == MAX_NESTED_COMPARISONS {
            return Err(EqualityLimit::Nodes);
        }
        self.nested_comparisons += 1;
        self.equals(left, right)
    }

    pub(super) fn equals(&mut self, left: &Value, right: &Value) -> Result<bool, EqualityLimit> {
        match (left, right) {
            (Value::String(left), Value::String(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                self.charge_equality_text(left.len())?;
                Ok(left == right)
            }
            (Value::Array(left), Value::Array(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (left, right) in left.iter().zip(right) {
                    if !self.equal_child(left, right)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (Value::Object(left), Value::Object(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (key, left) in left {
                    self.charge_equality_text(key.len())?;
                    let Some(right) = right.get(key) else {
                        return Ok(false);
                    };
                    if !self.equal_child(left, right)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(left == right),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exact_text_limit_includes_object_keys() {
        let mut budget = EqualityWork {
            equality_text_bytes: MAX_EQUALITY_TEXT_BYTES - 3,
            ..Default::default()
        };
        let value = json!({"key": 0});
        assert!(budget.equals(&value, &value).unwrap());
        assert!(matches!(
            budget.equals(&value, &value),
            Err(EqualityLimit::Text)
        ));
    }
}
