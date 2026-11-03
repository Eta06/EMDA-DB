//! Versioned log record encoding and validation.

use std::io;

const MAGIC: &[u8; 4] = b"DB01";
pub(crate) const HEADER_SIZE: usize = 17;
pub(crate) const CHECKSUM_SIZE: usize = 4;
const MAX_KEY: usize = 1024;
const MAX_VALUE: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    Put,
    Delete,
}

pub(crate) struct Header {
    pub(crate) operation: Operation,
    pub(crate) key_len: usize,
    pub(crate) value_len: usize,
}

impl Header {
    pub(crate) fn parse(bytes: &[u8; HEADER_SIZE]) -> io::Result<Self> {
        if &bytes[..4] != MAGIC || crc32fast::hash(&bytes[..13]) != read_u32(&bytes[13..17]) {
            return Err(invalid_data("invalid record header"));
        }
        let operation = match bytes[4] {
            1 => Operation::Put,
            2 => Operation::Delete,
            _ => return Err(invalid_data("invalid record operation")),
        };
        let key_len = read_u32(&bytes[5..9]) as usize;
        let value_len = read_u32(&bytes[9..13]) as usize;
        if key_len == 0
            || key_len > MAX_KEY
            || value_len > MAX_VALUE
            || (operation == Operation::Delete && value_len != 0)
        {
            return Err(invalid_data("invalid record length"));
        }
        Ok(Self {
            operation,
            key_len,
            value_len,
        })
    }

    pub(crate) fn record_len(&self) -> u64 {
        (HEADER_SIZE + self.key_len + self.value_len + CHECKSUM_SIZE) as u64
    }
}

pub(crate) fn encode(operation: Operation, key: &[u8], value: &[u8]) -> io::Result<Vec<u8>> {
    validate_key(key)?;
    if value.len() > MAX_VALUE {
        return Err(invalid_input("value exceeds 16 MiB"));
    }
    if operation == Operation::Delete && !value.is_empty() {
        return Err(invalid_input("delete records cannot have a value"));
    }
    let mut record = Vec::with_capacity(HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE);
    record.extend_from_slice(MAGIC);
    record.push(match operation {
        Operation::Put => 1,
        Operation::Delete => 2,
    });
    record.extend_from_slice(&(key.len() as u32).to_le_bytes());
    record.extend_from_slice(&(value.len() as u32).to_le_bytes());
    record.extend_from_slice(&crc32fast::hash(&record).to_le_bytes());
    record.extend_from_slice(key);
    record.extend_from_slice(value);
    record.extend_from_slice(&crc32fast::hash(&record).to_le_bytes());
    Ok(record)
}

pub(crate) fn verify(header: &[u8; HEADER_SIZE], payload: &[u8], expected: u32) -> io::Result<()> {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(header);
    hasher.update(payload);
    if hasher.finalize() != expected {
        return Err(invalid_data("record checksum mismatch"));
    }
    Ok(())
}

pub(crate) fn validate_key(key: &[u8]) -> io::Result<()> {
    if key.is_empty() || key.len() > MAX_KEY {
        return Err(invalid_input("key must contain 1 to 1024 bytes"));
    }
    Ok(())
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("fixed-size header field"))
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
pub(crate) fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
