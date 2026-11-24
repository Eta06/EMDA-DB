//! Versioned log record encoding and validation.

use std::io;

const MAGIC: &[u8; 4] = b"DB01";
const BATCH_MAGIC: &[u8; 4] = b"DB02";
pub(crate) const HEADER_SIZE: usize = 17;
pub(crate) const CHECKSUM_SIZE: usize = 4;
pub(crate) const MAX_KEY: usize = 1024;
pub(crate) const MAX_VALUE: usize = 16 * 1024 * 1024;

pub(crate) mod batch;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    Put,
    Delete,
    Batch,
}

pub(crate) struct Header {
    pub(crate) operation: Operation,
    pub(crate) key_len: usize,
    pub(crate) value_len: usize,
}

impl Header {
    pub(crate) fn parse(bytes: &[u8; HEADER_SIZE]) -> io::Result<Self> {
        if crc32fast::hash(&bytes[..13]) != read_u32(&bytes[13..17]) {
            return Err(invalid_data("invalid record header"));
        }
        let operation = match (&bytes[..4], bytes[4]) {
            (magic, 1) if magic == MAGIC => Operation::Put,
            (magic, 2) if magic == MAGIC => Operation::Delete,
            (magic, 3) if magic == BATCH_MAGIC => Operation::Batch,
            _ => return Err(invalid_data("invalid record operation")),
        };
        let key_len = read_u32(&bytes[5..9]) as usize;
        let value_len = read_u32(&bytes[9..13]) as usize;
        if key_len == 0
            || key_len > MAX_KEY
            || value_len > MAX_VALUE
            || (operation == Operation::Delete && value_len != 0)
            || (operation == Operation::Batch && key_len != 1)
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

pub(crate) fn validate(operation: Operation, key: &[u8], value: &[u8]) -> io::Result<()> {
    validate_lengths(operation, key, value.len())
}

fn validate_lengths(operation: Operation, key: &[u8], value_len: usize) -> io::Result<()> {
    validate_key(key)?;
    if value_len > MAX_VALUE {
        return Err(invalid_input("value exceeds 16 MiB"));
    }
    if operation == Operation::Delete && value_len != 0 {
        return Err(invalid_input("delete records cannot have a value"));
    }
    if operation == Operation::Batch && key != [0] {
        return Err(invalid_input("invalid batch record key"));
    }
    Ok(())
}

pub(crate) fn encode_header(
    operation: Operation,
    key: &[u8],
    value_len: usize,
) -> io::Result<[u8; HEADER_SIZE]> {
    validate_lengths(operation, key, value_len)?;
    let mut header = [0; HEADER_SIZE];
    header[..4].copy_from_slice(if operation == Operation::Batch {
        BATCH_MAGIC
    } else {
        MAGIC
    });
    header[4] = match operation {
        Operation::Put => 1,
        Operation::Delete => 2,
        Operation::Batch => 3,
    };
    header[5..9].copy_from_slice(&(key.len() as u32).to_le_bytes());
    header[9..13].copy_from_slice(&(value_len as u32).to_le_bytes());
    let checksum = crc32fast::hash(&header[..13]).to_le_bytes();
    header[13..].copy_from_slice(&checksum);
    Ok(header)
}

pub(crate) fn encode(operation: Operation, key: &[u8], value: &[u8]) -> io::Result<Vec<u8>> {
    let header = encode_header(operation, key, value.len())?;
    let mut record = encoding_buffer(HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE)?;
    append_record(&mut record, &header, key, value);
    Ok(record)
}

fn append_record(output: &mut Vec<u8>, header: &[u8; HEADER_SIZE], key: &[u8], value: &[u8]) {
    let start = output.len();
    output.extend_from_slice(header);
    output.extend_from_slice(key);
    output.extend_from_slice(value);
    output.extend_from_slice(&crc32fast::hash(&output[start..]).to_le_bytes());
}

pub(crate) fn encode_admitted(
    operation: Operation,
    key: &[u8],
    value: &[u8],
    admit: impl FnOnce(usize) -> io::Result<()>,
) -> io::Result<Vec<u8>> {
    validate(operation, key, value)?;
    let length = HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE;
    admit(length)?;
    let mut result = encoding_buffer(length)?;
    let header = encode_header(operation, key, value.len())?;
    append_record(&mut result, &header, key, value);
    Ok(result)
}

fn encoding_buffer(length: usize) -> io::Result<Vec<u8>> {
    let mut result = Vec::new();
    result.try_reserve_exact(length).map_err(|_| {
        io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate encoded record")
    })?;
    Ok(result)
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
