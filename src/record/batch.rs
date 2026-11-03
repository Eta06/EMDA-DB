//! Versioned, checksummed atomic-batch payloads inside one DB02 record.
use super::{
    CHECKSUM_SIZE, HEADER_SIZE, Header, MAX_VALUE, Operation, encode, invalid_data, validate,
    verify,
};
use std::io;

/// Maximum number of ordered operations in one atomic batch.
pub const MAX_ATOMIC_BATCH_OPERATIONS: usize = 1024;
/// Maximum encoded payload bytes in one atomic batch, including nested records.
pub const MAX_ATOMIC_BATCH_BYTES: usize = MAX_VALUE;

/// One put or delete in an ordered atomic batch.
#[derive(Clone, Copy, Debug)]
pub enum BatchOperation<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

impl<'a> BatchOperation<'a> {
    pub(crate) fn fields(self) -> (Operation, &'a [u8], &'a [u8]) {
        match self {
            Self::Put { key, value } => (Operation::Put, key, value),
            Self::Delete { key } => (Operation::Delete, key, &[]),
        }
    }
}

pub(crate) struct Decoded<'a> {
    pub(crate) operation: Operation,
    pub(crate) key: &'a [u8],
    // Relative to the beginning of the outer batch record's value.
    pub(crate) value_offset: usize,
    pub(crate) value_len: usize,
    pub(crate) checksum: u32,
}

pub(crate) fn encode_payload(operations: &[BatchOperation<'_>]) -> io::Result<Vec<u8>> {
    if operations.is_empty() || operations.len() > MAX_ATOMIC_BATCH_OPERATIONS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "atomic batch must contain 1 to 1024 operations",
        ));
    }
    let mut total = 4_usize;
    for &operation in operations {
        let (kind, key, value) = operation.fields();
        validate(kind, key, value)?;
        total = total
            .checked_add(HEADER_SIZE + key.len() + value.len() + CHECKSUM_SIZE)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "batch length overflow"))?;
        if total > MAX_ATOMIC_BATCH_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "atomic batch exceeds 16 MiB encoded payload",
            ));
        }
    }
    let mut result = Vec::with_capacity(total);
    result.extend_from_slice(&(operations.len() as u32).to_le_bytes());
    for &operation in operations {
        let (kind, key, value) = operation.fields();
        result.extend_from_slice(&encode(kind, key, value)?);
    }
    Ok(result)
}

pub(crate) fn decode_payload(payload: &[u8]) -> io::Result<Vec<Decoded<'_>>> {
    let count_bytes: [u8; 4] = payload
        .get(..4)
        .ok_or_else(|| invalid_data("incomplete batch count"))?
        .try_into()
        .expect("four bytes");
    let count = u32::from_le_bytes(count_bytes) as usize;
    if count == 0 || count > MAX_ATOMIC_BATCH_OPERATIONS {
        return Err(invalid_data("invalid batch operation count"));
    }
    let mut cursor = 4_usize;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        let header_end = cursor
            .checked_add(HEADER_SIZE)
            .ok_or_else(|| invalid_data("batch offset overflow"))?;
        let header: &[u8; HEADER_SIZE] = payload
            .get(cursor..header_end)
            .ok_or_else(|| invalid_data("incomplete nested header"))?
            .try_into()
            .expect("fixed header");
        let parsed = Header::parse(header)?;
        if parsed.operation == Operation::Batch {
            return Err(invalid_data("nested batches are not supported"));
        }
        let value_offset = header_end
            .checked_add(parsed.key_len)
            .ok_or_else(|| invalid_data("batch offset overflow"))?;
        let data_end = value_offset
            .checked_add(parsed.value_len)
            .ok_or_else(|| invalid_data("batch offset overflow"))?;
        let end = data_end
            .checked_add(CHECKSUM_SIZE)
            .ok_or_else(|| invalid_data("batch offset overflow"))?;
        let bytes = payload
            .get(header_end..data_end)
            .ok_or_else(|| invalid_data("incomplete nested payload"))?;
        let checksum: [u8; 4] = payload
            .get(data_end..end)
            .ok_or_else(|| invalid_data("incomplete nested checksum"))?
            .try_into()
            .expect("four bytes");
        verify(header, bytes, u32::from_le_bytes(checksum))?;
        result.push(Decoded {
            operation: parsed.operation,
            key: &payload[header_end..value_offset],
            value_offset,
            value_len: parsed.value_len,
            checksum: crc32fast::hash(&payload[value_offset..data_end]),
        });
        cursor = end;
    }
    if cursor != payload.len() {
        return Err(invalid_data("trailing bytes in atomic batch"));
    }
    Ok(result)
}
