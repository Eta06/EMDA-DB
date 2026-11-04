//! Versioned, length-delimited keys reserved for the document layer.
use std::io;

pub(super) const PREFIX: &[u8] = b"\0emda-document\x01";
const MAX_NAME_BYTES: usize = 128;
const MAX_ID_BYTES: usize = 256;

pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(super) fn name(name: &str) -> io::Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return Err(invalid("name must contain between 1 and 128 UTF-8 bytes"));
    }
    Ok(())
}

pub(super) fn id(id: &str) -> io::Result<()> {
    if id.is_empty() || id.len() > MAX_ID_BYTES {
        return Err(invalid(
            "document ID must contain between 1 and 256 UTF-8 bytes",
        ));
    }
    Ok(())
}

fn component(out: &mut Vec<u8>, value: &str) -> io::Result<()> {
    name(value)?;
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

pub(super) fn collection(kind: u8, name: &str) -> io::Result<Vec<u8>> {
    let mut result = PREFIX.to_vec();
    result.push(kind);
    component(&mut result, name)?;
    Ok(result)
}

pub(super) fn document(collection_name: &str, id_value: &str) -> io::Result<Vec<u8>> {
    id(id_value)?;
    let mut result = collection(b'D', collection_name)?;
    result.extend_from_slice(id_value.as_bytes());
    Ok(result)
}

pub(super) fn index(kind: u8, collection_name: &str, index_name: &str) -> io::Result<Vec<u8>> {
    let mut result = collection(kind, collection_name)?;
    component(&mut result, index_name)?;
    Ok(result)
}

/// The smallest byte string greater than every string with this prefix.
pub(super) fn end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != u8::MAX {
            end.push(last + 1);
            return end;
        }
    }
    unreachable!("reserved prefixes contain non-0xff bytes")
}

pub(super) fn raw_key(key: &[u8]) -> io::Result<()> {
    if key.starts_with(PREFIX) {
        return Err(invalid("key is reserved for the document layer"));
    }
    crate::record::validate_key(key)
}
