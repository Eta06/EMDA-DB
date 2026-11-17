//! Isolated, non-authoritative validation of the proposed EMDARB02 format.
//!
//! This validates bytes, not their source, historical peak provenance, or any
//! witness. It does not stage entries or provide recovery. The caller must keep
//! the input immutable during validation and any subsequent use. Limits bound
//! admitted bytes and work, not I/O latency. Validation allocates no memory
//! from input; lookup allocates at most the explicit returned-value budget.
use sha2::{Digest, Sha256};
use std::io::{self, Read};

/// Explicit finite resource ceilings. `work_bytes` includes one EOF probe byte.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub file_bytes: u64,
    pub entries: u64,
    pub key_bytes: u64,
    pub work_bytes: u64,
}

/// Self-consistent metadata only; none of these fields establishes provenance.
#[derive(Debug, PartialEq, Eq)]
pub struct Validated {
    pub source_id: [u8; 16],
    pub base_id: [u8; 16],
    pub boundary: u64,
    pub entries: u64,
    pub key_bytes: u64,
    pub peak_entries: u64,
    pub peak_key_bytes: u64,
    pub file_bytes: u64,
    pub digest: [u8; 32],
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn number(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().unwrap())
}
fn add(a: u64, b: u64) -> io::Result<u64> {
    a.checked_add(b).ok_or_else(|| invalid("length overflow"))
}

/// Validate one complete stream. Ordinary short reads are retried; an error,
/// truncation, trailing data or limit violation never yields metadata.
pub fn validate<R: Read>(reader: R, limits: Limits) -> io::Result<Validated> {
    scan(reader, limits, None).map(|(metadata, _)| metadata)
}

/// Independently supplied binding for a lookup. The digest must come from a
/// trusted caller; the file's own trailer is not a witness or authority.
#[derive(Clone, Copy, Debug)]
pub struct Expected {
    pub digest: [u8; 32],
    pub source_id: [u8; 16],
    pub base_id: [u8; 16],
    pub boundary: u64,
}

/// Verify the complete stream and return the selected value, if present.
/// The value is captured from the same reads used to hash it. This operation
/// provides no recovery, deletion, or live switch. The caller must obtain the
/// expected binding independently and decide whether it is trustworthy.
/// A value-budget rejection can occur before validation completes and does not
/// establish that the input is authentic or that the key is present.
pub fn lookup<R: Read>(
    reader: R,
    limits: Limits,
    key: &[u8],
    max_value_bytes: usize,
    expected: Expected,
) -> io::Result<Option<Vec<u8>>> {
    crate::record::validate_key(key)?;
    if max_value_bytes > 16 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "value budget exceeds format maximum",
        ));
    }
    let (metadata, value) = scan(reader, limits, Some((key, max_value_bytes)))?;
    if metadata.digest != expected.digest
        || metadata.source_id != expected.source_id
        || metadata.base_id != expected.base_id
        || metadata.boundary != expected.boundary
    {
        return Err(invalid("expected binding mismatch"));
    }
    Ok(value)
}

fn scan<R: Read>(
    mut reader: R,
    limits: Limits,
    selection: Option<(&[u8], usize)>,
) -> io::Result<(Validated, Option<Vec<u8>>)> {
    let mut header = [0u8; 112];
    // Require a budget for the header before performing any reads.
    if limits.file_bytes < 144 || limits.work_bytes < 145 {
        return Err(invalid("budget exceeded"));
    }
    reader.read_exact(&mut header)?;
    if &header[..8] != b"EMDARB02"
        || u32::from_le_bytes(header[8..12].try_into().unwrap()) != 2
        || u32::from_le_bytes(header[12..16].try_into().unwrap()) != 112
        || header[96..112].iter().any(|&b| b != 0)
    {
        return Err(invalid("invalid header"));
    }
    let source_id: [u8; 16] = header[16..32].try_into().unwrap();
    let base_id: [u8; 16] = header[32..48].try_into().unwrap();
    if source_id == [0; 16] || base_id == [0; 16] {
        return Err(invalid("zero identifier"));
    }
    let boundary = number(&header[48..56]);
    let entries = number(&header[56..64]);
    let key_bytes = number(&header[64..72]);
    let peak_entries = number(&header[72..80]);
    let peak_key_bytes = number(&header[80..88]);
    let encoded = number(&header[88..96]);
    let file_bytes = add(add(112, encoded)?, 32)?;
    if file_bytes > limits.file_bytes
        || add(file_bytes, 1)? > limits.work_bytes
        || entries > limits.entries
        || key_bytes > limits.key_bytes
        || peak_entries < entries
        || peak_key_bytes < key_bytes
        || entries.checked_mul(9).is_none_or(|min| min > encoded)
    {
        return Err(invalid("invalid totals or budget exceeded"));
    }
    let mut hash = Sha256::new();
    hash.update(b"EMDA retained base v2\0");
    hash.update(header);
    let mut previous = [0u8; 1024];
    let mut previous_len = 0usize;
    let mut key = [0u8; 1024];
    let mut buffer = [0u8; 65536];
    let mut captured = None;
    let mut observed = 0u64;
    let mut keys = 0u64;
    for _ in 0..entries {
        if add(observed, 8)? > encoded {
            return Err(invalid("entry exceeds declared bytes"));
        }
        let mut lengths = [0u8; 8];
        reader.read_exact(&mut lengths)?;
        hash.update(lengths);
        let kl = u32::from_le_bytes(lengths[..4].try_into().unwrap()) as usize;
        let vl = u32::from_le_bytes(lengths[4..].try_into().unwrap()) as usize;
        if kl == 0 || kl > 1024 || vl > 16 * 1024 * 1024 {
            return Err(invalid("invalid entry length"));
        }
        observed = add(add(add(observed, 8)?, kl as u64)?, vl as u64)?;
        keys = add(keys, kl as u64)?;
        if observed > encoded || keys > key_bytes {
            return Err(invalid("entry exceeds totals"));
        }
        reader.read_exact(&mut key[..kl])?;
        hash.update(&key[..kl]);
        if previous_len != 0 && previous[..previous_len] >= key[..kl] {
            return Err(invalid("keys not strictly ordered"));
        }
        previous[..kl].copy_from_slice(&key[..kl]);
        previous_len = kl;
        let selected = selection.is_some_and(|(wanted, _)| wanted == &key[..kl]);
        if selected {
            if vl > selection.unwrap().1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "value budget exceeded",
                ));
            }
            let mut value = Vec::new();
            value
                .try_reserve_exact(vl)
                .map_err(|_| io::Error::other("value allocation failed"))?;
            captured = Some(value);
        }
        let mut remaining = vl;
        while remaining != 0 {
            let n = remaining.min(buffer.len());
            reader.read_exact(&mut buffer[..n])?;
            hash.update(&buffer[..n]);
            if selected {
                captured.as_mut().unwrap().extend_from_slice(&buffer[..n]);
            }
            remaining -= n;
        }
    }
    if observed != encoded || keys != key_bytes {
        return Err(invalid("totals mismatch"));
    }
    let mut trailer = [0u8; 32];
    reader.read_exact(&mut trailer)?;
    let digest: [u8; 32] = hash.finalize().into();
    if trailer != digest {
        return Err(invalid("digest mismatch"));
    }
    let mut extra = [0u8; 1];
    loop {
        match reader.read(&mut extra) {
            Ok(0) => break,
            Ok(_) => return Err(invalid("trailing bytes")),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok((
        Validated {
            source_id,
            base_id,
            boundary,
            entries,
            key_bytes,
            peak_entries,
            peak_key_bytes,
            file_bytes,
            digest,
        },
        captured,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(value_len: usize) -> Vec<u8> {
        let mut h = [0u8; 112];
        h[..8].copy_from_slice(b"EMDARB02");
        h[8..12].copy_from_slice(&2u32.to_le_bytes());
        h[12..16].copy_from_slice(&112u32.to_le_bytes());
        h[16] = 1;
        h[32] = 2;
        for (offset, n) in [
            (56, 1u64),
            (64, 1),
            (72, 1),
            (80, 1),
            (88, 9 + value_len as u64),
        ] {
            h[offset..offset + 8].copy_from_slice(&n.to_le_bytes());
        }
        let mut data = h.to_vec();
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&(value_len as u32).to_le_bytes());
        data.push(b'a');
        data.resize(data.len() + value_len, 42);
        let mut hash = Sha256::new();
        hash.update(b"EMDA retained base v2\0");
        hash.update(&data);
        data.extend_from_slice(&hash.finalize());
        data
    }
    fn limits() -> Limits {
        Limits {
            file_bytes: 20_000_000,
            entries: 10,
            key_bytes: 1024,
            work_bytes: 20_000_001,
        }
    }
    struct Short<'a>(&'a [u8]);
    impl Read for Short<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = buf.len().min(self.0.len()).min(7);
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }
    #[test]
    fn maximum_value_and_short_reads() {
        let bytes = fixture(16 * 1024 * 1024);
        let result = validate(Short(&bytes), limits()).unwrap();
        assert_eq!(result.file_bytes, bytes.len() as u64);
    }
    #[test]
    fn truncation_corruption_and_extra_bytes_fail() {
        let bytes = fixture(3);
        for end in 0..bytes.len() {
            assert!(validate(&bytes[..end], limits()).is_err(), "{end}");
        }
        let mut corrupt = bytes.clone();
        corrupt[121] ^= 1;
        assert!(validate(&corrupt[..], limits()).is_err());
        let mut extra = bytes;
        extra.push(0);
        assert!(validate(&extra[..], limits()).is_err());
    }
    #[test]
    fn each_limit_is_enforced() {
        let bytes = fixture(0);
        for l in [
            Limits {
                file_bytes: bytes.len() as u64 - 1,
                ..limits()
            },
            Limits {
                entries: 0,
                ..limits()
            },
            Limits {
                key_bytes: 0,
                ..limits()
            },
            Limits {
                work_bytes: bytes.len() as u64,
                ..limits()
            },
        ] {
            assert!(validate(&bytes[..], l).is_err());
        }
        assert!(validate(&bytes[..], limits()).is_ok());
    }
    fn rehash(bytes: &mut [u8]) {
        let end = bytes.len() - 32;
        let mut h = Sha256::new();
        h.update(b"EMDA retained base v2\0");
        h.update(&bytes[..end]);
        bytes[end..].copy_from_slice(&h.finalize());
    }
    fn set64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }
    fn entries(keys: &[&[u8]]) -> Vec<u8> {
        let mut b = fixture(0);
        b.truncate(112);
        let mut total = 0;
        for key in keys {
            b.extend_from_slice(&(key.len() as u32).to_le_bytes());
            b.extend_from_slice(&0u32.to_le_bytes());
            b.extend_from_slice(key);
            total += key.len() as u64;
        }
        let encoded = b.len() as u64 - 112;
        set64(&mut b, 56, keys.len() as u64);
        set64(&mut b, 64, total);
        set64(&mut b, 72, keys.len() as u64);
        set64(&mut b, 80, total);
        set64(&mut b, 88, encoded);
        b.extend_from_slice(&[0; 32]);
        rehash(&mut b);
        b
    }
    #[test]
    fn independent_known_answer() {
        // Python hashlib.sha256 over the domain separator and serialized fixture.
        let expected = [
            0x64, 0xb1, 0x8b, 0xdb, 0xda, 0x8f, 0x84, 0xdf, 0x1a, 0x61, 0xde, 0xde, 0x13, 0x27,
            0xa1, 0xad, 0xdc, 0x17, 0x97, 0x78, 0x29, 0x2c, 0x0c, 0xb9, 0x91, 0x95, 0x06, 0x5b,
            0x0e, 0x72, 0x2e, 0x2e,
        ];
        assert_eq!(
            validate(&fixture(3)[..], limits()).unwrap().digest,
            expected
        );
    }
    #[test]
    fn rehashed_invalid_headers_and_totals() {
        let original = fixture(0);
        for (offset, value) in [
            (8, 3),
            (12, 111),
            (56, 2),
            (64, 2),
            (72, 0),
            (80, 0),
            (88, 8),
            (88, 10),
            (88, u64::MAX),
            (56, u64::MAX),
        ] {
            let mut b = original.clone();
            if offset == 8 || offset == 12 {
                b[offset..offset + 4].copy_from_slice(&(value as u32).to_le_bytes());
            } else {
                set64(&mut b, offset, value);
            }
            rehash(&mut b);
            assert!(
                validate(&b[..], limits()).is_err(),
                "offset {offset} value {value}"
            );
        }
        for offset in [0, 16, 32, 96] {
            let mut b = original.clone();
            if offset == 16 || offset == 32 {
                b[offset] = 0;
            } else {
                b[offset] ^= 1;
            }
            rehash(&mut b);
            assert!(validate(&b[..], limits()).is_err(), "offset {offset}");
        }
    }
    #[test]
    fn ordering_and_key_edges() {
        for keys in [
            vec![&b"b"[..], &b"a"[..]],
            vec![&b"a"[..], &b"a"[..]],
            vec![&b"ab"[..], &b"a"[..]],
            vec![&b""[..]],
        ] {
            assert!(validate(&entries(&keys)[..], limits()).is_err());
        }
        assert!(validate(&entries(&[])[..], limits()).is_ok());
        assert!(validate(&entries(&[b"a", b"ab", b"b"])[..], limits()).is_ok());
        let max = vec![1; 1024];
        assert!(validate(&entries(&[&max])[..], limits()).is_ok());
        let too_long = vec![1; 1025];
        assert!(
            validate(
                &entries(&[&too_long])[..],
                Limits {
                    key_bytes: 1025,
                    ..limits()
                }
            )
            .is_err()
        );
        let mut b = fixture(0);
        b[116..120].copy_from_slice(&(16u32 * 1024 * 1024 + 1).to_le_bytes());
        rehash(&mut b);
        assert!(validate(&b[..], limits()).is_err());
    }
    #[test]
    fn exact_budgets() {
        let b = fixture(0);
        let exact = Limits {
            file_bytes: b.len() as u64,
            work_bytes: b.len() as u64 + 1,
            entries: 1,
            key_bytes: 1,
        };
        assert!(validate(&b[..], exact).is_ok());
        for l in [
            Limits {
                file_bytes: exact.file_bytes - 1,
                ..exact
            },
            Limits {
                work_bytes: exact.work_bytes - 1,
                ..exact
            },
            Limits {
                entries: 0,
                ..exact
            },
            Limits {
                key_bytes: 0,
                ..exact
            },
        ] {
            assert!(validate(&b[..], l).is_err());
        }
    }
    struct Fault<'a> {
        bytes: &'a [u8],
        at: usize,
        position: usize,
        interrupt: bool,
    }
    impl Read for Fault<'_> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if self.interrupt {
                self.interrupt = false;
                return Err(io::ErrorKind::Interrupted.into());
            }
            if self.position >= self.at {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            let n = out
                .len()
                .min(self.bytes.len())
                .min(self.at - self.position)
                .min(5);
            out[..n].copy_from_slice(&self.bytes[..n]);
            self.bytes = &self.bytes[n..];
            self.position += n;
            Ok(n)
        }
    }
    #[test]
    fn io_errors_and_interruptions() {
        let b = fixture(3);
        for at in [0, 50, 112, 120, 121, 124, b.len() - 1, b.len()] {
            let err = validate(
                Fault {
                    bytes: &b,
                    at,
                    position: 0,
                    interrupt: true,
                },
                limits(),
            )
            .unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied, "at {at}");
        }
        assert!(
            validate(
                Fault {
                    bytes: &b,
                    at: usize::MAX,
                    position: 0,
                    interrupt: true
                },
                limits()
            )
            .is_ok()
        );
    }
}
