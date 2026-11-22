//! Isolated, non-authoritative validation of the proposed EMDARB02 format.
//!
//! This validates bytes, not their source, historical peak provenance, or any
//! witness. It does not provide recovery. The caller must keep
//! the input immutable during validation and any subsequent use. Limits bound
//! admitted bytes and work, not I/O latency. Validation allocates no memory
//! from input; lookup allocates at most the explicit returned-value budget. The
//! candidate loader stages privately under separate explicit limits.
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read},
    os::unix::fs::FileExt,
};

pub(crate) mod index;

const HEADER_BYTES: u64 = 112;
pub(crate) const FRAME_BYTES: u64 = HEADER_BYTES + 32;
const MAX_KEY_BYTES: usize = 1024;
const CHUNK_BYTES: usize = 64 * 1024;

// The key and value lengths are bounded by the format.
pub(crate) fn encoded_entry_bytes(key: &[u8], value_len: usize) -> u64 {
    8 + key.len() as u64 + value_len as u64
}

pub(crate) fn encode_header(
    source_id: [u8; 16],
    base_id: [u8; 16],
    fields: [u64; 6],
) -> [u8; HEADER_BYTES as usize] {
    let mut header = [0; HEADER_BYTES as usize];
    header[..8].copy_from_slice(b"EMDARB02");
    header[8..12].copy_from_slice(&2_u32.to_le_bytes());
    header[12..16].copy_from_slice(&(HEADER_BYTES as u32).to_le_bytes());
    header[16..32].copy_from_slice(&source_id);
    header[32..48].copy_from_slice(&base_id);
    for (slot, value) in header[48..96].chunks_exact_mut(8).zip(fields) {
        slot.copy_from_slice(&value.to_le_bytes());
    }
    header
}

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
    scan(reader, limits, None, None).map(|(metadata, _)| metadata)
}

pub(crate) fn validate_buffered<R: Read>(reader: R, limits: Limits) -> io::Result<Validated> {
    scan_buffered(reader, limits, None)
}

// Buffer only the admitted body and its EOF probe. Generic stream callers
// retain the existing reads and error ordering.
fn scan_buffered<R: Read>(
    reader: R,
    limits: Limits,
    index: Option<&mut index::Builder>,
) -> io::Result<Validated> {
    scan_buffered_using(
        reader,
        limits,
        index.map_or(ScanIndex::None, ScanIndex::Blocks),
    )
}

pub(crate) struct CapturedValue {
    pub offset: u64,
    pub length: usize,
    pub digest: [u8; 32],
}

type ValueVisitor<'a> = dyn FnMut(&[u8], CapturedValue) -> io::Result<()> + 'a;

enum ScanIndex<'a> {
    None,
    Blocks(&'a mut index::Builder),
    Locations {
        staging: StagingLimits,
        visit: &'a mut ValueVisitor<'a>,
    },
}

// Locations refer to the bytes hashed by this scan. Keep them private until
// the whole file and its independent binding have been verified.
pub(crate) fn scan_value_locations(
    reader: impl Read,
    limits: Limits,
    staging: StagingLimits,
    mut visit: impl FnMut(&[u8], CapturedValue) -> io::Result<()>,
) -> io::Result<Validated> {
    scan_buffered_using(
        reader,
        limits,
        ScanIndex::Locations {
            staging,
            visit: &mut visit,
        },
    )
}

fn scan_buffered_using(
    reader: impl Read,
    limits: Limits,
    index: ScanIndex<'_>,
) -> io::Result<Validated> {
    scan_using(
        reader,
        limits,
        None,
        None,
        index,
        |reader, bytes| {
            BufReader::with_capacity(bytes.min(CHUNK_BYTES as u64) as usize, reader.take(bytes))
        },
        visit_buffered_value,
    )
    .map(|(metadata, _)| metadata)
}

fn visit_buffered_value(
    reader: &mut impl BufRead,
    mut remaining: usize,
    visit: &mut dyn FnMut(&[u8]),
) -> io::Result<()> {
    while remaining != 0 {
        let bytes = match reader.fill_buf() {
            Ok([]) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        let count = remaining.min(bytes.len());
        visit(&bytes[..count]);
        reader.consume(count);
        remaining -= count;
    }
    Ok(())
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
    lookup_with_metadata(reader, limits, key, max_value_bytes, expected).map(|(_, value)| value)
}

pub(crate) fn lookup_with_metadata<R: Read>(
    reader: R,
    limits: Limits,
    key: &[u8],
    max_value_bytes: usize,
    expected: Expected,
) -> io::Result<(Validated, Option<Vec<u8>>)> {
    crate::record::validate_key(key)?;
    validate_value_budget(max_value_bytes)?;
    let (metadata, value) = scan(reader, limits, Some((key, max_value_bytes)), None)?;
    if metadata.digest != expected.digest
        || metadata.source_id != expected.source_id
        || metadata.base_id != expected.base_id
        || metadata.boundary != expected.boundary
    {
        return Err(invalid("expected binding mismatch"));
    }
    Ok((metadata, value))
}

pub(crate) fn validate_value_budget(max_value_bytes: usize) -> io::Result<()> {
    if max_value_bytes > crate::record::MAX_VALUE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "value budget exceeds format maximum",
        ));
    }
    Ok(())
}

fn entry_lengths(bytes: [u8; 8]) -> io::Result<(usize, usize)> {
    let key = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let value = u32::from_le_bytes(bytes[4..].try_into().unwrap()) as usize;
    if key == 0 || key > MAX_KEY_BYTES || value > crate::record::MAX_VALUE {
        return Err(invalid("invalid entry length"));
    }
    Ok((key, value))
}

/// Additional ceilings for private in-memory staging. Payload counts key and value bytes;
/// entry vector overhead is separately bounded by `Limits::entries`.
#[derive(Clone, Copy, Debug)]
pub struct StagingLimits {
    pub value_bytes: usize,
    pub staging_bytes: u64,
}

type StagedEntries = Vec<(Vec<u8>, Vec<u8>)>;

pub(crate) fn load_with_metadata<R: Read>(
    reader: R,
    limits: Limits,
    staging_limits: StagingLimits,
) -> io::Result<(Validated, StagedEntries)> {
    let mut staged = Vec::new();
    let (metadata, _) = scan(reader, limits, None, Some((&mut staged, staging_limits)))?;
    Ok((metadata, staged))
}

fn scan<R: Read>(
    reader: R,
    limits: Limits,
    selection: Option<(&[u8], usize)>,
    staging: Option<(&mut StagedEntries, StagingLimits)>,
) -> io::Result<(Validated, Option<Vec<u8>>)> {
    let mut buffer = [0u8; CHUNK_BYTES];
    scan_using(
        reader,
        limits,
        selection,
        staging,
        ScanIndex::None,
        |reader, _| reader,
        |reader, mut remaining, visit| {
            while remaining != 0 {
                let count = remaining.min(buffer.len());
                let bytes = &mut buffer[..count];
                reader.read_exact(bytes)?;
                visit(bytes);
                remaining -= count;
            }
            Ok(())
        },
    )
}

pub(crate) struct BaseHeader {
    source_id: [u8; 16],
    base_id: [u8; 16],
    boundary: u64,
    pub(crate) entries: u64,
    key_bytes: u64,
    peak_entries: u64,
    peak_key_bytes: u64,
    encoded: u64,
    file_bytes: u64,
}

impl BaseHeader {
    pub(crate) fn payload_bytes(&self) -> u64 {
        self.encoded - 8 * self.entries
    }
}

// Cheap rejection only. Callers must authenticate the full entry bytes
// against the independent digest before using the base.
pub(crate) fn preflight_file(
    file: &File,
    length: u64,
    limits: Limits,
    expected: Expected,
) -> io::Result<BaseHeader> {
    if length < FRAME_BYTES {
        return Err(invalid("incomplete retained base frame"));
    }
    let mut header = [0; HEADER_BYTES as usize];
    file.read_exact_at(&mut header, 0)?;
    let header = parse_header(&header, limits)?;
    if header.file_bytes != length
        || header.source_id != expected.source_id
        || header.base_id != expected.base_id
        || header.boundary != expected.boundary
    {
        return Err(invalid("retained base header does not match its binding"));
    }
    let mut trailer = [0; 32];
    file.read_exact_at(&mut trailer, length - 32)?;
    if trailer != expected.digest {
        return Err(invalid("retained base trailer does not match its binding"));
    }
    Ok(header)
}

fn parse_header(header: &[u8; HEADER_BYTES as usize], limits: Limits) -> io::Result<BaseHeader> {
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
    if source_id == base_id {
        return Err(invalid("base identifier equals source identifier"));
    }
    let boundary = number(&header[48..56]);
    let entries = number(&header[56..64]);
    let key_bytes = number(&header[64..72]);
    let peak_entries = number(&header[72..80]);
    let peak_key_bytes = number(&header[80..88]);
    let encoded = number(&header[88..96]);
    let file_bytes = add(add(HEADER_BYTES, encoded)?, 32)?;
    let framing_bytes = add(
        entries
            .checked_mul(8)
            .ok_or_else(|| invalid("invalid totals or budget exceeded"))?,
        key_bytes,
    )?;
    if file_bytes > limits.file_bytes
        || add(file_bytes, 1)? > limits.work_bytes
        || entries > limits.entries
        || key_bytes > limits.key_bytes
        || key_bytes < entries
        || key_bytes > entries.saturating_mul(MAX_KEY_BYTES as u64)
        || peak_entries < entries
        || peak_key_bytes < key_bytes
        || encoded
            .checked_sub(framing_bytes)
            .is_none_or(|value_bytes| {
                value_bytes > entries.saturating_mul(crate::record::MAX_VALUE as u64)
            })
    {
        return Err(invalid("invalid totals or budget exceeded"));
    }
    Ok(BaseHeader {
        source_id,
        base_id,
        boundary,
        entries,
        key_bytes,
        peak_entries,
        peak_key_bytes,
        encoded,
        file_bytes,
    })
}

fn scan_using<R: Read, B: Read>(
    mut reader: R,
    limits: Limits,
    selection: Option<(&[u8], usize)>,
    mut staging: Option<(&mut StagedEntries, StagingLimits)>,
    mut index: ScanIndex<'_>,
    prepare_body: impl FnOnce(R, u64) -> B,
    mut visit_value: impl FnMut(&mut B, usize, &mut dyn FnMut(&[u8])) -> io::Result<()>,
) -> io::Result<(Validated, Option<Vec<u8>>)> {
    let mut header = [0u8; HEADER_BYTES as usize];
    // Require a budget for the header before performing any reads.
    if limits.file_bytes < 144 || limits.work_bytes < 145 {
        return Err(invalid("budget exceeded"));
    }
    reader.read_exact(&mut header)?;
    let BaseHeader {
        source_id,
        base_id,
        boundary,
        entries,
        key_bytes,
        peak_entries,
        peak_key_bytes,
        encoded,
        file_bytes,
    } = parse_header(&header, limits)?;
    let mut reader = prepare_body(reader, file_bytes - HEADER_BYTES + 1);
    let mut hash = Sha256::new();
    hash.update(b"EMDA retained base v2\0");
    hash.update(header);
    let mut previous = [0u8; MAX_KEY_BYTES];
    let mut previous_len = 0usize;
    let mut key = [0u8; MAX_KEY_BYTES];
    let mut captured = None;
    let mut observed = 0u64;
    let mut keys = 0u64;
    let mut staged_bytes = 0u64;
    let staging_budget = match &index {
        ScanIndex::Locations { staging, .. } => Some(*staging),
        _ => staging.as_ref().map(|(_, limits)| *limits),
    };
    for _ in 0..entries {
        let start = observed;
        if add(observed, 8)? > encoded {
            return Err(invalid("entry exceeds declared bytes"));
        }
        let mut lengths = [0u8; 8];
        reader.read_exact(&mut lengths)?;
        hash.update(lengths);
        let (kl, vl) = entry_lengths(lengths)?;
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
        if let ScanIndex::Blocks(builder) = &mut index {
            builder.entry(
                add(HEADER_BYTES, start)?,
                observed - start,
                &key[..kl],
                &lengths,
            )?;
        }
        if let Some(budget) = staging_budget {
            staged_bytes = add(add(staged_bytes, kl as u64)?, vl as u64)?;
            if vl > budget.value_bytes || staged_bytes > budget.staging_bytes {
                return Err(invalid("staging budget exceeded"));
            }
        }
        let mut staged_entry = if staging.is_some() {
            let mut staged_key = Vec::new();
            staged_key
                .try_reserve_exact(kl)
                .map_err(|_| io::Error::other("staging allocation failed"))?;
            staged_key.extend_from_slice(&key[..kl]);
            let mut value = Vec::new();
            value
                .try_reserve_exact(vl)
                .map_err(|_| io::Error::other("staging allocation failed"))?;
            Some((staged_key, value))
        } else {
            None
        };
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
        let mut value_hash = matches!(index, ScanIndex::Locations { .. }).then(Sha256::new);
        visit_value(&mut reader, vl, &mut |bytes| {
            hash.update(bytes);
            if let ScanIndex::Blocks(builder) = &mut index {
                builder.value_chunk(bytes);
            }
            if let Some(value_hash) = &mut value_hash {
                value_hash.update(bytes);
            }
            if selected {
                captured.as_mut().unwrap().extend_from_slice(bytes);
            }
            if let Some((_, value)) = staged_entry.as_mut() {
                value.extend_from_slice(bytes);
            }
        })?;
        if let ScanIndex::Locations { visit, .. } = &mut index {
            visit(
                &key[..kl],
                CapturedValue {
                    offset: HEADER_BYTES + observed - vl as u64,
                    length: vl,
                    digest: value_hash.unwrap().finalize().into(),
                },
            )?;
        }
        if let Some(entry) = staged_entry {
            let entries = &mut staging.as_mut().unwrap().0;
            entries
                .try_reserve(1)
                .map_err(|_| io::Error::other("staging allocation failed"))?;
            entries.push(entry);
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
    fn staging_reads_once_and_unwinds_on_reader_panic() {
        struct Reader<'a> {
            bytes: &'a [u8],
            read: usize,
            panic_at: Option<usize>,
        }
        impl Read for Reader<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                if self.panic_at.is_some_and(|at| self.read >= at) {
                    panic!("injected panic");
                }
                let n = out.len().min(self.bytes.len()).min(7);
                out[..n].copy_from_slice(&self.bytes[..n]);
                self.bytes = &self.bytes[n..];
                self.read += n;
                Ok(n)
            }
        }
        let bytes = fixture(100);
        let stage = StagingLimits {
            value_bytes: 100,
            staging_bytes: 101,
        };
        let mut reader = Reader {
            bytes: &bytes,
            read: 0,
            panic_at: None,
        };
        let (_, entries) = load_with_metadata(&mut reader, limits(), stage).unwrap();
        assert_eq!(reader.read, bytes.len());
        assert_eq!(entries, vec![(b"a".to_vec(), vec![42; 100])]);
        let result = std::panic::catch_unwind(|| {
            let reader = Reader {
                bytes: &bytes,
                read: 0,
                panic_at: Some(130),
            };
            let _ = load_with_metadata(reader, limits(), stage);
        });
        assert!(result.is_err());
        assert!(load_with_metadata(Short(&bytes[..bytes.len() - 1]), limits(), stage).is_err());
    }
    #[test]
    fn maximum_value_and_short_reads() {
        let bytes = fixture(16 * 1024 * 1024);
        let result = validate(Short(&bytes), limits()).unwrap();
        assert_eq!(result.file_bytes, bytes.len() as u64);
    }

    #[test]
    fn indexed_base_preflight_admits_only_feasible_block_and_anchor_limits() {
        use index::{IndexLimits, IndexedBase};
        use std::{fs, io::Write};

        let entries = 128;
        let key_bytes = entries * MAX_KEY_BYTES as u64;
        let encoded = entries * 8 + key_bytes;
        let header = encode_header(
            [1; 16],
            [2; 16],
            [0, entries, key_bytes, entries, key_bytes, encoded],
        );
        let mut bytes = header.to_vec();
        for number in 0..entries {
            bytes.extend_from_slice(&(MAX_KEY_BYTES as u32).to_le_bytes());
            bytes.extend_from_slice(&0_u32.to_le_bytes());
            bytes.push(number as u8);
            bytes.resize(bytes.len() + MAX_KEY_BYTES - 1, 0);
        }
        let mut hash = Sha256::new();
        hash.update(b"EMDA retained base v2\0");
        hash.update(&bytes);
        let digest: [u8; 32] = hash.finalize().into();
        bytes.extend_from_slice(&digest);
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&bytes).unwrap();
        let limits = Limits {
            entries,
            key_bytes,
            ..limits()
        };
        let expected = Expected {
            source_id: [1; 16],
            base_id: [2; 16],
            boundary: 0,
            digest,
        };
        let preflight = |index_limits| {
            IndexedBase::preflight(
                file.as_file(),
                bytes.len() as u64,
                limits,
                index_limits,
                expected,
            )
        };
        let exact = IndexLimits {
            blocks: 2,
            anchor_bytes: MAX_KEY_BYTES * 2,
        };
        preflight(exact).unwrap();
        let base = IndexedBase::open(fs::File::open(file.path()).unwrap(), limits, exact).unwrap();
        assert_eq!(base.metadata().digest, digest);
        assert_eq!(
            preflight(IndexLimits { blocks: 1, ..exact })
                .unwrap_err()
                .to_string(),
            "retained base block limit exceeded"
        );
        assert_eq!(
            preflight(IndexLimits {
                anchor_bytes: 1,
                ..exact
            })
            .unwrap_err()
            .to_string(),
            "retained base anchor byte limit exceeded"
        );
    }

    #[test]
    fn buffered_validation_admits_the_header_before_prefetch_and_bounds_the_eof_probe() {
        let bytes = fixture(CHUNK_BYTES + 3);
        let exact = Limits {
            file_bytes: bytes.len() as u64,
            work_bytes: bytes.len() as u64 + 1,
            ..limits()
        };
        assert_eq!(
            validate_buffered(Short(&bytes), exact).unwrap(),
            validate(&bytes[..], exact).unwrap()
        );
        let mut rejected = &bytes[..];
        let short_budget = Limits {
            work_bytes: exact.work_bytes - 1,
            ..exact
        };
        assert!(validate_buffered(&mut rejected, short_budget).is_err());
        assert_eq!(rejected, &bytes[HEADER_BYTES as usize..]);

        let mut extended = bytes.clone();
        extended.extend([0; 100]);
        let mut remaining = extended.as_slice();
        assert_eq!(
            validate_buffered(&mut remaining, exact)
                .unwrap_err()
                .to_string(),
            "trailing bytes",
        );
        assert_eq!(remaining, &extended[bytes.len() + 1..]);
    }
    #[test]
    fn buffered_value_validation_retries_body_interrupts_and_rejects_truncation() {
        struct Interrupted<'a> {
            short: Short<'a>,
            interrupt: bool,
        }
        impl Read for Interrupted<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                self.interrupt = !self.interrupt;
                if self.interrupt {
                    return Err(io::ErrorKind::Interrupted.into());
                }
                self.short.read(out)
            }
        }
        fn reader(input: &[u8]) -> Interrupted<'_> {
            Interrupted {
                short: Short(input),
                interrupt: false,
            }
        }
        let bytes = fixture(CHUNK_BYTES + 3);
        let metadata = validate_buffered(reader(&bytes), limits()).unwrap();
        assert_eq!(metadata, validate(bytes.as_slice(), limits()).unwrap());
        let truncated = &bytes[..bytes.len() - 37];
        assert_eq!(
            validate_buffered(reader(truncated), limits())
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
        let mut corrupted = bytes;
        let last_value_byte = corrupted.len() - 33;
        corrupted[last_value_byte] ^= 1;
        assert_eq!(
            validate_buffered(reader(&corrupted), limits())
                .unwrap_err()
                .to_string(),
            "digest mismatch"
        );
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
    #[test]
    fn reused_source_identity_is_rejected_even_with_matching_digest_and_binding() {
        let mut bytes = fixture(3);
        let source: [u8; 16] = bytes[16..32].try_into().unwrap();
        bytes[32..48].copy_from_slice(&source);
        rehash(&mut bytes);
        let digest = bytes[bytes.len() - 32..].try_into().unwrap();
        let error = validate(&bytes[..], limits()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("base identifier equals source identifier")
        );
        let expected = Expected {
            digest,
            source_id: source,
            base_id: source,
            boundary: 0,
        };
        let error = lookup(&bytes[..], limits(), b"a", 3, expected).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("base identifier equals source identifier")
        );
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
    fn staged_load_rejects_rehashed_bad_order_and_totals() {
        let stage = StagingLimits {
            value_bytes: 10,
            staging_bytes: 100,
        };
        let unordered = entries(&[b"b", b"a"]);
        assert!(load_with_metadata(&unordered[..], limits(), stage).is_err());
        let mut totals = entries(&[b"a", b"b"]);
        set64(&mut totals, 64, 3);
        rehash(&mut totals);
        assert!(load_with_metadata(&totals[..], limits(), stage).is_err());
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
