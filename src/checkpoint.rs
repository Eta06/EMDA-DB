//! Explicit, verified index checkpoints. The log remains the source of truth.
use crate::backup;
use crate::log::{Log, ValueLocation};
use crate::record;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"EMDAIDX1";
const HEADER_BYTES: u64 = 68;
const DIGEST_BYTES: u64 = 32;
const ENTRY_FIXED_BYTES: u64 = 18;
const CHUNK: usize = 64 * 1024;
// Bump this when a checkpoint can cover a log record version that an older
// checkpoint reader does not understand. Unknown versions must fail closed.
const RECORD_CODEC_VERSION: u32 = 2;

pub(crate) struct Loaded {
    pub(crate) prefix: u64,
    pub(crate) index: BTreeMap<Vec<u8>, ValueLocation>,
    pub(crate) peak: usize,
}

fn hash_log(log: &Log, length: u64) -> io::Result<[u8; 32]> {
    if length > log.len()? {
        return Err(record::invalid_data("checkpoint prefix exceeds log"));
    }
    let mut hasher = Sha256::new();
    let mut bytes = [0_u8; CHUNK];
    let mut offset = 0_u64;
    while offset < length {
        let count = (length - offset).min(CHUNK as u64) as usize;
        log.read_bytes(&mut bytes[..count], offset)?;
        hasher.update(&bytes[..count]);
        offset += count as u64;
    }
    Ok(hasher.finalize().into())
}

fn write_checked(writer: &mut impl Write, digest: &mut Sha256, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

fn read_checked(reader: &mut impl Read, digest: &mut Sha256, bytes: &mut [u8]) -> io::Result<()> {
    reader.read_exact(bytes)?;
    digest.update(&*bytes);
    Ok(())
}

pub(crate) fn create_with_sync(
    log: &Log,
    index: &BTreeMap<Vec<u8>, ValueLocation>,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<[u8; 32]> {
    if log.is_legacy() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "checkpoint requires a witnessed store",
        ));
    }
    // This checks every historical record and confirms that the published live
    // index agrees with a complete replay before anything is written.
    let peak = log.verify_index(index)?;
    let prefix = log.len()?;
    let log_digest = hash_log(log, prefix)?;
    let parent = backup::parent_directory(destination)?;
    let mut temp = tempfile::Builder::new()
        .prefix(".db-index-")
        .tempfile_in(parent)?;
    let checkpoint_digest: [u8; 32];
    {
        let mut out = BufWriter::new(temp.as_file_mut());
        let mut digest = Sha256::new();
        write_checked(&mut out, &mut digest, MAGIC)?;
        write_checked(&mut out, &mut digest, &prefix.to_le_bytes())?;
        let count =
            u64::try_from(index.len()).map_err(|_| io::Error::other("index count overflow"))?;
        write_checked(&mut out, &mut digest, &count.to_le_bytes())?;
        let peak = u64::try_from(peak).map_err(|_| io::Error::other("index peak overflow"))?;
        write_checked(&mut out, &mut digest, &peak.to_le_bytes())?;
        write_checked(&mut out, &mut digest, &RECORD_CODEC_VERSION.to_le_bytes())?;
        write_checked(&mut out, &mut digest, &log_digest)?;
        for (key, location) in index {
            record::validate_key(key)?;
            let key_len =
                u16::try_from(key.len()).map_err(|_| io::Error::other("index key too long"))?;
            let value_len = u32::try_from(location.length)
                .map_err(|_| io::Error::other("index value too large"))?;
            write_checked(&mut out, &mut digest, &key_len.to_le_bytes())?;
            write_checked(&mut out, &mut digest, key)?;
            write_checked(&mut out, &mut digest, &location.offset.to_le_bytes())?;
            write_checked(&mut out, &mut digest, &value_len.to_le_bytes())?;
            write_checked(&mut out, &mut digest, &location.checksum.to_le_bytes())?;
        }
        checkpoint_digest = digest.finalize().into();
        out.write_all(&checkpoint_digest)?;
        out.flush()?;
    }
    temp.as_file().sync_all()?;
    let verified = load_file(temp.as_file(), log, prefix, None, &checkpoint_digest)?;
    if verified.prefix != prefix || verified.peak != peak || &verified.index != index {
        return Err(record::invalid_data("checkpoint verification mismatch"));
    }
    temp.persist_noclobber(destination)
        .map_err(|error| error.error)?;
    sync_parent(parent).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("checkpoint may exist but directory synchronization failed: {error}"),
        )
    })?;
    Ok(checkpoint_digest)
}

pub(crate) fn load(
    path: &Path,
    log: &Log,
    witness_boundary: u64,
    max_live_keys: Option<usize>,
    trusted_digest: &[u8; 32],
) -> io::Result<Loaded> {
    let file = File::open(path)?;
    load_file(&file, log, witness_boundary, max_live_keys, trusted_digest)
}

fn load_file(
    file: &File,
    log: &Log,
    witness_boundary: u64,
    max_live_keys: Option<usize>,
    trusted_digest: &[u8; 32],
) -> io::Result<Loaded> {
    let mut index = BTreeMap::new();
    let (prefix, peak) = visit_file(
        file,
        log,
        witness_boundary,
        max_live_keys,
        trusted_digest,
        |_, _, key, _, location| {
            index.insert(key.to_vec(), location);
            Ok(())
        },
    )?;
    Ok(Loaded {
        prefix,
        index,
        peak,
    })
}

// The visitor receives untrusted entries before the entire file and log hash
// are verified. Its results must remain private until this function succeeds.
pub(crate) fn visit_file(
    file: &File,
    log: &Log,
    witness_boundary: u64,
    max_live_keys: Option<usize>,
    trusted_digest: &[u8; 32],
    mut visit: impl FnMut(u64, &[u8; 2], &[u8], &[u8; 16], ValueLocation) -> io::Result<()>,
) -> io::Result<(u64, usize)> {
    let length = file.metadata()?.len();
    if length < HEADER_BYTES + DIGEST_BYTES {
        return Err(record::invalid_data("incomplete index checkpoint"));
    }
    let payload_len = length - DIGEST_BYTES;
    let mut reader = BufReader::new(file.try_clone()?);
    reader.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut header = [0_u8; HEADER_BYTES as usize];
    read_checked(&mut reader, &mut digest, &mut header)?;
    if &header[..8] != MAGIC {
        return Err(record::invalid_data("unknown index checkpoint version"));
    }
    let prefix = u64::from_le_bytes(header[8..16].try_into().expect("eight bytes"));
    let count = u64::from_le_bytes(header[16..24].try_into().expect("eight bytes"));
    let peak = u64::from_le_bytes(header[24..32].try_into().expect("eight bytes"));
    let codec = u32::from_le_bytes(header[32..36].try_into().expect("four bytes"));
    if codec != RECORD_CODEC_VERSION {
        return Err(record::invalid_data(
            "unsupported checkpoint record codec version",
        ));
    }
    let expected_log_digest: [u8; 32] = header[36..68].try_into().expect("32 bytes");
    if peak < count {
        return Err(record::invalid_data(
            "checkpoint peak is below its key count",
        ));
    }
    let peak = usize::try_from(peak)
        .map_err(|_| record::invalid_data("checkpoint peak does not fit this platform"))?;
    if max_live_keys.is_some_and(|max| peak > max) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "recovery live key limit would be exceeded by checkpoint history",
        ));
    }
    if prefix < crate::witness::LOG_HEADER_SIZE as u64
        || prefix > witness_boundary
        || prefix > log.len()?
    {
        return Err(record::invalid_data(
            "checkpoint boundary is outside the witnessed log",
        ));
    }
    // Even an empty value needs a fixed entry plus at least one key byte.
    if count > (payload_len - HEADER_BYTES) / (ENTRY_FIXED_BYTES + 1) {
        return Err(record::invalid_data("impossible checkpoint entry count"));
    }
    let mut consumed = HEADER_BYTES;
    let mut previous = Vec::<u8>::new();
    for _ in 0..count {
        let entry_start = consumed;
        let mut key_len = [0_u8; 2];
        read_checked(&mut reader, &mut digest, &mut key_len)?;
        let key_len_bytes = key_len;
        let key_len = u16::from_le_bytes(key_len_bytes) as usize;
        if key_len == 0 || key_len > 1024 {
            return Err(record::invalid_data("invalid checkpoint key length"));
        }
        let entry_len = ENTRY_FIXED_BYTES + key_len as u64;
        consumed = consumed
            .checked_add(entry_len)
            .ok_or_else(|| record::invalid_data("checkpoint length overflow"))?;
        if consumed > payload_len {
            return Err(record::invalid_data("checkpoint entry exceeds file"));
        }
        let mut key = vec![0_u8; key_len];
        read_checked(&mut reader, &mut digest, &mut key)?;
        let mut tail = [0_u8; 16];
        read_checked(&mut reader, &mut digest, &mut tail)?;
        let offset = u64::from_le_bytes(tail[..8].try_into().expect("eight bytes"));
        let value_len = u32::from_le_bytes(tail[8..12].try_into().expect("four bytes")) as usize;
        let checksum = u32::from_le_bytes(tail[12..].try_into().expect("four bytes"));
        if value_len > record::MAX_VALUE
            || offset < crate::witness::LOG_HEADER_SIZE as u64
            || offset
                .checked_add(value_len as u64)
                .is_none_or(|end| end > prefix)
        {
            return Err(record::invalid_data(
                "checkpoint value location outside log prefix",
            ));
        }
        if !previous.is_empty() && previous >= key {
            return Err(record::invalid_data(
                "checkpoint keys are not strictly ordered",
            ));
        }
        visit(
            entry_start,
            &key_len_bytes,
            &key,
            &tail,
            ValueLocation {
                offset,
                length: value_len,
                checksum,
            },
        )?;
        previous = key;
    }
    if consumed != payload_len {
        return Err(record::invalid_data("trailing data in index checkpoint"));
    }
    let mut expected = [0_u8; 32];
    reader.read_exact(&mut expected)?;
    if digest.finalize().as_slice() != expected || &expected != trusted_digest {
        return Err(record::invalid_data("index checkpoint checksum mismatch"));
    }
    if hash_log(log, prefix)? != expected_log_digest {
        return Err(record::invalid_data("checkpoint does not match log prefix"));
    }
    Ok((prefix, peak))
}
