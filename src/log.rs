//! Append, durability, read verification, and recovery for the record log.

use crate::checkpoint;
use crate::log_io::LogIo;
use crate::metadata_witness;
use crate::record::{self, HEADER_SIZE, Header, Operation};
use crate::recovery::RecoveryReader;
use crate::witness::{self, LOG_HEADER_SIZE, LogId, Witness};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Write};
use std::path::Path;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValueLocation {
    pub(crate) offset: u64,
    pub(crate) length: usize,
    pub(crate) checksum: u32,
}

pub(crate) struct BatchUpdate {
    pub(crate) key: Vec<u8>,
    pub(crate) location: Option<ValueLocation>,
}

/// Decode and validate every child before the caller publishes any changes.
pub(crate) fn prepare_batch(payload: &[u8], outer: ValueLocation) -> io::Result<Vec<BatchUpdate>> {
    if payload.len() != outer.length {
        return Err(record::invalid_data("batch value length mismatch"));
    }
    record::batch::decode_payload(payload)?
        .into_iter()
        .map(|entry| {
            let location = match entry.operation {
                Operation::Put => Some(ValueLocation {
                    offset: outer
                        .offset
                        .checked_add(entry.value_offset as u64)
                        .ok_or_else(|| record::invalid_data("batch value offset overflow"))?,
                    length: entry.value_len,
                    checksum: entry.checksum,
                }),
                Operation::Delete => None,
                Operation::Batch => return Err(record::invalid_data("nested batch")),
            };
            Ok(BatchUpdate {
                key: entry.key.to_vec(),
                location,
            })
        })
        .collect()
}

pub(crate) struct AppendRecord<'a> {
    pub(crate) encoded: &'a [u8],
    pub(crate) key_len: usize,
    pub(crate) value_len: usize,
    pub(crate) checksum: u32,
}

pub(crate) struct Log {
    file: Box<dyn LogIo>,
    log_id: Option<LogId>,
    witness: Option<Witness>,
    metadata: bool,
    anchor: Option<Witness>,
}

impl Log {
    /// Opens only the log and witness for an explicitly historical checkpoint
    /// view. The caller must hold a shared lock for the log's lifetime.
    pub(crate) fn open_checkpoint_view(path: &Path) -> io::Result<(Self, u64)> {
        let file = File::open(path.join("records.log"))?;
        let (id, metadata) = log_identity(&file)?;
        let id = id.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "checkpoint views require a witnessed store",
            )
        })?;
        if !metadata && metadata_witness::is_present(&file)? {
            return Err(record::invalid_data(
                "metadata witness exists without its V3 log header",
            ));
        }
        let (anchor, witness, boundary) = if metadata {
            let (anchor, initial) = Witness::open_readonly(path, id)?;
            if initial != LOG_HEADER_SIZE as u64 {
                return Err(record::invalid_data("invalid metadata log identity anchor"));
            }
            let (witness, boundary) = metadata_witness::open(file.try_clone()?, id)?;
            (Some(anchor), witness, boundary)
        } else {
            let (witness, boundary) = Witness::open_readonly(path, id)?;
            (None, witness, boundary)
        };
        if boundary < LOG_HEADER_SIZE as u64 || boundary > file.metadata()?.len() {
            return Err(record::invalid_data("commit witness is outside the log"));
        }
        Ok((
            Self {
                file: Box::new(file),
                log_id: Some(id),
                witness: Some(witness),
                metadata,
                anchor,
            },
            boundary,
        ))
    }

    pub(crate) fn open(
        path: &Path,
        may_initialize: bool,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_with_recovery_limit(path, may_initialize, None, None, None)
    }

    pub(crate) fn open_with_recovery_limit(
        path: &Path,
        may_initialize: bool,
        max_live_keys: Option<usize>,
        max_log_bytes: Option<u64>,
        max_live_key_bytes: Option<usize>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_configured(
            path,
            may_initialize,
            max_live_keys,
            max_log_bytes,
            max_live_key_bytes,
            None,
        )
    }

    pub(crate) fn open_with_checkpoint(
        path: &Path,
        may_initialize: bool,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        max_live_keys: Option<usize>,
        max_log_bytes: Option<u64>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_configured(
            path,
            may_initialize,
            max_live_keys,
            max_log_bytes,
            None,
            Some((checkpoint, trusted_digest)),
        )
    }

    fn open_configured(
        path: &Path,
        may_initialize: bool,
        max_live_keys: Option<usize>,
        max_log_bytes: Option<u64>,
        max_live_key_bytes: Option<usize>,
        checkpoint: Option<(&Path, &[u8; 32])>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let log_path = path.join("records.log");
        let witness_exists = witness::exists(path)?;
        let (file, created) = if may_initialize && !witness_exists {
            match OpenOptions::new()
                .read(true)
                .append(true)
                .create_new(true)
                .open(&log_path)
            {
                Ok(file) => (file, true),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
                    OpenOptions::new().read(true).append(true).open(&log_path)?,
                    false,
                ),
                Err(error) => return Err(error),
            }
        } else {
            (OpenOptions::new().read(true).append(true).open(&log_path).map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    record::invalid_data("existing store is missing its log; refusing to create an empty store")
                } else { error }
            })?, false)
        };
        if created {
            let mut header = witness::new_header()?;
            let id = witness::parse_header(&header)?;
            if metadata_witness::initialize(&file, &id, LOG_HEADER_SIZE as u64)? {
                metadata_witness::mark_header(&mut header);
            }
            (&file).write_all(&header)?;
            file.sync_all()?;
            let _ = Witness::create(path, id, LOG_HEADER_SIZE as u64)?;
        }
        // Persist directory entries before acknowledging future writes.
        File::open(path)?.sync_all()?;
        if let Some(max) = max_log_bytes {
            if file.metadata()?.len() > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing log exceeds configured limit",
                ));
            }
        }
        let (id, metadata) = log_identity(&file)?;
        if !metadata && metadata_witness::is_present(&file)? {
            return Err(record::invalid_data(
                "metadata witness exists without its V3 log header",
            ));
        }
        let mut anchor = None;
        let (witness, boundary) = if metadata {
            let (identity, initial_boundary) = Witness::open(path, id.unwrap())?;
            if initial_boundary != LOG_HEADER_SIZE as u64 {
                return Err(record::invalid_data("invalid metadata log identity anchor"));
            }
            anchor = Some(identity);
            let (witness, boundary) = metadata_witness::open(file.try_clone()?, id.unwrap())?;
            (Some(witness), Some(boundary))
        } else {
            match id {
                Some(id) => {
                    let (witness, boundary) = Witness::open(path, id)?;
                    (Some(witness), Some(boundary))
                }
                None if witness::exists(path)? => {
                    return Err(record::invalid_data(
                        "commit witness exists without its versioned log",
                    ));
                }
                None => (None, None),
            }
        };
        let file: Box<dyn LogIo> = Box::new(file);
        let (mut log, index) = Self::from_parts(
            file,
            id,
            witness,
            boundary,
            metadata,
            (max_live_keys, max_live_key_bytes),
            checkpoint,
        )?;
        log.anchor = anchor;
        if let Some(boundary) = boundary {
            let total = log.file.len()?;
            if total > boundary {
                // A complete but unacknowledged tail may survive a crash.
                // Validate it first, then durably publish the adopted boundary.
                log.sync_data()?;
            }
        }
        Ok((log, index))
    }

    pub(crate) fn from_io(
        file: Box<dyn LogIo>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let (id, metadata) = log_identity(file.as_ref())?;
        // Backup readers contain only the log bytes; their length and digest
        // are verified separately before a fresh witness is installed.
        Self::from_parts(file, id, None, None, metadata, (None, None), None)
    }

    fn from_parts(
        file: Box<dyn LogIo>,
        log_id: Option<LogId>,
        witness: Option<Witness>,
        boundary: Option<u64>,
        metadata: bool,
        limits: (Option<usize>, Option<usize>),
        checkpoint: Option<(&Path, &[u8; 32])>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let log = Self {
            file,
            log_id,
            witness,
            metadata,
            anchor: None,
        };
        let (max_live_keys, max_live_key_bytes) = limits;
        let start = if let Some((path, trusted_digest)) = checkpoint {
            if max_live_key_bytes.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "checkpoint does not track historical live key bytes",
                ));
            }
            let witnessed = boundary.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "checkpoint requires a witnessed store",
                )
            })?;
            Some(checkpoint::load(
                path,
                &log,
                witnessed,
                max_live_keys,
                trusted_digest,
            )?)
        } else {
            None
        };
        let mut peak = start.as_ref().map_or(0, |checkpoint| checkpoint.peak);
        let index = log.recover_from(
            boundary,
            max_live_keys,
            max_live_key_bytes,
            start,
            &mut peak,
        )?;
        Ok((log, index))
    }

    pub(crate) fn read(&self, location: ValueLocation) -> io::Result<Vec<u8>> {
        let mut value = vec![0; location.length];
        self.file.read_exact_at(&mut value, location.offset)?;
        if crc32fast::hash(&value) != location.checksum {
            return Err(record::invalid_data("stored value checksum mismatch"));
        }
        Ok(value)
    }

    pub(crate) fn len(&self) -> io::Result<u64> {
        self.file.len()
    }

    pub(crate) fn read_bytes(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buffer, offset)
    }

    pub(crate) fn append_batch_unsynced<'a>(
        &self,
        records: impl ExactSizeIterator<Item = AppendRecord<'a>>,
    ) -> io::Result<Vec<ValueLocation>> {
        let mut offset = self.file.len()?;
        if let Some(anchor) = &self.anchor {
            anchor.verify(LOG_HEADER_SIZE as u64)?;
        }
        if self.metadata && self.witness.is_none() {
            return Err(io::Error::other(
                "metadata log has no attached commit witness",
            ));
        }
        if let Some(witness) = &self.witness {
            witness.verify(offset)?;
        }
        let mut locations = Vec::with_capacity(records.len());
        let mut buffers = Vec::with_capacity(records.len());
        for record in records {
            let value_offset = offset
                .checked_add(HEADER_SIZE as u64)
                .and_then(|value| value.checked_add(record.key_len as u64))
                .ok_or_else(|| io::Error::other("log offset overflow"))?;
            offset = offset
                .checked_add(record.encoded.len() as u64)
                .ok_or_else(|| io::Error::other("log offset overflow"))?;
            locations.push(ValueLocation {
                offset: value_offset,
                length: record.value_len,
                checksum: record.checksum,
            });
            buffers.push(IoSlice::new(record.encoded));
        }
        if buffers.len() == 1 {
            self.file.append_all(&buffers[0])?;
        } else {
            self.file.append_vectored(&mut buffers)?;
        }
        Ok(locations)
    }

    pub(crate) fn sync_data(&self) -> io::Result<()> {
        // V3's witness uses fsync on the log inode, covering data AND xattrs.
        // V2 still requires log fdatasync followed by the external witness sync.
        if self.metadata {
            return self
                .witness
                .as_ref()
                .ok_or_else(|| io::Error::other("metadata log has no attached commit witness"))?
                .advance(self.file.len()?);
        }
        self.file.sync_data()?;
        if let Some(witness) = &self.witness {
            witness.advance(self.file.len()?)?;
        }
        Ok(())
    }

    pub(crate) fn install_restore_witness(&self, directory: &Path) -> io::Result<()> {
        if let Some(id) = self.log_id {
            let length = self.file.len()?;
            let boundary = if self.metadata {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(directory.join("records.log"))?;
                metadata_witness::install(&file, &id, length)?;
                LOG_HEADER_SIZE as u64
            } else {
                length
            };
            let _ = Witness::create(directory, id, boundary)?;
        }
        Ok(())
    }

    pub(crate) fn is_legacy(&self) -> bool {
        self.log_id.is_none()
    }

    #[cfg(test)]
    pub(crate) fn attach_test_witness(&mut self, witness: Witness) {
        self.witness = Some(witness);
    }

    pub(crate) fn verify_witness(&self) -> io::Result<()> {
        if log_identity(self.file.as_ref())? != (self.log_id, self.metadata) {
            return Err(record::invalid_data(
                "log identity changed while the store was open",
            ));
        }
        if let Some(anchor) = &self.anchor {
            anchor.verify(LOG_HEADER_SIZE as u64)?;
        }
        if let Some(witness) = &self.witness {
            witness.verify(self.file.len()?)?;
        }
        Ok(())
    }

    pub(crate) fn verify_index(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
    ) -> io::Result<usize> {
        self.verify_witness()?;
        let mut peak = 0;
        if &self.recover_from(None, None, None, None, &mut peak)? != expected {
            return Err(record::invalid_data(
                "live index does not match verified log",
            ));
        }
        self.verify_witness()?;
        Ok(peak)
    }

    fn recover_from(
        &self,
        boundary: Option<u64>,
        max_live_keys: Option<usize>,
        max_live_key_bytes: Option<usize>,
        start: Option<checkpoint::Loaded>,
        peak: &mut usize,
    ) -> io::Result<BTreeMap<Vec<u8>, ValueLocation>> {
        let (mut index, offset) = match start {
            Some(checkpoint) => (checkpoint.index, checkpoint.prefix),
            None => (BTreeMap::new(), 0),
        };
        let mut live_key_bytes = 0_usize;
        self.visit_records(offset, boundary, |key, location| {
            match location {
                Some(location) => {
                    if max_live_keys
                        .is_some_and(|max| index.len() >= max && !index.contains_key(&key))
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "recovery live key limit would be exceeded",
                        ));
                    }
                    if let Some(max) = max_live_key_bytes {
                        if !index.contains_key(&key) {
                            live_key_bytes =
                                add_recovery_key_bytes(live_key_bytes, key.len(), max)?;
                        }
                    }
                    index.insert(key, location);
                }
                None => {
                    if index.remove(&key).is_some() && max_live_key_bytes.is_some() {
                        live_key_bytes -= key.len();
                    }
                }
            }
            *peak = (*peak).max(index.len());
            Ok(())
        })?;
        Ok(index)
    }

    /// Visits fully verified log mutations without materializing the full index.
    /// Callers must keep visitor results private until the complete replay succeeds.
    pub(crate) fn visit_records(
        &self,
        mut offset: u64,
        boundary: Option<u64>,
        mut visit: impl FnMut(Vec<u8>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<()> {
        let total = self.file.len()?;
        let mut reader = RecoveryReader::at(self.file.as_ref(), total, offset)?;
        if offset == 0 && self.log_id.is_some() {
            let mut header = [0_u8; LOG_HEADER_SIZE];
            reader.read_exact(&mut header)?;
            if self.metadata {
                metadata_witness::parse_header(&header)?;
            } else {
                witness::parse_header(&header)?;
            }
            offset = LOG_HEADER_SIZE as u64;
        }
        let mut saw_boundary = boundary.is_none_or(|value| value == offset);
        while offset < total {
            if total - offset < HEADER_SIZE as u64 {
                return Err(incomplete_record(offset));
            }
            let mut header_bytes = [0_u8; HEADER_SIZE];
            reader.read_exact(&mut header_bytes)?;
            let header = Header::parse(&header_bytes)?;
            let record_len = header.record_len();
            if total - offset < record_len {
                return Err(incomplete_record(offset));
            }
            let mut payload = vec![0_u8; header.key_len + header.value_len];
            reader.read_exact(&mut payload)?;
            let mut checksum = [0_u8; 4];
            reader.read_exact(&mut checksum)?;
            record::verify(&header_bytes, &payload, u32::from_le_bytes(checksum))?;
            let key = payload[..header.key_len].to_vec();
            match header.operation {
                Operation::Put => {
                    visit(
                        key,
                        Some(ValueLocation {
                            offset: offset + HEADER_SIZE as u64 + header.key_len as u64,
                            length: header.value_len,
                            checksum: crc32fast::hash(&payload[header.key_len..]),
                        }),
                    )?;
                }
                Operation::Delete => {
                    visit(key, None)?;
                }
                Operation::Batch => {
                    if key != [0] {
                        return Err(record::invalid_data("invalid batch record key"));
                    }
                    let outer = ValueLocation {
                        offset: offset + HEADER_SIZE as u64 + header.key_len as u64,
                        length: header.value_len,
                        checksum: crc32fast::hash(&payload[header.key_len..]),
                    };
                    for update in prepare_batch(&payload[header.key_len..], outer)? {
                        visit(update.key, update.location)?;
                    }
                }
            }
            offset += record_len;
            if boundary == Some(offset) {
                saw_boundary = true;
            }
        }
        if !saw_boundary {
            return Err(record::invalid_data(
                "log does not contain its acknowledged commit boundary",
            ));
        }
        Ok(())
    }
}

fn add_recovery_key_bytes(current: usize, additional: usize, max: usize) -> io::Result<usize> {
    current
        .checked_add(additional)
        .filter(|bytes| *bytes <= max)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery live key byte limit would be exceeded",
            )
        })
}

fn log_identity(file: &dyn LogIo) -> io::Result<(Option<LogId>, bool)> {
    if file.len()? < witness::LOG_MAGIC.len() as u64 {
        return Ok((None, false));
    }
    let mut magic = [0_u8; 8];
    file.read_exact_at(&mut magic, 0)?;
    let metadata = &magic == metadata_witness::MAGIC;
    if &magic != witness::LOG_MAGIC && !metadata {
        return Ok((None, false));
    }
    if file.len()? < LOG_HEADER_SIZE as u64 {
        return Err(record::invalid_data("incomplete versioned log header"));
    }
    let mut header = [0_u8; LOG_HEADER_SIZE];
    file.read_exact_at(&mut header, 0)?;
    let id = if metadata {
        metadata_witness::parse_header(&header)?
    } else {
        witness::parse_header(&header)?
    };
    Ok((Some(id), metadata))
}

fn incomplete_record(offset: u64) -> io::Error {
    record::invalid_data(&format!(
        "incomplete log record at byte {offset}; store was not modified"
    ))
}

#[cfg(test)]
#[path = "log_tests.rs"]
mod tests;
