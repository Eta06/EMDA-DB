//! Append, durability, read verification, and recovery for the record log.

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
    offset: u64,
    pub(crate) length: usize,
    checksum: u32,
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
    pub(crate) fn open(
        path: &Path,
        may_initialize: bool,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_with_recovery_limit(path, may_initialize, None)
    }

    pub(crate) fn open_with_recovery_limit(
        path: &Path,
        may_initialize: bool,
        max_live_keys: Option<usize>,
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
        let (mut log, index) =
            Self::from_parts(file, id, witness, boundary, metadata, max_live_keys)?;
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
        Self::from_parts(file, id, None, None, metadata, None)
    }

    fn from_parts(
        file: Box<dyn LogIo>,
        log_id: Option<LogId>,
        witness: Option<Witness>,
        boundary: Option<u64>,
        metadata: bool,
        max_live_keys: Option<usize>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let log = Self {
            file,
            log_id,
            witness,
            metadata,
            anchor: None,
        };
        let index = log.recover(boundary, max_live_keys)?;
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
    ) -> io::Result<()> {
        self.verify_witness()?;
        if &self.recover(None, None)? != expected {
            return Err(record::invalid_data(
                "live index does not match verified log",
            ));
        }
        self.verify_witness()
    }

    fn recover(
        &self,
        boundary: Option<u64>,
        max_live_keys: Option<usize>,
    ) -> io::Result<BTreeMap<Vec<u8>, ValueLocation>> {
        let mut index = BTreeMap::new();
        let total = self.file.len()?;
        let mut offset = 0_u64;
        let mut reader = RecoveryReader::new(self.file.as_ref(), total);
        if self.log_id.is_some() {
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
                    if max_live_keys
                        .is_some_and(|max| index.len() >= max && !index.contains_key(&key))
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "recovery live key limit would be exceeded",
                        ));
                    }
                    index.insert(
                        key,
                        ValueLocation {
                            offset: offset + HEADER_SIZE as u64 + header.key_len as u64,
                            length: header.value_len,
                            checksum: crc32fast::hash(&payload[header.key_len..]),
                        },
                    );
                }
                Operation::Delete => {
                    index.remove(&key);
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
        Ok(index)
    }
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
