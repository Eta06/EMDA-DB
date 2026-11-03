//! Append, durability, read verification, and recovery for the record log.

use crate::log_io::LogIo;
use crate::record::{self, HEADER_SIZE, Header, Operation};
use crate::recovery::RecoveryReader;
use crate::witness::{self, LOG_HEADER_SIZE, LogId, Witness};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Write};
use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) struct ValueLocation {
    offset: u64,
    length: usize,
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
}

impl Log {
    pub(crate) fn open(
        path: &Path,
        may_initialize: bool,
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
            let header = witness::new_header()?;
            (&file).write_all(&header)?;
            file.sync_data()?;
            let id = witness::parse_header(&header)?;
            let _ = Witness::create(path, id, LOG_HEADER_SIZE as u64)?;
        }
        // Persist directory entries before acknowledging future writes.
        File::open(path)?.sync_all()?;
        let file: Box<dyn LogIo> = Box::new(file);
        let id = log_identity(file.as_ref())?;
        let (witness, boundary) = match id {
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
        };
        let (log, index) = Self::from_parts(file, id, witness, boundary)?;
        if let Some(boundary) = boundary {
            let total = log.file.len()?;
            if total > boundary {
                // A complete but unacknowledged tail may survive a crash.
                // Validate it first, then synchronize it before advancing the witness.
                log.file.sync_data()?;
                log.witness.as_ref().unwrap().advance(total)?;
            }
        }
        Ok((log, index))
    }

    pub(crate) fn from_io(
        file: Box<dyn LogIo>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let id = log_identity(file.as_ref())?;
        Self::from_parts(file, id, None, None)
    }

    fn from_parts(
        file: Box<dyn LogIo>,
        log_id: Option<LogId>,
        witness: Option<Witness>,
        boundary: Option<u64>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let mut log = Self {
            file,
            log_id,
            witness,
        };
        let index = log.recover(boundary)?;
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
        self.file.sync_data()?;
        if let Some(witness) = &self.witness {
            witness.advance(self.file.len()?)?;
        }
        Ok(())
    }

    pub(crate) fn install_restore_witness(&self, directory: &Path) -> io::Result<()> {
        if let Some(id) = self.log_id {
            let _ = Witness::create(directory, id, self.file.len()?)?;
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
        if let Some(witness) = &self.witness {
            witness.verify(self.file.len()?)?;
        }
        Ok(())
    }

    fn recover(&mut self, boundary: Option<u64>) -> io::Result<BTreeMap<Vec<u8>, ValueLocation>> {
        let mut index = BTreeMap::new();
        let total = self.file.len()?;
        let mut offset = 0_u64;
        let mut reader = RecoveryReader::new(self.file.as_ref(), total);
        if self.log_id.is_some() {
            let mut header = [0_u8; LOG_HEADER_SIZE];
            reader.read_exact(&mut header)?;
            witness::parse_header(&header)?;
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

fn log_identity(file: &dyn LogIo) -> io::Result<Option<LogId>> {
    if file.len()? < witness::LOG_MAGIC.len() as u64 {
        return Ok(None);
    }
    let mut magic = [0_u8; 8];
    file.read_exact_at(&mut magic, 0)?;
    if &magic != witness::LOG_MAGIC {
        return Ok(None);
    }
    if file.len()? < LOG_HEADER_SIZE as u64 {
        return Err(record::invalid_data("incomplete versioned log header"));
    }
    let mut header = [0_u8; LOG_HEADER_SIZE];
    file.read_exact_at(&mut header, 0)?;
    Ok(Some(witness::parse_header(&header)?))
}

fn incomplete_record(offset: u64) -> io::Error {
    record::invalid_data(&format!(
        "incomplete log record at byte {offset}; store was not modified"
    ))
}

#[cfg(test)]
#[path = "log_tests.rs"]
mod tests;
