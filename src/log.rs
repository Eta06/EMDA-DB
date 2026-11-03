//! Append, durability, read verification, and recovery for the record log.

use crate::log_io::LogIo;
use crate::record::{self, HEADER_SIZE, Header, Operation};
use crate::recovery::RecoveryReader;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice};
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
}

impl Log {
    pub(crate) fn open(
        path: &Path,
        may_initialize: bool,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(may_initialize)
            .open(path.join("records.log"))
            .map_err(|error| {
                if !may_initialize && error.kind() == io::ErrorKind::NotFound {
                    record::invalid_data(
                        "existing store is missing its log; refusing to create an empty store",
                    )
                } else {
                    error
                }
            })?;
        // Persist directory entries before acknowledging future writes.
        File::open(path)?.sync_all()?;
        Self::from_io(Box::new(file))
    }

    pub(crate) fn from_io(
        file: Box<dyn LogIo>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let mut log = Self { file };
        let index = log.recover()?;
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
        self.file.sync_data()
    }

    fn recover(&mut self) -> io::Result<BTreeMap<Vec<u8>, ValueLocation>> {
        let mut index = BTreeMap::new();
        let total = self.file.len()?;
        let mut offset = 0_u64;
        let mut reader = RecoveryReader::new(self.file.as_ref(), total);
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
        }
        Ok(index)
    }
}

fn incomplete_record(offset: u64) -> io::Error {
    record::invalid_data(&format!(
        "incomplete log record at byte {offset}; store was not modified"
    ))
}
