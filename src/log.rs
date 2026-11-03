//! Append, durability, read verification, and recovery for the record log.

use crate::log_io::LogIo;
use crate::record::{self, HEADER_SIZE, Header, Operation};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) struct ValueLocation {
    offset: u64,
    length: usize,
    checksum: u32,
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

    pub(crate) fn append_unsynced(
        &self,
        encoded: &[u8],
        key_len: usize,
        value_len: usize,
        checksum: u32,
    ) -> io::Result<ValueLocation> {
        let start = self.file.len()?;
        self.file.append_all(encoded)?;
        Ok(ValueLocation {
            offset: start + HEADER_SIZE as u64 + key_len as u64,
            length: value_len,
            checksum,
        })
    }

    pub(crate) fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn recover(&mut self) -> io::Result<BTreeMap<Vec<u8>, ValueLocation>> {
        let mut index = BTreeMap::new();
        let total = self.file.len()?;
        let mut offset = 0_u64;
        while offset < total {
            if total - offset < HEADER_SIZE as u64 {
                return Err(incomplete_record(offset));
            }
            let mut header_bytes = [0_u8; HEADER_SIZE];
            self.file.read_exact_at(&mut header_bytes, offset)?;
            let header = Header::parse(&header_bytes)?;
            let record_len = header.record_len();
            if total - offset < record_len {
                return Err(incomplete_record(offset));
            }
            let mut payload = vec![0_u8; header.key_len + header.value_len];
            self.file
                .read_exact_at(&mut payload, offset + HEADER_SIZE as u64)?;
            let mut checksum = [0_u8; 4];
            self.file.read_exact_at(
                &mut checksum,
                offset + HEADER_SIZE as u64 + payload.len() as u64,
            )?;
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
