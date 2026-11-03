//! Append, durability, read verification, and recovery for the record log.

use crate::record::{self, HEADER_SIZE, Header, Operation};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt as UnixFileExt;
use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) struct ValueLocation {
    offset: u64,
    length: usize,
    checksum: u32,
}

pub(crate) struct Log {
    file: File,
}

impl Log {
    pub(crate) fn open(path: &Path) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join("records.log"))?;
        // Persist directory entries before acknowledging future writes.
        File::open(path)?.sync_all()?;
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

    pub(crate) fn append(
        &mut self,
        encoded: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> io::Result<ValueLocation> {
        let start = self.file.metadata()?.len();
        self.file.write_all(encoded)?;
        self.file.sync_data()?;
        Ok(ValueLocation {
            offset: start + HEADER_SIZE as u64 + key.len() as u64,
            length: value.len(),
            checksum: crc32fast::hash(value),
        })
    }

    fn recover(&mut self) -> io::Result<BTreeMap<Vec<u8>, ValueLocation>> {
        let mut index = BTreeMap::new();
        let total = self.file.metadata()?.len();
        let mut offset = 0_u64;
        self.file.seek(SeekFrom::Start(0))?;
        while offset < total {
            if total - offset < HEADER_SIZE as u64 {
                self.truncate_tail(offset)?;
                break;
            }
            let mut header_bytes = [0_u8; HEADER_SIZE];
            self.file.read_exact(&mut header_bytes)?;
            let header = Header::parse(&header_bytes)?;
            let record_len = header.record_len();
            if total - offset < record_len {
                self.truncate_tail(offset)?;
                break;
            }
            let mut payload = vec![0_u8; header.key_len + header.value_len];
            self.file.read_exact(&mut payload)?;
            let mut checksum = [0_u8; 4];
            self.file.read_exact(&mut checksum)?;
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
        self.file.seek(SeekFrom::End(0))?;
        Ok(index)
    }

    fn truncate_tail(&mut self, offset: u64) -> io::Result<()> {
        self.file.set_len(offset)?;
        self.file.sync_data()
    }
}
