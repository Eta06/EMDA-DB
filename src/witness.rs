//! Versioned log identity and an independently synchronized commit boundary.
//!
//! A damaged witness is an error, even when the log looks valid: silently
//! falling back to an older boundary could hide loss of acknowledged writes.
use crate::record;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Mutex;

pub(crate) const LOG_MAGIC: &[u8; 8] = b"EMDALOG2";
const WITNESS_MAGIC: &[u8; 8] = b"EMDAWIT2";
pub(crate) const LOG_HEADER_SIZE: usize = 28;
pub(crate) const WITNESS_SIZE: usize = 36;
const NAME: &str = "commit.state";
pub(crate) type LogId = [u8; 16];

pub(crate) fn new_header() -> io::Result<[u8; LOG_HEADER_SIZE]> {
    let mut id = [0_u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut id)?;
    let mut header = [0_u8; LOG_HEADER_SIZE];
    header[..8].copy_from_slice(LOG_MAGIC);
    header[8..24].copy_from_slice(&id);
    let checksum = crc32fast::hash(&header[..24]);
    header[24..].copy_from_slice(&checksum.to_le_bytes());
    Ok(header)
}

pub(crate) fn parse_header(header: &[u8; LOG_HEADER_SIZE]) -> io::Result<LogId> {
    if &header[..8] != LOG_MAGIC
        || crc32fast::hash(&header[..24]) != u32::from_le_bytes(header[24..].try_into().unwrap())
    {
        return Err(record::invalid_data("invalid versioned log header"));
    }
    Ok(header[8..24].try_into().unwrap())
}

pub(crate) fn encode(id: &LogId, offset: u64) -> [u8; WITNESS_SIZE] {
    let mut bytes = [0_u8; WITNESS_SIZE];
    bytes[..8].copy_from_slice(WITNESS_MAGIC);
    bytes[8..24].copy_from_slice(id);
    bytes[24..32].copy_from_slice(&offset.to_le_bytes());
    let checksum = crc32fast::hash(&bytes[..32]);
    bytes[32..].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

#[cfg(test)]
pub(crate) fn encode_for_test(id: &LogId, offset: u64) -> Vec<u8> {
    encode(id, offset).to_vec()
}

pub(crate) trait WitnessIo: Send + Sync {
    fn len(&self) -> io::Result<u64>;
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()>;
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()>;
    fn sync_data(&self) -> io::Result<()>;
    fn read_marker(&self) -> io::Result<[u8; WITNESS_SIZE]> {
        if self.len()? != WITNESS_SIZE as u64 {
            return Err(record::invalid_data("invalid commit witness length"));
        }
        let mut bytes = [0; WITNESS_SIZE];
        self.read_exact_at(&mut bytes, 0)?;
        Ok(bytes)
    }
}

impl WitnessIo for File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(self, bytes, offset)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        FileExt::write_all_at(self, bytes, offset)
    }
    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }
}

fn read(file: &dyn WitnessIo, id: &LogId) -> io::Result<u64> {
    let bytes = file.read_marker()?;
    decode(&bytes, id)
}

pub(crate) fn decode(bytes: &[u8; WITNESS_SIZE], id: &LogId) -> io::Result<u64> {
    if &bytes[..8] != WITNESS_MAGIC
        || &bytes[8..24] != id
        || crc32fast::hash(&bytes[..32]) != u32::from_le_bytes(bytes[32..].try_into().unwrap())
    {
        return Err(record::invalid_data(
            "invalid commit witness or log identity",
        ));
    }
    Ok(u64::from_le_bytes(bytes[24..32].try_into().unwrap()))
}

pub(crate) fn exists(directory: &Path) -> io::Result<bool> {
    // Treat a dangling symlink as an existing, invalid witness entry. Following
    // it here would make a broken witness look absent and can permit a legacy
    // fallback or creation where recovery must instead fail closed.
    match std::fs::symlink_metadata(directory.join(NAME)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) struct Witness {
    file: Box<dyn WitnessIo>,
    id: LogId,
    offset: Mutex<u64>,
}

impl Witness {
    pub(crate) fn open_readonly(directory: &Path, id: LogId) -> io::Result<(Self, u64)> {
        Self::from_io(Box::new(File::open(directory.join(NAME))?), id)
    }

    pub(crate) fn create(directory: &Path, id: LogId, offset: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(directory.join(NAME))?;
        FileExt::write_all_at(&file, &encode(&id, offset), 0)?;
        file.sync_all()?;
        File::open(directory)?.sync_all()?;
        Ok(Self {
            file: Box::new(file),
            id,
            offset: Mutex::new(offset),
        })
    }

    pub(crate) fn open(directory: &Path, id: LogId) -> io::Result<(Self, u64)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join(NAME))
            .map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    record::invalid_data("versioned log is missing its commit witness")
                } else {
                    error
                }
            })?;
        let offset = read(&file, &id)?;
        Ok((
            Self {
                file: Box::new(file),
                id,
                offset: Mutex::new(offset),
            },
            offset,
        ))
    }

    pub(crate) fn advance(&self, offset: u64) -> io::Result<()> {
        let mut current = self
            .offset
            .lock()
            .map_err(|_| io::Error::other("commit witness lock poisoned"))?;
        if read(self.file.as_ref(), &self.id)? != *current || offset < *current {
            return Err(record::invalid_data(
                "commit witness changed or moved backward",
            ));
        }
        self.file.write_all_at(&encode(&self.id, offset), 0)?;
        self.file.sync_data()?;
        *current = offset;
        Ok(())
    }

    pub(crate) fn verify(&self, log_len: u64) -> io::Result<()> {
        let current = self
            .offset
            .lock()
            .map_err(|_| io::Error::other("commit witness lock poisoned"))?;
        if read(self.file.as_ref(), &self.id)? != *current || log_len != *current {
            return Err(record::invalid_data(
                "log or commit witness changed while the store was open",
            ));
        }
        Ok(())
    }

    pub(crate) fn from_io(file: Box<dyn WitnessIo>, id: LogId) -> io::Result<(Self, u64)> {
        let offset = read(file.as_ref(), &id)?;
        Ok((
            Self {
                file,
                id,
                offset: Mutex::new(offset),
            },
            offset,
        ))
    }

    #[cfg(test)]
    pub(crate) fn for_test(file: Box<dyn WitnessIo>, id: LogId, offset: u64) -> Self {
        Self {
            file,
            id,
            offset: Mutex::new(offset),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Clone, Copy)]
    enum Fault {
        Partial(usize),
        Sync,
    }
    struct Memory {
        bytes: Mutex<Vec<u8>>,
        fault: Fault,
    }
    struct Handle(Arc<Memory>);
    impl WitnessIo for Handle {
        fn len(&self) -> io::Result<u64> {
            Ok(self.0.bytes.lock().unwrap().len() as u64)
        }
        fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            let stored = self.0.bytes.lock().unwrap();
            bytes.copy_from_slice(&stored[offset as usize..offset as usize + bytes.len()]);
            Ok(())
        }
        fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
            let mut stored = self.0.bytes.lock().unwrap();
            let count = match self.0.fault {
                Fault::Partial(count) => count,
                Fault::Sync => bytes.len(),
            };
            stored[offset as usize..offset as usize + count].copy_from_slice(&bytes[..count]);
            if matches!(self.0.fault, Fault::Partial(_)) {
                return Err(io::Error::other("injected witness write failure"));
            }
            Ok(())
        }
        fn sync_data(&self) -> io::Result<()> {
            if matches!(self.0.fault, Fault::Sync) {
                return Err(io::Error::other("injected witness sync failure"));
            }
            Ok(())
        }
    }

    #[test]
    fn every_partial_witness_update_returns_error_and_never_forges_a_boundary() {
        let id = [7; 16];
        for count in 0..=WITNESS_SIZE {
            let memory = Arc::new(Memory {
                bytes: Mutex::new(encode(&id, 28).to_vec()),
                fault: Fault::Partial(count),
            });
            let handle = Handle(Arc::clone(&memory));
            let witness = Witness::for_test(Box::new(handle), id, 28);
            assert!(witness.advance(128).is_err(), "offset {count}");
            let state = read(&Handle(memory), &id);
            assert!(
                state.is_err() || matches!(state, Ok(28 | 128)),
                "offset {count}: {state:?}"
            );
        }
    }

    #[test]
    fn failed_witness_sync_returns_error_even_if_the_new_bytes_are_visible() {
        let id = [9; 16];
        let memory = Arc::new(Memory {
            bytes: Mutex::new(encode(&id, 28).to_vec()),
            fault: Fault::Sync,
        });
        let witness = Witness::for_test(Box::new(Handle(Arc::clone(&memory))), id, 28);
        assert!(witness.advance(128).is_err());
        assert_eq!(read(&Handle(memory), &id).unwrap(), 128);
        assert!(witness.verify(128).is_err()); // The original open instance is not safe to reuse.
    }
}
