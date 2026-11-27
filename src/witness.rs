//! Versioned log identity and an independently synchronized commit boundary.
//!
//! A damaged witness is an error, even when the log looks valid: silently
//! falling back to an older boundary could hide loss of acknowledged writes.
use crate::{publication_outcome::TailAdoptionTracker, record};
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
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
    // Full device-cache flush required after an ordered barrier sync on the log.
    fn sync_full(&self) -> io::Result<()> {
        self.sync_data()
    }
    fn device_id(&self) -> io::Result<Option<u64>> {
        Ok(None)
    }
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
    #[cfg(target_os = "macos")]
    fn sync_full(&self) -> io::Result<()> {
        crate::log_io::macos_sync::fullfsync(self)
    }
    fn device_id(&self) -> io::Result<Option<u64>> {
        Ok(Some(self.metadata()?.dev()))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CreationPoint {
    FileCreated,
    Written,
    FileSynced,
    DirectorySynced,
}

impl Witness {
    pub(crate) fn open_readonly(directory: &Path, id: LogId) -> io::Result<(Self, u64)> {
        Self::from_io(Box::new(File::open(directory.join(NAME))?), id)
    }

    pub(crate) fn create_with_hook(
        directory: &Path,
        id: LogId,
        offset: u64,
        mut hook: impl FnMut(CreationPoint) -> io::Result<()>,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(directory.join(NAME))?;
        hook(CreationPoint::FileCreated)?;
        FileExt::write_all_at(&file, &encode(&id, offset), 0)?;
        hook(CreationPoint::Written)?;
        file.sync_all()?;
        hook(CreationPoint::FileSynced)?;
        File::open(directory)?.sync_all()?;
        hook(CreationPoint::DirectorySynced)?;
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
        self.advance_tracked_with_sync(offset, None, false)
    }

    pub(crate) fn advance_with_sync(&self, offset: u64, require_full_sync: bool) -> io::Result<()> {
        self.advance_tracked_with_sync(offset, None, require_full_sync)
    }

    #[cfg(test)]
    pub(crate) fn advance_tracked(
        &self,
        offset: u64,
        tracker: Option<&TailAdoptionTracker>,
    ) -> io::Result<()> {
        self.advance_tracked_with_sync(offset, tracker, false)
    }

    pub(crate) fn advance_tracked_with_sync(
        &self,
        offset: u64,
        tracker: Option<&TailAdoptionTracker>,
        require_full_sync: bool,
    ) -> io::Result<()> {
        let mut current = self
            .offset
            .lock()
            .map_err(|_| io::Error::other("commit witness lock poisoned"))?;
        if read(self.file.as_ref(), &self.id)? != *current || offset < *current {
            return Err(record::invalid_data(
                "commit witness changed or moved backward",
            ));
        }
        let write_witness = || {
            self.file.write_all_at(&encode(&self.id, offset), 0)?;
            if require_full_sync {
                self.file.sync_full()
            } else {
                self.file.sync_data()
            }
        };
        match tracker {
            Some(tracker) => tracker.update(write_witness)?,
            None => write_witness()?,
        }
        *current = offset;
        Ok(())
    }

    pub(crate) fn device_id(&self) -> io::Result<Option<u64>> {
        self.file.device_id()
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
    use crate::publication_outcome::{ManagedRebuildFailure, track_managed_rebuild};
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
            let result = track_managed_rebuild(|tracker| {
                witness.advance_tracked(128, Some(&tracker.tail_adoption))
            });
            assert!(
                matches!(
                    result,
                    Err(ManagedRebuildFailure::TailAdoptionOutcomeUnknown(_))
                ),
                "offset {count}: {result:?}"
            );
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
        let result = track_managed_rebuild(|tracker| {
            witness.advance_tracked(128, Some(&tracker.tail_adoption))
        });
        assert!(matches!(
            result,
            Err(ManagedRebuildFailure::TailAdoptionOutcomeUnknown(_))
        ));
        assert_eq!(read(&Handle(memory), &id).unwrap(), 128);
        assert!(witness.verify(128).is_err()); // The original open instance is not safe to reuse.
    }

    #[test]
    fn substituted_witness_fails_before_advancement() {
        let id = [7; 16];
        let memory = Arc::new(Memory {
            bytes: Mutex::new(encode(&id, 64).to_vec()),
            fault: Fault::Partial(0),
        });
        let witness = Witness::for_test(Box::new(Handle(Arc::clone(&memory))), id, 28);
        let result = track_managed_rebuild(|tracker| {
            witness.advance_tracked(128, Some(&tracker.tail_adoption))
        });
        assert!(matches!(
            result,
            Err(ManagedRebuildFailure::NotPublished(error))
                if error.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(read(&Handle(memory), &id).unwrap(), 64);
    }

    #[test]
    fn require_full_sync_uses_sync_full_and_propagates_faults() {
        #[derive(Clone, Copy)]
        enum FullFault {
            None,
            Partial(usize),
            SyncFull,
        }
        struct FullMemory {
            bytes: Mutex<Vec<u8>>,
            fault: FullFault,
            sync_data_calls: Mutex<usize>,
            sync_full_calls: Mutex<usize>,
        }
        struct FullHandle(Arc<FullMemory>);
        impl WitnessIo for FullHandle {
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
                    FullFault::Partial(count) => count,
                    FullFault::None | FullFault::SyncFull => bytes.len(),
                };
                stored[offset as usize..offset as usize + count].copy_from_slice(&bytes[..count]);
                if matches!(self.0.fault, FullFault::Partial(_)) {
                    return Err(io::Error::other("injected witness write failure"));
                }
                Ok(())
            }
            fn sync_data(&self) -> io::Result<()> {
                *self.0.sync_data_calls.lock().unwrap() += 1;
                Ok(())
            }
            fn sync_full(&self) -> io::Result<()> {
                *self.0.sync_full_calls.lock().unwrap() += 1;
                if matches!(self.0.fault, FullFault::SyncFull) {
                    return Err(io::Error::other("injected witness full sync failure"));
                }
                Ok(())
            }
        }

        let id = [12; 16];

        // Success path invokes sync_full and never sync_data.
        let memory = Arc::new(FullMemory {
            bytes: Mutex::new(encode(&id, 28).to_vec()),
            fault: FullFault::None,
            sync_data_calls: Mutex::new(0),
            sync_full_calls: Mutex::new(0),
        });
        let witness = Witness::for_test(Box::new(FullHandle(Arc::clone(&memory))), id, 28);
        witness.advance_with_sync(128, true).unwrap();
        assert_eq!(*memory.sync_data_calls.lock().unwrap(), 0);
        assert_eq!(*memory.sync_full_calls.lock().unwrap(), 1);
        assert_eq!(read(&FullHandle(memory), &id).unwrap(), 128);
        witness.verify(128).unwrap();

        // SyncFull fault returns TailAdoptionOutcomeUnknown and blocks reuse.
        let memory = Arc::new(FullMemory {
            bytes: Mutex::new(encode(&id, 28).to_vec()),
            fault: FullFault::SyncFull,
            sync_data_calls: Mutex::new(0),
            sync_full_calls: Mutex::new(0),
        });
        let witness = Witness::for_test(Box::new(FullHandle(Arc::clone(&memory))), id, 28);
        let result = track_managed_rebuild(|tracker| {
            witness.advance_tracked_with_sync(128, Some(&tracker.tail_adoption), true)
        });
        assert!(matches!(
            result,
            Err(ManagedRebuildFailure::TailAdoptionOutcomeUnknown(_))
        ));
        assert_eq!(*memory.sync_data_calls.lock().unwrap(), 0);
        assert_eq!(*memory.sync_full_calls.lock().unwrap(), 1);
        assert_eq!(read(&FullHandle(memory), &id).unwrap(), 128);
        assert!(witness.verify(128).is_err());

        // Partial write before sync_full never calls sync_full and never forges a boundary.
        for count in [0, 12, WITNESS_SIZE] {
            let memory = Arc::new(FullMemory {
                bytes: Mutex::new(encode(&id, 28).to_vec()),
                fault: FullFault::Partial(count),
                sync_data_calls: Mutex::new(0),
                sync_full_calls: Mutex::new(0),
            });
            let witness = Witness::for_test(Box::new(FullHandle(Arc::clone(&memory))), id, 28);
            let result = track_managed_rebuild(|tracker| {
                witness.advance_tracked_with_sync(128, Some(&tracker.tail_adoption), true)
            });
            assert!(matches!(
                result,
                Err(ManagedRebuildFailure::TailAdoptionOutcomeUnknown(_))
            ));
            assert_eq!(*memory.sync_data_calls.lock().unwrap(), 0);
            assert_eq!(*memory.sync_full_calls.lock().unwrap(), 0);
            let state = read(&FullHandle(memory), &id);
            assert!(state.is_err() || matches!(state, Ok(28 | 128)));
        }

        // Substituted witness fails with NotPublished before touching write or sync_full.
        let memory = Arc::new(FullMemory {
            bytes: Mutex::new(encode(&id, 64).to_vec()),
            fault: FullFault::None,
            sync_data_calls: Mutex::new(0),
            sync_full_calls: Mutex::new(0),
        });
        let witness = Witness::for_test(Box::new(FullHandle(Arc::clone(&memory))), id, 28);
        let result = track_managed_rebuild(|tracker| {
            witness.advance_tracked_with_sync(128, Some(&tracker.tail_adoption), true)
        });
        assert!(matches!(
            result,
            Err(ManagedRebuildFailure::NotPublished(error))
                if error.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(*memory.sync_data_calls.lock().unwrap(), 0);
        assert_eq!(*memory.sync_full_calls.lock().unwrap(), 0);
        assert_eq!(read(&FullHandle(memory), &id).unwrap(), 64);
    }
}
