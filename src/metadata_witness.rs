//! Linux V3: a checksummed commit boundary in the log inode's xattrs.
//!
//! One fsync covers appended records and the boundary. Unlike a boundary in
//! the byte stream, truncating or replacing the log bytes does not also roll
//! back the boundary. A missing attribute fails closed. File copies must use
//! the verified backup/restore API or preserve xattrs as well as log bytes.
//!
//! Before fsync completes either data or metadata can persist first. Recovery
//! requires a valid boundary and complete checksummed records; uncertainty
//! never permits acknowledging a write or silently discarding a partial tail.
use crate::record;
#[cfg(target_os = "linux")]
use crate::witness;
use crate::witness::{LOG_HEADER_SIZE, LogId, Witness};
use std::fs::File;
use std::io;

pub(crate) const MAGIC: &[u8; 8] = b"EMDALOG3";

pub(crate) fn mark_header(header: &mut [u8; LOG_HEADER_SIZE]) {
    header[..8].copy_from_slice(MAGIC);
    let checksum = crc32fast::hash(&header[..24]);
    header[24..].copy_from_slice(&checksum.to_le_bytes());
}

pub(crate) fn parse_header(header: &[u8; LOG_HEADER_SIZE]) -> io::Result<LogId> {
    if &header[..8] != MAGIC
        || crc32fast::hash(&header[..24]) != u32::from_le_bytes(header[24..].try_into().unwrap())
    {
        return Err(record::invalid_data("invalid metadata log header"));
    }
    Ok(header[8..24].try_into().unwrap())
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;
    use crate::witness::{WITNESS_SIZE, WitnessIo};
    use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};
    use rustix::io::Errno;

    // An on-disk identifier, independent of the product's eventual name.
    const NAME: &str = "user.db.commit.v3";

    fn retry_interrupted_replace(
        mut replace: impl FnMut() -> Result<(), Errno>,
    ) -> Result<(), Errno> {
        loop {
            match replace() {
                Err(Errno::INTR) => continue,
                result => return result,
            }
        }
    }

    pub(crate) fn is_present(file: &File) -> io::Result<bool> {
        match fgetxattr(file, NAME, &mut [0_u8; 0][..]) {
            Ok(_) => Ok(true),
            Err(Errno::NODATA | Errno::NOTSUP | Errno::NOSYS) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn initialize(file: &File, id: &LogId, offset: u64) -> io::Result<bool> {
        match fsetxattr(file, NAME, &witness::encode(id, offset), XattrFlags::CREATE) {
            Ok(()) => Ok(true),
            // Safe fallback applies ONLY to new stores. An existing V3 store
            // must never silently open without its required boundary metadata.
            Err(Errno::NOTSUP | Errno::NOSYS) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn open(file: File, id: LogId) -> io::Result<(Witness, u64)> {
        Witness::from_io(Box::new(Metadata(file)), id)
    }

    struct Metadata(File);

    impl WitnessIo for Metadata {
        fn len(&self) -> io::Result<u64> {
            Ok(self.read_marker()?.len() as u64)
        }

        fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            if offset != 0 || bytes.len() != WITNESS_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid witness read",
                ));
            }
            bytes.copy_from_slice(&self.read_marker()?);
            Ok(())
        }

        fn read_marker(&self) -> io::Result<[u8; WITNESS_SIZE]> {
            let mut bytes = [0; WITNESS_SIZE];
            match fgetxattr(&self.0, NAME, &mut bytes[..]) {
                Ok(WITNESS_SIZE) => Ok(bytes),
                Ok(_) | Err(Errno::NODATA | Errno::RANGE) => Err(record::invalid_data(
                    "missing or invalid metadata commit witness",
                )),
                Err(error) => Err(error.into()),
            }
        }

        fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
            if offset != 0 || bytes.len() != WITNESS_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid witness update",
                ));
            }
            // `Witness::advance` holds its boundary lock throughout the
            // update. Repeating the same replacement is idempotent even if
            // the interrupted syscall already installed it. Acknowledgement
            // still requires the following full fsync to succeed.
            retry_interrupted_replace(|| fsetxattr(&self.0, NAME, bytes, XattrFlags::REPLACE))
                .map_err(Into::into)
        }

        fn sync_data(&self) -> io::Result<()> {
            // fdatasync need not persist xattrs. A full fsync is mandatory.
            self.0.sync_all()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn interrupted_replacement_retries_the_same_boundary_but_other_errors_stop() {
            let expected = [42_u8; WITNESS_SIZE];
            let mut installed = [0_u8; WITNESS_SIZE];
            let mut calls = 0;
            retry_interrupted_replace(|| {
                calls += 1;
                installed = expected;
                if calls <= 2 { Err(Errno::INTR) } else { Ok(()) }
            })
            .unwrap();
            assert_eq!(calls, 3);
            assert_eq!(installed, expected);

            calls = 0;
            let error = retry_interrupted_replace(|| {
                calls += 1;
                if calls == 1 {
                    Err(Errno::INTR)
                } else {
                    Err(Errno::IO)
                }
            })
            .unwrap_err();
            assert_eq!(calls, 2);
            assert_eq!(error, Errno::IO);
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::*;
    pub(crate) fn is_present(_file: &File) -> io::Result<bool> {
        Ok(false)
    }
    pub(crate) fn initialize(_file: &File, _id: &LogId, _offset: u64) -> io::Result<bool> {
        Ok(false)
    }
    pub(crate) fn open(_file: File, _id: LogId) -> io::Result<(Witness, u64)> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "V3 metadata durability is supported only on Linux",
        ))
    }
}

pub(crate) use platform::{initialize, is_present, open};

// Only used for newly built, private restore/upgrade staging directories.
// Upgrades can have initialized metadata while choosing their output format.
pub(crate) fn install(file: &File, id: &LogId, length: u64) -> io::Result<()> {
    match initialize(file, id, length) {
        Ok(true) => {}
        Ok(false) => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "restoring a V3 store requires durable extended attributes",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let (witness, _) = open(file.try_clone()?, *id)?;
            witness.verify(length)?;
        }
        Err(error) => return Err(error),
    }
    file.sync_all()
}
