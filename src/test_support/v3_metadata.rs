//! Test faults for the Linux log witness stored in an inode xattr.
use crate::witness::{self, WitnessIo};
use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};
use std::{fs::File, io};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Fault {
    WriteBefore,
    WriteAfter,
    WritePanic,
    SyncBefore,
    SyncAfter,
    SyncPanic,
}

pub(crate) struct Metadata {
    pub(crate) file: File,
    pub(crate) fault: Fault,
}

impl WitnessIo for Metadata {
    fn len(&self) -> io::Result<u64> {
        Ok(witness::WITNESS_SIZE as u64)
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        if offset != 0 || bytes.len() != witness::WITNESS_SIZE {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        bytes.copy_from_slice(&self.read_marker()?);
        Ok(())
    }
    fn read_marker(&self) -> io::Result<[u8; witness::WITNESS_SIZE]> {
        let mut bytes = [0; witness::WITNESS_SIZE];
        let count = fgetxattr(&self.file, "user.db.commit.v3", &mut bytes[..])?;
        if count != bytes.len() {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(bytes)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        if offset != 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if matches!(self.fault, Fault::WriteBefore) {
            return Err(io::Error::other("injected metadata write failure"));
        }
        fsetxattr(&self.file, "user.db.commit.v3", bytes, XattrFlags::REPLACE)?;
        match self.fault {
            Fault::WriteAfter => Err(io::Error::other("injected post-write failure")),
            Fault::WritePanic => panic!("injected metadata write panic"),
            _ => Ok(()),
        }
    }
    fn sync_data(&self) -> io::Result<()> {
        if matches!(self.fault, Fault::SyncBefore) {
            return Err(io::Error::other("injected metadata sync failure"));
        }
        self.file.sync_all()?;
        match self.fault {
            Fault::SyncAfter => Err(io::Error::other("injected post-sync failure")),
            Fault::SyncPanic => panic!("injected metadata sync panic"),
            _ => Ok(()),
        }
    }
}
