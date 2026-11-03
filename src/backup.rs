//! Checksummed full-log backups and non-overwriting restore.

use crate::log::Log;
use crate::log_io::LogIo;
use crate::{metadata_witness, record, witness};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

const HEADER: &[u8; 8] = b"DBBKP001";
const FOOTER: &[u8; 8] = b"DBBEND01";
const HEADER_SIZE: u64 = 8;
const FOOTER_SIZE: u64 = 48;
const BUFFER_SIZE: usize = 64 * 1024;

struct BackupInfo {
    log_len: u64,
    digest: [u8; 32],
}

pub(crate) fn create(log: &Log, destination: &Path) -> io::Result<()> {
    create_with_sync(log, destination, |parent| File::open(parent)?.sync_all())
}

fn create_with_sync(
    log: &Log,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    log.verify_witness()?;
    let parent = parent_directory(destination)?;
    let mut temp = tempfile::Builder::new()
        .prefix(".db-backup-")
        .tempfile_in(parent)?;
    let log_len = log.len()?;
    temp.write_all(HEADER)?;
    let digest = copy_data(
        log_len,
        |buffer, offset| log.read_bytes(buffer, offset),
        temp.as_file_mut(),
    )?;
    temp.write_all(FOOTER)?;
    temp.write_all(&log_len.to_le_bytes())?;
    temp.write_all(&digest)?;
    temp.as_file().sync_all()?;
    validate(temp.as_file())?;
    // The temp file is on the destination filesystem. Publish only a complete,
    // verified file, and never replace an existing destination.
    temp.persist_noclobber(destination)
        .map_err(|error| error.error)?;
    sync_parent(parent).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("backup may exist but directory synchronization failed: {error}"),
        )
    })
}

pub(crate) fn restore(source: &Path, destination: &Path) -> io::Result<()> {
    restore_with_sync(source, destination, |parent| File::open(parent)?.sync_all())
}

fn restore_with_sync(
    source: &Path,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let backup = File::open(source)?;
    let info = validate(&backup)?;
    let parent = parent_directory(destination)?;
    // Build and verify a complete store in a private staging directory. Publish
    // it atomically without replacing a pre-existing destination.
    let staging = tempfile::Builder::new()
        .prefix(".db-restore-")
        .tempdir_in(parent)?;
    restore_into(&backup, &info, staging.path())?;
    publish_directory(staging.path(), destination)?;
    // The directory moved; prevent TempDir from trying to clean the old path.
    let _ = staging.keep();
    sync_parent(parent).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("restore may exist but directory synchronization failed: {error}"),
        )
    })
}

#[cfg(test)]
mod publication_tests;

pub(crate) fn upgrade_legacy(source: &Log, destination: &Path) -> io::Result<()> {
    if !source.is_legacy() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "store already has a commit witness",
        ));
    }
    upgrade_format(source, destination)
}

pub(crate) fn upgrade_format(source: &Log, destination: &Path) -> io::Result<()> {
    source.verify_witness()?;
    let parent = parent_directory(destination)?;
    let staging = tempfile::Builder::new()
        .prefix(".db-upgrade-")
        .tempdir_in(parent)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(staging.path().join("store.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock)?;
    let mut target = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(staging.path().join("records.log"))?;
    let mut header = witness::new_header()?;
    target.write_all(&header)?;
    let start = if source.is_legacy() {
        0
    } else {
        witness::LOG_HEADER_SIZE as u64
    };
    let length = source
        .len()?
        .checked_sub(start)
        .ok_or_else(|| record::invalid_data("source log is shorter than its header"))?;
    let digest = copy_data(
        length,
        |buffer, offset| source.read_bytes(buffer, start + offset),
        &mut target,
    )?;
    let id = witness::parse_header(&header)?;
    if metadata_witness::initialize(&target, &id, witness::LOG_HEADER_SIZE as u64 + length)? {
        metadata_witness::mark_header(&mut header);
        FileExt::write_all_at(&target, &header, 0)?;
    }
    target.sync_all()?;
    if target.metadata()?.len() != witness::LOG_HEADER_SIZE as u64 + length
        || hash_data(length, |buffer, offset| {
            FileExt::read_exact_at(&target, buffer, witness::LOG_HEADER_SIZE as u64 + offset)
        })? != digest
    {
        return Err(record::invalid_data("upgraded log checksum mismatch"));
    }
    // Validate every copied record before establishing the new boundary.
    let (target, _) = Log::from_io(Box::new(target))?;
    target.install_restore_witness(staging.path())?;
    lock.sync_all()?;
    File::open(staging.path())?.sync_all()?;
    publish_directory(staging.path(), destination)?;
    let _ = staging.keep();
    File::open(parent)?.sync_all().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("upgraded store may exist but directory synchronization failed: {error}"),
        )
    })
}

fn restore_into(backup: &File, info: &BackupInfo, destination: &Path) -> io::Result<()> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination.join("store.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock)?;
    let mut log = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination.join("records.log"))?;
    let digest = copy_data(
        info.log_len,
        |buffer, offset| FileExt::read_exact_at(backup, buffer, HEADER_SIZE + offset),
        &mut log,
    )?;
    if digest != info.digest {
        return Err(record::invalid_data(
            "backup changed while it was being restored",
        ));
    }
    log.sync_all()?;
    // Read the target back after sync; a checksum of the source alone would
    // not detect corruption introduced while writing the restored file.
    if log.metadata()?.len() != info.log_len
        || hash_data(info.log_len, |buffer, offset| {
            FileExt::read_exact_at(&log, buffer, offset)
        })? != info.digest
    {
        return Err(record::invalid_data("restored log checksum mismatch"));
    }
    // Revalidate the restored bytes, including every record checksum.
    let (log, _) = Log::from_io(Box::new(log))?;
    log.install_restore_witness(destination)?;
    File::open(destination)?.sync_all()?;
    Ok(())
}

fn validate(file: &File) -> io::Result<BackupInfo> {
    let total = file.metadata()?.len();
    if total < HEADER_SIZE + FOOTER_SIZE {
        return Err(record::invalid_data("backup is incomplete"));
    }
    let mut header = [0_u8; 8];
    FileExt::read_exact_at(file, &mut header, 0)?;
    if &header != HEADER {
        return Err(record::invalid_data("unknown backup format"));
    }
    let mut footer = [0_u8; FOOTER_SIZE as usize];
    FileExt::read_exact_at(file, &mut footer, total - FOOTER_SIZE)?;
    if &footer[..8] != FOOTER {
        return Err(record::invalid_data("backup footer is missing"));
    }
    let log_len = u64::from_le_bytes(footer[8..16].try_into().expect("fixed footer field"));
    if HEADER_SIZE
        .checked_add(log_len)
        .and_then(|n| n.checked_add(FOOTER_SIZE))
        != Some(total)
    {
        return Err(record::invalid_data("backup length mismatch"));
    }
    let digest: [u8; 32] = footer[16..].try_into().expect("fixed digest field");
    let actual = hash_data(log_len, |buffer, offset| {
        FileExt::read_exact_at(file, buffer, HEADER_SIZE + offset)
    })?;
    if actual != digest {
        return Err(record::invalid_data("backup checksum mismatch"));
    }
    let reader = BackupLog {
        file: file.try_clone()?,
        log_len,
    };
    let _ = Log::from_io(Box::new(reader))?;
    Ok(BackupInfo { log_len, digest })
}

/// A read-only view of the log inside a backup; recovery verifies each record.
struct BackupLog {
    file: File,
    log_len: u64,
}

impl LogIo for BackupLog {
    fn len(&self) -> io::Result<u64> {
        Ok(self.log_len)
    }
    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        if offset
            .checked_add(buffer.len() as u64)
            .is_none_or(|end| end > self.log_len)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read exceeds backup log",
            ));
        }
        FileExt::read_exact_at(&self.file, buffer, HEADER_SIZE + offset)
    }
    fn append_all(&self, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "backup is read-only",
        ))
    }
    fn sync_data(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "backup is read-only",
        ))
    }
}

fn copy_data(
    length: u64,
    mut read: impl FnMut(&mut [u8], u64) -> io::Result<()>,
    output: &mut File,
) -> io::Result<[u8; 32]> {
    let mut digest = Sha256::new();
    let mut offset = 0_u64;
    let mut buffer = [0_u8; BUFFER_SIZE];
    while offset < length {
        let count = (length - offset).min(BUFFER_SIZE as u64) as usize;
        read(&mut buffer[..count], offset)?;
        output.write_all(&buffer[..count])?;
        digest.update(&buffer[..count]);
        offset += count as u64;
    }
    Ok(digest.finalize().into())
}

fn hash_data(
    length: u64,
    mut read: impl FnMut(&mut [u8], u64) -> io::Result<()>,
) -> io::Result<[u8; 32]> {
    let mut digest = Sha256::new();
    let mut offset = 0_u64;
    let mut buffer = [0_u8; BUFFER_SIZE];
    while offset < length {
        let count = (length - offset).min(BUFFER_SIZE as u64) as usize;
        read(&mut buffer[..count], offset)?;
        digest.update(&buffer[..count]);
        offset += count as u64;
    }
    Ok(digest.finalize().into())
}

pub(crate) fn parent_directory(path: &Path) -> io::Result<&Path> {
    if path.file_name().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a destination name is required",
        ));
    }
    Ok(path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(".")))
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(crate) fn publish_directory(source: &Path, destination: &Path) -> io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE).map_err(Into::into)
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
pub(crate) fn publish_directory(_source: &Path, _destination: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic non-overwriting restore is not supported on this platform",
    ))
}
