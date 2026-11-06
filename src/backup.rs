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

// A staging lock must be explicitly released before the operation returns.
// Another thread may spawn a child while it is held; an inherited descriptor
// can otherwise keep the open-file-description lock alive until that child execs.
struct StagingLock {
    file: File,
    active: bool,
}

impl StagingLock {
    fn create(directory: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(directory.join("store.lock"))?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        Ok(Self { file, active: true })
    }

    fn release(&mut self) -> io::Result<()> {
        if self.active {
            fs2::FileExt::unlock(&self.file)?;
            self.active = false;
        }
        Ok(())
    }
}

impl Drop for StagingLock {
    fn drop(&mut self) {
        // Closing the final descriptor remains the fallback if unlock fails.
        if self.active {
            let _ = fs2::FileExt::unlock(&self.file);
        }
    }
}

pub(crate) fn create(log: &Log, destination: &Path) -> io::Result<()> {
    create_with_sync(log, destination, crate::directory::sync_existing)
}

fn create_with_sync(
    log: &Log,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    create_with_hooks(log, destination, sync_parent, || Ok(()))
}

pub(crate) fn create_with_hooks(
    log: &Log,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    create_with_stage_hooks(log, destination, sync_parent, before_publish, |_| Ok(()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackupCreationPoint {
    HeaderWritten,
    DataChunk,
    DataCopied,
    FooterWritten,
    FileSynced,
    Verified,
}

fn create_with_stage_hooks(
    log: &Log,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
    mut hook: impl FnMut(BackupCreationPoint) -> io::Result<()>,
) -> io::Result<()> {
    log.verify_witness()?;
    let parent = parent_directory(destination)?;
    let (mut staging, mut temp) = crate::staging::Stage::file(parent)?;
    let log_len = log.len()?;
    temp.write_all(HEADER)?;
    hook(BackupCreationPoint::HeaderWritten)?;
    let digest = copy_data_with_hook(
        log_len,
        |buffer, offset| log.read_bytes(buffer, offset),
        &mut temp,
        || hook(BackupCreationPoint::DataChunk),
    )?;
    hook(BackupCreationPoint::DataCopied)?;
    temp.write_all(FOOTER)?;
    temp.write_all(&log_len.to_le_bytes())?;
    temp.write_all(&digest)?;
    hook(BackupCreationPoint::FooterWritten)?;
    temp.sync_all()?;
    hook(BackupCreationPoint::FileSynced)?;
    validate(&temp)?;
    hook(BackupCreationPoint::Verified)?;
    before_publish()?;
    // The temp file is on the destination filesystem. Publish only a complete,
    // verified file, and never replace an existing destination.
    staging.publish(destination)?;
    sync_parent(parent).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("backup may exist but directory synchronization failed: {error}"),
        )
    })?;
    staging.publication_synchronized();
    Ok(())
}

pub(crate) fn restore(source: &Path, destination: &Path) -> io::Result<()> {
    restore_with_sync(source, destination, crate::directory::sync_existing)
}

fn restore_with_sync(
    source: &Path,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    restore_with_hooks(source, destination, sync_parent, || Ok(()))
}

fn restore_with_hooks(
    source: &Path,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    restore_with_empty_hooks(source, destination, sync_parent, before_publish, |_| Ok(()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EmptyRestorePoint {
    BeforeMetadata,
    AfterMetadata,
    AfterHeaderWrite,
    AfterHeaderSync,
    AfterWitness,
    AfterDirectorySync,
}

fn restore_with_empty_hooks(
    source: &Path,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
    empty_hook: impl FnMut(EmptyRestorePoint) -> io::Result<()>,
) -> io::Result<()> {
    restore_with_witness_hooks(
        source,
        destination,
        sync_parent,
        before_publish,
        empty_hook,
        |_| Ok(()),
    )
}

fn restore_with_witness_hooks(
    source: &Path,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
    empty_hook: impl FnMut(EmptyRestorePoint) -> io::Result<()>,
    witness_hook: impl FnMut(witness::CreationPoint) -> io::Result<()>,
) -> io::Result<()> {
    restore_with_stage_hooks(
        source,
        destination,
        sync_parent,
        before_publish,
        empty_hook,
        witness_hook,
        |_| Ok(()),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RebuildPoint {
    LogCreated,
    HeaderWritten,
    DataChunk,
    DataCopied,
    MetadataPrepared,
    LogSynced,
    LogVerified,
    WitnessInstalled,
    LockSynced,
    DirectorySynced,
    LockReleased,
}

fn restore_with_stage_hooks(
    source: &Path,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
    mut empty_hook: impl FnMut(EmptyRestorePoint) -> io::Result<()>,
    mut witness_hook: impl FnMut(witness::CreationPoint) -> io::Result<()>,
    mut stage_hook: impl FnMut(RebuildPoint) -> io::Result<()>,
) -> io::Result<()> {
    let backup = File::open(source)?;
    let info = validate(&backup)?;
    let parent = parent_directory(destination)?;
    // Build and verify a complete store in a private staging directory. Publish
    // it atomically without replacing a pre-existing destination.
    let mut staging = crate::staging::Stage::directory(parent)?;
    restore_into(
        &backup,
        &info,
        staging.path(),
        &mut empty_hook,
        &mut witness_hook,
        &mut stage_hook,
    )?;
    before_publish()?;
    staging.publish(destination)?;
    sync_parent(parent).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("restore may exist but directory synchronization failed: {error}"),
        )
    })?;
    staging.publication_synchronized();
    Ok(())
}

#[cfg(test)]
mod publication_tests;

#[cfg(all(test, target_os = "linux"))]
mod backup_stage_tests;

#[cfg(all(test, target_os = "linux"))]
mod rebuild_stage_tests;

#[cfg(all(test, target_os = "linux"))]
mod witness_interruption_tests;

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
    upgrade_format_with_hooks(source, destination, crate::directory::sync_existing, || {
        Ok(())
    })
}

fn upgrade_format_with_hooks(
    source: &Log,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    upgrade_format_with_witness_hooks(source, destination, sync_parent, before_publish, |_| Ok(()))
}

fn upgrade_format_with_witness_hooks(
    source: &Log,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
    witness_hook: impl FnMut(witness::CreationPoint) -> io::Result<()>,
) -> io::Result<()> {
    upgrade_format_with_stage_hooks(
        source,
        destination,
        sync_parent,
        before_publish,
        witness_hook,
        |_| Ok(()),
    )
}

fn upgrade_format_with_stage_hooks(
    source: &Log,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
    witness_hook: impl FnMut(witness::CreationPoint) -> io::Result<()>,
    mut stage_hook: impl FnMut(RebuildPoint) -> io::Result<()>,
) -> io::Result<()> {
    source.verify_witness()?;
    let parent = parent_directory(destination)?;
    let mut staging = crate::staging::Stage::directory(parent)?;
    let mut lock = StagingLock::create(staging.path())?;
    let mut target = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(staging.path().join("records.log"))?;
    stage_hook(RebuildPoint::LogCreated)?;
    let mut header = witness::new_header()?;
    target.write_all(&header)?;
    stage_hook(RebuildPoint::HeaderWritten)?;
    let start = if source.is_legacy() {
        0
    } else {
        witness::LOG_HEADER_SIZE as u64
    };
    let length = source
        .len()?
        .checked_sub(start)
        .ok_or_else(|| record::invalid_data("source log is shorter than its header"))?;
    let digest = copy_data_with_hook(
        length,
        |buffer, offset| source.read_bytes(buffer, start + offset),
        &mut target,
        || stage_hook(RebuildPoint::DataChunk),
    )?;
    stage_hook(RebuildPoint::DataCopied)?;
    let id = witness::parse_header(&header)?;
    if metadata_witness::initialize(&target, &id, witness::LOG_HEADER_SIZE as u64 + length)? {
        metadata_witness::mark_header(&mut header);
        FileExt::write_all_at(&target, &header, 0)?;
    }
    stage_hook(RebuildPoint::MetadataPrepared)?;
    target.sync_all()?;
    stage_hook(RebuildPoint::LogSynced)?;
    if target.metadata()?.len() != witness::LOG_HEADER_SIZE as u64 + length
        || hash_data(length, |buffer, offset| {
            FileExt::read_exact_at(&target, buffer, witness::LOG_HEADER_SIZE as u64 + offset)
        })? != digest
    {
        return Err(record::invalid_data("upgraded log checksum mismatch"));
    }
    // Validate every copied record before establishing the new boundary.
    let target = Log::verify_io(Box::new(target))?;
    stage_hook(RebuildPoint::LogVerified)?;
    target.install_restore_witness_with_hook(staging.path(), witness_hook)?;
    stage_hook(RebuildPoint::WitnessInstalled)?;
    lock.file.sync_all()?;
    stage_hook(RebuildPoint::LockSynced)?;
    File::open(staging.path())?.sync_all()?;
    stage_hook(RebuildPoint::DirectorySynced)?;
    before_publish()?;
    staging.publish(destination)?;
    let synchronized = sync_parent(parent).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("upgraded store may exist but directory synchronization failed: {error}"),
        )
    });
    if synchronized.is_ok() {
        staging.publication_synchronized();
    }
    let released = lock.release().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("upgraded store may exist but staging lock release failed: {error}"),
        )
    });
    synchronized.and(released)
}

fn restore_into(
    backup: &File,
    info: &BackupInfo,
    destination: &Path,
    empty_hook: &mut impl FnMut(EmptyRestorePoint) -> io::Result<()>,
    witness_hook: &mut impl FnMut(witness::CreationPoint) -> io::Result<()>,
    stage_hook: &mut impl FnMut(RebuildPoint) -> io::Result<()>,
) -> io::Result<()> {
    let mut lock = StagingLock::create(destination)?;
    let mut log = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination.join("records.log"))?;
    stage_hook(RebuildPoint::LogCreated)?;
    let digest = copy_data_with_hook(
        info.log_len,
        |buffer, offset| FileExt::read_exact_at(backup, buffer, HEADER_SIZE + offset),
        &mut log,
        || stage_hook(RebuildPoint::DataChunk),
    )?;
    stage_hook(RebuildPoint::DataCopied)?;
    if digest != info.digest {
        return Err(record::invalid_data(
            "backup changed while it was being restored",
        ));
    }
    log.sync_all()?;
    stage_hook(RebuildPoint::LogSynced)?;
    // Read the target back after sync; a checksum of the source alone would
    // not detect corruption introduced while writing the restored file.
    if log.metadata()?.len() != info.log_len
        || hash_data(info.log_len, |buffer, offset| {
            FileExt::read_exact_at(&log, buffer, offset)
        })? != info.digest
    {
        return Err(record::invalid_data("restored log checksum mismatch"));
    }
    // A validated zero-byte legacy backup has no witness. Restore it as an
    // empty witnessed store so normal open cannot confuse it with an
    // interrupted initialization or silently weaken later acknowledgements.
    if info.log_len == 0 {
        initialize_empty_restored_log(&mut log, empty_hook, metadata_witness::initialize)?;
    }
    // Revalidate the restored bytes, including every record checksum.
    let log = Log::verify_io(Box::new(log))?;
    stage_hook(RebuildPoint::LogVerified)?;
    log.install_restore_witness_with_hook(destination, witness_hook)?;
    stage_hook(RebuildPoint::WitnessInstalled)?;
    if info.log_len == 0 {
        empty_hook(EmptyRestorePoint::AfterWitness)?;
    }
    File::open(destination)?.sync_all()?;
    stage_hook(RebuildPoint::DirectorySynced)?;
    if info.log_len == 0 {
        empty_hook(EmptyRestorePoint::AfterDirectorySync)?;
    }
    lock.release()?;
    stage_hook(RebuildPoint::LockReleased)?;
    Ok(())
}

fn initialize_empty_restored_log(
    log: &mut File,
    hook: &mut impl FnMut(EmptyRestorePoint) -> io::Result<()>,
    initialize_metadata: impl FnOnce(&File, &witness::LogId, u64) -> io::Result<bool>,
) -> io::Result<()> {
    let mut header = witness::new_header()?;
    let id = witness::parse_header(&header)?;
    hook(EmptyRestorePoint::BeforeMetadata)?;
    if initialize_metadata(log, &id, witness::LOG_HEADER_SIZE as u64)? {
        metadata_witness::mark_header(&mut header);
    }
    hook(EmptyRestorePoint::AfterMetadata)?;
    log.write_all(&header)?;
    hook(EmptyRestorePoint::AfterHeaderWrite)?;
    log.sync_all()?;
    hook(EmptyRestorePoint::AfterHeaderSync)
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
    let _ = Log::verify_io(Box::new(reader))?;
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

fn copy_data_with_hook(
    length: u64,
    mut read: impl FnMut(&mut [u8], u64) -> io::Result<()>,
    output: &mut File,
    mut hook: impl FnMut() -> io::Result<()>,
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
        hook()?;
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
    path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "a destination name is required",
        )
    })?;
    crate::staging::check_path(path)?;
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
