//! Verified separate-copy compaction from a checkpoint-backed index.
use super::compact::GroupedCopy;
use super::{Log, Store, backup, record};
use crate::checkpoint_view::CheckpointViewLimits;
use crate::recovered_view::RecoveredCheckpointView;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::ControlFlow;
use std::path::Path;

/// Name of the checkpoint created inside a checkpoint-store compacted copy.
pub const COMPACTED_CHECKPOINT_FILE: &str = "index.checkpoint";

// A private staging lock is explicitly released, including on ordinary error
// paths, so a duplicated descriptor cannot delay publication or cleanup.
struct PrivateLock {
    file: File,
    active: bool,
}

impl PrivateLock {
    fn acquire(directory: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
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

impl Drop for PrivateLock {
    fn drop(&mut self) {
        if self.active {
            let _ = fs2::FileExt::unlock(&self.file);
        }
    }
}

fn hash_entry(hash: &mut Sha256, key: &[u8], value_len: usize, value_digest: [u8; 32]) {
    hash.update((key.len() as u64).to_le_bytes());
    hash.update(key);
    hash.update((value_len as u64).to_le_bytes());
    hash.update(value_digest);
}

#[derive(Default)]
pub(crate) struct CheckpointCopyOptions {
    pub(crate) checkpoint_limits: Option<CheckpointViewLimits>,
    pub(crate) source_value_read_bytes: Option<u64>,
    pub(crate) target_log_bytes: Option<u64>,
}

impl CheckpointCopyOptions {
    pub(crate) fn charge_source_reads(&mut self, bytes: u64) -> io::Result<()> {
        charge(
            &mut self.source_value_read_bytes,
            bytes,
            "checkpoint compaction source log read byte limit exceeded",
        )
    }

    pub(crate) fn charge_target_log(&mut self, bytes: u64) -> io::Result<()> {
        charge(
            &mut self.target_log_bytes,
            bytes,
            "checkpoint compaction target log byte limit exceeded",
        )
    }
}

fn charge(remaining: &mut Option<u64>, bytes: u64, message: &'static str) -> io::Result<()> {
    if let Some(remaining) = remaining {
        *remaining = remaining
            .checked_sub(bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    }
    Ok(())
}

fn copy_live(
    source: &Log,
    view: &RecoveredCheckpointView,
    target: &Log,
    options: &mut CheckpointCopyOptions,
    reads: &mut crate::store::source_read_budget::SourceReadBudget,
) -> io::Result<(u64, [u8; 32])> {
    let mut copy = GroupedCopy::new(target, None)?;
    let mut count = 0_u64;
    let mut hash = Sha256::new();
    view.visit_entries(None, None, |key, location| {
        options.charge_source_reads(location.length as u64)?;
        reads.charge(location.length as u64)?;
        options.charge_target_log(
            (record::HEADER_SIZE + key.len() + location.length + record::CHECKSUM_SIZE) as u64,
        )?;
        let mut value = Sha256::new();
        copy.put_log_value(key, source, location, |chunk| value.update(chunk))?;
        hash_entry(&mut hash, key, location.length, value.finalize().into());
        count = count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("compacted key count overflow"))?;
        Ok(ControlFlow::Continue(()))
    })?;
    copy.finish()?;
    Ok((count, hash.finalize().into()))
}

pub(crate) fn compact_checkpoint(
    source: &Log,
    view: &RecoveredCheckpointView,
    count: u64,
    target: (&Path, CheckpointCopyOptions),
) -> io::Result<[u8; 32]> {
    compact_checkpoint_with_hooks(
        source,
        view,
        count,
        target,
        crate::directory::sync_existing,
        || Ok(()),
        |_| Ok(()),
    )
}

pub(crate) fn compact_checkpoint_with_hooks(
    source: &Log,
    view: &RecoveredCheckpointView,
    expected_count: u64,
    target_path: (&Path, CheckpointCopyOptions),
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
    after_copy: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<[u8; 32]> {
    compact_checkpoint_copy(
        source,
        view,
        expected_count,
        target_path,
        (sync_parent, before_publish, after_copy),
        true,
        &mut crate::store::source_read_budget::SourceReadBudget(None),
    )
}

/// Copies a captured view while the source may still accept commits. A
/// concurrent commit can advance the source witness, so the caller must
/// verify it later while excluding writers. `reads` is charged each live
/// value's length before that value is read.
pub(crate) fn compact_checkpoint_concurrent(
    source: &Log,
    view: &RecoveredCheckpointView,
    expected_count: u64,
    target_path: (&Path, CheckpointCopyOptions),
    before_publish: impl FnOnce() -> io::Result<()>,
    reads: &mut crate::store::source_read_budget::SourceReadBudget,
) -> io::Result<[u8; 32]> {
    compact_checkpoint_copy(
        source,
        view,
        expected_count,
        target_path,
        (
            crate::directory::sync_existing,
            before_publish,
            |_: &Path| Ok(()),
        ),
        false,
        reads,
    )
}

fn compact_checkpoint_copy(
    source: &Log,
    view: &RecoveredCheckpointView,
    expected_count: u64,
    target_path: (&Path, CheckpointCopyOptions),
    (sync_parent, before_publish, after_copy): (
        impl FnOnce(&Path) -> io::Result<()>,
        impl FnOnce() -> io::Result<()>,
        impl FnOnce(&Path) -> io::Result<()>,
    ),
    verify_source_witness: bool,
    reads: &mut crate::store::source_read_budget::SourceReadBudget,
) -> io::Result<[u8; 32]> {
    let (destination, mut options) = target_path;
    let parent = backup::parent_directory(destination)?;
    let mut staging = crate::staging::Stage::directory(parent)?;
    let target = Store::open(staging.path())?;
    let copied = copy_live(source, view, &target.log, &mut options, reads);
    let closed = target.close_private_store();
    let (copied_count, source_digest) = copied?;
    closed?;
    if copied_count != expected_count {
        return Err(record::invalid_data("compacted source key count mismatch"));
    }
    after_copy(staging.path())?;

    let mut lock = PrivateLock::acquire(staging.path())?;
    let (target, boundary) = Log::open_checkpoint_view(staging.path())?;
    let mut previous = Vec::new();
    let mut target_count = 0_u64;
    let mut target_hash = Sha256::new();
    let mut log_hasher = Sha256::new();
    target.verify_private_puts(boundary, Some(&mut log_hasher), |key, len, digest| {
        if !previous.is_empty() && previous.as_slice() >= key {
            return Err(record::invalid_data(
                "compacted keys are not strictly ordered",
            ));
        }
        hash_entry(&mut target_hash, key, len, digest);
        target_count = target_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("compacted key count overflow"))?;
        key.clone_into(&mut previous);
        Ok(())
    })?;
    let target_digest: [u8; 32] = target_hash.finalize().into();
    if target_count != expected_count || target_digest != source_digest {
        return Err(record::invalid_data("compacted data verification mismatch"));
    }
    let peak = usize::try_from(target_count)
        .map_err(|_| io::Error::other("compacted key count overflow"))?;
    let digest = crate::checkpoint::create_streaming_in_private_stage(
        &target,
        target_count,
        peak,
        staging.path(),
        crate::checkpoint::StreamingOptions {
            verified_log: crate::checkpoint::VerifiedLog::from_replay(boundary, log_hasher),
            limits: options.checkpoint_limits,
            tracker: None,
            pending_tail: None,
        },
        |emit| {
            target.visit_records(0, Some(boundary), |key, location| {
                let location = location
                    .ok_or_else(|| record::invalid_data("compacted log contains a deletion"))?;
                emit(&key, location)
            })
        },
    )?;
    target.verify_witness()?;
    if verify_source_witness {
        source.verify_witness()?;
    }
    drop(target);
    lock.release()?;
    File::open(staging.path())?.sync_all()?;
    before_publish()?;
    staging.publish(destination)?;
    sync_parent(parent).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("compacted store may exist but directory synchronization failed: {error}"),
        )
    })?;
    staging.publication_synchronized();
    Ok(digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_verification_lock_releases_even_with_duplicate_descriptors() {
        let directory = tempfile::tempdir().unwrap();
        Store::open(directory.path())
            .unwrap()
            .close_private_store()
            .unwrap();
        let mut lock = PrivateLock::acquire(directory.path()).unwrap();
        let duplicate = lock.file.try_clone().unwrap();
        assert!(Store::open(directory.path()).is_err());
        lock.release().unwrap();
        let reopened = Store::open(directory.path()).unwrap();
        drop(duplicate);
        reopened.close_private_store().unwrap();

        let lock = PrivateLock::acquire(directory.path()).unwrap();
        let duplicate = lock.file.try_clone().unwrap();
        drop(lock);
        let reopened = Store::open(directory.path()).unwrap();
        drop(duplicate);
        reopened.close_private_store().unwrap();
    }
}
