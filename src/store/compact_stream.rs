//! Verified separate-copy compaction from a checkpoint-backed index.
use super::compact::{
    CopyRecord, MAX_COPY_BYTES, MAX_COPY_RECORDS, close_private_store, flush_copy,
};
use super::{Log, Operation, Store, backup, record};
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

fn hash_entry(hash: &mut Sha256, key: &[u8], value: &[u8]) {
    hash.update((key.len() as u64).to_le_bytes());
    hash.update(key);
    hash.update((value.len() as u64).to_le_bytes());
    hash.update(value);
}

fn copy_live(
    source: &Log,
    view: &RecoveredCheckpointView,
    target: &Log,
) -> io::Result<(u64, [u8; 32])> {
    let mut pending = Vec::with_capacity(MAX_COPY_RECORDS);
    let mut bytes = 0_usize;
    let mut count = 0_u64;
    let mut hash = Sha256::new();
    view.visit_entries(None, None, |key, location| {
        let length = record::HEADER_SIZE + key.len() + location.length + record::CHECKSUM_SIZE;
        if !pending.is_empty()
            && (pending.len() == MAX_COPY_RECORDS || bytes + length > MAX_COPY_BYTES)
        {
            flush_copy(target, &mut pending)?;
            bytes = 0;
        }
        let value = source.read(location)?;
        hash_entry(&mut hash, key, &value);
        let encoded = record::encode(Operation::Put, key, &value)?;
        bytes += encoded.len();
        pending.push(CopyRecord {
            encoded,
            key_len: key.len(),
            value_len: value.len(),
            checksum: crc32fast::hash(&value),
        });
        count = count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("compacted key count overflow"))?;
        Ok(ControlFlow::Continue(()))
    })?;
    flush_copy(target, &mut pending)?;
    Ok((count, hash.finalize().into()))
}

pub(crate) fn compact_checkpoint(
    source: &Log,
    view: &RecoveredCheckpointView,
    count: u64,
    destination: &Path,
) -> io::Result<[u8; 32]> {
    compact_checkpoint_with_hooks(
        source,
        view,
        count,
        destination,
        crate::directory::sync_existing,
        || Ok(()),
        |_| Ok(()),
    )
}

pub(crate) fn compact_checkpoint_with_hooks(
    source: &Log,
    view: &RecoveredCheckpointView,
    expected_count: u64,
    destination: &Path,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    before_publish: impl FnOnce() -> io::Result<()>,
    after_copy: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<[u8; 32]> {
    let parent = backup::parent_directory(destination)?;
    let mut staging = crate::staging::Stage::directory(parent)?;
    let target = Store::open(staging.path())?;
    let copied = copy_live(source, view, &target.log);
    let closed = close_private_store(target);
    let (copied_count, source_digest) = copied?;
    closed?;
    if copied_count != expected_count {
        return Err(record::invalid_data("compacted source key count mismatch"));
    }
    after_copy(staging.path())?;

    let mut lock = PrivateLock::acquire(staging.path())?;
    let (target, boundary) = Log::open_checkpoint_view(staging.path())?;
    if target.len()? != boundary {
        return Err(record::invalid_data(
            "compacted log has an unacknowledged tail",
        ));
    }
    target.verify_witness()?;
    let mut previous = Vec::new();
    let mut target_count = 0_u64;
    let mut target_hash = Sha256::new();
    target.visit_records(0, Some(boundary), |key, location| {
        let location =
            location.ok_or_else(|| record::invalid_data("compacted log contains a deletion"))?;
        if !previous.is_empty() && previous >= key {
            return Err(record::invalid_data(
                "compacted keys are not strictly ordered",
            ));
        }
        let value = target.read(location)?;
        hash_entry(&mut target_hash, &key, &value);
        target_count = target_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("compacted key count overflow"))?;
        previous = key;
        Ok(())
    })?;
    let target_digest: [u8; 32] = target_hash.finalize().into();
    if target_count != expected_count || target_digest != source_digest {
        return Err(record::invalid_data("compacted data verification mismatch"));
    }
    target.verify_witness()?;
    let peak = usize::try_from(target_count)
        .map_err(|_| io::Error::other("compacted key count overflow"))?;
    let digest = crate::checkpoint::create_streaming_in_private_stage(
        &target,
        target_count,
        peak,
        staging.path(),
        |emit| {
            target.visit_records(0, Some(boundary), |key, location| {
                let location = location
                    .ok_or_else(|| record::invalid_data("compacted log contains a deletion"))?;
                emit(&key, location)
            })
        },
    )?;
    target.verify_witness()?;
    source.verify_witness()?;
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
    Ok(digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_verification_lock_releases_even_with_duplicate_descriptors() {
        let directory = tempfile::tempdir().unwrap();
        close_private_store(Store::open(directory.path()).unwrap()).unwrap();
        let mut lock = PrivateLock::acquire(directory.path()).unwrap();
        let duplicate = lock.file.try_clone().unwrap();
        assert!(Store::open(directory.path()).is_err());
        lock.release().unwrap();
        let reopened = Store::open(directory.path()).unwrap();
        drop(duplicate);
        close_private_store(reopened).unwrap();

        let lock = PrivateLock::acquire(directory.path()).unwrap();
        let duplicate = lock.file.try_clone().unwrap();
        drop(lock);
        let reopened = Store::open(directory.path()).unwrap();
        drop(duplicate);
        close_private_store(reopened).unwrap();
    }
}
