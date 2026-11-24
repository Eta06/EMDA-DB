//! Bounded-memory, read-only access to a trusted historical checkpoint index.
use crate::checkpoint;
use crate::log::{Log, ValueLocation};
use crate::record;
use crate::{Entry, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io;
use std::ops::ControlFlow;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Weak};

/// Maximum number of index blocks retained in one checkpoint view.
pub const MAX_CHECKPOINT_VIEW_BLOCKS: usize = 131_072;
/// Maximum checkpoint index bytes read into memory for one block.
pub const MAX_CHECKPOINT_VIEW_BLOCK_BYTES: usize = 32 * 1024;
const KEY_PREFIX_BYTES: usize = 16;
// A sparse directory of entry offsets reduces per-lookup decoding work while
// retaining the full block read and SHA-256 verification on every access.
const LANDMARK_INTERVAL: usize = 16;
const MIN_ENTRY_BYTES: usize = checkpoint::ENTRY_FIXED_BYTES as usize + 1;
const MAX_BLOCK_LANDMARKS: usize =
    (MAX_CHECKPOINT_VIEW_BLOCK_BYTES / MIN_ENTRY_BYTES - 1) / LANDMARK_INTERVAL;
/// Largest checkpoint file accepted by the bounded view.
pub const MAX_CHECKPOINT_VIEW_FILE_BYTES: u64 =
    MAX_CHECKPOINT_VIEW_BLOCKS as u64 * MAX_CHECKPOINT_VIEW_BLOCK_BYTES as u64 + 100;

/// Per-view admission limits for checkpoint verification and its block directory.
/// The block and file limits bound the capacities requested for the sparse
/// directory and its packed landmark offsets. Allocator overhead, temporary
/// buffers and other views have separate memory costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointViewLimits {
    /// Maximum verified checkpoint file length in bytes.
    pub max_file_bytes: u64,
    /// Maximum number of retained block-directory entries.
    pub max_blocks: usize,
}

impl Default for CheckpointViewLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: MAX_CHECKPOINT_VIEW_FILE_BYTES,
            max_blocks: MAX_CHECKPOINT_VIEW_BLOCKS,
        }
    }
}

impl CheckpointViewLimits {
    pub(crate) fn validate(self) -> io::Result<()> {
        if self.max_file_bytes > MAX_CHECKPOINT_VIEW_FILE_BYTES
            || self.max_blocks > MAX_CHECKPOINT_VIEW_BLOCKS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint view limits exceed the supported maximum",
            ));
        }
        Ok(())
    }

    fn max_landmarks(self) -> usize {
        // Each recorded offset follows at least LANDMARK_INTERVAL distinct
        // entries. A block's first entry needs no offset.
        let from_bytes = self.max_file_bytes / (MIN_ENTRY_BYTES * LANDMARK_INTERVAL) as u64;
        let from_blocks = self.max_blocks * MAX_BLOCK_LANDMARKS;
        from_bytes.min(from_blocks as u64) as usize
    }
}

fn reserve_directory_slot<T>(
    directory: &mut Vec<T>,
    limit: usize,
    limit_error: impl FnOnce() -> io::Error,
) -> io::Result<()> {
    if directory.len() >= limit {
        return Err(limit_error());
    }
    if directory.len() == directory.capacity() {
        let capacity = directory.capacity().saturating_mul(2).max(4).min(limit);
        directory
            .try_reserve_exact(capacity - directory.len())
            .map_err(|_| io::Error::other("checkpoint block directory allocation failed"))?;
    }
    Ok(())
}

struct Block {
    first_key_prefix: [u8; KEY_PREFIX_BYTES],
    first_key_len: usize,
    offset: u64,
    length: usize,
    digest: [u8; 32],
    landmark_end: u32,
}

impl Block {
    fn compare_prefix(&self, key: &[u8]) -> Option<std::cmp::Ordering> {
        let shared = self.first_key_len.min(key.len()).min(KEY_PREFIX_BYTES);
        let comparison = self.first_key_prefix[..shared].cmp(&key[..shared]);
        if comparison != std::cmp::Ordering::Equal {
            return Some(comparison);
        }
        if self.first_key_len <= KEY_PREFIX_BYTES || key.len() <= KEY_PREFIX_BYTES {
            return Some(self.first_key_len.cmp(&key.len()));
        }
        None
    }
}

struct BlockBuilder {
    first_key_prefix: [u8; KEY_PREFIX_BYTES],
    first_key_len: usize,
    offset: u64,
    length: usize,
    digest: Sha256,
    entries: usize,
}

impl BlockBuilder {
    fn finish(self, landmarks: usize) -> io::Result<Block> {
        Ok(Block {
            first_key_prefix: self.first_key_prefix,
            first_key_len: self.first_key_len,
            offset: self.offset,
            length: self.length,
            digest: self.digest.finalize().into(),
            landmark_end: u32::try_from(landmarks)
                .map_err(|_| record::invalid_data("checkpoint landmark directory overflow"))?,
        })
    }
}

/// A read-only historical view of a verified checkpoint, with a bounded
/// in-memory block directory. It holds a shared advisory store lock and cannot
/// coexist with a cooperating writer. It does not validate or apply log records
/// after the checkpoint boundary, and therefore is not necessarily current.
/// The trusted digest must be stored independently from the checkpoint file.
pub struct CheckpointView {
    pub(crate) log: Arc<Log>,
    index: Arc<CheckpointIndex>,
    prefix: u64,
    log_digest: [u8; 32],
    keys: u64,
    peak: usize,
    trusted_digest: [u8; 32],
    limits: CheckpointViewLimits,
    has_tail: bool,
    pub(crate) witnessed_boundary: u64,
    // Borrowed snapshots rely on their still-live store's lock instead.
    _lock_file: Option<crate::store_lock::StoreLock>,
}

// A snapshot pins the exact file and directory verified together at open.
struct CheckpointIndex {
    file: File,
    blocks: Vec<Block>,
    landmarks: Vec<u16>,
}

pub(crate) struct CheckpointPin(Weak<CheckpointIndex>);

impl CheckpointPin {
    pub(crate) fn is_live(&self) -> bool {
        self.0.strong_count() != 0
    }
}

impl CheckpointIndex {
    fn landmarks(&self, position: usize) -> &[u16] {
        let start = position
            .checked_sub(1)
            .map_or(0, |i| self.blocks[i].landmark_end);
        &self.landmarks[start as usize..self.blocks[position].landmark_end as usize]
    }
}

pub(crate) enum TailAdmission {
    Unrestricted,
    Bounded(u64),
    Witnessed(u64),
}

/// The checkpoint index has been authenticated, but the covered log has
/// not. Only the suffix range is available until prefix verification passes.
pub(crate) struct PreparedCheckpoint {
    base: CheckpointView,
    verified: checkpoint::VerifiedCheckpoint,
    log_length: u64,
}

impl PreparedCheckpoint {
    pub(crate) fn suffix(&self) -> (&Log, std::ops::Range<u64>, u64) {
        (
            &self.base.log,
            self.verified.prefix..self.log_length,
            self.base.witnessed_boundary,
        )
    }

    pub(crate) fn verify(self) -> io::Result<(CheckpointView, Sha256)> {
        let covered_log_hash = checkpoint::verify_log_prefix(&self.base.log, &self.verified)?;
        if self.base.log.len()? != self.log_length {
            return Err(record::invalid_data(
                "log length changed while opening checkpoint view",
            ));
        }
        Ok((self.base, covered_log_hash))
    }

    // Verify the covered bytes on the final writer descriptor, and bind the
    // suffix to the exact bytes that were replayed via the view's descriptor.
    pub(crate) fn verify_for_adoption(
        self,
        writer: &Log,
        suffix_digest: &[u8; 32],
    ) -> io::Result<CheckpointView> {
        writer.require_same_checkpoint_log(&self.base.log)?;
        checkpoint::bind_replayed_log(
            writer,
            self.verified.prefix,
            self.log_length,
            &self.verified.log_digest,
            suffix_digest,
        )?;
        Ok(self.base)
    }
}

impl CheckpointView {
    /// Opens a historical view without materializing the full key index.
    pub fn open(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
    ) -> io::Result<Self> {
        Self::open_with_limits(
            path,
            checkpoint,
            trusted_digest,
            CheckpointViewLimits::default(),
        )
    }

    /// Opens a view with caller-selected checkpoint file and block-count caps.
    /// Covered log bytes are still fully hashed; these are not total memory,
    /// storage or elapsed-time limits. The caller must reapply limits on reopen.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: CheckpointViewLimits,
    ) -> io::Result<Self> {
        Self::open_with_recovery_limits(path, checkpoint, trusted_digest, limits, None)
    }

    /// Opens with an optional whole-log byte limit in addition to the view
    /// limits. An oversized log is rejected before checkpoint verification
    /// hashes its covered prefix. This cap includes any unacknowledged or
    /// later suffix, is reapplied on each open, and is not a time, total
    /// memory or project-disk quota.
    pub fn open_with_recovery_limits(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: CheckpointViewLimits,
        max_log_bytes: Option<u64>,
    ) -> io::Result<Self> {
        Self::prepare_shared(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            limits,
            max_log_bytes,
            TailAdmission::Unrestricted,
        )?
        .verify()
        .map(|(view, _)| view)
    }

    pub(crate) fn prepare_shared(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        limits: CheckpointViewLimits,
        max_log_bytes: Option<u64>,
        tail: TailAdmission,
    ) -> io::Result<PreparedCheckpoint> {
        limits.validate()?;
        let lock_file = File::open(path.join("store.lock"))?;
        fs2::FileExt::try_lock_shared(&lock_file)?;
        let lock_file = crate::store_lock::StoreLock::acquired(lock_file);
        lock_file.verify_identity(path)?;
        Self::prepare_locked(
            path,
            checkpoint,
            trusted_digest,
            lock_file,
            limits,
            (max_log_bytes, None),
            tail,
        )
    }

    // The caller owns a shared or exclusive lock for the entire view lifetime.
    pub(crate) fn open_locked_with_limits(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        lock_file: crate::store_lock::StoreLock,
        limits: CheckpointViewLimits,
    ) -> io::Result<Self> {
        Self::open_locked_with_recovery_limits(
            path,
            checkpoint,
            trusted_digest,
            lock_file,
            limits,
            (None, None),
            TailAdmission::Unrestricted,
        )
        .map(|(view, _)| view)
    }

    pub(crate) fn open_locked_with_recovery_limits(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        lock_file: crate::store_lock::StoreLock,
        limits: CheckpointViewLimits,
        log_limits: (Option<u64>, Option<usize>),
        tail: TailAdmission,
    ) -> io::Result<(Self, Sha256)> {
        Self::prepare_locked(
            path,
            checkpoint,
            trusted_digest,
            lock_file,
            limits,
            log_limits,
            tail,
        )?
        .verify()
    }

    pub(crate) fn prepare_locked(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        lock_file: crate::store_lock::StoreLock,
        limits: CheckpointViewLimits,
        (max_log_bytes, max_live_keys): (Option<u64>, Option<usize>),
        tail: TailAdmission,
    ) -> io::Result<PreparedCheckpoint> {
        limits.validate()?;
        let (mut log, boundary) = Log::open_checkpoint_view(path)?;
        let length = Self::check_log_limit(&log, max_log_bytes)?;
        let max_tail_bytes = match tail {
            TailAdmission::Unrestricted => None,
            TailAdmission::Bounded(limit) => Some(limit),
            TailAdmission::Witnessed(limit) => {
                checkpoint::check_witnessed_length(length, boundary)?;
                Some(limit)
            }
        };
        let checkpoint = checkpoint::open_index_file(checkpoint)?;
        let mut blocks = Vec::new();
        let mut landmarks = Vec::new();
        let max_landmarks = limits.max_landmarks();
        let mut current: Option<BlockBuilder> = None;
        let mut keys = 0_u64;
        let verified = checkpoint::visit_index_file(
            &checkpoint,
            length,
            boundary,
            trusted_digest,
            checkpoint::IndexAdmission {
                max_live_keys,
                max_file_bytes: Some(limits.max_file_bytes),
                max_tail_bytes,
                max_view_blocks: Some(limits.max_blocks),
            },
            |offset, key_len, key, tail, _| {
                let size = key_len.len() + key.len() + tail.len();
                if current
                    .as_ref()
                    .is_some_and(|block| block.length + size > MAX_CHECKPOINT_VIEW_BLOCK_BYTES)
                {
                    blocks.push(
                        current
                            .take()
                            .expect("current block")
                            .finish(landmarks.len())?,
                    );
                }
                if current.is_none() {
                    reserve_directory_slot(
                        &mut blocks,
                        limits.max_blocks,
                        checkpoint::block_limit_error,
                    )?;
                    let mut first_key_prefix = [0; KEY_PREFIX_BYTES];
                    let prefix_len = key.len().min(KEY_PREFIX_BYTES);
                    first_key_prefix[..prefix_len].copy_from_slice(&key[..prefix_len]);
                    current = Some(BlockBuilder {
                        first_key_prefix,
                        first_key_len: key.len(),
                        offset,
                        length: 0,
                        digest: Sha256::new(),
                        entries: 0,
                    });
                }
                let block = current.as_mut().expect("current block");
                if block.entries != 0 && block.entries % LANDMARK_INTERVAL == 0 {
                    reserve_directory_slot(&mut landmarks, max_landmarks, || {
                        record::invalid_data("checkpoint landmark directory exceeds its bounds")
                    })?;
                    landmarks.push(u16::try_from(block.length).map_err(|_| {
                        record::invalid_data("checkpoint landmark offset overflow")
                    })?);
                }
                block.digest.update(key_len);
                block.digest.update(key);
                block.digest.update(tail);
                block.length += size;
                block.entries += 1;
                keys += 1;
                Ok(())
            },
        )?;
        if let Some(block) = current {
            blocks.push(block.finish(landmarks.len())?);
        }
        let length = Self::check_log_limit(&log, max_log_bytes)?;
        if let Some(limit) = max_tail_bytes {
            checkpoint::checked_tail_bytes(length, verified.prefix, limit)?;
        }
        if matches!(tail, TailAdmission::Witnessed(_)) || length < boundary {
            checkpoint::check_witnessed_length(length, boundary)?;
        }
        let has_tail = length > verified.prefix;
        log.release_view_witness()?;
        let view = Self {
            log: Arc::new(log),
            index: Arc::new(CheckpointIndex {
                file: checkpoint,
                blocks,
                landmarks,
            }),
            prefix: verified.prefix,
            log_digest: verified.log_digest,
            keys,
            peak: verified.peak,
            trusted_digest: *trusted_digest,
            limits,
            has_tail,
            witnessed_boundary: boundary,
            _lock_file: Some(lock_file),
        };
        Ok(PreparedCheckpoint {
            base: view,
            verified,
            log_length: length,
        })
    }

    pub(crate) fn check_log_limit(log: &Log, max_log_bytes: Option<u64>) -> io::Result<u64> {
        let length = log.len()?;
        if max_log_bytes.is_some_and(|limit| length > limit) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint view whole-log byte limit exceeded",
            ));
        }
        Ok(length)
    }

    /// Number of live keys in this historical checkpoint.
    pub fn len(&self) -> u64 {
        self.keys
    }
    /// Whether this checkpoint contains no live keys.
    pub fn is_empty(&self) -> bool {
        self.keys == 0
    }
    /// Number of bounded index blocks retained in this view's directory.
    pub fn block_count(&self) -> usize {
        self.index.blocks.len()
    }
    /// Byte boundary covered by this checkpoint in the source log.
    pub fn log_boundary(&self) -> u64 {
        self.prefix
    }

    pub(crate) fn historical_peak(&self) -> usize {
        self.peak
    }

    pub(crate) fn checkpoint_pin(&self) -> CheckpointPin {
        CheckpointPin(Arc::downgrade(&self.index))
    }

    // Share ownership of the exclusive lock through a view handoff.
    pub(crate) fn share_store_lock(&self) -> io::Result<crate::store_lock::StoreLock> {
        self._lock_file
            .clone()
            .ok_or_else(|| io::Error::other("snapshot view does not own the store lock"))
    }

    // The caller's snapshot borrows the open store and thus relies on its
    // exclusive lock; leaking a snapshot must not leak an extra lock descriptor.
    pub(crate) fn for_snapshot(&self) -> Self {
        Self {
            log: Arc::clone(&self.log),
            index: Arc::clone(&self.index),
            prefix: self.prefix,
            log_digest: self.log_digest,
            keys: self.keys,
            peak: self.peak,
            trusted_digest: self.trusted_digest,
            limits: self.limits,
            has_tail: self.has_tail,
            witnessed_boundary: self.witnessed_boundary,
            _lock_file: None,
        }
    }

    // Revalidate the original trusted checkpoint and its covered log bytes
    // before deriving another checkpoint from this long-lived view.
    pub(crate) fn verify_for_rollover(&self) -> io::Result<Sha256> {
        checkpoint::verify_log_prefix(&self.log, &self.verify_checkpoint_file()?)
    }

    // Bounded rebuild separately binds the log to this verified checkpoint
    // and to the suffix bytes consumed during replay.
    pub(crate) fn verify_checkpoint_file(&self) -> io::Result<checkpoint::VerifiedCheckpoint> {
        let mut count = 0_u64;
        let verified = checkpoint::visit_index_file(
            &self.index.file,
            self.log.len()?,
            self.witnessed_boundary,
            &self.trusted_digest,
            checkpoint::IndexAdmission {
                max_file_bytes: Some(self.limits.max_file_bytes),
                max_view_blocks: Some(self.limits.max_blocks),
                ..Default::default()
            },
            |_, _, _, _, _| {
                count += 1;
                Ok(())
            },
        )?;
        if verified.prefix != self.prefix
            || verified.peak != self.peak
            || verified.log_digest != self.log_digest
            || count != self.keys
        {
            return Err(record::invalid_data("checkpoint changed since opening"));
        }
        Ok(verified)
    }

    /// Whether the log contained bytes beyond the checkpoint at open time.
    /// Those bytes may include committed or unacknowledged records and are not read by this view.
    pub fn has_log_tail(&self) -> bool {
        self.has_tail
    }

    fn read_block(&self, block: &Block, bytes: &mut Vec<u8>) -> io::Result<()> {
        bytes.resize(block.length, 0);
        self.index.file.read_exact_at(bytes, block.offset)?;
        if Sha256::digest(bytes.as_slice()).as_slice() != block.digest {
            return Err(record::invalid_data(
                "checkpoint view block checksum mismatch",
            ));
        }
        Ok(())
    }

    /// Reads one key from the historical checkpoint.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_admitted(key, |_| Ok(()))
    }

    pub(crate) fn get_admitted(
        &self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        self.location(key)?
            .map(|location| {
                admit(location.length)?;
                self.log.read(location)
            })
            .transpose()
    }

    pub(crate) fn location(&self, key: &[u8]) -> io::Result<Option<ValueLocation>> {
        self.lookup().location(key)
    }

    pub(crate) fn lookup(&self) -> CheckpointLookup<'_> {
        CheckpointLookup {
            view: self,
            position: None,
            bytes: Vec::new(),
        }
    }

    /// Scans historical keys in bytewise order using exclusive optional bounds.
    /// Each result is capped by `MAX_SCAN_ENTRIES` and `MAX_SCAN_BYTES`.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        self.scan_admitted(after, before, limit, |_, _| true)
    }

    pub(crate) fn scan_admitted(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
        mut admit: impl FnMut(&[u8], usize) -> bool,
    ) -> io::Result<Vec<Entry>> {
        for key in [after, before].into_iter().flatten() {
            record::validate_key(key)?;
        }
        if limit == 0 || limit > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scan limit must be between 1 and 1024",
            ));
        }
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        let mut size = 0_usize;
        self.visit_entries(after, before, |key, location| {
            let bytes = key.len() + location.length;
            if result.len() == limit
                || size + bytes > MAX_SCAN_BYTES
                || !admit(key, location.length)
            {
                return Ok(ControlFlow::Break(()));
            }
            size += bytes;
            result.push((key.to_vec(), self.log.read(location)?));
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(result)
    }

    /// Visit verified checkpoint entries without reading their values.
    /// Each borrowed key remains valid only for the duration of the callback.
    pub(crate) fn visit_entries(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        mut visit: impl FnMut(&[u8], ValueLocation) -> io::Result<ControlFlow<()>>,
    ) -> io::Result<()> {
        let mut lookup = self.lookup();
        let mut after = after;
        let start = match after {
            Some(key) => lookup.preceding_block(key)?.unwrap_or(0),
            None => 0,
        };
        for position in start..self.index.blocks.len() {
            let bytes = lookup.block(position)?;
            // Blocks after the first are strictly above the lower bound.
            let lower = after.take();
            let mut cursor = match lower {
                Some(key) => {
                    BlockCursor::at_key(bytes, self.prefix, self.index.landmarks(position), key)?
                }
                None => BlockCursor::new(bytes, self.prefix),
            };
            while let Some((key, location)) = cursor.next()? {
                if lower.is_some_and(|bound| key <= bound) {
                    continue;
                }
                if before.is_some_and(|bound| key >= bound) {
                    return Ok(());
                }
                if visit(key, location)?.is_break() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

/// One operation's lookup buffer, tied to the verified directory it reads.
pub(crate) struct CheckpointLookup<'a> {
    view: &'a CheckpointView,
    position: Option<usize>,
    bytes: Vec<u8>,
}

impl CheckpointLookup<'_> {
    fn block(&mut self, position: usize) -> io::Result<&[u8]> {
        if self.position != Some(position) {
            self.position = None;
            self.view
                .read_block(&self.view.index.blocks[position], &mut self.bytes)?;
            self.position = Some(position);
        }
        Ok(&self.bytes)
    }

    fn preceding_block(&mut self, key: &[u8]) -> io::Result<Option<usize>> {
        let view = self.view;
        let mut low = 0;
        let mut high = view.index.blocks.len();
        while low < high {
            let middle = low + (high - low) / 2;
            // The directory prefix is verified on open. An inconclusive
            // comparison uses the complete checked block.
            let comparison = match view.index.blocks[middle].compare_prefix(key) {
                Some(comparison) => comparison,
                None => {
                    let mut cursor = BlockCursor::new(self.block(middle)?, view.prefix);
                    let (first, _) = cursor
                        .next()?
                        .ok_or_else(|| record::invalid_data("empty checkpoint index block"))?;
                    first.cmp(key)
                }
            };
            if comparison == std::cmp::Ordering::Greater {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        Ok(low.checked_sub(1))
    }

    pub(crate) fn location(&mut self, key: &[u8]) -> io::Result<Option<ValueLocation>> {
        let Some(position) = self.preceding_block(key)? else {
            return Ok(None);
        };
        let view = self.view;
        let landmarks = view.index.landmarks(position);
        let mut cursor = BlockCursor::at_key(self.block(position)?, view.prefix, landmarks, key)?;
        while let Some((candidate, location)) = cursor.next()? {
            match candidate.cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(location)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }
}

struct BlockCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
    prefix: u64,
}
impl<'a> BlockCursor<'a> {
    fn new(bytes: &'a [u8], prefix: u64) -> Self {
        Self {
            bytes,
            offset: 0,
            prefix,
        }
    }
    fn at(bytes: &'a [u8], prefix: u64, offset: usize) -> Self {
        Self {
            bytes,
            offset,
            prefix,
        }
    }
    // The caller verifies the complete block before using landmark offsets.
    fn at_key(bytes: &'a [u8], prefix: u64, landmarks: &[u16], key: &[u8]) -> io::Result<Self> {
        let mut low = 0;
        let mut high = landmarks.len();
        while low < high {
            let middle = low + (high - low) / 2;
            let (candidate, _) = Self::at(bytes, prefix, usize::from(landmarks[middle]))
                .next()?
                .ok_or_else(|| record::invalid_data("empty checkpoint landmark"))?;
            if candidate <= key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let offset = low
            .checked_sub(1)
            .map_or(0, |index| usize::from(landmarks[index]));
        Ok(Self::at(bytes, prefix, offset))
    }

    fn next(&mut self) -> io::Result<Option<(&'a [u8], ValueLocation)>> {
        if self.offset == self.bytes.len() {
            return Ok(None);
        }
        let key_header_end = self
            .offset
            .checked_add(2)
            .ok_or_else(|| record::invalid_data("checkpoint block offset overflow"))?;
        let key_len = u16::from_le_bytes(
            self.bytes
                .get(self.offset..key_header_end)
                .ok_or_else(|| record::invalid_data("truncated checkpoint block"))?
                .try_into()
                .expect("two bytes"),
        ) as usize;
        if key_len == 0 || key_len > 1024 {
            return Err(record::invalid_data("invalid checkpoint block key length"));
        }
        let key_end = key_header_end
            .checked_add(key_len)
            .ok_or_else(|| record::invalid_data("checkpoint block offset overflow"))?;
        let end = key_end
            .checked_add(16)
            .ok_or_else(|| record::invalid_data("checkpoint block offset overflow"))?;
        let key = self
            .bytes
            .get(key_header_end..key_end)
            .ok_or_else(|| record::invalid_data("truncated checkpoint block"))?;
        let tail = self
            .bytes
            .get(key_end..end)
            .ok_or_else(|| record::invalid_data("truncated checkpoint block"))?;
        let offset = u64::from_le_bytes(tail[..8].try_into().expect("eight bytes"));
        let length = u32::from_le_bytes(tail[8..12].try_into().expect("four bytes")) as usize;
        let checksum = u32::from_le_bytes(tail[12..].try_into().expect("four bytes"));
        if length > record::MAX_VALUE
            || offset < crate::witness::LOG_HEADER_SIZE as u64
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > self.prefix)
        {
            return Err(record::invalid_data(
                "checkpoint block location outside log prefix",
            ));
        }
        self.offset = end;
        Ok(Some((
            key,
            ValueLocation {
                offset,
                length,
                checksum,
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_landmarks_preserve_lookups_and_avoid_overreserving_blocks() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("index");
        let keys = (0_u16..145)
            .map(|number| {
                let mut key = vec![0; 1_000];
                key[..2].copy_from_slice(&number.to_be_bytes());
                key
            })
            .collect::<Vec<_>>();
        let operations = keys
            .iter()
            .map(|key| crate::BatchOperation::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        let store = crate::Store::open(&path).unwrap();
        store.write_batch(&operations).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        store.close_private_store().unwrap();
        let limits = CheckpointViewLimits {
            max_file_bytes: std::fs::metadata(&checkpoint).unwrap().len(),
            max_blocks: 5,
        };
        let view = CheckpointView::open_with_limits(&path, &checkpoint, &digest, limits).unwrap();
        assert_eq!(view.block_count(), 5);
        assert!(view.index.blocks.capacity() <= limits.max_blocks);
        assert_eq!(view.index.landmarks, vec![16 * 1_018; 5]);
        assert!(view.index.landmarks.capacity() <= limits.max_landmarks());
        for key in &keys {
            assert_eq!(view.get(key).unwrap().as_deref(), Some(b"v".as_slice()));
        }
        let scan = view.scan(Some(&keys[30]), Some(&keys[66]), 100).unwrap();
        assert_eq!(
            scan.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
            keys[31..66]
        );
    }

    #[test]
    fn snapshots_share_the_verified_file_and_directory_until_the_last_drop() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("index");
        let store = crate::Store::open(&path).unwrap();
        store.put(b"key", b"original").unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);
        let base =
            crate::test_support::retry_lock(|| CheckpointView::open(&path, &checkpoint, &digest))
                .unwrap();
        let first = base.for_snapshot();
        let second = base.for_snapshot();
        assert!(Arc::ptr_eq(&first.index, &base.index));
        assert!(Arc::ptr_eq(&second.index, &base.index));
        assert!(first._lock_file.is_none() && second._lock_file.is_none());
        let index = Arc::downgrade(&base.index);
        std::fs::remove_file(&checkpoint).unwrap();
        assert_eq!(
            first.get(b"key").unwrap().as_deref(),
            Some(&b"original"[..])
        );
        drop(first);
        drop(second);
        assert!(index.upgrade().is_some());
        drop(base);
        assert!(index.upgrade().is_none());
    }

    #[test]
    fn prefix_order_is_exact_or_requires_full_key_comparison() {
        let mut keys = Vec::new();
        for len in 1..=40 {
            for fill in [0_u8, 42, 255] {
                let key = vec![fill; len];
                keys.push(key.clone());
                for position in [0, len / 2, len - 1] {
                    let mut different = key.clone();
                    different[position] = fill.wrapping_add(1);
                    keys.push(different);
                }
            }
        }
        for first in &keys {
            let mut prefix = [0_u8; KEY_PREFIX_BYTES];
            let len = first.len().min(KEY_PREFIX_BYTES);
            prefix[..len].copy_from_slice(&first[..len]);
            let block = Block {
                first_key_prefix: prefix,
                first_key_len: first.len(),
                offset: 0,
                length: 0,
                digest: [0; 32],
                landmark_end: 0,
            };
            for key in &keys {
                match block.compare_prefix(key) {
                    Some(ordering) => assert_eq!(ordering, first.cmp(key)),
                    None => {
                        assert!(first.len() > KEY_PREFIX_BYTES && key.len() > KEY_PREFIX_BYTES);
                        assert_eq!(&first[..KEY_PREFIX_BYTES], &key[..KEY_PREFIX_BYTES]);
                    }
                }
            }
        }
    }
}
