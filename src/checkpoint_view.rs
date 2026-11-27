//! Bounded-memory, read-only access to a trusted historical checkpoint index.
use crate::checkpoint;
use crate::log::{Log, ValueLocation};
use crate::record;
use crate::{Entry, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io;
use std::ops::ControlFlow;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Maximum number of index blocks retained in one checkpoint view.
pub const MAX_CHECKPOINT_VIEW_BLOCKS: usize = 131_072;
/// Maximum checkpoint index bytes read into memory for one block.
pub const MAX_CHECKPOINT_VIEW_BLOCK_BYTES: usize = 32 * 1024;
// Store up to 256 bytes of each block's first key in a packed directory array
// so binary search resolves long shared prefixes (such as document and index
// key namespaces) without reading intermediate checkpoint blocks. Keys sharing
// a longer prefix still fall back to verifying the complete block.
const MAX_FIRST_KEY_PREFIX_BYTES: usize = 256;
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

    fn max_first_key_bytes(self) -> usize {
        let from_blocks = self.max_blocks.saturating_mul(MAX_FIRST_KEY_PREFIX_BYTES);
        let from_file = usize::try_from(self.max_file_bytes).unwrap_or(usize::MAX);
        from_blocks.min(from_file)
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

fn reserve_directory_bytes(
    buffer: &mut Vec<u8>,
    added: usize,
    limit: usize,
    limit_error: impl FnOnce() -> io::Error,
) -> io::Result<()> {
    let needed = buffer
        .len()
        .checked_add(added)
        .filter(|&total| total <= limit)
        .ok_or_else(limit_error)?;
    if needed > buffer.capacity() {
        let capacity = buffer
            .capacity()
            .saturating_mul(2)
            .max(needed)
            .max(64)
            .min(limit);
        buffer
            .try_reserve_exact(capacity - buffer.len())
            .map_err(|_| io::Error::other("checkpoint block directory allocation failed"))?;
    }
    Ok(())
}

struct Block {
    first_key_end: u32,
    first_key_len: u16,
    offset: u64,
    length: usize,
    digest: [u8; 32],
    landmark_end: u32,
}

fn compare_key_prefix(
    prefix: &[u8],
    first_key_len: usize,
    key: &[u8],
) -> Option<std::cmp::Ordering> {
    let shared = prefix.len().min(key.len());
    let comparison = prefix[..shared].cmp(&key[..shared]);
    if comparison != std::cmp::Ordering::Equal {
        return Some(comparison);
    }
    if first_key_len <= prefix.len() || key.len() <= prefix.len() {
        return Some(first_key_len.cmp(&key.len()));
    }
    None
}

struct BlockBuilder {
    first_key_end: u32,
    first_key_len: u16,
    offset: u64,
    length: usize,
    digest: Sha256,
    entries: usize,
}

impl BlockBuilder {
    fn finish(self, landmarks: usize) -> io::Result<Block> {
        Ok(Block {
            first_key_end: self.first_key_end,
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
    first_keys: Vec<u8>,
    landmarks: Vec<u16>,
    read_budget: Option<checkpoint::CheckpointReadBudget>,
    cache: BlockCache,
}

/// Usage of a checkpoint's verified-block cache.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CheckpointBlockCacheStatus {
    /// Inclusive limit on cached block bytes; zero disables the cache.
    pub limit_bytes: usize,
    /// Bytes of verified blocks currently cached.
    pub cached_bytes: usize,
    /// Blocks currently cached.
    pub cached_blocks: usize,
    /// Lookups served from the cache.
    pub hits: u64,
    /// Lookups that read and verified a block from the file.
    pub misses: u64,
}

#[derive(Default)]
struct CacheState {
    limit: usize,
    used: usize,
    tick: u64,
    blocks: HashMap<usize, (Arc<[u8]>, u64)>,
    order: BTreeMap<u64, usize>,
    hits: u64,
    misses: u64,
}

// A least-recently-used cache of verified block bytes. The checkpoint file is
// immutable once its directory is verified, so cached bytes stay correct.
#[derive(Default)]
struct BlockCache {
    // Fast-path mirror of `state.limit` so a disabled cache costs one atomic
    // load. Every mutation checks `state.limit` under `state` to stay exact.
    limit: AtomicUsize,
    state: Mutex<CacheState>,
}

impl BlockCache {
    fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, CacheState>> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("checkpoint block cache poisoned"))
    }

    fn get(&self, position: usize) -> io::Result<Option<Arc<[u8]>>> {
        if self.limit.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }
        let mut state = self.lock()?;
        if state.limit == 0 {
            return Ok(None);
        }
        let current_tick = state.tick;
        let Some((bytes, used)) = state.blocks.get_mut(&position) else {
            state.misses += 1;
            return Ok(None);
        };
        let bytes = Arc::clone(bytes);
        let previous = *used;
        if previous != current_tick {
            let next_tick = current_tick + 1;
            *used = next_tick;
            state.tick = next_tick;
            state.order.remove(&previous);
            state.order.insert(next_tick, position);
        }
        state.hits += 1;
        Ok(Some(bytes))
    }

    fn insert(&self, position: usize, bytes: Arc<[u8]>) -> io::Result<()> {
        if self.limit.load(Ordering::Acquire) < bytes.len() {
            return Ok(());
        }
        let mut state = self.lock()?;
        // Recheck the authoritative limit under the lock so a concurrent
        // limit reduction or rollover invalidation can never be bypassed.
        let limit = state.limit;
        if bytes.len() > limit {
            return Ok(());
        }
        if state.blocks.contains_key(&position) {
            return Ok(());
        }
        while state.used + bytes.len() > limit {
            let Some((_, evicted)) = state.order.pop_first() else {
                break;
            };
            if let Some((old, _)) = state.blocks.remove(&evicted) {
                state.used -= old.len();
            }
        }
        state.tick += 1;
        let tick = state.tick;
        state.used += bytes.len();
        state.order.insert(tick, position);
        state.blocks.insert(position, (bytes, tick));
        Ok(())
    }

    fn set_limit(&self, limit: usize) -> io::Result<()> {
        let mut state = self.lock()?;
        state.limit = limit;
        self.limit.store(limit, Ordering::Release);
        while state.used > limit {
            let Some((_, evicted)) = state.order.pop_first() else {
                break;
            };
            if let Some((old, _)) = state.blocks.remove(&evicted) {
                state.used -= old.len();
            }
        }
        Ok(())
    }

    fn status(&self) -> io::Result<CheckpointBlockCacheStatus> {
        let state = self.lock()?;
        Ok(CheckpointBlockCacheStatus {
            limit_bytes: state.limit,
            cached_bytes: state.used,
            cached_blocks: state.blocks.len(),
            hits: state.hits,
            misses: state.misses,
        })
    }
}

pub(crate) struct CheckpointPin(Weak<CheckpointIndex>);

impl CheckpointPin {
    pub(crate) fn is_live(&self) -> bool {
        self.0.strong_count() != 0
    }
}

impl CheckpointIndex {
    fn first_key_prefix(&self, position: usize) -> &[u8] {
        let start = position
            .checked_sub(1)
            .map_or(0, |i| self.blocks[i].first_key_end as usize);
        &self.first_keys[start..self.blocks[position].first_key_end as usize]
    }

    fn compare_first_key_prefix(&self, position: usize, key: &[u8]) -> Option<std::cmp::Ordering> {
        compare_key_prefix(
            self.first_key_prefix(position),
            usize::from(self.blocks[position].first_key_len),
            key,
        )
    }

    fn landmarks(&self, position: usize) -> &[u16] {
        let start = position
            .checked_sub(1)
            .map_or(0, |i| self.blocks[i].landmark_end);
        &self.landmarks[start as usize..self.blocks[position].landmark_end as usize]
    }
}

pub(crate) enum TailAdmission {
    Historical(Option<u64>),
    Writable {
        max_tail_bytes: u64,
        recovery_scan: checkpoint::RecoveryScanLimits,
    },
    Witnessed {
        max_tail_bytes: u64,
        max_total_scan_bytes: Option<u64>,
    },
}

/// The checkpoint index has been authenticated, but the covered log has
/// not. Only the suffix range is available until prefix verification passes.
pub(crate) struct PreparedCheckpoint {
    base: CheckpointView,
    verified: checkpoint::VerifiedCheckpoint,
    log_length: u64,
}

impl PreparedCheckpoint {
    pub(crate) fn live_key_bytes(&self) -> u64 {
        self.verified.key_bytes
    }

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

    /// The covered log prefix length and its digest from the checkpoint.
    pub(crate) fn covered_prefix(&self) -> (u64, [u8; 32]) {
        (self.base.prefix, self.base.log_digest)
    }

    /// Returns the view without hashing the covered prefix. Each value read
    /// still checks the checksum the authenticated checkpoint records for it.
    /// The caller records that the prefix digest remains to be verified.
    pub(crate) fn defer_prefix(self) -> io::Result<CheckpointView> {
        if self.base.log.len()? != self.log_length {
            return Err(record::invalid_data(
                "log length changed while opening checkpoint view",
            ));
        }
        Ok(self.base)
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
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            limits,
            max_log_bytes,
            None,
        )
    }

    /// Bound the combined logical scan of the checkpoint and its covered log
    /// prefix. The optional whole-log length cap applies separately. An
    /// oversized scan is rejected before traversing index entries. Metadata,
    /// header probes, physical I/O retries and later reads are separate.
    pub fn open_with_total_recovery_scan_limit(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
        limits: CheckpointViewLimits,
        max_log_bytes: Option<u64>,
        max_total_scan_bytes: u64,
    ) -> io::Result<Self> {
        Self::open_using(
            path.as_ref(),
            checkpoint.as_ref(),
            trusted_digest,
            limits,
            max_log_bytes,
            Some(max_total_scan_bytes),
        )
    }

    fn open_using(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        limits: CheckpointViewLimits,
        max_log_bytes: Option<u64>,
        max_total_scan_bytes: Option<u64>,
    ) -> io::Result<Self> {
        Self::prepare_shared(
            path,
            checkpoint,
            trusted_digest,
            limits,
            max_log_bytes,
            TailAdmission::Historical(max_total_scan_bytes),
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
        Self::prepare_locked(
            path,
            checkpoint,
            trusted_digest,
            lock_file,
            limits,
            (None, None),
            TailAdmission::Historical(None),
        )?
        .verify()
        .map(|(view, _)| view)
    }

    pub(crate) fn prepare_locked(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        lock_file: crate::store_lock::StoreLock,
        limits: CheckpointViewLimits,
        log_limits: (Option<u64>, Option<usize>),
        tail: TailAdmission,
    ) -> io::Result<PreparedCheckpoint> {
        Self::prepare_locked_with_read_limit(
            path,
            checkpoint,
            trusted_digest,
            lock_file,
            (limits, None),
            log_limits,
            tail,
        )
    }

    pub(crate) fn prepare_locked_with_read_limit(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        lock_file: crate::store_lock::StoreLock,
        (limits, max_checkpoint_read_bytes): (CheckpointViewLimits, Option<u64>),
        (max_log_bytes, max_live_keys): (Option<u64>, Option<usize>),
        tail: TailAdmission,
    ) -> io::Result<PreparedCheckpoint> {
        limits.validate()?;
        let (mut log, boundary) = Log::open_checkpoint_view(path)?;
        let length = Self::check_log_limit(&log, max_log_bytes)?;
        let (max_tail_bytes, recovery_scan) = match tail {
            TailAdmission::Historical(total_bytes) => (
                None,
                checkpoint::RecoveryScanLimits {
                    total_bytes,
                    prefix_only: true,
                    ..Default::default()
                },
            ),
            TailAdmission::Writable {
                max_tail_bytes,
                recovery_scan,
            } => (Some(max_tail_bytes), recovery_scan),
            TailAdmission::Witnessed {
                max_tail_bytes,
                max_total_scan_bytes,
            } => {
                checkpoint::check_witnessed_length(length, boundary)?;
                (
                    Some(max_tail_bytes),
                    checkpoint::RecoveryScanLimits {
                        total_bytes: max_total_scan_bytes,
                        ..Default::default()
                    },
                )
            }
        };
        let checkpoint = checkpoint::open_index_file(checkpoint)?;
        let read_budget = max_checkpoint_read_bytes.map(checkpoint::CheckpointReadBudget::new);
        let mut blocks = Vec::new();
        let mut first_keys = Vec::new();
        let mut landmarks = Vec::new();
        let max_landmarks = limits.max_landmarks();
        let max_first_key_bytes = limits.max_first_key_bytes();
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
                recovery_scan,
                max_view_blocks: Some(limits.max_blocks),
                read_budget: read_budget.as_ref(),
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
                    let prefix_len = key.len().min(MAX_FIRST_KEY_PREFIX_BYTES);
                    reserve_directory_bytes(
                        &mut first_keys,
                        prefix_len,
                        max_first_key_bytes,
                        || {
                            record::invalid_data(
                                "checkpoint block prefix directory exceeds its bounds",
                            )
                        },
                    )?;
                    first_keys.extend_from_slice(&key[..prefix_len]);
                    let first_key_end = u32::try_from(first_keys.len()).map_err(|_| {
                        record::invalid_data("checkpoint block prefix directory overflow")
                    })?;
                    let first_key_len = u16::try_from(key.len()).map_err(|_| {
                        record::invalid_data("checkpoint block key length overflow")
                    })?;
                    current = Some(BlockBuilder {
                        first_key_end,
                        first_key_len,
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
        if matches!(tail, TailAdmission::Witnessed { .. }) || length < boundary {
            checkpoint::check_witnessed_length(length, boundary)?;
        }
        recovery_scan.admit(length, verified.prefix, boundary, verified.file_bytes)?;
        let has_tail = length > verified.prefix;
        log.release_view_witness()?;
        let view = Self {
            log: Arc::new(log),
            index: Arc::new(CheckpointIndex {
                file: checkpoint,
                blocks,
                first_keys,
                landmarks,
                read_budget,
                cache: BlockCache::default(),
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
    /// Verifies the checkpoint entries and log prefix for rollover, first
    /// admitting `scan` against this view's pinned checkpoint descriptor and
    /// its log length after reading only the checkpoint header.
    ///
    /// `witnessed` replaces the boundary captured at open when the caller has
    /// just verified the witness for the current log length while excluding
    /// writers. Acknowledged appends since open are then charged as one pass.
    pub(crate) fn verify_for_rollover_admitted(
        &self,
        scan: checkpoint::RecoveryScanLimits,
        witnessed: Option<u64>,
        visit_entry: impl FnMut(&[u8]),
    ) -> io::Result<Sha256> {
        let verified = self.verify_checkpoint_entries_admitted(scan, witnessed, visit_entry)?;
        checkpoint::verify_log_prefix(&self.log, &verified)
    }

    // Bounded rebuild separately binds the log to this verified checkpoint
    // and to the suffix bytes consumed during replay.
    pub(crate) fn verify_checkpoint_file(&self) -> io::Result<checkpoint::VerifiedCheckpoint> {
        self.verify_checkpoint_entries(|_| {})
    }

    fn verify_checkpoint_entries(
        &self,
        visit_entry: impl FnMut(&[u8]),
    ) -> io::Result<checkpoint::VerifiedCheckpoint> {
        self.verify_checkpoint_entries_admitted(
            checkpoint::RecoveryScanLimits::default(),
            None,
            visit_entry,
        )
    }

    fn verify_checkpoint_entries_admitted(
        &self,
        scan: checkpoint::RecoveryScanLimits,
        witnessed: Option<u64>,
        mut visit_entry: impl FnMut(&[u8]),
    ) -> io::Result<checkpoint::VerifiedCheckpoint> {
        // A freshly witnessed boundary is never below the one captured at
        // open; otherwise keep the captured boundary and its checks.
        let witnessed_boundary = witnessed
            .filter(|boundary| *boundary >= self.witnessed_boundary)
            .unwrap_or(self.witnessed_boundary);
        let mut count = 0_u64;
        let verified = checkpoint::visit_index_file(
            &self.index.file,
            self.log.len()?,
            witnessed_boundary,
            &self.trusted_digest,
            checkpoint::IndexAdmission {
                max_file_bytes: Some(self.limits.max_file_bytes),
                max_view_blocks: Some(self.limits.max_blocks),
                read_budget: self.index.read_budget.as_ref(),
                recovery_scan: scan,
                ..Default::default()
            },
            |_, _, key, _, _| {
                count += 1;
                visit_entry(key);
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

    /// Bytes held by the retained block directory, packed first-key prefixes,
    /// and landmark array.
    pub(crate) fn directory_bytes(&self) -> usize {
        self.index.blocks.capacity() * std::mem::size_of::<Block>()
            + self.index.first_keys.capacity()
            + self.index.landmarks.capacity() * std::mem::size_of::<u16>()
    }

    /// Sets the verified-block cache limit for this checkpoint, shared by
    /// every view of it. Lowering it evicts least recently used blocks.
    pub(crate) fn set_block_cache_limit(&self, limit: usize) -> io::Result<()> {
        self.index.cache.set_limit(limit)
    }

    pub(crate) fn block_cache_status(&self) -> io::Result<CheckpointBlockCacheStatus> {
        self.index.cache.status()
    }

    fn read_block(&self, block: &Block, bytes: &mut Vec<u8>) -> io::Result<()> {
        if let Some(budget) = &self.index.read_budget {
            budget.charge(block.length as u64)?;
        }
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
            shared: None,
        }
    }

    pub(crate) fn with_point_session<T>(
        &self,
        action: impl FnOnce(&mut CheckpointViewPointSession<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut session = CheckpointViewPointSession {
            lookup: self.lookup(),
            read_ahead: crate::log::LogReadAhead::new(&self.log),
        };
        action(&mut session)
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
        let mut read_ahead = crate::log::LogReadAhead::new(&self.log);
        self.visit_entries_with_peek(after, before, |key, location, peek| {
            let bytes = key.len() + location.length;
            if result.len() == limit
                || size + bytes > MAX_SCAN_BYTES
                || !admit(key, location.length)
            {
                return Ok(ControlFlow::Break(()));
            }
            let remaining = limit - result.len() - 1;
            let value = read_ahead.read_with_peek(location, |push| {
                let mut count = 0;
                peek(&mut |next| {
                    if count >= remaining {
                        return false;
                    }
                    count += 1;
                    push(next)
                });
            })?;
            size += bytes;
            result.push((key.to_vec(), value));
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
        self.visit_entries_with_peek(after, before, |key, location, _| visit(key, location))
    }

    /// Visit verified checkpoint entries while allowing the callback to peek
    /// upcoming value locations in the already-verified current block for
    /// bounded log read-ahead. Peeking performs no I/O or read-budget charges.
    pub(crate) fn visit_entries_with_peek(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        mut visit: impl FnMut(
            &[u8],
            ValueLocation,
            crate::log::LocationPeek<'_>,
        ) -> io::Result<ControlFlow<()>>,
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
                let mut peek = |push: &mut dyn FnMut(ValueLocation) -> bool| {
                    let mut ahead = cursor.clone();
                    while let Ok(Some((next_key, next_location))) = ahead.next() {
                        if before.is_some_and(|bound| next_key >= bound) {
                            break;
                        }
                        if !push(next_location) {
                            break;
                        }
                    }
                };
                if visit(key, location, &mut peek)?.is_break() {
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
    // A cached block, used instead of `bytes` when set.
    shared: Option<Arc<[u8]>>,
}

impl CheckpointLookup<'_> {
    fn block(&mut self, position: usize) -> io::Result<&[u8]> {
        if self.position != Some(position) {
            self.position = None;
            self.shared = None;
            let block = &self.view.index.blocks[position];
            let cache = &self.view.index.cache;
            if let Some(bytes) = cache.get(position)? {
                // A hit reads nothing from the file, but logical read
                // admission is unchanged.
                if let Some(budget) = &self.view.index.read_budget {
                    budget.charge(block.length as u64)?;
                }
                self.shared = Some(bytes);
            } else {
                self.view.read_block(block, &mut self.bytes)?;
                if cache.limit.load(Ordering::Acquire) != 0 {
                    cache.insert(position, Arc::from(self.bytes.as_slice()))?;
                }
            }
            self.position = Some(position);
        }
        Ok(self.shared.as_deref().unwrap_or(&self.bytes))
    }

    fn preceding_block(&mut self, key: &[u8]) -> io::Result<Option<usize>> {
        let view = self.view;
        let mut low = 0;
        let mut high = view.index.blocks.len();
        while low < high {
            let middle = low + (high - low) / 2;
            // The directory prefix is verified on open. An inconclusive
            // comparison uses the complete checked block.
            let comparison = match view.index.compare_first_key_prefix(middle, key) {
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

    /// Searches only the block already loaded and verified in this lookup
    /// buffer, performing no I/O, cache access, or read-budget accounting.
    pub(crate) fn location_if_loaded(&self, key: &[u8]) -> Option<ValueLocation> {
        let position = self.position?;
        if self.view.index.compare_first_key_prefix(position, key)
            == Some(std::cmp::Ordering::Greater)
        {
            return None;
        }
        if position + 1 < self.view.index.blocks.len()
            && matches!(
                self.view.index.compare_first_key_prefix(position + 1, key),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            )
        {
            return None;
        }
        let bytes = self.shared.as_deref().unwrap_or(&self.bytes);
        let landmarks = self.view.index.landmarks(position);
        let mut cursor = BlockCursor::at_key(bytes, self.view.prefix, landmarks, key).ok()?;
        while let Some((candidate, location)) = cursor.next().ok()? {
            match candidate.cmp(key) {
                std::cmp::Ordering::Equal => return Some(location),
                std::cmp::Ordering::Greater => return None,
                std::cmp::Ordering::Less => {}
            }
        }
        None
    }
}

pub(crate) struct CheckpointViewPointSession<'a> {
    lookup: CheckpointLookup<'a>,
    read_ahead: crate::log::LogReadAhead<'a>,
}

impl CheckpointViewPointSession<'_> {
    pub(crate) fn get_admitted_with_peek(
        &mut self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
        peek_keys: impl FnOnce(&mut dyn FnMut(&[u8]) -> bool),
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let Some(location) = self.lookup.location(key)? else {
            return Ok(None);
        };
        admit(location.length)?;
        let value = self.read_ahead.read_with_peek(location, |push| {
            peek_keys(&mut |next_key| {
                if let Some(next_loc) = self.lookup.location_if_loaded(next_key) {
                    push(next_loc)
                } else {
                    true
                }
            });
        })?;
        Ok(Some(value))
    }
}

#[derive(Clone)]
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
        assert!(view.index.first_keys.capacity() <= limits.max_first_key_bytes());
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
        for len in (1..=40).chain(250..=270) {
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
            let prefix = &first[..first.len().min(MAX_FIRST_KEY_PREFIX_BYTES)];
            for key in &keys {
                match compare_key_prefix(prefix, first.len(), key) {
                    Some(ordering) => assert_eq!(ordering, first.cmp(key)),
                    None => {
                        assert!(
                            first.len() > MAX_FIRST_KEY_PREFIX_BYTES
                                && key.len() > MAX_FIRST_KEY_PREFIX_BYTES
                        );
                        assert_eq!(
                            &first[..MAX_FIRST_KEY_PREFIX_BYTES],
                            &key[..MAX_FIRST_KEY_PREFIX_BYTES]
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn shared_prefix_keys_over_16_bytes_avoid_intermediate_block_reads() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("index");
        // Document-style 32-byte shared prefix across many blocks.
        let shared = b"\0emda-document\x01D\0\x05items-collection";
        let keys = (0_u32..3_000)
            .map(|n| {
                let mut key = shared.to_vec();
                key.extend_from_slice(format!("{n:08}").as_bytes());
                key
            })
            .collect::<Vec<_>>();
        let store = crate::Store::open(&path).unwrap();
        for chunk in keys.chunks(500) {
            let ops = chunk
                .iter()
                .map(|key| crate::BatchOperation::Put { key, value: b"ok" })
                .collect::<Vec<_>>();
            store.write_batch(&ops).unwrap();
        }
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        store.close_private_store().unwrap();

        let view = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
        assert!(view.block_count() >= 4, "need multi-block checkpoint");
        // Limit=1 counts misses without storing blocks in the cache.
        view.set_block_cache_limit(1).unwrap();
        assert_eq!(view.get(&keys[1_500]).unwrap().as_deref(), Some(&b"ok"[..]));
        let status = view.block_cache_status().unwrap();
        assert_eq!(
            status.misses, 1,
            "32-byte shared prefix must resolve target block with zero intermediate reads: {status:?}"
        );
    }

    #[test]
    fn shared_prefix_keys_over_256_bytes_fall_back_to_verified_blocks_accurately() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("index");
        let shared = vec![b'p'; MAX_FIRST_KEY_PREFIX_BYTES + 44];
        let keys = (0_u32..260)
            .map(|n| {
                let mut key = shared.clone();
                key.extend_from_slice(format!("-{n:05}").as_bytes());
                key
            })
            .collect::<Vec<_>>();
        let store = crate::Store::open(&path).unwrap();
        let ops = keys
            .iter()
            .map(|key| crate::BatchOperation::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        store.write_batch(&ops).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        store.close_private_store().unwrap();

        let view = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
        assert!(view.block_count() >= 3);
        for key in &keys {
            assert_eq!(view.get(key).unwrap().as_deref(), Some(&b"v"[..]));
        }
        let mut missing_mid = shared.clone();
        missing_mid.extend_from_slice(b"-00120x");
        assert_eq!(view.get(&missing_mid).unwrap(), None);
        let scanned = view.scan(Some(&keys[10]), Some(&keys[200]), 300).unwrap();
        assert_eq!(
            scanned.into_iter().map(|(k, _)| k).collect::<Vec<_>>(),
            keys[11..200]
        );
    }
}
