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
use std::sync::Arc;

/// Maximum number of index blocks retained in one checkpoint view.
pub const MAX_CHECKPOINT_VIEW_BLOCKS: usize = 131_072;
/// Maximum checkpoint index bytes read into memory for one block.
pub const MAX_CHECKPOINT_VIEW_BLOCK_BYTES: usize = 32 * 1024;
const KEY_PREFIX_BYTES: usize = 16;
/// Largest checkpoint file accepted by the bounded view.
pub const MAX_CHECKPOINT_VIEW_FILE_BYTES: u64 =
    MAX_CHECKPOINT_VIEW_BLOCKS as u64 * MAX_CHECKPOINT_VIEW_BLOCK_BYTES as u64 + 100;

/// Per-view admission limits for checkpoint verification and its block directory.
/// These logical limits are not a total process-memory or disk quota.
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
}

#[derive(Clone)]
struct Block {
    first_key_prefix: [u8; KEY_PREFIX_BYTES],
    first_key_len: usize,
    offset: u64,
    length: usize,
    digest: [u8; 32],
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
}

impl BlockBuilder {
    fn finish(self) -> Block {
        Block {
            first_key_prefix: self.first_key_prefix,
            first_key_len: self.first_key_len,
            offset: self.offset,
            length: self.length,
            digest: self.digest.finalize().into(),
        }
    }
}

/// A read-only historical view of a verified checkpoint, with a bounded
/// in-memory block directory. It holds a shared advisory store lock and cannot
/// coexist with a cooperating writer. It does not validate or apply log records
/// after the checkpoint boundary, and therefore is not necessarily current.
/// The trusted digest must be stored independently from the checkpoint file.
pub struct CheckpointView {
    pub(crate) log: Arc<Log>,
    checkpoint: File,
    blocks: Vec<Block>,
    prefix: u64,
    keys: u64,
    peak: usize,
    trusted_digest: [u8; 32],
    limits: CheckpointViewLimits,
    has_tail: bool,
    pub(crate) witnessed_boundary: u64,
    // Borrowed snapshots rely on their still-live store's lock instead.
    _lock_file: Option<File>,
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
        limits.validate()?;
        let path = path.as_ref();
        let lock_file = File::open(path.join("store.lock"))?;
        fs2::FileExt::try_lock_shared(&lock_file)?;
        crate::directory::verify_lock_identity(path, &lock_file)?;
        Self::open_locked_with_recovery_limits(
            path,
            checkpoint.as_ref(),
            trusted_digest,
            lock_file,
            limits,
            max_log_bytes,
        )
    }

    // The caller owns a shared or exclusive lock for the entire view lifetime.
    pub(crate) fn open_locked(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        lock_file: File,
    ) -> io::Result<Self> {
        Self::open_locked_with_limits(
            path,
            checkpoint,
            trusted_digest,
            lock_file,
            CheckpointViewLimits::default(),
        )
    }

    pub(crate) fn open_locked_with_limits(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        lock_file: File,
        limits: CheckpointViewLimits,
    ) -> io::Result<Self> {
        Self::open_locked_with_recovery_limits(
            path,
            checkpoint,
            trusted_digest,
            lock_file,
            limits,
            None,
        )
    }

    fn open_locked_with_recovery_limits(
        path: &Path,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        lock_file: File,
        limits: CheckpointViewLimits,
        max_log_bytes: Option<u64>,
    ) -> io::Result<Self> {
        limits.validate()?;
        let (log, boundary) = Log::open_checkpoint_view(path)?;
        Self::check_log_limit(&log, max_log_bytes)?;
        let checkpoint = File::open(checkpoint)?;
        let mut blocks = Vec::new();
        let mut current: Option<BlockBuilder> = None;
        let mut keys = 0_u64;
        let (prefix, peak) = checkpoint::visit_file(
            &checkpoint,
            &log,
            boundary,
            None,
            trusted_digest,
            Some(limits.max_file_bytes),
            |offset, key_len, key, tail, _| {
                let size = key_len.len() + key.len() + tail.len();
                if current
                    .as_ref()
                    .is_some_and(|block| block.length + size > MAX_CHECKPOINT_VIEW_BLOCK_BYTES)
                {
                    blocks.push(current.take().expect("current block").finish());
                }
                if current.is_none() {
                    if blocks.len() >= limits.max_blocks {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "checkpoint has too many view blocks",
                        ));
                    }
                    let mut first_key_prefix = [0; KEY_PREFIX_BYTES];
                    let prefix_len = key.len().min(KEY_PREFIX_BYTES);
                    first_key_prefix[..prefix_len].copy_from_slice(&key[..prefix_len]);
                    current = Some(BlockBuilder {
                        first_key_prefix,
                        first_key_len: key.len(),
                        offset,
                        length: 0,
                        digest: Sha256::new(),
                    });
                }
                let block = current.as_mut().expect("current block");
                block.digest.update(key_len);
                block.digest.update(key);
                block.digest.update(tail);
                block.length += size;
                keys += 1;
                Ok(())
            },
        )?;
        if let Some(block) = current {
            blocks.push(block.finish());
        }
        let has_tail = Self::check_log_limit(&log, max_log_bytes)? > prefix;
        Ok(Self {
            log: Arc::new(log),
            checkpoint,
            blocks,
            prefix,
            keys,
            peak,
            trusted_digest: *trusted_digest,
            limits,
            has_tail,
            witnessed_boundary: boundary,
            _lock_file: Some(lock_file),
        })
    }

    fn check_log_limit(log: &Log, max_log_bytes: Option<u64>) -> io::Result<u64> {
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
        self.blocks.len()
    }
    /// Byte boundary covered by this checkpoint in the source log.
    pub fn log_boundary(&self) -> u64 {
        self.prefix
    }

    pub(crate) fn historical_peak(&self) -> usize {
        self.peak
    }

    // Duplicating the same open file description preserves the exclusive
    // advisory lock across a checkpoint-store view handoff.
    pub(crate) fn duplicate_store_lock(&self) -> io::Result<File> {
        self._lock_file
            .as_ref()
            .ok_or_else(|| io::Error::other("snapshot view does not own the store lock"))?
            .try_clone()
    }

    // The caller's snapshot borrows the open store and thus relies on its
    // exclusive lock; leaking a snapshot must not leak an extra lock descriptor.
    pub(crate) fn duplicate_for_snapshot(&self) -> io::Result<Self> {
        Ok(Self {
            log: Arc::clone(&self.log),
            checkpoint: self.checkpoint.try_clone()?,
            blocks: self.blocks.clone(),
            prefix: self.prefix,
            keys: self.keys,
            peak: self.peak,
            trusted_digest: self.trusted_digest,
            limits: self.limits,
            has_tail: self.has_tail,
            witnessed_boundary: self.witnessed_boundary,
            _lock_file: None,
        })
    }

    // Revalidate the original trusted checkpoint and its covered log bytes
    // before deriving another checkpoint from this long-lived view.
    pub(crate) fn verify_for_rollover(&self) -> io::Result<()> {
        let mut count = 0_u64;
        let (prefix, peak) = checkpoint::visit_file(
            &self.checkpoint,
            &self.log,
            self.witnessed_boundary,
            None,
            &self.trusted_digest,
            Some(self.limits.max_file_bytes),
            |_, _, _, _, _| {
                count += 1;
                Ok(())
            },
        )?;
        if prefix != self.prefix || peak != self.peak || count != self.keys {
            return Err(record::invalid_data("checkpoint changed since opening"));
        }
        Ok(())
    }
    /// Whether the log contained bytes beyond the checkpoint at open time.
    /// Those bytes may include committed or unacknowledged records and are not read by this view.
    pub fn has_log_tail(&self) -> bool {
        self.has_tail
    }

    fn read_block(&self, block: &Block) -> io::Result<Vec<u8>> {
        let mut bytes = vec![0_u8; block.length];
        self.checkpoint.read_exact_at(&mut bytes, block.offset)?;
        if Sha256::digest(&bytes).as_slice() != block.digest {
            return Err(record::invalid_data(
                "checkpoint view block checksum mismatch",
            ));
        }
        Ok(bytes)
    }

    // Prefixes suffice for most comparisons. When both keys share a complete
    // prefix, verify and inspect the on-disk block before choosing a branch.
    // The directory is derived from the checkpoint only after its trusted
    // digest and covered log prefix have been checked.
    fn compare_first_key(&self, block: &Block, key: &[u8]) -> io::Result<std::cmp::Ordering> {
        if let Some(comparison) = block.compare_prefix(key) {
            return Ok(comparison);
        }
        let bytes = self.read_block(block)?;
        let mut cursor = BlockCursor::new(&bytes, self.prefix);
        let (first, _) = cursor
            .next()?
            .ok_or_else(|| record::invalid_data("empty checkpoint index block"))?;
        Ok(first.cmp(key))
    }

    fn preceding_block(&self, key: &[u8]) -> io::Result<Option<usize>> {
        let mut low = 0;
        let mut high = self.blocks.len();
        while low < high {
            let middle = low + (high - low) / 2;
            if self.compare_first_key(&self.blocks[middle], key)? != std::cmp::Ordering::Greater {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low.checked_sub(1))
    }

    /// Reads one key from the historical checkpoint.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        self.location(key)?
            .map(|location| self.log.read(location))
            .transpose()
    }

    pub(crate) fn location(&self, key: &[u8]) -> io::Result<Option<ValueLocation>> {
        let Some(position) = self.preceding_block(key)? else {
            return Ok(None);
        };
        let bytes = self.read_block(&self.blocks[position])?;
        let mut cursor = BlockCursor::new(&bytes, self.prefix);
        while let Some((candidate, location)) = cursor.next()? {
            match candidate.cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(location)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }

    /// Scans historical keys in bytewise order using exclusive optional bounds.
    /// Each result is capped by `MAX_SCAN_ENTRIES` and `MAX_SCAN_BYTES`.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
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
            if result.len() == limit || size + key.len() + location.length > MAX_SCAN_BYTES {
                return Ok(ControlFlow::Break(()));
            }
            size += key.len() + location.length;
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
        let start = match after {
            Some(key) => self.preceding_block(key)?.unwrap_or(0),
            None => 0,
        };
        for block in self.blocks.iter().skip(start) {
            let bytes = self.read_block(block)?;
            let mut cursor = BlockCursor::new(&bytes, self.prefix);
            while let Some((key, location)) = cursor.next()? {
                if after.is_some_and(|bound| key <= bound) {
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
