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

/// Maximum number of index blocks retained in one checkpoint view.
pub const MAX_CHECKPOINT_VIEW_BLOCKS: usize = 131_072;
/// Maximum checkpoint index bytes read into memory for one block.
pub const MAX_CHECKPOINT_VIEW_BLOCK_BYTES: usize = 32 * 1024;
const KEY_PREFIX_BYTES: usize = 16;
const MAX_CHECKPOINT_VIEW_FILE_BYTES: u64 =
    MAX_CHECKPOINT_VIEW_BLOCKS as u64 * MAX_CHECKPOINT_VIEW_BLOCK_BYTES as u64 + 100;

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
    pub(crate) log: Log,
    checkpoint: File,
    blocks: Vec<Block>,
    prefix: u64,
    keys: u64,
    has_tail: bool,
    pub(crate) witnessed_boundary: u64,
    _lock_file: File,
}

impl CheckpointView {
    /// Opens a historical view without materializing the full key index.
    pub fn open(
        path: impl AsRef<Path>,
        checkpoint: impl AsRef<Path>,
        trusted_digest: &[u8; 32],
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let lock_file = File::open(path.join("store.lock"))?;
        fs2::FileExt::try_lock_shared(&lock_file)?;
        let (log, boundary) = Log::open_checkpoint_view(path)?;
        let checkpoint = File::open(checkpoint)?;
        if checkpoint.metadata()?.len() > MAX_CHECKPOINT_VIEW_FILE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint exceeds bounded view size",
            ));
        }
        let mut blocks = Vec::new();
        let mut current: Option<BlockBuilder> = None;
        let mut keys = 0_u64;
        let (prefix, _) = checkpoint::visit_file(
            &checkpoint,
            &log,
            boundary,
            None,
            trusted_digest,
            |offset, key_len, key, tail, _| {
                let size = key_len.len() + key.len() + tail.len();
                if current
                    .as_ref()
                    .is_some_and(|block| block.length + size > MAX_CHECKPOINT_VIEW_BLOCK_BYTES)
                {
                    blocks.push(current.take().expect("current block").finish());
                }
                if current.is_none() {
                    if blocks.len() >= MAX_CHECKPOINT_VIEW_BLOCKS {
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
        let has_tail = log.len()? > prefix;
        Ok(Self {
            log,
            checkpoint,
            blocks,
            prefix,
            keys,
            has_tail,
            witnessed_boundary: boundary,
            _lock_file: lock_file,
        })
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
        let Some(position) = self.preceding_block(key)? else {
            return Ok(None);
        };
        let bytes = self.read_block(&self.blocks[position])?;
        let mut cursor = BlockCursor::new(&bytes, self.prefix);
        while let Some((candidate, location)) = cursor.next()? {
            match candidate.cmp(key) {
                std::cmp::Ordering::Equal => return self.log.read(location).map(Some),
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
