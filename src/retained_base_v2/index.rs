//! Sparse in-memory directory over the validated, immutable base file.
use super::{
    CHUNK_BYTES, Limits, MAX_KEY_BYTES, Validated, add, entry_lengths, invalid, scan,
    validate_value_budget,
};
use crate::{Entry, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES};
use sha2::{Digest, Sha256};
use std::{fs::File, io, os::unix::fs::FileExt};

// Whole entries are kept in one block, so an entry larger than this occupies
// its own block. It is still read through the fixed chunk buffer.
const BLOCK_BYTES: u64 = 128 * 1024;

/// Limits for the in-memory directory of a read-only retained base.
/// Key bytes count only block anchors. Vector capacity and allocator
/// overhead are separate, and queries have their own result limits.
#[derive(Clone, Copy, Debug)]
pub struct IndexLimits {
    pub blocks: usize,
    pub anchor_bytes: usize,
}

struct Block {
    first_key: Vec<u8>,
    start: u64,
    end: u64,
    digest: [u8; 32],
}

pub(super) struct Builder {
    limits: IndexLimits,
    blocks: Vec<Block>,
    anchor_bytes: usize,
    hash: Sha256,
}

impl Builder {
    pub(super) fn entry(
        &mut self,
        start: u64,
        bytes: u64,
        key: &[u8],
        lengths: &[u8; 8],
    ) -> io::Result<()> {
        let end = add(start, bytes)?;
        if self
            .blocks
            .last()
            .is_none_or(|block| end - block.start > BLOCK_BYTES)
        {
            self.finish_block();
            self.anchor_bytes = self
                .anchor_bytes
                .checked_add(key.len())
                .filter(|&sum| sum <= self.limits.anchor_bytes)
                .ok_or_else(|| invalid("retained base anchor byte limit exceeded"))?;
            if self.blocks.len() >= self.limits.blocks {
                return Err(invalid("retained base block limit exceeded"));
            }
            let mut first_key = Vec::new();
            first_key.try_reserve_exact(key.len()).map_err(allocation)?;
            first_key.extend_from_slice(key);
            self.blocks.try_reserve(1).map_err(allocation)?;
            self.blocks.push(Block {
                first_key,
                start,
                end,
                digest: [0; 32],
            });
        } else {
            self.blocks.last_mut().unwrap().end = end;
        }
        self.hash.update(lengths);
        self.hash.update(key);
        Ok(())
    }

    pub(super) fn value_chunk(&mut self, bytes: &[u8]) {
        self.hash.update(bytes);
    }

    fn finish_block(&mut self) {
        if let Some(block) = self.blocks.last_mut() {
            block.digest = std::mem::take(&mut self.hash).finalize().into();
        }
    }
}

fn allocation(_: std::collections::TryReserveError) -> io::Error {
    io::Error::other("retained base allocation failed")
}

pub(crate) struct IndexedBase {
    file: File,
    blocks: Vec<Block>,
    metadata: Validated,
}

enum Selection {
    Skip,
    Take,
    Stop,
}

impl IndexedBase {
    pub(crate) fn open(file: File, limits: Limits, index_limits: IndexLimits) -> io::Result<Self> {
        let mut builder = Builder {
            limits: index_limits,
            blocks: Vec::new(),
            anchor_bytes: 0,
            hash: Sha256::new(),
        };
        let (metadata, _) = scan(&file, limits, None, None, Some(&mut builder))?;
        builder.finish_block();
        Ok(Self {
            file,
            blocks: builder.blocks,
            metadata,
        })
    }

    pub(crate) fn metadata(&self) -> &Validated {
        &self.metadata
    }

    pub(crate) fn get(&self, key: &[u8], max_value_bytes: usize) -> io::Result<Option<Vec<u8>>> {
        crate::record::validate_key(key)?;
        validate_value_budget(max_value_bytes)?;
        self.check_length()?;
        let Some(position) = self.preceding_block(key) else {
            return Ok(None);
        };
        let mut result = Vec::new();
        let mut found = false;
        self.read_block(&self.blocks[position], &mut result, |candidate, len| {
            if found {
                return Ok(Selection::Stop);
            }
            match candidate.cmp(key) {
                std::cmp::Ordering::Less => Ok(Selection::Skip),
                std::cmp::Ordering::Greater => Ok(Selection::Stop),
                std::cmp::Ordering::Equal if len <= max_value_bytes => {
                    found = true;
                    Ok(Selection::Take)
                }
                std::cmp::Ordering::Equal => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "value budget exceeded",
                )),
            }
        })?;
        Ok(result.pop().map(|(_, value)| value))
    }

    pub(crate) fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        for key in [after, before].into_iter().flatten() {
            crate::record::validate_key(key)?;
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
        self.check_length()?;
        let start = after.and_then(|key| self.preceding_block(key)).unwrap_or(0);
        let mut result = Vec::new();
        let mut count = 0;
        let mut bytes = 0;
        for block in &self.blocks[start..] {
            if before.is_some_and(|key| block.first_key.as_slice() >= key) {
                break;
            }
            let stopped = self.read_block(block, &mut result, |key, len| {
                if after.is_some_and(|bound| key <= bound) {
                    return Ok(Selection::Skip);
                }
                if before.is_some_and(|bound| key >= bound)
                    || count == limit
                    || bytes + key.len() + len > MAX_SCAN_BYTES
                {
                    return Ok(Selection::Stop);
                }
                count += 1;
                bytes += key.len() + len;
                Ok(Selection::Take)
            })?;
            if stopped || count == limit {
                break;
            }
        }
        Ok(result)
    }

    fn check_length(&self) -> io::Result<()> {
        if self.file.metadata()?.len() != self.metadata.file_bytes {
            return Err(invalid("retained base file length changed"));
        }
        Ok(())
    }

    fn preceding_block(&self, key: &[u8]) -> Option<usize> {
        self.blocks
            .partition_point(|block| block.first_key.as_slice() <= key)
            .checked_sub(1)
    }

    fn read_block(
        &self,
        block: &Block,
        result: &mut Vec<Entry>,
        mut select: impl FnMut(&[u8], usize) -> io::Result<Selection>,
    ) -> io::Result<bool> {
        let mut reader = BlockReader {
            file: &self.file,
            offset: block.start,
            end: block.end,
            hash: Sha256::new(),
        };
        let mut key = [0u8; MAX_KEY_BYTES];
        let mut buffer = [0u8; CHUNK_BYTES];
        let mut stopped = false;
        while reader.offset < reader.end {
            let mut lengths = [0u8; 8];
            reader.read(&mut lengths)?;
            let (kl, vl) = entry_lengths(lengths)?;
            if (kl + vl) as u64 > reader.end - reader.offset {
                return Err(invalid("retained base entry exceeds block"));
            }
            let key = &mut key[..kl];
            reader.read(key)?;
            let selected = if stopped {
                false
            } else {
                match select(key, vl)? {
                    Selection::Skip => false,
                    Selection::Take => true,
                    Selection::Stop => {
                        stopped = true;
                        false
                    }
                }
            };
            let mut value = Vec::new();
            if selected {
                value.try_reserve_exact(vl).map_err(allocation)?;
            }
            let mut remaining = vl;
            while remaining != 0 {
                let bytes = remaining.min(buffer.len());
                reader.read(&mut buffer[..bytes])?;
                if selected {
                    value.extend_from_slice(&buffer[..bytes]);
                }
                remaining -= bytes;
            }
            if selected {
                let mut owned_key = Vec::new();
                owned_key.try_reserve_exact(kl).map_err(allocation)?;
                owned_key.extend_from_slice(key);
                result.try_reserve(1).map_err(allocation)?;
                result.push((owned_key, value));
            }
        }
        let digest: [u8; 32] = reader.hash.finalize().into();
        if digest != block.digest {
            return Err(invalid("retained base block digest mismatch"));
        }
        Ok(stopped)
    }
}

struct BlockReader<'a> {
    file: &'a File,
    offset: u64,
    end: u64,
    hash: Sha256,
}

impl BlockReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        let end = add(self.offset, bytes.len() as u64)?;
        if end > self.end {
            return Err(invalid("retained base block read exceeds boundary"));
        }
        self.file.read_exact_at(bytes, self.offset)?;
        self.hash.update(&*bytes);
        self.offset = end;
        Ok(())
    }
}
