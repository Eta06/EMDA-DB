//! Sparse in-memory directory over the validated, immutable base file.
use super::{
    CHUNK_BYTES, Limits, MAX_KEY_BYTES, Validated, add, entry_lengths, invalid, scan_buffered,
    validate_value_budget,
};
use crate::{Entry, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Seek},
    os::unix::fs::FileExt,
};

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

pub(crate) enum EntryPart<'a> {
    Header { key: &'a [u8], value_len: usize },
    Value(&'a [u8]),
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
        let metadata = scan_buffered(&file, limits, Some(&mut builder))?;
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

    pub(crate) fn verify(&self, limits: Limits) -> io::Result<()> {
        let mut file = &self.file;
        file.rewind()?;
        if scan_buffered(file, limits, None)? != self.metadata {
            return Err(invalid("retained base changed"));
        }
        Ok(())
    }

    pub(crate) fn visit_keys(&self, mut visit: impl FnMut(&[u8])) -> io::Result<()> {
        self.check_length()?;
        for block in &self.blocks {
            self.visit_block(block, |key, _| {
                visit(key);
                Ok(())
            })?;
        }
        Ok(())
    }

    // Visits complete blocks in order. Callers must keep side effects private
    // until every block has authenticated, including those hidden by tombstones.
    pub(crate) fn visit_merged<'a>(
        &self,
        overlay: impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)>,
        mut visit: impl FnMut(EntryPart<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.check_length()?;
        let mut overlay = overlay.peekable();
        for block in &self.blocks {
            self.visit_block(block, |key, value| {
                while overlay.peek().is_some_and(|&(other, _)| other <= key) {
                    let (other, replacement) = overlay.next().unwrap();
                    emit_value(other, replacement, &mut visit)?;
                    if other == key {
                        return Ok(());
                    }
                }
                visit(EntryPart::Header {
                    key,
                    value_len: value.remaining,
                })?;
                value.read_chunks(|bytes| visit(EntryPart::Value(bytes)))
            })?;
        }
        for (key, value) in overlay {
            emit_value(key, value, &mut visit)?;
        }
        Ok(())
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
        self.read_block(&self.blocks[position], &mut result, |candidate, len, _| {
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
        self.scan_merged(after, before, limit, std::iter::empty())
    }

    // The overlay supplies sorted distinct keys already restricted to the
    // requested bounds. None marks a deletion.
    pub(crate) fn scan_merged<'a>(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
        overlay: impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)>,
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
        let mut budget = PageBudget {
            bytes: 0,
            limit,
            exhausted: false,
        };
        let mut overlay = overlay.peekable();
        for block in &self.blocks[start..] {
            if before.is_some_and(|key| block.first_key.as_slice() >= key) {
                break;
            }
            let stopped = self.read_block(block, &mut result, |key, len, result| {
                if after.is_some_and(|bound| key <= bound) {
                    return Ok(Selection::Skip);
                }
                if before.is_some_and(|bound| key >= bound) {
                    return Ok(Selection::Stop);
                }
                while overlay.peek().is_some_and(|&(other, _)| other <= key) {
                    let (other, value) = overlay.next().unwrap();
                    if !budget.push_overlay(result, other, value)? {
                        return Ok(Selection::Stop);
                    }
                    if other == key {
                        return Ok(Selection::Skip);
                    }
                }
                if budget.admit(result.len(), key, len) {
                    Ok(Selection::Take)
                } else {
                    Ok(Selection::Stop)
                }
            })?;
            if stopped || result.len() == limit {
                break;
            }
        }
        if !budget.exhausted {
            for (key, value) in overlay {
                if !budget.push_overlay(&mut result, key, value)? {
                    break;
                }
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
        mut select: impl FnMut(&[u8], usize, &mut Vec<Entry>) -> io::Result<Selection>,
    ) -> io::Result<bool> {
        let mut stopped = false;
        self.visit_block(block, |key, value| {
            if !stopped {
                match select(key, value.remaining, result)? {
                    Selection::Skip => {}
                    Selection::Stop => stopped = true,
                    Selection::Take => {
                        let mut owned = Vec::new();
                        owned
                            .try_reserve_exact(value.remaining)
                            .map_err(allocation)?;
                        value.read_chunks(|bytes| {
                            owned.extend_from_slice(bytes);
                            Ok(())
                        })?;
                        push_entry(result, key, owned)?;
                    }
                }
            }
            Ok(())
        })?;
        Ok(stopped)
    }

    fn visit_block(
        &self,
        block: &Block,
        mut visit: impl FnMut(&[u8], &mut BlockValue<'_, '_>) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut read_buffer = [0; CHUNK_BYTES];
        let mut reader = BlockReader {
            file: &self.file,
            offset: block.start,
            end: block.end,
            hash: Sha256::new(),
            buffer: &mut read_buffer,
            buffer_position: 0,
            buffer_len: 0,
        };
        let mut key = [0u8; MAX_KEY_BYTES];
        let mut buffer = [0u8; CHUNK_BYTES];
        while reader.offset < reader.end {
            let mut lengths = [0u8; 8];
            reader.read(&mut lengths)?;
            let (kl, vl) = entry_lengths(lengths)?;
            if (kl + vl) as u64 > reader.end - reader.offset {
                return Err(invalid("retained base entry exceeds block"));
            }
            let key = &mut key[..kl];
            reader.read(key)?;
            let mut value = BlockValue {
                reader: &mut reader,
                buffer: &mut buffer,
                remaining: vl,
            };
            visit(key, &mut value)?;
            value.read_chunks(|_| Ok(()))?;
        }
        let digest: [u8; 32] = reader.hash.finalize().into();
        if digest != block.digest {
            return Err(invalid("retained base block digest mismatch"));
        }
        Ok(())
    }
}

fn emit_value(
    key: &[u8],
    value: Option<&[u8]>,
    visit: &mut impl FnMut(EntryPart<'_>) -> io::Result<()>,
) -> io::Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    visit(EntryPart::Header {
        key,
        value_len: value.len(),
    })?;
    for chunk in value.chunks(CHUNK_BYTES) {
        visit(EntryPart::Value(chunk))?;
    }
    Ok(())
}

struct BlockValue<'a, 'b> {
    reader: &'a mut BlockReader<'b>,
    buffer: &'a mut [u8; CHUNK_BYTES],
    remaining: usize,
}

impl BlockValue<'_, '_> {
    fn read_chunks(&mut self, mut visit: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
        while self.remaining != 0 {
            let bytes = self.remaining.min(self.buffer.len());
            let chunk = &mut self.buffer[..bytes];
            self.reader.read(chunk)?;
            self.remaining -= bytes;
            visit(chunk)?;
        }
        Ok(())
    }
}

struct PageBudget {
    bytes: usize,
    limit: usize,
    exhausted: bool,
}

impl PageBudget {
    fn admit(&mut self, count: usize, key: &[u8], value_len: usize) -> bool {
        let bytes = self.bytes + key.len() + value_len;
        if count == self.limit || bytes > MAX_SCAN_BYTES {
            self.exhausted = true;
            return false;
        }
        self.bytes = bytes;
        true
    }

    fn push_overlay(
        &mut self,
        result: &mut Vec<Entry>,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> io::Result<bool> {
        let Some(value) = value else {
            return Ok(true);
        };
        if !self.admit(result.len(), key, value.len()) {
            return Ok(false);
        }
        let mut copy = Vec::new();
        copy.try_reserve_exact(value.len()).map_err(allocation)?;
        copy.extend_from_slice(value);
        push_entry(result, key, copy)?;
        Ok(true)
    }
}

fn push_entry(result: &mut Vec<Entry>, key: &[u8], value: Vec<u8>) -> io::Result<()> {
    let mut owned_key = Vec::new();
    owned_key.try_reserve_exact(key.len()).map_err(allocation)?;
    owned_key.extend_from_slice(key);
    result.try_reserve(1).map_err(allocation)?;
    result.push((owned_key, value));
    Ok(())
}

struct BlockReader<'a> {
    file: &'a File,
    offset: u64,
    end: u64,
    hash: Sha256,
    buffer: &'a mut [u8; CHUNK_BYTES],
    buffer_position: usize,
    buffer_len: usize,
}

impl BlockReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        let end = add(self.offset, bytes.len() as u64)?;
        if end > self.end {
            return Err(invalid("retained base block read exceeds boundary"));
        }
        let mut offset = self.offset;
        let mut output = &mut *bytes;
        while !output.is_empty() {
            if self.buffer_position == self.buffer_len {
                let len = (self.end - offset).min(CHUNK_BYTES as u64) as usize;
                self.file.read_exact_at(&mut self.buffer[..len], offset)?;
                self.buffer_len = len;
                self.buffer_position = 0;
            }
            let copied = output.len().min(self.buffer_len - self.buffer_position);
            let next = self.buffer_position + copied;
            output[..copied].copy_from_slice(&self.buffer[self.buffer_position..next]);
            self.buffer_position = next;
            offset += copied as u64;
            output = &mut output[copied..];
        }
        self.hash.update(&*bytes);
        self.offset = end;
        Ok(())
    }
}
