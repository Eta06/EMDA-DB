//! Sparse in-memory directory over the validated, immutable base file.
use super::{
    CHUNK_BYTES, Expected, FRAME_BYTES, HEADER_BYTES, Limits, MAX_KEY_BYTES, Validated, add,
    encode_header, entry_lengths, invalid, parse_header, scan_buffered, validate_value_budget,
};
use crate::recovery::PositionalReader;
use crate::{Entry, MAX_SCAN_BYTES, MAX_SCAN_ENTRIES};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read, Write},
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
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

pub(crate) enum OverlayValue<'a> {
    Inline(&'a [u8]),
    Disk {
        file: &'a File,
        offset: u64,
        len: usize,
        digest: &'a [u8; 32],
    },
}

impl OverlayValue<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline(bytes) => bytes.len(),
            Self::Disk { len, .. } => *len,
        }
    }

    pub(crate) fn to_owned_bytes(&self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(self.len()).map_err(allocation)?;
        match *self {
            Self::Inline(value) => bytes.extend_from_slice(value),
            Self::Disk {
                file,
                offset,
                len,
                digest,
            } => {
                bytes.resize(len, 0);
                if len != 0 {
                    file.read_exact_at(&mut bytes, offset)?;
                }
                verify_overlay_digest(Sha256::digest(&bytes).into(), digest)?;
            }
        }
        Ok(bytes)
    }

    fn visit_chunks(self, visit: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
        self.visit_chunks_with(&mut [0; CHUNK_BYTES], visit)
    }

    pub(crate) fn visit_chunks_with(
        self,
        scratch: &mut [u8],
        mut visit: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        assert!(!scratch.is_empty());
        match self {
            Self::Inline(bytes) => {
                for chunk in bytes.chunks(scratch.len()) {
                    visit(chunk)?;
                }
            }
            Self::Disk {
                file,
                mut offset,
                len,
                digest,
            } => {
                let mut hash = Sha256::new();
                let mut remaining = len;
                while remaining != 0 {
                    let bytes = remaining.min(scratch.len());
                    let chunk = &mut scratch[..bytes];
                    file.read_exact_at(chunk, offset)?;
                    hash.update(&*chunk);
                    visit(chunk)?;
                    offset += bytes as u64;
                    remaining -= bytes;
                }
                verify_overlay_digest(hash.finalize().into(), digest)?;
            }
        }
        Ok(())
    }
}

fn verify_overlay_digest(actual: [u8; 32], expected: &[u8; 32]) -> io::Result<()> {
    if actual != *expected {
        return Err(invalid("retained suffix value digest mismatch"));
    }
    Ok(())
}

enum Selection {
    Skip,
    Take,
    Stop,
}

impl IndexedBase {
    // Reject impossible or mismatched frames before paying replay costs. The
    // trailer is only a hint here. Opening must still verify the entire base.
    pub(crate) fn preflight(
        file: &File,
        length: u64,
        limits: Limits,
        expected: Expected,
    ) -> io::Result<()> {
        if length < FRAME_BYTES {
            return Err(invalid("incomplete retained base frame"));
        }
        let mut header = [0; HEADER_BYTES as usize];
        file.read_exact_at(&mut header, 0)?;
        let header = parse_header(&header, limits)?;
        if header.file_bytes != length
            || header.source_id != expected.source_id
            || header.base_id != expected.base_id
            || header.boundary != expected.boundary
        {
            return Err(invalid("retained base header does not match its binding"));
        }
        let mut trailer = [0; 32];
        file.read_exact_at(&mut trailer, length - 32)?;
        if trailer != expected.digest {
            return Err(invalid("retained base trailer does not match its binding"));
        }
        Ok(())
    }

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
        let reader = PositionalReader::new(&self.file, 0);
        if scan_buffered(reader, limits, None)? != self.metadata {
            return Err(invalid("retained base changed"));
        }
        Ok(())
    }

    pub(crate) fn check_path(&self, path: &Path) -> io::Result<()> {
        let entry = path.symlink_metadata()?;
        let pinned = self.file.metadata()?;
        if !entry.is_file()
            || (entry.dev(), entry.ino()) != (pinned.dev(), pinned.ino())
            || entry.len() != self.metadata.file_bytes
        {
            return Err(invalid("retained base path or length changed"));
        }
        Ok(())
    }

    /// Copy the captured bytes into private staging for digest verification.
    pub(crate) fn copy_to(&self, output: &mut impl Write) -> io::Result<()> {
        let length = self.metadata.file_bytes;
        let reader = PositionalReader::new(&self.file, 0).take(length);
        let mut reader = io::BufReader::with_capacity(CHUNK_BYTES, reader);
        if io::copy(&mut reader, output)? != length {
            return Err(invalid("retained base shortened during copy"));
        }
        Ok(())
    }

    /// Verify the frame and visit blocks that could contain the supplied
    /// sorted keys. The caller verifies all other blocks before publication.
    pub(crate) fn visit_selected_block_lengths<'a>(
        &self,
        keys: impl Iterator<Item = &'a [u8]>,
        mut visit: impl FnMut(&[u8], usize) -> io::Result<()>,
    ) -> io::Result<()> {
        self.check_length()?;
        let base = &self.metadata;
        let expected = encode_header(
            base.source_id,
            base.base_id,
            [
                base.boundary,
                base.entries,
                base.key_bytes,
                base.peak_entries,
                base.peak_key_bytes,
                base.file_bytes - FRAME_BYTES,
            ],
        );
        let mut header = [0; HEADER_BYTES as usize];
        self.file.read_exact_at(&mut header, 0)?;
        if header != expected {
            return Err(invalid("retained base header changed"));
        }
        let mut trailer = [0; 32];
        self.file
            .read_exact_at(&mut trailer, base.file_bytes - 32)?;
        if trailer != base.digest {
            return Err(invalid("retained base trailer changed"));
        }
        let mut keys = keys.peekable();
        while let Some(key) = keys.next() {
            let Some(position) = self.preceding_block(key) else {
                continue;
            };
            let next_anchor = self.blocks.get(position + 1).map(|block| &block.first_key);
            while keys
                .peek()
                .is_some_and(|key| next_anchor.is_none_or(|anchor| *key < anchor.as_slice()))
            {
                keys.next();
            }
            self.visit_block(&self.blocks[position], |key, value| {
                visit(key, value.remaining)
            })?;
        }
        Ok(())
    }

    // Visits complete blocks in order. Callers must keep side effects private
    // until every block has authenticated, including those hidden by tombstones.
    pub(crate) fn visit_merged<'a>(
        &self,
        overlay: impl Iterator<Item = (&'a [u8], Option<OverlayValue<'a>>)>,
        mut visit: impl FnMut(EntryPart<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.check_length()?;
        let mut overlay = overlay.peekable();
        for block in &self.blocks {
            self.visit_block(block, |key, value| {
                while overlay.peek().is_some_and(|(other, _)| *other <= key) {
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
        overlay: impl Iterator<Item = (&'a [u8], Option<OverlayValue<'a>>)>,
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
                while overlay.peek().is_some_and(|(other, _)| *other <= key) {
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
    value: Option<OverlayValue<'_>>,
    visit: &mut impl FnMut(EntryPart<'_>) -> io::Result<()>,
) -> io::Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    visit(EntryPart::Header {
        key,
        value_len: value.len(),
    })?;
    value.visit_chunks(|bytes| visit(EntryPart::Value(bytes)))
}

struct BlockValue<'a, 'b> {
    reader: &'a mut BlockReader<'b>,
    remaining: usize,
}

impl BlockValue<'_, '_> {
    fn read_chunks(&mut self, mut visit: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
        self.reader.read_chunks(self.remaining, |bytes| {
            self.remaining -= bytes.len();
            visit(bytes)
        })
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
        value: Option<OverlayValue<'_>>,
    ) -> io::Result<bool> {
        let Some(value) = value else {
            return Ok(true);
        };
        if !self.admit(result.len(), key, value.len()) {
            return Ok(false);
        }
        push_entry(result, key, value.to_owned_bytes()?)?;
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
        let mut written = 0;
        self.read_chunks(bytes.len(), |chunk| {
            bytes[written..written + chunk.len()].copy_from_slice(chunk);
            written += chunk.len();
            Ok(())
        })
    }

    fn read_chunks(
        &mut self,
        mut remaining: usize,
        mut visit: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let end = add(self.offset, remaining as u64)?;
        if end > self.end {
            return Err(invalid("retained base block read exceeds boundary"));
        }
        while remaining != 0 {
            if self.buffer_position == self.buffer_len {
                let len = (self.end - self.offset).min(CHUNK_BYTES as u64) as usize;
                self.file
                    .read_exact_at(&mut self.buffer[..len], self.offset)?;
                self.buffer_len = len;
                self.buffer_position = 0;
            }
            let count = remaining.min(self.buffer_len - self.buffer_position);
            let next = self.buffer_position + count;
            let chunk = &self.buffer[self.buffer_position..next];
            self.hash.update(chunk);
            self.buffer_position = next;
            self.offset += count as u64;
            remaining -= count;
            visit(chunk)?;
        }
        Ok(())
    }
}
