//! Bounded sequential input for checksummed log recovery.
//!
//! Replay hashes each record payload through [`RECOVERY_READ_BUFFER_BYTES`]
//! and drops the chunk. A put or delete retains its key. A batch retains at
//! most 1,024 owned child keys and their value locations until the outer
//! checksum, outer key, and nested records have been checked. Values are not
//! retained. This buffer is not a bound on the live key index, returned
//! values, other allocations, or recovery time.
use crate::log_io::LogIo;
use sha2::{Digest, Sha256};
use std::{fs::File, io, os::unix::fs::FileExt};

/// A private read cursor over a retained descriptor shared by several readers.
pub(crate) struct PositionalReader<'a> {
    file: &'a File,
    offset: u64,
}

impl<'a> PositionalReader<'a> {
    pub(crate) fn new(file: &'a File, offset: u64) -> Self {
        Self { file, offset }
    }
}

impl io::Read for PositionalReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.file.read_at(buffer, self.offset)?;
        self.offset = self.offset.checked_add(count as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "file read offset overflow")
        })?;
        Ok(count)
    }
}

/// Maximum log bytes held while checksumming one slice of a record payload.
///
/// Sequential recovery feeds payloads through a buffer of this size and
/// discards each chunk after updating checksums. The buffer is allocated
/// on first use, with at most the bytes remaining in the admitted range.
/// A put or delete retains its key of up to 1,024 bytes plus this buffer.
/// A batch retains at most
/// 1,024 child keys and their locations, not its encoded values. `read_exact`
/// can still fill a caller-supplied buffer in one read when this buffer is
/// empty; record replay does not use that path for payloads. The in-memory
/// key index, checkpoint metadata, caller-visible values, allocator overhead,
/// and elapsed recovery time are outside this buffer.
pub(crate) const RECOVERY_READ_BUFFER_BYTES: usize = 64 * 1024;

pub(crate) struct RecoveryReader<'a> {
    file: &'a dyn LogIo,
    total: u64,
    offset: u64,
    buffer: Vec<u8>,
    used: usize,
    filled: usize,
    digest: Option<&'a mut Sha256>,
}

impl<'a> RecoveryReader<'a> {
    pub(crate) fn new(file: &'a dyn LogIo, total: u64) -> Self {
        Self {
            file,
            total,
            offset: 0,
            buffer: Vec::new(),
            used: 0,
            filled: 0,
            digest: None,
        }
    }

    pub(crate) fn at(file: &'a dyn LogIo, total: u64, offset: u64) -> io::Result<Self> {
        if offset > total {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recovery offset exceeds log length",
            ));
        }
        let mut reader = Self::new(file, total);
        reader.offset = offset;
        Ok(reader)
    }

    pub(crate) fn position(&self) -> u64 {
        self.offset
    }

    /// Hash every byte read into this sequential reader, including headers
    /// and checksums. Use the digest only after validating the entire range.
    pub(crate) fn with_digest(mut self, digest: &'a mut Sha256) -> Self {
        self.digest = Some(digest);
        self
    }

    pub(crate) fn read_exact(&mut self, mut output: &mut [u8]) -> io::Result<()> {
        self.ensure_available(output.len())?;
        while !output.is_empty() {
            // Replay checksums payloads through `read_with`, which stays
            // inside this buffer. A `read_exact` of at least one full buffer,
            // issued when the buffer is empty, still reads directly into the
            // caller-supplied output.
            if self.used == self.filled && output.len() >= RECOVERY_READ_BUFFER_BYTES {
                self.file.read_exact_at(output, self.offset)?;
                if let Some(digest) = self.digest.as_mut() {
                    digest.update(&*output);
                }
                self.offset += output.len() as u64;
                return Ok(());
            }
            self.fill()?;
            let count = output.len().min(self.filled - self.used);
            output[..count].copy_from_slice(&self.buffer[self.used..self.used + count]);
            self.used += count;
            self.offset += count as u64;
            output = &mut output[count..];
        }
        Ok(())
    }

    /// Passes exactly `len` bytes to `consume` in pieces no larger than the
    /// recovery buffer. The callback must not retain the slice; the next
    /// read reuses that buffer.
    pub(crate) fn read_with(
        &mut self,
        mut len: usize,
        mut consume: impl FnMut(&[u8]),
    ) -> io::Result<()> {
        self.ensure_available(len)?;
        while len > 0 {
            self.fill()?;
            let count = len.min(self.filled - self.used);
            consume(&self.buffer[self.used..self.used + count]);
            self.used += count;
            self.offset += count as u64;
            len -= count;
        }
        Ok(())
    }

    fn ensure_available(&self, len: usize) -> io::Result<()> {
        if len as u64 > self.total - self.offset {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of log",
            ));
        }
        Ok(())
    }

    fn fill(&mut self) -> io::Result<()> {
        if self.used != self.filled {
            return Ok(());
        }
        if self.offset >= self.total {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of log",
            ));
        }
        let count = (self.total - self.offset).min(RECOVERY_READ_BUFFER_BYTES as u64) as usize;
        if self.buffer.is_empty() {
            self.buffer
                .try_reserve_exact(count)
                .map_err(|_| io::Error::other("log recovery buffer allocation failed"))?;
            self.buffer.resize(count, 0);
        }
        self.file
            .read_exact_at(&mut self.buffer[..count], self.offset)?;
        if let Some(digest) = self.digest.as_mut() {
            digest.update(&self.buffer[..count]);
        }
        self.used = 0;
        self.filled = count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, Write};
    use std::sync::Mutex;

    #[test]
    fn positional_readers_keep_the_open_file_cursor_and_each_other_independent() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"abcdef").unwrap();
        let mut first = PositionalReader::new(&file, 0);
        let mut second = PositionalReader::new(&file, 3);
        let mut bytes = [0; 2];
        first.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"ab");
        second.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"de");
        first.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"cd");
        assert_eq!(second.read(&mut bytes).unwrap(), 1);
        assert_eq!(bytes[0], b'f');
        assert_eq!(file.stream_position().unwrap(), 6);
    }

    struct Memory {
        bytes: Vec<u8>,
        reads: Mutex<Vec<usize>>,
    }

    impl LogIo for Memory {
        fn len(&self) -> io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }

        fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
            self.reads.lock().unwrap().push(buffer.len());
            let start = usize::try_from(offset).unwrap();
            let end = start.checked_add(buffer.len()).unwrap();
            buffer.copy_from_slice(self.bytes.get(start..end).ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "memory read past end")
            })?);
            Ok(())
        }

        fn append_all(&self, _bytes: &[u8]) -> io::Result<()> {
            Err(io::Error::other("memory log is read-only"))
        }

        fn sync_data(&self) -> io::Result<()> {
            Err(io::Error::other("memory log is read-only"))
        }
    }

    #[test]
    fn recovery_allocates_only_the_admitted_suffix_on_first_read() {
        let file = Memory {
            bytes: vec![0x42; RECOVERY_READ_BUFFER_BYTES + 13],
            reads: Mutex::new(Vec::new()),
        };
        let total = file.len().unwrap();
        let mut hash = Sha256::new();
        let mut reader = RecoveryReader::at(&file, total, total - 13)
            .unwrap()
            .with_digest(&mut hash);
        assert_eq!(reader.buffer.capacity(), 0);
        let mut header = [0; 4];
        reader.read_exact(&mut header).unwrap();
        assert_eq!(header, [0x42; 4]);
        assert_eq!(reader.buffer.len(), 13);
        assert_eq!(reader.buffer.capacity(), 13);
        reader
            .read_with(9, |bytes| assert_eq!(bytes, [0x42; 9]))
            .unwrap();
        assert_eq!(reader.position(), total);
        assert_eq!(
            reader.read_exact(&mut header).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(*file.reads.lock().unwrap(), [13]);
        assert_eq!(hash.finalize(), Sha256::digest([0x42; 13]));

        let mut empty = RecoveryReader::at(&file, total, total).unwrap();
        empty.read_exact(&mut []).unwrap();
        assert_eq!(empty.buffer.capacity(), 0);
    }

    #[test]
    fn chunked_reads_match_the_source_and_stay_within_the_buffer() {
        let mut bytes = vec![0_u8; 28];
        bytes.extend((0_u32..200_000).map(|value| (value % 251) as u8));
        bytes.extend_from_slice(&[9, 8, 7, 6]);
        let file = Memory {
            bytes: bytes.clone(),
            reads: Mutex::new(Vec::new()),
        };
        let mut reader = RecoveryReader::new(&file, file.len().unwrap());
        let mut header = [0_u8; 28];
        reader.read_exact(&mut header).unwrap();
        assert_eq!(header, &bytes[..28]);

        let mut collected = Vec::new();
        let mut hasher = crc32fast::Hasher::new();
        reader
            .read_with(200_000, |chunk| {
                assert!(chunk.len() <= RECOVERY_READ_BUFFER_BYTES);
                assert!(!chunk.is_empty());
                hasher.update(chunk);
                collected.extend_from_slice(chunk);
            })
            .unwrap();
        assert_eq!(collected, &bytes[28..28 + 200_000]);
        assert_eq!(hasher.finalize(), crc32fast::hash(&bytes[28..28 + 200_000]));

        let mut trailer = [0_u8; 4];
        reader.read_exact(&mut trailer).unwrap();
        assert_eq!(trailer, [9, 8, 7, 6]);
        assert_eq!(reader.position(), bytes.len() as u64);
        assert!(
            file.reads
                .lock()
                .unwrap()
                .iter()
                .all(|len| *len <= RECOVERY_READ_BUFFER_BYTES)
        );
    }

    #[test]
    fn a_large_direct_read_leaves_the_following_chunked_read_aligned() {
        let bytes: Vec<u8> = (0_u32..90_000).map(|value| value as u8).collect();
        let file = Memory {
            bytes: bytes.clone(),
            reads: Mutex::new(Vec::new()),
        };
        let mut reader = RecoveryReader::new(&file, bytes.len() as u64);
        let mut direct = vec![0_u8; 70_000];
        reader.read_exact(&mut direct).unwrap();
        assert_eq!(direct, &bytes[..70_000]);
        let mut rest = Vec::new();
        reader
            .read_with(20_000, |chunk| rest.extend_from_slice(chunk))
            .unwrap();
        assert_eq!(rest, &bytes[70_000..]);
        assert_eq!(reader.position(), bytes.len() as u64);
        assert!(file.reads.lock().unwrap().contains(&70_000));
    }

    #[test]
    fn large_read_drains_buffer_then_reads_remaining_bytes_directly() {
        let bytes: Vec<u8> = (0_u32..200_000).map(|value| value as u8).collect();
        let file = Memory {
            bytes: bytes.clone(),
            reads: Mutex::new(Vec::new()),
        };
        let mut reader = RecoveryReader::new(&file, bytes.len() as u64);
        let mut first = [0_u8; 1];
        reader.read_exact(&mut first).unwrap();
        let mut next = vec![0_u8; 150_000];
        reader.read_exact(&mut next).unwrap();
        assert_eq!(first[0], bytes[0]);
        assert_eq!(next, &bytes[1..150_001]);
        assert_eq!(reader.position(), 150_001);
        assert!(
            file.reads
                .lock()
                .unwrap()
                .iter()
                .any(|size| *size > RECOVERY_READ_BUFFER_BYTES)
        );
    }

    #[test]
    fn reads_past_the_declared_end_fail_before_consuming_input() {
        let file = Memory {
            bytes: vec![1, 2, 3, 4],
            reads: Mutex::new(Vec::new()),
        };
        let mut reader = RecoveryReader::at(&file, 4, 2).unwrap();
        let error = reader.read_with(3, |_| ()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(reader.position(), 2);
        assert!(file.reads.lock().unwrap().is_empty());
        reader
            .read_with(0, |_| panic!("empty read must not consume"))
            .unwrap();
        let mut exact = [0_u8; 3];
        assert_eq!(
            reader.read_exact(&mut exact).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(reader.position(), 2);
    }
}
