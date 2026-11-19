//! Positional reads within the admitted physical suffix.
use super::{SuffixExpected, SuffixLimits, invalid, open_suffix, require_suffix_eof};
use crate::record::{self, HEADER_SIZE, Header};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, BufReader, Read, Write},
    os::unix::fs::FileExt,
    path::Path,
};

const IMPORT_BUFFER_BYTES: usize = 1024 * 1024;

/// A bounded detached input, bound to one opened descriptor before staging.
pub(super) struct SuffixImport {
    input: File,
    len: usize,
    digest: [u8; 32],
}

impl SuffixImport {
    pub(super) fn open(
        path: &Path,
        limits: SuffixLimits,
        expected: &SuffixExpected,
    ) -> io::Result<Self> {
        let (input, len) = open_suffix(path, limits, expected.end)?;
        Ok(Self {
            input,
            len,
            digest: expected.digest,
        })
    }

    /// The caller must discard the private output if copying or binding fails.
    pub(super) fn write(mut self, output: &mut impl Write) -> io::Result<(u64, [u8; 32])> {
        let chunk_size = self.len.min(IMPORT_BUFFER_BYTES);
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(chunk_size)
            .map_err(|_| io::Error::other("retained suffix copy allocation failed"))?;
        scratch.resize(chunk_size, 0);
        let mut hash = Sha256::new();
        let mut remaining = self.len;
        while remaining != 0 {
            let len = remaining.min(scratch.len());
            let chunk = &mut scratch[..len];
            self.input.read_exact(chunk)?;
            hash.update(&*chunk);
            output.write_all(chunk)?;
            remaining -= len;
        }
        require_suffix_eof(&mut self.input)?;
        if <[u8; 32]>::from(hash.finalize()) != self.digest {
            return Err(invalid());
        }
        Ok((self.len as u64, self.digest))
    }
}

#[derive(Clone, Copy)]
pub(super) struct SuffixFile<'a> {
    pub file: &'a File,
    pub start: u64,
    pub len: usize,
}

impl<'a> SuffixFile<'a> {
    pub(super) fn visit_records(
        self,
        committed: Option<usize>,
        mut visit: impl FnMut(usize, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut reader = BufReader::with_capacity(64 * 1024, self.reader());
        let mut offset = 0;
        let mut header = [0; HEADER_SIZE];
        let mut bytes = Vec::new();
        while offset < self.len {
            let remaining = self.len - offset;
            if remaining < HEADER_SIZE {
                return Err(record::invalid_data("incomplete retained suffix header"));
            }
            reader.read_exact(&mut header)?;
            let len = Header::parse(&header)?.record_len() as usize;
            if len > remaining {
                return Err(record::invalid_data("incomplete retained suffix record"));
            }
            let end = offset + len;
            if committed.is_some_and(|boundary| offset < boundary && boundary < end) {
                return Err(record::invalid_data(
                    "commit witness splits a suffix record",
                ));
            }
            if bytes.len() < len {
                bytes
                    .try_reserve_exact(len - bytes.len())
                    .map_err(|_| io::Error::other("retained suffix record allocation failed"))?;
                bytes.resize(len, 0);
            }
            bytes[..HEADER_SIZE].copy_from_slice(&header);
            reader.read_exact(&mut bytes[HEADER_SIZE..len])?;
            visit(offset, &bytes[..len])?;
            offset = end;
        }
        Ok(())
    }

    pub(super) fn copy_to(self, output: &mut impl Write) -> io::Result<()> {
        if io::copy(&mut self.reader(), output)? != self.len as u64 {
            return Err(record::invalid_data(
                "retained suffix shortened during copy",
            ));
        }
        Ok(())
    }

    fn reader(self) -> SuffixReader<'a> {
        SuffixReader {
            file: self.file,
            position: self.start,
            remaining: self.len,
        }
    }
}

struct SuffixReader<'a> {
    file: &'a File,
    position: u64,
    remaining: usize,
}

impl Read for SuffixReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let len = buffer.len().min(self.remaining);
        if len == 0 {
            return Ok(0);
        }
        let read = self.file.read_at(&mut buffer[..len], self.position)?;
        self.position += read as u64;
        self.remaining -= read;
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retained_candidate::Expected;

    #[test]
    fn checked_import_caps_writes_and_rejects_late_input_changes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("suffix");
        let bytes = vec![0x34; 2 * IMPORT_BUFFER_BYTES + 7];
        let expected = SuffixExpected {
            candidate: Expected {
                header: [0; 112],
                witness: [0; 32],
                base_digest: [0; 32],
            },
            end: bytes.len() as u64,
            digest: Sha256::digest(&bytes).into(),
        };
        let limits = SuffixLimits {
            bytes: expected.end,
            operations: 0,
            work: 0,
        };

        for mode in ["unchanged", "grown", "shortened", "substituted"] {
            std::fs::write(&path, &bytes).unwrap();
            let import = SuffixImport::open(&path, limits, &expected).unwrap();
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            match mode {
                "grown" => file.write_all_at(b"extra", expected.end).unwrap(),
                "shortened" => file.set_len(expected.end - 1).unwrap(),
                "substituted" => file.write_all_at(b"changed", expected.end - 7).unwrap(),
                _ => {}
            }
            let mut output = Vec::new();
            let result = import.write(&mut output);
            match mode {
                "unchanged" => {
                    assert_eq!(result.unwrap(), (expected.end, expected.digest));
                    assert_eq!(output, bytes);
                }
                "shortened" => {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
                    assert_eq!(output, bytes[..2 * IMPORT_BUFFER_BYTES]);
                }
                _ => {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
                    assert_eq!(output.len(), bytes.len());
                }
            }
        }
    }
}
