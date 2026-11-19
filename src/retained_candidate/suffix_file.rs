//! Positional reads within the admitted physical suffix.
use crate::record::{self, HEADER_SIZE, Header};
use std::{
    fs::File,
    io::{self, BufReader, Read, Write},
    os::unix::fs::FileExt,
};

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
