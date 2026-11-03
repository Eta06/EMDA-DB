//! Bounded sequential input for checksummed log recovery.
use crate::log_io::LogIo;
use std::io;

const BUFFER_SIZE: usize = 64 * 1024;

pub(crate) struct RecoveryReader<'a> {
    file: &'a dyn LogIo,
    total: u64,
    offset: u64,
    buffer: Vec<u8>,
    used: usize,
    filled: usize,
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
        }
    }

    pub(crate) fn read_exact(&mut self, mut output: &mut [u8]) -> io::Result<()> {
        if output.len() as u64 > self.total - self.offset {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of log",
            ));
        }
        while !output.is_empty() {
            if self.used == self.filled {
                if output.len() >= BUFFER_SIZE {
                    self.file.read_exact_at(output, self.offset)?;
                    self.offset += output.len() as u64;
                    return Ok(());
                }
                if self.buffer.is_empty() {
                    self.buffer.resize(BUFFER_SIZE, 0);
                }
                let count = (self.total - self.offset).min(BUFFER_SIZE as u64) as usize;
                self.file
                    .read_exact_at(&mut self.buffer[..count], self.offset)?;
                self.used = 0;
                self.filled = count;
            }
            let count = output.len().min(self.filled - self.used);
            output[..count].copy_from_slice(&self.buffer[self.used..self.used + count]);
            self.used += count;
            self.offset += count as u64;
            output = &mut output[count..];
        }
        Ok(())
    }
}
