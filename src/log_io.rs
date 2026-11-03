//! The log's minimal I/O boundary, allowing deterministic failure tests.

use std::fs::File;
use std::io::{self, IoSlice, Write};
use std::os::unix::fs::FileExt;

pub(crate) trait LogIo: Send + Sync {
    fn len(&self) -> io::Result<u64>;
    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()>;
    fn append_all(&self, bytes: &[u8]) -> io::Result<()>;
    // Implementations that do not support vectored I/O preserve append order
    // by writing one record at a time. Any error leaves the outcome uncertain.
    fn append_vectored(&self, buffers: &mut [IoSlice<'_>]) -> io::Result<()> {
        for buffer in buffers {
            self.append_all(buffer)?;
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()>;
}

impl LogIo for File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(self, buffer, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        let mut file = self;
        file.write_all(bytes)
    }

    fn append_vectored(&self, buffers: &mut [IoSlice<'_>]) -> io::Result<()> {
        write_all_vectored(self, buffers)
    }

    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }
}

fn write_all_vectored(mut writer: impl Write, mut buffers: &mut [IoSlice<'_>]) -> io::Result<()> {
    while !buffers.is_empty() {
        match writer.write_vectored(buffers) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to append log",
                ));
            }
            Ok(written) => IoSlice::advance_slices(&mut buffers, written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    enum Step {
        Write(usize),
        Interrupted,
        Zero,
        Error,
    }

    struct ShortWriter {
        steps: VecDeque<Step>,
        bytes: Vec<u8>,
    }

    impl Write for ShortWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            unreachable!("the batch path uses vectored writes")
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
            let mut remaining = match self.steps.pop_front().expect("unexpected extra write") {
                Step::Write(limit) => limit,
                Step::Interrupted => return Err(io::ErrorKind::Interrupted.into()),
                Step::Zero => return Ok(0),
                Step::Error => return Err(io::ErrorKind::BrokenPipe.into()),
            };
            let mut written = 0;
            for buffer in buffers {
                let count = buffer.len().min(remaining);
                self.bytes.extend_from_slice(&buffer[..count]);
                written += count;
                remaining -= count;
                if remaining == 0 {
                    break;
                }
            }
            Ok(written)
        }
    }

    #[test]
    fn vectored_append_retries_interruptions_and_short_writes_across_records() {
        let mut writer = ShortWriter {
            steps: VecDeque::from([
                Step::Interrupted,
                Step::Write(1),
                Step::Write(3),
                Step::Interrupted,
                Step::Write(2),
                Step::Write(100),
            ]),
            bytes: Vec::new(),
        };
        let mut buffers = [
            IoSlice::new(b"abc"),
            IoSlice::new(b"de"),
            IoSlice::new(b"fghi"),
        ];
        write_all_vectored(&mut writer, &mut buffers).unwrap();
        assert_eq!(writer.bytes, b"abcdefghi");
        assert!(writer.steps.is_empty());
    }

    #[test]
    fn vectored_append_propagates_zero_and_errors_after_a_partial_write() {
        for (step, kind) in [
            (Step::Zero, io::ErrorKind::WriteZero),
            (Step::Error, io::ErrorKind::BrokenPipe),
        ] {
            let mut writer = ShortWriter {
                steps: VecDeque::from([Step::Write(4), step]),
                bytes: Vec::new(),
            };
            let mut buffers = [IoSlice::new(b"abc"), IoSlice::new(b"def")];
            assert_eq!(
                write_all_vectored(&mut writer, &mut buffers)
                    .unwrap_err()
                    .kind(),
                kind
            );
            assert_eq!(writer.bytes, b"abcd");
        }
    }
}
