//! The log's minimal I/O boundary, allowing deterministic failure tests.

use std::fs::File;
use std::io::{self, Write};
use std::os::unix::fs::FileExt;

pub(crate) trait LogIo: Send + Sync {
    fn len(&self) -> io::Result<u64>;
    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()>;
    fn append_all(&self, bytes: &[u8]) -> io::Result<()>;
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

    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }
}
