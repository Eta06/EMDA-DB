use super::{
    Expected, RetainedStore, create_file, invalid_input, marker, open_regular, random_id,
    verify_files,
};
use crate::record;
use crate::retained_base_v2::{self, Limits, encode_header};
use sha2::{Digest, Sha256};
use std::{
    io::{self, BufWriter, Write},
    os::unix::fs::FileExt,
    path::Path,
};

impl RetainedStore {
    // Maintenance verifies actual file bytes, rather than just the recorded
    // digest and the cached view, before discarding an older generation.
    pub(super) fn verify_complete(&self) -> io::Result<()> {
        self.ensure_healthy()?;
        let expected = marker(
            self.header.bytes(),
            self.suffix.len() as u64,
            &self.hash.clone().finalize().into(),
        );
        let total = self.header.len as u64 + self.suffix.len() as u64;
        verify_files(
            &self.log,
            &self.witness,
            self.header.bytes(),
            total,
            &expected,
        )?;
        let mut offset = self.header.len as u64;
        let mut buffer = [0; 64 * 1024];
        for bytes in self.suffix.chunks(buffer.len()) {
            self.log.read_exact_at(&mut buffer[..bytes.len()], offset)?;
            if buffer[..bytes.len()] != *bytes {
                return Err(record::invalid_data("retained suffix changed"));
            }
            offset += bytes.len() as u64;
        }
        self.base.verify(self.limits.base)?;
        verify_files(
            &self.log,
            &self.witness,
            self.header.bytes(),
            total,
            &expected,
        )
    }

    pub(super) fn write_rebased_base(
        &self,
        destination: &Path,
        limits: Limits,
    ) -> io::Result<Expected> {
        self.verify_complete()?;
        let peaks = self
            .overlay
            .live_peaks(&self.base, &self.suffix, self.limits.suffix)?;
        if peaks.entries > limits.entries || peaks.key_bytes > limits.key_bytes {
            return Err(invalid_input(
                "compacted base exceeds entry or key byte limit",
            ));
        }
        let mut encoded = 0_u64;
        self.visit_live(|key, value| {
            encoded = encoded
                .checked_add(8 + key.len() as u64 + value.len() as u64)
                .ok_or_else(|| invalid_input("compacted base size overflow"))?;
            Ok(())
        })?;
        let size = encoded
            .checked_add(144)
            .ok_or_else(|| invalid_input("compacted base size overflow"))?;
        if size > limits.file_bytes || size.checked_add(1).is_none_or(|n| n > limits.work_bytes) {
            return Err(invalid_input("compacted base byte limit exceeded"));
        }
        let source = self.generation_id();
        let base = random_id()?;
        let header = encode_header(
            source,
            base,
            [
                self.header.len as u64 + self.suffix.len() as u64,
                peaks.entries,
                peaks.key_bytes,
                peaks.peak_entries,
                peaks.peak_key_bytes,
                encoded,
            ],
        );
        let mut file = BufWriter::with_capacity(64 * 1024, create_file(destination)?);
        let mut digest = Sha256::new();
        digest.update(b"EMDA retained base v2\0");
        file.write_all(&header)?;
        digest.update(header);
        self.visit_live(|key, value| {
            for bytes in [
                &(key.len() as u32).to_le_bytes()[..],
                &(value.len() as u32).to_le_bytes()[..],
                key,
                value,
            ] {
                file.write_all(bytes)?;
                digest.update(bytes);
            }
            Ok(())
        })?;
        let digest: [u8; 32] = digest.finalize().into();
        file.write_all(&digest)?;
        file.flush()?;
        file.get_ref().sync_all()?;
        drop(file);
        let validated = retained_base_v2::validate_buffered(open_regular(destination)?, limits)?;
        if validated.digest != digest || validated.source_id != source {
            return Err(record::invalid_data("compacted base verification mismatch"));
        }
        self.verify_complete()?;
        crate::retained_candidate::binding(&validated, random_id()?)
    }

    fn visit_live(&self, mut visit: impl FnMut(&[u8], &[u8]) -> io::Result<()>) -> io::Result<()> {
        let mut after = None;
        loop {
            let page = self.scan(after.as_deref(), None, crate::MAX_SCAN_ENTRIES)?;
            if page.is_empty() {
                return Ok(());
            }
            for (key, value) in page {
                visit(&key, &value)?;
                after = Some(key);
            }
        }
    }
}
