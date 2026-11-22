use super::{
    Expected, OverlaySource, RetainedStore, create_file, invalid_input, marker, random_id,
    verify_files,
};
use crate::record;
use crate::retained_base_v2::index::EntryPart;
use crate::retained_base_v2::{FRAME_BYTES, Limits, encode_header, encoded_entry_bytes};
use sha2::{Digest, Sha256};
use std::{
    io::{self, BufWriter, Write},
    path::Path,
};

impl RetainedStore {
    // Maintenance verifies actual file bytes, rather than just the recorded
    // digest and the cached view, before discarding an older generation.
    pub(super) fn verify_complete(&self) -> io::Result<()> {
        self.ensure_healthy()?;
        let digest: [u8; 32] = self.hash.clone().finalize().into();
        self.verify_boundary(&digest)?;
        let mut observed = Sha256::new();
        self.suffix_file().copy_to(&mut observed)?;
        if <[u8; 32]>::from(observed.finalize()) != digest {
            return Err(record::invalid_data("retained suffix changed"));
        }
        self.base.verify()?;
        self.verify_boundary(&digest)
    }

    pub(super) fn verify_boundary(&self, digest: &[u8; 32]) -> io::Result<()> {
        verify_files(
            &self.log,
            &self.witness,
            self.header.bytes(),
            self.header.len as u64 + self.suffix_len as u64,
            &marker(self.header.bytes(), self.suffix_len as u64, digest),
        )
    }

    // The returned binding records what was written. The caller must
    // validate it against the synced file and recheck the source before
    // publishing the successor.
    pub(super) fn write_rebased_base(
        &self,
        destination: &Path,
        limits: Limits,
        max_base_bytes: u64,
    ) -> io::Result<Expected> {
        self.ensure_healthy()?;
        let suffix_digest = self.hash.clone().finalize().into();
        self.verify_boundary(&suffix_digest)?;
        // The history sweep authenticates the frame, the base blocks that
        // could contain touched keys, and the entire suffix. The private
        // merge below authenticates every base block before publication.
        let peaks = self.overlay.live_peaks(
            &self.base,
            self.suffix_file(),
            self.limits.suffix,
            suffix_digest,
        )?;
        if peaks.entries > limits.entries || peaks.key_bytes > limits.key_bytes {
            return Err(invalid_input(
                "compacted base exceeds entry or key byte limit",
            ));
        }
        let space = limits
            .file_bytes
            .min(max_base_bytes)
            .min(limits.work_bytes.saturating_sub(1))
            .checked_sub(FRAME_BYTES)
            .ok_or_else(|| invalid_input("compacted base byte limit exceeded"))?;
        let encoded = peaks.encoded_bytes;
        if encoded > space {
            return Err(invalid_input("compacted base byte limit exceeded"));
        }
        let source = self.generation_id();
        let base = random_id()?;
        let boundary = self.header.len as u64 + self.suffix_len as u64;
        let header = encode_header(
            source,
            base,
            [
                boundary,
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
        let mut remaining = encoded;
        {
            let mut write = |bytes: &[u8]| -> io::Result<()> {
                file.write_all(bytes)?;
                digest.update(bytes);
                Ok(())
            };
            self.overlay.visit_merged(
                &self.base,
                OverlaySource::Disk(self.suffix_file()),
                |part| match part {
                    EntryPart::Header { key, value_len } => {
                        remaining = remaining
                            .checked_sub(encoded_entry_bytes(key, value_len))
                            .ok_or_else(|| {
                                record::invalid_data("compacted base grew during export")
                            })?;
                        write(&(key.len() as u32).to_le_bytes())?;
                        write(&(value_len as u32).to_le_bytes())?;
                        write(key)
                    }
                    EntryPart::Value(bytes) => write(bytes),
                },
            )?;
        }
        if remaining != 0 {
            return Err(record::invalid_data("compacted base shrank during export"));
        }
        let digest: [u8; 32] = digest.finalize().into();
        file.write_all(&digest)?;
        file.flush()?;
        file.get_ref().sync_all()?;
        crate::retained_candidate::binding_from_parts(
            source,
            base,
            boundary,
            encoded + FRAME_BYTES,
            digest,
            random_id()?,
        )
    }
}
