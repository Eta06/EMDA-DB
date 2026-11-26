use super::{
    Expected, OverlaySource, RetainedStore, RetainedStoreLimits, create_file, invalid_input,
    marker, random_id, verify_files,
};
use crate::record;
use crate::retained_base_v2::index::{BlockLayout, EntryPart, IndexedBase};
use crate::retained_base_v2::{FRAME_BYTES, encode_header, encoded_entry_bytes};
use sha2::{Digest, Sha256};
use std::{
    io::{self, BufWriter, Write},
    os::unix::fs::MetadataExt,
    path::Path,
};

impl RetainedStore {
    // Maintenance verifies actual file bytes, rather than just the recorded
    // digest and the cached view, before discarding an older generation.
    pub(super) fn verify_complete(&self, directory: &Path) -> io::Result<()> {
        self.ensure_healthy()?;
        self.base.check_path(&directory.join("base"))?;
        for (name, file) in [("records.log", &self.log), ("commit.state", &self.witness)] {
            let pinned = file.metadata()?;
            let named = directory.join(name).symlink_metadata()?;
            if !named.is_file() || (pinned.dev(), pinned.ino()) != (named.dev(), named.ino()) {
                return Err(record::invalid_data("retained generation file replaced"));
            }
        }
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
        limits: RetainedStoreLimits,
        max_base_bytes: u64,
        scan_remaining: &mut Option<u64>,
    ) -> io::Result<Expected> {
        self.ensure_healthy()?;
        self.rebase_source().write(
            destination,
            limits,
            max_base_bytes,
            scan_remaining,
            |digest| self.verify_boundary(digest),
        )
    }

    pub(super) fn rebase_source(&self) -> RebaseSource<'_> {
        RebaseSource {
            base: &self.base,
            log: &self.log,
            header: &self.header,
            suffix_len: self.suffix_len,
            hash: &self.hash,
            overlay: &self.overlay,
            suffix_limits: self.limits.suffix,
        }
    }
}

/// The parts of a retained generation that a rebased base is built from.
/// A frozen copy can supply them while the live store keeps writing.
pub(super) struct RebaseSource<'a> {
    pub(super) base: &'a IndexedBase,
    pub(super) log: &'a std::fs::File,
    pub(super) header: &'a super::Header,
    pub(super) suffix_len: usize,
    pub(super) hash: &'a Sha256,
    pub(super) overlay: &'a super::Overlay,
    pub(super) suffix_limits: super::SuffixLimits,
}

impl RebaseSource<'_> {
    fn suffix_file(&self) -> super::SuffixFile<'_> {
        super::SuffixFile {
            file: self.log,
            start: self.header.len as u64,
            len: self.suffix_len,
        }
    }

    // The returned binding records what was written. The caller must
    // validate it against the synced file and recheck the source before
    // publishing the successor. `verify_boundary` checks the commit witness
    // at the point where the live store always has.
    pub(super) fn write(
        &self,
        destination: &Path,
        limits: RetainedStoreLimits,
        max_base_bytes: u64,
        scan_remaining: &mut Option<u64>,
        verify_boundary: impl FnOnce(&[u8; 32]) -> io::Result<()>,
    ) -> io::Result<Expected> {
        let space = limits
            .base
            .file_bytes
            .min(max_base_bytes)
            .min(limits.base.work_bytes.saturating_sub(1))
            .checked_sub(FRAME_BYTES)
            .ok_or_else(|| invalid_input("compacted base byte limit exceeded"))?;
        let mut charge = |bytes| {
            if let Some(remaining) = scan_remaining {
                *remaining = remaining
                    .checked_sub(bytes)
                    .ok_or_else(|| invalid_input("retained rotation scan byte limit exceeded"))?;
            }
            Ok(())
        };
        let base_bytes = self.base.metadata().file_bytes;
        charge(base_bytes - FRAME_BYTES)?; // Merge reads every base block.
        charge(base_bytes)?; // Final source verification reads the frame too.
        charge(self.suffix_len as u64)?; // History replay.
        charge(self.suffix_len as u64)?; // Final source verification.
        let suffix_digest = self.hash.clone().finalize().into();
        verify_boundary(&suffix_digest)?;
        // The history sweep authenticates the frame, the base blocks that
        // could contain touched keys, and the entire suffix. The private
        // merge below authenticates every base block before publication.
        let peaks = self.overlay.live_peaks(
            self.base,
            self.suffix_file(),
            self.suffix_limits,
            suffix_digest,
            &mut charge,
        )?;
        if peaks.entries > limits.base.entries || peaks.key_bytes > limits.base.key_bytes {
            return Err(invalid_input(
                "compacted base exceeds entry or key byte limit",
            ));
        }
        let encoded = peaks.encoded_bytes;
        if encoded > space {
            return Err(invalid_input("compacted base byte limit exceeded"));
        }
        IndexedBase::admit_totals(peaks.entries, peaks.key_bytes, encoded, limits.index)?;
        // The successor opens and verifies the complete output before publication.
        charge(encoded + FRAME_BYTES)?;
        let source = self.header.id();
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
        let mut layout = BlockLayout::new(limits.index);
        {
            let mut write = |bytes: &[u8]| -> io::Result<()> {
                file.write_all(bytes)?;
                digest.update(bytes);
                Ok(())
            };
            self.overlay.visit_merged(
                self.base,
                OverlaySource::Disk(self.suffix_file()),
                |part| match part {
                    EntryPart::Header { key, value_len } => {
                        let start = encoded - remaining;
                        remaining = remaining
                            .checked_sub(encoded_entry_bytes(key, value_len))
                            .ok_or_else(|| {
                                record::invalid_data("compacted base grew during export")
                            })?;
                        layout.admit(start, encoded - remaining, key.len())?;
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
/// An owned, read-only copy of a generation's state at one committed
/// boundary, captured so a successor base can be prepared while the live
/// store keeps appending. The base file and the suffix bytes up to the
/// boundary are immutable, so duplicated descriptors keep reading exactly
/// the captured state.
pub(super) struct FrozenRetained {
    base: IndexedBase,
    log: std::fs::File,
    header: super::Header,
    suffix_len: usize,
    hash: Sha256,
    overlay: super::Overlay,
    suffix_limits: super::SuffixLimits,
}

/// One acknowledged record replayed into a successor: its ordered puts and
/// deletes.
pub(super) struct CatchUpRecord {
    pub(super) changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl RetainedStore {
    /// Captures the committed state. The commit witness is checked now,
    /// because the live store may advance it before the copy is used.
    pub(super) fn freeze(&self) -> io::Result<FrozenRetained> {
        self.ensure_healthy()?;
        let digest: [u8; 32] = self.hash.clone().finalize().into();
        self.verify_boundary(&digest)?;
        Ok(FrozenRetained {
            base: self.base.try_clone()?,
            log: self.log.try_clone()?,
            header: self.header.clone(),
            suffix_len: self.suffix_len,
            hash: self.hash.clone(),
            overlay: self.overlay.clone(),
            suffix_limits: self.limits.suffix,
        })
    }

    /// The mutations committed after `frozen` was captured, in commit order.
    /// The copy must come from this generation, and the bytes after its
    /// boundary must extend its suffix digest to this store's.
    pub(super) fn changes_since(&self, frozen: &FrozenRetained) -> io::Result<Vec<CatchUpRecord>> {
        self.ensure_healthy()?;
        if self.header.id() != frozen.header.id() || self.suffix_len < frozen.suffix_len {
            return Err(record::invalid_data(
                "retained generation changed since the rotation was captured",
            ));
        }
        let start = self.header.len as u64 + frozen.suffix_len as u64;
        let delta = super::SuffixFile {
            file: &self.log,
            start,
            len: self.suffix_len - frozen.suffix_len,
        };
        let mut extended = frozen.hash.clone();
        super::SuffixFile { ..delta }.copy_to(&mut extended)?;
        if extended.finalize() != self.hash.clone().finalize() {
            return Err(record::invalid_data(
                "retained suffix changed since the rotation was captured",
            ));
        }
        let read_value = |location: Option<crate::record::batch::stream::StreamValue<u32>>| {
            location
                .map(|location| -> io::Result<Vec<u8>> {
                    let mut value = vec![0; location.length];
                    std::os::unix::fs::FileExt::read_exact_at(
                        &self.log,
                        &mut value,
                        start + location.offset,
                    )?;
                    if crc32fast::hash(&value) != location.digest {
                        return Err(record::invalid_data(
                            "retained suffix value checksum mismatch",
                        ));
                    }
                    Ok(value)
                })
                .transpose()
        };
        let mut records = Vec::new();
        delta.replay_records::<crc32fast::Hasher>(None, u64::MAX, None, |changes| {
            let record = match changes {
                crate::retained_candidate::suffix_file::Changes::Single(key, location) => {
                    CatchUpRecord {
                        changes: vec![(key.to_vec(), read_value(location)?)],
                    }
                }
                crate::retained_candidate::suffix_file::Changes::Batch(updates) => CatchUpRecord {
                    changes: updates
                        .into_iter()
                        .map(|update| Ok((update.key, read_value(update.location)?)))
                        .collect::<io::Result<_>>()?,
                },
            };
            records.push(record);
            Ok(None)
        })?;
        Ok(records)
    }
}

impl RetainedStore {
    /// Cheap checks a rotation makes at completion: the generation's files
    /// are still the ones this store holds, and the witness matches its
    /// committed suffix. The bytes since capture were verified by
    /// `changes_since`, and the frozen copy verified the rest.
    pub(super) fn verify_current(&self, directory: &Path) -> io::Result<()> {
        self.ensure_healthy()?;
        self.base.check_path(&directory.join("base"))?;
        for (name, file) in [("records.log", &self.log), ("commit.state", &self.witness)] {
            let pinned = file.metadata()?;
            let named = directory.join(name).symlink_metadata()?;
            if !named.is_file() || (pinned.dev(), pinned.ino()) != (named.dev(), named.ino()) {
                return Err(record::invalid_data("retained generation file replaced"));
            }
        }
        let digest: [u8; 32] = self.hash.clone().finalize().into();
        self.verify_boundary(&digest)
    }

    /// An upper bound on a rebased base of this state: the current base and
    /// every suffix byte.
    pub(super) fn rebase_bound(&self) -> u64 {
        self.base
            .metadata()
            .file_bytes
            .saturating_add(self.suffix_len as u64)
    }
}

impl FrozenRetained {
    fn source(&self) -> RebaseSource<'_> {
        RebaseSource {
            base: &self.base,
            log: &self.log,
            header: &self.header,
            suffix_len: self.suffix_len,
            hash: &self.hash,
            overlay: &self.overlay,
            suffix_limits: self.suffix_limits,
        }
    }

    /// Writes the successor base from the captured state. The witness was
    /// checked at capture.
    pub(super) fn write_rebased_base(
        &self,
        destination: &Path,
        limits: RetainedStoreLimits,
        max_base_bytes: u64,
        scan_remaining: &mut Option<u64>,
    ) -> io::Result<Expected> {
        self.source()
            .write(destination, limits, max_base_bytes, scan_remaining, |_| {
                Ok(())
            })
    }

    /// Verifies the captured suffix bytes and every base block, the checks
    /// the live store's rotation makes before discarding a generation.
    pub(super) fn verify(&self) -> io::Result<()> {
        let mut observed = Sha256::new();
        self.source().suffix_file().copy_to(&mut observed)?;
        if observed.finalize() != self.hash.clone().finalize() {
            return Err(record::invalid_data("retained suffix changed"));
        }
        self.base.verify()
    }
}
