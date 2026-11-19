//! Non-authoritative, non-destructive retained-base export.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{Seek, SeekFrom, Write};

/// Explicit limits for full replay and the resulting file.
#[derive(Clone, Copy, Debug)]
pub struct RetainedExportLimits {
    pub max_log_bytes: u64,
    pub max_live_keys: usize,
    pub max_live_key_bytes: usize,
    pub max_output_bytes: u64,
}

/// Publication result for a detached suffix. Preserve `expected` independently
/// when publication is uncertain and reconcile the destination before retrying.
#[derive(Debug)]
pub enum SuffixExportFailure {
    NotPublished(io::Error),
    OutcomeUnknown {
        error: io::Error,
        expected: Box<crate::retained_candidate::SuffixExpected>,
    },
}

impl std::fmt::Display for SuffixExportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPublished(error) => write!(f, "suffix publication not started: {error}"),
            Self::OutcomeUnknown { error, .. } => {
                write!(f, "suffix publication outcome unknown: {error}")
            }
        }
    }
}
impl std::error::Error for SuffixExportFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error) | Self::OutcomeUnknown { error, .. } => Some(error),
        }
    }
}

impl Store {
    /// Exports an untrusted retained base. The caller supplies a nonzero fresh
    /// identifier; this operation does not establish authority or permit recovery.
    /// The caller must choose a destination outside the source directory and
    /// ensure the source and both directories remain stable and trusted against
    /// uncoordinated mutation for the entire operation. Store does not retain
    /// its source path, so it cannot enforce the destination precondition.
    pub fn export_retained_base(
        &self,
        destination: impl AsRef<Path>,
        base_id: [u8; 16],
        limits: RetainedExportLimits,
    ) -> Result<crate::retained_base_v2::Validated, crate::PublicationFailure> {
        self.export_retained_base_with_sync(
            destination.as_ref(),
            base_id,
            limits,
            crate::directory::sync_existing,
        )
    }

    /// Build an experimental, non-authoritative candidate. The caller must choose
    /// a destination outside the source directory and all aliases of it; this
    /// precondition cannot be enforced by the store.
    /// The source directory, destination parent and their ancestors must remain
    /// trusted and stable against uncoordinated filesystem mutation. The caller
    /// must preserve the returned binding independently for subsequent verification,
    /// including the binding returned with an uncertain publication outcome.
    /// The candidate reflects the verified source point captured during base export;
    /// concurrent writes after that point can precede candidate publication and are omitted.
    /// `max_output_bytes` bounds the total base, header, and witness bytes.
    pub fn build_retained_candidate(
        &self,
        destination: impl AsRef<Path>,
        base_id: [u8; 16],
        log_id: [u8; 16],
        limits: RetainedExportLimits,
    ) -> Result<crate::retained_candidate::Expected, crate::retained_candidate::Failure> {
        self.build_retained_candidate_with_hook(
            destination.as_ref(),
            base_id,
            log_id,
            limits,
            |_| Ok(()),
        )
    }

    fn build_retained_candidate_with_hook(
        &self,
        destination: &Path,
        base_id: [u8; 16],
        log_id: [u8; 16],
        limits: RetainedExportLimits,
        hook: impl FnMut(&str) -> io::Result<()>,
    ) -> Result<crate::retained_candidate::Expected, crate::retained_candidate::Failure> {
        self.build_retained_candidate_with_io(
            destination,
            base_id,
            log_id,
            limits,
            hook,
            |_, file, bytes| match bytes {
                Some(bytes) => file.write_all(bytes),
                None => file.sync_all(),
            },
        )
    }

    fn build_retained_candidate_with_io(
        &self,
        destination: &Path,
        base_id: [u8; 16],
        log_id: [u8; 16],
        mut limits: RetainedExportLimits,
        mut hook: impl FnMut(&str) -> io::Result<()>,
        mut output: impl FnMut(&str, &mut File, Option<&[u8]>) -> io::Result<()>,
    ) -> Result<crate::retained_candidate::Expected, crate::retained_candidate::Failure> {
        use crate::retained_candidate::Failure;
        limits.max_output_bytes = limits.max_output_bytes.checked_sub(144).ok_or_else(|| {
            Failure::NotPublished(io::Error::new(
                io::ErrorKind::InvalidInput,
                "candidate limit smaller than header and witness",
            ))
        })?;
        let mut verified_binding = None;
        let result = crate::publication_outcome::track(|tracker| {
            let parent = backup::parent_directory(destination)?;
            let mut stage = crate::staging::Stage::directory(parent)?;
            let base = self
                .export_retained_base_impl(
                    &stage.child_path(std::ffi::OsStr::new("base"))?,
                    Some(stage.path()),
                    base_id,
                    limits,
                    crate::directory::sync_existing,
                    &mut hook,
                )
                .map_err(|e| match e {
                    crate::PublicationFailure::NotPublished(e)
                    | crate::PublicationFailure::OutcomeUnknown(e) => e,
                })?;
            let expected = crate::retained_candidate::binding(&base, log_id)?;
            for (name, bytes) in [
                ("records.log", expected.header.as_slice()),
                ("store.lock", &[]),
                ("witness", expected.witness.as_slice()),
            ] {
                let path = stage.child_path(std::ffi::OsStr::new(name))?;
                let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
                #[cfg(test)]
                hook(match name {
                    "records.log" => "header_write",
                    "store.lock" => "lock_write",
                    _ => "witness_write",
                })?;
                output(name, &mut file, Some(bytes))?;
                #[cfg(test)]
                hook(match name {
                    "records.log" => "header_sync",
                    "store.lock" => "lock_sync",
                    _ => "witness_sync",
                })?;
                output(name, &mut file, None)?;
            }
            #[cfg(test)]
            hook("stage_sync")?;
            directory::sync_existing(stage.path())?;
            crate::retained_candidate::verify(
                stage.path(),
                crate::retained_base_v2::Limits {
                    file_bytes: limits.max_output_bytes,
                    entries: limits.max_live_keys as u64,
                    key_bytes: limits.max_live_key_bytes as u64,
                    work_bytes: limits.max_output_bytes.saturating_add(1),
                },
                &expected,
            )?;
            verified_binding = Some(expected.clone());
            hook("verified")?;
            tracker.begin_publication();
            hook("publish")?;
            stage.publish(destination)?;
            hook("parent_sync")?;
            directory::sync_existing(parent)?;
            stage.publication_synchronized();
            Ok(expected)
        });
        result.map_err(|failure| match failure {
            crate::PublicationFailure::NotPublished(error) => Failure::NotPublished(error),
            crate::PublicationFailure::OutcomeUnknown(error) => Failure::OutcomeUnknown {
                error,
                expected: Box::new(verified_binding.expect("publication follows verification")),
            },
        })
    }

    /// Copy and verify the exact source suffix after an independently bound candidate.
    /// Candidate, source and destination parents must remain trusted and stable;
    /// destination must be outside the source and candidate directories and aliases.
    /// This is non-authoritative and does not change the source or enable recovery.
    /// Memory includes the suffix byte cap, replay staging payload and entry overhead,
    /// plus the full-index verification limits and a fixed 64 KiB comparison buffer.
    #[allow(clippy::too_many_arguments)]
    pub fn export_retained_suffix(
        &self,
        candidate: impl AsRef<Path>,
        expected: &crate::retained_candidate::Expected,
        destination: impl AsRef<Path>,
        base_limits: crate::retained_base_v2::Limits,
        staging_limits: crate::retained_base_v2::StagingLimits,
        suffix_limits: crate::retained_candidate::SuffixLimits,
        source_limits: RetainedExportLimits,
    ) -> Result<crate::retained_candidate::SuffixExpected, SuffixExportFailure> {
        self.export_retained_suffix_impl(
            candidate.as_ref(),
            expected,
            destination.as_ref(),
            base_limits,
            staging_limits,
            suffix_limits,
            source_limits,
            crate::directory::sync_existing,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn export_retained_suffix_impl(
        &self,
        candidate: &Path,
        expected: &crate::retained_candidate::Expected,
        destination: &Path,
        base_limits: crate::retained_base_v2::Limits,
        staging_limits: crate::retained_base_v2::StagingLimits,
        suffix_limits: crate::retained_candidate::SuffixLimits,
        source_limits: RetainedExportLimits,
        sync: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<crate::retained_candidate::SuffixExpected, SuffixExportFailure> {
        let mut binding = None;
        let result = crate::publication_outcome::track(|tracker| {
            let _writer = self
                .writes
                .write()
                .map_err(|_| io::Error::other("write barrier poisoned"))?;
            let inner = self.read()?;
            if self.commits.failed() {
                return Err(uncertain_error());
            }
            let base = crate::retained_candidate::verify(candidate, base_limits, expected)?;
            if self.log.id() != Some(base.source_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "candidate source identity mismatch",
                ));
            }
            let (prefix, peak_entries, peak_key_bytes) = self.log.reconstruct_prefix_index_peaks(
                &inner.index,
                base.boundary,
                crate::log::PeakVerificationLimits {
                    max_log_bytes: source_limits.max_log_bytes,
                    max_live_keys: source_limits.max_live_keys,
                    max_live_key_bytes: source_limits.max_live_key_bytes,
                },
            )?;
            if base.peak_entries != peak_entries as u64
                || base.peak_key_bytes != peak_key_bytes as u64
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "candidate historical peaks mismatch",
                ));
            }
            let candidate_entries =
                crate::retained_candidate::load(candidate, base_limits, staging_limits, expected)?;
            if candidate_entries.len() != prefix.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "candidate prefix count mismatch",
                ));
            }
            let mut buffer = [0u8; 65536];
            for ((key, value), (source_key, location)) in
                candidate_entries.iter().zip(prefix.iter())
            {
                if key != source_key || value.len() != location.length {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "candidate prefix index mismatch",
                    ));
                }
                for (i, chunk) in value.chunks(buffer.len()).enumerate() {
                    let offset = location
                        .offset
                        .checked_add((i * buffer.len()) as u64)
                        .ok_or_else(|| io::Error::other("offset overflow"))?;
                    self.log.read_bytes(&mut buffer[..chunk.len()], offset)?;
                    if &buffer[..chunk.len()] != chunk {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "candidate prefix value mismatch",
                        ));
                    }
                }
            }
            drop(candidate_entries);
            drop(prefix);
            let end = self.log.len()?;
            let len = end.checked_sub(base.boundary).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "boundary beyond source")
            })?;
            if len > suffix_limits.bytes || len > source_limits.max_output_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "suffix byte limit exceeded",
                ));
            }
            let size = usize::try_from(len).map_err(|_| io::Error::other("suffix too large"))?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(size)
                .map_err(|_| io::Error::other("suffix allocation failed"))?;
            bytes.resize(size, 0);
            self.log.read_bytes(&mut bytes, base.boundary)?;
            let suffix_binding = crate::retained_candidate::SuffixExpected {
                candidate: expected.clone(),
                end: len,
                digest: Sha256::digest(&bytes).into(),
            };
            let replay = crate::retained_candidate::replay_suffix(
                candidate,
                base_limits,
                staging_limits,
                &bytes,
                suffix_limits,
                &suffix_binding,
            )?;
            if replay.len() != inner.index.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "replay count mismatch",
                ));
            }
            for ((key, value), (live_key, location)) in replay.iter().zip(inner.index.iter()) {
                if key != live_key || value.len() != location.length {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "replay index mismatch",
                    ));
                }
                for (i, chunk) in value.chunks(buffer.len()).enumerate() {
                    let offset = location
                        .offset
                        .checked_add((i * buffer.len()) as u64)
                        .ok_or_else(|| io::Error::other("offset overflow"))?;
                    self.log.read_bytes(&mut buffer[..chunk.len()], offset)?;
                    if &buffer[..chunk.len()] != chunk {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "replay value mismatch",
                        ));
                    }
                }
            }
            self.log.verify_witness()?;
            let parent = backup::parent_directory(destination)?;
            let (mut stage, mut file) = crate::staging::Stage::file(parent)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            file.seek(SeekFrom::Start(0))?;
            let mut staged_hash = Sha256::new();
            let mut remaining = len;
            while remaining > 0 {
                let n = remaining.min(buffer.len() as u64) as usize;
                std::io::Read::read_exact(&mut file, &mut buffer[..n])?;
                staged_hash.update(&buffer[..n]);
                remaining -= n as u64;
            }
            if file.metadata()?.len() != len
                || <[u8; 32]>::from(staged_hash.finalize()) != suffix_binding.digest
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "staged suffix mismatch",
                ));
            }
            binding = Some(suffix_binding.clone());
            tracker.begin_publication();
            stage.publish(destination)?;
            sync(parent)?;
            stage.publication_synchronized();
            Ok(suffix_binding)
        });
        result.map_err(|failure| match failure {
            crate::PublicationFailure::NotPublished(error) => {
                SuffixExportFailure::NotPublished(error)
            }
            crate::PublicationFailure::OutcomeUnknown(error) => {
                SuffixExportFailure::OutcomeUnknown {
                    error,
                    expected: Box::new(binding.expect("publication follows verification")),
                }
            }
        })
    }

    /// Export into a directory stage held by the caller. The caller must keep
    /// the source and stage trusted and stable, as for the public export.
    #[allow(dead_code)]
    pub(crate) fn export_retained_base_in_stage(
        &self,
        stage: &crate::staging::Stage,
        name: &std::ffi::OsStr,
        base_id: [u8; 16],
        limits: RetainedExportLimits,
    ) -> Result<crate::retained_base_v2::Validated, crate::PublicationFailure> {
        let destination = stage
            .child_path(name)
            .map_err(crate::PublicationFailure::NotPublished)?;
        self.export_retained_base_impl(
            &destination,
            Some(stage.path()),
            base_id,
            limits,
            crate::directory::sync_existing,
            |_| Ok(()),
        )
    }

    fn export_retained_base_with_sync(
        &self,
        destination: &Path,
        base_id: [u8; 16],
        limits: RetainedExportLimits,
        sync: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<crate::retained_base_v2::Validated, crate::PublicationFailure> {
        self.export_retained_base_with_hook(destination, base_id, limits, sync, |_| Ok(()))
    }

    fn export_retained_base_with_hook(
        &self,
        destination: &Path,
        base_id: [u8; 16],
        limits: RetainedExportLimits,
        sync: impl FnOnce(&Path) -> io::Result<()>,
        hook: impl FnMut(&str) -> io::Result<()>,
    ) -> Result<crate::retained_base_v2::Validated, crate::PublicationFailure> {
        self.export_retained_base_impl(destination, None, base_id, limits, sync, hook)
    }

    fn export_retained_base_impl(
        &self,
        destination: &Path,
        owned_parent: Option<&Path>,
        base_id: [u8; 16],
        limits: RetainedExportLimits,
        sync: impl FnOnce(&Path) -> io::Result<()>,
        mut hook: impl FnMut(&str) -> io::Result<()>,
    ) -> Result<crate::retained_base_v2::Validated, crate::PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            let _writer = self
                .writes
                .write()
                .map_err(|_| io::Error::other("write barrier poisoned"))?;
            let inner = self.read()?;
            if self.commits.failed() {
                return Err(uncertain_error());
            }
            let source = self.log.id().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "retained export requires a witnessed store",
                )
            })?;
            if base_id == [0; 16] || base_id == source {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid base identifier",
                ));
            }
            let (peak, peak_bytes) = self.log.verify_index_peaks_with_limits(
                &inner.index,
                Some(crate::log::PeakVerificationLimits {
                    max_log_bytes: limits.max_log_bytes,
                    max_live_keys: limits.max_live_keys,
                    max_live_key_bytes: limits.max_live_key_bytes,
                }),
            )?;
            hook("after_replay")?;
            let boundary = self.log.len()?;
            let mut encoded = 0u64;
            let mut key_bytes = 0u64;
            for (key, location) in inner.index.iter() {
                crate::record::validate_key(key)?;
                if location.length > 16 * 1024 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "value too large",
                    ));
                }
                key_bytes = key_bytes
                    .checked_add(key.len() as u64)
                    .ok_or_else(|| io::Error::other("key bytes overflow"))?;
                encoded = encoded
                    .checked_add(8)
                    .and_then(|n| n.checked_add(key.len() as u64))
                    .and_then(|n| n.checked_add(location.length as u64))
                    .ok_or_else(|| io::Error::other("output overflow"))?;
            }
            let size = encoded
                .checked_add(144)
                .ok_or_else(|| io::Error::other("output overflow"))?;
            if size > limits.max_output_bytes || size == u64::MAX {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "output budget exceeded",
                ));
            }
            let mut header = [0u8; 112];
            header[..8].copy_from_slice(b"EMDARB02");
            header[8..12].copy_from_slice(&2u32.to_le_bytes());
            header[12..16].copy_from_slice(&112u32.to_le_bytes());
            header[16..32].copy_from_slice(&source);
            header[32..48].copy_from_slice(&base_id);
            for (offset, value) in [
                (48, boundary),
                (56, inner.index.len() as u64),
                (64, key_bytes),
                (72, peak as u64),
                (80, peak_bytes as u64),
                (88, encoded),
            ] {
                header[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            }
            let parent = match owned_parent {
                Some(parent) => parent,
                None => backup::parent_directory(destination)?,
            };
            let (mut stage, mut file) = crate::staging::Stage::file(parent)?;
            let mut hash = Sha256::new();
            hash.update(b"EMDA retained base v2\0");
            file.write_all(&header)?;
            hash.update(header);
            let mut buffer = [0u8; 65536];
            for (key, location) in inner.index.iter() {
                let mut lengths = [0u8; 8];
                lengths[..4].copy_from_slice(&(key.len() as u32).to_le_bytes());
                lengths[4..].copy_from_slice(&(location.length as u32).to_le_bytes());
                file.write_all(&lengths)?;
                hash.update(lengths);
                file.write_all(key)?;
                hash.update(key);
                let mut crc = crc32fast::Hasher::new();
                let mut offset = location.offset;
                let mut remaining = location.length;
                while remaining > 0 {
                    let n = remaining.min(buffer.len());
                    hook("value_read")?;
                    self.log.read_bytes(&mut buffer[..n], offset)?;
                    crc.update(&buffer[..n]);
                    hash.update(&buffer[..n]);
                    file.write_all(&buffer[..n])?;
                    offset = offset
                        .checked_add(n as u64)
                        .ok_or_else(|| io::Error::other("offset overflow"))?;
                    remaining -= n;
                }
                if crc.finalize() != location.checksum {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "stored value checksum mismatch",
                    ));
                }
            }
            let digest: [u8; 32] = hash.finalize().into();
            file.write_all(&digest)?;
            hook("output_sync")?;
            file.sync_all()?;
            file.seek(SeekFrom::Start(0))?;
            let result = crate::retained_base_v2::validate(
                &mut file,
                crate::retained_base_v2::Limits {
                    file_bytes: limits.max_output_bytes,
                    entries: limits.max_live_keys as u64,
                    key_bytes: limits.max_live_key_bytes as u64,
                    work_bytes: size + 1,
                },
            )?;
            if result.digest != digest || result.source_id != source || result.boundary != boundary
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "export verification mismatch",
                ));
            }
            // Recheck source witness after the final value read.
            hook("before_witness")?;
            self.log.verify_witness()?;
            tracker.begin_publication();
            stage.publish(destination)?;
            sync(parent)?;
            stage.publication_synchronized();
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::batch::BatchOperation;

    #[test]
    fn suffix_rejects_forged_prefix_even_if_later_removed() {
        for delete in [false, true] {
            for forged_peak in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let store = Store::open(root.path().join("source")).unwrap();
                store.put(b"key", b"original-value").unwrap();
                let candidate = root.path().join("candidate");
                store
                    .build_retained_candidate(&candidate, [1; 16], [2; 16], limits())
                    .unwrap();
                let path = candidate.join("base");
                let mut bytes = std::fs::read(&path).unwrap();
                let value = bytes
                    .windows(14)
                    .position(|w| w == b"original-value")
                    .unwrap();
                bytes[value] ^= 1;
                if forged_peak {
                    bytes[72..80].copy_from_slice(&2u64.to_le_bytes());
                    bytes[80..88].copy_from_slice(&4u64.to_le_bytes());
                }
                let end = bytes.len() - 32;
                let mut hash = Sha256::new();
                hash.update(b"EMDA retained base v2\0");
                hash.update(&bytes[..end]);
                bytes[end..].copy_from_slice(&hash.finalize());
                std::fs::write(&path, &bytes).unwrap();
                let base =
                    crate::retained_base_v2::validate(&bytes[..], candidate_budgets()).unwrap();
                let expected = crate::retained_candidate::binding(&base, [2; 16]).unwrap();
                std::fs::write(candidate.join("records.log"), expected.header).unwrap();
                std::fs::write(candidate.join("witness"), expected.witness).unwrap();
                if delete {
                    store.delete(b"key").unwrap();
                } else {
                    store.put(b"key", b"replacement").unwrap();
                }
                let out = root.path().join("suffix");
                let failure = store
                    .export_retained_suffix(
                        &candidate,
                        &expected,
                        &out,
                        candidate_budgets(),
                        crate::retained_base_v2::StagingLimits {
                            value_bytes: 1000,
                            staging_bytes: 10000,
                        },
                        crate::retained_candidate::SuffixLimits {
                            bytes: 10000,
                            operations: 10,
                            work: 1000,
                        },
                        limits(),
                    )
                    .unwrap_err();
                let message = if forged_peak {
                    "candidate historical peaks mismatch"
                } else {
                    "candidate prefix value mismatch"
                };
                assert!(
                    matches!(&failure, SuffixExportFailure::NotPublished(e) if e.to_string().contains(message)),
                    "{failure:?}"
                );
                assert!(!out.exists());
                assert!(!root.path().join("selection").exists());
            }
        }
    }

    #[test]
    fn suffix_entry_rejects_wrong_source_and_superseded_corruption() {
        for corrupt in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let store = Store::open(&source).unwrap();
            store.put(b"key", b"original-value").unwrap();
            let candidate = root.path().join("candidate");
            let expected = store
                .build_retained_candidate(&candidate, [1; 16], [2; 16], limits())
                .unwrap();
            store.put(b"key", b"replacement").unwrap();
            let other = root.path().join("other");
            let other_store = Store::open(&other).unwrap();
            if corrupt {
                let path = source.join("records.log");
                let mut bytes = std::fs::read(&path).unwrap();
                let offset = bytes
                    .windows(14)
                    .position(|w| w == b"original-value")
                    .unwrap();
                bytes[offset] ^= 1;
                std::fs::write(path, bytes).unwrap();
            }
            let out = root.path().join("suffix");
            let failure = (if corrupt { &store } else { &other_store })
                .export_retained_suffix(
                    &candidate,
                    &expected,
                    &out,
                    candidate_budgets(),
                    crate::retained_base_v2::StagingLimits {
                        value_bytes: 1000,
                        staging_bytes: 10000,
                    },
                    crate::retained_candidate::SuffixLimits {
                        bytes: 10000,
                        operations: 10,
                        work: 1000,
                    },
                    limits(),
                )
                .unwrap_err();
            let message = if corrupt {
                "record checksum mismatch"
            } else {
                "candidate source identity mismatch"
            };
            assert!(
                matches!(&failure, SuffixExportFailure::NotPublished(e) if e.to_string().contains(message)),
                "{failure:?}"
            );
            assert!(!out.exists());
            assert!(!root.path().join("selection").exists());
            assert_no_candidate_stages(root.path());
        }
    }

    #[test]
    fn suffix_entry_rejects_boundaries_inside_batch() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        let candidate = root.path().join("candidate");
        store
            .build_retained_candidate(&candidate, [1; 16], [2; 16], limits())
            .unwrap();
        let batch_start = store.log.len().unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"first",
                    value: b"one",
                },
                BatchOperation::Put {
                    key: b"second",
                    value: b"two",
                },
            ])
            .unwrap();
        let log = std::fs::read(root.path().join("source/records.log")).unwrap();
        let second = log.windows(6).position(|w| w == b"second").unwrap() as u64;
        // A nested record starts before its key; locate it from its encoded bytes.
        let child =
            crate::record::encode(crate::record::Operation::Put, b"second", b"two").unwrap();
        let child_start = log.windows(child.len()).position(|w| w == child).unwrap() as u64;
        assert!(child_start > batch_start && child_start < second);
        for boundary in [child_start, child_start + 1] {
            let path = candidate.join("base");
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[48..56].copy_from_slice(&boundary.to_le_bytes());
            let end = bytes.len() - 32;
            let mut hash = Sha256::new();
            hash.update(b"EMDA retained base v2\0");
            hash.update(&bytes[..end]);
            bytes[end..].copy_from_slice(&hash.finalize());
            std::fs::write(&path, &bytes).unwrap();
            let base = crate::retained_base_v2::validate(&bytes[..], candidate_budgets()).unwrap();
            let expected = crate::retained_candidate::binding(&base, [2; 16]).unwrap();
            std::fs::write(candidate.join("records.log"), expected.header).unwrap();
            std::fs::write(candidate.join("witness"), expected.witness).unwrap();
            let out = root.path().join("suffix");
            let failure = store
                .export_retained_suffix(
                    &candidate,
                    &expected,
                    &out,
                    candidate_budgets(),
                    crate::retained_base_v2::StagingLimits {
                        value_bytes: 1000,
                        staging_bytes: 10000,
                    },
                    crate::retained_candidate::SuffixLimits {
                        bytes: 10000,
                        operations: 10,
                        work: 1000,
                    },
                    limits(),
                )
                .unwrap_err();
            assert!(
                matches!(&failure, SuffixExportFailure::NotPublished(e) if e.to_string().contains("log does not contain its acknowledged commit boundary")),
                "{failure:?}"
            );
            assert!(!out.exists());
            assert!(!root.path().join("selection").exists());
        }
    }

    #[test]
    fn suffix_export_holds_writer_through_sync() {
        use std::sync::{Arc, mpsc};
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(root.path().join("source")).unwrap());
        let candidate = root.path().join("candidate");
        let expected = store
            .build_retained_candidate(&candidate, [1; 16], [2; 16], limits())
            .unwrap();
        let output = root.path().join("suffix");
        let (entered, wait) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let exporting = Arc::clone(&store);
        let thread = std::thread::spawn(move || {
            exporting.export_retained_suffix_impl(
                &candidate,
                &expected,
                &output,
                candidate_budgets(),
                crate::retained_base_v2::StagingLimits {
                    value_bytes: 1000,
                    staging_bytes: 10000,
                },
                crate::retained_candidate::SuffixLimits {
                    bytes: 10000,
                    operations: 10,
                    work: 1000,
                },
                limits(),
                |parent| {
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                    crate::directory::sync_existing(parent)
                },
            )
        });
        wait.recv().unwrap();
        let (done, result) = mpsc::channel();
        let writer = Arc::clone(&store);
        let write = std::thread::spawn(move || {
            done.send(writer.put(b"key", b"value")).unwrap();
        });
        assert!(result.recv_timeout(Duration::from_millis(100)).is_err());
        release.send(()).unwrap();
        thread.join().unwrap().unwrap();
        result
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        write.join().unwrap();
    }

    #[test]
    fn empty_restore_has_fresh_identity_and_witness() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(
            source.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap();
        std::fs::write(
            source.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap();
        let store = Store::open(&source).unwrap();
        let candidate = root.path().join("candidate");
        let expected = store
            .build_retained_candidate(&candidate, [1; 16], [2; 16], limits())
            .unwrap();
        let staging = crate::retained_base_v2::StagingLimits {
            value_bytes: 1000,
            staging_bytes: 10000,
        };
        let suffix_limits = crate::retained_candidate::SuffixLimits {
            bytes: 10000,
            operations: 10,
            work: 1000,
        };
        let suffix = root.path().join("suffix");
        let binding = store
            .export_retained_suffix(
                &candidate,
                &expected,
                &suffix,
                candidate_budgets(),
                staging,
                suffix_limits,
                limits(),
            )
            .unwrap();
        let destination = root.path().join("restored");
        crate::retained_candidate::restore_exported_suffix(
            &candidate,
            &suffix,
            &destination,
            candidate_budgets(),
            staging,
            suffix_limits,
            &binding,
            crate::retained_candidate::RestoreLimits {
                log_bytes: 10000,
                live_keys: 0,
                live_key_bytes: 0,
            },
        )
        .unwrap();
        let restored = Store::open(&destination).unwrap();
        assert_ne!(restored.log.id().unwrap(), store.log.id().unwrap());
        restored.log.verify_witness().unwrap();
        assert!(restored.scan(None, None, 1).unwrap().is_empty());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn private_restore_store_reopens_while_forked_child_holds_descriptor() {
        use crate::test_support::fork_lock::{Child, check_reopened, isolated};

        if isolated(
            concat!(
                module_path!(),
                "::private_restore_store_reopens_while_forked_child_holds_descriptor"
            ),
            "EMDA_PRIVATE_RESTORE_LOCK_ISOLATED",
        ) {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let mut stage = crate::staging::Stage::directory(root.path()).unwrap();
        let store = Store::open(stage.path()).unwrap();
        store.put(b"key", b"value").unwrap();
        let child = Child::spawn();
        child.assert_inode(stage.path());
        let blocked = Store::open(stage.path()).err().unwrap();
        assert_eq!(blocked.kind(), io::ErrorKind::WouldBlock);

        store.close_private_store().unwrap();
        let reopened = Store::open(stage.path()).unwrap();
        reopened.log.verify_witness().unwrap();
        assert_eq!(
            reopened.get(b"key").unwrap().as_deref(),
            Some(&b"value"[..])
        );
        reopened.close_private_store().unwrap();

        let destination = root.path().join("restored");
        stage.publish(&destination).unwrap();
        check_reopened(&destination, child);
    }

    #[test]
    fn source_suffix_export_replays_and_preserves_source() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(
            source.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap();
        std::fs::write(
            source.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap();
        let store = Store::open(&source).unwrap();
        store.put(b"old", b"value").unwrap();
        store.put(b"historical-long-key", b"value").unwrap();
        let candidate = root.path().join("candidate");
        let expected = store
            .build_retained_candidate(&candidate, [1; 16], [2; 16], limits())
            .unwrap();
        store
            .write_batch(&[
                BatchOperation::Delete { key: b"old" },
                BatchOperation::Delete {
                    key: b"historical-long-key",
                },
                BatchOperation::Put {
                    key: b"new",
                    value: b"value2",
                },
            ])
            .unwrap();
        let original = std::fs::read(source.join("records.log")).unwrap();
        let snapshot = |dir: &Path| {
            let mut files: Vec<_> = std::fs::read_dir(dir)
                .unwrap()
                .map(|e| {
                    let e = e.unwrap();
                    (e.file_name(), std::fs::read(e.path()).unwrap())
                })
                .collect();
            files.sort();
            files
        };
        let source_before = snapshot(&source);
        let candidate_before = snapshot(&candidate);
        let base_limits = candidate_budgets();
        let staging = crate::retained_base_v2::StagingLimits {
            value_bytes: 1000,
            staging_bytes: 10000,
        };
        let suffix = crate::retained_candidate::SuffixLimits {
            bytes: 10000,
            operations: 10,
            work: 1000,
        };
        let out = root.path().join("suffix");
        let binding = store
            .export_retained_suffix(
                &candidate,
                &expected,
                &out,
                base_limits,
                staging,
                suffix,
                limits(),
            )
            .unwrap();
        let restored = root.path().join("restored");
        let output_limits = crate::retained_candidate::RestoreLimits {
            log_bytes: 10000,
            live_keys: 1,
            live_key_bytes: 3,
        };
        let restore =
            |destination: &Path, binding: &crate::retained_candidate::SuffixExpected, output| {
                crate::retained_candidate::restore_exported_suffix(
                    &candidate,
                    &out,
                    destination,
                    base_limits,
                    staging,
                    suffix,
                    binding,
                    output,
                )
            };
        restore(&restored, &binding, output_limits).unwrap();
        assert_eq!(
            Store::open(&restored).unwrap().get(b"new").unwrap(),
            Some(b"value2".to_vec())
        );
        {
            let reopened = Store::open(&restored).unwrap();
            assert_ne!(reopened.log.id().unwrap(), store.log.id().unwrap());
            reopened.log.verify_witness().unwrap();
        }
        for point in ["init", "write", "reopen", "check", "stage_sync", "rename"] {
            let destination = root.path().join(format!("failure-{point}"));
            let before: std::collections::BTreeSet<_> = std::fs::read_dir(root.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            let result = crate::retained_candidate::restore_with_hooks(
                &candidate,
                &out,
                &destination,
                base_limits,
                staging,
                suffix,
                &binding,
                output_limits,
                crate::directory::sync_existing,
                |at| {
                    if at == point {
                        Err(std::io::Error::other("injected"))
                    } else {
                        Ok(())
                    }
                },
            );
            if point == "rename" {
                assert!(
                    matches!(result, Err(crate::PublicationFailure::OutcomeUnknown(_))),
                    "{point}: {result:?}"
                );
            } else {
                assert!(
                    matches!(result, Err(crate::PublicationFailure::NotPublished(_))),
                    "{point}: {result:?}"
                );
            }
            assert!(!destination.exists());
            let after: std::collections::BTreeSet<_> = std::fs::read_dir(root.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            assert_eq!(before, after, "stage cleanup at {point}");
            assert_eq!(snapshot(&source), source_before);
            assert_eq!(snapshot(&candidate), candidate_before);
        }
        assert!(matches!(
            restore(
                &root.path().join("zero-keys"),
                &binding,
                crate::retained_candidate::RestoreLimits {
                    live_keys: 0,
                    ..output_limits
                }
            ),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        assert!(restore(&restored, &binding, output_limits).is_err());
        {
            let reopened = Store::open(&restored).unwrap();
            reopened.put(b"later", b"write").unwrap();
        }
        assert_eq!(
            Store::open(&restored).unwrap().get(b"later").unwrap(),
            Some(b"write".to_vec())
        );
        // Use an unmodified restore to measure its exact output size.
        let measured = root.path().join("measured");
        restore(&measured, &binding, output_limits).unwrap();
        let inside = candidate.join("nested");
        assert!(matches!(
            restore(&inside, &binding, output_limits),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        #[cfg(unix)]
        {
            let alias = root.path().join("candidate-alias");
            std::os::unix::fs::symlink(&candidate, &alias).unwrap();
            assert!(matches!(
                restore(&alias.join("nested"), &binding, output_limits),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
            let link = root.path().join("existing-link");
            std::os::unix::fs::symlink(&out, &link).unwrap();
            assert!(matches!(
                restore(&link, &binding, output_limits),
                Err(crate::PublicationFailure::OutcomeUnknown(_))
            ));
            assert_eq!(std::fs::read_link(&link).unwrap(), out);
        }
        let existing_file = root.path().join("existing-file");
        std::fs::write(&existing_file, b"keep").unwrap();
        assert!(matches!(
            restore(&existing_file, &binding, output_limits),
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert_eq!(std::fs::read(&existing_file).unwrap(), b"keep");
        let existing_dir = root.path().join("existing-dir");
        std::fs::create_dir(&existing_dir).unwrap();
        assert!(matches!(
            restore(&existing_dir, &binding, output_limits),
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert_eq!(std::fs::read_dir(&existing_dir).unwrap().count(), 0);
        let race = root.path().join("race");
        std::thread::scope(|scope| {
            let a = scope.spawn(|| restore(&race, &binding, output_limits));
            let b = scope.spawn(|| restore(&race, &binding, output_limits));
            let results = [a.join().unwrap(), b.join().unwrap()];
            assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
            assert_eq!(
                results
                    .iter()
                    .filter(|result| matches!(
                        result,
                        Err(crate::PublicationFailure::OutcomeUnknown(_))
                    ))
                    .count(),
                1
            );
        });
        assert_eq!(
            Store::open(&race).unwrap().get(b"new").unwrap(),
            Some(b"value2".to_vec())
        );
        let exact_log = std::fs::metadata(measured.join("records.log"))
            .unwrap()
            .len();
        let exact = root.path().join("exact");
        restore(
            &exact,
            &binding,
            crate::retained_candidate::RestoreLimits {
                log_bytes: exact_log,
                ..output_limits
            },
        )
        .unwrap();
        let too_small = root.path().join("too-small");
        assert!(
            restore(
                &too_small,
                &binding,
                crate::retained_candidate::RestoreLimits {
                    log_bytes: exact_log - 1,
                    ..output_limits
                }
            )
            .is_err()
        );
        assert!(!too_small.exists());
        let corrupt = root.path().join("corrupt-suffix");
        let mut damaged = std::fs::read(&out).unwrap();
        damaged[0] ^= 1;
        std::fs::write(&corrupt, damaged).unwrap();
        assert!(
            crate::retained_candidate::restore_exported_suffix(
                &candidate,
                &corrupt,
                &too_small,
                base_limits,
                staging,
                suffix,
                &binding,
                output_limits
            )
            .is_err()
        );
        assert!(!too_small.exists());
        let mut wrong = binding.clone();
        wrong.digest[0] ^= 1;
        let rejected = root.path().join("rejected");
        assert!(restore(&rejected, &wrong, output_limits).is_err());
        assert!(!rejected.exists());
        assert!(
            restore(
                &rejected,
                &binding,
                crate::retained_candidate::RestoreLimits {
                    live_key_bytes: 2,
                    ..output_limits
                }
            )
            .is_err()
        );
        assert!(!rejected.exists());
        let published = root.path().join("injected-published");
        assert!(matches!(
            crate::retained_candidate::restore_with_hooks(
                &candidate,
                &out,
                &published,
                base_limits,
                staging,
                suffix,
                &binding,
                output_limits,
                crate::directory::sync_existing,
                |at| if at == "published" {
                    Err(std::io::Error::other("injected"))
                } else {
                    Ok(())
                },
            ),
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert_eq!(
            Store::open(&published).unwrap().get(b"new").unwrap(),
            Some(b"value2".to_vec())
        );
        let uncertain = root.path().join("uncertain");
        assert!(matches!(
            crate::retained_candidate::restore_with_sync(
                &candidate,
                &out,
                &uncertain,
                base_limits,
                staging,
                suffix,
                &binding,
                output_limits,
                |_| Err(std::io::Error::other("injected fsync failure")),
            ),
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert!(uncertain.exists());
        let bytes = std::fs::read(&out).unwrap();
        let open = |path: &Path, budget, binding: &crate::retained_candidate::SuffixExpected| {
            crate::retained_candidate::open_exported_suffix(
                &candidate,
                path,
                base_limits,
                staging,
                budget,
                binding,
            )
        };
        assert_eq!(
            open(&out, suffix, &binding).unwrap(),
            vec![(b"new".to_vec(), b"value2".to_vec())]
        );
        assert!(
            open(
                &out,
                crate::retained_candidate::SuffixLimits { bytes: 0, ..suffix },
                &binding
            )
            .is_err()
        );
        let mut swapped = binding.clone();
        swapped.candidate.header[8] ^= 1;
        assert!(open(&out, suffix, &swapped).is_err());
        swapped = binding.clone();
        swapped.digest[0] ^= 1;
        assert!(open(&out, suffix, &swapped).is_err());
        let bad = root.path().join("bad-suffix");
        let mut corrupt = bytes.clone();
        corrupt[0] ^= 1;
        std::fs::write(&bad, &corrupt).unwrap();
        assert!(open(&bad, suffix, &binding).is_err());
        std::fs::write(&bad, &bytes[..bytes.len() - 1]).unwrap();
        assert!(open(&bad, suffix, &binding).is_err());
        let mut extended = bytes.clone();
        extended.push(0);
        std::fs::write(&bad, extended).unwrap();
        assert!(open(&bad, suffix, &binding).is_err());
        #[cfg(unix)]
        {
            let link = root.path().join("suffix-link");
            std::os::unix::fs::symlink(&out, &link).unwrap();
            assert!(open(&link, suffix, &binding).is_err());
        }
        assert_eq!(
            crate::retained_candidate::replay_suffix(
                &candidate,
                base_limits,
                staging,
                &bytes,
                suffix,
                &binding
            )
            .unwrap(),
            vec![(b"new".to_vec(), b"value2".to_vec())]
        );
        let conflict = store
            .export_retained_suffix(
                &candidate,
                &expected,
                &out,
                base_limits,
                staging,
                suffix,
                limits(),
            )
            .unwrap_err();
        assert!(
            matches!(conflict, SuffixExportFailure::OutcomeUnknown { error, .. } if error.kind() == io::ErrorKind::AlreadyExists)
        );
        assert_eq!(std::fs::read(&out).unwrap(), bytes);
        assert_eq!(std::fs::read(source.join("records.log")).unwrap(), original);
        assert_eq!(snapshot(&source), source_before);
        assert_eq!(snapshot(&candidate), candidate_before);
        let tiny = crate::retained_candidate::SuffixLimits { bytes: 0, ..suffix };
        let small = root.path().join("small");
        let failure = store
            .export_retained_suffix(
                &candidate,
                &expected,
                &small,
                base_limits,
                staging,
                tiny,
                limits(),
            )
            .unwrap_err();
        assert!(
            matches!(failure, SuffixExportFailure::NotPublished(e) if e.to_string().contains("suffix byte limit exceeded"))
        );
        assert!(!small.exists());
        assert!(!root.path().join("selection").exists());
        let uncertain = store.export_retained_suffix_impl(
            &candidate,
            &expected,
            &root.path().join("uncertain"),
            base_limits,
            staging,
            suffix,
            limits(),
            |_| Err(io::Error::other("sync")),
        );
        assert!(matches!(
            uncertain,
            Err(SuffixExportFailure::OutcomeUnknown { .. })
        ));
    }

    #[test]
    fn full_index_verification_requires_an_exact_historical_boundary() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        store.put(b"key", b"old").unwrap();
        let boundary = store.log.len().unwrap();
        store.put(b"key", b"new").unwrap();
        let batch_start = store.log.len().unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"b",
                    value: b"v",
                },
                BatchOperation::Delete { key: b"key" },
            ])
            .unwrap();
        let batch_end = store.log.len().unwrap();
        let inner = store.read().unwrap();
        assert!(
            store
                .log
                .verify_index_peaks_at_boundary(&inner.index, None, Some(boundary))
                .is_ok()
        );
        assert!(
            store
                .log
                .verify_index_peaks_at_boundary(&inner.index, None, Some(boundary - 1))
                .is_err()
        );
        assert!(
            store
                .log
                .verify_index_peaks_at_boundary(&inner.index, None, Some(batch_start))
                .is_ok()
        );
        assert!(
            store
                .log
                .verify_index_peaks_at_boundary(&inner.index, None, Some(batch_start + 1))
                .is_err()
        );
        assert!(
            store
                .log
                .verify_index_peaks_at_boundary(&inner.index, None, Some(batch_end))
                .is_ok()
        );
        assert!(
            store
                .log
                .verify_index_peaks_at_boundary(
                    &inner.index,
                    None,
                    Some(store.log.len().unwrap() + 1),
                )
                .is_err()
        );
    }

    fn limits() -> RetainedExportLimits {
        RetainedExportLimits {
            max_log_bytes: 10_000_000,
            max_live_keys: 10,
            max_live_key_bytes: 100,
            max_output_bytes: 10_000_000,
        }
    }
    fn candidate_budgets() -> crate::retained_base_v2::Limits {
        crate::retained_base_v2::Limits {
            file_bytes: 10000,
            entries: 10,
            key_bytes: 100,
            work_bytes: 10001,
        }
    }

    fn assert_no_candidate_stages(root: &Path) {
        assert!(!std::fs::read_dir(root).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".emda-stage-v1-")
        }));
    }

    #[test]
    fn candidate_partial_output_and_sync_failures_are_not_published() {
        struct Partial<'a> {
            file: &'a mut File,
            written: usize,
            zero: bool,
        }
        impl Write for Partial<'_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.written == 0 {
                    let n = self.file.write(&bytes[..bytes.len().min(7)])?;
                    self.written += n;
                    Ok(n)
                } else if self.zero {
                    Ok(0)
                } else {
                    Err(io::Error::other("injected after short write"))
                }
            }
            fn flush(&mut self) -> io::Result<()> {
                self.file.flush()
            }
        }
        for target in ["records.log", "witness"] {
            for fault in ["error", "zero", "sync"] {
                let root = tempfile::tempdir().unwrap();
                let source = root.path().join("source");
                std::fs::create_dir(&source).unwrap();
                std::fs::write(
                    source.join("records.log"),
                    include_bytes!("../../tests/fixtures/v2-empty-records.log"),
                )
                .unwrap();
                std::fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
                )
                .unwrap();
                let store = Store::open(&source).unwrap();
                store.put(b"key", b"value").unwrap();
                let log = std::fs::read(source.join("records.log")).unwrap();
                let witness = std::fs::read(source.join("commit.state")).unwrap();
                let dest = root.path().join("candidate");
                let mut injected = false;
                let failure = store
                    .build_retained_candidate_with_io(
                        &dest,
                        [1; 16],
                        [2; 16],
                        limits(),
                        |_| Ok(()),
                        |name, file, bytes| match bytes {
                            Some(bytes) if name == target && fault != "sync" => {
                                let mut writer = Partial {
                                    file,
                                    written: 0,
                                    zero: fault == "zero",
                                };
                                let result = writer.write_all(bytes);
                                assert_eq!(writer.written, 7);
                                assert_eq!(writer.file.metadata()?.len(), 7);
                                injected = true;
                                result
                            }
                            Some(bytes) => file.write_all(bytes),
                            None if name == target && fault == "sync" => {
                                assert!(file.metadata()?.len() > 0);
                                injected = true;
                                Err(io::Error::other("injected sync failure"))
                            }
                            None => file.sync_all(),
                        },
                    )
                    .unwrap_err();
                assert!(injected, "{target} {fault}");
                let crate::retained_candidate::Failure::NotPublished(error) = failure else {
                    panic!("unexpected publication outcome for {target} {fault}");
                };
                assert_eq!(
                    error.kind(),
                    if fault == "zero" {
                        io::ErrorKind::WriteZero
                    } else {
                        io::ErrorKind::Other
                    }
                );
                assert!(!dest.exists());
                assert_eq!(std::fs::read(source.join("records.log")).unwrap(), log);
                assert_eq!(std::fs::read(source.join("commit.state")).unwrap(), witness);
                assert_no_candidate_stages(root.path());
            }
        }
    }

    #[test]
    fn candidate_injected_io_and_publication_outcomes() {
        for point in [
            "header_write",
            "header_sync",
            "lock_write",
            "lock_sync",
            "witness_write",
            "witness_sync",
            "stage_sync",
            "verified",
            "publish",
            "parent_sync",
            "conflict",
        ] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let store = Store::open(&source).unwrap();
            store.put(b"key", b"value").unwrap();
            let log = std::fs::read(source.join("records.log")).unwrap();
            let witness = std::fs::read(source.join("commit.state")).ok();
            let dest = root.path().join("candidate");
            if point == "conflict" {
                std::fs::create_dir(&dest).unwrap();
                std::fs::write(dest.join("sentinel"), b"untouched").unwrap();
            }
            let failure = store
                .build_retained_candidate_with_hook(&dest, [1; 16], [2; 16], limits(), |at| {
                    if at == point {
                        Err(io::Error::other("injected"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            match point {
                "publish" => {
                    assert!(matches!(
                        failure,
                        crate::retained_candidate::Failure::OutcomeUnknown { .. }
                    ));
                    assert!(!dest.exists());
                }
                "parent_sync" => {
                    let crate::retained_candidate::Failure::OutcomeUnknown { expected, .. } =
                        failure
                    else {
                        panic!("wrong outcome")
                    };
                    crate::retained_candidate::verify(&dest, candidate_budgets(), &expected)
                        .unwrap();
                }
                "conflict" => {
                    let crate::retained_candidate::Failure::OutcomeUnknown { expected, .. } =
                        failure
                    else {
                        panic!("wrong outcome")
                    };
                    assert_eq!(std::fs::read(dest.join("sentinel")).unwrap(), b"untouched");
                    assert!(
                        crate::retained_candidate::verify(&dest, candidate_budgets(), &expected)
                            .is_err()
                    );
                }
                _ => {
                    assert!(
                        matches!(failure, crate::retained_candidate::Failure::NotPublished(_)),
                        "{point}"
                    );
                    assert!(!dest.exists());
                }
            }
            assert_eq!(std::fs::read(source.join("records.log")).unwrap(), log);
            assert_eq!(std::fs::read(source.join("commit.state")).ok(), witness);
            assert_no_candidate_stages(root.path());
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn candidate_source_faults_during_base_export() {
        use super::super::fault_tests::{Fault, open_versioned_fault_store};
        use std::sync::{Arc, Mutex};

        for point in [
            "replay",
            "value_read",
            "output_sync",
            "after_replay",
            "before_witness",
        ] {
            for mutation in if point == "after_replay" || point == "before_witness" {
                vec!["stale", "malformed", "identity", "append"]
            } else {
                vec!["io"]
            } {
                let root = tempfile::tempdir().unwrap();
                let source = root.path().join("source");
                std::fs::create_dir(&source).unwrap();
                let fault = Arc::new(Mutex::new(None));
                let store = open_versioned_fault_store(&source, Arc::clone(&fault)).unwrap();
                store.put(b"key", b"value").unwrap();
                let log_path = source.join("records.log");
                let marker = source.join("commit.state");
                let log = std::fs::read(&log_path).unwrap();
                // The marker is the V2 witness on macOS. Do not claim V3 coverage here.
                assert!(log.starts_with(b"EMDALOG2"));
                let witness = std::fs::read(&marker).unwrap();
                let changed = match mutation {
                    "stale" => crate::witness::encode_for_test(&store.log.id().unwrap(), 0),
                    "identity" => {
                        crate::witness::encode_for_test(&[9; 16], store.log.len().unwrap())
                    }
                    "malformed" => vec![0; witness.len()],
                    _ => witness.clone(),
                };
                let dest = root.path().join("candidate");
                if point == "replay" {
                    *fault.lock().unwrap() = Some(Fault::ShortRead(1));
                }
                let mut reached = 0;
                let result = store.build_retained_candidate_with_hook(
                    &dest,
                    [1; 16],
                    [2; 16],
                    limits(),
                    |at| {
                        if at == point {
                            reached += 1;
                            match mutation {
                                "io" if point == "value_read" => {
                                    *fault.lock().unwrap() = Some(Fault::ShortRead(1))
                                }
                                "io" if point == "output_sync" => {
                                    return Err(io::Error::other("injected sync failure"));
                                }
                                "append" => OpenOptions::new()
                                    .append(true)
                                    .open(&log_path)?
                                    .write_all(b"unexpected")?,
                                "stale" | "malformed" | "identity" => {
                                    std::fs::write(&marker, &changed)?
                                }
                                _ => {}
                            }
                        }
                        Ok(())
                    },
                );
                match result {
                    Err(crate::retained_candidate::Failure::NotPublished(error)) => {
                        if mutation != "io" {
                            assert_eq!(
                                error.kind(),
                                io::ErrorKind::InvalidData,
                                "{point} {mutation}"
                            );
                        }
                    }
                    other => panic!("unexpected {point} {mutation}: {other:?}"),
                }
                assert_eq!(
                    reached,
                    usize::from(point != "replay"),
                    "{point} {mutation}"
                );
                assert!(!dest.exists());
                let mut expected_log = log;
                if mutation == "append" {
                    expected_log.extend_from_slice(b"unexpected");
                }
                assert_eq!(std::fs::read(&log_path).unwrap(), expected_log);
                assert_eq!(std::fs::read(&marker).unwrap(), changed);
                assert_no_candidate_stages(root.path());
            }
        }
    }

    #[test]
    fn candidate_kill_child() {
        let Ok(root) = std::env::var("EMDA_CANDIDATE_KILL_ROOT") else {
            return;
        };
        let point = std::env::var("EMDA_CANDIDATE_KILL_POINT").unwrap();
        let root = Path::new(&root);
        let store = Store::open(root.join("source")).unwrap();
        let _ = store.build_retained_candidate_with_hook(
            &root.join("candidate"),
            [1; 16],
            [2; 16],
            limits(),
            |at| {
                if at == point {
                    println!("CANDIDATE_READY");
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                Ok(())
            },
        );
        panic!("boundary not reached");
    }

    #[test]
    fn candidate_process_kill_boundaries() {
        use std::io::BufRead;
        use std::process::{Command, Stdio};
        // A running OS survives these kills; this does not simulate power loss.
        for point in [
            "header_write",
            "stage_sync",
            "verified",
            "publish",
            "parent_sync",
        ] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let store = Store::open(&source).unwrap();
            store.put(b"key", b"value").unwrap();
            // Retain a binding independently before starting the child, from the same
            // unchanged source and identifiers; never infer it from the killed stage.
            let expected = store
                .build_retained_candidate(root.path().join("reference"), [1; 16], [2; 16], limits())
                .unwrap();
            let log = std::fs::read(source.join("records.log")).unwrap();
            let witness = std::fs::read(source.join("commit.state")).ok();
            drop(store);
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::retained_export::tests::candidate_kill_child",
                    "--nocapture",
                ])
                .env("EMDA_CANDIDATE_KILL_ROOT", root.path())
                .env("EMDA_CANDIDATE_KILL_POINT", point)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let reader = std::thread::spawn(move || {
                let ready = std::io::BufReader::new(stdout)
                    .lines()
                    .any(|line| line.is_ok_and(|line| line.contains("CANDIDATE_READY")));
                let _ = ready_tx.send(ready);
            });
            let ready = ready_rx.recv_timeout(std::time::Duration::from_secs(20));
            if !matches!(ready, Ok(true)) {
                let _ = child.kill();
                let _ = child.wait();
                reader.join().unwrap();
                panic!("child did not reach {point}");
            }
            child.kill().unwrap();
            child.wait().unwrap();
            reader.join().unwrap();
            let dest = root.path().join("candidate");
            if point == "parent_sync" {
                crate::retained_candidate::verify(&dest, candidate_budgets(), &expected).unwrap();
            } else {
                assert!(!dest.exists(), "{point}");
            }
            assert_eq!(std::fs::read(source.join("records.log")).unwrap(), log);
            assert_eq!(std::fs::read(source.join("commit.state")).ok(), witness);
            crate::staging::cleanup_abandoned_staging(root.path()).unwrap();
            assert_no_candidate_stages(root.path());
        }
    }

    #[test]
    fn restore_kill_child() {
        let Ok(root) = std::env::var("EMDA_RESTORE_KILL_ROOT") else {
            return;
        };
        let root = Path::new(&root);
        let point = std::env::var("EMDA_RESTORE_KILL_POINT").unwrap();
        let bytes = std::fs::read(root.join("binding")).unwrap();
        let binding = crate::retained_candidate::SuffixExpected {
            candidate: crate::retained_candidate::Expected {
                header: bytes[0..112].try_into().unwrap(),
                witness: bytes[112..144].try_into().unwrap(),
                base_digest: bytes[144..176].try_into().unwrap(),
            },
            end: u64::from_le_bytes(bytes[176..184].try_into().unwrap()),
            digest: bytes[184..216].try_into().unwrap(),
        };
        let writes = std::cell::Cell::new(0);
        let _ = crate::retained_candidate::restore_with_hooks(
            &root.join("candidate"),
            &root.join("suffix"),
            &root.join("output"),
            candidate_budgets(),
            crate::retained_base_v2::StagingLimits {
                value_bytes: 1000,
                staging_bytes: 10000,
            },
            crate::retained_candidate::SuffixLimits {
                bytes: 10000,
                operations: 10,
                work: 1000,
            },
            &binding,
            crate::retained_candidate::RestoreLimits {
                log_bytes: 10000,
                live_keys: 10,
                live_key_bytes: 1000,
            },
            crate::directory::sync_existing,
            |at| {
                if at == "write" {
                    writes.set(writes.get() + 1);
                }
                if at == point || (point == "write2" && at == "write" && writes.get() == 2) {
                    println!("RESTORE_READY");
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                Ok(())
            },
        );
        panic!("boundary not reached");
    }

    #[test]
    fn restore_process_kill_boundaries() {
        use std::io::BufRead;
        use std::process::{Command, Stdio};
        for point in [
            "init",
            "write",
            "write2",
            "reopen",
            "check",
            "stage_sync",
            "rename",
            "published",
        ] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let store = Store::open(&source).unwrap();
            store.put(b"old", b"base").unwrap();
            let candidate = root.path().join("candidate");
            let expected = store
                .build_retained_candidate(&candidate, [1; 16], [2; 16], limits())
                .unwrap();
            store.put(b"new", b"suffix").unwrap();
            let suffix = root.path().join("suffix");
            let staging = crate::retained_base_v2::StagingLimits {
                value_bytes: 1000,
                staging_bytes: 10000,
            };
            let suffix_limits = crate::retained_candidate::SuffixLimits {
                bytes: 10000,
                operations: 10,
                work: 1000,
            };
            let binding = store
                .export_retained_suffix(
                    &candidate,
                    &expected,
                    &suffix,
                    candidate_budgets(),
                    staging,
                    suffix_limits,
                    limits(),
                )
                .unwrap();
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&binding.candidate.header);
            bytes.extend_from_slice(&binding.candidate.witness);
            bytes.extend_from_slice(&binding.candidate.base_digest);
            bytes.extend_from_slice(&binding.end.to_le_bytes());
            bytes.extend_from_slice(&binding.digest);
            std::fs::write(root.path().join("binding"), bytes).unwrap();
            drop(store);
            let snapshot = |dir: &Path| {
                let mut files: Vec<_> = std::fs::read_dir(dir)
                    .unwrap()
                    .map(|e| {
                        let e = e.unwrap();
                        (e.file_name(), std::fs::read(e.path()).unwrap())
                    })
                    .collect();
                files.sort();
                files
            };
            let source_before = snapshot(&source);
            let candidate_before = snapshot(&candidate);
            let suffix_before = std::fs::read(&suffix).unwrap();
            let sentinel = root.path().join(".emda-stage-v1-d-unowned.owner");
            std::fs::write(&sentinel, b"unowned").unwrap();
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::retained_export::tests::restore_kill_child",
                    "--nocapture",
                ])
                .env("EMDA_RESTORE_KILL_ROOT", root.path())
                .env("EMDA_RESTORE_KILL_POINT", point)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let reader = std::thread::spawn(move || {
                let ready = std::io::BufReader::new(stdout)
                    .lines()
                    .any(|l| l.is_ok_and(|l| l.contains("RESTORE_READY")));
                let _ = tx.send(ready);
            });
            let ready = rx.recv_timeout(std::time::Duration::from_secs(20));
            if !matches!(ready, Ok(true)) {
                let _ = child.kill();
                let _ = child.wait();
                reader.join().unwrap();
                panic!("child did not reach {point}");
            }
            child.kill().unwrap();
            child.wait().unwrap();
            reader.join().unwrap();
            let output = root.path().join("output");
            if output.exists() {
                let reopened = Store::open(&output).unwrap();
                assert_eq!(
                    reopened.scan(None, None, 10).unwrap(),
                    vec![
                        (b"new".to_vec(), b"suffix".to_vec()),
                        (b"old".to_vec(), b"base".to_vec())
                    ]
                );
            } else {
                assert_ne!(point, "published");
            }
            assert_eq!(snapshot(&source), source_before);
            assert_eq!(snapshot(&candidate), candidate_before);
            assert_eq!(std::fs::read(&suffix).unwrap(), suffix_before);
            crate::staging::cleanup_abandoned_staging(root.path()).unwrap();
            assert_eq!(std::fs::read(&sentinel).unwrap(), b"unowned");
            if output.exists() {
                assert_eq!(
                    Store::open(&output)
                        .unwrap()
                        .scan(None, None, 10)
                        .unwrap()
                        .len(),
                    2
                );
            }
        }
    }

    #[test]
    fn candidate_load_is_bounded_and_transactional() {
        use crate::retained_base_v2::StagingLimits;
        use crate::retained_candidate::load;
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        store.put(b"a", b"one").unwrap();
        store.put(b"b", b"two").unwrap();
        let dest = root.path().join("candidate");
        let expected = store
            .build_retained_candidate(&dest, [1; 16], [2; 16], limits())
            .unwrap();
        let stage = StagingLimits {
            value_bytes: 3,
            staging_bytes: 8,
        };
        assert_eq!(
            load(&dest, candidate_budgets(), stage, &expected).unwrap(),
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec())
            ]
        );
        assert!(
            load(
                &dest,
                candidate_budgets(),
                StagingLimits {
                    value_bytes: 2,
                    ..stage
                },
                &expected
            )
            .is_err()
        );
        assert!(
            load(
                &dest,
                candidate_budgets(),
                StagingLimits {
                    staging_bytes: 7,
                    ..stage
                },
                &expected
            )
            .is_err()
        );
        let mut wrong = expected.clone();
        wrong.base_digest[0] ^= 1;
        assert!(load(&dest, candidate_budgets(), stage, &wrong).is_err());
        let path = dest.join("base");
        let original = std::fs::read(&path).unwrap();
        let mut corrupt = original.clone();
        let index = corrupt.len() - 33;
        corrupt[index] ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        assert!(load(&dest, candidate_budgets(), stage, &expected).is_err());
        std::fs::write(&path, &original[..original.len() - 1]).unwrap();
        assert!(load(&dest, candidate_budgets(), stage, &expected).is_err());
        assert_eq!(std::fs::read_dir(&dest).unwrap().count(), 4);
    }
    #[test]
    fn detached_suffix_replay_checks_binding_records_and_intermediate_limits() {
        use crate::record::batch::{self, BatchOperation};
        use crate::record::{self, Operation};
        use crate::retained_base_v2::StagingLimits;
        use crate::retained_candidate::{SuffixExpected, SuffixLimits, replay_suffix};
        use sha2::{Digest, Sha256};
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        store.put(b"a", b"one").unwrap();
        let dest = root.path().join("candidate");
        let candidate = store
            .build_retained_candidate(&dest, [1; 16], [2; 16], limits())
            .unwrap();
        let batch = batch::encode_payload(&[
            BatchOperation::Put {
                key: b"b",
                value: b"two",
            },
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put {
                key: b"b",
                value: b"x",
            },
        ])
        .unwrap();
        let suffix = record::encode(Operation::Batch, &[0], &batch).unwrap();
        let binding = |bytes: &[u8]| SuffixExpected {
            candidate: candidate.clone(),
            end: bytes.len() as u64,
            digest: Sha256::digest(bytes).into(),
        };
        let stage = StagingLimits {
            value_bytes: 3,
            staging_bytes: 8,
        };
        let cap = SuffixLimits {
            bytes: 1000,
            operations: 3,
            work: 100,
        };
        let run = |bytes: &[u8], expected: &SuffixExpected, stage, cap| {
            replay_suffix(&dest, candidate_budgets(), stage, bytes, cap, expected)
        };
        assert_eq!(
            run(&suffix, &binding(&suffix), stage, cap).unwrap(),
            vec![(b"b".to_vec(), b"x".to_vec())]
        );
        assert!(
            run(
                &suffix,
                &binding(&suffix),
                StagingLimits {
                    staging_bytes: 7,
                    ..stage
                },
                cap
            )
            .is_err()
        );
        // One initial entry visit, then conservative charges of 4, 7 and 4.
        assert!(
            run(
                &suffix,
                &binding(&suffix),
                stage,
                SuffixLimits { work: 16, ..cap }
            )
            .is_ok()
        );
        assert!(
            run(
                &suffix,
                &binding(&suffix),
                stage,
                SuffixLimits { work: 15, ..cap }
            )
            .is_err()
        );
        assert!(run(&[], &binding(&[]), stage, SuffixLimits { work: 0, ..cap }).is_err());
        assert_eq!(
            run(&[], &binding(&[]), stage, SuffixLimits { work: 1, ..cap }).unwrap(),
            vec![(b"a".to_vec(), b"one".to_vec())]
        );
        // Every replay allocation failure returns an error, including after prior
        // private mutations in the batch. No partial result can escape.
        for fail_at in 0..5 {
            crate::retained_candidate::FAIL_ALLOCATION.with(|slot| slot.set(Some(fail_at)));
            let result = run(&suffix, &binding(&suffix), stage, cap);
            crate::retained_candidate::FAIL_ALLOCATION.with(|slot| slot.set(None));
            assert_eq!(
                result.unwrap_err().to_string(),
                "injected allocation failure"
            );
        }
        let large_batch = batch::encode_payload(&vec![
            BatchOperation::Delete { key: b"z" };
            batch::MAX_ATOMIC_BATCH_OPERATIONS
        ])
        .unwrap();
        let large_suffix = record::encode(Operation::Batch, &[0], &large_batch).unwrap();
        let large_cap = SuffixLimits {
            bytes: large_suffix.len() as u64,
            operations: batch::MAX_ATOMIC_BATCH_OPERATIONS as u64,
            work: 0,
        };
        assert!(run(&large_suffix, &binding(&large_suffix), stage, large_cap).is_err());
        let zero_work = run(
            &suffix,
            &binding(&suffix),
            stage,
            SuffixLimits { work: 0, ..cap },
        );
        assert!(zero_work.is_err());
        let mut bad_checksum = suffix.clone();
        *bad_checksum.last_mut().unwrap() ^= 1;
        let err = run(
            &bad_checksum,
            &binding(&bad_checksum),
            stage,
            SuffixLimits {
                operations: 0,
                ..cap
            },
        )
        .unwrap_err();
        assert_ne!(err.to_string(), "suffix operation limit exceeded");
        let nested = record::encode(Operation::Batch, &[0], &batch).unwrap();
        let mut nested_payload = 1u32.to_le_bytes().to_vec();
        nested_payload.extend(nested);
        let nested_outer = record::encode(Operation::Batch, &[0], &nested_payload).unwrap();
        assert!(run(&nested_outer, &binding(&nested_outer), stage, cap).is_err());
        let empty = [];
        assert!(
            run(
                &empty,
                &binding(&empty),
                stage,
                SuffixLimits {
                    bytes: 0,
                    operations: 0,
                    work: 1
                }
            )
            .is_ok()
        );
        // A count over budget is rejected even when nested bytes are malformed.
        let malformed = record::encode(Operation::Batch, &[0], &3u32.to_le_bytes()).unwrap();
        let err = run(
            &malformed,
            &binding(&malformed),
            stage,
            SuffixLimits {
                operations: 2,
                ..cap
            },
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "suffix operation limit exceeded");
        assert!(
            run(
                &suffix,
                &binding(&suffix),
                stage,
                SuffixLimits {
                    operations: 2,
                    ..cap
                }
            )
            .is_err()
        );
        assert!(
            run(
                &suffix,
                &binding(&suffix),
                stage,
                SuffixLimits { bytes: 1, ..cap }
            )
            .is_err()
        );
        let mut singles = record::encode(Operation::Put, b"a", b"new").unwrap();
        singles.extend(record::encode(Operation::Delete, b"a", b"").unwrap());
        assert!(
            run(&singles, &binding(&singles), stage, cap)
                .unwrap()
                .is_empty()
        );
        let mut wrong = binding(&suffix);
        wrong.end += 1;
        assert!(run(&suffix, &wrong, stage, cap).is_err());
        let mut wrong = binding(&suffix);
        wrong.candidate.base_digest[0] ^= 1;
        assert!(run(&suffix, &wrong, stage, cap).is_err());
        let mut corrupt = suffix.clone();
        corrupt[20] ^= 1;
        assert!(run(&corrupt, &binding(&corrupt), stage, cap).is_err());
        // Recompute the outer checksum to exercise nested validation independently.
        let mut broken_batch = batch.clone();
        broken_batch[10] ^= 1;
        let broken_outer = record::encode(Operation::Batch, &[0], &broken_batch).unwrap();
        assert!(run(&broken_outer, &binding(&broken_outer), stage, cap).is_err());
        let truncated = &suffix[..suffix.len() - 1];
        assert!(run(truncated, &binding(truncated), stage, cap).is_err());
        assert!(run(&corrupt, &binding(&suffix), stage, cap).is_err());
    }
    #[test]
    fn seeded_suffix_matches_full_log_store_and_preserves_inputs() {
        use crate::retained_base_v2::StagingLimits;
        use crate::retained_candidate::{
            IndexLimits, IndexedSuffix, OverlayLimits, SuffixExpected, SuffixLimits, replay_suffix,
        };
        use std::collections::BTreeMap;

        for seed in 1u64..=8 {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let store = Store::open(&source).unwrap();
            let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
            for key in [b"a", b"c", b"e"] {
                store.put(key, b"0").unwrap();
                model.insert(key.to_vec(), b"0".to_vec());
            }
            let dest = root.path().join("candidate");
            let candidate = store
                .build_retained_candidate(&dest, [1; 16], [2; 16], limits())
                .unwrap();
            let log_path = source.join("records.log");
            let start = std::fs::read(&log_path).unwrap().len();
            let mut state = seed;
            let mut work = model.len() as u64;
            let mut max_payload = 6usize;
            let mut count = 0;
            for step in 0..18 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = [b'a' + ((state >> 32) % 6) as u8];
                let value = [b'0' + (step % 10) as u8];
                // Every third record is an atomic batch with ordered repeated keys.
                if step % 3 == 0 {
                    let ops = [
                        BatchOperation::Put {
                            key: &key,
                            value: &value,
                        },
                        BatchOperation::Delete { key: &key },
                        BatchOperation::Put {
                            key: &key,
                            value: b"xy",
                        },
                    ];
                    store.write_batch(&ops).unwrap();
                    for val in [Some(value.as_slice()), None, Some(b"xy".as_slice())] {
                        work += 3 * model.len() as u64 + 1;
                        if let Some(v) = val {
                            model.insert(key.to_vec(), v.to_vec());
                        } else {
                            model.remove(key.as_slice());
                        }
                        max_payload =
                            max_payload.max(model.iter().map(|(k, v)| k.len() + v.len()).sum());
                        count += 1;
                    }
                } else {
                    work += 3 * model.len() as u64 + 1;
                    if step % 2 == 0 {
                        store.delete(&key).unwrap();
                        model.remove(key.as_slice());
                    } else {
                        store.put(&key, &value).unwrap();
                        model.insert(key.to_vec(), value.to_vec());
                    }
                    max_payload =
                        max_payload.max(model.iter().map(|(k, v)| k.len() + v.len()).sum());
                    count += 1;
                }
            }
            let full_log = std::fs::read(&log_path).unwrap();
            let suffix = &full_log[start..];
            let expected = SuffixExpected {
                candidate,
                end: suffix.len() as u64,
                digest: Sha256::digest(suffix).into(),
            };
            let stage = StagingLimits {
                value_bytes: 2,
                staging_bytes: max_payload as u64,
            };
            let cap = SuffixLimits {
                bytes: suffix.len() as u64,
                operations: count,
                work,
            };
            let files: Vec<_> = ["base", "records.log", "store.lock", "witness"]
                .iter()
                .map(|name| (name, std::fs::read(dest.join(name)).unwrap()))
                .collect();
            let actual =
                replay_suffix(&dest, candidate_budgets(), stage, suffix, cap, &expected).unwrap();
            let ordinary: Vec<_> = (b'a'..=b'f')
                .filter_map(|k| store.get(&[k]).unwrap().map(|v| (vec![k], v)))
                .collect();
            assert_eq!(actual, ordinary, "seed {seed}");
            assert_eq!(actual, model.into_iter().collect::<Vec<_>>());
            let suffix_path = root.path().join("suffix");
            std::fs::write(&suffix_path, suffix).unwrap();
            let indexed = IndexedSuffix::open(
                &dest,
                &suffix_path,
                candidate_budgets(),
                IndexLimits {
                    blocks: 1,
                    anchor_bytes: 1,
                },
                SuffixLimits { work: 1000, ..cap },
                OverlayLimits {
                    entries: 6,
                    key_bytes: 6,
                },
                &expected,
            )
            .unwrap();
            for after in [None, Some(b"a".as_slice()), Some(b"c"), Some(b"f")] {
                for before in [None, Some(b"b".as_slice()), Some(b"e"), Some(b"g")] {
                    for limit in [1, 2, 6] {
                        let expected_page = store.scan(after, before, limit).unwrap();
                        assert_eq!(
                            indexed.scan(after, before, limit).unwrap(),
                            expected_page,
                            "seed {seed}, after {after:?}, before {before:?}, limit {limit}"
                        );
                    }
                }
            }
            for reduced in [
                SuffixLimits {
                    bytes: cap.bytes - 1,
                    ..cap
                },
                SuffixLimits {
                    operations: cap.operations - 1,
                    ..cap
                },
                SuffixLimits {
                    work: cap.work - 1,
                    ..cap
                },
            ] {
                assert!(
                    replay_suffix(
                        &dest,
                        candidate_budgets(),
                        stage,
                        suffix,
                        reduced,
                        &expected
                    )
                    .is_err()
                );
            }
            assert!(
                replay_suffix(
                    &dest,
                    candidate_budgets(),
                    StagingLimits {
                        staging_bytes: stage.staging_bytes - 1,
                        ..stage
                    },
                    suffix,
                    cap,
                    &expected
                )
                .is_err()
            );
            let mut corrupt = suffix.to_vec();
            *corrupt.last_mut().unwrap() ^= 1;
            let corrupt_binding = SuffixExpected {
                digest: Sha256::digest(&corrupt).into(),
                ..expected.clone()
            };
            assert!(
                replay_suffix(
                    &dest,
                    candidate_budgets(),
                    stage,
                    &corrupt,
                    cap,
                    &corrupt_binding
                )
                .is_err()
            );
            assert_eq!(std::fs::read(&log_path).unwrap(), full_log);
            for (name, bytes) in files {
                assert_eq!(std::fs::read(dest.join(name)).unwrap(), bytes);
            }
        }
    }
    #[test]
    fn candidate_lookup_validates_full_bound_snapshot() {
        use crate::retained_candidate::lookup;
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let store = Store::open(&source).unwrap();
        let large = vec![42; 150_000];
        store.put(b"a", b"").unwrap();
        store.put(b"b", &large).unwrap();
        store.put(b"z", b"last").unwrap();
        let dest = root.path().join("candidate");
        let expected = store
            .build_retained_candidate(&dest, [1; 16], [2; 16], limits())
            .unwrap();
        let budgets = crate::retained_base_v2::Limits {
            file_bytes: 1_000_000,
            entries: 10,
            key_bytes: 100,
            work_bytes: 1_000_001,
        };
        let before = std::fs::read(source.join("records.log")).unwrap();
        assert_eq!(
            lookup(&dest, budgets, b"a", 0, &expected).unwrap(),
            Some(vec![])
        );
        assert_eq!(
            lookup(&dest, budgets, b"b", large.len(), &expected).unwrap(),
            Some(large)
        );
        assert_eq!(
            lookup(&dest, budgets, b"missing", 0, &expected).unwrap(),
            None
        );
        assert!(lookup(&dest, budgets, b"b", 1, &expected).is_err());
        assert!(lookup(&dest, budgets, b"", 0, &expected).is_err());
        assert!(lookup(&dest, budgets, &vec![1; 1025], 0, &expected).is_err());
        assert!(lookup(&dest, budgets, b"a", 16 * 1024 * 1024 + 1, &expected).is_err());
        let mut wrong = expected.clone();
        wrong.base_digest[0] ^= 1;
        assert!(lookup(&dest, budgets, b"a", 0, &wrong).is_err());
        let mut wrong = expected.clone();
        wrong.header[48] ^= 1;
        assert!(lookup(&dest, budgets, b"a", 0, &wrong).is_err());
        let mut small = budgets;
        small.work_bytes = 145;
        assert!(lookup(&dest, small, b"a", 0, &expected).is_err());
        let base = dest.join("base");
        let original = std::fs::read(&base).unwrap();
        let mut mutated = original.clone();
        let pos = mutated.len() - 33;
        mutated[pos] ^= 1;
        std::fs::write(&base, &mutated).unwrap();
        assert!(lookup(&dest, budgets, b"a", 0, &expected).is_err());
        std::fs::write(&base, &original[..original.len() - 1]).unwrap();
        assert!(lookup(&dest, budgets, b"a", 0, &expected).is_err());
        std::fs::write(&base, original).unwrap();
        std::fs::write(dest.join("extra"), b"x").unwrap();
        assert!(lookup(&dest, budgets, b"a", 0, &expected).is_err());
        assert_eq!(std::fs::read(source.join("records.log")).unwrap(), before);
        assert_eq!(store.get(b"z").unwrap(), Some(b"last".to_vec()));
    }
    #[test]
    fn candidate_verification_and_failure_paths() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        store.put(b"key", b"value").unwrap();
        let dest = root.path().join("candidate");
        let expected = store
            .build_retained_candidate(&dest, [1; 16], [2; 16], limits())
            .unwrap();
        let budgets = crate::retained_base_v2::Limits {
            file_bytes: 10000,
            entries: 10,
            key_bytes: 100,
            work_bytes: 10001,
        };
        let verified = crate::retained_candidate::verify(&dest, budgets, &expected).unwrap();
        assert_eq!(verified.entries, 1);
        let probe = root.path().join("reader-probe");
        std::fs::create_dir(&probe).unwrap();
        for name in ["base", "records.log", "store.lock", "witness"] {
            std::fs::copy(dest.join(name), probe.join(name)).unwrap();
        }
        assert!(Store::open(&probe).is_err());
        for name in ["base", "records.log", "store.lock", "witness"] {
            assert_eq!(
                std::fs::read(dest.join(name)).unwrap(),
                std::fs::read(probe.join(name)).unwrap()
            );
        }
        assert!(crate::retained_candidate::verify(&probe, budgets, &expected).is_ok());
        let mut wrong = expected.clone();
        wrong.base_digest[0] ^= 1;
        assert!(crate::retained_candidate::verify(&dest, budgets, &wrong).is_err());
        let mut small = budgets;
        small.file_bytes = 1;
        assert!(crate::retained_candidate::verify(&dest, small, &expected).is_err());
        let conflict = store
            .build_retained_candidate(&dest, [3; 16], [4; 16], limits())
            .unwrap_err();
        let crate::retained_candidate::Failure::OutcomeUnknown {
            expected: other, ..
        } = conflict
        else {
            panic!("expected uncertain publication for existing destination");
        };
        assert!(crate::retained_candidate::verify(&dest, budgets, &other).is_err());
        assert!(crate::retained_candidate::verify(&dest, budgets, &expected).is_ok());
        let failed = root.path().join("failed");
        assert!(
            store
                .build_retained_candidate(&failed, [3; 16], [3; 16], limits())
                .is_err()
        );
        assert!(!failed.exists());
        assert_eq!(store.get(b"key").unwrap(), Some(b"value".to_vec()));
        std::fs::write(dest.join("extra"), b"x").unwrap();
        assert!(crate::retained_candidate::verify(&dest, budgets, &expected).is_err());
        std::fs::remove_file(dest.join("extra")).unwrap();
        let base_path = dest.join("base");
        let base_bytes = std::fs::read(&base_path).unwrap();
        let root_link = root.path().join("candidate-link");
        std::os::unix::fs::symlink(&dest, &root_link).unwrap();
        assert!(crate::retained_candidate::verify(&root_link, budgets, &expected).is_err());
        std::fs::remove_file(&base_path).unwrap();
        std::os::unix::fs::symlink(dest.join("records.log"), &base_path).unwrap();
        assert!(crate::retained_candidate::verify(&dest, budgets, &expected).is_err());
        std::fs::remove_file(&base_path).unwrap();
        std::fs::write(&base_path, &base_bytes).unwrap();
        let mut corrupt_base = base_bytes.clone();
        corrupt_base[0] ^= 1;
        std::fs::write(&base_path, corrupt_base).unwrap();
        assert!(crate::retained_candidate::verify(&dest, budgets, &expected).is_err());
        std::fs::write(&base_path, &base_bytes).unwrap();
        std::fs::remove_file(&base_path).unwrap();
        std::fs::create_dir(&base_path).unwrap();
        assert!(crate::retained_candidate::verify(&dest, budgets, &expected).is_err());
        std::fs::remove_dir(&base_path).unwrap();
        std::fs::write(&base_path, base_bytes).unwrap();
        let witness = dest.join("witness");
        let witness_bytes = std::fs::read(&witness).unwrap();
        let mut corrupt_witness = witness_bytes.clone();
        corrupt_witness[0] ^= 1;
        std::fs::write(&witness, corrupt_witness).unwrap();
        assert!(crate::retained_candidate::verify(&dest, budgets, &expected).is_err());
        std::fs::write(&witness, &witness_bytes).unwrap();
        std::fs::remove_file(&witness).unwrap();
        std::os::unix::fs::symlink(dest.join("records.log"), &witness).unwrap();
        assert!(crate::retained_candidate::verify(&dest, budgets, &expected).is_err());
        std::fs::remove_file(&witness).unwrap();
        std::fs::write(&witness, witness_bytes).unwrap();
        let header = dest.join("records.log");
        let mut bytes = std::fs::read(&header).unwrap();
        bytes[8] ^= 1;
        std::fs::write(&header, bytes).unwrap();
        assert!(crate::retained_candidate::verify(&dest, budgets, &expected).is_err());
    }
    #[test]
    fn candidate_point_in_time_limits_and_publication_faults() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        store.put(b"before", b"value").unwrap();
        let source = root.path().join("source");
        let log_before = std::fs::read(source.join("records.log")).unwrap();
        let witness_before = std::fs::read(source.join("commit.state")).unwrap();
        let independent = store
            .build_retained_candidate(root.path().join("reference"), [1; 16], [2; 16], limits())
            .unwrap();
        let dest = root.path().join("snapshot");
        let expected = store
            .build_retained_candidate_with_hook(&dest, [1; 16], [2; 16], limits(), |point| {
                if point == "verified" {
                    std::thread::scope(|scope| {
                        scope
                            .spawn(|| store.put(b"after", b"value"))
                            .join()
                            .unwrap()
                    })?;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(expected, independent);
        assert_eq!(store.get(b"after").unwrap().as_deref(), Some(&b"value"[..]));
        assert_ne!(
            std::fs::read(source.join("records.log")).unwrap(),
            log_before
        );
        if log_before.starts_with(b"EMDALOG2") {
            assert_ne!(
                std::fs::read(source.join("commit.state")).unwrap(),
                witness_before
            );
        }
        assert_no_candidate_stages(root.path());
        let budgets = crate::retained_base_v2::Limits {
            file_bytes: 10000,
            entries: 10,
            key_bytes: 100,
            work_bytes: 10001,
        };
        assert_eq!(
            crate::retained_candidate::verify(&dest, budgets, &expected)
                .unwrap()
                .entries,
            1
        );
        let total = std::fs::metadata(dest.join("base")).unwrap().len() + 144;
        // Use a fresh store with the same contents to check the exact candidate size.
        let other = Store::open(root.path().join("other")).unwrap();
        other.put(b"before", b"value").unwrap();
        let mut exact = limits();
        exact.max_output_bytes = total;
        other
            .build_retained_candidate(root.path().join("exact"), [3; 16], [4; 16], exact)
            .unwrap();
        exact.max_output_bytes -= 1;
        assert!(matches!(
            other.build_retained_candidate(root.path().join("short"), [3; 16], [4; 16], exact),
            Err(crate::retained_candidate::Failure::NotPublished(_))
        ));
        for point in ["publish", "parent_sync"] {
            let path = root.path().join(point);
            let failure = other
                .build_retained_candidate_with_hook(&path, [3; 16], [4; 16], limits(), |at| {
                    if at == point {
                        Err(io::Error::other("injected"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            let crate::retained_candidate::Failure::OutcomeUnknown { expected, .. } = failure
            else {
                panic!("wrong failure")
            };
            if point == "parent_sync" {
                crate::retained_candidate::verify(&path, budgets, &expected).unwrap();
            } else {
                assert!(!path.exists());
            }
        }
        let path = root.path().join("before_publish");
        assert!(matches!(
            other.build_retained_candidate_with_hook(&path, [3; 16], [4; 16], limits(), |at| {
                if at == "verified" {
                    Err(io::Error::other("injected"))
                } else {
                    Ok(())
                }
            }),
            Err(crate::retained_candidate::Failure::NotPublished(_))
        ));
    }

    #[test]
    fn owned_directory_export_matches_public_and_cleans_up() {
        use std::ffi::OsStr;
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("source")).unwrap();
        store.put(b"key", &vec![5; 150_000]).unwrap();
        let public = root.path().join("public");
        let expected = store
            .export_retained_base(&public, [1; 16], limits())
            .unwrap();
        let stage = crate::staging::Stage::directory(root.path()).unwrap();
        let path = stage.path().to_owned();
        let actual = store
            .export_retained_base_in_stage(&stage, OsStr::new("base"), [1; 16], limits())
            .unwrap();
        assert_eq!(actual.digest, expected.digest);
        assert_eq!(
            std::fs::read(path.join("base")).unwrap(),
            std::fs::read(public).unwrap()
        );
        let cleanup_root = root.path().to_owned();
        let cleanup = std::thread::spawn(move || crate::cleanup_abandoned_staging(cleanup_root));
        assert_eq!(
            cleanup.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(path.join("base").exists());
        assert!(matches!(
            store.export_retained_base_in_stage(&stage, OsStr::new("base"), [1; 16], limits()),
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert!(matches!(
            store.export_retained_base_in_stage(&stage, OsStr::new("../escape"), [1; 16], limits()),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        assert!(matches!(
            store.export_retained_base_in_stage(
                &stage,
                OsStr::new("limited"),
                [1; 16],
                RetainedExportLimits {
                    max_output_bytes: 1,
                    ..limits()
                }
            ),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        assert!(!path.join("limited").exists());
        let failed = path.join("failed");
        assert!(matches!(
            store.export_retained_base_impl(
                &failed,
                Some(&path),
                [1; 16],
                limits(),
                crate::directory::sync_existing,
                |point| if point == "before_witness" {
                    Err(io::Error::other("injected"))
                } else {
                    Ok(())
                }
            ),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        assert!(!failed.exists());
        let uncertain = path.join("uncertain");
        assert!(matches!(
            store.export_retained_base_impl(
                &uncertain,
                Some(&path),
                [1; 16],
                limits(),
                |_| Err(io::Error::other("injected sync")),
                |_| Ok(())
            ),
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert!(uncertain.exists());
        drop(stage);
        assert!(!path.exists());
    }

    #[test]
    fn exports_batch_peak_and_chunked_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        store.put(b"aa", &vec![7; 150_000]).unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"long-key",
                    value: b"",
                },
                BatchOperation::Delete { key: b"long-key" },
            ])
            .unwrap();
        let path = dir.path().join("base");
        let result = store
            .export_retained_base(&path, [1; 16], limits())
            .unwrap();
        assert_eq!(
            (
                result.entries,
                result.key_bytes,
                result.peak_entries,
                result.peak_key_bytes
            ),
            (1, 2, 2, 10)
        );
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(&bytes[122..122 + 150_000], &vec![7; 150_000]);
    }
    #[test]
    fn lookup_export_verifies_binding_and_complete_stream() {
        use crate::retained_base_v2::{Expected, Limits, lookup};
        use std::io::{self, Read};
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        let value = vec![7; 150_000];
        store.put(b"a", &value).unwrap();
        store.put(b"empty", b"").unwrap();
        let path = dir.path().join("base");
        let meta = store
            .export_retained_base(&path, [1; 16], limits())
            .unwrap();
        let bytes = std::fs::read(path).unwrap();
        let binding = Expected {
            digest: meta.digest,
            source_id: meta.source_id,
            base_id: meta.base_id,
            boundary: meta.boundary,
        };
        let lim = Limits {
            file_bytes: bytes.len() as u64,
            work_bytes: bytes.len() as u64 + 1,
            entries: 10,
            key_bytes: 100,
        };
        for invalid_key in [&b""[..], &vec![b'x'; 1025][..]] {
            assert_eq!(
                lookup(&bytes[..], lim, invalid_key, 0, binding)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        assert_eq!(
            lookup(&bytes[..], lim, b"a", value.len() - 1, binding)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            lookup(&bytes[..], lim, b"a", value.len(), binding).unwrap(),
            Some(value)
        );
        assert_eq!(
            lookup(&bytes[..], lim, b"empty", 0, binding).unwrap(),
            Some(vec![])
        );
        assert_eq!(
            lookup(&bytes[..], lim, b"missing", 0, binding).unwrap(),
            None
        );
        assert!(lookup(&bytes[..], lim, b"a", 1, binding).is_err());
        assert!(lookup(&bytes[..], lim, b"a", 16 * 1024 * 1024 + 1, binding).is_err());
        for expected in [
            Expected {
                digest: [0; 32],
                ..binding
            },
            Expected {
                source_id: [9; 16],
                ..binding
            },
            Expected {
                base_id: [9; 16],
                ..binding
            },
            Expected {
                boundary: binding.boundary + 1,
                ..binding
            },
        ] {
            assert!(lookup(&bytes[..], lim, b"a", 150_000, expected).is_err());
        }
        for end in [0, 112, 121, bytes.len() - 1] {
            assert!(lookup(&bytes[..end], lim, b"missing", 0, binding).is_err());
        }
        let mut corrupt = bytes.clone();
        corrupt[122] ^= 1;
        assert!(lookup(&corrupt[..], lim, b"a", 150_000, binding).is_err());
        // A forged self-consistent file still cannot satisfy the independent binding.
        use sha2::{Digest, Sha256};
        let end = corrupt.len() - 32;
        let mut hash = Sha256::new();
        hash.update(b"EMDA retained base v2\0");
        hash.update(&corrupt[..end]);
        corrupt[end..].copy_from_slice(&hash.finalize());
        assert!(lookup(&corrupt[..], lim, b"a", 150_000, binding).is_err());
        let mut extra = bytes.clone();
        extra.push(1);
        assert!(
            lookup(
                &extra[..],
                Limits {
                    file_bytes: lim.file_bytes + 1,
                    work_bytes: lim.work_bytes + 1,
                    ..lim
                },
                b"missing",
                0,
                binding
            )
            .is_err()
        );
        struct Fault<'a> {
            bytes: &'a [u8],
            remaining: usize,
        }
        impl Read for Fault<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                if self.remaining == 0 {
                    return Err(io::ErrorKind::PermissionDenied.into());
                }
                let n = out.len().min(self.bytes.len()).min(self.remaining).min(7);
                out[..n].copy_from_slice(&self.bytes[..n]);
                self.bytes = &self.bytes[n..];
                self.remaining -= n;
                Ok(n)
            }
        }
        // Switch the source after some bytes have been read, simulating a
        // deterministic mid-stream mutation of the selected value.
        struct Changing<'a> {
            original: &'a [u8],
            changed: &'a [u8],
            pos: usize,
        }
        impl Read for Changing<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                let source = if self.pos < 122 {
                    self.original
                } else {
                    self.changed
                };
                let limit = if self.pos < 122 { 122 } else { source.len() };
                let n = out.len().min(limit - self.pos);
                out[..n].copy_from_slice(&source[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            }
        }
        assert!(
            lookup(
                Changing {
                    original: &bytes,
                    changed: &corrupt,
                    pos: 0
                },
                lim,
                b"a",
                150_000,
                binding
            )
            .is_err()
        );
        for at in [1, 122, bytes.len()] {
            assert!(
                lookup(
                    Fault {
                        bytes: &bytes,
                        remaining: at
                    },
                    lim,
                    b"missing",
                    0,
                    binding
                )
                .is_err()
            );
        }
    }
    #[test]
    fn rejects_budgets_conflicts_and_reports_uncertainty() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        store.put(b"key", b"value").unwrap();
        for limit in [
            RetainedExportLimits {
                max_log_bytes: 1,
                ..limits()
            },
            RetainedExportLimits {
                max_live_keys: 0,
                ..limits()
            },
            RetainedExportLimits {
                max_live_key_bytes: 2,
                ..limits()
            },
            RetainedExportLimits {
                max_output_bytes: 144,
                ..limits()
            },
        ] {
            assert!(matches!(
                store.export_retained_base(dir.path().join("out"), [1; 16], limit),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
        }
        let path = dir.path().join("out");
        std::fs::write(&path, b"existing").unwrap();
        assert!(
            store
                .export_retained_base(&path, [1; 16], limits())
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"existing");
        let path = dir.path().join("uncertain");
        assert!(matches!(
            store.export_retained_base_with_sync(&path, [1; 16], limits(), |_| Err(
                io::Error::other("sync failed")
            )),
            Err(crate::PublicationFailure::OutcomeUnknown(_))
        ));
        assert!(path.exists());
    }
    #[test]
    fn rejects_corrupt_obsolete_history() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let store = Store::open(&source).unwrap();
        store.put(b"key", b"old").unwrap();
        let location = *store.read().unwrap().index.get(&b"key"[..]).unwrap();
        store.put(b"key", b"new").unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(source.join("records.log"))
            .unwrap();
        file.write_all_at(b"X", location.offset).unwrap();
        assert!(matches!(
            store.export_retained_base(dir.path().join("out"), [1; 16], limits()),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
    }
    #[test]
    fn exact_limits_ids_empty_and_max_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        let out = dir.path().join("out");
        for id in [[0; 16], store.log.id().unwrap()] {
            assert!(matches!(
                store.export_retained_base(&out, id, limits()),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
            assert!(!out.exists());
        }
        let empty = RetainedExportLimits {
            max_log_bytes: store.log.len().unwrap(),
            max_live_keys: 0,
            max_live_key_bytes: 0,
            max_output_bytes: 144,
        };
        store.export_retained_base(&out, [1; 16], empty).unwrap();
        std::fs::remove_file(&out).unwrap();
        store.put(b"k", &vec![42; 16 * 1024 * 1024]).unwrap();
        let exact = RetainedExportLimits {
            max_log_bytes: store.log.len().unwrap(),
            max_live_keys: 1,
            max_live_key_bytes: 1,
            max_output_bytes: 144 + 8 + 1 + 16 * 1024 * 1024,
        };
        for limit in [
            RetainedExportLimits {
                max_log_bytes: exact.max_log_bytes - 1,
                ..exact
            },
            RetainedExportLimits {
                max_live_keys: 0,
                ..exact
            },
            RetainedExportLimits {
                max_live_key_bytes: 0,
                ..exact
            },
            RetainedExportLimits {
                max_output_bytes: exact.max_output_bytes - 1,
                ..exact
            },
        ] {
            assert!(matches!(
                store.export_retained_base(&out, [1; 16], limit),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
            assert!(!out.exists());
        }
        store.export_retained_base(&out, [1; 16], exact).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlink_destination_is_not_replaced() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("source")).unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, b"keep").unwrap();
        let out = dir.path().join("out");
        symlink(&target, &out).unwrap();
        assert!(store.export_retained_base(&out, [1; 16], limits()).is_err());
        assert!(
            std::fs::symlink_metadata(&out)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read(target).unwrap(), b"keep");
    }

    #[test]
    fn changed_source_and_live_corruption_fail_without_publication() {
        use std::os::unix::fs::FileExt;
        for append in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            let store = Store::open(&source).unwrap();
            store.put(b"key", b"value").unwrap();
            let location = *store.read().unwrap().index.get(&b"key"[..]).unwrap();
            let file = OpenOptions::new()
                .write(true)
                .open(source.join("records.log"))
                .unwrap();
            file.write_all_at(
                b"X",
                if append {
                    store.log.len().unwrap()
                } else {
                    location.offset
                },
            )
            .unwrap();
            let before = std::fs::read(source.join("records.log")).unwrap();
            let out = dir.path().join("out");
            assert!(matches!(
                store.export_retained_base(&out, [1; 16], limits()),
                Err(crate::PublicationFailure::NotPublished(_))
            ));
            assert!(!out.exists());
            assert_eq!(std::fs::read(source.join("records.log")).unwrap(), before);
        }
    }

    #[test]
    fn writer_waits_until_publication_sync_finishes() {
        use std::sync::{Arc, mpsc};
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("source")).unwrap());
        let out = dir.path().join("out");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let exporting = Arc::clone(&store);
        let export = std::thread::spawn(move || {
            exporting.export_retained_base_with_sync(&out, [1; 16], limits(), |parent| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                crate::directory::sync_existing(parent)
            })
        });
        entered_rx.recv().unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let writing = Arc::clone(&store);
        let writer = std::thread::spawn(move || {
            let result = writing.put(b"k", b"v");
            done_tx.send(result).unwrap();
        });
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        release_tx.send(()).unwrap();
        export.join().unwrap().unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        writer.join().unwrap();
    }
    #[test]
    fn failed_store_rejects_export() {
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let fault = Arc::new(Mutex::new(None));
        let store =
            super::super::fault_tests::open_versioned_fault_store(&source, Arc::clone(&fault))
                .unwrap();
        store.put(b"safe", b"value").unwrap();
        // A partial append poisons both V2 and V3 stores. V3 synchronizes via
        // its metadata witness rather than this adapter's data-sync hook.
        *fault.lock().unwrap() = Some(super::super::fault_tests::Fault::PartialAppend(1));
        assert!(store.put(b"uncertain", b"value").is_err());
        let before = std::fs::read(source.join("records.log")).unwrap();
        let out = dir.path().join("out");
        assert!(matches!(
            store.export_retained_base(&out, [1; 16], limits()),
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        assert!(!out.exists());
        assert_eq!(std::fs::read(source.join("records.log")).unwrap(), before);
    }
    #[test]
    fn injected_read_and_staging_sync_failures_preserve_source() {
        use super::super::fault_tests::{Fault, open_versioned_fault_store};
        use std::sync::{Arc, Mutex};
        for point in ["replay", "value_read", "output_sync"] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source");
            std::fs::create_dir(&source).unwrap();
            let fault = Arc::new(Mutex::new(None));
            let store = open_versioned_fault_store(&source, Arc::clone(&fault)).unwrap();
            store.put(b"key", b"value").unwrap();
            let log = std::fs::read(source.join("records.log")).unwrap();
            let witness = std::fs::read(source.join("commit.state")).ok();
            let out = dir.path().join("out");
            if point == "replay" {
                *fault.lock().unwrap() = Some(Fault::ShortRead(1));
            }
            let result = store.export_retained_base_with_hook(
                &out,
                [1; 16],
                limits(),
                crate::directory::sync_existing,
                |at| {
                    if point == "value_read" && at == point {
                        *fault.lock().unwrap() = Some(Fault::ShortRead(1));
                    }
                    if point == "output_sync" && at == point {
                        return Err(io::Error::other("injected output sync failure"));
                    }
                    Ok(())
                },
            );
            assert!(
                matches!(result, Err(crate::PublicationFailure::NotPublished(_))),
                "{point}"
            );
            assert!(!out.exists());
            assert_eq!(std::fs::read(source.join("records.log")).unwrap(), log);
            assert_eq!(std::fs::read(source.join("commit.state")).ok(), witness);
            assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        }
    }

    #[test]
    fn v2_witness_or_v3_anchor_changes_after_replay_and_staging_fail_closed() {
        for point in ["after_replay", "before_witness"] {
            for identity in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let source = dir.path().join("source");
                let store = Store::open(&source).unwrap();
                store.put(b"key", b"value").unwrap();
                let log = std::fs::read(source.join("records.log")).unwrap();
                let marker = source.join("commit.state");
                // V2 stores use this marker as the mutable witness. V3 stores
                // use it as the identity anchor, with a separate xattr witness.
                let original = std::fs::read(&marker).unwrap();
                let id = if identity {
                    [9; 16]
                } else {
                    store.log.id().unwrap()
                };
                let changed = crate::witness::encode_for_test(
                    &id,
                    if identity {
                        store.log.len().unwrap()
                    } else {
                        0
                    },
                );
                let out = dir.path().join("out");
                let result = store.export_retained_base_with_hook(
                    &out,
                    [1; 16],
                    limits(),
                    crate::directory::sync_existing,
                    |at| {
                        if at == point {
                            std::fs::write(&marker, &changed)?;
                        }
                        Ok(())
                    },
                );
                assert!(
                    matches!(result, Err(crate::PublicationFailure::NotPublished(_))),
                    "{point} {identity}"
                );
                assert!(!out.exists());
                assert_eq!(std::fs::read(source.join("records.log")).unwrap(), log);
                assert_eq!(std::fs::read(&marker).unwrap(), changed);
                std::fs::write(&marker, &original).unwrap();
                assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn v3_xattr_witness_changes_fail_closed_at_export_hooks() {
        use rustix::fs::{XattrFlags, getxattr, setxattr};

        const XATTR: &str = "user.db.commit.v3";
        for point in ["after_replay", "before_witness"] {
            for identity in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let source = dir.path().join("source");
                let store = Store::open(&source).unwrap();
                store.put(b"key", b"value").unwrap();
                let log_path = source.join("records.log");
                let log = std::fs::read(&log_path).unwrap();
                // This test requires V3; do not silently exercise the V2 fallback.
                assert!(log.starts_with(b"EMDALOG3"));
                let anchor = std::fs::read(source.join("commit.state")).unwrap();
                let mut original = [0u8; crate::witness::WITNESS_SIZE];
                assert_eq!(
                    getxattr(&log_path, XATTR, &mut original[..]).unwrap(),
                    original.len()
                );
                let source_id = store.log.id().unwrap();
                let changed_id = if identity { [9; 16] } else { source_id };
                assert!(!identity || changed_id != source_id);
                let changed = crate::witness::encode_for_test(
                    &changed_id,
                    if identity {
                        store.log.len().unwrap()
                    } else {
                        crate::witness::LOG_HEADER_SIZE as u64
                    },
                );
                assert_ne!(&changed[..], &original[..]);
                let out = dir.path().join("out");
                let mut mutations = 0;
                let result = store.export_retained_base_with_hook(
                    &out,
                    [1; 16],
                    limits(),
                    |_| panic!("publication sync must not run"),
                    |at| {
                        if at == point {
                            mutations += 1;
                            setxattr(&log_path, XATTR, &changed, XattrFlags::REPLACE)?;
                        }
                        Ok(())
                    },
                );
                match result {
                    Err(crate::PublicationFailure::NotPublished(error)) => {
                        assert_eq!(
                            error.kind(),
                            io::ErrorKind::InvalidData,
                            "{point} identity={identity}"
                        )
                    }
                    other => panic!("unexpected result at {point} identity={identity}: {other:?}"),
                }
                assert_eq!(mutations, 1);
                assert!(!out.exists());
                assert_eq!(std::fs::read(&log_path).unwrap(), log);
                assert_eq!(std::fs::read(source.join("commit.state")).unwrap(), anchor);
                let mut actual = [0u8; crate::witness::WITNESS_SIZE];
                assert_eq!(
                    getxattr(&log_path, XATTR, &mut actual[..]).unwrap(),
                    actual.len()
                );
                assert_eq!(&actual[..], &changed[..]);

                setxattr(&log_path, XATTR, &original, XattrFlags::REPLACE).unwrap();
                store.export_retained_base(&out, [1; 16], limits()).unwrap();
                assert!(out.is_file());
                assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
                assert_eq!(std::fs::read(&log_path).unwrap(), log);
                assert_eq!(std::fs::read(source.join("commit.state")).unwrap(), anchor);
            }
        }
    }

    #[test]
    fn appended_log_during_staging_is_not_published() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let store = Store::open(&source).unwrap();
        store.put(b"key", b"value").unwrap();
        let log_path = source.join("records.log");
        let original = std::fs::read(&log_path).unwrap();
        let witness = std::fs::read(source.join("commit.state")).ok();
        let out = dir.path().join("out");
        let result = store.export_retained_base_with_hook(
            &out,
            [1; 16],
            limits(),
            crate::directory::sync_existing,
            |at| {
                if at == "before_witness" {
                    OpenOptions::new()
                        .append(true)
                        .open(&log_path)?
                        .write_all(b"unexpected")?;
                }
                Ok(())
            },
        );
        assert!(matches!(
            result,
            Err(crate::PublicationFailure::NotPublished(_))
        ));
        assert!(!out.exists());
        let mut expected = original;
        expected.extend_from_slice(b"unexpected");
        assert_eq!(std::fs::read(log_path).unwrap(), expected);
        assert_eq!(std::fs::read(source.join("commit.state")).ok(), witness);
    }
}
