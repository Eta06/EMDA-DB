//! Append, durability, read verification, and recovery for the record log.

use crate::checkpoint;
use crate::log_io::LogIo;
use crate::metadata_witness;
use crate::record::{self, HEADER_SIZE, Header, Operation};
use crate::recovery::RecoveryReader;
use crate::witness::{self, LOG_HEADER_SIZE, LogId, Witness};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Write};
use std::path::Path;
use std::sync::Arc;

/// Admission caps for the extra index built by full historical verification.
/// Log bytes also bound the encoded input, including atomic batch records;
/// neither cap is a total process-memory or recovery bound.
#[derive(Clone, Copy)]
pub struct PeakVerificationLimits {
    /// Maximum encoded historical log length.
    pub max_log_bytes: u64,
    /// Maximum simultaneous live keys during replay.
    pub max_live_keys: usize,
    /// Maximum sum of live key lengths during replay.
    pub max_live_key_bytes: usize,
}

fn verified_key_bytes(current: usize, added: usize, bounded: bool) -> io::Result<usize> {
    current.checked_add(added).ok_or_else(|| {
        if bounded {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "verification live key limit exceeded",
            )
        } else {
            record::invalid_data("replayed live key bytes overflow")
        }
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct GenerationId(pub(crate) LogId);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct GenerationLocation {
    pub(crate) generation: Option<GenerationId>,
    pub(crate) location: ValueLocation,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct ValueLocation {
    pub(crate) offset: u64,
    pub(crate) length: usize,
    pub(crate) checksum: u32,
}

pub(crate) struct BatchUpdate {
    pub(crate) key: Vec<u8>,
    pub(crate) location: Option<ValueLocation>,
}

/// Decode and validate every child before the caller publishes any changes.
pub(crate) fn prepare_batch(payload: &[u8], outer: ValueLocation) -> io::Result<Vec<BatchUpdate>> {
    if payload.len() != outer.length {
        return Err(record::invalid_data("batch value length mismatch"));
    }
    record::batch::decode_payload(payload)?
        .into_iter()
        .map(|entry| {
            let location = match entry.operation {
                Operation::Put => Some(ValueLocation {
                    offset: outer
                        .offset
                        .checked_add(entry.value_offset as u64)
                        .ok_or_else(|| record::invalid_data("batch value offset overflow"))?,
                    length: entry.value_len,
                    checksum: entry.checksum,
                }),
                Operation::Delete => None,
                Operation::Batch => return Err(record::invalid_data("nested batch")),
            };
            Ok(BatchUpdate {
                key: entry.key.to_vec(),
                location,
            })
        })
        .collect()
}

pub(crate) struct AppendRecord<'a> {
    pub(crate) encoded: &'a [u8],
    pub(crate) key_len: usize,
    pub(crate) value_len: usize,
    pub(crate) checksum: u32,
}

pub(crate) struct Log {
    file: Box<dyn LogIo>,
    log_id: Option<LogId>,
    witness: Option<Witness>,
    metadata: bool,
    anchor: Option<Witness>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InitializationPoint {
    LogCreated,
    MetadataDecided,
    HeaderWritten,
    LogSynced,
    WitnessFileCreated,
    WitnessWritten,
    WitnessSynced,
    WitnessDirectorySynced,
    WitnessCreated,
    StoreDirectorySynced,
}

impl Log {
    /// Reopen an already validated, locked witnessed log for appends without
    /// building a second in-memory index. The caller must validate the whole
    /// suffix and synchronize any complete unacknowledged tail before use.
    pub(crate) fn open_checkpoint_writer(
        path: &Path,
        expected: u64,
        expected_len: u64,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))?;
        let (id, metadata) = log_identity(&file)?;
        let id = id.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "checkpoint writer requires a witnessed store",
            )
        })?;
        if !metadata && metadata_witness::is_present(&file)? {
            return Err(record::invalid_data(
                "metadata witness exists without its V3 log header",
            ));
        }
        let (file, anchor, witness, boundary): (Box<dyn LogIo>, _, _, _) = if metadata {
            let (anchor, initial) = Witness::open(path, id)?;
            if initial != LOG_HEADER_SIZE as u64 {
                return Err(record::invalid_data("invalid metadata log identity anchor"));
            }
            let file = Arc::new(file);
            let (witness, boundary) = metadata_witness::open_shared(Arc::clone(&file), id)?;
            (Box::new(file), Some(anchor), witness, boundary)
        } else {
            let (witness, boundary) = Witness::open(path, id)?;
            (Box::new(file), None, witness, boundary)
        };
        if boundary != expected || expected_len < boundary || file.len()? != expected_len {
            return Err(record::invalid_data(
                "log or witness changed while opening checkpoint writer",
            ));
        }
        // Match ordinary writable-open's persistence requirement for entries.
        File::open(path)?.sync_all()?;
        Ok(Self {
            file,
            log_id: Some(id),
            witness: Some(witness),
            metadata,
            anchor,
        })
    }

    /// Opens only the log and witness for an explicitly historical checkpoint
    /// view. The caller must hold a shared lock for the log's lifetime.
    pub(crate) fn open_checkpoint_view(path: &Path) -> io::Result<(Self, u64)> {
        let file = File::open(path.join("records.log"))?;
        let (id, metadata) = log_identity(&file)?;
        let id = id.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "checkpoint views require a witnessed store",
            )
        })?;
        if !metadata && metadata_witness::is_present(&file)? {
            return Err(record::invalid_data(
                "metadata witness exists without its V3 log header",
            ));
        }
        let (anchor, witness, boundary) = if metadata {
            let (anchor, initial) = Witness::open_readonly(path, id)?;
            if initial != LOG_HEADER_SIZE as u64 {
                return Err(record::invalid_data("invalid metadata log identity anchor"));
            }
            let (witness, boundary) = metadata_witness::open(file.try_clone()?, id)?;
            (Some(anchor), witness, boundary)
        } else {
            let (witness, boundary) = Witness::open_readonly(path, id)?;
            (None, witness, boundary)
        };
        if boundary < LOG_HEADER_SIZE as u64 || boundary > file.metadata()?.len() {
            return Err(record::invalid_data("commit witness is outside the log"));
        }
        Ok((
            Self {
                file: Box::new(file),
                log_id: Some(id),
                witness: Some(witness),
                metadata,
                anchor,
            },
            boundary,
        ))
    }

    pub(crate) fn open(
        path: &Path,
        may_initialize: bool,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_with_recovery_limit(path, may_initialize, None, None, None)
    }

    pub(crate) fn open_with_recovery_limit(
        path: &Path,
        may_initialize: bool,
        max_live_keys: Option<usize>,
        max_log_bytes: Option<u64>,
        max_live_key_bytes: Option<usize>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_configured(
            path,
            may_initialize,
            max_live_keys,
            max_log_bytes,
            max_live_key_bytes,
            None,
            |_| Ok(()),
        )
    }

    pub(crate) fn open_with_checkpoint(
        path: &Path,
        may_initialize: bool,
        checkpoint: &Path,
        trusted_digest: &[u8; 32],
        max_live_keys: Option<usize>,
        max_log_bytes: Option<u64>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_configured(
            path,
            may_initialize,
            max_live_keys,
            max_log_bytes,
            None,
            Some((checkpoint, trusted_digest)),
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    pub(crate) fn open_with_initialization_hook(
        path: &Path,
        may_initialize: bool,
        hook: impl FnMut(InitializationPoint) -> io::Result<()>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_configured(path, may_initialize, None, None, None, None, hook)
    }

    fn open_configured(
        path: &Path,
        may_initialize: bool,
        max_live_keys: Option<usize>,
        max_log_bytes: Option<u64>,
        max_live_key_bytes: Option<usize>,
        checkpoint: Option<(&Path, &[u8; 32])>,
        mut initialization_hook: impl FnMut(InitializationPoint) -> io::Result<()>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let log_path = path.join("records.log");
        let witness_exists = witness::exists(path)?;
        let (file, created) = if may_initialize && !witness_exists {
            match OpenOptions::new()
                .read(true)
                .append(true)
                .create_new(true)
                .open(&log_path)
            {
                Ok(file) => (file, true),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
                    OpenOptions::new().read(true).append(true).open(&log_path)?,
                    false,
                ),
                Err(error) => return Err(error),
            }
        } else {
            (OpenOptions::new().read(true).append(true).open(&log_path).map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    record::invalid_data("existing store is missing its log; refusing to create an empty store")
                } else { error }
            })?, false)
        };
        // An existing zero-byte log without a witness is indistinguishable
        // from an interrupted initialization or lost log contents. Do not
        // silently reopen it as a legacy store and acknowledge weaker writes.
        if !created && file.metadata()?.len() == 0 {
            return Err(record::invalid_data(
                "existing empty log is ambiguous; inspect it before upgrading",
            ));
        }
        if created {
            initialization_hook(InitializationPoint::LogCreated)?;
            let mut header = witness::new_header()?;
            let id = witness::parse_header(&header)?;
            if metadata_witness::initialize(&file, &id, LOG_HEADER_SIZE as u64)? {
                metadata_witness::mark_header(&mut header);
            }
            initialization_hook(InitializationPoint::MetadataDecided)?;
            (&file).write_all(&header)?;
            initialization_hook(InitializationPoint::HeaderWritten)?;
            file.sync_all()?;
            initialization_hook(InitializationPoint::LogSynced)?;
            let _ = Witness::create_with_hook(path, id, LOG_HEADER_SIZE as u64, |point| {
                initialization_hook(match point {
                    witness::CreationPoint::FileCreated => InitializationPoint::WitnessFileCreated,
                    witness::CreationPoint::Written => InitializationPoint::WitnessWritten,
                    witness::CreationPoint::FileSynced => InitializationPoint::WitnessSynced,
                    witness::CreationPoint::DirectorySynced => {
                        InitializationPoint::WitnessDirectorySynced
                    }
                })
            })?;
            initialization_hook(InitializationPoint::WitnessCreated)?;
        }
        // Persist directory entries before acknowledging future writes.
        File::open(path)?.sync_all()?;
        if created {
            initialization_hook(InitializationPoint::StoreDirectorySynced)?;
        }
        if let Some(max) = max_log_bytes {
            if file.metadata()?.len() > max {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "existing log exceeds configured limit",
                ));
            }
        }
        let (id, metadata) = log_identity(&file)?;
        if !metadata && metadata_witness::is_present(&file)? {
            return Err(record::invalid_data(
                "metadata witness exists without its V3 log header",
            ));
        }
        let mut anchor = None;
        let (witness, boundary) = if metadata {
            let (identity, initial_boundary) = Witness::open(path, id.unwrap())?;
            if initial_boundary != LOG_HEADER_SIZE as u64 {
                return Err(record::invalid_data("invalid metadata log identity anchor"));
            }
            anchor = Some(identity);
            let (witness, boundary) = metadata_witness::open(file.try_clone()?, id.unwrap())?;
            (Some(witness), Some(boundary))
        } else {
            match id {
                Some(id) => {
                    let (witness, boundary) = Witness::open(path, id)?;
                    (Some(witness), Some(boundary))
                }
                None if witness::exists(path)? => {
                    return Err(record::invalid_data(
                        "commit witness exists without its versioned log",
                    ));
                }
                None => (None, None),
            }
        };
        let file: Box<dyn LogIo> = Box::new(file);
        let (mut log, index, validated_end) = Self::from_parts(
            file,
            id,
            witness,
            boundary,
            metadata,
            (max_live_keys, max_live_key_bytes),
            checkpoint,
        )?;
        log.anchor = anchor;
        log.adopt_replayed_tail(boundary, validated_end)?;
        Ok((log, index))
    }

    fn adopt_replayed_tail(&self, boundary: Option<u64>, validated_end: u64) -> io::Result<()> {
        if let Some(boundary) = boundary {
            if validated_end > boundary {
                // A complete but unacknowledged tail may survive a crash.
                // Validate it first, then durably publish the adopted boundary.
                self.sync_validated_boundary(validated_end)?;
            }
            if self.file.len()? != validated_end {
                return Err(record::invalid_data("log length changed after recovery"));
            }
        }
        Ok(())
    }

    // Only used by the explicit, non-mutating empty-legacy upgrade path. The
    // caller holds the store lock and has independently accepted the ambiguity.
    pub(crate) fn open_empty_legacy_for_upgrade(
        path: &Path,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let file = File::open(path.join("records.log"))?;
        let info = file.metadata()?;
        if !info.is_file()
            || info.len() != 0
            || witness::exists(path)?
            || metadata_witness::is_present(&file)?
        {
            return Err(record::invalid_data(
                "source is not an empty unwitnessed legacy log",
            ));
        }
        let log = Self::verify_io(Box::new(file))?;
        Ok((log, BTreeMap::new()))
    }

    /// Verify every record from a read-only log source without building an
    /// index. Used for backup/restore verification; the returned log has no
    /// witness and cannot acknowledge mutations.
    pub(crate) fn verify_io(file: Box<dyn LogIo>) -> io::Result<Self> {
        let (log_id, metadata) = log_identity(file.as_ref())?;
        let log = Self {
            file,
            log_id,
            witness: None,
            metadata,
            anchor: None,
        };
        log.visit_records(0, None, |_, _| Ok(()))?;
        Ok(log)
    }

    #[cfg(test)]
    pub(crate) fn from_io(
        file: Box<dyn LogIo>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let (id, metadata) = log_identity(file.as_ref())?;
        // Backup readers contain only the log bytes; their length and digest
        // are verified separately before a fresh witness is installed.
        Self::from_parts(file, id, None, None, metadata, (None, None), None)
            .map(|(log, index, _)| (log, index))
    }

    fn from_parts(
        file: Box<dyn LogIo>,
        log_id: Option<LogId>,
        witness: Option<Witness>,
        boundary: Option<u64>,
        metadata: bool,
        limits: (Option<usize>, Option<usize>),
        checkpoint: Option<(&Path, &[u8; 32])>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>, u64)> {
        let log = Self {
            file,
            log_id,
            witness,
            metadata,
            anchor: None,
        };
        let (max_live_keys, max_live_key_bytes) = limits;
        let start = if let Some((path, trusted_digest)) = checkpoint {
            if max_live_key_bytes.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "checkpoint does not track historical live key bytes",
                ));
            }
            let witnessed = boundary.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "checkpoint requires a witnessed store",
                )
            })?;
            Some(checkpoint::load(
                path,
                &log,
                witnessed,
                max_live_keys,
                trusted_digest,
            )?)
        } else {
            None
        };
        let mut peak = start.as_ref().map_or(0, |checkpoint| checkpoint.peak);
        let validated_end = log.file.len()?;
        let index = log.recover_from(
            validated_end,
            boundary,
            max_live_keys,
            max_live_key_bytes,
            start,
            &mut peak,
        )?;
        Ok((log, index, validated_end))
    }

    pub(crate) fn read(&self, location: ValueLocation) -> io::Result<Vec<u8>> {
        let mut value = vec![0; location.length];
        self.file.read_exact_at(&mut value, location.offset)?;
        if crc32fast::hash(&value) != location.checksum {
            return Err(record::invalid_data("stored value checksum mismatch"));
        }
        Ok(value)
    }

    pub(crate) fn len(&self) -> io::Result<u64> {
        self.file.len()
    }

    pub(crate) fn read_bytes(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buffer, offset)
    }

    pub(crate) fn append_batch_unsynced<'a>(
        &self,
        records: impl ExactSizeIterator<Item = AppendRecord<'a>>,
    ) -> io::Result<Vec<ValueLocation>> {
        self.require_attached_witness()?;
        let mut offset = self.file.len()?;
        if let Some(anchor) = &self.anchor {
            anchor.verify(LOG_HEADER_SIZE as u64)?;
        }
        if self.metadata && self.witness.is_none() {
            return Err(io::Error::other(
                "metadata log has no attached commit witness",
            ));
        }
        if let Some(witness) = &self.witness {
            witness.verify(offset)?;
        }
        let mut locations = Vec::with_capacity(records.len());
        let mut buffers = Vec::with_capacity(records.len());
        for record in records {
            let value_offset = offset
                .checked_add(HEADER_SIZE as u64)
                .and_then(|value| value.checked_add(record.key_len as u64))
                .ok_or_else(|| io::Error::other("log offset overflow"))?;
            offset = offset
                .checked_add(record.encoded.len() as u64)
                .ok_or_else(|| io::Error::other("log offset overflow"))?;
            locations.push(ValueLocation {
                offset: value_offset,
                length: record.value_len,
                checksum: record.checksum,
            });
            buffers.push(IoSlice::new(record.encoded));
        }
        if buffers.len() == 1 {
            self.file.append_all(&buffers[0])?;
        } else {
            self.file.append_vectored(&mut buffers)?;
        }
        Ok(locations)
    }

    pub(crate) fn sync_data(&self) -> io::Result<()> {
        self.require_attached_witness()?;
        // V3's witness uses fsync on the log inode, covering data AND xattrs.
        // V2 still requires log fdatasync followed by the external witness sync.
        if self.metadata {
            return self
                .witness
                .as_ref()
                .ok_or_else(|| io::Error::other("metadata log has no attached commit witness"))?
                .advance(self.file.len()?);
        }
        self.file.sync_data()?;
        if let Some(witness) = &self.witness {
            witness.advance(self.file.len()?)?;
        }
        Ok(())
    }

    // Adopt only the boundary whose records the caller actually validated.
    pub(crate) fn sync_validated_boundary(&self, boundary: u64) -> io::Result<()> {
        self.require_attached_witness()?;
        if self.file.len()? != boundary {
            return Err(record::invalid_data(
                "log length changed before tail adoption",
            ));
        }
        if !self.metadata {
            self.file.sync_data()?;
        }
        if let Some(witness) = &self.witness {
            witness.advance(boundary)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn install_restore_witness(&self, directory: &Path) -> io::Result<()> {
        self.install_restore_witness_with_hook(directory, |_| Ok(()))
    }

    pub(crate) fn install_restore_witness_with_hook(
        &self,
        directory: &Path,
        hook: impl FnMut(witness::CreationPoint) -> io::Result<()>,
    ) -> io::Result<()> {
        if let Some(id) = self.log_id {
            let length = self.file.len()?;
            let boundary = if self.metadata {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(directory.join("records.log"))?;
                metadata_witness::install(&file, &id, length)?;
                LOG_HEADER_SIZE as u64
            } else {
                length
            };
            let _ = Witness::create_with_hook(directory, id, boundary, hook)?;
        }
        Ok(())
    }

    pub(crate) fn is_legacy(&self) -> bool {
        self.log_id.is_none()
    }

    pub(crate) fn id(&self) -> Option<LogId> {
        self.log_id
    }

    // A validated checkpoint view only reads immutable log locations. It keeps
    // the captured boundary separately and does not use these descriptors after
    // validation. An active writer keeps its own full witness and identity.
    pub(crate) fn release_view_witness(&mut self) -> io::Result<()> {
        if self.log_id.is_none() || self.witness.is_none() {
            return Err(io::Error::other("checkpoint view has no commit witness"));
        }
        self.witness = None;
        self.anchor = None;
        Ok(())
    }

    // A checkpoint view has already verified its own read-only handle. Share
    // the writer's V3 file only when both handles identify the same inode and
    // the view has discarded its witness. Any uncertainty keeps the original
    // independent read handle. Reads use positional I/O and do not change the
    // append handle's file offset.
    pub(crate) fn try_share_checkpoint_view_file(&mut self, writer: &Log) -> bool {
        if !self.metadata
            || !writer.metadata
            || self.log_id.is_none()
            || self.log_id != writer.log_id
            || self.witness.is_some()
            || self.anchor.is_some()
            || writer.witness.is_none()
            || writer.anchor.is_none()
        {
            return false;
        }
        let Some(shared) = writer.file.shared_file() else {
            return false;
        };
        let (Ok(Some(view_identity)), Ok(Some(writer_identity)), Ok(Some(shared_identity))) = (
            self.file.file_identity(),
            writer.file.file_identity(),
            LogIo::file_identity(shared.as_ref()),
        ) else {
            return false;
        };
        if view_identity != writer_identity || writer_identity != shared_identity {
            return false;
        }
        self.file = Box::new(shared);
        true
    }

    #[cfg(test)]
    pub(crate) fn shared_file_for_test(&self) -> Option<Arc<File>> {
        self.file.shared_file()
    }

    fn require_attached_witness(&self) -> io::Result<()> {
        if self.log_id.is_some() && self.witness.is_none() {
            return Err(io::Error::other("log has no attached commit witness"));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn attach_test_witness(&mut self, witness: Witness) {
        self.witness = Some(witness);
    }

    #[cfg(test)]
    pub(crate) fn replace_io_for_test(&mut self, file: Box<dyn LogIo>) {
        self.file = file;
    }

    pub(crate) fn verify_witness(&self) -> io::Result<()> {
        self.require_attached_witness()?;
        if log_identity(self.file.as_ref())? != (self.log_id, self.metadata) {
            return Err(record::invalid_data(
                "log identity changed while the store was open",
            ));
        }
        if let Some(anchor) = &self.anchor {
            anchor.verify(LOG_HEADER_SIZE as u64)?;
        }
        if let Some(witness) = &self.witness {
            witness.verify(self.file.len()?)?;
        }
        Ok(())
    }

    pub(crate) fn verify_index(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
    ) -> io::Result<usize> {
        Ok(self.verify_index_peaks(expected)?.0)
    }

    /// Reconstruct both high-water marks from a fully verified ordered replay.
    /// The caller must exclude writes for the entire verification. This is not
    /// a stable export view or a provenance record.
    pub(crate) fn verify_index_peaks(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
    ) -> io::Result<(usize, usize)> {
        self.verify_index_peaks_with_limits(expected, None)
    }

    pub(crate) fn verify_index_peaks_with_limits(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
        limits: Option<PeakVerificationLimits>,
    ) -> io::Result<(usize, usize)> {
        self.verify_index_peaks_at_boundary(expected, limits, None)
    }

    pub(crate) fn verify_index_peaks_at_boundary(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
        limits: Option<PeakVerificationLimits>,
        boundary: Option<u64>,
    ) -> io::Result<(usize, usize)> {
        self.verify_witness()?;
        let total = self.file.len()?;
        if limits.is_some_and(|limit| total > limit.max_log_bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "verification log byte limit exceeded",
            ));
        }
        let (index, peak_count, peak_bytes) = self.replay_index_peaks(total, boundary, limits)?;
        if &index != expected {
            return Err(record::invalid_data(
                "live index does not match verified log",
            ));
        }
        if self.file.len()? != total {
            return Err(record::invalid_data(
                "log length changed during verification",
            ));
        }
        self.verify_witness()?;
        Ok((peak_count, peak_bytes))
    }

    /// Reconstruct a verified prefix while the caller holds the writer barrier.
    /// The source and expected index must remain stable and trusted throughout both
    /// scans. Limits bound logical replay state and full source bytes, not total
    /// allocator usage: BTreeMap and record decoding include infallible allocations.
    #[allow(dead_code)] // Reserved for internal consumers of verified prefixes.
    pub(crate) fn reconstruct_prefix_index_peaks(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
        boundary: u64,
        limits: PeakVerificationLimits,
    ) -> io::Result<(BTreeMap<Vec<u8>, ValueLocation>, usize, usize)> {
        if self.witness.is_none() {
            return Err(io::Error::other("log has no attached commit witness"));
        }
        self.verify_index_peaks_at_boundary(expected, Some(limits), Some(boundary))?;
        let result = self.replay_index_peaks(boundary, None, Some(limits))?;
        self.verify_witness()?;
        Ok(result)
    }

    fn replay_index_peaks(
        &self,
        end: u64,
        boundary: Option<u64>,
        limits: Option<PeakVerificationLimits>,
    ) -> io::Result<(BTreeMap<Vec<u8>, ValueLocation>, usize, usize)> {
        let mut index = BTreeMap::new();
        let (mut bytes, mut peak_count, mut peak_bytes) = (0_usize, 0_usize, 0_usize);
        self.visit_records_to(0, end, boundary, |key, location| {
            match location {
                Some(location) => {
                    if !index.contains_key(&key) {
                        let next_bytes = verified_key_bytes(bytes, key.len(), limits.is_some())?;
                        if limits.is_some_and(|limit| {
                            index.len() >= limit.max_live_keys
                                || next_bytes > limit.max_live_key_bytes
                        }) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "verification live key limit exceeded",
                            ));
                        }
                        bytes = next_bytes;
                    }
                    index.insert(key, location);
                }
                None => {
                    if index.remove(&key).is_some() {
                        bytes = bytes.checked_sub(key.len()).ok_or_else(|| {
                            record::invalid_data("replayed live key bytes underflow")
                        })?;
                    }
                }
            }
            peak_count = peak_count.max(index.len());
            peak_bytes = peak_bytes.max(bytes);
            Ok(())
        })?;
        Ok((index, peak_count, peak_bytes))
    }

    fn recover_from(
        &self,
        total: u64,
        boundary: Option<u64>,
        max_live_keys: Option<usize>,
        max_live_key_bytes: Option<usize>,
        start: Option<checkpoint::Loaded>,
        peak: &mut usize,
    ) -> io::Result<BTreeMap<Vec<u8>, ValueLocation>> {
        let (mut index, offset) = match start {
            Some(checkpoint) => (checkpoint.index, checkpoint.prefix),
            None => (BTreeMap::new(), 0),
        };
        let mut live_key_bytes = 0_usize;
        self.visit_records_to(offset, total, boundary, |key, location| {
            match location {
                Some(location) => {
                    if max_live_keys
                        .is_some_and(|max| index.len() >= max && !index.contains_key(&key))
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "recovery live key limit would be exceeded",
                        ));
                    }
                    if let Some(max) = max_live_key_bytes {
                        if !index.contains_key(&key) {
                            live_key_bytes =
                                add_recovery_key_bytes(live_key_bytes, key.len(), max)?;
                        }
                    }
                    index.insert(key, location);
                }
                None => {
                    if index.remove(&key).is_some() && max_live_key_bytes.is_some() {
                        live_key_bytes -= key.len();
                    }
                }
            }
            *peak = (*peak).max(index.len());
            Ok(())
        })?;
        Ok(index)
    }

    /// Visits fully verified log mutations without materializing record values.
    ///
    /// Payloads are checksummed through `RECOVERY_READ_BUFFER_BYTES` and are
    /// not retained. A batch keeps each owned child key and value location
    /// until that record's outer checksum, outer key, and nested records have
    /// all been checked, then gives them to `visit` in order. The format
    /// allows at most 1,024 children. `visit` may retain keys; this function
    /// does not bound that index, returned values, or the time required to
    /// scan the log. Callers must keep visitor results private until the
    /// complete replay succeeds.
    pub(crate) fn visit_records(
        &self,
        offset: u64,
        boundary: Option<u64>,
        visit: impl FnMut(Vec<u8>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.visit_records_to(offset, self.file.len()?, boundary, visit)
    }

    /// Visits a fixed, previously synchronized record range. The caller must
    /// capture both boundaries while excluding in-flight writes and keep the
    /// append-only log alive. Later appends are not read. A truncated range or
    /// a boundary in the middle of a record fails closed.
    pub(crate) fn visit_synchronized_range(
        &self,
        offset: u64,
        end: u64,
        visit: impl FnMut(Vec<u8>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<()> {
        if self.file.len()? < end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "log was shortened during bounded replay",
            ));
        }
        self.visit_records_to(offset, end, None, visit)
    }

    fn visit_records_to(
        &self,
        mut offset: u64,
        total: u64,
        boundary: Option<u64>,
        mut visit: impl FnMut(Vec<u8>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut reader = RecoveryReader::at(self.file.as_ref(), total, offset)?;
        if offset == 0 && self.log_id.is_some() {
            let mut header = [0_u8; LOG_HEADER_SIZE];
            reader.read_exact(&mut header)?;
            if self.metadata {
                metadata_witness::parse_header(&header)?;
            } else {
                witness::parse_header(&header)?;
            }
            offset = LOG_HEADER_SIZE as u64;
            debug_assert_eq!(reader.position(), offset);
        }
        let mut saw_boundary = boundary.is_none_or(|value| value == offset);
        while offset < total {
            if total - offset < HEADER_SIZE as u64 {
                return Err(incomplete_record(offset));
            }
            let mut header_bytes = [0_u8; HEADER_SIZE];
            reader.read_exact(&mut header_bytes)?;
            let header = Header::parse(&header_bytes)?;
            let record_len = header.record_len();
            if total - offset < record_len {
                return Err(incomplete_record(offset));
            }
            if header.operation == Operation::Batch {
                let updates = stream_batch(&mut reader, &header_bytes, &header, offset)?;
                for update in updates {
                    visit(update.key, update.location)?;
                }
            } else {
                let mut key = vec![0_u8; header.key_len];
                reader.read_exact(&mut key)?;
                let mut record_hasher = crc32fast::Hasher::new();
                record_hasher.update(&header_bytes);
                record_hasher.update(&key);
                let mut value_hasher = crc32fast::Hasher::new();
                reader.read_with(header.value_len, |chunk| {
                    record_hasher.update(chunk);
                    value_hasher.update(chunk);
                })?;
                let mut checksum = [0_u8; 4];
                reader.read_exact(&mut checksum)?;
                if record_hasher.finalize() != u32::from_le_bytes(checksum) {
                    return Err(record::invalid_data("record checksum mismatch"));
                }
                let location = if header.operation == Operation::Put {
                    Some(ValueLocation {
                        offset: offset + HEADER_SIZE as u64 + header.key_len as u64,
                        length: header.value_len,
                        checksum: value_hasher.finalize(),
                    })
                } else {
                    None
                };
                visit(key, location)?;
            }
            offset += record_len;
            debug_assert_eq!(reader.position(), offset);
            if boundary == Some(offset) {
                saw_boundary = true;
            }
        }
        if !saw_boundary {
            return Err(record::invalid_data(
                "log does not contain its acknowledged commit boundary",
            ));
        }
        Ok(())
    }
}

/// One forward scan of a DB02 payload.
///
/// Child keys and value locations are retained until `finish`. Value bytes
/// are hashed and dropped. At most [`record::batch::MAX_ATOMIC_BATCH_OPERATIONS`]
/// keys are stored. Nested failures release those keys while the caller
/// continues hashing the rest of the record for the outer checksum.
struct BatchScan {
    phase: BatchPhase,
    pending: Option<PendingChild>,
    error: Option<io::Error>,
    updates: Vec<BatchUpdate>,
    payload_base: u64,
    pos: usize,
    ops_left: usize,
}

struct PendingChild {
    operation: Operation,
    key: Vec<u8>,
    key_need: usize,
    value_need: usize,
    value_len: usize,
    value_offset: usize,
    nested: crc32fast::Hasher,
    value_hash: crc32fast::Hasher,
}

enum BatchPhase {
    Count {
        bytes: [u8; 4],
        filled: usize,
    },
    Header {
        bytes: [u8; HEADER_SIZE],
        filled: usize,
    },
    Key,
    Value,
    Checksum {
        bytes: [u8; 4],
        filled: usize,
    },
    Trailing,
    Draining,
}

impl BatchScan {
    fn new(payload_base: u64) -> Self {
        Self {
            phase: BatchPhase::Count {
                bytes: [0; 4],
                filled: 0,
            },
            pending: None,
            error: None,
            updates: Vec::new(),
            payload_base,
            pos: 0,
            ops_left: 0,
        }
    }

    fn push(&mut self, mut chunk: &[u8]) {
        while !chunk.is_empty() {
            if matches!(self.phase, BatchPhase::Draining) {
                return;
            }
            chunk = self.consume(chunk);
        }
    }

    fn finish(self) -> io::Result<Vec<BatchUpdate>> {
        if let Some(error) = self.error {
            return Err(error);
        }
        match self.phase {
            BatchPhase::Trailing => Ok(self.updates),
            BatchPhase::Count { .. } => Err(record::invalid_data("incomplete batch count")),
            BatchPhase::Header { .. } => Err(record::invalid_data("incomplete nested header")),
            BatchPhase::Key | BatchPhase::Value => {
                Err(record::invalid_data("incomplete nested payload"))
            }
            BatchPhase::Checksum { .. } => Err(record::invalid_data("incomplete nested checksum")),
            BatchPhase::Draining => Err(record::invalid_data("incomplete batch payload")),
        }
    }

    fn consume<'a>(&mut self, input: &'a [u8]) -> &'a [u8] {
        let phase = std::mem::replace(&mut self.phase, BatchPhase::Draining);
        match phase {
            BatchPhase::Draining => &[],
            BatchPhase::Trailing => {
                self.fail(record::invalid_data("trailing bytes in atomic batch"));
                &[]
            }
            BatchPhase::Count { mut bytes, filled } => {
                let (got, rest) = split_at_most(input, 4 - filled);
                bytes[filled..filled + got.len()].copy_from_slice(got);
                let filled = filled + got.len();
                self.pos += got.len();
                if filled < 4 {
                    self.phase = BatchPhase::Count { bytes, filled };
                    return rest;
                }
                let count = u32::from_le_bytes(bytes) as usize;
                if count == 0 || count > record::batch::MAX_ATOMIC_BATCH_OPERATIONS {
                    self.fail(record::invalid_data("invalid batch operation count"));
                    return &[];
                }
                self.ops_left = count;
                self.updates.reserve(count);
                self.phase = BatchPhase::Header {
                    bytes: [0; HEADER_SIZE],
                    filled: 0,
                };
                rest
            }
            BatchPhase::Header { mut bytes, filled } => {
                let (got, rest) = split_at_most(input, HEADER_SIZE - filled);
                bytes[filled..filled + got.len()].copy_from_slice(got);
                let filled = filled + got.len();
                self.pos += got.len();
                if filled < HEADER_SIZE {
                    self.phase = BatchPhase::Header { bytes, filled };
                    return rest;
                }
                self.begin_child(bytes);
                rest
            }
            BatchPhase::Key => {
                let (done, rest) = {
                    let child = self.pending.as_mut().expect("batch child key");
                    let (got, rest) = split_at_most(input, child.key_need);
                    child.key.extend_from_slice(got);
                    child.nested.update(got);
                    child.key_need -= got.len();
                    let done = child.key_need == 0;
                    self.pos += got.len();
                    (done, rest)
                };
                if done {
                    self.after_key();
                } else {
                    self.phase = BatchPhase::Key;
                }
                rest
            }
            BatchPhase::Value => {
                let (done, rest) = {
                    let child = self.pending.as_mut().expect("batch child value");
                    let (got, rest) = split_at_most(input, child.value_need);
                    child.nested.update(got);
                    child.value_hash.update(got);
                    child.value_need -= got.len();
                    let done = child.value_need == 0;
                    self.pos += got.len();
                    (done, rest)
                };
                if done {
                    self.phase = BatchPhase::Checksum {
                        bytes: [0; 4],
                        filled: 0,
                    };
                } else {
                    self.phase = BatchPhase::Value;
                }
                rest
            }
            BatchPhase::Checksum { mut bytes, filled } => {
                let (got, rest) = split_at_most(input, 4 - filled);
                bytes[filled..filled + got.len()].copy_from_slice(got);
                let filled = filled + got.len();
                self.pos += got.len();
                if filled < 4 {
                    self.phase = BatchPhase::Checksum { bytes, filled };
                    return rest;
                }
                self.accept(u32::from_le_bytes(bytes));
                rest
            }
        }
    }

    fn begin_child(&mut self, header_bytes: [u8; HEADER_SIZE]) {
        let parsed = match Header::parse(&header_bytes) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.fail(error);
                return;
            }
        };
        if parsed.operation == Operation::Batch {
            self.fail(record::invalid_data("nested batches are not supported"));
            return;
        }
        let value_offset = match self.pos.checked_add(parsed.key_len) {
            Some(offset) => offset,
            None => {
                self.fail(record::invalid_data("batch offset overflow"));
                return;
            }
        };
        if value_offset
            .checked_add(parsed.value_len)
            .and_then(|end| end.checked_add(record::CHECKSUM_SIZE))
            .is_none()
        {
            self.fail(record::invalid_data("batch offset overflow"));
            return;
        }
        let mut nested = crc32fast::Hasher::new();
        nested.update(&header_bytes);
        let key_need = parsed.key_len;
        self.pending = Some(PendingChild {
            operation: parsed.operation,
            key: Vec::with_capacity(key_need),
            key_need,
            value_need: parsed.value_len,
            value_len: parsed.value_len,
            value_offset,
            nested,
            value_hash: crc32fast::Hasher::new(),
        });
        if key_need == 0 {
            self.after_key();
        } else {
            self.phase = BatchPhase::Key;
        }
    }

    fn after_key(&mut self) {
        let value_need = self.pending.as_ref().expect("batch child").value_need;
        self.phase = if value_need == 0 {
            BatchPhase::Checksum {
                bytes: [0; 4],
                filled: 0,
            }
        } else {
            BatchPhase::Value
        };
    }

    fn accept(&mut self, checksum: u32) {
        let child = self.pending.take().expect("batch child checksum");
        if child.nested.finalize() != checksum {
            self.fail(record::invalid_data("record checksum mismatch"));
            return;
        }
        let location = match child.operation {
            Operation::Put => {
                let Some(offset) = self.payload_base.checked_add(child.value_offset as u64) else {
                    self.fail(record::invalid_data("batch value offset overflow"));
                    return;
                };
                Some(ValueLocation {
                    offset,
                    length: child.value_len,
                    checksum: child.value_hash.finalize(),
                })
            }
            Operation::Delete => None,
            Operation::Batch => {
                self.fail(record::invalid_data("nested batch"));
                return;
            }
        };
        debug_assert!(self.updates.len() < record::batch::MAX_ATOMIC_BATCH_OPERATIONS);
        self.updates.push(BatchUpdate {
            key: child.key,
            location,
        });
        self.ops_left -= 1;
        self.phase = if self.ops_left == 0 {
            BatchPhase::Trailing
        } else {
            BatchPhase::Header {
                bytes: [0; HEADER_SIZE],
                filled: 0,
            }
        };
    }

    fn fail(&mut self, error: io::Error) {
        if self.error.is_none() {
            self.error = Some(error);
            self.updates.clear();
            self.pending = None;
        }
        self.phase = BatchPhase::Draining;
    }
}

fn split_at_most(input: &[u8], n: usize) -> (&[u8], &[u8]) {
    let n = n.min(input.len());
    input.split_at(n)
}

/// Checksums one batch record without retaining its payload.
///
/// The outer checksum is compared first, then the outer key, then nested
/// decoding. The key is known before the value, but a key mismatch is
/// reported only after the outer checksum matches. Nested bytes are hashed
/// either way. Children are parsed only for the canonical key, so a key
/// failure cannot replace itself with a nested error. The caller publishes
/// the returned updates only after this function succeeds.
fn stream_batch(
    reader: &mut RecoveryReader<'_>,
    header_bytes: &[u8; HEADER_SIZE],
    header: &Header,
    record_offset: u64,
) -> io::Result<Vec<BatchUpdate>> {
    debug_assert!(header.operation == Operation::Batch);
    let mut record_hasher = crc32fast::Hasher::new();
    record_hasher.update(header_bytes);
    let mut key = vec![0_u8; header.key_len];
    reader.read_exact(&mut key)?;
    record_hasher.update(&key);
    let key_ok = key.as_slice() == [0_u8];
    let payload_base = record_offset + HEADER_SIZE as u64 + header.key_len as u64;
    let mut scan = BatchScan::new(payload_base);
    reader.read_with(header.value_len, |chunk| {
        record_hasher.update(chunk);
        if key_ok {
            scan.push(chunk);
        }
    })?;
    let mut checksum = [0_u8; 4];
    reader.read_exact(&mut checksum)?;
    if record_hasher.finalize() != u32::from_le_bytes(checksum) {
        return Err(record::invalid_data("record checksum mismatch"));
    }
    if !key_ok {
        return Err(record::invalid_data("invalid batch record key"));
    }
    scan.finish()
}

fn add_recovery_key_bytes(current: usize, additional: usize, max: usize) -> io::Result<usize> {
    current
        .checked_add(additional)
        .filter(|bytes| *bytes <= max)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery live key byte limit would be exceeded",
            )
        })
}

fn log_identity(file: &dyn LogIo) -> io::Result<(Option<LogId>, bool)> {
    if file.len()? < witness::LOG_MAGIC.len() as u64 {
        return Ok((None, false));
    }
    let mut magic = [0_u8; 8];
    file.read_exact_at(&mut magic, 0)?;
    let metadata = &magic == metadata_witness::MAGIC;
    if &magic != witness::LOG_MAGIC && !metadata {
        return Ok((None, false));
    }
    if file.len()? < LOG_HEADER_SIZE as u64 {
        return Err(record::invalid_data("incomplete versioned log header"));
    }
    let mut header = [0_u8; LOG_HEADER_SIZE];
    file.read_exact_at(&mut header, 0)?;
    let id = if metadata {
        metadata_witness::parse_header(&header)?
    } else {
        witness::parse_header(&header)?
    };
    Ok((Some(id), metadata))
}

fn incomplete_record(offset: u64) -> io::Error {
    record::invalid_data(&format!(
        "incomplete log record at byte {offset}; store was not modified"
    ))
}

#[cfg(test)]
#[path = "log_tests.rs"]
mod tests;
