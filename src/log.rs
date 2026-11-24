//! Append, durability, read verification, and recovery for the record log.

use crate::checkpoint;
use crate::log_io::LogIo;
use crate::metadata_witness;
use crate::record::batch::stream::{StreamValue, ValueHash};
use crate::record::{self, HEADER_SIZE, Header, Operation};
use crate::recovery::RecoveryReader;
use crate::witness::{self, LOG_HEADER_SIZE, LogId, Witness};
use sha2::Sha256;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;

/// Optional logical limits for a full-log ordinary store open.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreRecoveryLimits {
    /// Total encoded log bytes, including superseded records and tombstones.
    pub max_log_bytes: Option<u64>,
    /// Simultaneously live keys at any replayed point.
    pub max_live_keys: Option<usize>,
    /// Combined bytes of simultaneously live keys at any replayed point.
    pub max_live_key_bytes: Option<usize>,
    /// Mutations admitted during recovery, including each ordered batch child.
    pub max_recovery_operations: Option<u64>,
}

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

struct ReplayedEnd {
    length: u64,
    adoption: Option<checkpoint::VerifiedLog>,
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
        Self::open_with_recovery_limit_checked(
            path,
            may_initialize,
            StoreRecoveryLimits::default(),
            || Ok(()),
            None,
        )
    }

    pub(crate) fn open_with_recovery_limit_checked(
        path: &Path,
        may_initialize: bool,
        limits: StoreRecoveryLimits,
        before_adoption: impl FnOnce() -> io::Result<()>,
        adoption_tracker: Option<&crate::publication_outcome::TailAdoptionTracker>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_configured(
            path,
            may_initialize,
            limits,
            None,
            (|_| Ok(()), before_adoption, adoption_tracker),
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
            StoreRecoveryLimits {
                max_live_keys,
                max_log_bytes,
                ..Default::default()
            },
            Some((checkpoint, trusted_digest)),
            (|_| Ok(()), || Ok(()), None),
        )
    }

    #[cfg(test)]
    pub(crate) fn open_with_initialization_hook(
        path: &Path,
        may_initialize: bool,
        hook: impl FnMut(InitializationPoint) -> io::Result<()>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        Self::open_configured(
            path,
            may_initialize,
            StoreRecoveryLimits::default(),
            None,
            (hook, || Ok(()), None),
        )
    }

    fn open_configured(
        path: &Path,
        may_initialize: bool,
        limits: StoreRecoveryLimits,
        checkpoint: Option<(&Path, &[u8; 32])>,
        (mut initialization_hook, before_adoption, adoption_tracker): (
            impl FnMut(InitializationPoint) -> io::Result<()>,
            impl FnOnce() -> io::Result<()>,
            Option<&crate::publication_outcome::TailAdoptionTracker>,
        ),
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
        if let Some(max) = limits.max_log_bytes {
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
        let (mut log, index, validated_end) =
            Self::from_parts(file, id, witness, boundary, metadata, limits, checkpoint)?;
        log.anchor = anchor;
        log.adopt_replayed_tail(boundary, validated_end, before_adoption, adoption_tracker)?;
        Ok((log, index))
    }

    fn adopt_replayed_tail(
        &self,
        boundary: Option<u64>,
        validated_end: ReplayedEnd,
        before_adoption: impl FnOnce() -> io::Result<()>,
        adoption_tracker: Option<&crate::publication_outcome::TailAdoptionTracker>,
    ) -> io::Result<()> {
        if boundary.is_some() {
            if let Some(adoption) = validated_end.adoption {
                // A complete but unacknowledged tail may survive a crash.
                // Validate it first, then durably publish the adopted boundary.
                adoption.verify_current_bytes(self)?;
                before_adoption()?;
                self.sync_validated_boundary_tracked(validated_end.length, adoption_tracker)?;
            }
            if self.file.len()? != validated_end.length {
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
        Self::verify_io_using(file, None)
    }

    /// Check the digest against the same byte stream the record parser sees.
    pub(crate) fn verify_io_digest(
        file: Box<dyn LogIo>,
        length: u64,
        digest: &[u8; 32],
    ) -> io::Result<Self> {
        Self::verify_io_using(file, Some((length, digest)))
    }

    fn verify_io_using(
        file: Box<dyn LogIo>,
        expected: Option<(u64, &[u8; 32])>,
    ) -> io::Result<Self> {
        use sha2::Digest;
        let (log_id, metadata) = log_identity(file.as_ref())?;
        let log = Self {
            file,
            log_id,
            witness: None,
            metadata,
            anchor: None,
        };
        if let Some((length, digest)) = expected {
            if log.len()? != length {
                return Err(record::invalid_data("verified log length mismatch"));
            }
            let mut hasher = Sha256::new();
            log.visit_records_with_digest(0, length, length, &mut hasher, |_, _| Ok(()))?;
            if hasher.finalize().as_slice() != digest || log.len()? != length {
                return Err(record::invalid_data(
                    "verified log checksum or length mismatch",
                ));
            }
        } else {
            log.visit_records(0, None, |_, _| Ok(()))?;
        }
        Ok(log)
    }

    #[cfg(test)]
    pub(crate) fn from_io(
        file: Box<dyn LogIo>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>)> {
        let (id, metadata) = log_identity(file.as_ref())?;
        // Backup readers contain only the log bytes; their length and digest
        // are verified separately before a fresh witness is installed.
        Self::from_parts(
            file,
            id,
            None,
            None,
            metadata,
            StoreRecoveryLimits::default(),
            None,
        )
        .map(|(log, index, _)| (log, index))
    }

    fn from_parts(
        file: Box<dyn LogIo>,
        log_id: Option<LogId>,
        witness: Option<Witness>,
        boundary: Option<u64>,
        metadata: bool,
        limits: StoreRecoveryLimits,
        checkpoint: Option<(&Path, &[u8; 32])>,
    ) -> io::Result<(Self, BTreeMap<Vec<u8>, ValueLocation>, ReplayedEnd)> {
        let log = Self {
            file,
            log_id,
            witness,
            metadata,
            anchor: None,
        };
        let start = if let Some((path, trusted_digest)) = checkpoint {
            if limits.max_live_key_bytes.is_some() {
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
                limits.max_live_keys,
                trusted_digest,
            )?)
        } else {
            None
        };
        let mut peak = start.as_ref().map_or(0, |checkpoint| checkpoint.peak);
        let validated_end = log.file.len()?;
        let (index, replayed_end) =
            log.recover_from(validated_end, boundary, limits, start, &mut peak)?;
        Ok((log, index, replayed_end))
    }

    pub(crate) fn read(&self, location: ValueLocation) -> io::Result<Vec<u8>> {
        let mut value = vec![0; location.length];
        self.read_into(location, &mut value)?;
        Ok(value)
    }

    pub(crate) fn read_into(&self, location: ValueLocation, value: &mut [u8]) -> io::Result<()> {
        if value.len() != location.length {
            return Err(record::invalid_data("stored value buffer length mismatch"));
        }
        self.file.read_exact_at(value, location.offset)?;
        if crc32fast::hash(value) != location.checksum {
            return Err(record::invalid_data("stored value checksum mismatch"));
        }
        Ok(())
    }

    pub(crate) fn value_matches(
        &self,
        location: Option<ValueLocation>,
        expected: Option<&[u8]>,
    ) -> io::Result<bool> {
        let Some(location) = location else {
            return Ok(expected.is_none());
        };
        let mut small = [0; 256];
        if location.length <= small.len() {
            let bytes = &mut small[..location.length];
            self.read_into(location, bytes)?;
            return Ok(expected == Some(bytes));
        }
        self.stream_value_matches(location, expected)
    }

    #[inline(never)]
    fn stream_value_matches(
        &self,
        location: ValueLocation,
        expected: Option<&[u8]>,
    ) -> io::Result<bool> {
        let mut expected = expected.filter(|bytes| bytes.len() == location.length);
        let mut buffer = [0; crate::recovery::RECOVERY_READ_BUFFER_BYTES];
        let mut checksum = crc32fast::Hasher::new();
        let mut offset = location.offset;
        let mut remaining = location.length;
        // A mismatch still requires reading and checking the entire value.
        // Corruption or truncation must never be reported as a failed match.
        while remaining > 0 {
            let count = remaining.min(buffer.len());
            let chunk = &mut buffer[..count];
            self.file.read_exact_at(chunk, offset)?;
            checksum.update(chunk);
            if let Some(bytes) = expected {
                let (expected_chunk, rest) = bytes.split_at(count);
                expected = (expected_chunk == chunk).then_some(rest);
            }
            offset = offset
                .checked_add(count as u64)
                .ok_or_else(|| record::invalid_data("stored value offset overflow"))?;
            remaining -= count;
        }
        if checksum.finalize() != location.checksum {
            return Err(record::invalid_data("stored value checksum mismatch"));
        }
        Ok(expected.is_some())
    }

    pub(crate) fn read_i64_or_zero(&self, location: Option<ValueLocation>) -> io::Result<i64> {
        let Some(location) = location else {
            return Ok(0);
        };
        let mut bytes = [0; 8];
        if location.length != bytes.len() {
            // Validate malformed values too, preserving the checksum error.
            self.value_matches(Some(location), None)?;
            return Err(record::invalid_data(
                "increment requires an eight-byte signed integer",
            ));
        }
        self.read_into(location, &mut bytes)?;
        Ok(i64::from_le_bytes(bytes))
    }

    pub(crate) fn len(&self) -> io::Result<u64> {
        self.file.len()
    }

    pub(crate) fn read_bytes(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buffer, offset)
    }

    fn checked_append_offset(&self) -> io::Result<u64> {
        self.require_attached_witness()?;
        let offset = self.file.len()?;
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
        Ok(offset)
    }

    pub(crate) fn append_batch_unsynced<'a>(
        &self,
        records: impl ExactSizeIterator<Item = AppendRecord<'a>>,
    ) -> io::Result<Vec<ValueLocation>> {
        let mut offset = self.checked_append_offset()?;
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

    /// Appends a record whose index locations were prepared against this end.
    pub(crate) fn append_checkpoint_record(&self, offset: u64, encoded: &[u8]) -> io::Result<()> {
        if self.checked_append_offset()? != offset {
            return Err(record::invalid_data(
                "log length changed before checkpoint append",
            ));
        }
        self.file.append_all(encoded)
    }

    /// Appends one ordinary record to an exclusively owned private stage.
    /// The producer must propagate append errors and authenticate the complete
    /// value before returning success.
    /// If any step fails, discard the stage. Partial bytes may be present.
    pub(crate) fn append_private_put(
        &self,
        key: &[u8],
        value_len: usize,
        produce: impl FnOnce(&mut dyn FnMut(&[u8]) -> io::Result<()>) -> io::Result<()>,
    ) -> io::Result<()> {
        let header = record::encode_header(Operation::Put, key, value_len)?;
        let offset = self.checked_append_offset()?;
        let size = HEADER_SIZE + key.len() + value_len + record::CHECKSUM_SIZE;
        let end = offset
            .checked_add(size as u64)
            .ok_or_else(|| io::Error::other("log offset overflow"))?;
        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&header);
        checksum.update(key);
        self.file
            .append_vectored(&mut [IoSlice::new(&header), IoSlice::new(key)])?;
        let mut remaining = value_len;
        produce(&mut |bytes| {
            remaining = remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| record::invalid_data("stream exceeded declared value length"))?;
            checksum.update(bytes);
            self.file.append_all(bytes)
        })?;
        if remaining != 0 {
            return Err(record::invalid_data(
                "stream ended before declared value length",
            ));
        }
        self.file.append_all(&checksum.finalize().to_le_bytes())?;
        self.sync_validated_boundary(end)
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
        self.sync_validated_boundary_tracked(boundary, None)
    }

    pub(crate) fn sync_validated_boundary_tracked(
        &self,
        boundary: u64,
        tracker: Option<&crate::publication_outcome::TailAdoptionTracker>,
    ) -> io::Result<()> {
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
            witness.advance_tracked(boundary, tracker)?;
        }
        Ok(())
    }

    pub(crate) fn verify_staged_file(&self, directory: &Path, length: u64) -> io::Result<()> {
        let entry = std::fs::symlink_metadata(directory.join("records.log"))?;
        if !entry.is_file()
            || entry.len() != length
            || self.file.file_identity()? != Some((entry.dev(), entry.ino()))
            || self.len()? != length
        {
            return Err(record::invalid_data(
                "staged log identity or length changed after verification",
            ));
        }
        Ok(())
    }

    pub(crate) fn install_restore_witness_with_hook(
        &self,
        directory: &Path,
        length: u64,
        hook: impl FnMut(witness::CreationPoint) -> io::Result<()>,
    ) -> io::Result<()> {
        self.verify_staged_file(directory, length)?;
        if let Some(id) = self.log_id {
            let boundary = if self.metadata {
                let file = self.file.shared_file().ok_or_else(|| {
                    io::Error::other("verified staged log is missing its file handle")
                })?;
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

    pub(crate) fn require_same_checkpoint_log(&self, view: &Self) -> io::Result<()> {
        let identity = self.file.file_identity()?;
        if self.log_id != view.log_id
            || identity.is_none()
            || identity != view.file.file_identity()?
        {
            return Err(record::invalid_data(
                "log identity changed while opening checkpoint writer",
            ));
        }
        Ok(())
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
        self.verify_witness_boundary(None)
    }

    /// Verify the captured witness when an independently replayed tail still
    /// extends past it. Callers must validate the physical log separately.
    pub(crate) fn verify_witness_at(&self, boundary: u64) -> io::Result<()> {
        self.verify_witness_boundary(Some(boundary))
    }

    fn verify_witness_boundary(&self, boundary: Option<u64>) -> io::Result<()> {
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
            witness.verify(boundary.map_or_else(|| self.file.len(), Ok)?)?;
        }
        Ok(())
    }

    pub(crate) fn verify_index(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
    ) -> io::Result<usize> {
        Ok(self.verify_index_peaks(expected)?.0)
    }

    pub(crate) fn verify_checkpoint_index(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
    ) -> io::Result<(usize, checkpoint::VerifiedLog)> {
        use sha2::Digest;
        let mut hasher = Sha256::new();
        let (peak, _, length) =
            self.verify_index_replay(expected, None, None, Some(&mut hasher))?;
        Ok((peak, checkpoint::VerifiedLog::from_replay(length, hasher)))
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
        let (count, bytes, _) = self.verify_index_replay(expected, limits, boundary, None)?;
        Ok((count, bytes))
    }

    fn verify_index_replay(
        &self,
        expected: &BTreeMap<Vec<u8>, ValueLocation>,
        limits: Option<PeakVerificationLimits>,
        boundary: Option<u64>,
        hasher: Option<&mut Sha256>,
    ) -> io::Result<(usize, usize, u64)> {
        self.verify_witness()?;
        let total = self.file.len()?;
        if limits.is_some_and(|limit| total > limit.max_log_bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "verification log byte limit exceeded",
            ));
        }
        let mut reader = RecoveryReader::at(self.file.as_ref(), total, 0)?;
        if let Some(hasher) = hasher {
            reader = reader.with_digest(hasher);
        }
        let (index, peak_count, peak_bytes) =
            self.replay_index_peaks(reader, total, boundary, limits)?;
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
        Ok((peak_count, peak_bytes, total))
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
        let reader = RecoveryReader::at(self.file.as_ref(), boundary, 0)?;
        let result = self.replay_index_peaks(reader, boundary, None, Some(limits))?;
        self.verify_witness()?;
        Ok(result)
    }

    fn replay_index_peaks(
        &self,
        reader: RecoveryReader<'_>,
        end: u64,
        boundary: Option<u64>,
        limits: Option<PeakVerificationLimits>,
    ) -> io::Result<(BTreeMap<Vec<u8>, ValueLocation>, usize, usize)> {
        let mut index = BTreeMap::<Vec<u8>, ValueLocation>::new();
        let (mut bytes, mut peak_count, mut peak_bytes) = (0_usize, 0_usize, 0_usize);
        self.visit_locations(reader, end, boundary, None, |key, location| {
            match location {
                Some(location) => {
                    if let Some(existing) = index.get_mut(key.as_ref()) {
                        *existing = location;
                    } else {
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
                        index.insert(key.into_owned(), location);
                    }
                }
                None => {
                    if index.remove(key.as_ref()).is_some() {
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
        limits: StoreRecoveryLimits,
        start: Option<checkpoint::Loaded>,
        peak: &mut usize,
    ) -> io::Result<(BTreeMap<Vec<u8>, ValueLocation>, ReplayedEnd)> {
        use sha2::Digest;
        let (mut index, offset, log_hasher) = match start {
            Some(checkpoint) => (checkpoint.index, checkpoint.prefix, checkpoint.log_hasher),
            None => (BTreeMap::new(), 0, Sha256::new()),
        };
        let mut adoption_hasher = boundary.filter(|&end| end < total).map(|_| log_hasher);
        let mut reader = RecoveryReader::at(self.file.as_ref(), total, offset)?;
        if let Some(hasher) = adoption_hasher.as_mut() {
            reader = reader.with_digest(hasher);
        }
        let mut live_key_bytes = 0_usize;
        let limit = limits
            .max_recovery_operations
            .map(|count| (count, "recovery operation limit exceeded"));
        self.visit_locations(reader, total, boundary, limit, |key, location| {
            match location {
                Some(location) => {
                    if let Some(existing) = index.get_mut(key.as_ref()) {
                        *existing = location;
                    } else {
                        if limits.max_live_keys.is_some_and(|max| index.len() >= max) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "recovery live key limit would be exceeded",
                            ));
                        }
                        if let Some(max) = limits.max_live_key_bytes {
                            live_key_bytes =
                                add_recovery_key_bytes(live_key_bytes, key.len(), max)?;
                        }
                        index.insert(key.into_owned(), location);
                    }
                }
                None => {
                    if index.remove(key.as_ref()).is_some() && limits.max_live_key_bytes.is_some() {
                        live_key_bytes -= key.len();
                    }
                }
            }
            *peak = (*peak).max(index.len());
            Ok(())
        })?;
        Ok((
            index,
            ReplayedEnd {
                length: total,
                adoption: adoption_hasher
                    .map(|hasher| checkpoint::VerifiedLog::from_replay(total, hasher)),
            },
        ))
    }

    /// Visits fully verified log mutations without materializing record values.
    ///
    /// Payloads are checksummed through `RECOVERY_READ_BUFFER_BYTES` and are
    /// not retained. Single-record keys borrow a reusable scratch buffer.
    /// A batch keeps each owned child key and value location
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
        visit: impl FnMut(Cow<'_, [u8]>, Option<ValueLocation>) -> io::Result<()>,
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
        visit: impl FnMut(Cow<'_, [u8]>, Option<ValueLocation>) -> io::Result<()>,
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
        offset: u64,
        total: u64,
        boundary: Option<u64>,
        visit: impl FnMut(Cow<'_, [u8]>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<()> {
        let reader = RecoveryReader::at(self.file.as_ref(), total, offset)?;
        self.visit_locations(reader, total, boundary, None, visit)
    }

    /// Replay an admitted checkpoint suffix and hash its exact bytes if it
    /// contains an unwitnessed tail. The caller pins the range and checks its
    /// length before adopting any bytes.
    pub(crate) fn visit_checkpoint_suffix(
        &self,
        start: u64,
        end: u64,
        boundary: u64,
        max_operations: Option<u64>,
        visit: impl FnMut(Cow<'_, [u8]>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<Option<[u8; 32]>> {
        use sha2::Digest;
        let mut digest = (end > boundary).then(Sha256::new);
        let mut reader = RecoveryReader::at(self.file.as_ref(), end, start)?;
        if let Some(digest) = &mut digest {
            reader = reader.with_digest(digest);
        }
        let limit =
            max_operations.map(|count| (count, "checkpoint suffix operation limit exceeded"));
        self.visit_locations(reader, end, Some(boundary), limit, visit)?;
        Ok(digest.map(|digest| digest.finalize().into()))
    }

    // Continue hashing a verified prefix with exactly the bytes validated
    // during replay of the next range.
    pub(crate) fn visit_records_with_digest(
        &self,
        start: u64,
        end: u64,
        boundary: u64,
        digest: &mut Sha256,
        visit: impl FnMut(Cow<'_, [u8]>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.visit_records_with_digest_and_limit(start, end, boundary, digest, None, visit)
    }

    pub(crate) fn visit_records_with_digest_and_limit(
        &self,
        start: u64,
        end: u64,
        boundary: u64,
        digest: &mut Sha256,
        max_operations: Option<u64>,
        visit: impl FnMut(Cow<'_, [u8]>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<()> {
        let reader = RecoveryReader::at(self.file.as_ref(), end, start)?.with_digest(digest);
        let limit = max_operations.map(|count| (count, "rebuild operation limit exceeded"));
        self.visit_locations(reader, end, Some(boundary), limit, visit)
    }

    fn visit_locations(
        &self,
        reader: RecoveryReader<'_>,
        total: u64,
        boundary: Option<u64>,
        max_operations: Option<(u64, &'static str)>,
        mut visit: impl FnMut(Cow<'_, [u8]>, Option<ValueLocation>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.visit_hashed_records_to::<crc32fast::Hasher>(
            reader,
            total,
            boundary,
            max_operations,
            |key, value| {
                visit(
                    key,
                    value.map(|value| ValueLocation {
                        offset: value.offset,
                        length: value.length,
                        checksum: value.digest,
                    }),
                )
            },
        )
    }

    /// Verify a private restored log and compare the SHA-256 of each value
    /// as part of the same scan that validates its enclosing record.
    pub(crate) fn verify_restored_records(
        &self,
        end: u64,
        mut visit: impl FnMut(&[u8], usize, [u8; 32]) -> io::Result<()>,
    ) -> io::Result<()> {
        let verify_end = || {
            if self.len()? != end {
                return Err(record::invalid_data("restored log length mismatch"));
            }
            self.verify_witness()
        };
        verify_end()?;
        let reader = RecoveryReader::at(self.file.as_ref(), end, 0)?;
        self.visit_hashed_records_to::<sha2::Sha256>(
            reader,
            end,
            Some(end),
            None,
            |key, value| {
                let value =
                    value.ok_or_else(|| record::invalid_data("unexpected restored deletion"))?;
                visit(&key, value.length, value.digest)
            },
        )?;
        verify_end()
    }

    fn visit_hashed_records_to<D: ValueHash>(
        &self,
        mut reader: RecoveryReader<'_>,
        total: u64,
        boundary: Option<u64>,
        mut operation_limit: Option<(u64, &'static str)>,
        mut visit: impl FnMut(Cow<'_, [u8]>, Option<StreamValue<D::Output>>) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut admit = |operations: usize| {
            if let Some((remaining, message)) = &mut operation_limit {
                *remaining = remaining
                    .checked_sub(operations as u64)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, *message))?;
            }
            Ok(())
        };
        let mut offset = reader.position();
        if let Some(expected_id) = self.log_id.filter(|_| offset == 0) {
            let mut header = [0_u8; LOG_HEADER_SIZE];
            reader.read_exact(&mut header)?;
            let id = if self.metadata {
                metadata_witness::parse_header(&header)?
            } else {
                witness::parse_header(&header)?
            };
            if id != expected_id {
                return Err(record::invalid_data("log identity changed during replay"));
            }
            offset = LOG_HEADER_SIZE as u64;
            debug_assert_eq!(reader.position(), offset);
        }
        let mut saw_boundary = boundary.is_none_or(|value| value == offset);
        let mut key_buffer = [0; record::MAX_KEY];
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
                let updates = record::batch::stream::read::<D>(
                    &mut reader,
                    &header_bytes,
                    &header,
                    offset,
                    |_| {},
                    &mut admit,
                )?;
                for update in updates {
                    visit(Cow::Owned(update.key), update.location)?;
                }
            } else {
                let key = &mut key_buffer[..header.key_len];
                reader.read_exact(key)?;
                let mut record_hasher = crc32fast::Hasher::new();
                record_hasher.update(&header_bytes);
                record_hasher.update(key);
                let mut value_hasher = D::default();
                reader.read_with(header.value_len, |chunk| {
                    record_hasher.update(chunk);
                    value_hasher.update_value(chunk);
                })?;
                let mut checksum = [0_u8; 4];
                reader.read_exact(&mut checksum)?;
                if record_hasher.finalize() != u32::from_le_bytes(checksum) {
                    return Err(record::invalid_data("record checksum mismatch"));
                }
                admit(1)?;
                let location = if header.operation == Operation::Put {
                    Some(StreamValue {
                        offset: offset + HEADER_SIZE as u64 + header.key_len as u64,
                        length: header.value_len,
                        digest: value_hasher.finish(),
                    })
                } else {
                    None
                };
                visit(Cow::Borrowed(key), location)?;
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
