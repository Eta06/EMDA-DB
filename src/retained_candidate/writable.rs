use super::indexed_suffix::{Overlay, OverlaySource};
use super::suffix_file::{SuffixFile, SuffixImport};
use super::{
    Expected, IndexLimits, Limits, OverlayLimits, SuffixLimits, check_binding, check_files,
};
use super::{SuffixExpected, open_regular, open_regular_with, suffix_capacity};
use crate::record::{self, Operation};
use crate::retained_base_v2::index::IndexedBase;
use crate::store_lock::StoreLock;
use crate::{BatchOperation, Entry, MutationFailure, PublicationFailure};
use rustix::fs::OFlags;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
};

const VERSION: [u8; 8] = *b"EMDARW01";
const MANAGED_VERSION: [u8; 8] = *b"EMDARW02";
const AUTHORIZED_VERSION: [u8; 8] = *b"EMDARW03";
const MANAGED_QUOTA_VERSION: [u8; 8] = *b"EMDARW04";
const AUTHORIZED_QUOTA_VERSION: [u8; 8] = *b"EMDARW05";
const WITNESS_MAGIC: [u8; 8] = *b"EMDAWIT4";
const LIMITS_OFFSET: usize = 136;
const HEADER_BYTES: usize = LIMITS_OFFSET + 11 * 8;
const MANAGED_HEADER_BYTES: usize = HEADER_BYTES + 16;
const QUOTA_HEADER_BYTES: usize = MANAGED_HEADER_BYTES + 16;
const WITNESS_BYTES: usize = 84;

mod managed;
pub use managed::{
    AuthorityPoint, AuthorityRecord, AuthorizedReclamationFailure, AuthorizedRestoreFailure,
    AuthorizedRetainedStore, CatalogStorageLimits, CutoverArchiveBinding,
    CutoverArchiveRetirementFailure, CutoverArchiveRetirementStep, FileRetainedAuthority,
    ManagedRetainedStore, ReclamationFenceOutcome, RetainedAuthority, RetainedRotationJob,
    RetainedStartupLimits,
};
mod rebase;

#[cfg(test)]
mod tests;

/// Admission ceilings for one writable retained generation. These are
/// persisted at creation. Standalone reopen requires the same configuration.
/// Suffix limits include physical bytes, ordered operations and replay work.
/// Overlay limits include tombstones. Result buffers, transient allocations,
/// allocator overhead and filesystem blocks are outside these logical caps.
#[derive(Clone, Copy, Debug)]
pub struct RetainedStoreLimits {
    pub base: Limits,
    pub index: IndexLimits,
    pub suffix: SuffixLimits,
    pub overlay: OverlayLimits,
}

impl RetainedStoreLimits {
    fn fields(self) -> [u64; 11] {
        [
            self.base.file_bytes,
            self.base.entries,
            self.base.key_bytes,
            self.base.work_bytes,
            self.index.blocks as u64,
            self.index.anchor_bytes as u64,
            self.suffix.bytes,
            self.suffix.operations,
            self.suffix.work,
            self.overlay.entries as u64,
            self.overlay.key_bytes as u64,
        ]
    }
}

#[derive(Clone, Copy)]
enum GenerationRole {
    Standalone,
    Managed([u8; 16]),
    Authorized([u8; 16]),
}

enum RecoveryLimits {
    Exact(RetainedStoreLimits),
    Ceiling(RetainedStoreLimits),
}

#[derive(PartialEq, Eq)]
enum TailRecovery {
    AlreadyWitnessed,
    AdoptedAndSynced,
}

#[derive(Clone, Copy)]
enum RecoveryScanLimit {
    Suffix(u64),
    BaseAndSuffix(u64),
}

impl RecoveryScanLimit {
    fn remaining(self, base: u64, suffix: u64, pending: bool) -> io::Result<u64> {
        let (base, limit, message) = match self {
            Self::Suffix(limit) => (
                0,
                limit,
                "retained recovery suffix scan byte limit exceeded",
            ),
            Self::BaseAndSuffix(limit) => {
                (base, limit, "retained recovery scan byte limit exceeded")
            }
        };
        let confirmation = if pending { suffix } else { 0 };
        suffix
            .checked_add(confirmation)
            .and_then(|scans| scans.checked_add(base))
            .and_then(|scans| limit.checked_sub(scans))
            .ok_or_else(|| invalid_input(message))
    }
}

/// An independently created writable retained generation. It keeps one
/// immutable, authenticated base on disk and admits writes to a bounded
/// suffix. The existing source and candidate remain separate copies.
///
/// The caller supplies the independently retained base binding on each open.
/// The local witness binds the generation header and the exact committed
/// suffix prefix. As with the ordinary store, the directory and filesystem
/// must be trusted and cooperating. The local witness cannot detect rollback
/// of the complete generation and witness together.
pub struct RetainedStore {
    base: IndexedBase,
    log: File,
    witness: File,
    header: Header,
    suffix_len: usize,
    hash: Sha256,
    overlay: Overlay,
    limits: RetainedStoreLimits,
    failed: bool,
    _lock: StoreLock,
}

impl RetainedStore {
    /// Publish a new writable generation initialized from a verified
    /// candidate and an empty suffix. The destination directory must be
    /// absent and outside the candidate. A publication attempt with an
    /// uncertain outcome requires inspection before retry.
    pub fn create(
        candidate: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &Expected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::create_using(
            candidate.as_ref(),
            destination.as_ref(),
            limits,
            expected,
            None,
            crate::directory::sync_existing,
        )
    }

    /// Publish a new writable generation from an independently verified base
    /// and detached suffix. The imported suffix is charged against the
    /// generation's persisted limits and witnessed before publication.
    /// The suffix is copied in fixed-size chunks and replayed from staging.
    pub fn create_from_suffix(
        candidate: impl AsRef<Path>,
        suffix: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &SuffixExpected,
    ) -> Result<[u8; 16], PublicationFailure> {
        Self::create_using(
            candidate.as_ref(),
            destination.as_ref(),
            limits,
            &expected.candidate,
            Some((suffix.as_ref(), expected)),
            crate::directory::sync_existing,
        )
    }

    fn create_using(
        candidate: &Path,
        destination: &Path,
        limits: RetainedStoreLimits,
        expected: &Expected,
        suffix: Option<(&Path, &SuffixExpected)>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<[u8; 16], PublicationFailure> {
        crate::publication_outcome::track(|tracker| {
            let parent = crate::backup::parent_directory(destination)?;
            if parent
                .canonicalize()?
                .starts_with(candidate.canonicalize()?)
            {
                return Err(invalid_input("destination parent is inside candidate"));
            }
            check_files(candidate, expected)?;
            let base_size = expected.base_bytes();
            if base_size > limits.base.file_bytes
                || base_size
                    .checked_add(1)
                    .is_none_or(|n| n > limits.base.work_bytes)
            {
                return Err(invalid_input("retained base byte limit exceeded"));
            }
            let suffix = suffix
                .map(|(path, binding)| SuffixImport::open(path, limits.suffix, binding))
                .transpose()?;
            let base = BaseImport::open(candidate, limits, expected, u64::MAX)?;
            let mut stage = crate::staging::Stage::directory(parent)?;
            let dir = stage.path();
            let verified = Self::initialize(
                dir,
                limits,
                expected,
                suffix,
                GenerationRole::Standalone,
                None,
                Some(base),
            )?;
            let id = verified.generation_id();
            drop(verified);
            tracker.begin_publication();
            stage.publish(destination)?;
            sync_parent(parent)?;
            stage.publication_synchronized();
            Ok(id)
        })
    }

    fn initialize(
        dir: &Path,
        limits: RetainedStoreLimits,
        expected: &Expected,
        suffix: Option<SuffixImport>,
        role: GenerationRole,
        storage_limits: Option<CatalogStorageLimits>,
        base: Option<BaseImport>,
    ) -> io::Result<Self> {
        create_file(&dir.join("store.lock"))?.sync_all()?;
        let mut header = Header::new(expected, limits, role)?;
        if let Some(storage_limits) = storage_limits {
            header.set_storage_limits(role, storage_limits)?;
        }
        let mut log = create_file(&dir.join("records.log"))?;
        log.write_all(header.bytes())?;
        let (end, digest) = match suffix {
            Some(suffix) => suffix.write(&mut log)?,
            None => (0, Sha256::digest([]).into()),
        };
        log.sync_all()?;
        let mut witness = create_file(&dir.join("commit.state"))?;
        witness.write_all(&marker(header.bytes(), end, &digest))?;
        witness.sync_all()?;
        drop((log, witness));
        let (store, _) = Self::open_role(
            dir,
            (RecoveryLimits::Exact(limits), None),
            expected,
            (role, None, base),
            |_, _, _| Ok(()),
            |_, _, actual_end, actual_digest| {
                if (actual_end, actual_digest) != (end, digest) {
                    return Err(record::invalid_data("staged retained suffix changed"));
                }
                Ok(())
            },
            || Ok(()),
        )?;
        crate::directory::sync_existing(dir)?;
        Ok(store)
    }

    /// Verify the base, witness and complete bounded suffix under an
    /// exclusive store lock. A complete tail may be adopted after validation
    /// and synchronization. The directory ancestry is synchronized before
    /// the handle can accept writes, including after uncertain creation.
    /// Torn records, split commit boundaries, changed
    /// witnessed bytes and missing acknowledged bytes fail closed.
    pub fn open(
        directory: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &Expected,
    ) -> io::Result<Self> {
        Self::open_using(
            directory.as_ref(),
            limits,
            expected,
            None,
            crate::directory::sync_existing,
        )
    }

    /// Recover under an inclusive logical suffix scan byte limit. A tail
    /// beyond the local witness requires two complete suffix scans. Open
    /// reserves both before replay. Header, witness and base reads, physical
    /// I/O retries, and reads after open are outside this per-open limit.
    pub fn open_with_suffix_scan_limit(
        directory: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &Expected,
        max_suffix_scan_bytes: u64,
    ) -> io::Result<Self> {
        Self::open_using(
            directory.as_ref(),
            limits,
            expected,
            Some(RecoveryScanLimit::Suffix(max_suffix_scan_bytes)),
            crate::directory::sync_existing,
        )
    }

    /// Recover under an inclusive logical byte limit for the full encoded
    /// base and suffix scans. Replay charges the suffix once, and a pending
    /// local tail needs a second scan before adoption. Admission precedes
    /// both payload scans. Metadata and framing probes, physical I/O retries,
    /// and later reads have separate costs. Supply the limit on each open.
    pub fn open_with_recovery_scan_limit(
        directory: impl AsRef<Path>,
        limits: RetainedStoreLimits,
        expected: &Expected,
        max_scan_bytes: u64,
    ) -> io::Result<Self> {
        Self::open_using(
            directory.as_ref(),
            limits,
            expected,
            Some(RecoveryScanLimit::BaseAndSuffix(max_scan_bytes)),
            crate::directory::sync_existing,
        )
    }

    fn open_using(
        dir: &Path,
        limits: RetainedStoreLimits,
        expected: &Expected,
        scan_limit: Option<RecoveryScanLimit>,
        sync_directory: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<Self> {
        Self::open_role(
            dir,
            (RecoveryLimits::Exact(limits), scan_limit),
            expected,
            (GenerationRole::Standalone, None, None),
            |_, _, _| Ok(()),
            |directory, _, _, _| sync_directory(directory),
            || Ok(()),
        )
        .map(|(store, _)| store)
    }

    fn open_role(
        dir: &Path,
        (limits, scan_limit): (RecoveryLimits, Option<RecoveryScanLimit>),
        expected: &Expected,
        (role, generation, imported_base): (GenerationRole, Option<[u8; 16]>, Option<BaseImport>),
        preflight: impl FnOnce(&Header, u64, bool) -> io::Result<()>,
        authorize: impl FnOnce(&Path, &Header, u64, [u8; 32]) -> io::Result<()>,
        check_selection: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<(Self, TailRecovery)> {
        if !dir.symlink_metadata()?.file_type().is_dir() {
            return Err(record::invalid_data("invalid retained store directory"));
        }
        let lock = open_regular_with(&dir.join("store.lock"), OFlags::RDWR)?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let lock = StoreLock::acquired(lock);
        lock.verify_identity(dir)?;
        let log = open_regular_with(&dir.join("records.log"), OFlags::RDWR | OFlags::APPEND)?;
        let witness = open_regular_with(&dir.join("commit.state"), OFlags::RDWR)?;
        let header = Header::read(&log, role)?;
        header.verify_identity(expected, generation)?;
        let saved = read_marker(&witness)?;
        let (committed, committed_hash) = decode_marker(&saved, header.bytes())?;
        let limits = header.recovery_limits(limits)?;
        let log_size = log.metadata()?.len();
        let suffix_len = log_size
            .checked_sub(header.len as u64)
            .ok_or_else(|| record::invalid_data("missing retained log header"))?;
        if committed > suffix_len {
            return Err(record::invalid_data(
                "suffix is shorter than the commit witness",
            ));
        }
        if suffix_len > limits.suffix.bytes {
            return Err(invalid_input("suffix byte limit exceeded"));
        }
        let pending = committed < suffix_len;
        if let Some(limit) = scan_limit {
            limit.remaining(expected.base_bytes(), suffix_len, pending)?;
        }
        preflight(&header, suffix_len, pending)?;
        let base_path = dir.join("base");
        let base = match imported_base {
            Some(import) => RecoveryBase::Import(import),
            None => {
                let file = open_regular(&base_path)?;
                preflight_base(&file, limits, expected)?;
                RecoveryBase::Local(file)
            }
        };
        let suffix_len = usize::try_from(suffix_len)
            .map_err(|_| invalid_input("suffix length exceeds platform range"))?;
        let committed = usize::try_from(committed).unwrap();
        let mut overlay = Overlay::default();
        let suffix = SuffixFile {
            file: &log,
            start: header.len as u64,
            len: suffix_len,
        };
        let hash = overlay.replay_disk(
            suffix,
            (committed, committed_hash),
            limits.suffix,
            limits.overlay,
        )?;
        let replayed_digest: [u8; 32] = hash.clone().finalize().into();
        authorize(dir, &header, suffix_len as u64, replayed_digest)?;
        let base = match base {
            RecoveryBase::Import(import) => {
                import.copy_to(&base_path)?;
                open_regular(&base_path)?
            }
            RecoveryBase::Local(file) => file,
        };
        // A rejected suffix cannot trigger a base copy or full base scan.
        let mut base_limits = limits.base;
        if matches!(scan_limit, Some(RecoveryScanLimit::BaseAndSuffix(_))) {
            base_limits.file_bytes = base_limits.file_bytes.min(expected.base_bytes());
        }
        let base = IndexedBase::open(base, base_limits, limits.index)?;
        check_binding(base.metadata(), expected)?;
        lock.verify_identity(dir)?;
        verify_files(&log, &witness, header.bytes(), log_size, &saved)?;
        if pending {
            log.sync_data()?;
            // Bind the witness update to the bytes used to build the overlay.
            let mut observed = Sha256::new();
            suffix.copy_to(&mut observed)?;
            if <[u8; 32]>::from(observed.finalize()) != replayed_digest {
                return Err(record::invalid_data(
                    "retained suffix bytes changed after recovery replay",
                ));
            }
            verify_files(&log, &witness, header.bytes(), log_size, &saved)?;
        }
        check_selection()?;
        base.check_path(&base_path)?;
        let recovery = if pending {
            update_witness(&witness, header.bytes(), suffix_len, &hash)?;
            TailRecovery::AdoptedAndSynced
        } else {
            TailRecovery::AlreadyWitnessed
        };
        Ok((
            Self {
                base,
                log,
                witness,
                header,
                suffix_len,
                hash,
                overlay,
                limits,
                failed: false,
                _lock: lock,
            },
            recovery,
        ))
    }

    /// The unique identity generated when this writable generation was created.
    pub fn generation_id(&self) -> [u8; 16] {
        self.header.id()
    }

    fn sync_recovered_commit(&self) -> io::Result<()> {
        self.log.sync_data()?;
        self.witness.sync_data()
    }

    pub fn get(&self, key: &[u8], max_value_bytes: usize) -> io::Result<Option<Vec<u8>>> {
        self.ensure_healthy()?;
        self.overlay.get(
            &self.base,
            OverlaySource::Disk(self.suffix_file()),
            key,
            max_value_bytes,
        )
    }

    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        self.ensure_healthy()?;
        self.overlay.scan(
            &self.base,
            OverlaySource::Disk(self.suffix_file()),
            after,
            before,
            limit,
        )
    }

    pub(super) fn get_admitted(
        &self,
        key: &[u8],
        admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        self.ensure_healthy()?;
        self.overlay.get_admitted(
            &self.base,
            OverlaySource::Disk(self.suffix_file()),
            key,
            admit,
        )
    }

    fn suffix_file(&self) -> SuffixFile<'_> {
        SuffixFile {
            file: &self.log,
            start: self.header.len as u64,
            len: self.suffix_len,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MutationFailure> {
        self.write(Append::Put(key, value), |_| Ok(()))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), MutationFailure> {
        self.write(Append::Delete(key), |_| Ok(()))
    }

    pub fn write_batch(
        &mut self,
        operations: &[BatchOperation<'_>],
    ) -> Result<(), MutationFailure> {
        self.write(Append::Batch(operations), |_| Ok(()))
    }

    fn ensure_healthy(&self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "retained write outcome uncertain; reopen required",
            ));
        }
        Ok(())
    }

    fn write(
        &mut self,
        append: Append<'_>,
        hook: impl FnMut(CommitPoint) -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        self.ensure_healthy().map_err(MutationFailure::Rejected)?;
        let encoded = self.encode_append(append, false)?;
        self.write_encoded(&encoded, |_, _| Ok(()), hook)
    }

    fn encode_append(
        &self,
        append: Append<'_>,
        may_rotate: bool,
    ) -> Result<Vec<u8>, MutationFailure> {
        // Automatic maintenance can replace the suffix. It still cannot
        // admit work that would exceed the limits of an empty generation.
        let (bytes, operations) = if may_rotate {
            (0, 0)
        } else {
            (self.suffix_len, self.overlay.operations())
        };
        let admit = |length| {
            suffix_end(bytes, length, self.limits)?;
            if append.operations() > self.limits.suffix.operations.saturating_sub(operations) {
                return Err(suffix_capacity("suffix operation limit exceeded"));
            }
            Ok(())
        };
        let encoded = match append {
            Append::Put(key, value) => record::encode_admitted(Operation::Put, key, value, admit),
            Append::Delete(key) => record::encode_admitted(Operation::Delete, key, &[], admit),
            Append::Batch(operations) => record::batch::encode_record_admitted(operations, admit),
        };
        encoded.map_err(MutationFailure::Rejected)
    }

    fn write_encoded(
        &mut self,
        encoded: &[u8],
        admit: impl FnOnce(u64, [u8; 32]) -> io::Result<()>,
        mut hook: impl FnMut(CommitPoint) -> io::Result<()>,
    ) -> Result<(), MutationFailure> {
        crate::transaction_outcome::mutation(|tracker| {
            self.ensure_healthy()?;
            let end = suffix_end(self.suffix_len, encoded.len(), self.limits)?;
            let overlay = self.overlay.prepare_append(
                encoded,
                self.suffix_len,
                self.limits.suffix,
                self.limits.overlay,
            )?;
            let expected = marker(
                self.header.bytes(),
                self.suffix_len as u64,
                &self.hash.clone().finalize().into(),
            );
            if let Err(error) = verify_files(
                &self.log,
                &self.witness,
                self.header.bytes(),
                self.header.len as u64 + self.suffix_len as u64,
                &expected,
            ) {
                self.failed = true;
                return Err(error);
            }
            let mut hash = self.hash.clone();
            hash.update(encoded);
            let digest = hash.clone().finalize().into();
            admit(end as u64, digest)?;
            tracker.commit();
            self.failed = true;
            self.log.write_all(encoded)?;
            hook(CommitPoint::Appended)?;
            self.log.sync_data()?;
            hook(CommitPoint::LogSynced)?;
            write_witness(&self.witness, self.header.bytes(), end, &digest)?;
            #[cfg(test)]
            hook(CommitPoint::WitnessWritten)?;
            self.witness.sync_data()?;
            hook(CommitPoint::WitnessSynced)?;
            self.suffix_len = end;
            self.overlay.commit_append(overlay);
            self.hash = hash;
            self.failed = false;
            Ok(())
        })
    }
}

fn suffix_end(current: usize, incoming: usize, limits: RetainedStoreLimits) -> io::Result<usize> {
    current
        .checked_add(incoming)
        .filter(|&n| n as u64 <= limits.suffix.bytes)
        .ok_or_else(|| suffix_capacity("suffix byte limit exceeded"))
}

#[derive(Clone, Copy)]
enum Append<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
    Batch(&'a [BatchOperation<'a>]),
}

impl Append<'_> {
    fn operations(self) -> u64 {
        match self {
            Self::Put(..) | Self::Delete(..) => 1,
            Self::Batch(operations) => operations.len() as u64,
        }
    }
}

#[cfg(test)]
fn encode_batch(operations: &[BatchOperation<'_>]) -> io::Result<Vec<u8>> {
    record::encode(
        Operation::Batch,
        &[0],
        &record::batch::encode_payload(operations)?,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CommitPoint {
    Appended,
    LogSynced,
    #[cfg(test)]
    WitnessWritten,
    WitnessSynced,
}

fn create_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
}

enum RecoveryBase {
    Local(File),
    Import(BaseImport),
}

struct BaseImport {
    file: File,
    size: u64,
}

impl BaseImport {
    // The frame is checked up front. The private destination will still be
    // read back completely after copying. Keep the same source file pinned.
    fn open(
        candidate: &Path,
        limits: RetainedStoreLimits,
        expected: &Expected,
        max_output_bytes: u64,
    ) -> io::Result<Self> {
        let size = expected.base_bytes();
        if size > max_output_bytes {
            return Err(invalid_input("catalog byte limit exceeded"));
        }
        if size > limits.base.file_bytes
            || size
                .checked_add(1)
                .is_none_or(|n| n > limits.base.work_bytes)
        {
            return Err(invalid_input("retained base byte limit exceeded"));
        }
        let file = open_regular(&candidate.join("base"))?;
        preflight_base(&file, limits, expected)?;
        Ok(Self { file, size })
    }

    fn copy_to(self, destination: &Path) -> io::Result<()> {
        let mut base = self.file.take(self.size);
        let mut target = create_file(destination)?;
        let copied = io::copy(&mut base, &mut target)?;
        base.set_limit(1);
        if copied != self.size || io::copy(&mut base, &mut io::sink())? != 0 {
            return Err(record::invalid_data(
                "retained base length changed during copy",
            ));
        }
        target.sync_all()
    }
}

fn preflight_base(base: &File, limits: RetainedStoreLimits, expected: &Expected) -> io::Result<()> {
    let size = expected.base_bytes();
    if base.metadata()?.len() != size {
        return Err(record::invalid_data(
            "retained base length does not match its binding",
        ));
    }
    if size > limits.base.file_bytes
        || size
            .checked_add(1)
            .is_none_or(|n| n > limits.base.work_bytes)
    {
        return Err(record::invalid_data("retained base byte limit exceeded"));
    }
    // Reject impossible imports before copying any entries into the stage.
    // Reopening the staged generation still authenticates the entire base.
    IndexedBase::preflight(
        base,
        size,
        limits.base,
        limits.index,
        expected.base_binding(),
    )
}

fn random_id() -> io::Result<[u8; 16]> {
    let mut id = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut id)?;
    if id == [0; 16] {
        return Err(io::Error::other("random identifier is zero"));
    }
    Ok(id)
}

#[derive(Clone)]
struct Header {
    bytes: [u8; QUOTA_HEADER_BYTES],
    len: usize,
}

impl Header {
    fn for_role(role: GenerationRole) -> Self {
        let mut bytes = [0; QUOTA_HEADER_BYTES];
        let (version, len) = match role {
            GenerationRole::Managed(id) | GenerationRole::Authorized(id) => {
                bytes[HEADER_BYTES..MANAGED_HEADER_BYTES].copy_from_slice(&id);
                (
                    if matches!(role, GenerationRole::Authorized(_)) {
                        AUTHORIZED_VERSION
                    } else {
                        MANAGED_VERSION
                    },
                    MANAGED_HEADER_BYTES,
                )
            }
            GenerationRole::Standalone => (VERSION, HEADER_BYTES),
        };
        bytes[112..120].copy_from_slice(&version);
        Self { bytes, len }
    }

    fn new(
        expected: &Expected,
        limits: RetainedStoreLimits,
        role: GenerationRole,
    ) -> io::Result<Self> {
        let mut header = Self::for_role(role);
        header.bytes[..112].copy_from_slice(&expected.header);
        header.bytes[120..LIMITS_OFFSET].copy_from_slice(&random_id()?);
        header.fill_limits(limits);
        header.verify_identity(expected, None)?;
        Ok(header)
    }

    fn read(log: &File, role: GenerationRole) -> io::Result<Self> {
        let expected = Self::for_role(role);
        let mut observed = Self::for_role(role);
        log.read_exact_at(&mut observed.bytes[..observed.len], 0)?;
        let version = &observed.bytes[112..120];
        if quota_version(role).is_some_and(|quota| version == quota) {
            log.read_exact_at(
                &mut observed.bytes[MANAGED_HEADER_BYTES..],
                MANAGED_HEADER_BYTES as u64,
            )?;
            observed.len = QUOTA_HEADER_BYTES;
        } else if version != &expected.bytes[112..120] {
            return Err(record::invalid_data(
                "unsupported retained generation version",
            ));
        }
        if observed.bytes[HEADER_BYTES..MANAGED_HEADER_BYTES]
            != expected.bytes[HEADER_BYTES..MANAGED_HEADER_BYTES]
        {
            return Err(record::invalid_data(
                "retained generation ownership mismatch",
            ));
        }
        Ok(observed)
    }

    fn set_storage_limits(
        &mut self,
        role: GenerationRole,
        limits: CatalogStorageLimits,
    ) -> io::Result<()> {
        let version = quota_version(role)
            .ok_or_else(|| invalid_input("standalone generation has no catalog"))?;
        self.bytes[112..120].copy_from_slice(&version);
        self.bytes[MANAGED_HEADER_BYTES..MANAGED_HEADER_BYTES + 8]
            .copy_from_slice(&limits.bytes.to_le_bytes());
        self.bytes[MANAGED_HEADER_BYTES + 8..].copy_from_slice(&limits.entries.to_le_bytes());
        self.len = QUOTA_HEADER_BYTES;
        Ok(())
    }

    fn storage_limits(&self) -> Option<CatalogStorageLimits> {
        (self.len == QUOTA_HEADER_BYTES).then(|| CatalogStorageLimits {
            bytes: u64::from_le_bytes(
                self.bytes[MANAGED_HEADER_BYTES..MANAGED_HEADER_BYTES + 8]
                    .try_into()
                    .unwrap(),
            ),
            entries: u64::from_le_bytes(self.bytes[MANAGED_HEADER_BYTES + 8..].try_into().unwrap()),
        })
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    fn id(&self) -> [u8; 16] {
        self.bytes[120..LIMITS_OFFSET].try_into().unwrap()
    }

    fn fill_limits(&mut self, limits: RetainedStoreLimits) {
        for (slot, value) in self.bytes[LIMITS_OFFSET..HEADER_BYTES]
            .chunks_exact_mut(8)
            .zip(limits.fields())
        {
            slot.copy_from_slice(&value.to_le_bytes());
        }
    }

    fn verify_identity(&self, expected: &Expected, generation: Option<[u8; 16]>) -> io::Result<()> {
        let id = self.id();
        if self.bytes[..112] != expected.header
            || id == [0; 16]
            || generation.is_some_and(|expected| id != expected)
            || [
                &self.bytes[8..24],
                &self.bytes[24..40],
                &self.bytes[88..104],
            ]
            .contains(&id.as_slice())
        {
            return Err(record::invalid_data("retained generation header mismatch"));
        }
        Ok(())
    }

    fn recovery_limits(&self, requested: RecoveryLimits) -> io::Result<RetainedStoreLimits> {
        let mut fields = self.bytes[LIMITS_OFFSET..HEADER_BYTES]
            .chunks_exact(8)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()));
        match requested {
            RecoveryLimits::Exact(limits) => {
                if fields.eq(limits.fields()) {
                    Ok(limits)
                } else {
                    Err(invalid_input(
                        "retained recovery limits differ from persisted limits",
                    ))
                }
            }
            RecoveryLimits::Ceiling(ceiling) => {
                let limits: [u64; 11] = std::array::from_fn(|_| fields.next().unwrap());
                if limits.iter().zip(ceiling.fields()).any(|(&n, max)| n > max) {
                    return Err(invalid_input(
                        "persisted retained limits exceed recovery ceiling",
                    ));
                }
                let [
                    file_bytes,
                    entries,
                    key_bytes,
                    work_bytes,
                    blocks,
                    anchor_bytes,
                    suffix_bytes,
                    operations,
                    work,
                    overlay_entries,
                    overlay_bytes,
                ] = limits;
                // The comparison with the caller's usize limits makes these casts lossless.
                Ok(RetainedStoreLimits {
                    base: Limits {
                        file_bytes,
                        entries,
                        key_bytes,
                        work_bytes,
                    },
                    index: IndexLimits {
                        blocks: blocks as usize,
                        anchor_bytes: anchor_bytes as usize,
                    },
                    suffix: SuffixLimits {
                        bytes: suffix_bytes,
                        operations,
                        work,
                    },
                    overlay: OverlayLimits {
                        entries: overlay_entries as usize,
                        key_bytes: overlay_bytes as usize,
                    },
                })
            }
        }
    }
}

fn quota_version(role: GenerationRole) -> Option<[u8; 8]> {
    match role {
        GenerationRole::Standalone => None,
        GenerationRole::Managed(_) => Some(MANAGED_QUOTA_VERSION),
        GenerationRole::Authorized(_) => Some(AUTHORIZED_QUOTA_VERSION),
    }
}

fn marker(header: &[u8], end: u64, digest: &[u8; 32]) -> [u8; WITNESS_BYTES] {
    let mut result = [0; WITNESS_BYTES];
    result[..8].copy_from_slice(&WITNESS_MAGIC);
    result[8..40].copy_from_slice(&Sha256::digest(header));
    result[40..48].copy_from_slice(&end.to_le_bytes());
    result[48..80].copy_from_slice(digest);
    let crc = crc32fast::hash(&result[..80]);
    result[80..].copy_from_slice(&crc.to_le_bytes());
    result
}

fn decode_marker(bytes: &[u8; WITNESS_BYTES], header: &[u8]) -> io::Result<(u64, [u8; 32])> {
    let end = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
    let digest = bytes[48..80].try_into().unwrap();
    if *bytes != marker(header, end, &digest) {
        return Err(record::invalid_data(
            "invalid retained commit witness or generation",
        ));
    }
    Ok((end, digest))
}

fn update_witness(file: &File, header: &[u8], end: usize, hash: &Sha256) -> io::Result<()> {
    write_witness(file, header, end, &hash.clone().finalize().into())?;
    file.sync_data()
}

fn write_witness(file: &File, header: &[u8], end: usize, digest: &[u8; 32]) -> io::Result<()> {
    file.write_all_at(&marker(header, end as u64, digest), 0)
}

fn read_marker(witness: &File) -> io::Result<[u8; WITNESS_BYTES]> {
    if witness.metadata()?.len() != WITNESS_BYTES as u64 {
        return Err(record::invalid_data(
            "invalid retained commit witness length",
        ));
    }
    let mut bytes = [0; WITNESS_BYTES];
    witness.read_exact_at(&mut bytes, 0)?;
    Ok(bytes)
}

fn verify_files(
    log: &File,
    witness: &File,
    header: &[u8],
    log_size: u64,
    saved: &[u8; WITNESS_BYTES],
) -> io::Result<()> {
    let mut observed = [0; QUOTA_HEADER_BYTES];
    log.read_exact_at(&mut observed[..header.len()], 0)?;
    if observed[..header.len()] != *header
        || log.metadata()?.len() != log_size
        || read_marker(witness)? != *saved
    {
        return Err(record::invalid_data("retained log or witness changed"));
    }
    Ok(())
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
