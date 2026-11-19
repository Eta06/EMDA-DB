//! Experimental, non-authoritative retained candidate. This format is not a store.
//! Callers must keep the directory and its parents trusted and stable throughout
//! verification, and obtain the expected binding independently.
pub use crate::retained_base_v2::index::IndexLimits;
use crate::retained_base_v2::index::IndexedBase;
use crate::retained_base_v2::{self, Limits, Validated};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Read},
    path::Path,
};

mod indexed_suffix;
pub use indexed_suffix::{IndexedSuffix, OverlayLimits};
mod suffix_file;
mod writable;
pub use writable::{
    AuthorityPoint, AuthorityRecord, AuthorizedRetainedStore, CutoverArchiveBinding,
    FileRetainedAuthority, ManagedRetainedStore, RetainedAuthority, RetainedStore,
    RetainedStoreLimits,
};

/// An independently retained copy of the exact candidate header and witness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expected {
    pub header: [u8; 112],
    pub witness: [u8; 32],
    pub base_digest: [u8; 32],
}

/// Publication failure for a candidate. A binding is returned only after the
/// complete staged candidate has passed verification. `NotPublished` does not
/// imply that the destination was absent before the call or that all staging
/// artifacts were removed. For `OutcomeUnknown`, inspect and reconcile the
/// destination before retrying and retain `expected` independently of it.
#[derive(Debug)]
pub enum Failure {
    NotPublished(io::Error),
    OutcomeUnknown {
        error: io::Error,
        expected: Box<Expected>,
    },
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPublished(e) => write!(f, "candidate publication not started: {e}"),
            Self::OutcomeUnknown { error, .. } => {
                write!(f, "candidate publication outcome unknown: {error}")
            }
        }
    }
}
impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotPublished(error) | Self::OutcomeUnknown { error, .. } => Some(error),
        }
    }
}

pub(crate) fn binding(base: &Validated, log_id: [u8; 16]) -> io::Result<Expected> {
    if log_id == [0; 16] || log_id == base.source_id || log_id == base.base_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid new log identity",
        ));
    }
    let mut header = [0; 112];
    header[..8].copy_from_slice(b"EMDALOG4");
    header[8..24].copy_from_slice(&base.source_id);
    header[24..40].copy_from_slice(&base.base_id);
    header[40..48].copy_from_slice(&base.boundary.to_le_bytes());
    header[48..56].copy_from_slice(&base.file_bytes.to_le_bytes());
    header[56..88].copy_from_slice(&base.digest);
    header[88..104].copy_from_slice(&log_id);
    // Bytes 104..112 encode the empty suffix boundary, zero.
    Ok(binding_from_header(header))
}

fn binding_from_header(header: [u8; 112]) -> Expected {
    let mut hash = Sha256::new();
    hash.update(b"EMDA experimental candidate header v1\0");
    hash.update(header);
    Expected {
        header,
        witness: hash.finalize().into(),
        base_digest: header[56..88].try_into().unwrap(),
    }
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid candidate")
}
fn open_regular(path: &Path) -> io::Result<fs::File> {
    open_regular_with(path, rustix::fs::OFlags::RDONLY)
}

fn open_regular_with(path: &Path, flags: rustix::fs::OFlags) -> io::Result<fs::File> {
    use rustix::fs::{Mode, OFlags, open};
    // Reject known non-regular files before open; a stable trusted directory is
    // required because type checks cannot prevent a concurrent replacement.
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid());
    }
    let file = fs::File::from(open(
        path,
        flags | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )?);
    if !file.metadata()?.is_file() {
        return Err(invalid());
    }
    Ok(file)
}

#[cfg(test)]
#[test]
fn open_regular_rejects_fifo_and_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let fifo = dir.path().join("fifo");
    #[cfg(target_os = "linux")]
    {
        rustix::fs::mkfifoat(rustix::fs::CWD, &fifo, rustix::fs::Mode::RUSR).unwrap();
        assert_eq!(
            open_regular(&fifo).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
    #[cfg(not(target_os = "linux"))]
    fs::write(&fifo, b"regular target").unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&fifo, &link).unwrap();
    assert_eq!(
        open_regular(&link).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}
fn exact(path: &Path, len: usize) -> io::Result<Vec<u8>> {
    let mut file = open_regular(path)?;
    if file.metadata()?.len() != len as u64 {
        return Err(invalid());
    }
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)?;
    let mut extra = [0];
    if file.read(&mut extra)? != 0 {
        return Err(invalid());
    }
    Ok(bytes)
}
/// Verify all candidate files without exposing values. Limits apply to the base,
/// not the whole candidate. Path checks do not prevent concurrent replacement;
/// the caller must keep the directory and its ancestors trusted and stable.
pub fn verify(
    directory: impl AsRef<Path>,
    limits: Limits,
    expected: &Expected,
) -> io::Result<Validated> {
    let dir = directory.as_ref();
    check_files(dir, expected)?;
    let base = retained_base_v2::validate_buffered(open_regular(&dir.join("base"))?, limits)?;
    check_binding(&base, expected)?;
    Ok(base)
}

/// A read-only retained candidate with a sparse in-memory directory and one
/// captured base file descriptor. The complete base and candidate binding are
/// verified at open. Reads verify each touched block as they capture results.
/// Untouched blocks and candidate companion files are checked at open only.
pub struct IndexedCandidate {
    base: IndexedBase,
}

impl IndexedCandidate {
    /// Verify an immutable candidate against a binding retained independently.
    /// The directory and its ancestors must be trusted and stable during open.
    /// The base must remain immutable for this handle's lifetime. The index
    /// limits bound the directory without storing per-key entries or values.
    /// A limit or I/O error can precede full binding verification.
    pub fn open(
        directory: impl AsRef<Path>,
        limits: Limits,
        index_limits: IndexLimits,
        expected: &Expected,
    ) -> io::Result<Self> {
        let dir = directory.as_ref();
        check_files(dir, expected)?;
        let base = IndexedBase::open(open_regular(&dir.join("base"))?, limits, index_limits)?;
        check_binding(base.metadata(), expected)?;
        Ok(Self { base })
    }

    pub fn metadata(&self) -> &Validated {
        self.base.metadata()
    }

    /// Find one key and verify its candidate block before returning a value.
    /// Limit and I/O errors do not establish that the key exists.
    pub fn get(&self, key: &[u8], max_value_bytes: usize) -> io::Result<Option<Vec<u8>>> {
        self.base.get(key, max_value_bytes)
    }

    /// Return a byte-ordered page using exclusive bounds. Each page is capped
    /// by the ordinary store's maximum scan entries and result bytes.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<crate::Entry>> {
        self.base.scan(after, before, limit)
    }
}

/// Read one value only after validating the entire candidate against an independently
/// retained binding. The directory, its parents, and their contents must remain
/// trusted and stable throughout the operation. Limits apply to the base stream.
/// A limit or I/O error can occur before full validation; it does not establish
/// that the key exists or that the candidate matches the binding.
/// This is a read-only, non-authoritative operation, not a store or recovery.
pub fn lookup(
    directory: impl AsRef<Path>,
    limits: Limits,
    key: &[u8],
    max_value_bytes: usize,
    expected: &Expected,
) -> io::Result<Option<Vec<u8>>> {
    let dir = directory.as_ref();
    check_files(dir, expected)?;
    let base_expected = retained_base_v2::Expected {
        digest: expected.base_digest,
        source_id: expected.header[8..24].try_into().unwrap(),
        base_id: expected.header[24..40].try_into().unwrap(),
        boundary: u64::from_le_bytes(expected.header[40..48].try_into().unwrap()),
    };
    let (base, value) = retained_base_v2::lookup_with_metadata(
        open_regular(&dir.join("base"))?,
        limits,
        key,
        max_value_bytes,
        base_expected,
    )?;
    check_binding(&base, expected)?;
    Ok(value)
}

/// Load the complete live state privately, returning it only after stream and
/// independently supplied candidate binding validation. The directory, parents,
/// and contents must remain trusted and stable throughout. Limits bound entries,
/// total key bytes, each value, and staged key/value payload. Container and
/// allocator overhead is bounded by the entry limit, but is not payload bytes.
/// Budget or I/O errors may precede full validation and do not establish that
/// the candidate matches the independently supplied binding.
/// This is non-authoritative and does not open or modify a store.
pub fn load(
    directory: impl AsRef<Path>,
    limits: Limits,
    staging_limits: retained_base_v2::StagingLimits,
    expected: &Expected,
) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let dir = directory.as_ref();
    check_files(dir, expected)?;
    let (base, staged) = retained_base_v2::load_with_metadata(
        open_regular(&dir.join("base"))?,
        limits,
        staging_limits,
    )?;
    check_binding(&base, expected)?;
    Ok(staged)
}

fn check_files(dir: &Path, expected: &Expected) -> io::Result<()> {
    if !fs::symlink_metadata(dir)?.file_type().is_dir() {
        return Err(invalid());
    }
    let mut names = Vec::new();
    for entry in fs::read_dir(dir)? {
        if names.len() == 4 {
            return Err(invalid());
        }
        names.push(entry?.file_name());
    }
    names.sort();
    if names != ["base", "records.log", "store.lock", "witness"].map(std::ffi::OsString::from) {
        return Err(invalid());
    }
    exact(&dir.join("store.lock"), 0)?;
    let header = exact(&dir.join("records.log"), 112)?;
    let witness = exact(&dir.join("witness"), 32)?;
    if header != expected.header || witness != expected.witness {
        return Err(invalid());
    }
    Ok(())
}

fn check_binding(base: &Validated, expected: &Expected) -> io::Result<()> {
    if base.digest != expected.base_digest {
        return Err(invalid());
    }
    let log_id = expected.header[88..104].try_into().unwrap();
    if binding(base, log_id).map_err(|_| invalid())? != *expected {
        return Err(invalid());
    }
    Ok(())
}

/// Independently retained binding for a detached suffix of this exact candidate.
/// `end` is relative to the candidate's empty suffix boundary. The digest is
/// SHA-256 of the exact suffix bytes; neither it nor the boundary is authority.
#[derive(Clone, Debug)]
pub struct SuffixExpected {
    pub candidate: Expected,
    pub end: u64,
    pub digest: [u8; 32],
}

/// Bounds on detached suffix input and replay work. Single records count as one
/// operation; batches count their nested operations. Base and live-set limits are separate.
/// Work bounds entry visits and vector shifts only, not byte comparisons or allocation.
/// The suffix byte limit separately bounds hashing and checksumming; batch validation
/// is also bounded by the record format limits. Other verifier allocations may be infallible.
#[derive(Clone, Copy, Debug)]
pub struct SuffixLimits {
    pub bytes: u64,
    pub operations: u64,
    /// Maximum initial live-entry visits and suffix entry visits and shifts.
    pub work: u64,
}

/// Open a detached exported suffix and privately replay it with its candidate.
/// The independently retained binding identifies the candidate's source and
/// captured boundary and the exact suffix endpoint and digest; it is not proof
/// of freshness or authority. Directory ancestors and contents must remain
/// trusted and stable during the operation. Final-component symlinks are rejected.
/// The file is read once, with exact EOF, into a buffer bounded by `suffix_limits.bytes`;
/// the same bytes are hashed and replayed. Peak memory includes that buffer plus
/// the staged live set, transient replacement values, batch decoding and allocator
/// overhead. Entry and payload limits do not constitute a total process memory cap.
/// Errors return no state and may precede full verification. This does not open,
/// modify, select, or recover an ordinary store.
pub fn open_exported_suffix(
    directory: impl AsRef<Path>,
    suffix_path: impl AsRef<Path>,
    limits: Limits,
    staging: retained_base_v2::StagingLimits,
    suffix_limits: SuffixLimits,
    expected: &SuffixExpected,
) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let bytes = read_suffix(suffix_path.as_ref(), suffix_limits, expected.end)?;
    replay_suffix(directory, limits, staging, &bytes, suffix_limits, expected)
}

fn read_suffix(path: &Path, limits: SuffixLimits, end: u64) -> io::Result<Vec<u8>> {
    let (mut file, len) = open_suffix(path, limits, end)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| io::Error::other("suffix allocation failed"))?;
    bytes.resize(len, 0);
    file.read_exact(&mut bytes)?;
    require_suffix_eof(&mut file)?;
    Ok(bytes)
}

fn open_suffix(path: &Path, limits: SuffixLimits, end: u64) -> io::Result<(fs::File, usize)> {
    if end > limits.bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "suffix byte limit exceeded",
        ));
    }
    let len = usize::try_from(end).map_err(|_| invalid())?;
    let file = open_regular(path)?;
    if file.metadata()?.len() != end {
        return Err(invalid());
    }
    Ok((file, len))
}

fn require_suffix_eof(reader: &mut impl Read) -> io::Result<()> {
    let mut extra = [0];
    loop {
        match reader.read(&mut extra) {
            Ok(0) => return Ok(()),
            Ok(_) => return Err(invalid()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

/// Validate and privately replay a detached suffix against an independently
/// trusted binding. The candidate directory and suffix bytes must stay stable
/// throughout. Errors, including budget errors, can precede full validation and
/// establish nothing about authenticity. No partial state is returned. This is
/// an offline, non-authoritative inspection; it does not modify or open a store.
pub fn replay_suffix(
    directory: impl AsRef<Path>,
    limits: Limits,
    staging: retained_base_v2::StagingLimits,
    suffix: &[u8],
    suffix_limits: SuffixLimits,
    expected: &SuffixExpected,
) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    check_suffix_binding(suffix, suffix_limits, expected)?;
    let mut state = load(directory, limits, staging, &expected.candidate)?;
    let initial_work = u64::try_from(state.len()).map_err(|_| invalid())?;
    if initial_work > suffix_limits.work {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "suffix work limit exceeded",
        ));
    }
    let (entries, keys, payload) = live_totals(&state)?;
    if entries > limits.entries || keys > limits.key_bytes || payload > staging.staging_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "live set limit exceeded",
        ));
    }
    let mut replay = ReplayBudget {
        limits,
        staging,
        work: initial_work,
        work_limit: suffix_limits.work,
        entries,
        keys,
        payload,
    };
    visit_suffix(suffix, suffix_limits, |operation, key, value| {
        apply_suffix(&mut state, operation, key, &suffix[value], &mut replay)
    })?;
    Ok(state)
}

fn check_suffix_binding(
    suffix: &[u8],
    suffix_limits: SuffixLimits,
    expected: &SuffixExpected,
) -> io::Result<()> {
    let length = u64::try_from(suffix.len()).map_err(|_| invalid())?;
    if length > suffix_limits.bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "suffix byte limit exceeded",
        ));
    }
    if length != expected.end || <[u8; 32]>::from(Sha256::digest(suffix)) != expected.digest {
        return Err(invalid());
    }
    Ok(())
}

fn visit_suffix(
    suffix: &[u8],
    suffix_limits: SuffixLimits,
    mut visit: impl FnMut(crate::record::Operation, &[u8], std::ops::Range<usize>) -> io::Result<()>,
) -> io::Result<u64> {
    use crate::record::{self, CHECKSUM_SIZE, HEADER_SIZE, Operation};

    let mut cursor = 0;
    let mut operations = 0u64;
    while cursor < suffix.len() {
        let header_end = cursor.checked_add(HEADER_SIZE).ok_or_else(invalid)?;
        let header: &[u8; HEADER_SIZE] = suffix
            .get(cursor..header_end)
            .ok_or_else(invalid)?
            .try_into()
            .unwrap();
        let parsed = record::Header::parse(header)?;
        let end = cursor
            .checked_add(usize::try_from(parsed.record_len()).map_err(|_| invalid())?)
            .ok_or_else(invalid)?;
        let data_end = end.checked_sub(CHECKSUM_SIZE).ok_or_else(invalid)?;
        let payload = suffix.get(header_end..data_end).ok_or_else(invalid)?;
        let checksum = suffix.get(data_end..end).ok_or_else(invalid)?;
        record::verify(
            header,
            payload,
            u32::from_le_bytes(checksum.try_into().unwrap()),
        )?;
        let (key, value) = payload.split_at(parsed.key_len);
        let value_start = header_end.checked_add(parsed.key_len).ok_or_else(invalid)?;
        if parsed.operation == Operation::Batch {
            if key != [0] {
                return Err(invalid());
            }
            let count = record::batch::payload_count(value)?;
            operations = operations.checked_add(count as u64).ok_or_else(invalid)?;
            if operations > suffix_limits.operations {
                return Err(suffix_capacity("suffix operation limit exceeded"));
            }
            allocation_point()?;
            let mut decoded = Vec::new();
            decoded
                .try_reserve_exact(count)
                .map_err(|_| io::Error::other("batch allocation failed"))?;
            let decoded = record::batch::decode_payload_into(value, decoded)?;
            for op in decoded {
                let start = value_start
                    .checked_add(op.value_offset)
                    .ok_or_else(invalid)?;
                let end = start.checked_add(op.value_len).ok_or_else(invalid)?;
                visit(op.operation, op.key, start..end)?;
            }
        } else {
            operations = operations.checked_add(1).ok_or_else(invalid)?;
            if operations > suffix_limits.operations {
                return Err(suffix_capacity("suffix operation limit exceeded"));
            }
            visit(parsed.operation, key, value_start..data_end)?;
        }
        cursor = end;
    }
    Ok(operations)
}

#[cfg(test)]
thread_local! { pub(crate) static FAIL_ALLOCATION: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) }; }

fn allocation_point() -> io::Result<()> {
    #[cfg(test)]
    FAIL_ALLOCATION.with(|slot| {
        if let Some(n) = slot.get() {
            if n == 0 {
                return Err(io::Error::other("injected allocation failure"));
            }
            slot.set(Some(n - 1));
        }
        Ok(())
    })?;
    Ok(())
}

struct ReplayBudget {
    limits: Limits,
    staging: retained_base_v2::StagingLimits,
    work: u64,
    work_limit: u64,
    entries: u64,
    keys: u64,
    payload: u64,
}

fn live_totals(state: &[(Vec<u8>, Vec<u8>)]) -> io::Result<(u64, u64, u64)> {
    let mut keys = 0u64;
    let mut payload = 0u64;
    for (key, value) in state {
        let k = u64::try_from(key.len()).map_err(|_| invalid())?;
        let v = u64::try_from(value.len()).map_err(|_| invalid())?;
        keys = keys.checked_add(k).ok_or_else(invalid)?;
        payload = payload
            .checked_add(k)
            .and_then(|n| n.checked_add(v))
            .ok_or_else(invalid)?;
    }
    Ok((
        u64::try_from(state.len()).map_err(|_| invalid())?,
        keys,
        payload,
    ))
}

#[derive(Debug)]
struct SuffixCapacity(&'static str);

impl std::fmt::Display for SuffixCapacity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for SuffixCapacity {}

fn suffix_capacity(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, SuffixCapacity(message))
}

fn charge_suffix_work(work: &mut u64, limit: u64, entries: usize) -> io::Result<()> {
    // Conservatively bound binary search comparisons and vector shifts.
    // The extra unit covers the final binary-search comparison.
    let cost = u64::try_from(entries)
        .map_err(|_| invalid())?
        .checked_mul(3)
        .and_then(|n| n.checked_add(1))
        .ok_or_else(invalid)?;
    *work = work.checked_add(cost).ok_or_else(invalid)?;
    if *work > limit {
        return Err(suffix_capacity("suffix work limit exceeded"));
    }
    Ok(())
}

fn apply_suffix(
    state: &mut Vec<(Vec<u8>, Vec<u8>)>,
    operation: crate::record::Operation,
    key: &[u8],
    value: &[u8],
    budget: &mut ReplayBudget,
) -> io::Result<()> {
    charge_suffix_work(&mut budget.work, budget.work_limit, state.len())?;
    let position = state.binary_search_by(|entry| entry.0.as_slice().cmp(key));
    if operation == crate::record::Operation::Delete {
        if let Ok(index) = position {
            let (k, v) = &state[index];
            let kl = u64::try_from(k.len()).map_err(|_| invalid())?;
            let vl = u64::try_from(v.len()).map_err(|_| invalid())?;
            let entries = budget.entries.checked_sub(1).ok_or_else(invalid)?;
            let keys = budget.keys.checked_sub(kl).ok_or_else(invalid)?;
            let payload = budget
                .payload
                .checked_sub(kl)
                .and_then(|n| n.checked_sub(vl))
                .ok_or_else(invalid)?;
            state.remove(index);
            (budget.entries, budget.keys, budget.payload) = (entries, keys, payload);
        }
        return Ok(());
    }
    if value.len() > budget.staging.value_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "value limit exceeded",
        ));
    }
    let old = position.ok().map(|i| &state[i]);
    let entries = budget
        .entries
        .checked_add(u64::from(old.is_none()))
        .ok_or_else(invalid)?;
    let (mut keys, mut payload) = (budget.keys, budget.payload);
    let key_len = u64::try_from(key.len()).map_err(|_| invalid())?;
    let value_len = u64::try_from(value.len()).map_err(|_| invalid())?;
    if old.is_none() {
        keys = keys.checked_add(key_len).ok_or_else(invalid)?;
    }
    if let Some((k, v)) = old {
        payload = payload
            .checked_sub(u64::try_from(k.len()).map_err(|_| invalid())?)
            .and_then(|n| {
                u64::try_from(v.len())
                    .ok()
                    .and_then(|len| n.checked_sub(len))
            })
            .ok_or_else(invalid)?;
    }
    payload = payload
        .checked_add(key_len)
        .and_then(|n| n.checked_add(value_len))
        .ok_or_else(invalid)?;
    if entries > budget.limits.entries
        || keys > budget.limits.key_bytes
        || payload > budget.staging.staging_bytes
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "live set limit exceeded",
        ));
    }
    fn copy(bytes: &[u8]) -> io::Result<Vec<u8>> {
        allocation_point()?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(bytes.len())
            .map_err(|_| io::Error::other("suffix allocation failed"))?;
        result.extend_from_slice(bytes);
        Ok(result)
    }
    let new_value = copy(value)?;
    match position {
        Ok(i) => state[i].1 = new_value,
        Err(i) => {
            let new_key = copy(key)?;
            allocation_point()?;
            state
                .try_reserve(1)
                .map_err(|_| io::Error::other("suffix allocation failed"))?;
            state.insert(i, (new_key, new_value));
        }
    }
    (budget.entries, budget.keys, budget.payload) = (entries, keys, payload);
    Ok(())
}

#[cfg(test)]
mod suffix_budget_tests {
    use super::*;
    use crate::record::Operation;

    #[test]
    fn shifted_insert_and_delete_obey_work_limit() {
        let mut state = vec![(vec![2], vec![2]), (vec![3], vec![3])];
        let mut budget = ReplayBudget {
            limits: Limits {
                file_bytes: u64::MAX,
                entries: u64::MAX,
                key_bytes: u64::MAX,
                work_bytes: u64::MAX,
            },
            staging: retained_base_v2::StagingLimits {
                value_bytes: usize::MAX,
                staging_bytes: u64::MAX,
            },
            work: 0,
            work_limit: 17,
            entries: 2,
            keys: 2,
            payload: 4,
        };
        apply_suffix(&mut state, Operation::Put, &[1], &[1], &mut budget).unwrap();
        apply_suffix(&mut state, Operation::Delete, &[1], &[], &mut budget).unwrap();
        assert_eq!(budget.work, 17);
        assert_eq!(state, vec![(vec![2], vec![2]), (vec![3], vec![3])]);
        assert!(apply_suffix(&mut state, Operation::Put, &[1], &[1], &mut budget).is_err());
    }

    #[test]
    fn seeded_totals_match_rescan_and_fail_atomically() {
        let mut state = Vec::new();
        let mut budget = ReplayBudget {
            limits: Limits {
                file_bytes: u64::MAX,
                entries: 8,
                key_bytes: 8,
                work_bytes: u64::MAX,
            },
            staging: retained_base_v2::StagingLimits {
                value_bytes: 5,
                staging_bytes: 24,
            },
            work: 0,
            work_limit: u64::MAX,
            entries: 0,
            keys: 0,
            payload: 0,
        };
        let mut seed = 0x12345678u64;
        for _ in 0..2000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = [(seed >> 32) as u8 % 12];
            let value = vec![7; (seed as usize >> 4) % 7];
            let op = if seed & 1 == 0 {
                Operation::Put
            } else {
                Operation::Delete
            };
            let before = state.clone();
            let totals = (budget.entries, budget.keys, budget.payload);
            if apply_suffix(&mut state, op, &key, &value, &mut budget).is_err() {
                assert_eq!(state, before);
                assert_eq!((budget.entries, budget.keys, budget.payload), totals);
            }
            assert_eq!(
                live_totals(&state).unwrap(),
                (budget.entries, budget.keys, budget.payload)
            );
        }
        budget.payload = u64::MAX;
        let before = state.clone();
        assert!(apply_suffix(&mut state, Operation::Put, &[99], &[1], &mut budget).is_err());
        assert_eq!(state, before);
        assert_eq!(budget.payload, u64::MAX);
    }

    #[test]
    fn work_overflow_does_not_mutate_state() {
        let mut state = vec![(vec![1], vec![2])];
        let mut budget = ReplayBudget {
            limits: Limits {
                file_bytes: u64::MAX,
                entries: u64::MAX,
                key_bytes: u64::MAX,
                work_bytes: u64::MAX,
            },
            staging: retained_base_v2::StagingLimits {
                value_bytes: usize::MAX,
                staging_bytes: u64::MAX,
            },
            work: u64::MAX,
            work_limit: u64::MAX,
            entries: 1,
            keys: 1,
            payload: 2,
        };
        assert!(apply_suffix(&mut state, Operation::Delete, &[1], &[], &mut budget).is_err());
        assert_eq!(state, vec![(vec![1], vec![2])]);
    }
}

/// Independent limits for the newly created store. Input validation limits are
/// supplied separately to [`restore_exported_suffix`].
#[derive(Clone, Copy, Debug)]
pub struct RestoreLimits {
    pub log_bytes: u64,
    pub live_keys: usize,
    pub live_key_bytes: usize,
}

/// Restore a validated candidate and detached suffix into a new directory.
///
/// This creates a new log identity and history. Historical key-count and
/// key-byte peaks are neither preserved nor enforced. Input validation and
/// output creation use distinct limits. The independently retained binding
/// must be trusted by the caller; all external directories and their ancestors
/// must remain trusted and stable throughout. The complete reconstructed state
/// and the new store index coexist in memory. The caller must keep the source
/// and suffix separate from the destination; only candidate descendants are
/// rejected here. This is not recovery of the old
/// witness, a live switch or reclamation, or anti-rollback protection.
/// The destination must not exist. A publication outcome error requires
/// inspection before retrying. Source files are not modified.
#[allow(clippy::too_many_arguments)]
pub fn restore_exported_suffix(
    candidate: impl AsRef<Path>,
    suffix: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: Limits,
    staging: retained_base_v2::StagingLimits,
    suffix_limits: SuffixLimits,
    expected: &SuffixExpected,
    output: RestoreLimits,
) -> Result<(), crate::PublicationFailure> {
    restore_with_sync(
        candidate.as_ref(),
        suffix.as_ref(),
        destination.as_ref(),
        limits,
        staging,
        suffix_limits,
        expected,
        output,
        crate::directory::sync_existing,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn restore_with_sync(
    candidate: &Path,
    suffix: &Path,
    destination: &Path,
    limits: Limits,
    staging: retained_base_v2::StagingLimits,
    suffix_limits: SuffixLimits,
    expected: &SuffixExpected,
    output: RestoreLimits,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> Result<(), crate::PublicationFailure> {
    restore_with_hooks(
        candidate,
        suffix,
        destination,
        limits,
        staging,
        suffix_limits,
        expected,
        output,
        sync_parent,
        |_| Ok(()),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn restore_with_hooks(
    candidate: &Path,
    suffix: &Path,
    destination: &Path,
    limits: Limits,
    staging: retained_base_v2::StagingLimits,
    suffix_limits: SuffixLimits,
    expected: &SuffixExpected,
    output: RestoreLimits,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    hook: impl Fn(&str) -> io::Result<()>,
) -> Result<(), crate::PublicationFailure> {
    crate::publication_outcome::track(|tracker| {
        let parent = crate::backup::parent_directory(destination)?;
        let candidate_real = fs::canonicalize(candidate)?;
        let parent_real = fs::canonicalize(parent)?;
        if parent_real.starts_with(&candidate_real) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination parent is inside candidate",
            ));
        }
        let state =
            open_exported_suffix(candidate, suffix, limits, staging, suffix_limits, expected)?;
        let mut stage = crate::staging::Stage::directory(parent)?;
        {
            hook("init")?;
            let store = crate::Store::open_with_resource_limits(
                stage.path(),
                Some(output.log_bytes),
                Some(output.live_keys),
                Some(output.live_key_bytes),
            )?;
            for (key, value) in &state {
                hook("write")?;
                store.put(key, value)?;
            }
            store.close_private_store()?;
        }
        {
            hook("reopen")?;
            let store = crate::Store::open_with_resource_limits(
                stage.path(),
                Some(output.log_bytes),
                Some(output.live_keys),
                Some(output.live_key_bytes),
            )?;
            hook("check")?;
            let mut after: Option<Vec<u8>> = None;
            let mut count = 0usize;
            loop {
                let page = store.scan(after.as_deref(), None, 1)?;
                if page.is_empty() {
                    break;
                }
                let (key, value) = &page[0];
                if !state
                    .get(count)
                    .is_some_and(|entry| &entry.0 == key && &entry.1 == value)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "restored state mismatch",
                    ));
                }
                count += 1;
                after = Some(key.clone());
            }
            if count != state.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "restored state mismatch",
                ));
            }
            store.close_private_store()?;
        }
        hook("stage_sync")?;
        crate::directory::sync_existing(stage.path())?;
        tracker.begin_publication();
        hook("rename")?;
        stage.publish(destination)?;
        #[cfg(test)]
        hook("published")?;
        sync_parent(parent)?;
        stage.publication_synchronized();
        Ok(())
    })
}
