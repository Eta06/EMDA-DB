//! Persistent logical disk budget for one managed checkpoint-backed store.
//!
//! The budget charges logical file lengths, never filesystem block
//! allocation. Charged bytes are the sum of:
//!
//! - every regular file under the store directory, plus the selected
//!   checkpoint file when it lies outside that directory, and one spare
//!   record length for the budget file's atomic replacement;
//! - a reservation covering the complete private replacement while online
//!   compaction runs, kept until it is reconciled if interrupted;
//! - a previous generation still named at its `previous` path or held in a
//!   marked retired stage owned by the live generation, measured while a
//!   retirement is pending;
//! - retired generations that owned snapshots in this process still hold
//!   open, at their logical length when retired, and an external checkpoint
//!   an automatic rollover deselected while a snapshot still reads it;
//! - while an automatic checkpoint is prepared under the writer gate, an
//!   in-memory reservation for its slot, staging markers, selection
//!   replacement and the append that triggered it. Restart needs no record
//!   of it: the open removes automatic stages and measures every remaining
//!   file before admitting anything.
//!
//! Log appends and online maintenance are admitted against this sum before
//! they allocate. The record is persisted inside the store directory and is
//! reloaded by every checkpoint-store open of that directory.
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

pub(crate) const BUDGET_FILE: &str = "disk.budget";
pub(crate) const BUDGET_TEMP: &str = "disk.budget.tmp";
pub(crate) const RECORD_BYTES: usize = 4096;
const MAGIC: &[u8; 8] = b"EMDADB01";
const PATH_OFFSET: usize = 27;
const DIGEST_OFFSET: usize = RECORD_BYTES - 32;

/// Conservative allowance for a replacement's small files: its selection,
/// commit witness, lock, budget record and that record's temporary copy,
/// plus staging markers in the parent directory.
pub(crate) const METADATA_ALLOWANCE: u64 = 64 * 1024;

/// Maintenance recorded in a managed store's persistent disk budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DiskBudgetPending {
    /// No online maintenance is outstanding.
    None,
    /// Online compaction began and its full reservation is still charged.
    /// Remove any unselected candidate at the recorded previous path and
    /// abandoned copy stages, then reconcile.
    Compaction,
    /// A previous generation may still be named or staged for removal. Its
    /// measured logical bytes are charged until it is gone.
    Retirement,
}

/// Logical usage under a managed store's persistent disk budget.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DiskBudgetStatus {
    /// Persistent inclusive limit on charged bytes.
    pub limit: u64,
    /// Total charged bytes; admission requires this plus a request to fit.
    pub charged: u64,
    /// Store directory files, an external selected checkpoint and one spare
    /// budget-record length. The live log is included at its admitted length.
    pub live_bytes: u64,
    /// Reservation for an online compaction replacement.
    pub maintenance_reserved: u64,
    /// Measured bytes of a previous generation awaiting removal.
    pub retired_bytes: u64,
    /// Bytes of removed generations still held open by owned snapshots.
    pub pinned_bytes: u64,
    /// In-memory reservation of an automatic checkpoint being prepared.
    pub automatic_reserved: u64,
    /// Outstanding maintenance recorded on disk.
    pub pending: DiskBudgetPending,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Pending {
    Idle,
    Compacting { previous: PathBuf, reserved: u64 },
    Retiring { previous: PathBuf },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) limit: u64,
    pub(crate) pending: Pending,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// The refusal of an admission that would exceed the disk budget.
#[derive(Debug)]
pub(crate) struct BudgetExceeded;

impl std::fmt::Display for BudgetExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("managed store disk budget would be exceeded")
    }
}

impl std::error::Error for BudgetExceeded {}

pub(crate) fn exceeded() -> io::Error {
    io::Error::new(io::ErrorKind::StorageFull, BudgetExceeded)
}

impl Record {
    fn encode(&self) -> io::Result<[u8; RECORD_BYTES]> {
        let mut out = [0_u8; RECORD_BYTES];
        out[..8].copy_from_slice(MAGIC);
        out[8..16].copy_from_slice(&self.limit.to_le_bytes());
        let (kind, reserved, path) = match &self.pending {
            Pending::Idle => (0_u8, 0_u64, None),
            Pending::Compacting { previous, reserved } => (1, *reserved, Some(previous)),
            Pending::Retiring { previous } => (2, 0, Some(previous)),
        };
        out[16] = kind;
        out[17..25].copy_from_slice(&reserved.to_le_bytes());
        let path = path.map_or(&[][..], |path| path.as_os_str().as_bytes());
        if path.len() > DIGEST_OFFSET - PATH_OFFSET {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "previous-store path is too long for the disk budget record",
            ));
        }
        out[25..27].copy_from_slice(&(path.len() as u16).to_le_bytes());
        out[PATH_OFFSET..PATH_OFFSET + path.len()].copy_from_slice(path);
        let digest = Sha256::digest(&out[..DIGEST_OFFSET]);
        out[DIGEST_OFFSET..].copy_from_slice(&digest);
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != RECORD_BYTES
            || &bytes[..8] != MAGIC
            || Sha256::digest(&bytes[..DIGEST_OFFSET]).as_slice() != &bytes[DIGEST_OFFSET..]
        {
            return Err(invalid("disk budget record is damaged or unsupported"));
        }
        let limit = u64::from_le_bytes(bytes[8..16].try_into().expect("eight bytes"));
        let reserved = u64::from_le_bytes(bytes[17..25].try_into().expect("eight bytes"));
        let length = usize::from(u16::from_le_bytes([bytes[25], bytes[26]]));
        if length > DIGEST_OFFSET - PATH_OFFSET
            || bytes[PATH_OFFSET + length..DIGEST_OFFSET]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(invalid("disk budget record has an invalid path"));
        }
        let path = &bytes[PATH_OFFSET..PATH_OFFSET + length];
        let previous = || {
            let path = PathBuf::from(std::ffi::OsStr::from_bytes(path));
            if path.is_absolute() && !path.as_os_str().as_bytes().contains(&0) {
                Ok(path)
            } else {
                Err(invalid("disk budget record has an invalid path"))
            }
        };
        let pending = match bytes[16] {
            0 if length == 0 && reserved == 0 => Pending::Idle,
            1 => Pending::Compacting {
                previous: previous()?,
                reserved,
            },
            2 if reserved == 0 => Pending::Retiring {
                previous: previous()?,
            },
            _ => return Err(invalid("disk budget record has an invalid state")),
        };
        Ok(Self { limit, pending })
    }
}

/// Loads a store's persistent budget record, if one exists.
pub(crate) fn read(store: &Path) -> io::Result<Option<Record>> {
    let path = store.join(BUDGET_FILE);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
        Ok(metadata) if !metadata.is_file() || metadata.len() != RECORD_BYTES as u64 => {
            return Err(invalid("disk budget record is damaged or unsupported"));
        }
        Ok(_) => {}
    }
    let mut bytes = Vec::with_capacity(RECORD_BYTES);
    OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(&path)?
        .take(RECORD_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    Record::decode(&bytes).map(Some)
}

/// A record replacement that did not complete.
pub(crate) enum WriteFailure {
    /// Failed before the rename: the previous record is still the visible one.
    NotPublished(io::Error),
    /// Failed at or after the rename: either record may be visible and
    /// durable, so in-memory accounting can no longer be trusted.
    Uncertain(io::Error),
}

impl WriteFailure {
    pub(crate) fn into_io(self) -> io::Error {
        match self {
            Self::NotPublished(error) => error,
            Self::Uncertain(error) => io::Error::new(
                error.kind(),
                format!("disk budget record publication is uncertain; reopen the store: {error}"),
            ),
        }
    }
}

#[cfg(test)]
thread_local! {
    // Injects a directory sync failure after the record rename.
    pub(crate) static FAIL_RECORD_DIRECTORY_SYNC: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Durably replaces a store's budget record. The temporary copy has the
/// same fixed length and is charged as live spare space.
pub(crate) fn write(store: &Path, record: &Record) -> io::Result<()> {
    write_tracked(store, record).map_err(WriteFailure::into_io)
}

/// Like [`write`], distinguishing a failure before the rename from one at or
/// after it, when the new record may already be visible.
pub(crate) fn write_tracked(store: &Path, record: &Record) -> Result<(), WriteFailure> {
    let not_published = WriteFailure::NotPublished;
    let bytes = record.encode().map_err(not_published)?;
    let temp = store.join(BUDGET_TEMP);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(&temp)
        .map_err(not_published)?;
    file.write_all(&bytes).map_err(not_published)?;
    file.sync_all().map_err(not_published)?;
    drop(file);
    let uncertain = WriteFailure::Uncertain;
    fs::rename(&temp, store.join(BUDGET_FILE)).map_err(uncertain)?;
    #[cfg(test)]
    if FAIL_RECORD_DIRECTORY_SYNC.get() {
        return Err(uncertain(io::Error::other(
            "injected record directory sync failure",
        )));
    }
    File::open(store)
        .and_then(|directory| directory.sync_all())
        .map_err(uncertain)
}

/// Refuses an operation that would write a budgeted store without enforcing
/// its budget. Call it while holding the store's exclusive lock, so a budget
/// cannot be installed between this check and the write.
pub(crate) fn reject_unbudgeted_writer(store: &Path) -> io::Result<()> {
    for name in [BUDGET_FILE, BUDGET_TEMP] {
        match fs::symlink_metadata(store.join(name)) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "store has a managed disk budget; open it as a checkpoint store",
                ));
            }
        }
    }
    Ok(())
}

fn remove(store: &Path) -> Result<(), crate::PublicationFailure> {
    use crate::PublicationFailure::{NotPublished, OutcomeUnknown};
    let remove = |name| match fs::remove_file(store.join(name)) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    };
    remove(BUDGET_FILE).map_err(NotPublished)?;
    #[cfg(test)]
    if FAIL_RECORD_DIRECTORY_SYNC.get() {
        return Err(OutcomeUnknown(io::Error::other(
            "injected record directory sync failure",
        )));
    }
    remove(BUDGET_TEMP)
        .and_then(|()| File::open(store)?.sync_all())
        .map_err(OutcomeUnknown)
}

/// Owned by exactly one store generation as its last-declared field.
pub(crate) struct GenerationToken;

pub(crate) fn generation_token() -> Arc<GenerationToken> {
    Arc::new(GenerationToken)
}

struct Accounting {
    limit: u64,
    // Logical bytes of live files other than the log.
    live_other: u64,
    // Highest admitted live log length, including an in-flight append.
    live_log: u64,
    maintenance: u64,
    // A generation's token is its store's last field, so its release is
    // ordered after the log, checkpoint and lock descriptors are dropped.
    pinned: Vec<(Weak<GenerationToken>, u64)>,
    // External checkpoints deselected by an automatic rollover, charged
    // while a reader of the old view keeps their index alive.
    deselected: Vec<(crate::checkpoint_view::CheckpointPin, u64)>,
    // Reservation held by an automatic rollover under the writer gate.
    automatic: u64,
    // The generation being retired in this process. While it is also named
    // or staged it is charged once, at the larger of the two measures.
    retiring: Option<(Weak<GenerationToken>, u64)>,
    pending: Pending,
    live_id: Option<[u8; 16]>,
    // Set when a record publication became uncertain. The accounting may not
    // match the visible record, so every admission fails until reopen.
    uncertain: bool,
}

/// Shared by every generation of an online store, so pinned retired
/// generations and maintenance reservations stay charged across handoffs.
pub(crate) struct DiskBudget {
    store: PathBuf,
    parent: PathBuf,
    state: Mutex<Accounting>,
}

/// An automatic rollover's in-memory share of the budget. It is released
/// when dropped, including on every failure path.
pub(crate) struct AutomaticReservation {
    budget: Option<Arc<DiskBudget>>,
    bytes: u64,
    growth: u64,
}

impl Drop for AutomaticReservation {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.take() {
            let mut state = budget
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.automatic = 0;
        }
    }
}

fn poisoned() -> io::Error {
    io::Error::other("disk budget accounting poisoned; reopen the store")
}

fn unreliable() -> io::Error {
    io::Error::other(
        "disk budget record publication is uncertain; reopen the store to load the visible record",
    )
}

// Locks the accounting, refusing every admission after an uncertain
// record publication.
fn lock_reliable(state: &Mutex<Accounting>) -> io::Result<std::sync::MutexGuard<'_, Accounting>> {
    let state = state.lock().map_err(|_| poisoned())?;
    if state.uncertain {
        return Err(unreliable());
    }
    Ok(state)
}

/// Live bytes other than the log: every file in the store directory except
/// `records.log`, an external selected checkpoint and a spare record length.
pub(crate) fn live_other_bytes(
    store: &Path,
    log_length: u64,
    selected: Option<&Path>,
) -> io::Result<u64> {
    let tree = crate::staging::tree_bytes(store)?;
    let external = match selected {
        Some(checkpoint) if !checkpoint.starts_with(store) => {
            crate::staging::tree_bytes(checkpoint)?
        }
        _ => 0,
    };
    tree.checked_sub(log_length)
        .and_then(|other| other.checked_add(external))
        .and_then(|other| other.checked_add(RECORD_BYTES as u64))
        .ok_or_else(|| invalid("store directory is smaller than its log"))
}

impl DiskBudget {
    pub(crate) fn new(
        store: &Path,
        record: Record,
        log_length: u64,
        log_id: Option<[u8; 16]>,
        selected: Option<&Path>,
    ) -> io::Result<Self> {
        let store = fs::canonicalize(store)?;
        let parent = store
            .parent()
            .ok_or_else(|| io::Error::other("store has no parent directory"))?
            .to_path_buf();
        let maintenance = match &record.pending {
            Pending::Compacting { reserved, .. } => *reserved,
            _ => 0,
        };
        Ok(Self {
            state: Mutex::new(Accounting {
                limit: record.limit,
                live_other: live_other_bytes(&store, log_length, selected)?,
                live_log: log_length,
                maintenance,
                pinned: Vec::new(),
                deselected: Vec::new(),
                automatic: 0,
                retiring: None,
                uncertain: false,
                pending: record.pending,
                live_id: log_id,
            }),
            store,
            parent,
        })
    }

    fn retired_locked(&self, state: &Accounting) -> io::Result<u64> {
        let held = state
            .retiring
            .as_ref()
            .filter(|(token, _)| token.strong_count() != 0)
            .map_or(0, |(_, bytes)| *bytes);
        Ok(self.named_retired_locked(state)?.max(held))
    }

    // Logical bytes of a previous generation still named or staged.
    fn named_retired_locked(&self, state: &Accounting) -> io::Result<u64> {
        let Pending::Retiring { previous } = &state.pending else {
            return Ok(0);
        };
        let staged = match &state.live_id {
            Some(id) => crate::staging::retired_stage_bytes(&self.parent, id)?,
            None => 0,
        };
        crate::staging::tree_bytes(previous)?
            .checked_add(staged)
            .ok_or_else(|| io::Error::other("accounted storage size overflow"))
    }

    fn pinned_locked(state: &mut Accounting) -> io::Result<u64> {
        state.pinned.retain(|(token, _)| token.strong_count() != 0);
        state.deselected.retain(|(pin, _)| pin.is_live());
        state
            .pinned
            .iter()
            .map(|(_, bytes)| *bytes)
            .chain(state.deselected.iter().map(|(_, bytes)| *bytes))
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| io::Error::other("pinned generation byte count overflow"))
    }

    fn status_locked(&self, state: &mut Accounting) -> io::Result<DiskBudgetStatus> {
        let retired_bytes = self.retired_locked(state)?;
        let pinned_bytes = Self::pinned_locked(state)?;
        let live_bytes = state
            .live_other
            .checked_add(state.live_log)
            .ok_or_else(|| io::Error::other("disk budget charge overflow"))?;
        let charged = [
            state.maintenance,
            retired_bytes,
            pinned_bytes,
            state.automatic,
        ]
        .into_iter()
        .try_fold(live_bytes, u64::checked_add)
        .ok_or_else(|| io::Error::other("disk budget charge overflow"))?;
        Ok(DiskBudgetStatus {
            limit: state.limit,
            charged,
            live_bytes,
            maintenance_reserved: state.maintenance,
            retired_bytes,
            pinned_bytes,
            automatic_reserved: state.automatic,
            pending: match state.pending {
                Pending::Idle => DiskBudgetPending::None,
                Pending::Compacting { .. } => DiskBudgetPending::Compaction,
                Pending::Retiring { .. } => DiskBudgetPending::Retirement,
            },
        })
    }

    pub(crate) fn status(&self) -> io::Result<DiskBudgetStatus> {
        // After an uncertain publication no charge or limit is authoritative.
        let mut state = lock_reliable(&self.state)?;
        self.status_locked(&mut state)
    }

    /// Admits a live log growing to `next_log` bytes. The caller holds the
    /// store's writer gate, so at most one append is in flight. The admitted
    /// length stays charged even if the append then fails.
    pub(crate) fn admit_log(&self, next_log: u64) -> io::Result<()> {
        let mut state = lock_reliable(&self.state)?;
        self.admit_log_locked(&mut state, next_log)
    }

    /// Reports whether `next_log` would be admitted now, without charging
    /// it, so a predictably refused append is rejected before it is encoded.
    pub(crate) fn check_log(&self, next_log: u64) -> io::Result<()> {
        let mut state = lock_reliable(&self.state)?;
        let charged = self.status_locked(&mut state)?.charged;
        if (charged - state.live_log)
            .checked_add(next_log.max(state.live_log))
            .is_none_or(|total| total > state.limit)
        {
            return Err(exceeded());
        }
        Ok(())
    }

    fn admit_log_locked(&self, state: &mut Accounting, next_log: u64) -> io::Result<()> {
        let status = self.status_locked(state)?;
        // `charged` includes `live_log`, so this cannot underflow.
        let others = status.charged - state.live_log;
        if others
            .checked_add(next_log)
            .is_none_or(|total| total > state.limit)
        {
            return Err(exceeded());
        }
        state.live_log = state.live_log.max(next_log);
        Ok(())
    }

    /// Reserves an automatic rollover's peak before it writes anything:
    /// `bytes` of new files, less `credit` bytes of measured files it removes
    /// first, plus growth of the live log to `next_log` for the append that
    /// follows. The caller holds the writer gate, so no other automatic
    /// rollover or append runs, and online maintenance admits against the
    /// same charge. Dropping the reservation releases it.
    pub(crate) fn reserve_automatic(
        self: &Arc<Self>,
        bytes: u64,
        credit: u64,
        next_log: Option<u64>,
    ) -> io::Result<AutomaticReservation> {
        let mut state = lock_reliable(&self.state)?;
        if state.automatic != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "another automatic checkpoint reservation is outstanding",
            ));
        }
        let charged = self.status_locked(&mut state)?.charged;
        let growth = next_log.map_or(0, |next| next.saturating_sub(state.live_log));
        let peak = charged
            .checked_sub(credit)
            .ok_or_else(|| io::Error::other("automatic checkpoint credit exceeds the charge"))?
            .checked_add(bytes)
            .and_then(|total| total.checked_add(growth));
        if peak.is_none_or(|total| total > state.limit) {
            return Err(exceeded());
        }
        state.automatic = bytes.saturating_sub(credit) + growth;
        Ok(AutomaticReservation {
            budget: Some(Arc::clone(self)),
            bytes,
            growth,
        })
    }

    /// Re-measures live files after an automatic rollover's removal or
    /// publication. With `removed`, the reservation becomes its full byte
    /// count plus log growth; otherwise only log growth stays reserved. A
    /// measurement failure marks the accounting uncertain, refusing every
    /// later admission until reopen.
    pub(crate) fn refresh_automatic(
        &self,
        reservation: &AutomaticReservation,
        log_length: u64,
        selected: Option<&Path>,
        published: bool,
        deselected: Option<(crate::checkpoint_view::CheckpointPin, u64)>,
    ) -> io::Result<()> {
        let mut state = self.state.lock().map_err(|_| poisoned())?;
        match live_other_bytes(&self.store, log_length, selected) {
            Ok(live_other) => {
                state.live_other = live_other;
                state.automatic = if published {
                    reservation.growth
                } else {
                    reservation.bytes + reservation.growth
                };
                state.deselected.extend(deselected);
                Ok(())
            }
            Err(error) => {
                state.uncertain = true;
                Err(io::Error::new(
                    error.kind(),
                    format!("disk budget accounting could not be refreshed: {error}"),
                ))
            }
        }
    }

    /// Admits the append that follows an automatic rollover, converting
    /// its reserved log growth in the same critical section.
    pub(crate) fn admit_log_reserved(
        &self,
        next_log: u64,
        mut reservation: AutomaticReservation,
    ) -> io::Result<()> {
        let mut state = lock_reliable(&self.state)?;
        // Disarm the guard; its release happens here under the same lock.
        let _budget = reservation.budget.take();
        state.automatic = 0;
        self.admit_log_locked(&mut state, next_log)
    }

    /// Reserves `bytes` for a replacement at `previous` and records the
    /// reservation durably before any of it is allocated.
    pub(crate) fn reserve_compaction(&self, previous: &Path, bytes: u64) -> io::Result<()> {
        let mut state = lock_reliable(&self.state)?;
        if state.pending != Pending::Idle {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "reconcile outstanding disk-budget maintenance first",
            ));
        }
        let status = self.status_locked(&mut state)?;
        if status
            .charged
            .checked_add(bytes)
            .is_none_or(|total| total > state.limit)
        {
            return Err(exceeded());
        }
        let pending = Pending::Compacting {
            previous: previous.to_path_buf(),
            reserved: bytes,
        };
        let record = Record {
            limit: state.limit,
            pending: pending.clone(),
        };
        match write_tracked(&self.store, &record) {
            Ok(()) => {}
            Err(WriteFailure::NotPublished(error)) => return Err(error),
            Err(failure) => {
                // A visible reservation would not match idle accounting.
                state.uncertain = true;
                return Err(failure.into_io());
            }
        }
        state.maintenance = bytes;
        state.pending = pending;
        Ok(())
    }

    /// The record a replacement carries before exchange: after the exchange
    /// the previous generation is named at `previous` until it is removed.
    pub(crate) fn replacement_record(&self, previous: &Path) -> io::Result<Record> {
        let state = self.state.lock().map_err(|_| poisoned())?;
        Ok(Record {
            limit: state.limit,
            pending: Pending::Retiring {
                previous: previous.to_path_buf(),
            },
        })
    }

    /// Moves the reservation to the installed replacement. The previous
    /// generation stays charged while named, staged or held open.
    pub(crate) fn complete_exchange(
        &self,
        previous: &Path,
        replacement: (u64, u64, Option<[u8; 16]>),
        retired: (Weak<GenerationToken>, u64),
    ) -> io::Result<()> {
        let (live_other, live_log, live_id) = replacement;
        let mut state = self.state.lock().map_err(|_| poisoned())?;
        state.maintenance = 0;
        state.live_other = live_other;
        state.live_log = live_log;
        state.live_id = live_id;
        state.retiring = Some(retired);
        state.pending = Pending::Retiring {
            previous: previous.to_path_buf(),
        };
        Ok(())
    }

    /// Clears outstanding maintenance once nothing it could have allocated
    /// remains. Returns whether the record is now idle. If writing the idle
    /// record fails, the in-memory charge is kept, which is never less than
    /// either visible record charges.
    pub(crate) fn reconcile(&self) -> io::Result<bool> {
        let mut state = lock_reliable(&self.state)?;
        let clean = match &state.pending {
            Pending::Idle => return Ok(true),
            // Copy stages record no owner, so any one keeps the charge.
            Pending::Compacting { previous, .. } => {
                fs::symlink_metadata(previous)
                    .is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
                    && !crate::staging::has_unowned_stage(&self.parent)?
            }
            Pending::Retiring { .. } => self.named_retired_locked(&state)? == 0,
        };
        if !clean {
            return Ok(false);
        }
        write(
            &self.store,
            &Record {
                limit: state.limit,
                pending: Pending::Idle,
            },
        )?;
        state.maintenance = 0;
        state.pending = Pending::Idle;
        // Names are gone; an owned snapshot may still hold the generation.
        if let Some(retired) = state.retiring.take() {
            state.pinned.push(retired);
        }
        Ok(true)
    }

    /// Changes the persistent limit. A limit below the charged total returns
    /// `WouldBlock` and leaves the record unchanged.
    pub(crate) fn set_limit(&self, limit: u64) -> Result<(), crate::PublicationFailure> {
        use crate::PublicationFailure::{NotPublished, OutcomeUnknown};
        let mut state = lock_reliable(&self.state).map_err(NotPublished)?;
        if self
            .status_locked(&mut state)
            .map_err(NotPublished)?
            .charged
            > limit
        {
            return Err(NotPublished(io::Error::new(
                io::ErrorKind::WouldBlock,
                "disk budget limit is below the charged bytes",
            )));
        }
        let record = Record {
            limit,
            pending: state.pending.clone(),
        };
        match write_tracked(&self.store, &record) {
            Ok(()) => {
                state.limit = limit;
                Ok(())
            }
            Err(WriteFailure::NotPublished(error)) => Err(NotPublished(error)),
            Err(failure) => {
                // Either limit may be the durable one; admit nothing more.
                state.uncertain = true;
                Err(OutcomeUnknown(failure.into_io()))
            }
        }
    }

    /// Removes the record when nothing is outstanding or pinned. If removal
    /// fails, the budget stays attached and enforced, which is never less
    /// restrictive than a removed record.
    ///
    /// A failure before the record is unlinked is `NotPublished`. Once it is
    /// unlinked, a later failure is `OutcomeUnknown`: a reopen may or may not
    /// find the record, while this handle keeps enforcing it.
    pub(crate) fn remove(&self) -> Result<(), crate::PublicationFailure> {
        use crate::PublicationFailure::NotPublished;
        let mut state = lock_reliable(&self.state).map_err(NotPublished)?;
        if state.pending != Pending::Idle
            || Self::pinned_locked(&mut state).map_err(NotPublished)? != 0
        {
            return Err(NotPublished(io::Error::new(
                io::ErrorKind::WouldBlock,
                "disk budget still charges maintenance or pinned generations",
            )));
        }
        remove(&self.store)
    }
}

impl super::CheckpointStore {
    pub(crate) fn disk_budget(&self) -> io::Result<Option<Arc<DiskBudget>>> {
        Ok(self
            .disk_budget
            .read()
            .map_err(|_| poisoned())?
            .as_ref()
            .map(Arc::clone))
    }

    /// Refuses operations that would allocate outside a disk budget's
    /// accounting, such as checkpoints that change the selection.
    pub(crate) fn reject_under_disk_budget(&self, operation: &str) -> io::Result<()> {
        if self.disk_budget()?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("{operation} is not supported for a disk-budgeted store"),
            ));
        }
        Ok(())
    }

    pub(crate) fn generation_token(&self) -> Weak<GenerationToken> {
        Arc::downgrade(&self.generation_token)
    }

    /// Reports logical usage under this store's persistent disk budget, or
    /// `None` when the store has no budget.
    pub fn disk_budget_status(&self) -> io::Result<Option<DiskBudgetStatus>> {
        self.disk_budget()?
            .map(|budget| budget.status())
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_reject_damage() {
        let records = [
            Record {
                limit: 7,
                pending: Pending::Idle,
            },
            Record {
                limit: u64::MAX,
                pending: Pending::Compacting {
                    previous: PathBuf::from("/tmp/previous"),
                    reserved: 42,
                },
            },
            Record {
                limit: 1,
                pending: Pending::Retiring {
                    previous: PathBuf::from("/a/b"),
                },
            },
        ];
        for record in records {
            let mut bytes = record.encode().unwrap();
            assert_eq!(Record::decode(&bytes).unwrap(), record);
            bytes[9] ^= 1;
            assert!(Record::decode(&bytes).is_err());
        }
        let relative = Record {
            limit: 1,
            pending: Pending::Retiring {
                previous: PathBuf::from("relative"),
            },
        };
        assert!(Record::decode(&relative.encode().unwrap()).is_err());
    }
}
