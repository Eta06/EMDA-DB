//! Bounded in-process read-only index snapshots over immutable log locations.
use super::*;
use std::sync::{Arc, TryLockError, Weak};
use std::time::Instant;

/// Hard per-snapshot limit on the number of keys represented by a view.
pub const MAX_SNAPSHOT_KEYS: usize = 16_384;
/// Hard per-snapshot limit on the logical bytes in keys represented by a view.
pub const MAX_SNAPSHOT_KEY_BYTES: usize = 4 * 1024 * 1024;
/// Maximum simultaneously active snapshots per open store.
pub const MAX_ACTIVE_SNAPSHOTS: usize = 16;
/// Maximum combined logical keys across snapshots on one open store.
pub const MAX_SNAPSHOT_TOTAL_KEYS: usize = 65_536;
/// Maximum combined logical key bytes across snapshots on one open store.
pub const MAX_SNAPSHOT_TOTAL_KEY_BYTES: usize = 16 * 1024 * 1024;

/// Per-open logical limits and reservations for ordinary-store snapshots.
/// Transactions and optimized standalone document reads also reserve an active
/// slot. These counters are not total memory or cross-store quotas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotBudgetStatus {
    /// Maximum concurrent reservations.
    pub active_limit: usize,
    /// Current reservations, including transaction snapshots and point reads.
    pub active: usize,
    /// Maximum combined logical keys represented by reservations.
    pub key_limit: usize,
    /// Combined logical keys currently reserved.
    pub keys: usize,
    /// Maximum combined logical key bytes represented by reservations.
    pub key_byte_limit: usize,
    /// Combined logical key bytes currently reserved.
    pub key_bytes: usize,
}

pub(super) struct SnapshotBudget {
    active: usize,
    keys: usize,
    key_bytes: usize,
    active_limit: usize,
    key_limit: usize,
    key_byte_limit: usize,
}

pub(super) enum SnapshotStorage<T> {
    Local(Mutex<T>),
    Shared(Arc<Mutex<T>>),
}

impl<T: Default> Default for SnapshotStorage<T> {
    fn default() -> Self {
        Self::Local(Mutex::default())
    }
}

impl<T> std::ops::Deref for SnapshotStorage<T> {
    type Target = Mutex<T>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Local(budget) => budget,
            Self::Shared(budget) => budget,
        }
    }
}

impl<T: Default> SnapshotStorage<T> {
    pub(super) fn make_shared(&mut self) {
        *self = match std::mem::take(self) {
            Self::Local(budget) => Self::Shared(Arc::new(budget)),
            shared => shared,
        };
    }

    pub(super) fn clone_shared(&self) -> Self {
        let Self::Shared(budget) = self else {
            unreachable!("an online store always shares its snapshot resources");
        };
        Self::Shared(Arc::clone(budget))
    }
}

impl Default for SnapshotBudget {
    fn default() -> Self {
        Self {
            active: 0,
            keys: 0,
            key_bytes: 0,
            active_limit: MAX_ACTIVE_SNAPSHOTS,
            key_limit: MAX_SNAPSHOT_TOTAL_KEYS,
            key_byte_limit: MAX_SNAPSHOT_TOTAL_KEY_BYTES,
        }
    }
}

impl SnapshotBudget {
    fn status(&self) -> SnapshotBudgetStatus {
        SnapshotBudgetStatus {
            active_limit: self.active_limit,
            active: self.active,
            key_limit: self.key_limit,
            keys: self.keys,
            key_byte_limit: self.key_byte_limit,
            key_bytes: self.key_bytes,
        }
    }

    fn set_limits(&mut self, active: usize, keys: usize, key_bytes: usize) -> io::Result<()> {
        if active > MAX_ACTIVE_SNAPSHOTS
            || keys > MAX_SNAPSHOT_TOTAL_KEYS
            || key_bytes > MAX_SNAPSHOT_TOTAL_KEY_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot budget exceeds hard maximum",
            ));
        }
        if active < self.active || keys < self.keys || key_bytes < self.key_bytes {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "snapshot budget is below current reservations",
            ));
        }
        self.active_limit = active;
        self.key_limit = keys;
        self.key_byte_limit = key_bytes;
        Ok(())
    }

    pub(super) fn reserve(&mut self, keys: usize, key_bytes: usize) -> io::Result<()> {
        if self.active >= self.active_limit
            || keys > self.key_limit - self.keys
            || key_bytes > self.key_byte_limit - self.key_bytes
        {
            return Err(io::Error::other("aggregate snapshot limit exceeded"));
        }
        self.active += 1;
        self.keys += keys;
        self.key_bytes += key_bytes;
        Ok(())
    }

    pub(super) fn release(&mut self, keys: usize, key_bytes: usize) {
        self.active -= 1;
        self.keys -= keys;
        self.key_bytes -= key_bytes;
    }
}

/// A stable read-only index view over a captured store state.
///
/// Drop it to release its retained index view. Snapshots are process-local, do not
/// survive reopening, and do not make independent writes transactional.
/// Per-store snapshot count, total keys and key bytes are limited. These are
/// not byte-accurate process memory limits and do not apply across stores.
/// Expired snapshots can be reclaimed on later snapshot admission, an explicit
/// reaping call or an optionally started worker. Expiry is checked at
/// operation boundaries, not during I/O.
pub struct Snapshot<'a> {
    store: SnapshotStore<'a>,
    pub(super) generation: u64,
    pub(super) log_generation: Option<GenerationId>,
    state: Arc<RwLock<SnapshotData>>,
}

enum SnapshotStore<'a> {
    Borrowed(&'a Store),
    Owned(crate::direct_read::DirectReads),
}

impl<'a> SnapshotStore<'a> {
    fn borrowed(&self) -> Option<&'a Store> {
        match self {
            Self::Borrowed(store) => Some(store),
            Self::Owned(_) => None,
        }
    }

    fn direct_reads(&self) -> &crate::direct_read::DirectReads {
        match self {
            Self::Borrowed(store) => &store.direct_reads,
            Self::Owned(reads) => reads,
        }
    }
}

pub(super) type SnapshotRegistry = Vec<Weak<RwLock<SnapshotData>>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SnapshotStatus {
    Active,
    Expired,
    Cancelled,
}

pub(super) struct SnapshotData {
    pub(super) log_generation: Option<GenerationId>,
    pub(super) log: Option<Arc<Log>>,
    pub(super) index: Arc<BTreeMap<Vec<u8>, ValueLocation>>,
    owned_store: Option<Arc<Store>>,
    reserved_keys: usize,
    reserved_key_bytes: usize,
    status: SnapshotStatus,
    deadline: Option<Instant>,
}

impl SnapshotData {
    fn store<'a>(&'a self, borrowed: Option<&'a Store>) -> &'a Store {
        self.owned_store
            .as_deref()
            .or(borrowed)
            .expect("active snapshot has a store")
    }

    fn release(&mut self, borrowed: Option<&Store>, status: SnapshotStatus) {
        if self.status == SnapshotStatus::Active {
            // Release this view before capacity becomes available again.
            self.index = Arc::new(BTreeMap::new());
            self.log = None;
            let owned_budget = self
                .owned_store
                .as_ref()
                .map(|store| store.snapshot_budget.clone_shared());
            self.owned_store = None;
            owned_budget
                .as_ref()
                .or_else(|| borrowed.map(|store| &store.snapshot_budget))
                .expect("active snapshot has a budget")
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(self.reserved_keys, self.reserved_key_bytes);
        }
        self.status = status;
    }
}

struct SnapshotReservation<'a> {
    store: &'a Store,
    keys: usize,
    key_bytes: usize,
    active: bool,
}

impl Drop for SnapshotReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.store
                .snapshot_budget
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .release(self.keys, self.key_bytes);
        }
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl Store {
    /// Reports per-open logical snapshot reservation limits and usage.
    /// Expired views may remain reserved until reaped, cancelled or dropped.
    /// An ordinary transaction releases its private snapshot after successful
    /// validation, even if its write is still synchronizing.
    pub fn snapshot_budget_status(&self) -> io::Result<SnapshotBudgetStatus> {
        Ok(self
            .snapshot_budget
            .lock()
            .map_err(|_| io::Error::other("snapshot budget poisoned"))?
            .status())
    }

    /// Sets aggregate active, key-count and key-byte reservation limits.
    /// Reductions below existing reservations return `WouldBlock` unchanged.
    /// Transactions, standalone document point reads and online-compaction
    /// snapshots also use these slots. Zero can reject those operations. The
    /// limits reset on reopen and do not bound total memory or project usage.
    pub fn set_snapshot_budget_limits(
        &self,
        active: usize,
        keys: usize,
        key_bytes: usize,
    ) -> io::Result<()> {
        self.snapshot_budget
            .lock()
            .map_err(|_| io::Error::other("snapshot budget poisoned"))?
            .set_limits(active, keys, key_bytes)
    }

    /// Captures a stable index view, rejecting a store that exceeds either limit.
    /// Limits cannot exceed the public hard per-snapshot limits. The operation
    /// retains an immutable index view, not values, and never returns a partial
    /// snapshot. Later writes may copy the index while a view is active.
    pub fn snapshot(&self, max_keys: usize, max_key_bytes: usize) -> io::Result<Snapshot<'_>> {
        self.snapshot_with_deadline(max_keys, max_key_bytes, None)
    }

    fn snapshot_with_deadline(
        &self,
        max_keys: usize,
        max_key_bytes: usize,
        deadline: Option<Instant>,
    ) -> io::Result<Snapshot<'_>> {
        Self::capture_snapshot(Some(self), None, max_keys, max_key_bytes, deadline)
    }

    pub(super) fn owned_snapshot(
        store: Arc<Self>,
        max_keys: usize,
        max_key_bytes: usize,
        deadline: Option<Instant>,
    ) -> io::Result<Snapshot<'static>> {
        Self::capture_snapshot(None, Some(store), max_keys, max_key_bytes, deadline)
    }

    fn capture_snapshot(
        borrowed: Option<&Store>,
        owned: Option<Arc<Self>>,
        max_keys: usize,
        max_key_bytes: usize,
        deadline: Option<Instant>,
    ) -> io::Result<Snapshot<'_>> {
        let store = borrowed
            .or(owned.as_deref())
            .expect("snapshot capture has a store");
        if max_keys > MAX_SNAPSHOT_KEYS || max_key_bytes > MAX_SNAPSHOT_KEY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot limits exceed supported maximum",
            ));
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "snapshot expired"));
        }
        store.reap_expired_snapshots()?;
        let inner = store.read()?;
        if store.commits.failed() {
            return Err(uncertain_error());
        }
        if inner.index.len() > max_keys {
            return Err(io::Error::other("snapshot key count limit exceeded"));
        }
        let key_bytes = inner.index_key_bytes;
        if key_bytes > max_key_bytes {
            return Err(io::Error::other("snapshot key byte limit exceeded"));
        }
        let key_count = inner.index.len();
        store
            .snapshot_budget
            .lock()
            .map_err(|_| io::Error::other("snapshot budget poisoned"))?
            .reserve(key_count, key_bytes)?;
        // Protect the reservation even if allocating the shared state unwinds.
        let mut reservation = SnapshotReservation {
            store,
            keys: key_count,
            key_bytes,
            active: true,
        };
        let snapshot = Snapshot {
            store: match borrowed {
                Some(store) => SnapshotStore::Borrowed(store),
                None => SnapshotStore::Owned(store.direct_reads.clone()),
            },
            generation: inner.generation,
            log_generation: inner.log_generation,
            state: Arc::new(RwLock::new(SnapshotData {
                log_generation: inner.log_generation,
                log: Some(Arc::clone(
                    inner.log.as_ref().expect("published log missing"),
                )),
                index: Arc::clone(&inner.index),
                owned_store: owned.clone(),
                reserved_keys: key_count,
                reserved_key_bytes: key_bytes,
                status: SnapshotStatus::Active,
                deadline,
            })),
        };
        reservation.active = false;
        snapshot.ensure_healthy()?;
        if deadline.is_some() {
            let mut registry = store
                .snapshot_registry
                .lock()
                .map_err(|_| io::Error::other("snapshot registry poisoned"))?;
            // Many callers can pass the earlier reap concurrently; prune again
            // under the registry lock and cap even stale or busy entries.
            registry.retain(|weak| {
                let Some(state) = weak.upgrade() else {
                    return false;
                };
                match state.try_read() {
                    Ok(data) => data.status == SnapshotStatus::Active,
                    Err(TryLockError::WouldBlock) => true,
                    Err(TryLockError::Poisoned(error)) => {
                        error.into_inner().status == SnapshotStatus::Active
                    }
                }
            });
            if registry.len() >= MAX_ACTIVE_SNAPSHOTS {
                return Err(io::Error::other(
                    "snapshot deadline registry temporarily full",
                ));
            }
            registry.push(Arc::downgrade(&snapshot.state));
        }
        Ok(snapshot)
    }

    /// Captures a snapshot that rejects new operations at or after `deadline`.
    ///
    /// The deadline is process-local and monotonic. An operation started before
    /// expiry can finish afterwards; I/O and lock waits are not interrupted.
    /// Expired views are reclaimed on a later snapshot request or explicit
    /// `reap_expired_snapshots` call when no read is using them. An optional
    /// snapshot reaper can also schedule checks; ongoing reads are not interrupted.
    pub fn snapshot_until(
        &self,
        max_keys: usize,
        max_key_bytes: usize,
        deadline: Instant,
    ) -> io::Result<Snapshot<'_>> {
        self.snapshot_with_deadline(max_keys, max_key_bytes, Some(deadline))
    }

    /// Reclaims expired snapshots that have no operation currently using them.
    /// Busy snapshots are skipped; call again later. For owned online views,
    /// reaping also releases their ownership of the captured generation.
    pub fn reap_expired_snapshots(&self) -> io::Result<usize> {
        let mut registry = self
            .snapshot_registry
            .lock()
            .map_err(|_| io::Error::other("snapshot registry poisoned"))?;
        let mut reaped = 0;
        registry.retain(|weak| {
            let Some(state) = weak.upgrade() else {
                return false;
            };
            let mut data = match state.try_write() {
                Ok(data) => data,
                Err(TryLockError::WouldBlock) => return true,
                Err(TryLockError::Poisoned(error)) => error.into_inner(),
            };
            if data.status != SnapshotStatus::Active {
                return false;
            }
            if data
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                data.release(Some(self), SnapshotStatus::Expired);
                reaped += 1;
                return false;
            }
            true
        });
        Ok(reaped)
    }
}

impl Snapshot<'_> {
    pub(super) fn borrowed_store(&self) -> &Store {
        self.store
            .borrowed()
            .expect("transaction snapshots borrow their store")
    }

    pub(super) fn ensure_healthy(&self) -> io::Result<()> {
        drop(self.read_index()?);
        Ok(())
    }

    pub(super) fn read_index(&self) -> io::Result<RwLockReadGuard<'_, SnapshotData>> {
        let data = self
            .state
            .read()
            .map_err(|_| io::Error::other("snapshot state poisoned"))?;
        if data.status == SnapshotStatus::Cancelled {
            return Err(io::Error::other("snapshot has been cancelled"));
        }
        if data.status == SnapshotStatus::Expired
            || data
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "snapshot expired"));
        }
        if data.store(self.store.borrowed()).commits.failed() {
            return Err(uncertain_error());
        }
        Ok(data)
    }

    /// Cancels this snapshot and releases its index view and reservations.
    ///
    /// Cancellation is idempotent. Later reads return an error rather than an
    /// empty result. It has no effect on other snapshots or transactions.
    pub fn cancel(&mut self) {
        let mut data = self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        data.release(self.store.borrowed(), SnapshotStatus::Cancelled);
    }

    /// Whether this snapshot has been explicitly cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .status
            == SnapshotStatus::Cancelled
    }

    /// Reads the value selected when this snapshot was created.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_admitted(key, |_| Ok(()))
    }

    pub(crate) fn get_admitted(
        &self,
        key: &[u8],
        mut admit: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Option<Vec<u8>>> {
        record::validate_key(key)?;
        let _admission = self.store.direct_reads().acquire_snapshot()?;
        let data = self.read_index()?;
        data.index
            .get(key)
            .copied()
            .map(|location| {
                admit(location.length)?;
                data.log
                    .as_ref()
                    .expect("active snapshot retains its log")
                    .read(location)
            })
            .transpose()
    }

    /// Scans this snapshot in bytewise key order with exclusive bounds.
    /// Page count and byte limits match `Store::scan`; continue until empty.
    pub fn scan(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Entry>> {
        self.scan_admitted(after, before, limit, |_, _| true)
    }

    pub(crate) fn scan_admitted(
        &self,
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
        mut admit: impl FnMut(&[u8], usize) -> bool,
    ) -> io::Result<Vec<Entry>> {
        for key in [after, before].into_iter().flatten() {
            record::validate_key(key)?;
        }
        if limit == 0 || limit > MAX_SCAN_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scan limit must be between 1 and 1024",
            ));
        }
        let _admission = self.store.direct_reads().acquire_snapshot()?;
        let data = self.read_index()?;
        if matches!((after, before), (Some(a), Some(b)) if a >= b) {
            return Ok(Vec::new());
        }
        let bounds = (
            after.map_or(Unbounded, Excluded),
            before.map_or(Unbounded, Excluded),
        );
        let mut result = Vec::new();
        let mut bytes = 0_usize;
        for (key, &location) in data.index.range::<[u8], _>(bounds).take(limit) {
            let size = key.len() + location.length;
            if bytes + size > MAX_SCAN_BYTES || !admit(key, location.length) {
                break;
            }
            bytes += size;
            result.push((
                key.clone(),
                data.log
                    .as_ref()
                    .expect("active snapshot retains its log")
                    .read(location)?,
            ));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn request_reaper_check(worker: &crate::SnapshotReaper) {
        let checks = worker.status().checks;
        worker.request_check().unwrap();
        let timeout = Instant::now() + Duration::from_secs(10);
        while worker.status().checks == checks {
            assert!(Instant::now() < timeout, "snapshot reaper did not complete");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn reaping_an_owned_deadline_skips_busy_state_and_frees_capacity_once() {
        let root = tempfile::tempdir().unwrap();
        let online = Arc::new(OnlineStore::open(root.path()).unwrap());
        online
            .with_store(|store| {
                store.set_snapshot_budget_limits(1, 1, 1)?;
                store.put(b"k", b"v")
            })
            .unwrap();
        assert_eq!(
            online
                .snapshot_until(1, 1, Instant::now())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::TimedOut
        );
        let mut owned = online
            .snapshot_until(1, 1, Instant::now() + std::time::Duration::from_secs(3600))
            .unwrap();
        let snapshot = owned.as_snapshot().unwrap();
        snapshot.state.write().unwrap().deadline = Some(Instant::now());
        let busy = snapshot.state.read().unwrap();
        assert_eq!(online.reap_expired_snapshots().unwrap(), 0);
        assert!(online.snapshot(1, 1).is_err());
        drop(busy);
        let replacement = online.snapshot(1, 1).unwrap();
        assert_eq!(
            snapshot.get(b"k").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            snapshot.scan(None, None, 1).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert!(snapshot.state.read().unwrap().owned_store.is_none());
        assert_eq!(online.reap_expired_snapshots().unwrap(), 0);
        owned.cancel();
        assert!(online.snapshot(1, 1).is_err());
        drop(replacement);
        let view = online
            .snapshot_until(1, 1, Instant::now() + Duration::from_secs(3600))
            .unwrap();
        view.as_snapshot().unwrap().state.write().unwrap().deadline = Some(Instant::now());
        let worker = online
            .start_snapshot_reaper(Duration::from_secs(3600))
            .unwrap();
        request_reaper_check(&worker);
        assert_eq!(worker.stop().unwrap().reaped, 1);
        assert!(online.snapshot(1, 1).is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn online_reaper_releases_an_unlinked_generation_after_handoff() {
        let root = tempfile::tempdir().unwrap();
        let online = Arc::new(OnlineStore::open(root.path().join("live")).unwrap());
        let previous = root.path().join("previous");
        online.with_store(|store| store.put(b"k", b"old")).unwrap();
        let view = online
            .snapshot_until(1, 1, Instant::now() + Duration::from_secs(3600))
            .unwrap();
        let snapshot = view.as_snapshot().unwrap();
        let (store, log) = {
            let data = snapshot.read_index().unwrap();
            (
                Arc::downgrade(data.owned_store.as_ref().unwrap()),
                Arc::downgrade(data.log.as_ref().unwrap()),
            )
        };
        let worker = online
            .start_snapshot_reaper(Duration::from_secs(3600))
            .unwrap();
        assert_eq!(
            online
                .start_snapshot_reaper(Duration::from_secs(3600))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        online
            .compact_and_reclaim_detailed(&previous, &Default::default())
            .unwrap();
        assert!(!previous.exists());
        online.with_store(|store| store.put(b"k", b"new")).unwrap();
        assert_eq!(
            snapshot.get(b"k").unwrap().as_deref(),
            Some(b"old".as_slice())
        );
        assert!(log.upgrade().is_some());
        snapshot.state.write().unwrap().deadline = Some(Instant::now());
        request_reaper_check(&worker);
        assert_eq!(worker.stop().unwrap().reaped, 1);
        assert!(store.upgrade().is_none());
        assert!(log.upgrade().is_none());
        assert_eq!(
            snapshot.get(b"k").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            online
                .with_store(|store| store.get(b"k"))
                .unwrap()
                .as_deref(),
            Some(b"new".as_slice())
        );
    }

    #[test]
    fn aggregate_key_and_byte_budgets_are_released_after_reservation() {
        let mut budget = SnapshotBudget::default();
        budget.reserve(MAX_SNAPSHOT_TOTAL_KEYS, 1).unwrap();
        assert!(budget.reserve(1, 0).is_err());
        budget.release(MAX_SNAPSHOT_TOTAL_KEYS, 1);
        budget.reserve(1, MAX_SNAPSHOT_TOTAL_KEY_BYTES).unwrap();
        assert!(budget.reserve(0, 1).is_err());
        budget.release(1, MAX_SNAPSHOT_TOTAL_KEY_BYTES);
        assert!(budget.reserve(1, 1).is_ok());
    }

    #[test]
    fn expired_snapshot_fails_reads_until_cancelled_and_keeps_its_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put(b"k", b"v").unwrap();
        let mut snapshot = store.snapshot(1, 1).unwrap();
        snapshot.state.write().unwrap().deadline = Some(Instant::now());
        assert_eq!(
            snapshot.get(b"k").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            snapshot.scan(None, None, 1).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 1);
        snapshot.cancel();
        assert!(snapshot.is_cancelled());
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 0);
        assert!(snapshot.get(b"k").is_err());
    }

    #[test]
    fn admission_reaps_expired_snapshots_without_double_releasing_capacity() {
        use std::time::Duration;
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put(b"k", b"v").unwrap();
        let expired = store
            .snapshot_until(1, 1, Instant::now() + Duration::from_secs(60))
            .unwrap();
        let others = (1..MAX_ACTIVE_SNAPSHOTS)
            .map(|_| store.snapshot(1, 1).unwrap())
            .collect::<Vec<_>>();
        assert!(store.snapshot(1, 1).is_err());
        expired.state.write().unwrap().deadline = Some(Instant::now());
        let replacement = store.snapshot(1, 1).unwrap();
        assert_eq!(
            expired.get(b"k").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            store.snapshot_budget.lock().unwrap().active,
            MAX_ACTIVE_SNAPSHOTS
        );
        drop(expired);
        assert_eq!(
            store.snapshot_budget.lock().unwrap().active,
            MAX_ACTIVE_SNAPSHOTS
        );
        drop(replacement);
        drop(others);
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 0);
    }

    #[test]
    fn reaping_skips_a_snapshot_while_its_index_is_in_use() {
        use std::time::Duration;
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put(b"k", b"v").unwrap();
        let snapshot = store
            .snapshot_until(1, 1, Instant::now() + Duration::from_secs(60))
            .unwrap();
        snapshot.state.write().unwrap().deadline = Some(Instant::now());
        let active_read = snapshot.state.read().unwrap();
        assert_eq!(store.reap_expired_snapshots().unwrap(), 0);
        assert_eq!(active_read.index.len(), 1);
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 1);
        drop(active_read);
        assert_eq!(store.reap_expired_snapshots().unwrap(), 1);
        assert_eq!(store.reap_expired_snapshots().unwrap(), 0);
        assert_eq!(
            snapshot.get(b"k").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 0);
    }

    #[test]
    fn background_reaper_skips_busy_expired_views_until_the_next_check() {
        use std::time::{Duration, Instant};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path()).unwrap());
        store.put(b"k", b"v").unwrap();
        let snapshot = store
            .snapshot_until(1, 1, Instant::now() + Duration::from_secs(60))
            .unwrap();
        snapshot.state.write().unwrap().deadline = Some(Instant::now());
        let active_read = snapshot.state.read().unwrap();
        let worker = store
            .start_snapshot_reaper(Duration::from_secs(3600))
            .unwrap();
        worker.request_check().unwrap();
        let timeout = Instant::now() + Duration::from_secs(10);
        while worker.status().checks < 1 {
            assert!(Instant::now() < timeout);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(worker.status().reaped, 0);
        assert_eq!(active_read.index.len(), 1);
        drop(active_read);
        worker.request_check().unwrap();
        while worker.status().checks < 2 {
            assert!(Instant::now() < timeout);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(worker.stop().unwrap().reaped, 1);
        assert_eq!(
            snapshot.get(b"k").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 0);
    }

    #[test]
    fn background_reaper_races_with_cancellation_and_drop_without_leaking_budget() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{Duration, Instant};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path()).unwrap());
        store.put(b"k", b"v").unwrap();
        let worker = store
            .start_snapshot_reaper(Duration::from_millis(1))
            .unwrap();
        let admitted = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let (store, admitted) = (&store, &admitted);
                scope.spawn(move || {
                    for i in 0..100 {
                        if let Ok(mut snapshot) =
                            store.snapshot_until(1, 1, Instant::now() + Duration::from_secs(60))
                        {
                            admitted.fetch_add(1, Ordering::Relaxed);
                            snapshot.state.write().unwrap().deadline = Some(Instant::now());
                            std::thread::yield_now();
                            if i % 2 == 0 {
                                snapshot.cancel();
                            }
                        }
                    }
                });
            }
        });
        let status = worker.stop().unwrap();
        assert!(status.last_error.is_none());
        assert!(admitted.load(Ordering::Relaxed) > 0);
        store.reap_expired_snapshots().unwrap();
        let budget = store.snapshot_budget.lock().unwrap();
        assert_eq!((budget.active, budget.keys, budget.key_bytes), (0, 0, 0));
        assert!(store.snapshot_registry.lock().unwrap().is_empty());
    }

    #[test]
    fn concurrent_expiry_reaping_and_drop_release_each_reservation_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put(b"k", b"v").unwrap();
        let admitted = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let (store, admitted) = (&store, &admitted);
                scope.spawn(move || {
                    for _ in 0..50 {
                        if let Ok(snapshot) =
                            store.snapshot_until(1, 1, Instant::now() + Duration::from_secs(60))
                        {
                            admitted.fetch_add(1, Ordering::SeqCst);
                            assert!(
                                store.snapshot_registry.lock().unwrap().len()
                                    <= MAX_ACTIVE_SNAPSHOTS
                            );
                            snapshot.state.write().unwrap().deadline = Some(Instant::now());
                            assert_eq!(
                                snapshot.get(b"k").unwrap_err().kind(),
                                io::ErrorKind::TimedOut
                            );
                            store.reap_expired_snapshots().unwrap();
                            std::thread::yield_now();
                        }
                    }
                });
            }
        });
        assert!(admitted.load(Ordering::SeqCst) > 0);
        store.reap_expired_snapshots().unwrap();
        let budget = store.snapshot_budget.lock().unwrap();
        assert_eq!((budget.active, budget.keys, budget.key_bytes), (0, 0, 0));
        assert!(store.snapshot_registry.lock().unwrap().is_empty());
    }

    #[test]
    fn busy_stale_registry_entries_cannot_grow_registry_or_leak_reservations() {
        use std::time::Duration;
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put(b"k", b"v").unwrap();
        let snapshots = (0..MAX_ACTIVE_SNAPSHOTS)
            .map(|_| {
                store
                    .snapshot_until(1, 1, Instant::now() + Duration::from_secs(60))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut guards = snapshots
            .iter()
            .map(|snapshot| snapshot.state.write().unwrap())
            .collect::<Vec<_>>();
        for data in &mut guards {
            data.release(Some(&store), SnapshotStatus::Cancelled);
        }
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 0);
        let error = store
            .snapshot_until(1, 1, Instant::now() + Duration::from_secs(60))
            .err()
            .unwrap();
        assert!(error.to_string().contains("registry temporarily full"));
        assert_eq!(
            store.snapshot_registry.lock().unwrap().len(),
            MAX_ACTIVE_SNAPSHOTS
        );
        assert_eq!(store.snapshot_budget.lock().unwrap().active, 0);
        drop(guards);
        assert!(
            store
                .snapshot_until(1, 1, Instant::now() + Duration::from_secs(60))
                .is_ok()
        );
    }
}
