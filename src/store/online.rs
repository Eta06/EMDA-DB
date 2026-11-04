//! A guarded store handle supporting bounded, concurrent compaction preparation.
mod copy;

use super::{LogQuota, Store, compact_exchange};
use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, TryLockError};

thread_local! { static ACTIVE_ACCESS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) }; }

struct Access(usize);
impl Access {
    fn enter(id: usize) -> io::Result<Self> {
        ACTIVE_ACCESS.with(|active| {
            let mut active = active.borrow_mut();
            if active.contains(&id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "nested access to the same online store is unsupported",
                ));
            }
            active.push(id);
            Ok(Self(id))
        })
    }
    fn check_not_active(id: usize) -> io::Result<()> {
        ACTIVE_ACCESS.with(|active| {
            if active.borrow().contains(&id) {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot compact from an online store callback",
                ))
            } else {
                Ok(())
            }
        })
    }
}
impl Drop for Access {
    fn drop(&mut self) {
        ACTIVE_ACCESS.with(|active| active.borrow_mut().retain(|id| *id != self.0));
    }
}

struct State {
    store: Arc<Store>,
    failed: bool,
}

/// A guarded ordinary store that can prepare a compacted copy while callbacks
/// continue using the current store. It is experimental and single-process;
/// it is not a server, cross-process transaction manager or security boundary.
pub struct OnlineStore {
    path: PathBuf,
    state: RwLock<State>,
    maintenance: Mutex<()>,
}

/// Logical limits for one online compaction attempt. These are not total
/// process-memory or disk quotas.
#[derive(Clone, Debug)]
pub struct OnlineCompactionOptions {
    /// Maximum keys captured in the initial index snapshot.
    pub max_snapshot_keys: usize,
    /// Maximum logical key bytes in the initial snapshot and target live index.
    pub max_snapshot_key_bytes: usize,
    /// Maximum synchronized catch-up passes while normal operations continue.
    pub catchup_rounds: usize,
    /// Reject final handoff if the remaining source suffix exceeds this size.
    pub max_final_delta_bytes: u64,
    /// Per-open cap for the temporary target log, including its history.
    pub max_temporary_log_bytes: u64,
}

impl Default for OnlineCompactionOptions {
    fn default() -> Self {
        Self {
            max_snapshot_keys: super::MAX_SNAPSHOT_KEYS,
            max_snapshot_key_bytes: super::MAX_SNAPSHOT_KEY_BYTES,
            catchup_rounds: 2,
            max_final_delta_bytes: 16 * 1024 * 1024,
            max_temporary_log_bytes: 512 * 1024 * 1024,
        }
    }
}

/// Counters recorded by a successful online compaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnlineCompactionReport {
    /// Bytes in the source log at initial snapshot capture.
    pub initial_log_bytes: u64,
    /// Bytes in the source log at the final writer barrier.
    pub final_log_bytes: u64,
    /// Source suffix bytes replayed while other operations were admitted.
    pub concurrent_replay_bytes: u64,
    /// Remaining source suffix bytes replayed while callbacks were paused.
    pub final_replay_bytes: u64,
    /// Bytes in the compacted target log at handoff.
    pub compacted_log_bytes: u64,
}

impl OnlineStore {
    /// Opens an ordinary store through the guarded handle.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_resource_limits(path, None, None, None)
    }

    /// Opens with the ordinary store's per-open logical limits.
    pub fn open_with_resource_limits(
        path: impl AsRef<Path>,
        max_log_bytes: Option<u64>,
        max_live_keys: Option<usize>,
        max_live_key_bytes: Option<usize>,
    ) -> io::Result<Self> {
        let store = Store::open_with_resource_limits(
            path.as_ref(),
            max_log_bytes,
            max_live_keys,
            max_live_key_bytes,
        )?;
        let path = fs::canonicalize(path)?;
        crate::directory::verify_lock_identity(&path, &store._lock_file)?;
        Ok(Self {
            path,
            state: RwLock::new(State {
                store: Arc::new(store),
                failed: false,
            }),
            maintenance: Mutex::new(()),
        })
    }

    /// Uses the current store for the duration of the callback. The callback
    /// must not re-enter this handle or wait for another thread that is trying
    /// to compact it. Transactions and document operations can be run here.
    /// No borrowed store, transaction or snapshot may escape the callback.
    pub fn with_store<T>(&self, action: impl FnOnce(&Store) -> io::Result<T>) -> io::Result<T> {
        let _access = Access::enter(self as *const Self as usize)?;
        let state = self.state.read().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        action(&state.store)
    }

    /// Creates a verified compacted sibling while ordinary callbacks continue,
    /// catches up acknowledged writes, then excludes callbacks for final
    /// replay, verification and a Linux atomic directory exchange. The old
    /// complete store remains at `previous`. A publication or sync error after
    /// exchange poisons this handle; drop and reopen both paths to inspect them.
    ///
    /// Only callbacks through this handle are coordinated. The directory must
    /// be trusted, contain only ordinary-store files and be accessed by current
    /// cooperating clients. The previous path must be an unused sibling. This
    /// rejects histories or targets exceeding the supplied logical limits and
    /// does not guarantee bounded total memory, disk use or pause time. It does
    /// not reclaim the retained previous copy, provide persistent snapshots, or
    /// establish physical power-loss behavior.
    pub fn compact_and_replace(
        &self,
        previous: impl AsRef<Path>,
        options: &OnlineCompactionOptions,
    ) -> io::Result<OnlineCompactionReport> {
        self.compact_with_hooks(
            previous.as_ref(),
            options,
            |_| Ok(()),
            crate::directory::sync_existing,
        )
    }

    fn compact_with_hooks(
        &self,
        previous: &Path,
        options: &OnlineCompactionOptions,
        mut hook: impl FnMut(copy::Point) -> io::Result<()>,
        sync_parent: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<OnlineCompactionReport> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "online directory exchange currently requires Linux",
            ));
        }
        Access::check_not_active(self as *const Self as usize)?;
        let _maintenance = match self.maintenance.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "online compaction already running",
                ));
            }
            Err(TryLockError::Poisoned(_)) => return Err(uncertain()),
        };
        options.validate()?;
        let (source, previous, parent) = compact_exchange::paths(&self.path, previous)?;
        let current = {
            let state = self.state.read().map_err(|_| uncertain())?;
            if state.failed {
                return Err(uncertain());
            }
            Arc::clone(&state.store)
        };
        crate::directory::verify_lock_identity(&source, &current._lock_file)?;
        compact_exchange::source_directory_is_owned(&source)?;
        let (snapshot, initial) = copy::capture(&current, options)?;
        copy::verify_initial(&current, &snapshot, initial, options)?;
        let mut stage = crate::staging::Stage::directory(&parent)?;
        let target = Store::open_with_resource_limits(
            stage.path(),
            Some(options.max_temporary_log_bytes),
            Some(options.max_snapshot_keys),
            Some(options.max_snapshot_key_bytes),
        )?;
        copy::copy_snapshot(&current, &snapshot, &target)?;
        hook(copy::Point::InitialCopy)?;
        let mut offset = initial;
        for _ in 0..options.catchup_rounds {
            let end = copy::boundary(&current)?;
            if end == offset {
                break;
            }
            copy::replay(&current, &target, offset, end)?;
            offset = end;
        }
        let concurrent_replay_bytes = offset
            .checked_sub(initial)
            .ok_or_else(|| io::Error::other("source log boundary moved backward"))?;
        hook(copy::Point::BeforeFinalBarrier)?;
        let mut state = self.state.write().map_err(|_| uncertain())?;
        if state.failed || !Arc::ptr_eq(&state.store, &current) {
            return Err(uncertain());
        }
        let _writer = current
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        let final_boundary = copy::boundary_locked(&current)?;
        let final_replay_bytes = final_boundary
            .checked_sub(offset)
            .ok_or_else(|| io::Error::other("source log boundary moved backward"))?;
        if final_replay_bytes > options.max_final_delta_bytes {
            return Err(io::Error::other(
                "final compaction suffix exceeds configured limit",
            ));
        }
        copy::replay(&current, &target, offset, final_boundary)?;
        copy::verify_target(&current, &target)?;
        let target_length = target.log.len()?;
        let original_log_limit = current.log_quota.as_ref().map(LogQuota::limit);
        let original_key_limit = current.max_live_keys;
        let original_key_bytes = current.max_live_key_bytes;
        super::compact::close_private_store(target)?;
        let candidate = Store::open_with_resource_limits(
            stage.path(),
            original_log_limit,
            original_key_limit,
            original_key_bytes,
        )?;
        crate::directory::verify_lock_identity(&source, &current._lock_file)?;
        compact_exchange::source_directory_is_owned(&source)?;
        fs::File::open(stage.path())?.sync_all()?;
        stage.publish(&previous)?;
        crate::directory::verify_lock_identity(&previous, &candidate._lock_file)?;
        crate::directory::sync_existing(&parent).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "compacted sibling may have been published; source remains active: {error}"
                ),
            )
        })?;
        hook(copy::Point::BeforeExchange)?;
        state.failed = true;
        compact_exchange::exchange(&source, &previous).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("online exchange may have occurred; reopen required: {error}"),
            )
        })?;
        hook(copy::Point::AfterExchange).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("online exchange may have occurred; reopen required: {error}"),
            )
        })?;
        sync_parent(&parent).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "online exchange may have occurred; directory synchronization failed: {error}"
                ),
            )
        })?;
        state.store = Arc::new(candidate);
        state.failed = false;
        Ok(OnlineCompactionReport {
            initial_log_bytes: initial,
            final_log_bytes: final_boundary,
            concurrent_replay_bytes,
            final_replay_bytes,
            compacted_log_bytes: target_length,
        })
    }
}

impl OnlineCompactionOptions {
    fn validate(&self) -> io::Result<()> {
        if self.max_snapshot_keys > super::MAX_SNAPSHOT_KEYS
            || self.max_snapshot_key_bytes > super::MAX_SNAPSHOT_KEY_BYTES
            || self.catchup_rounds > 16
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "online compaction options exceed supported limits",
            ));
        }
        Ok(())
    }
}

fn uncertain() -> io::Error {
    io::Error::other("online store handoff may have an uncertain outcome; reopen required")
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
