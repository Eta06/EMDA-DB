//! Opt-in scheduling of verified ordinary-store reclamation.
use super::{
    Access, OnlineCompactionOptions, OnlineCompactionReport, OnlineReclaimError, OnlineStore,
    Store, compact_exchange, copy, uncertain,
};
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MIN_INTERVAL: Duration = Duration::from_millis(1);
const MAX_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// The operation at which a reclamation worker stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OnlineReclamationFailureStage {
    /// Inspecting the current synchronized log boundary failed.
    Observation,
    /// Compaction or publication failed; the handoff outcome may be uncertain.
    Compaction,
    /// The handoff succeeded but removing the old copy did not complete.
    Reclamation,
    /// The worker panicked; the store or its locks may require reopening.
    Panic,
}

/// An error that stopped the worker. Inspect the store and any retained files
/// before restarting it; messages are diagnostic, not a stable error protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnlineReclamationFailure {
    /// The stage at which the worker stopped.
    pub stage: OnlineReclamationFailureStage,
    /// A human-readable description.
    pub message: String,
}

/// A consistent snapshot of a worker's completed checks and latest result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OnlineReclamationWorkerStatus {
    /// Completed checks, including checks that did not compact.
    pub checks: u64,
    /// Handoffs known to have succeeded, including incomplete cleanup.
    pub handoffs: u64,
    /// Handoffs whose reclamation completed successfully.
    pub reclaimed: u64,
    /// Most recent known successful handoff, including one with cleanup error.
    pub last_report: Option<OnlineCompactionReport>,
    /// Whether a check is running or waiting for a store lock.
    pub in_flight: bool,
    /// Whether the worker has exited.
    pub stopped: bool,
    /// The failure that stopped the worker, if any.
    pub last_error: Option<OnlineReclamationFailure>,
}

#[derive(Default)]
struct WorkerState {
    stopping: bool,
    requested: bool,
    busy_checks: u64,
    status: OnlineReclamationWorkerStatus,
}

#[derive(Default)]
struct Control {
    state: Mutex<WorkerState>,
    changed: Condvar,
}

impl Control {
    fn lock(&self) -> MutexGuard<'_, WorkerState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

// Keep the lease until the worker exits, including on unwind or spawn failure.
struct WorkerLease(Arc<OnlineStore>);

impl Drop for WorkerLease {
    fn drop(&mut self) {
        self.0.reclamation_worker.store(false, Ordering::Release);
    }
}

/// An explicitly enabled background worker. Dropping it requests stop and
/// joins its thread. An in-flight check is not interrupted; stop and drop have
/// no bounded-completion guarantee. Never wait for the worker while holding a
/// callback or another resource needed by that worker.
pub struct OnlineReclamationWorker {
    control: Arc<Control>,
    thread: Option<JoinHandle<()>>,
}

impl OnlineReclamationWorker {
    /// Requests an additional check. Concurrent requests may be coalesced;
    /// both configured size and growth thresholds still apply.
    pub fn request_check(&self) -> io::Result<()> {
        let mut state = self.control.lock();
        if state.stopping || state.status.stopped {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "reclamation worker has stopped",
            ));
        }
        state.requested = true;
        self.control.changed.notify_one();
        Ok(())
    }

    /// Returns a consistent snapshot of the worker's status.
    pub fn status(&self) -> OnlineReclamationWorkerStatus {
        self.control.lock().status.clone()
    }

    /// Returns the number of checks deferred because another maintenance
    /// operation held the lock before this worker began compaction.
    pub fn busy_checks(&self) -> u64 {
        self.control.lock().busy_checks
    }

    fn shutdown(&mut self) -> io::Result<()> {
        {
            let mut state = self.control.lock();
            state.stopping = true;
            self.control.changed.notify_all();
        }
        if let Some(worker) = self.thread.take() {
            worker
                .join()
                .map_err(|_| io::Error::other("reclamation worker panicked"))?;
        }
        Ok(())
    }

    /// Requests stop, joins the worker, and returns its final status. Check
    /// `last_error` to determine whether the worker stopped because of error.
    pub fn stop(mut self) -> io::Result<OnlineReclamationWorkerStatus> {
        self.shutdown()?;
        Ok(self.status())
    }
}

impl Drop for OnlineReclamationWorker {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

struct Schedule {
    previous: PathBuf,
    interval: Duration,
    minimum_log_bytes: u64,
    minimum_growth_bytes: u64,
    options: OnlineCompactionOptions,
}

struct Observation {
    store: Weak<Store>,
    bytes: u64,
}

impl OnlineStore {
    /// Starts one opt-in Linux worker that periodically calls
    /// `compact_and_reclaim`. The old log history is permanently discarded
    /// after a synchronized handoff; use independent backups where needed.
    /// `previous` must be an unused sibling in a trusted directory. The same
    /// path is reused only after a complete successful cleanup.
    ///
    /// A check considers compaction when the synchronized log length is at
    /// least `minimum_log_bytes`. After its own successful compaction the
    /// worker also waits for at least `minimum_growth_bytes` of growth from
    /// the boundary it subsequently observes, preventing repeated compaction
    /// of unchanged live data. Other maintenance can change the store between
    /// observation and compaction; thresholds are scheduling hints, not caps.
    /// Intervals must be 1 ms through 24 hours, and both thresholds nonzero.
    ///
    /// A maintenance-lock conflict before compaction begins defers the check
    /// until the next interval or explicit request. Other errors stop the
    /// worker for inspection, including I/O errors with uncertain outcomes.
    /// It does not retry those failures, delete crash leftovers,
    /// persist across reopen, bound disk/memory/recovery or pause time, provide
    /// secure erasure, or make power-loss guarantees. It holds an `Arc` to this
    /// store until stopped. Callers must avoid cross-thread dependency cycles.
    pub fn start_reclamation_worker(
        self: &Arc<Self>,
        previous: impl AsRef<Path>,
        interval: Duration,
        minimum_log_bytes: u64,
        minimum_growth_bytes: u64,
        options: OnlineCompactionOptions,
    ) -> io::Result<OnlineReclamationWorker> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "online reclamation requires Linux",
            ));
        }
        Access::check_not_active(self.as_ref() as *const Self as usize)?;
        if !(MIN_INTERVAL..=MAX_INTERVAL).contains(&interval)
            || minimum_log_bytes == 0
            || minimum_growth_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid reclamation schedule",
            ));
        }
        options.validate()?;
        let (_, previous, _) = compact_exchange::paths(&self.path, previous.as_ref())?;
        if self
            .reclamation_worker
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a reclamation worker is already active",
            ));
        }
        let lease = WorkerLease(Arc::clone(self));
        let control = Arc::new(Control::default());
        let thread_control = Arc::clone(&control);
        let schedule = Schedule {
            previous,
            interval,
            minimum_log_bytes,
            minimum_growth_bytes,
            options,
        };
        let thread = thread::Builder::new()
            .name("reclamation-worker".into())
            .spawn(move || {
                guarded_run(&thread_control, || {
                    run(&lease.0, &thread_control, &schedule)
                });
                drop(lease);
            })?;
        Ok(OnlineReclamationWorker {
            control,
            thread: Some(thread),
        })
    }

    fn reclamation_observation(&self) -> io::Result<Observation> {
        let state = self.state.read().map_err(|_| uncertain())?;
        if state.failed {
            return Err(uncertain());
        }
        let bytes = copy::boundary(&state.store)?;
        Ok(Observation {
            store: Arc::downgrade(&state.store),
            bytes,
        })
    }
}

struct Failure {
    stage: OnlineReclamationFailureStage,
    error: io::Error,
    report: Option<OnlineCompactionReport>,
    reclaimed: bool,
}

enum Outcome {
    Skipped,
    MaintenanceBusy,
    Reclaimed(OnlineCompactionReport, Observation),
    Failed(Failure),
}

fn check(store: &OnlineStore, schedule: &Schedule, baseline: Option<&Observation>) -> Outcome {
    check_with(store, schedule, baseline, || {
        store.compact_and_reclaim(&schedule.previous, &schedule.options)
    })
}

fn check_with(
    store: &OnlineStore,
    schedule: &Schedule,
    baseline: Option<&Observation>,
    compact: impl FnOnce() -> Result<OnlineCompactionReport, OnlineReclaimError>,
) -> Outcome {
    let observed = match store.reclamation_observation() {
        Ok(value) => value,
        Err(error) => {
            return Outcome::Failed(Failure {
                stage: OnlineReclamationFailureStage::Observation,
                error,
                report: None,
                reclaimed: false,
            });
        }
    };
    if let Some(previous) =
        baseline.filter(|previous| Weak::ptr_eq(&previous.store, &observed.store))
    {
        let Some(growth) = observed.bytes.checked_sub(previous.bytes) else {
            return Outcome::Failed(Failure {
                stage: OnlineReclamationFailureStage::Observation,
                error: io::Error::other("synchronized log boundary moved backward"),
                report: None,
                reclaimed: false,
            });
        };
        if growth < schedule.minimum_growth_bytes {
            return Outcome::Skipped;
        }
    }
    if observed.bytes < schedule.minimum_log_bytes {
        return Outcome::Skipped;
    }
    let report = match compact() {
        Ok(report) => report,
        Err(OnlineReclaimError::Compaction(error)) if super::is_maintenance_busy(&error) => {
            return Outcome::MaintenanceBusy;
        }
        Err(OnlineReclaimError::Compaction(error)) => {
            return Outcome::Failed(Failure {
                stage: OnlineReclamationFailureStage::Compaction,
                error,
                report: None,
                reclaimed: false,
            });
        }
        Err(OnlineReclaimError::Reclamation { report, error }) => {
            return Outcome::Failed(Failure {
                stage: OnlineReclamationFailureStage::Reclamation,
                error,
                report: Some(report),
                reclaimed: false,
            });
        }
    };
    match store.reclamation_observation() {
        Ok(observed) => Outcome::Reclaimed(report, observed),
        Err(error) => Outcome::Failed(Failure {
            stage: OnlineReclamationFailureStage::Observation,
            error,
            report: Some(report),
            reclaimed: true,
        }),
    }
}

fn guarded_run(control: &Control, action: impl FnOnce()) {
    if catch_unwind(AssertUnwindSafe(action)).is_err() {
        let mut state = control.lock();
        state.status.in_flight = false;
        state.status.stopped = true;
        state.status.last_error = Some(OnlineReclamationFailure {
            stage: OnlineReclamationFailureStage::Panic,
            message: "reclamation worker panicked; inspect and reopen the store".into(),
        });
        control.changed.notify_all();
    }
}

fn run(store: &OnlineStore, control: &Control, schedule: &Schedule) {
    run_with(store, control, schedule, check);
}

fn run_with(
    store: &OnlineStore,
    control: &Control,
    schedule: &Schedule,
    mut check: impl FnMut(&OnlineStore, &Schedule, Option<&Observation>) -> Outcome,
) {
    let mut baseline = None;
    loop {
        let mut state = control.lock();
        if !state.stopping && !state.requested {
            let (next, _) = control
                .changed
                .wait_timeout_while(state, schedule.interval, |state| {
                    !state.stopping && !state.requested
                })
                .unwrap_or_else(|poison| poison.into_inner());
            state = next;
        }
        if state.stopping {
            state.status.stopped = true;
            control.changed.notify_all();
            return;
        }
        state.requested = false;
        state.status.in_flight = true;
        control.changed.notify_all();
        drop(state);
        let outcome = check(store, schedule, baseline.as_ref());
        let mut state = control.lock();
        state.status.in_flight = false;
        state.status.checks = state.status.checks.saturating_add(1);
        match outcome {
            Outcome::Skipped => {}
            Outcome::MaintenanceBusy => {
                state.busy_checks = state.busy_checks.saturating_add(1);
            }
            Outcome::Reclaimed(report, observed) => {
                state.status.handoffs = state.status.handoffs.saturating_add(1);
                state.status.reclaimed = state.status.reclaimed.saturating_add(1);
                state.status.last_report = Some(report);
                baseline = Some(observed);
            }
            Outcome::Failed(failure) => {
                if let Some(report) = failure.report {
                    state.status.handoffs = state.status.handoffs.saturating_add(1);
                    if failure.reclaimed {
                        state.status.reclaimed = state.status.reclaimed.saturating_add(1);
                    }
                    state.status.last_report = Some(report);
                }
                state.status.last_error = Some(OnlineReclamationFailure {
                    stage: failure.stage,
                    message: failure.error.to_string(),
                });
                state.status.stopped = true;
                control.changed.notify_all();
                return;
            }
        }
        control.changed.notify_all();
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
