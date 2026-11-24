//! Opt-in scheduling of verified online checkpoint-store reclamation.
use super::{Access, CheckpointStore, OnlineCheckpointCompactionOptions, OnlineCheckpointStore};
use crate::store::online::worker::{
    Control, Failure, MAX_INTERVAL, MIN_INTERVAL, Outcome, guarded_run, run_loop,
};
use crate::{
    DiskBudgetPending, OnlineHandoffFailure, OnlineReclaimFailure, OnlineReclamationFailureStage,
    OnlineReclamationWorker,
};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

struct Schedule {
    previous: PathBuf,
    interval: Duration,
    minimum_log_bytes: u64,
    minimum_growth_bytes: u64,
    options: OnlineCheckpointCompactionOptions,
}

struct Observation {
    store: Weak<CheckpointStore>,
    bytes: u64,
}

// Releases the one-worker-per-handle lease when the worker exits, including
// on unwind or a spawn failure.
struct WorkerLease(Arc<OnlineCheckpointStore>);

impl Drop for WorkerLease {
    fn drop(&mut self) {
        self.0.reclamation_worker.store(false, Ordering::Release);
    }
}

fn failure(stage: OnlineReclamationFailureStage, error: io::Error) -> Outcome<Observation> {
    Outcome::Failed(Failure {
        stage,
        error,
        report: None,
        reclaimed: false,
    })
}

impl OnlineCheckpointStore {
    /// Starts one opt-in worker that calls
    /// [`compact_and_reclaim_detailed`](Self::compact_and_reclaim_detailed)
    /// on a schedule. It permanently discards each previous generation after
    /// its synchronized handoff, so keep independent backups where needed.
    ///
    /// A check compacts when the synchronized log is at least
    /// `minimum_log_bytes` and, after this worker's own handoff, has grown by
    /// at least `minimum_growth_bytes` from the boundary observed afterwards.
    /// The worker wakes at `interval` (1 ms through 24 hours) or on
    /// [`OnlineReclamationWorker::request_check`]; requests made during a
    /// check coalesce, and waiting never spins. Thresholds are scheduling
    /// hints, not caps.
    ///
    /// The deferral conditions below are decided while holding the handle's
    /// maintenance lease, the same lease the admitted compaction then uses,
    /// so a manual compaction, cleanup or reconciliation cannot interleave.
    /// A check is deferred to the next wake, counted by
    /// [`OnlineReclamationWorker::busy_checks`] or
    /// [`OnlineReclamationWorker::deferred_checks`], when another maintenance
    /// operation holds the handle; when the disk budget still records
    /// outstanding maintenance that cannot yet be reconciled; when a
    /// generation previously replaced through this handle is still held by an
    /// owned snapshot, so scheduled handoffs never stack retired generations;
    /// or when the disk budget refuses the replacement reservation. Nothing
    /// is published in those cases. Reconciling a budget record only clears
    /// it when nothing remains; the worker never deletes a candidate or a
    /// previous generation it did not just replace.
    ///
    /// The worker stops, with the failure in its status, when `previous`
    /// already exists, when a handoff fails or has an uncertain outcome, or
    /// when reclamation after a successful handoff fails. Inspect and
    /// reconcile before starting another worker; it does not retry those
    /// failures. One worker can run per handle. It holds an `Arc` to this
    /// handle until stopped; never stop or drop it from a callback of this
    /// handle. It does not persist across reopen and bounds neither memory,
    /// recovery nor pause time.
    pub fn start_reclamation_worker(
        self: &Arc<Self>,
        previous: impl AsRef<Path>,
        interval: Duration,
        minimum_log_bytes: u64,
        minimum_growth_bytes: u64,
        options: OnlineCheckpointCompactionOptions,
    ) -> io::Result<OnlineReclamationWorker> {
        Access::check_not_active(self.id())?;
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
        let (_, previous, _) =
            crate::store::compact_exchange::paths(&self.path, previous.as_ref())?;
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
            .name("checkpoint-reclamation-worker".into())
            .spawn(move || {
                guarded_run(&thread_control, || {
                    run_loop(&thread_control, schedule.interval, |baseline| {
                        check(&lease.0, &schedule, baseline, |store, schedule| {
                            store.compact_scheduled(schedule)
                        })
                    });
                });
                drop(lease);
            })?;
        Ok(OnlineReclamationWorker::from_parts(control, thread))
    }

    fn reclamation_observation(&self) -> io::Result<Observation> {
        let state = self.state.read().map_err(|_| super::uncertain())?;
        if state.failed {
            return Err(super::uncertain());
        }
        let bytes = super::boundary(&state.store)?;
        Ok(Observation {
            store: Arc::downgrade(&state.store),
            bytes,
        })
    }

    // Whether a scheduled compaction has no safe room. The caller holds the
    // maintenance lease, which every compaction, cleanup and reconciliation
    // of this handle also takes, so reconciling here cannot clear a
    // reservation that a manual operation is still using.
    pub(super) fn scheduled_deferral_locked(&self) -> io::Result<bool> {
        let pinned = {
            let mut retired = self
                .retired
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            retired.retain(|token| token.strong_count() != 0);
            !retired.is_empty()
        };
        if pinned {
            return Ok(true);
        }
        let budget = self
            .state
            .read()
            .map_err(|_| super::uncertain())?
            .store
            .disk_budget()?;
        if let Some(budget) = budget {
            if budget.status()?.pending != DiskBudgetPending::None && !budget.reconcile()? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn compact_scheduled(
        &self,
        schedule: &Schedule,
    ) -> Result<crate::OnlineCompactionReport, OnlineReclaimFailure> {
        self.compact_reclaim_scheduled(
            &schedule.previous,
            &schedule.options,
            (|_| Ok(()), crate::directory::sync_existing),
        )
    }
}

fn check(
    store: &OnlineCheckpointStore,
    schedule: &Schedule,
    baseline: Option<&Observation>,
    compact: impl FnOnce(
        &OnlineCheckpointStore,
        &Schedule,
    ) -> Result<crate::OnlineCompactionReport, OnlineReclaimFailure>,
) -> Outcome<Observation> {
    use OnlineReclamationFailureStage::{Compaction, Observation as Observe, Reclamation};
    let observed = match store.reclamation_observation() {
        Ok(observed) => observed,
        Err(error) => return failure(Observe, error),
    };
    if let Some(previous) =
        baseline.filter(|previous| Weak::ptr_eq(&previous.store, &observed.store))
    {
        let Some(growth) = observed.bytes.checked_sub(previous.bytes) else {
            return failure(
                Observe,
                io::Error::other("synchronized log boundary moved backward"),
            );
        };
        if growth < schedule.minimum_growth_bytes {
            return Outcome::Skipped;
        }
    }
    if observed.bytes < schedule.minimum_log_bytes {
        return Outcome::Skipped;
    }
    // A leftover at the previous path, such as an interrupted candidate or
    // an unretired generation, needs inspection rather than waiting.
    if std::fs::symlink_metadata(&schedule.previous).is_ok() {
        return failure(
            Compaction,
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "previous-store path {} exists; inspect, reconcile and remove it before restarting the worker",
                    schedule.previous.display()
                ),
            ),
        );
    }
    let report = match compact(store, schedule) {
        Err(OnlineReclaimFailure::Compaction(OnlineHandoffFailure::NotPublished(error)))
            if error.kind() == io::ErrorKind::WouldBlock
                && error
                    .get_ref()
                    .is_some_and(|inner| inner.is::<super::ScheduledDeferral>()) =>
        {
            return Outcome::Deferred;
        }
        Ok(report) => report,
        Err(OnlineReclaimFailure::Compaction(OnlineHandoffFailure::NotPublished(error)))
            if error.kind() == io::ErrorKind::WouldBlock
                && error
                    .get_ref()
                    .is_some_and(|inner| inner.is::<super::MaintenanceBusy>()) =>
        {
            return Outcome::MaintenanceBusy;
        }
        Err(OnlineReclaimFailure::Compaction(OnlineHandoffFailure::NotPublished(error)))
            if error.kind() == io::ErrorKind::StorageFull =>
        {
            return Outcome::Deferred;
        }
        Err(OnlineReclaimFailure::Compaction(failure_kind)) => {
            let error = io::Error::other(format!(
                "{failure_kind}; reopen and reconcile both paths before restarting the worker"
            ));
            return failure(Compaction, error);
        }
        Err(OnlineReclaimFailure::Reclamation { report, error }) => {
            return Outcome::Failed(Failure {
                stage: Reclamation,
                error,
                report: Some(report),
                reclaimed: false,
            });
        }
    };
    match store.reclamation_observation() {
        Ok(observed) => Outcome::Reclaimed(report, observed),
        Err(error) => Outcome::Failed(Failure {
            stage: Observe,
            error,
            report: Some(report),
            reclaimed: true,
        }),
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
