//! Optional process-local reclamation of expired, idle snapshot views.
use crate::{CheckpointStore, Store};
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MIN_INTERVAL: Duration = Duration::from_millis(1);
const MAX_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Observable state of one snapshot-reclamation worker.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotReaperStatus {
    /// Completed checks, including checks that found no idle expired views.
    pub checks: u64,
    /// Expired views reclaimed by this worker, saturated at `u64::MAX`.
    pub reaped: u64,
    /// A check is executing or waiting for a store lock.
    pub in_flight: bool,
    /// The worker has stopped; join it before starting another.
    pub stopped: bool,
    /// Error or panic that stopped the worker, if any.
    pub last_error: Option<String>,
}

#[derive(Default)]
struct WorkerState {
    stopping: bool,
    requested: bool,
    status: SnapshotReaperStatus,
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

/// A stoppable worker. Drop requests shutdown and joins its thread.
/// An in-flight check and lock wait are not interrupted or time-bounded.
pub struct SnapshotReaper {
    control: Arc<Control>,
    thread: Option<JoinHandle<()>>,
}
impl SnapshotReaper {
    /// Requests an extra check; concurrent requests may be coalesced.
    pub fn request_check(&self) -> io::Result<()> {
        let mut state = self.control.lock();
        if state.stopping || state.status.stopped {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "snapshot reaper has stopped",
            ));
        }
        state.requested = true;
        self.control.changed.notify_one();
        Ok(())
    }

    /// Returns a consistent snapshot of the worker's status.
    pub fn status(&self) -> SnapshotReaperStatus {
        self.control.lock().status.clone()
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
                .map_err(|_| io::Error::other("snapshot reaper panicked"))?;
        }
        Ok(())
    }

    /// Stops and joins the worker; inspect `last_error` for check failures.
    pub fn stop(mut self) -> io::Result<SnapshotReaperStatus> {
        self.shutdown()?;
        Ok(self.status())
    }
}
impl Drop for SnapshotReaper {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

trait Target: Send + Sync + 'static {
    fn flag(&self) -> &AtomicBool;
    fn reap(&self) -> io::Result<usize>;
}
impl Target for Store {
    fn flag(&self) -> &AtomicBool {
        &self.snapshot_reaper_active
    }
    fn reap(&self) -> io::Result<usize> {
        self.reap_expired_snapshots()
    }
}
impl Target for CheckpointStore {
    fn flag(&self) -> &AtomicBool {
        &self.snapshot_reaper_active
    }
    fn reap(&self) -> io::Result<usize> {
        self.reap_expired_snapshots()
    }
}

struct Lease<T: Target>(Arc<T>);
impl<T: Target> Drop for Lease<T> {
    fn drop(&mut self) {
        self.0.flag().store(false, Ordering::Release);
    }
}

fn start<T: Target>(store: &Arc<T>, interval: Duration) -> io::Result<SnapshotReaper> {
    if !(MIN_INTERVAL..=MAX_INTERVAL).contains(&interval) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid snapshot reaper interval",
        ));
    }
    if store
        .flag()
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "a snapshot reaper is already active",
        ));
    }
    let lease = Lease(Arc::clone(store));
    let control = Arc::new(Control::default());
    let thread_control = Arc::clone(&control);
    let thread = thread::Builder::new()
        .name("snapshot-reaper".into())
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                run(&*lease.0, &thread_control, interval)
            }));
            if result.is_err() {
                let mut state = thread_control.lock();
                state.status.in_flight = false;
                state.status.stopped = true;
                state.status.last_error =
                    Some("snapshot reaper panicked; inspect or reopen the store".into());
                thread_control.changed.notify_all();
            }
            drop(lease);
        })?;
    Ok(SnapshotReaper {
        control,
        thread: Some(thread),
    })
}

fn run<T: Target>(store: &T, control: &Control, interval: Duration) {
    loop {
        let mut state = control.lock();
        if !state.stopping && !state.requested {
            let (next, _) = control
                .changed
                .wait_timeout_while(state, interval, |state| !state.stopping && !state.requested)
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
        let result = store.reap();
        let mut state = control.lock();
        state.status.in_flight = false;
        state.status.checks = state.status.checks.saturating_add(1);
        match result {
            Ok(count) => {
                state.status.reaped = state
                    .status
                    .reaped
                    .saturating_add(u64::try_from(count).unwrap_or(u64::MAX))
            }
            Err(error) => {
                state.status.stopped = true;
                state.status.last_error = Some(error.to_string());
                control.changed.notify_all();
                return;
            }
        }
        control.changed.notify_all();
    }
}

impl Store {
    /// Starts one optional worker to reclaim expired, idle snapshots at the
    /// configured 1 ms to 24 h interval or after a requested check. In-use
    /// snapshots are skipped, not interrupted; snapshots without a deadline
    /// are unaffected. Checks stop on error or panic. The worker retains an
    /// `Arc` to this store; stop it before consuming or closing the store.
    /// The worker is process-local, not persisted, and neither bounds total
    /// memory nor guarantees when expired views are reclaimed.
    pub fn start_snapshot_reaper(
        self: &Arc<Self>,
        interval: Duration,
    ) -> io::Result<SnapshotReaper> {
        start(self, interval)
    }
}
impl CheckpointStore {
    /// Starts one optional expired-snapshot worker with the same semantics as
    /// [`Store::start_snapshot_reaper`]. It does not compact the source log.
    pub fn start_snapshot_reaper(
        self: &Arc<Self>,
        interval: Duration,
    ) -> io::Result<SnapshotReaper> {
        start(self, interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Instant;

    struct Fake {
        flag: AtomicBool,
        mode: AtomicUsize,
    }
    impl Target for Fake {
        fn flag(&self) -> &AtomicBool {
            &self.flag
        }
        fn reap(&self) -> io::Result<usize> {
            match self.mode.load(Ordering::Relaxed) {
                0 => Err(io::Error::other("injected reaping error")),
                1 => panic!("injected reaping panic"),
                _ => Ok(3),
            }
        }
    }
    fn until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !condition() {
            assert!(
                Instant::now() < deadline,
                "snapshot worker did not make progress"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn failures_and_panics_stop_and_release_the_worker_lease() {
        for mode in [0, 1] {
            let target = Arc::new(Fake {
                flag: AtomicBool::new(false),
                mode: AtomicUsize::new(mode),
            });
            let worker = start(&target, Duration::from_secs(3600)).unwrap();
            worker.request_check().unwrap();
            until(|| worker.status().stopped);
            let status = worker.stop().unwrap();
            assert!(status.last_error.is_some());
            assert!(!status.in_flight);
            assert!(!target.flag.load(Ordering::Acquire));
            target.mode.store(2, Ordering::Relaxed);
            let worker = start(&target, Duration::from_secs(3600)).unwrap();
            worker.request_check().unwrap();
            until(|| worker.status().checks == 1);
            assert_eq!(worker.stop().unwrap().reaped, 3);
        }
    }

    #[test]
    fn concurrent_start_admits_one_worker_per_target() {
        for _ in 0..32 {
            let target = Arc::new(Fake {
                flag: AtomicBool::new(false),
                mode: AtomicUsize::new(2),
            });
            let barrier = Barrier::new(9);
            let winners = Mutex::new(Vec::new());
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    scope.spawn(|| {
                        barrier.wait();
                        match start(&target, Duration::from_secs(3600)) {
                            Ok(worker) => winners.lock().unwrap().push(worker),
                            Err(error) => assert_eq!(error.kind(), io::ErrorKind::AlreadyExists),
                        }
                    });
                }
                barrier.wait();
            });
            let workers = winners.into_inner().unwrap();
            assert_eq!(workers.len(), 1);
            assert!(target.flag.load(Ordering::Acquire));
            drop(workers);
            assert!(!target.flag.load(Ordering::Acquire));
        }
    }

    struct Blocking {
        flag: AtomicBool,
        entered: mpsc::Sender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }
    impl Target for Blocking {
        fn flag(&self) -> &AtomicBool {
            &self.flag
        }
        fn reap(&self) -> io::Result<usize> {
            self.entered.send(()).unwrap();
            self.resume.lock().unwrap().recv().unwrap();
            Ok(1)
        }
    }

    #[test]
    fn stop_waits_for_an_in_flight_check_then_releases_its_lease() {
        let (entered, started) = mpsc::channel();
        let (resume, wait) = mpsc::channel();
        let target = Arc::new(Blocking {
            flag: AtomicBool::new(false),
            entered,
            resume: Mutex::new(wait),
        });
        let worker = start(&target, Duration::from_secs(3600)).unwrap();
        worker.request_check().unwrap();
        started.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(worker.status().in_flight);
        let (finished, stopped) = mpsc::channel();
        let join = std::thread::spawn(move || finished.send(worker.stop()).unwrap());
        assert!(stopped.recv_timeout(Duration::from_millis(50)).is_err());
        resume.send(()).unwrap();
        let status = stopped
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        join.join().unwrap();
        assert!(status.stopped);
        assert_eq!((status.checks, status.reaped), (1, 1));
        assert!(!target.flag.load(Ordering::Acquire));
    }
}
