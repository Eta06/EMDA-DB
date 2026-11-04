//! Optional, stoppable background scheduling for managed checkpoints.
use super::CheckpointStore;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MIN_INTERVAL: Duration = Duration::from_millis(1);
const MAX_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Observable state of a background checkpoint worker.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckpointWorkerStatus {
    /// Number of completed checks, including checks that did not checkpoint.
    pub checks: u64,
    /// Number of successful checkpoint handoffs made by this worker.
    pub checkpoints: u64,
    /// Whether a check is executing or waiting for the store's writer lock.
    pub in_flight: bool,
    /// Whether the worker has exited.
    pub stopped: bool,
    /// The error that stopped the worker, if any. Reopen the store when its
    /// underlying checkpoint publication has an uncertain outcome.
    pub last_error: Option<String>,
}

#[derive(Default)]
struct WorkerState {
    stopping: bool,
    requested: bool,
    status: CheckpointWorkerStatus,
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

// A worker owns this lease until its thread exits, even during unwind.
struct WorkerLease(Arc<CheckpointStore>);

impl Drop for WorkerLease {
    fn drop(&mut self) {
        self.0.checkpoint_worker.store(false, Ordering::Release);
    }
}

/// A single optional background checkpoint worker. Dropping it requests stop
/// and joins the worker. A check already performing I/O is allowed to finish,
/// so neither drop nor stop has a bounded-completion guarantee.
pub struct CheckpointWorker {
    control: Arc<Control>,
    thread: Option<JoinHandle<()>>,
}

impl CheckpointWorker {
    /// Requests an additional check; concurrent requests can be coalesced.
    /// The configured minimum suffix threshold still applies.
    pub fn request_check(&self) -> io::Result<()> {
        let mut state = self.control.lock();
        if state.stopping || state.status.stopped {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "checkpoint worker has stopped",
            ));
        }
        state.requested = true;
        self.control.changed.notify_one();
        Ok(())
    }

    /// Returns a consistent snapshot of the worker's status.
    pub fn status(&self) -> CheckpointWorkerStatus {
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
                .map_err(|_| io::Error::other("checkpoint worker panicked"))?;
        }
        Ok(())
    }

    /// Requests stop, waits for the worker to exit, and returns its final
    /// status. Inspect `last_error` for a failure reported by the worker.
    pub fn stop(mut self) -> io::Result<CheckpointWorkerStatus> {
        self.shutdown()?;
        Ok(self.status())
    }
}

impl Drop for CheckpointWorker {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl CheckpointStore {
    /// Starts one optional worker for a managed store with automatic
    /// checkpoints already enabled. It checks at `interval` and after explicit
    /// requests, and rolls over only when the suffix has at least
    /// `minimum_suffix_bytes`. Intervals must be 1 ms to 24 hours and the
    /// threshold must fit the store's configured suffix limit.
    ///
    /// The worker holds an `Arc` to the store and stops on the first check
    /// error. Disabling automatic checkpoints makes checks skip until it is
    /// enabled again. Stop the worker before trying to consume the store for
    /// compaction. This does not reclaim live-log bytes, guarantee bounded
    /// latency or memory, persist the worker across reopen, or implement a
    /// project sleep/wake service.
    pub fn start_checkpoint_worker(
        self: &Arc<Self>,
        interval: Duration,
        minimum_suffix_bytes: u64,
    ) -> io::Result<CheckpointWorker> {
        if !(MIN_INTERVAL..=MAX_INTERVAL).contains(&interval)
            || minimum_suffix_bytes == 0
            || minimum_suffix_bytes > self.max_tail_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid checkpoint interval or suffix threshold",
            ));
        }
        if !self.read()?.automatic {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "background checkpoints require automatic checkpoints to be enabled",
            ));
        }
        if self
            .checkpoint_worker
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a checkpoint worker is already active",
            ));
        }
        let lease = WorkerLease(Arc::clone(self));
        let control = Arc::new(Control::default());
        let thread_control = Arc::clone(&control);
        let thread = thread::Builder::new()
            .name("checkpoint-worker".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    run(&lease.0, &thread_control, interval, minimum_suffix_bytes)
                }));
                if result.is_err() {
                    let mut state = thread_control.lock();
                    state.status.in_flight = false;
                    state.status.stopped = true;
                    state.status.last_error = Some(
                        "checkpoint worker panicked; reopen the store before relying on it".into(),
                    );
                    thread_control.changed.notify_all();
                }
                drop(lease);
            })?;
        Ok(CheckpointWorker {
            control,
            thread: Some(thread),
        })
    }

    fn background_checkpoint(&self, minimum_suffix_bytes: u64) -> io::Result<bool> {
        let mut state = self.write()?;
        if !state.automatic || state.view.tail_bytes < minimum_suffix_bytes {
            return Ok(false);
        }
        self.automatic_rollover(&mut state)?;
        Ok(true)
    }
}

fn run(store: &CheckpointStore, control: &Control, interval: Duration, minimum_suffix_bytes: u64) {
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
        let result = store.background_checkpoint(minimum_suffix_bytes);
        let mut state = control.lock();
        state.status.in_flight = false;
        state.status.checks = state.status.checks.saturating_add(1);
        match result {
            Ok(true) => state.status.checkpoints = state.status.checkpoints.saturating_add(1),
            Ok(false) => {}
            Err(error) => {
                state.status.last_error = Some(error.to_string());
                state.status.stopped = true;
                control.changed.notify_all();
                return;
            }
        }
        control.changed.notify_all();
    }
}

#[cfg(test)]
mod tests;
