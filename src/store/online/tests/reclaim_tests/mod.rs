use super::*;
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;

mod basic;
mod concurrency;
mod failure;
mod recovery;
mod scoped_cleanup;

// A private one-stage fixture can temporarily be skipped when an unrelated
// process-test child inherits its advisory lock before exec. Retry that
// no-progress result only; persistent skips still fail after the deadline.
fn retry_fixture_cleanup(store: &OnlineStore) -> io::Result<crate::StagingCleanup> {
    retry_fixture_cleanup_with_options(store, &Default::default())
}

fn retry_fixture_cleanup_with_options(
    store: &OnlineStore,
    options: &OnlineCompactionOptions,
) -> io::Result<crate::StagingCleanup> {
    retry_lock(|| {
        let report = store.cleanup_retired_staging(options)?;
        if report.removed_payloads == 0 && report.removed_markers == 0 && report.skipped == 1 {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        Ok(report)
    })
}

fn wait_for(
    worker: &OnlineReclamationWorker,
    predicate: impl Fn(&OnlineReclamationWorkerStatus) -> bool,
) -> OnlineReclamationWorkerStatus {
    let timeout = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = worker.status();
        if predicate(&status) {
            return status;
        }
        assert!(
            std::time::Instant::now() < timeout,
            "worker did not make progress: {status:?}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

// Once the fixture releases its lock, unrelated fork/exec tests may briefly
// retain an inherited descriptor. Request further checks until the intended
// outcome occurs; a permanent skip or unexpected worker failure still fails.
fn request_checks_until(
    worker: &OnlineReclamationWorker,
    predicate: impl Fn(&OnlineReclamationWorkerStatus) -> bool,
) -> OnlineReclamationWorkerStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let before = worker.status().checks;
        worker.request_check().unwrap();
        let status = wait_for(worker, |status| status.checks > before || status.stopped);
        if predicate(&status) {
            return status;
        }
        assert!(
            !status.stopped,
            "worker stopped before expected result: {status:?}"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "worker never reached expected result: {status:?}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}
