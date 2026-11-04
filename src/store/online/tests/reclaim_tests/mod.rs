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
