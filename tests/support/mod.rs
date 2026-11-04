//! Helpers for isolated fixtures reopened while other tests fork child processes.
use std::io;
use std::time::{Duration, Instant};

// A child forked by another test can briefly retain an inherited descriptor
// after its owner closes it. Retry only that documented transient conflict;
// deliberate lock-exclusion assertions must use the API directly.
pub(crate) fn retry_lock<T>(mut open: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match open() {
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            result => return result,
        }
    }
}
