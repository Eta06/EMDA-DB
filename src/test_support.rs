//! Helpers for tests that reopen private fixtures while other tests fork.
use std::io;
use std::time::{Duration, Instant};

// A child forked by another test can temporarily retain an inherited lock
// after its owner drops. Retry only that documented transient conflict; tests
// which intentionally assert lock exclusion must call the API directly.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_transient_busy_without_masking_the_next_recovery_error() {
        let mut attempts = 0;
        let result: io::Result<()> = retry_lock(|| {
            attempts += 1;
            match attempts {
                1 => Err(io::Error::from(io::ErrorKind::WouldBlock)),
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, "corrupt log")),
            }
        });
        assert_eq!(attempts, 2);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
