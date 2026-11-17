//! Helpers for tests that reopen private fixtures while other tests fork.
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

/// Report lock-holder evidence without changing the result or retrying.
pub(crate) fn diagnose_lock<T>(path: &Path, phase: &str, result: io::Result<T>) -> io::Result<T> {
    if let Err(error) = &result {
        report_lock_error(path, phase, error);
    }
    result
}

pub(crate) fn report_lock_error(path: &Path, phase: &str, error: &io::Error) {
    if error.kind() == io::ErrorKind::WouldBlock {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(path.join("store.lock"));
        eprintln!(
            "lock diagnostic phase={phase} pid={} thread={:?} path={} error={:?}",
            std::process::id(),
            std::thread::current().id(),
            path.display(),
            error
        );
        if let Ok(metadata) = metadata {
            eprintln!(
                "lock diagnostic device={} inode={}",
                metadata.dev(),
                metadata.ino()
            );
            #[cfg(target_os = "linux")]
            if let Ok(locks) = std::fs::read_to_string("/proc/locks") {
                for line in locks.lines().filter(|line| {
                    line.split_whitespace().any(|field| {
                        field
                            .rsplit(':')
                            .next()
                            .and_then(|inode| inode.parse::<u64>().ok())
                            == Some(metadata.ino())
                            && field.contains(':')
                    })
                }) {
                    eprintln!("lock diagnostic proc-lock={line}");
                }
            }
        }
    }
}

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
    fn diagnostic_returns_the_original_lock_error_without_retrying() {
        let root = tempfile::tempdir().unwrap();
        let error = io::Error::from_raw_os_error(11);
        let result: io::Result<()> = diagnose_lock(root.path(), "test", Err(error));
        assert_eq!(result.unwrap_err().raw_os_error(), Some(11));
    }

    #[test]
    fn diagnostic_preserves_actual_store_lock_exclusion() {
        let root = tempfile::tempdir().unwrap();
        let owner = crate::Store::open(root.path()).unwrap();
        let error =
            match diagnose_lock(root.path(), "held fixture", crate::Store::open(root.path())) {
                Ok(_) => panic!("diagnostic acquired an already held store lock"),
                Err(error) => error,
            };
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(owner.get(b"missing").unwrap(), None);
    }

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
