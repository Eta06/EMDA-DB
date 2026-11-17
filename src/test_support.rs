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

#[cfg(target_os = "linux")]
pub(crate) mod fork_lock {
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::{fs::MetadataExt, net::UnixStream};
    use std::path::Path;

    unsafe extern "C" {
        fn fork() -> i32;
        fn read(fd: i32, buf: *mut std::ffi::c_void, count: usize) -> isize;
        fn _exit(status: i32) -> !;
        fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
        fn kill(pid: i32, signal: i32) -> i32;
    }
    pub struct Child(i32, Option<UnixStream>);
    impl Child {
        pub fn spawn() -> Self {
            let (parent, child) = UnixStream::pair().unwrap();
            let (pid, parent) = std::thread::spawn(move || {
                let pid = unsafe { fork() };
                if pid == 0 {
                    drop(parent);
                    let mut byte = 0u8;
                    unsafe {
                        read(child.as_raw_fd(), (&mut byte as *mut u8).cast(), 1);
                        _exit(0)
                    }
                }
                assert!(pid > 0);
                drop(child);
                (pid, parent)
            })
            .join()
            .unwrap();
            Self(pid, Some(parent))
        }
        pub fn assert_inode(&self, path: &Path) {
            let lock = std::fs::metadata(path.join("store.lock")).unwrap();
            assert!(
                std::fs::read_dir(format!("/proc/{}/fd", self.0))
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter_map(|e| std::fs::metadata(e.path()).ok())
                    .any(|m| m.dev() == lock.dev() && m.ino() == lock.ino())
            );
        }
    }
    impl Drop for Child {
        fn drop(&mut self) {
            if let Some(mut stream) = self.1.take() {
                let _ = stream.write_all(&[1]);
            }
            let mut status = 0;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut killed = false;
            let result = loop {
                let result = unsafe { waitpid(self.0, &mut status, 1) }; // WNOHANG
                if result == self.0
                    || (result == -1
                        && std::io::Error::last_os_error().kind()
                            != std::io::ErrorKind::Interrupted)
                {
                    break result;
                }
                if std::time::Instant::now() >= deadline {
                    unsafe {
                        kill(self.0, 9);
                    } // SIGKILL
                    killed = true;
                    break unsafe { waitpid(self.0, &mut status, 0) };
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            if !std::thread::panicking() {
                assert_eq!(result, self.0);
                assert!(!killed, "forked child timed out");
                assert_eq!(status, 0);
            }
        }
    }
    pub fn isolated(name: &str, env: &str) -> bool {
        if std::env::var_os(env).is_some() {
            return false;
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(env, "1")
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("isolated regression timed out: {name}");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        assert!(status.success(), "isolated test failed: {status}");
        true
    }
    pub fn check(path: &Path, child: Child, released: bool) {
        child.assert_inode(path);
        if released {
            drop(crate::Store::open(path).unwrap());
        } else {
            let error = match crate::Store::open(path) {
                Ok(_) => panic!("reopen succeeded"),
                Err(e) => e,
            };
            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        }
        drop(child);
    }
}
