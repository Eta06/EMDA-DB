//! Per-open log-length reservations held across admission and synchronization.
use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct LogQuota {
    max: u64,
    // Includes the initial log length and both committed and in-flight records.
    used: Mutex<u64>,
    failed: AtomicBool,
}

impl LogQuota {
    pub(super) fn new(max: u64, length: u64) -> io::Result<Self> {
        if length > max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "existing log exceeds configured limit",
            ));
        }
        Ok(Self {
            max,
            used: Mutex::new(length),
            failed: AtomicBool::new(false),
        })
    }

    pub(super) fn limit(&self) -> u64 {
        self.max
    }

    pub(super) fn reserve(&self, bytes: u64) -> io::Result<LogReservation<'_>> {
        if self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "log quota accounting failed; reopen the store",
            ));
        }
        let mut used = self
            .used
            .lock()
            .map_err(|_| io::Error::other("log quota accounting poisoned; reopen the store"))?;
        if self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "log quota accounting failed; reopen the store",
            ));
        }
        *used = used
            .checked_add(bytes)
            .filter(|next| *next <= self.max)
            .ok_or_else(|| io::Error::other("configured log size limit would be exceeded"))?;
        Ok(LogReservation {
            quota: self,
            bytes,
            active: true,
        })
    }
}

pub(super) struct LogReservation<'a> {
    quota: &'a LogQuota,
    bytes: u64,
    active: bool,
}

impl LogReservation<'_> {
    pub(super) fn committed(&mut self) {
        self.active = false;
    }
}

impl Drop for LogReservation<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut used = self
            .quota
            .used
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(next) = used.checked_sub(self.bytes) {
            *used = next;
        } else {
            // Never admit more records if an internal accounting invariant fails.
            self.quota.failed.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn reservations_count_pending_and_committed_bytes_and_release_on_unwind() {
        let quota = LogQuota::new(100, 40).unwrap();
        let first = quota.reserve(30).unwrap();
        let second = quota.reserve(30).unwrap();
        assert!(quota.reserve(1).is_err());
        drop(first);
        let mut committed = quota.reserve(30).unwrap();
        committed.committed();
        drop(committed);
        drop(second);
        assert!(quota.reserve(31).is_err());
        let outcome = std::panic::catch_unwind(|| {
            let _pending = quota.reserve(30).unwrap();
            panic!("injected preparation panic");
        });
        assert!(outcome.is_err());
        drop(quota.reserve(30).unwrap());
    }

    #[test]
    fn simultaneous_reservations_cannot_exceed_capacity() {
        let quota = Arc::new(LogQuota::new(10, 0).unwrap());
        let started = Barrier::new(33);
        let finished = Barrier::new(33);
        let winners = thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..32 {
                let (quota, started, finished) = (&quota, &started, &finished);
                workers.push(scope.spawn(move || {
                    started.wait();
                    let reservation = quota.reserve(1);
                    let won = reservation.is_ok();
                    finished.wait();
                    drop(reservation);
                    won
                }));
            }
            started.wait();
            finished.wait();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .filter(|won| *won)
                .count()
        });
        assert_eq!(winners, 10);
        drop(quota.reserve(10).unwrap());
    }

    #[test]
    fn overflow_poison_and_inconsistent_accounting_fail_closed() {
        let quota = LogQuota::new(u64::MAX, u64::MAX - 1).unwrap();
        assert!(quota.reserve(2).is_err());
        drop(quota.reserve(1).unwrap());

        let inconsistent = LogQuota::new(10, 0).unwrap();
        let reservation = inconsistent.reserve(5).unwrap();
        *inconsistent.used.lock().unwrap() = 0;
        drop(reservation);
        assert!(inconsistent.reserve(1).is_err());

        let poisoned = LogQuota::new(10, 0).unwrap();
        let outcome = std::panic::catch_unwind(|| {
            let _guard = poisoned.used.lock().unwrap();
            panic!("injected quota lock panic");
        });
        assert!(outcome.is_err());
        assert!(poisoned.reserve(1).is_err());
    }
}
