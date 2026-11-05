//! Per-open admission for independent, non-transactional write calls.
//!
//! Limit and active count occupy separate 16-bit fields in one atomic word.
//! Acquisition and reconfiguration update both fields with compare-exchange,
//! so neither can publish a count above the limit. Only a successful acquire
//! constructs a non-cloneable guard; its drop releases exactly one admission.
//! The storage locks, not this counter, protect database data.
use crate::{CheckpointStore, Store};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Hard maximum admitted independent write calls on one open store.
/// Transaction callbacks have a separate active-transaction limit.
pub const MAX_ACTIVE_DIRECT_WRITES: usize = u16::MAX as usize;
const SHIFT: u32 = 16;
const MASK: usize = u16::MAX as usize;

/// Process-local admission state for independent writes on one open store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DirectWriteStatus {
    /// Current maximum number of admitted independent write calls.
    pub limit: usize,
    /// Calls holding admission, including lock waiters; release precedes return.
    pub active: usize,
}

pub(crate) struct DirectWrites(AtomicUsize);

impl Default for DirectWrites {
    fn default() -> Self {
        Self(AtomicUsize::new(MAX_ACTIVE_DIRECT_WRITES << SHIFT))
    }
}

impl DirectWrites {
    fn status(&self) -> DirectWriteStatus {
        let state = self.0.load(Ordering::Acquire);
        DirectWriteStatus {
            limit: (state >> SHIFT) & MASK,
            active: state & MASK,
        }
    }

    fn set_limit(&self, limit: usize) -> io::Result<()> {
        if limit > MAX_ACTIVE_DIRECT_WRITES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct-write admission limit exceeds hard maximum",
            ));
        }
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let active = current & MASK;
            if active > limit {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "direct-write limit is below active admissions",
                ));
            }
            let next = (limit << SHIFT) | active;
            match self
                .0
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(changed) => current = changed,
            }
        }
    }

    pub(crate) fn acquire(&self) -> io::Result<DirectWriteGuard<'_>> {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let active = current & MASK;
            let limit = (current >> SHIFT) & MASK;
            if active >= limit {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "active direct-write admission limit exceeded",
                ));
            }
            match self.0.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(DirectWriteGuard(self)),
                Err(changed) => current = changed,
            }
        }
    }
}

pub(crate) struct DirectWriteGuard<'a>(&'a DirectWrites);
impl Drop for DirectWriteGuard<'_> {
    fn drop(&mut self) {
        let previous = self.0.0.fetch_sub(1, Ordering::Release);
        debug_assert!(previous & MASK > 0, "direct-write admission underflow");
    }
}

macro_rules! direct_write_api {
    ($store:ty) => {
        impl $store {
            /// Reports the current per-open independent-write admission limit and usage.
            /// These counts exclude transactions, reads, maintenance, caller-owned
            /// buffers and other stores; they are not a project or memory quota.
            pub fn direct_write_status(&self) -> DirectWriteStatus {
                self.direct_writes.status()
            }

            /// Sets the maximum concurrently admitted independent write calls.
            /// Lowering below currently admitted calls returns `WouldBlock`
            /// without changing the limit. Zero rejects new independent writes.
            /// Admission rejection occurs before checking store health: it does
            /// not resolve an earlier uncertain write or prove the store healthy.
            /// Calls already admitted may complete; I/O after submission retains
            /// unknown-outcome semantics. The per-open setting survives supported
            /// in-process compaction handoffs and resets on reopen. Transactions,
            /// reads, maintenance and other stores have separate controls.
            pub fn set_active_direct_write_limit(&self, limit: usize) -> io::Result<()> {
                self.direct_writes.set_limit(limit)
            }
        }
    };
}

direct_write_api!(Store);
direct_write_api!(CheckpointStore);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn limits_and_raii_release_are_atomic_on_failure_and_unwind() {
        let budget = DirectWrites::default();
        assert_eq!(
            budget.status(),
            DirectWriteStatus {
                limit: MAX_ACTIVE_DIRECT_WRITES,
                active: 0
            }
        );
        assert_eq!(
            budget
                .set_limit(MAX_ACTIVE_DIRECT_WRITES + 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        budget.set_limit(1).unwrap();
        let guard = budget.acquire().unwrap();
        assert_eq!(
            budget.status(),
            DirectWriteStatus {
                limit: 1,
                active: 1
            }
        );
        assert_eq!(
            budget.set_limit(0).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            budget.acquire().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(guard);
        let result = std::panic::catch_unwind(|| {
            let _guard = budget.acquire().unwrap();
            panic!("injected direct-write panic");
        });
        assert!(result.is_err());
        assert_eq!(budget.status().active, 0);
        budget.set_limit(0).unwrap();
        assert_eq!(
            budget.acquire().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn simultaneous_acquisitions_never_exceed_the_limit() {
        let budget = Arc::new(DirectWrites::default());
        budget.set_limit(10).unwrap();
        let started = Arc::new(Barrier::new(33));
        let release = Arc::new(Barrier::new(33));
        let mut workers = Vec::new();
        for _ in 0..32 {
            let budget = Arc::clone(&budget);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            workers.push(thread::spawn(move || {
                started.wait();
                let guard = budget.acquire();
                let admitted = guard.is_ok();
                release.wait();
                drop(guard);
                admitted
            }));
        }
        started.wait();
        release.wait();
        let admitted = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|admitted| *admitted)
            .count();
        assert_eq!(admitted, 10);
        assert_eq!(
            budget.status(),
            DirectWriteStatus {
                limit: 10,
                active: 0
            }
        );
    }

    #[test]
    fn packed_counter_does_not_carry_into_limit_at_the_hard_boundary() {
        let budget = DirectWrites::default();
        let guards = (0..MAX_ACTIVE_DIRECT_WRITES)
            .map(|_| budget.acquire().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            budget.status(),
            DirectWriteStatus {
                limit: MAX_ACTIVE_DIRECT_WRITES,
                active: MAX_ACTIVE_DIRECT_WRITES
            }
        );
        assert_eq!(
            budget.acquire().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(guards);
        assert_eq!(budget.status().active, 0);
    }

    #[test]
    fn concurrent_reconfiguration_never_publishes_an_oversubscribed_state() {
        let budget = Arc::new(DirectWrites::default());
        budget.set_limit(4).unwrap();
        let start = Arc::new(Barrier::new(10));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let budget = Arc::clone(&budget);
            let start = Arc::clone(&start);
            workers.push(thread::spawn(move || {
                start.wait();
                for _ in 0..1_000 {
                    let guard = budget.acquire();
                    if let Err(error) = &guard {
                        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
                    }
                    let status = budget.status();
                    assert!(status.active <= status.limit);
                    drop(guard);
                    thread::yield_now();
                }
            }));
        }
        let changing = Arc::clone(&budget);
        let starting = Arc::clone(&start);
        let setter = thread::spawn(move || {
            starting.wait();
            for iteration in 0..4_000 {
                if let Err(error) = changing.set_limit([0, 1, 4, 8][iteration % 4]) {
                    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
                }
                let status = changing.status();
                assert!(status.active <= status.limit);
                thread::yield_now();
            }
        });
        start.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        setter.join().unwrap();
        assert_eq!(budget.status().active, 0);
    }
}
