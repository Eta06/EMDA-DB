//! Per-open admission for independent key-value read calls.
//!
//! The limit and active count share one atomic word, so acquisition cannot
//! race a limit reduction into an oversubscribed state. These counters do not
//! protect storage data or account for memory used by callers or other APIs.
use crate::{CheckpointStore, Store};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Maximum concurrently admitted independent reads on one open store.
pub const MAX_ACTIVE_DIRECT_READS: usize = u16::MAX as usize;
const SHIFT: u32 = 16;
const MASK: usize = u16::MAX as usize;

/// Process-local admission state for independent key-value reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DirectReadStatus {
    /// Maximum concurrently admitted independent read calls.
    pub limit: usize,
    /// Calls holding admission, including lock and I/O waiters.
    pub active: usize,
}

pub(crate) struct DirectReads(AtomicUsize);
impl Default for DirectReads {
    fn default() -> Self {
        Self(AtomicUsize::new(MAX_ACTIVE_DIRECT_READS << SHIFT))
    }
}
impl DirectReads {
    fn status(&self) -> DirectReadStatus {
        let state = self.0.load(Ordering::Acquire);
        DirectReadStatus {
            limit: (state >> SHIFT) & MASK,
            active: state & MASK,
        }
    }
    fn set_limit(&self, limit: usize) -> io::Result<()> {
        if limit > MAX_ACTIVE_DIRECT_READS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct-read admission limit exceeds hard maximum",
            ));
        }
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let active = current & MASK;
            if active > limit {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "direct-read limit is below active admissions",
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
    pub(crate) fn acquire(&self) -> io::Result<DirectReadGuard<'_>> {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let active = current & MASK;
            let limit = (current >> SHIFT) & MASK;
            if active >= limit {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "active direct-read admission limit exceeded",
                ));
            }
            match self.0.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(DirectReadGuard(self)),
                Err(changed) => current = changed,
            }
        }
    }
}
pub(crate) struct DirectReadGuard<'a>(&'a DirectReads);
impl Drop for DirectReadGuard<'_> {
    fn drop(&mut self) {
        let previous = self.0.0.fetch_sub(1, Ordering::Release);
        debug_assert!(previous & MASK > 0, "direct-read admission underflow");
    }
}

macro_rules! direct_read_api {
    ($store:ty) => {
        impl $store {
            /// Reports independent `get`, `get_many` and `scan` admission.
            /// Transactions, snapshots, document calls, maintenance, other
            /// stores and caller buffers are outside this per-open count.
            pub fn direct_read_status(&self) -> DirectReadStatus {
                self.direct_reads.status()
            }
            /// Sets the per-open maximum for concurrently admitted independent
            /// key-value `get`, `get_many` and `scan` calls. Zero rejects new
            /// calls; lowering below active calls returns `WouldBlock` without
            /// changing the limit. Saturation may mask a previously unhealthy
            /// store. In-process compaction handoffs preserve the limit; reopen
            /// resets it. This is neither a project quota nor a memory bound.
            pub fn set_active_direct_read_limit(&self, limit: usize) -> io::Result<()> {
                self.direct_reads.set_limit(limit)
            }
        }
    };
}
direct_read_api!(Store);
direct_read_api!(CheckpointStore);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn limits_and_raii_release_are_atomic_on_failure_and_unwind() {
        let budget = DirectReads::default();
        assert_eq!(
            budget.status(),
            DirectReadStatus {
                limit: MAX_ACTIVE_DIRECT_READS,
                active: 0
            }
        );
        assert_eq!(
            budget
                .set_limit(MAX_ACTIVE_DIRECT_READS + 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        budget.set_limit(1).unwrap();
        let guard = budget.acquire().unwrap();
        assert_eq!(
            budget.status(),
            DirectReadStatus {
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
            panic!("injected direct-read panic");
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
        let budget = Arc::new(DirectReads::default());
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
            DirectReadStatus {
                limit: 10,
                active: 0
            }
        );
    }

    #[test]
    fn packed_counter_does_not_carry_into_limit_at_the_hard_boundary() {
        let budget = DirectReads::default();
        let guards = (0..MAX_ACTIVE_DIRECT_READS)
            .map(|_| budget.acquire().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            budget.status(),
            DirectReadStatus {
                limit: MAX_ACTIVE_DIRECT_READS,
                active: MAX_ACTIVE_DIRECT_READS
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
        let budget = Arc::new(DirectReads::default());
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
