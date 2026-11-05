//! Opt-in per-open admission for independent key-value read calls.
//!
//! Enabling requires exclusive access to the store, ensuring no earlier reads
//! are untracked. Once enabled, limit and active count share one atomic word,
//! so reconfiguration cannot race admission into an oversubscribed state.
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
    /// Configured maximum, or `None` when admission is disabled.
    pub limit: Option<usize>,
    /// Admitted calls including lock and I/O waiters, or `None` when disabled.
    pub active: Option<usize>,
}

#[derive(Default)]
pub(crate) struct DirectReads(Option<AtomicUsize>);
impl DirectReads {
    fn status(&self) -> DirectReadStatus {
        match &self.0 {
            None => DirectReadStatus {
                limit: None,
                active: None,
            },
            Some(state) => {
                let state = state.load(Ordering::Acquire);
                DirectReadStatus {
                    limit: Some((state >> SHIFT) & MASK),
                    active: Some(state & MASK),
                }
            }
        }
    }
    fn enable(&mut self, limit: usize) -> io::Result<()> {
        if limit > MAX_ACTIVE_DIRECT_READS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct-read admission limit exceeds hard maximum",
            ));
        }
        // Exclusive access ensures no previous guard or read is still active.
        self.0 = Some(AtomicUsize::new(limit << SHIFT));
        Ok(())
    }
    fn set_limit(&self, limit: usize) -> io::Result<()> {
        if limit > MAX_ACTIVE_DIRECT_READS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct-read admission limit exceeds hard maximum",
            ));
        }
        let state = self.0.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "enable direct-read admission with exclusive store access first",
            )
        })?;
        let mut current = state.load(Ordering::Acquire);
        loop {
            let active = current & MASK;
            if active > limit {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "direct-read limit is below active admissions",
                ));
            }
            let next = (limit << SHIFT) | active;
            match state.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(()),
                Err(changed) => current = changed,
            }
        }
    }
    pub(crate) fn acquire(&self) -> io::Result<Option<DirectReadGuard<'_>>> {
        let Some(state) = &self.0 else {
            return Ok(None);
        };
        let mut current = state.load(Ordering::Acquire);
        loop {
            let active = current & MASK;
            let limit = (current >> SHIFT) & MASK;
            if active >= limit {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "active direct-read admission limit exceeded",
                ));
            }
            match state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Some(DirectReadGuard(state))),
                Err(changed) => current = changed,
            }
        }
    }
}
pub(crate) struct DirectReadGuard<'a>(&'a AtomicUsize);
impl Drop for DirectReadGuard<'_> {
    fn drop(&mut self) {
        let previous = self.0.fetch_sub(1, Ordering::Release);
        debug_assert!(previous & MASK > 0, "direct-read admission underflow");
    }
}

macro_rules! direct_read_api {
    ($store:ty) => {
        impl $store {
            /// Reports the optional independent key-value read admission state.
            /// Disabled admission does not track reads; transactions, snapshots,
            /// document calls, maintenance and other stores are always excluded.
            pub fn direct_read_status(&self) -> DirectReadStatus {
                self.direct_reads.status()
            }
            /// Enables independent `get`, `get_many` and `scan` admission before
            /// sharing the store. Exclusive access ensures no untracked read is
            /// active. Zero rejects these calls. The cap resets on reopen, but is
            /// preserved by supported in-process compaction handoffs.
            pub fn enable_direct_read_limit(&mut self, limit: usize) -> io::Result<()> {
                self.direct_reads.enable(limit)
            }
            /// Reconfigures enabled admission; disabled admission must first be
            /// enabled with exclusive access. A reduction below currently active
            /// calls returns `WouldBlock` and leaves the old limit in place.
            /// Saturation can mask a previous uncertain write. This counter is
            /// neither a project quota nor a memory or completion-time bound.
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
        let mut budget = DirectReads::default();
        assert_eq!(
            budget.status(),
            DirectReadStatus {
                limit: None,
                active: None
            }
        );
        assert_eq!(
            budget
                .set_limit(MAX_ACTIVE_DIRECT_READS + 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            budget.set_limit(1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        budget.enable(1).unwrap();
        let guard = budget.acquire().unwrap();
        assert_eq!(
            budget.status(),
            DirectReadStatus {
                limit: Some(1),
                active: Some(1)
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
        assert_eq!(budget.status().active, Some(0));
        budget.set_limit(0).unwrap();
        assert_eq!(
            budget.acquire().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn simultaneous_acquisitions_never_exceed_the_limit() {
        let mut initial = DirectReads::default();
        initial.enable(10).unwrap();
        let budget = Arc::new(initial);
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
                limit: Some(10),
                active: Some(0)
            }
        );
    }

    #[test]
    fn packed_counter_does_not_carry_into_limit_at_the_hard_boundary() {
        let mut budget = DirectReads::default();
        budget.enable(MAX_ACTIVE_DIRECT_READS).unwrap();
        let guards = (0..MAX_ACTIVE_DIRECT_READS)
            .map(|_| budget.acquire().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            budget.status(),
            DirectReadStatus {
                limit: Some(MAX_ACTIVE_DIRECT_READS),
                active: Some(MAX_ACTIVE_DIRECT_READS)
            }
        );
        assert_eq!(
            budget.acquire().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(guards);
        assert_eq!(budget.status().active, Some(0));
    }

    #[test]
    fn concurrent_reconfiguration_never_publishes_an_oversubscribed_state() {
        let mut initial = DirectReads::default();
        initial.enable(4).unwrap();
        let budget = Arc::new(initial);
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
                    assert!(status.active.unwrap() <= status.limit.unwrap());
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
                assert!(status.active.unwrap() <= status.limit.unwrap());
                thread::yield_now();
            }
        });
        start.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        setter.join().unwrap();
        assert_eq!(budget.status().active, Some(0));
    }
}
