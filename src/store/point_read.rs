//! Conservative two-point reads without copying the complete key index.
use super::*;

struct PointReservation<'a>(&'a Store);

impl Drop for PointReservation<'_> {
    fn drop(&mut self) {
        self.0
            .snapshot_budget
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .release(0, 0);
    }
}

impl Store {
    // The callback reads only captured immutable log locations. It is private to
    // the engine and must not perform external side effects: like a transaction,
    // its result is discarded when an intervening mutation causes a conflict.
    pub(crate) fn read_two_consistent<T>(
        &self,
        first: &[u8],
        second: &[u8],
        action: impl FnOnce(
            Option<Vec<u8>>,
            &mut dyn FnMut() -> io::Result<Option<Vec<u8>>>,
        ) -> io::Result<T>,
    ) -> io::Result<T> {
        record::validate_key(first)?;
        record::validate_key(second)?;
        self.reap_expired_snapshots()?;
        let inner = self.read()?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        self.snapshot_budget
            .lock()
            .map_err(|_| io::Error::other("snapshot budget poisoned"))?
            .reserve(0, 0)?;
        let _reservation = PointReservation(self);
        // Both offsets and their generation come from one index state. Log
        // locations are immutable while the borrowed store remains open.
        let first = inner.index.get(first).copied();
        let second = inner.index.get(second).copied();
        let generation = inner.generation;
        drop(inner);
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        let first = first.map(|location| self.log.read(location)).transpose()?;
        let mut read_second = || {
            if self.commits.failed() {
                return Err(uncertain_error());
            }
            second.map(|location| self.log.read(location)).transpose()
        };
        let result = action(first, &mut read_second)?;
        // Match a conservative read-only transaction: wait for in-flight
        // writes, reject uncertain outcomes, and reject any changed generation.
        let _writer = self
            .writes
            .write()
            .map_err(|_| io::Error::other("write barrier poisoned"))?;
        if self.commits.failed() {
            return Err(uncertain_error());
        }
        if self.read()?.generation != generation {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "transaction conflict: observed state changed since snapshot",
            ));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::fault_tests::{Fault, open_fault_store};
    use std::sync::Arc;

    #[test]
    fn returns_two_captured_values_and_conflicts_on_any_intervening_write() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        store.put(b"first", b"one").unwrap();
        store.put(b"second", b"two").unwrap();
        let result = store
            .read_two_consistent(b"first", b"second", |first, second| {
                assert_eq!(first, Some(b"one".to_vec()));
                assert_eq!(second()?, Some(b"two".to_vec()));
                Ok(17)
            })
            .unwrap();
        assert_eq!(result, 17);
        let error = store
            .read_two_consistent(b"first", b"second", |first, second| {
                assert_eq!(first, Some(b"one".to_vec()));
                store.put(b"unrelated", b"write")?;
                assert_eq!(second()?, Some(b"two".to_vec()));
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn a_failed_action_wins_over_later_validation() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let error = store
            .read_two_consistent(b"first", b"second", |_, _| {
                store.put(b"other", b"write")?;
                Err::<(), _>(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "callback failed",
                ))
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn an_uncertain_write_during_the_read_prevents_a_successful_result() {
        for injected in [
            Fault::PartialAppend(0),
            Fault::PartialAppend(5),
            Fault::SyncBefore,
            Fault::SyncAfter,
            Fault::PanicAfterAppend(5),
        ] {
            let root = tempfile::tempdir().unwrap();
            let fault = Arc::new(Mutex::new(None));
            let store = open_fault_store(&root, Arc::clone(&fault)).unwrap();
            store.put(b"first", b"one").unwrap();
            store.put(b"second", b"two").unwrap();
            let error = store
                .read_two_consistent(b"first", b"second", |_, second| {
                    *fault.lock().unwrap() = Some(injected);
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        store.put(b"unknown", b"value")
                    }));
                    assert!(!matches!(outcome, Ok(Ok(()))));
                    assert!(second().is_err());
                    Ok(())
                })
                .unwrap_err();
            assert!(error.to_string().contains("uncertain"));
            assert!(store.get(b"first").is_err());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn v3_witness_failures_during_a_read_never_return_success() {
        use crate::store::metadata_tests::{Fault, store as fault_store};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::mpsc;
        for injected in [
            Fault::Read,
            Fault::WriteBefore,
            Fault::WriteAfter,
            Fault::SyncBefore,
            Fault::SyncTimeout,
            Fault::SyncAfter,
            Fault::Panic,
        ] {
            let root = tempfile::tempdir().unwrap();
            let armed = Arc::new(AtomicBool::new(false));
            let (entered, _) = mpsc::channel();
            let (_, resume) = mpsc::channel();
            let store = fault_store(root.path(), injected, Arc::clone(&armed), entered, resume);
            store.put(b"first", b"one").unwrap();
            store.put(b"second", b"two").unwrap();
            let error = store
                .read_two_consistent(b"first", b"second", |_, second| {
                    armed.store(true, Ordering::Release);
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        store.put(b"unknown", b"value")
                    }));
                    armed.store(false, Ordering::Release);
                    assert!(!matches!(outcome, Ok(Ok(()))));
                    assert!(second().is_err());
                    Ok(())
                })
                .unwrap_err();
            assert!(error.to_string().contains("uncertain"));
            drop(store);
            let reopened = crate::test_support::retry_lock(|| Store::open(root.path())).unwrap();
            assert_eq!(reopened.get(b"first").unwrap(), Some(b"one".to_vec()));
            assert_eq!(reopened.get(b"second").unwrap(), Some(b"two".to_vec()));
        }
    }

    #[test]
    fn point_reads_share_snapshot_admission_and_release_on_unwind() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let snapshots = (0..15)
            .map(|_| store.snapshot(1, 100).unwrap())
            .collect::<Vec<_>>();
        store
            .read_two_consistent(b"first", b"second", |_, _| {
                assert!(store.snapshot(1, 100).is_err());
                assert!(
                    store
                        .read_two_consistent(b"first", b"second", |_, _| Ok(()))
                        .is_err()
                );
                Ok(())
            })
            .unwrap();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = store.read_two_consistent::<()>(b"first", b"second", |_, _| panic!("injected"));
        }));
        assert!(unwind.is_err());
        drop(store.snapshot(1, 100).unwrap());
        drop(snapshots);
    }
}
