use super::*;
use crate::DocumentDatabase;
use crate::store::compound_partial_index_fault_tests::{prepare, update, verify};
use crate::test_support::retry_lock;

#[test]
fn compound_partial_indexes_and_raw_key_recover_together_after_v3_metadata_faults() {
    for fault in [
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
        let store = store(root.path(), fault, Arc::clone(&armed), entered, resume);
        prepare(&DocumentDatabase::new(&store));
        armed.store(true, Ordering::Release);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            update(&DocumentDatabase::new(&store))
        }));
        assert!(!matches!(outcome, Ok(Ok(()))));
        assert!(store.get(b"receipt").is_err());
        drop(store);
        let reopened = retry_lock(|| Store::open(root.path())).unwrap();
        verify(&DocumentDatabase::new(&reopened));
    }
}
