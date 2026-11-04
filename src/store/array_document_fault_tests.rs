//! Byte-by-byte V3 interrupted append tests for multi-model array transactions.
use super::{
    Store,
    fault_tests::{Fault, open_versioned_fault_store},
};
use crate::DocumentDatabase;
use crate::document::array_fault_model::{prepare, update, verify};
use crate::test_support::retry_lock;
use std::fs;
use std::sync::{Arc, Mutex};

fn transaction_length() -> usize {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    prepare(&db);
    let path = root.path().join("records.log");
    let before = fs::metadata(&path).unwrap().len();
    update(&db).unwrap();
    usize::try_from(fs::metadata(path).unwrap().len() - before).unwrap()
}

#[test]
fn array_index_and_raw_write_recover_together_for_every_v3_append_cut() {
    let length = transaction_length();
    assert!(length > 30);
    eprintln!("V3 array transaction append cut points: {}", length + 1);
    let mut faults = (0..=length).map(Fault::PartialAppend).collect::<Vec<_>>();
    faults.extend([
        Fault::PanicAfterAppend(0),
        Fault::PanicAfterAppend(length / 2),
        Fault::PanicAfterAppend(length),
    ]);
    for fault in faults {
        let root = tempfile::tempdir().unwrap();
        let armed = Arc::new(Mutex::new(None));
        let store = open_versioned_fault_store(root.path(), Arc::clone(&armed)).unwrap();
        assert_eq!(
            &fs::read(root.path().join("records.log")).unwrap()[..8],
            b"EMDALOG3"
        );
        prepare(&DocumentDatabase::new(&store));
        *armed.lock().unwrap() = Some(fault);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            update(&DocumentDatabase::new(&store))
        }));
        assert!(!matches!(outcome, Ok(Ok(()))));
        assert!(store.get(b"receipt").is_err());
        drop(store);
        let path = root.path().join("records.log");
        let bytes = fs::read(&path).unwrap();
        match retry_lock(|| Store::open(root.path())) {
            Ok(reopened) => verify(&DocumentDatabase::new(&reopened)),
            Err(error) => assert!(matches!(
                error.kind(),
                std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
            )),
        }
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}
