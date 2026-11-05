//! Partial appends of a collection-drop step and raw receipt remain atomic.
use super::{
    Store,
    fault_tests::{Fault, open_versioned_fault_store},
};
use crate::test_support::retry_lock;
use crate::{CollectionSchema, DocumentDatabase, IndexDefinition};
use serde_json::json;
use std::{
    fs, io,
    sync::{Arc, Mutex},
};

pub(super) fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "idx".into(),
            fields: vec![vec!["n".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "a", &json!({"n":1})).unwrap();
    db.insert("c", "b", &json!({"n":2})).unwrap();
    db.start_collection_drop("c").unwrap();
}

pub(super) fn step(db: &DocumentDatabase<'_>) -> io::Result<()> {
    db.transaction(|tx| {
        assert_eq!(tx.advance_collection_drop("c", 1)?.removed, 1);
        tx.put_raw(b"receipt", b"committed")
    })
}

pub(super) fn verify(db: &DocumentDatabase<'_>) {
    assert!(db.collection_drop_in_progress("c").unwrap());
    assert_eq!(
        db.get("c", "a").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let receipt = db.transaction(|tx| tx.get_raw(b"receipt")).unwrap();
    let committed = match receipt.as_deref() {
        None => false,
        Some(b"committed") => true,
        other => panic!("bad receipt {other:?}"),
    };
    let mut removed = 0;
    loop {
        let step = db.advance_collection_drop("c", 16).unwrap();
        removed += step.removed;
        if step.complete {
            break;
        }
    }
    assert_eq!(removed, if committed { 5 } else { 6 });
}

#[test]
fn partial_drop_step_append_recovers_all_or_none_with_its_raw_receipt() {
    let probe = tempfile::tempdir().unwrap();
    let store = Store::open(probe.path()).unwrap();
    prepare(&DocumentDatabase::new(&store));
    let path = probe.path().join("records.log");
    let before = fs::metadata(&path).unwrap().len();
    step(&DocumentDatabase::new(&store)).unwrap();
    let length = usize::try_from(fs::metadata(&path).unwrap().len() - before).unwrap();
    eprintln!("collection-drop batch append cut points: {}", length + 1);
    for fault in (0..=length).map(Fault::PartialAppend).chain([
        Fault::PanicAfterAppend(0),
        Fault::PanicAfterAppend(length / 2),
        Fault::PanicAfterAppend(length),
    ]) {
        let root = tempfile::tempdir().unwrap();
        let armed = Arc::new(Mutex::new(None));
        let store = open_versioned_fault_store(root.path(), Arc::clone(&armed)).unwrap();
        prepare(&DocumentDatabase::new(&store));
        *armed.lock().unwrap() = Some(fault);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            step(&DocumentDatabase::new(&store))
        }));
        assert!(!matches!(result, Ok(Ok(()))));
        drop(store);
        let path = root.path().join("records.log");
        let bytes = fs::read(&path).unwrap();
        match retry_lock(|| Store::open(root.path())) {
            Ok(store) => verify(&DocumentDatabase::new(&store)),
            Err(error) => {
                assert!(matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ));
                assert_eq!(fs::read(path).unwrap(), bytes);
            }
        }
    }
}
