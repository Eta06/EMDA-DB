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

pub(crate) fn prepare(db: &DocumentDatabase<'_>) {
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

pub(crate) fn step(db: &DocumentDatabase<'_>) -> io::Result<()> {
    db.transaction(|tx| {
        assert_eq!(tx.advance_collection_drop("c", 1)?.removed, 1);
        tx.put_raw(b"receipt", b"committed")
    })
}

pub(crate) fn prepare_second(db: &DocumentDatabase<'_>) {
    prepare(db);
    step(db).unwrap();
}

pub(crate) fn step_second(db: &DocumentDatabase<'_>) -> io::Result<()> {
    db.transaction(|tx| {
        assert_eq!(tx.advance_collection_drop("c", 1)?.removed, 1);
        tx.put_raw(b"receipt", b"second")
    })
}

pub(crate) fn prepare_final(db: &DocumentDatabase<'_>) {
    prepare(db);
    for _ in 0..6 {
        let step = db.advance_collection_drop("c", 1).unwrap();
        assert_eq!((step.removed, step.complete), (1, false));
    }
}

pub(crate) fn step_final(db: &DocumentDatabase<'_>) -> io::Result<()> {
    db.transaction(|tx| {
        let step = tx.advance_collection_drop("c", 1)?;
        assert_eq!((step.removed, step.complete), (0, true));
        tx.put_raw(b"receipt", b"final")
    })
}

pub(crate) fn verify_final(store: &Store) {
    let db = DocumentDatabase::new(store);
    match store.get(b"receipt").unwrap().as_deref() {
        None => {
            let cursor = store
                .get(CURSOR_KEY)
                .unwrap()
                .expect("final cursor must remain");
            assert!(cursor.starts_with(b"\0emda-document\x01U\0\x01c"));
            assert_cursor_state(store, 4, Some(&cursor));
            assert!(db.advance_collection_drop("c", 1).unwrap().complete);
        }
        Some(b"final") => {
            assert!(store.get(METADATA_KEY).unwrap().is_none());
            assert!(store.get(CURSOR_KEY).unwrap().is_none());
        }
        other => panic!("bad final-step receipt {other:?}"),
    }
    assert!(store.get(CURSOR_KEY).unwrap().is_none());
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    assert!(db.get("c", "a").unwrap().is_none());
}

const CURSOR_KEY: &[u8] = b"\0emda-document\x01P\0\x01c";
const METADATA_KEY: &[u8] = b"\0emda-document\x01M\0\x01c";
const CURSOR_A: &[u8] = b"\0emda-document\x01D\0\x01ca";
const CURSOR_B: &[u8] = b"\0emda-document\x01D\0\x01cb";

fn assert_cursor_state(store: &Store, version: u64, expected: Option<&[u8]>) {
    let metadata: serde_json::Value =
        serde_json::from_slice(&store.get(METADATA_KEY).unwrap().unwrap()).unwrap();
    assert_eq!(metadata["version"].as_u64(), Some(version));
    assert_eq!(store.get(CURSOR_KEY).unwrap().as_deref(), expected);
}

pub(crate) fn verify_checkpoint_first(store: &Store) {
    match store.get(b"receipt").unwrap().as_deref() {
        None => assert_cursor_state(store, 3, None),
        Some(b"committed") => assert_cursor_state(store, 4, Some(CURSOR_A)),
        other => panic!("bad first-step receipt {other:?}"),
    }
    verify(&DocumentDatabase::new(store));
}

pub(crate) fn verify_second(store: &Store) {
    let db = DocumentDatabase::new(store);
    assert!(db.collection_drop_in_progress("c").unwrap());
    let receipt = db.transaction(|tx| tx.get_raw(b"receipt")).unwrap();
    let committed = match receipt.as_deref() {
        Some(b"committed") => false,
        Some(b"second") => true,
        other => panic!("bad second-step receipt {other:?}"),
    };
    assert_cursor_state(store, 4, Some(if committed { CURSOR_B } else { CURSOR_A }));
    let mut removed = 0;
    loop {
        let step = db.advance_collection_drop("c", 16).unwrap();
        removed += step.removed;
        if step.complete {
            break;
        }
    }
    assert_eq!(removed, if committed { 4 } else { 5 });
}

pub(crate) fn verify(db: &DocumentDatabase<'_>) {
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
