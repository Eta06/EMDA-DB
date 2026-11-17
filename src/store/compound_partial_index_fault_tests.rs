//! Torn commits that move a compound unique partial index, leave another
//! partial index, and write a raw key must recover as one outcome.
use super::{
    Store,
    fault_tests::{Fault, open_versioned_fault_store},
};
use crate::test_support::retry_lock;
use crate::{
    CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue, PartialFilter,
};
use serde_json::json;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

const ITEM: &str = "item";
const OTHER: &str = "other";

pub(crate) fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "place".into(),
            fields: vec![vec!["region".into()], vec!["sku".into()]],
            unique: true,
            partial: Some(PartialFilter::Equals {
                path: vec!["status".into()],
                value: IndexValue::String("open".into()),
            }),
        },
    )
    .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "shown".into(),
            fields: vec![vec!["region".into()]],
            unique: false,
            partial: Some(PartialFilter::Exists {
                path: vec!["visible".into()],
                exists: true,
            }),
        },
    )
    .unwrap();
    db.insert(
        "c",
        OTHER,
        &json!({"status":"open","region":"north","sku":7,"visible":true}),
    )
    .unwrap();
    db.insert(
        "c",
        ITEM,
        &json!({"status":"open","region":"east","sku":1,"visible":true}),
    )
    .unwrap();
}

pub(crate) fn update(db: &DocumentDatabase<'_>) -> io::Result<()> {
    db.transaction(|tx| {
        tx.replace(
            "c",
            ITEM,
            &json!({"status":"open","region":"west","sku":2}),
            false,
        )?;
        tx.put_raw(b"receipt", b"moved")
    })
}

pub(crate) fn verify(db: &DocumentDatabase<'_>) -> bool {
    db.transaction(|tx| -> io::Result<bool> {
        let item = tx.get("c", ITEM)?.expect("updated document remains");
        let other = tx.get("c", OTHER)?.expect("unchanged document remains");
        assert_eq!(
            other,
            json!({"status":"open","region":"north","sku":7,"visible":true})
        );
        let committed = item == json!({"status":"open","region":"west","sku":2});
        if !committed {
            assert_eq!(
                item,
                json!({"status":"open","region":"east","sku":1,"visible":true})
            );
        }
        assert_eq!(
            tx.get_raw(b"receipt")?,
            committed.then(|| b"moved".to_vec())
        );
        let integrity = tx.verify_collection("c")?;
        assert_eq!(
            (integrity.documents, integrity.indexes, integrity.index_keys),
            (2, 2, if committed { 5 } else { 6 })
        );
        assert_eq!(
            indexed(tx, "place", vec![region("east"), IndexValue::Integer(1)], 2)?,
            if committed {
                vec![]
            } else {
                vec![ITEM.to_string()]
            }
        );
        assert_eq!(
            indexed(tx, "place", vec![region("west"), IndexValue::Integer(2)], 2)?,
            if committed {
                vec![ITEM.to_string()]
            } else {
                vec![]
            }
        );
        assert_eq!(
            indexed(
                tx,
                "place",
                vec![region("north"), IndexValue::Integer(7)],
                2
            )?,
            vec![OTHER.to_string()]
        );
        assert_eq!(
            indexed(tx, "shown", vec![region("east")], 1)?,
            if committed {
                vec![]
            } else {
                vec![ITEM.to_string()]
            }
        );
        assert_eq!(
            indexed(tx, "shown", vec![region("west")], 1)?,
            Vec::<String>::new()
        );
        assert_eq!(
            indexed(tx, "shown", vec![region("north")], 1)?,
            vec![OTHER.to_string()]
        );
        Ok(committed)
    })
    .unwrap()
}

fn region(value: &str) -> IndexValue {
    IndexValue::String(value.into())
}

fn indexed(
    tx: &crate::DocumentTransaction<'_, '_>,
    name: &str,
    equal: Vec<IndexValue>,
    equality_fields: usize,
) -> io::Result<Vec<String>> {
    let page = tx.query_index(
        "c",
        name,
        &IndexQuery {
            equal,
            ..Default::default()
        },
    )?;
    assert!(page.plan.partial);
    assert_eq!(page.plan.index, name);
    assert_eq!(page.plan.equality_fields, equality_fields);
    Ok(page.documents.into_iter().map(|entry| entry.id).collect())
}

fn witnessed_header(path: &Path) -> &'static str {
    let bytes = fs::read(path.join("records.log")).unwrap();
    match &bytes[..8] {
        b"EMDALOG2" => "EMDALOG2",
        b"EMDALOG3" => "EMDALOG3",
        other => panic!("compound partial fault fixture is not a witnessed log: {other:?}"),
    }
}

fn transaction_length() -> usize {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let header = witnessed_header(root.path());
    let db = DocumentDatabase::new(&store);
    prepare(&db);
    let log = root.path().join("records.log");
    let before = fs::metadata(&log).unwrap().len();
    update(&db).unwrap();
    let length = usize::try_from(fs::metadata(&log).unwrap().len() - before).unwrap();
    assert!(
        verify(&db),
        "successful update was not recovered as committed"
    );
    eprintln!(
        "{header} compound partial index append cut points: {}",
        length + 1
    );
    length
}

#[test]
fn compound_partial_indexes_and_raw_key_recover_together_at_every_append_cut() {
    let length = transaction_length();
    assert!(
        length > 64,
        "compound partial update did not append a record"
    );
    let mut faults = (0..=length).map(Fault::PartialAppend).collect::<Vec<_>>();
    faults.extend([
        Fault::PanicAfterAppend(0),
        Fault::PanicAfterAppend(length / 2),
        Fault::PanicAfterAppend(length),
    ]);
    let mut committed = false;
    let mut absent = false;
    let mut rejected = false;
    for fault in faults {
        let root = tempfile::tempdir().unwrap();
        let armed = Arc::new(Mutex::new(None));
        let store = open_versioned_fault_store(root.path(), Arc::clone(&armed)).unwrap();
        witnessed_header(root.path());
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
            Ok(reopened) => {
                if verify(&DocumentDatabase::new(&reopened)) {
                    committed = true;
                } else {
                    absent = true;
                }
            }
            Err(error) => {
                assert!(
                    matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                    ),
                    "unexpected recovery failure: {error}"
                );
                rejected = true;
            }
        }
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    assert!(
        committed && absent && rejected,
        "append cuts did not cover committed ({committed}), absent ({absent}), and rejected ({rejected}) recovery"
    );
}
