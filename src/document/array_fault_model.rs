//! Shared invariants for interrupted document-array and scalar commits.
use crate::{
    CollectionIntegrity, CollectionSchema, DocumentDatabase, DocumentEntry, DocumentUpdate,
    IndexDefinition, IndexQuery, IndexValue, PartialFilter,
};
use serde_json::json;
use std::io;

pub(crate) fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "i".into(),
            fields: vec![vec!["x".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "with_moved".into(),
            fields: vec![vec!["x".into()]],
            unique: false,
            partial: Some(PartialFilter::Exists {
                path: vec!["moved".into()],
                exists: true,
            }),
        },
    )
    .unwrap();
    db.insert("c", "id", &json!({"x":1, "items":[1], "extra":[7, 8]}))
        .unwrap();
}

pub(crate) fn update(db: &DocumentDatabase<'_>) -> io::Result<()> {
    db.transaction(|tx| {
        tx.update_if(
            "c",
            "id",
            &[],
            &[
                DocumentUpdate::ArrayPush {
                    path: vec!["items".into()],
                    value: json!(2),
                },
                DocumentUpdate::ArrayTruncate {
                    path: vec!["extra".into()],
                    max_len: 1,
                },
                DocumentUpdate::Multiply {
                    path: vec!["x".into()],
                    factor: 2,
                },
                DocumentUpdate::SetIfMissing {
                    path: vec!["source".into()],
                    value: json!("ready"),
                },
                DocumentUpdate::Rename {
                    from: vec!["source".into()],
                    to: vec!["moved".into()],
                },
                DocumentUpdate::Min {
                    path: vec!["x".into()],
                    value: IndexValue::Integer(2),
                },
                DocumentUpdate::Max {
                    path: vec!["x".into()],
                    value: IndexValue::Integer(2),
                },
            ],
            false,
        )?;
        tx.put_raw(b"receipt", b"new")
    })
}

pub(crate) fn verify(db: &DocumentDatabase<'_>) {
    let document = db.get("c", "id").unwrap().unwrap();
    let version = document["x"].as_i64().unwrap();
    assert!(version == 1 || version == 2);
    assert_eq!(
        document["items"],
        if version == 1 {
            json!([1])
        } else {
            json!([1, 2])
        }
    );
    assert_eq!(
        document["extra"],
        if version == 1 {
            json!([7, 8])
        } else {
            json!([7])
        }
    );
    assert!(document.get("source").is_none());
    if version == 2 {
        assert_eq!(document.get("moved"), Some(&json!("ready")));
    } else {
        assert!(document.get("moved").is_none());
    }
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
        (version == 2).then(|| b"new".to_vec())
    );
    assert_eq!(
        db.verify_collection("c").unwrap(),
        CollectionIntegrity {
            documents: 1,
            indexes: 2,
            index_keys: if version == 2 { 3 } else { 2 },
        }
    );
    let present = indexed(db, "i", version, false);
    assert_eq!(present.len(), 1);
    assert_eq!(present[0].id, "id");
    assert_eq!(present[0].document, document);
    assert!(indexed(db, "i", if version == 1 { 2 } else { 1 }, false).is_empty());
    let shown = indexed(db, "with_moved", 2, true);
    if version == 2 {
        assert_eq!(shown.len(), 1);
        assert_eq!(shown[0].id, "id");
        assert_eq!(shown[0].document, document);
    } else {
        assert!(shown.is_empty());
    }
    assert!(indexed(db, "with_moved", 1, true).is_empty());
}

fn indexed(db: &DocumentDatabase<'_>, name: &str, value: i64, partial: bool) -> Vec<DocumentEntry> {
    let page = db
        .query_index(
            "c",
            name,
            &IndexQuery {
                equal: vec![IndexValue::Integer(value)],
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(page.plan.index, name);
    assert_eq!(page.plan.equality_fields, 1);
    assert!(!page.plan.has_range);
    assert_eq!(page.plan.partial, partial);
    page.documents
}

#[test]
fn committed_truncate_keeps_push_and_publishes_partial_index() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = crate::test_support::retry_lock(|| crate::Store::open(&path)).unwrap();
        if checkpoint_mode {
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let store = crate::test_support::retry_lock(|| {
                crate::CheckpointStore::open(&path, &checkpoint, &digest)
            })
            .unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            prepare(&db);
            update(&db).unwrap();
            verify(&db);
            drop(store);
        } else {
            let db = DocumentDatabase::new(&store);
            prepare(&db);
            update(&db).unwrap();
            verify(&db);
            drop(store);
        }
        let reopened = crate::test_support::retry_lock(|| crate::Store::open(&path)).unwrap();
        verify(&DocumentDatabase::new(&reopened));
    }
}
