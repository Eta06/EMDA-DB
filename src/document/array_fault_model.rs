//! Shared invariants for interrupted document-array commits.
use crate::{
    CollectionSchema, DocumentDatabase, DocumentUpdate, IndexDefinition, IndexQuery, IndexValue,
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
    db.insert("c", "id", &json!({"x":1, "items":[1]})).unwrap();
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
                DocumentUpdate::Set {
                    path: vec!["x".into()],
                    value: json!(2),
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
        db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
        (version == 2).then(|| b"new".to_vec())
    );
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
    let rows = db
        .query_index(
            "c",
            "i",
            &IndexQuery {
                equal: vec![IndexValue::Integer(version)],
                ..Default::default()
            },
        )
        .unwrap()
        .documents;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "id");
}
