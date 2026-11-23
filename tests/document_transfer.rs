use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, FieldRule, FieldType, IndexDefinition,
    IndexQuery, MAX_COLLECTION_ARCHIVE_BYTES, MAX_COLLECTION_ARCHIVE_DOCUMENTS, PartialFilter,
    Store, TransactionFailure,
};
use serde_json::{Value, json};
use std::io;

mod support;
use support::retry_lock;

fn schema() -> CollectionSchema {
    CollectionSchema {
        fields: vec![FieldRule {
            path: vec!["email".into()],
            field_type: FieldType::String,
            required: true,
            nullable: false,
        }],
    }
}

fn index() -> IndexDefinition {
    IndexDefinition {
        name: "active_email".into(),
        fields: vec![vec!["email".into()]],
        unique: true,
        partial: Some(PartialFilter::Exists {
            path: vec!["active".into()],
            exists: true,
        }),
    }
}

fn seed(db: &DocumentDatabase<'_>) -> Vec<u8> {
    db.create_collection("source", schema()).unwrap();
    db.create_index("source", index()).unwrap();
    db.insert("source", "z", &json!({"email": "a@example.test"}))
        .unwrap();
    db.insert(
        "source",
        "a",
        &json!({"active": true, "email": "a@example.test"}),
    )
    .unwrap();
    db.export_collection("source").unwrap()
}

#[test]
fn logical_document_archive_round_trips_through_a_checkpoint_store() {
    let root = tempfile::tempdir().unwrap();
    let source = Store::open(root.path().join("source")).unwrap();
    let source = DocumentDatabase::new(&source);
    let original = seed(&source);

    let path = root.path().join("destination");
    let checkpoint = root.path().join("checkpoint");
    let empty = Store::open(&path).unwrap();
    let digest = empty.checkpoint_to(&checkpoint).unwrap();
    drop(empty);
    let open = || retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    {
        let store = open();
        let destination = DocumentDatabase::with_checkpoint_store(&store);
        destination
            .import_collection("destination", &original)
            .unwrap();
        assert!(matches!(
            destination.import_collection("destination", &original),
            Err(TransactionFailure::Callback(error)) if error.kind() == io::ErrorKind::AlreadyExists
        ));
    }
    let store = open();
    let destination = DocumentDatabase::with_checkpoint_store(&store);
    let exported = destination.export_collection("destination").unwrap();
    assert_eq!(exported, original);
    assert_eq!(destination.indexes("destination").unwrap(), [index()]);
    let entries = destination
        .query_index("destination", "active_email", &IndexQuery::default())
        .unwrap();
    assert_eq!(entries.documents.len(), 1);
    assert_eq!(entries.documents[0].id, "a");
    assert_eq!(
        destination
            .insert(
                "destination",
                "duplicate",
                &json!({"email": "a@example.test", "active": true})
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(
        destination
            .insert("destination", "incompatible", &json!({"email": 42}))
            .is_err()
    );
    source.import_collection("roundtrip", &exported).unwrap();
    assert_eq!(source.export_collection("roundtrip").unwrap(), original);
}

#[test]
fn invalid_document_archives_never_publish_a_partial_collection() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    let original = seed(&db);
    let parsed: Value = serde_json::from_slice(&original).unwrap();
    let mut invalid_archives = [
        ("version", json!(2)),
        ("unknown", json!(true)),
        (
            "documents",
            json!(vec![
                parsed["documents"][0].clone();
                MAX_COLLECTION_ARCHIVE_DOCUMENTS + 1
            ]),
        ),
        ("indexes", json!(vec![parsed["indexes"][0].clone(); 17])),
        (
            "documents",
            json!([parsed["documents"][1], parsed["documents"][0]]),
        ),
    ]
    .into_iter()
    .map(|(name, value)| {
        let mut invalid = parsed.clone();
        invalid[name] = value;
        invalid
    })
    .collect::<Vec<_>>();
    let mut duplicate_index_value = parsed;
    duplicate_index_value["documents"][1]["document"]["active"] = json!(true);
    invalid_archives.push(duplicate_index_value);
    for wrong in invalid_archives {
        let encoded = serde_json::to_vec(&wrong).unwrap();
        let result = db.transaction(|tx| {
            tx.put_raw(b"receipt", b"do not publish")?;
            assert!(tx.import_collection("invalid", &encoded).is_err());
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(
            db.export_collection("invalid").unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(store.get(b"receipt").unwrap(), None);
    }
    let orphan = b"\0emda-document\x01D\0\x06orphanid";
    store.put(orphan, b"{}").unwrap();
    assert!(matches!(
        db.import_collection("orphan", &original),
        Err(TransactionFailure::Callback(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    assert_eq!(
        store.get(orphan).unwrap().as_deref(),
        Some(b"{}".as_slice())
    );

    let mut padded = original;
    padded.resize(MAX_COLLECTION_ARCHIVE_BYTES, b' ');
    db.import_collection("exact", &padded).unwrap();
    padded.push(b' ');
    assert!(matches!(
        db.import_collection("too_large", &padded),
        Err(TransactionFailure::Callback(error)) if error.kind() == io::ErrorKind::InvalidInput
    ));

    db.start_schema_migration("source", schema()).unwrap();
    assert_eq!(
        db.export_collection("source").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn version_one_fixture_stays_readable_and_export_enforces_the_row_cap() {
    const V1: &[u8] = br#"{"version":1,"schema":{"fields":[]},"indexes":[],"documents":[{"id":"a","document":{"x":1}}]}"#;
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.import_collection("fixture", V1).unwrap();
    assert_eq!(db.export_collection("fixture").unwrap(), V1);

    db.create_collection("bounded", CollectionSchema::default())
        .unwrap();
    db.transaction(|tx| {
        for id in 0..MAX_COLLECTION_ARCHIVE_DOCUMENTS {
            tx.insert("bounded", &format!("{id:03}"), &json!({}))?;
        }
        Ok(())
    })
    .unwrap();
    let full = db.export_collection("bounded").unwrap();
    db.import_collection("copy", &full).unwrap();
    assert_eq!(db.export_collection("copy").unwrap(), full);
    db.insert("bounded", "more", &json!({})).unwrap();
    assert_eq!(
        db.export_collection("bounded").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}
