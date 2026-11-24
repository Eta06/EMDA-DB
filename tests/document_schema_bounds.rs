use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, FieldRule, FieldType,
    IndexDefinition, IndexQuery, Store,
};
use serde_json::{Value, json};
use std::io;

mod support;
use support::retry_lock;

fn schema() -> CollectionSchema {
    CollectionSchema {
        fields: vec![
            FieldRule {
                path: vec!["score".into()],
                field_type: FieldType::IntegerRange { min: -3, max: 5 },
                required: true,
                nullable: false,
            },
            FieldRule {
                path: vec!["profile".into(), "label".into()],
                field_type: FieldType::StringBytes { min: 2, max: 4 },
                required: false,
                nullable: true,
            },
        ],
    }
}

fn start_migration(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    for invalid in [
        FieldType::IntegerRange { min: 1, max: 0 },
        FieldType::StringBytes { min: 2, max: 1 },
        FieldType::StringBytes {
            min: 0,
            max: 1_048_577,
        },
    ] {
        let mut invalid_schema = schema();
        invalid_schema.fields[0].field_type = invalid;
        assert_eq!(
            db.start_schema_migration("c", invalid_schema)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(db.schema_migration("c").unwrap().is_none());
    }
    db.create_index(
        "c",
        IndexDefinition {
            name: "score".into(),
            fields: vec![vec!["score".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "a", &json!({"score":-3, "profile":{"label":"é"}}))
        .unwrap();
    db.insert("c", "b", &json!({"score":6})).unwrap();
    db.start_schema_migration("c", schema()).unwrap();
    assert_eq!(db.advance_schema_migration("c", 1).unwrap().processed, 1);
    assert_eq!(
        db.advance_schema_migration("c", 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

fn finish_and_check(db: &DocumentDatabase<'_>) {
    let pending = db.schema_migration("c").unwrap().unwrap();
    assert_eq!(pending.schema, schema());
    assert_eq!(pending.after.as_deref(), Some("a"));
    assert_eq!(
        db.insert("c", "0", &json!({"score":6})).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    db.replace(
        "c",
        "b",
        &json!({"score":5,"profile":{"label":"🙂"}}),
        false,
    )
    .unwrap();
    assert_eq!(db.advance_schema_migration("c", 1).unwrap().processed, 1);
    assert!(db.advance_schema_migration("c", 1).unwrap().complete);

    for invalid in [
        json!({"score":-4}),
        json!({"score":6}),
        json!({"score":0.0}),
        json!({"score":u64::MAX}),
        json!({"score":null}),
        json!({"profile":{"label":null}}),
        json!({"score":0,"profile":{"label":"x"}}),
        json!({"score":0,"profile":{"label":"é🙂"}}),
        json!({"score":0,"profile":{"label":false}}),
    ] {
        assert_eq!(
            db.insert("c", "invalid", &invalid).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    db.insert("c", "null", &json!({"score":0,"profile":{"label":null}}))
        .unwrap();
    db.insert("c", "missing", &json!({"score":1})).unwrap();

    let failed = db.transaction(|tx| {
        tx.put_raw(b"before-update", b"must roll back")?;
        tx.update_if(
            "c",
            "b",
            &[],
            &[DocumentUpdate::Increment {
                path: vec!["score".into()],
                amount: 1,
            }],
            false,
        )
    });
    assert_eq!(failed.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"before-update")).unwrap(),
        None
    );
    assert_eq!(db.get("c", "b").unwrap().unwrap()["score"], json!(5));
    assert_eq!(
        db.query_index("c", "score", &IndexQuery::default())
            .unwrap()
            .documents
            .len(),
        4
    );

    let archive = db.export_collection("c").unwrap();
    let parsed: Value = serde_json::from_slice(&archive).unwrap();
    assert_eq!(parsed["version"], json!(2));
    assert_eq!(parsed["schema"], serde_json::to_value(schema()).unwrap());
    assert_eq!(
        parsed["schema"]["fields"][0]["field_type"],
        json!({"IntegerRange":{"min":-3,"max":5}})
    );
    assert_eq!(
        parsed["schema"]["fields"][1]["field_type"],
        json!({"StringBytes":{"min":2,"max":4}})
    );
    for invalid in [
        {
            let mut wrong = parsed.clone();
            wrong["version"] = json!(1);
            wrong
        },
        {
            let mut wrong = parsed.clone();
            wrong["schema"]["fields"][0]["field_type"]["IntegerRange"]["ignored"] = json!(true);
            wrong
        },
        {
            let mut wrong = parsed.clone();
            wrong["schema"]["fields"][0]["field_type"]["IntegerRange"]["min"] = json!(9);
            wrong
        },
        {
            let mut wrong = parsed.clone();
            let last = wrong["documents"]
                .as_array_mut()
                .unwrap()
                .last_mut()
                .unwrap();
            last["document"]["score"] = json!(6);
            wrong
        },
    ] {
        assert!(
            db.import_collection("copy", &serde_json::to_vec(&invalid).unwrap())
                .is_err()
        );
        assert_eq!(
            db.get("copy", "a").unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }
    db.import_collection("copy", &archive).unwrap();
    assert_eq!(db.export_collection("copy").unwrap(), archive);
}

fn assert_installed(db: &DocumentDatabase<'_>) {
    for name in ["c", "copy"] {
        assert_eq!(db.get(name, "a").unwrap().unwrap()["score"], json!(-3));
        assert_eq!(
            db.insert(name, "invalid", &json!({"score":6}))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        db.verify_collection(name).unwrap();
    }
}

#[test]
fn bounded_schema_migrates_and_round_trips_across_reopens() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("ordinary");
    let open = || retry_lock(|| Store::open(&path)).unwrap();
    start_migration(&DocumentDatabase::new(&open()));
    finish_and_check(&DocumentDatabase::new(&open()));
    assert_installed(&DocumentDatabase::new(&open()));

    let path = root.path().join("checkpoint");
    let base = root.path().join("base");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&base).unwrap();
    drop(initial);
    let open = || retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
    start_migration(&DocumentDatabase::with_checkpoint_store(&open()));
    finish_and_check(&DocumentDatabase::with_checkpoint_store(&open()));
    assert_installed(&DocumentDatabase::with_checkpoint_store(&open()));
}
