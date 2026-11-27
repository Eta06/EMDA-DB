mod support;

use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, Entry, FieldRule, FieldType,
    IndexDefinition, IndexQuery, Store,
};
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use support::retry_lock;

fn index() -> IndexDefinition {
    IndexDefinition {
        name: "i".into(),
        fields: vec![vec!["x".into()]],
        unique: true,
        partial: None,
    }
}

#[test]
fn failed_unique_index_build_leaves_no_partial_metadata_or_entries() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    db.insert("c", "b", &json!({"x":1})).unwrap();
    assert_eq!(
        db.create_index("c", index()).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(db.indexes("c").unwrap().is_empty());
    db.delete("c", "b").unwrap();
    db.create_index("c", index()).unwrap();
    assert_eq!(
        db.query_index("c", "i", &IndexQuery::default())
            .unwrap()
            .documents
            .len(),
        1
    );
}

#[test]
fn corrupted_index_and_metadata_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    let rows = store.scan(None, None, 100).unwrap();
    let index_key = rows
        .iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01I"))
        .unwrap()
        .0
        .clone();
    store.put(&index_key, b"unexpected").unwrap();
    assert_eq!(
        db.query_index("c", "i", &IndexQuery::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let metadata_key = rows
        .iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01M"))
        .unwrap()
        .0
        .clone();
    store.put(&metadata_key, b"{broken").unwrap();
    assert_eq!(
        db.get("c", "a").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn document_bounds_and_unsupported_index_values_fail_before_commit() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    assert_eq!(
        db.insert("c", "a", &json!({"x":[1,2]})).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "a").unwrap(), None);
    let large = json!({"text":"x".repeat(1024*1024+1)});
    assert_eq!(
        db.insert("c", "large", &large).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let mut nested = Value::Null;
    for _ in 0..66 {
        nested = json!({"next":nested});
    }
    assert_eq!(
        db.insert("c", "deep", &nested).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "deep").unwrap(), None);
}

#[test]
fn verification_detects_missing_index_and_rebuild_restores_it_atomically() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index("c", index()).unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    let report = db.verify_collection("c").unwrap();
    assert_eq!(
        (report.documents, report.indexes, report.index_keys),
        (1, 1, 2)
    );
    let key = store
        .scan(None, None, 100)
        .unwrap()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01I"))
        .unwrap()
        .0;
    store.delete(&key).unwrap();
    assert!(
        db.query_index("c", "i", &IndexQuery::default())
            .unwrap()
            .documents
            .is_empty()
    );
    assert_eq!(
        db.verify_collection("c").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    db.rebuild_index("c", "i").unwrap();
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
    assert_eq!(
        db.query_index("c", "i", &IndexQuery::default())
            .unwrap()
            .documents[0]
            .id,
        "a"
    );
}

fn change_last_value_byte(path: &Path, original: u8) -> (File, u64) {
    let log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    // The last single-record value is immediately before its four-byte CRC.
    let offset = log.metadata().unwrap().len() - 5;
    let mut byte = [0];
    log.read_exact_at(&mut byte, offset).unwrap();
    assert_eq!(byte, [original]);
    log.write_all_at(&[original ^ 1], offset).unwrap();
    (log, offset)
}

fn maintenance_admission(
    db: &DocumentDatabase<'_>,
    source: &Path,
    put: impl Fn(&[u8], &[u8]),
    scan: impl Fn() -> Vec<Entry>,
) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
    let overhead = br#"{"x":2,"body":""}"#.len();
    let maximum = json!({"x": 2, "body": "x".repeat(1024 * 1024 - overhead)});
    let encoded_maximum = serde_json::to_vec(&maximum).unwrap();
    assert_eq!(encoded_maximum.len(), 1024 * 1024);
    db.insert("c", "b", &maximum).unwrap();
    db.start_index_build("c", index()).unwrap();
    db.start_schema_migration(
        "c",
        CollectionSchema {
            fields: vec![FieldRule {
                path: vec!["x".into()],
                field_type: FieldType::Integer,
                required: true,
                nullable: false,
            }],
        },
    )
    .unwrap();
    let jobs = db.index_jobs("c").unwrap();
    let migration = db.schema_migration("c").unwrap();
    let document_key = b"\0emda-document\x01D\0\x01cb";
    let mut oversized = encoded_maximum.clone();
    oversized.push(b' ');
    put(document_key, &oversized);
    let (log, offset) = change_last_value_byte(source, b' ');
    let build_error = db.advance_index_build("c", "i", 2).unwrap_err();
    let schema_error = db.advance_schema_migration("c", 2).unwrap_err();
    log.write_all_at(b" ", offset).unwrap();
    for error in [build_error, schema_error] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "stored document exceeds size limit");
    }
    assert_eq!(db.index_jobs("c").unwrap(), jobs);
    assert_eq!(db.schema_migration("c").unwrap(), migration);
    assert!(
        !scan()
            .into_iter()
            .any(|(key, _)| key.starts_with(b"\0emda-document\x01I")
                || key.starts_with(b"\0emda-document\x01U"))
    );

    put(document_key, &encoded_maximum);
    assert_eq!(db.advance_index_build("c", "i", 2).unwrap().processed, 2);
    assert!(db.advance_index_build("c", "i", 2).unwrap().complete);
    assert_eq!(db.advance_schema_migration("c", 2).unwrap().processed, 2);
    assert!(db.advance_schema_migration("c", 2).unwrap().complete);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);

    let (key, correct) = scan()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01U"))
        .unwrap();
    put(&key, b"invalid-owner");
    let (log, offset) = change_last_value_byte(source, b'r');
    let error = db.verify_collection("c").unwrap_err();
    log.write_all_at(b"r", offset).unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "unexpected or incorrect index entry");

    // A same-length owner must still be read and checked.
    assert_eq!(correct.len(), 1);
    put(&key, b"z");
    let error = db.verify_collection("c").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "unexpected or incorrect index entry");
    put(&key, &correct);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 4);

    assert_eq!(
        db.insert("c", "duplicate", &json!({"x":1}))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    for (owner, reason) in [
        (b"".as_slice(), "invalid unique index owner length"),
        (b"\xff", "invalid unique index owner"),
    ] {
        put(&key, owner);
        let error = db.insert("c", "duplicate", &json!({"x":1})).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), reason);
    }
    let oversized_owner = [b'x'; 257];
    put(&key, &oversized_owner);
    let (log, offset) = change_last_value_byte(source, b'x');
    let error = db
        .transaction(|tx| {
            tx.put_raw(b"owner-rollback", b"uncommitted")?;
            tx.insert("c", "duplicate", &json!({"x":1}))?;
            Ok(())
        })
        .unwrap_err();
    let query_error = db
        .query_index("c", "i", &IndexQuery::default())
        .unwrap_err();
    log.write_all_at(b"x", offset).unwrap();
    for error in [error, query_error] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "invalid unique index owner length");
    }
    put(&key, &correct);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"owner-rollback")).unwrap(),
        None
    );
    assert_eq!(db.get("c", "duplicate").unwrap(), None);

    let (entry, value) = scan()
        .into_iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01I"))
        .unwrap();
    assert!(value.is_empty());
    put(&entry, b"x");
    let (log, offset) = change_last_value_byte(source, b'x');
    let error = db.delete("c", "a").unwrap_err();
    log.write_all_at(b"x", offset).unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "document index entry is invalid");
    put(&entry, &value);
    assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":1})));

    let mut next = index();
    next.name = "next".into();
    db.start_index_build("c", next).unwrap();
    let longest_id = "ü".repeat(128);
    assert_eq!(longest_id.len(), 256);
    db.insert("c", &longest_id, &json!({"x":3})).unwrap();
    let jobs = db.index_jobs("c").unwrap();
    let before = scan();
    let (pending_owner, correct_owner) = before
        .iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01U\0\x01c\0\x04next"))
        .unwrap();
    assert_eq!(correct_owner, longest_id.as_bytes());
    put(pending_owner, &oversized_owner);
    let (log, offset) = change_last_value_byte(source, b'x');
    let error = db.advance_index_build("c", "next", 3).unwrap_err();
    log.write_all_at(b"x", offset).unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "invalid unique index owner length");
    put(pending_owner, correct_owner);
    assert_eq!(db.index_jobs("c").unwrap(), jobs);
    assert_eq!(scan(), before);
    assert_eq!(db.advance_index_build("c", "next", 3).unwrap().processed, 3);
    assert!(db.advance_index_build("c", "next", 3).unwrap().complete);
    assert_eq!(db.verify_collection("c").unwrap().index_keys, 12);
}

#[test]
fn maintenance_admits_stored_document_and_index_sizes_on_both_backends() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("ordinary");
    let ordinary = Store::open(&source).unwrap();
    maintenance_admission(
        &DocumentDatabase::new(&ordinary),
        &source,
        |key, value| ordinary.put(key, value).unwrap(),
        || ordinary.scan(None, None, 16).unwrap(),
    );
    let source = root.path().join("checkpoint-source");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&source).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    maintenance_admission(
        &DocumentDatabase::with_checkpoint_store(&store),
        &source,
        |key, value| store.put(key, value).unwrap(),
        || store.scan(None, None, 16).unwrap(),
    );
}

#[test]
fn indexed_range_reuses_verified_checkpoint_block_even_without_block_cache() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base_ckpt = root.path().join("base");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&base_ckpt).unwrap();
    drop(initial);
    let store = retry_lock(|| CheckpointStore::open(&path, &base_ckpt, &digest)).unwrap();
    let db = DocumentDatabase::with_checkpoint_store(&store);
    db.create_collection("items", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "items",
        IndexDefinition {
            name: "by_code".into(),
            fields: vec![vec!["code".into()]],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    for chunk in (0_i64..300).collect::<Vec<_>>().chunks(50) {
        db.transaction(|tx| {
            for &i in chunk {
                tx.insert(
                    "items",
                    &format!("{i:08}"),
                    &json!({"code": i, "label": format!("row-{i:04}")}),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
    store.rollover_managed(root.path().join("rolled")).unwrap();
    // Limit=1 counts every checkpoint block read without caching any block.
    store.set_checkpoint_block_cache_limit(1).unwrap();
    let before = store.checkpoint_memory_status().unwrap().block_cache.misses;
    let page = db
        .query_index(
            "items",
            "by_code",
            &IndexQuery {
                equal: vec![],
                lower: Some(database_engine::IndexBound {
                    value: database_engine::IndexValue::Integer(50),
                    inclusive: true,
                }),
                upper: Some(database_engine::IndexBound {
                    value: database_engine::IndexValue::Integer(70),
                    inclusive: false,
                }),
                after: None,
                limit: 20,
            },
        )
        .unwrap();
    assert_eq!(page.documents.len(), 20);
    let misses = store.checkpoint_memory_status().unwrap().block_cache.misses - before;
    // Metadata + index range + reused document block(s): far fewer than 20
    // unbatched document block reads.
    assert!(
        misses <= 6,
        "batched index-to-document reads should reuse verified blocks even uncached, got {misses} misses"
    );
}

#[test]
fn document_stored_validation_matches_full_validation_on_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    let schema = CollectionSchema {
        fields: vec![
            FieldRule {
                path: vec!["age".into()],
                field_type: FieldType::IntegerRange { min: 0, max: 150 },
                required: true,
                nullable: false,
            },
            FieldRule {
                path: vec!["nested".into(), "tag".into()],
                field_type: FieldType::StringBytes { min: 1, max: 16 },
                required: false,
                nullable: false,
            },
        ],
    };
    db.create_collection("users", schema).unwrap();
    db.insert("users", "ok", &json!({"age": 30})).unwrap();

    // Construct the document prefix from an existing valid key.
    let doc_key = store
        .scan(None, None, 16)
        .unwrap()
        .into_iter()
        .find(|(k, _)| k.starts_with(b"\0emda-document\x01D"))
        .unwrap()
        .0;
    let doc_prefix = &doc_key[..doc_key.len() - 2];
    let make_key = |id: &str| {
        let mut key = doc_prefix.to_vec();
        key.extend_from_slice(id.as_bytes());
        key
    };

    // 1. Non-object JSON values (well under 128 bytes) must fail with InvalidData.
    for (id, raw) in [
        ("arr", b"[1,2,3]".as_slice()),
        ("num", b"42".as_slice()),
        ("str", b"\"hello\"".as_slice()),
    ] {
        store.put(&make_key(id), raw).unwrap();
        assert_eq!(
            db.get("users", id).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        store.delete(&make_key(id)).unwrap();
    }

    // 2. Small (<= 128 B) and medium (> 128 B) schema violations must always fail.
    let small_bad_type = br#"{"age":"nope"}"#;
    let small_missing = br#"{"other":1}"#;
    let small_range = br#"{"age":200}"#;
    let medium_nested_null = format!(
        r#"{{"age":25,"pad":"{}","nested":{{"tag":null}}}}"#,
        "x".repeat(160)
    );
    for (id, raw) in [
        ("bad1", small_bad_type.as_slice()),
        ("bad2", small_missing.as_slice()),
        ("bad3", small_range.as_slice()),
        ("bad4", medium_nested_null.as_bytes()),
    ] {
        store.put(&make_key(id), raw).unwrap();
        assert_eq!(
            db.get("users", id).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            db.transaction(|tx| tx.scan_documents("users", None, 10))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        store.delete(&make_key(id)).unwrap();
    }

    // 3. Exact nesting depth boundaries: depth 64 succeeds; depth 65 (134+ bytes) fails.
    let depth_64_raw = format!(
        r#"{{"age":1,"d":{}{}}}"#,
        "[".repeat(63),
        "0".to_string() + &"]".repeat(63)
    );
    assert!(depth_64_raw.len() > 128);
    store
        .put(&make_key("d64"), depth_64_raw.as_bytes())
        .unwrap();
    assert!(db.get("users", "d64").unwrap().is_some());
    store.delete(&make_key("d64")).unwrap();

    // Minimal depth-65 object with empty schema on a second collection: exact 134 bytes!
    db.create_collection("raw", CollectionSchema::default())
        .unwrap();
    db.insert("raw", "seed", &json!({})).unwrap();
    let raw_key = store
        .scan(None, None, 16)
        .unwrap()
        .into_iter()
        .find(|(k, _)| k.starts_with(b"\0emda-document\x01D") && k.ends_with(b"seed"))
        .unwrap()
        .0;
    let raw_prefix = &raw_key[..raw_key.len() - 4];
    let mut d65_key = raw_prefix.to_vec();
    d65_key.extend_from_slice(b"d65");
    let depth_65_min = format!("{{\"\":{}0{}}}", "[".repeat(64), "]".repeat(64));
    assert_eq!(depth_65_min.len(), 134);
    store.put(&d65_key, depth_65_min.as_bytes()).unwrap();
    assert_eq!(
        db.get("raw", "d65").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        db.transaction(|tx| tx.scan_documents("raw", None, 10))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
}
