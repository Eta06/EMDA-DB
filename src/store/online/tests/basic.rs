use super::*;
use crate::{
    BatchOperation, CollectionSchema, DocumentDatabase, FieldRule, FieldType, IndexDefinition,
    IndexJobStatus, IndexQuery,
};
use serde_json::json;

#[test]
fn pending_document_jobs_continue_through_compaction() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store
        .with_store(|s| {
            let db = DocumentDatabase::new(s);
            db.create_collection("c", CollectionSchema::default())?;
            for (id, n) in [("a", 1), ("b", 2)] {
                db.insert("c", id, &json!({"n":n}))?;
            }
            db.start_index_build(
                "c",
                IndexDefinition {
                    name: "n".into(),
                    fields: vec![vec!["n".into()]],
                    unique: true,
                    partial: None,
                },
            )?;
            db.advance_index_build("c", "n", 1)?;
            db.start_schema_migration(
                "c",
                CollectionSchema {
                    fields: vec![FieldRule {
                        path: vec!["n".into()],
                        field_type: FieldType::Integer,
                        required: true,
                        nullable: false,
                    }],
                },
            )?;
            db.advance_schema_migration("c", 1)?;
            Ok(())
        })
        .unwrap();
    store
        .compact_and_replace(&previous, &OnlineCompactionOptions::default())
        .unwrap();
    store.with_store(|s| {
        let db = DocumentDatabase::new(s);
        assert!(matches!(&db.index_jobs("c")?[0], IndexJobStatus::Building { after: Some(id), .. } if id == "a"));
        assert_eq!(db.schema_migration("c")?.unwrap().after.as_deref(), Some("a"));
        db.insert("c", "c", &json!({"n":3}))?;
        assert!(db.insert("c", "invalid", &json!({})).is_err());
        while !db.advance_schema_migration("c", 1)?.complete {}
        while !db.advance_index_build("c", "n", 1)?.complete {}
        assert_eq!(db.query_index("c", "n", &IndexQuery::default())?.documents.len(), 3);
        db.verify_collection("c")?;
        Ok(())
    }).unwrap();
}

#[test]
fn configured_limits_and_unsafe_paths_fail_before_exchange() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store =
        OnlineStore::open_with_resource_limits(&source, Some(1_000_000), Some(2), Some(2)).unwrap();
    store
        .with_store(|s| {
            s.put(b"a", b"one")?;
            s.put(b"b", b"two")
        })
        .unwrap();
    let bad = OnlineCompactionOptions {
        max_snapshot_keys: 1,
        ..Default::default()
    };
    assert!(store.compact_and_replace(&previous, &bad).is_err());
    let bad = OnlineCompactionOptions {
        max_temporary_log_bytes: 1,
        ..Default::default()
    };
    assert!(store.compact_and_replace(&previous, &bad).is_err());
    let bad = OnlineCompactionOptions {
        catchup_rounds: 17,
        ..Default::default()
    };
    assert_eq!(
        store
            .compact_and_replace(&previous, &bad)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    fs::write(source.join("user-file"), b"keep").unwrap();
    assert_eq!(
        store
            .compact_and_replace(&previous, &Default::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    fs::remove_file(source.join("user-file")).unwrap();
    fs::create_dir(root.path().join("other")).unwrap();
    assert_eq!(
        store
            .compact_and_replace(
                root.path().join("other").join("previous"),
                &Default::default()
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    fs::create_dir(&previous).unwrap();
    assert_eq!(
        store
            .compact_and_replace(&previous, &Default::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    fs::remove_dir(&previous).unwrap();
    store
        .compact_and_replace(&previous, &Default::default())
        .unwrap();
    assert!(store.with_store(|s| s.put(b"c", b"limit")).is_err());
    assert_eq!(
        store.with_store(|s| s.get(b"a")).unwrap(),
        Some(b"one".to_vec())
    );
}

#[test]
fn fixed_log_ranges_exclude_later_appends_and_reject_torn_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.put(b"a", b"one").unwrap();
    let first = copy::boundary(&store).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"b",
                value: b"two",
            },
            BatchOperation::Delete { key: b"a" },
        ])
        .unwrap();
    let second = copy::boundary(&store).unwrap();
    let mut initial = Vec::new();
    store
        .log
        .visit_synchronized_range(0, first, |key, location| {
            initial.push((key, location.is_some()));
            Ok(())
        })
        .unwrap();
    assert_eq!(initial, vec![(b"a".to_vec(), true)]);
    let mut tail = Vec::new();
    store
        .log
        .visit_synchronized_range(first, second, |key, location| {
            tail.push((key, location.is_some()));
            Ok(())
        })
        .unwrap();
    assert_eq!(tail, vec![(b"b".to_vec(), true), (b"a".to_vec(), false)]);
    assert!(
        store
            .log
            .visit_synchronized_range(first, second - 1, |_, _| Ok(()))
            .is_err()
    );
    assert!(
        store
            .log
            .visit_synchronized_range(second, first, |_, _| Ok(()))
            .is_err()
    );
    assert!(
        store
            .log
            .visit_synchronized_range(first, second + 1, |_, _| Ok(()))
            .is_err()
    );
}

#[test]
fn large_unbatchable_values_are_copied_and_reopened() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    let value = vec![0x5a; crate::record::MAX_VALUE];
    store.with_store(|s| s.put(b"large", &value)).unwrap();
    store
        .compact_and_replace(&previous, &Default::default())
        .unwrap();
    assert_eq!(
        store.with_store(|s| s.get(b"large")).unwrap(),
        Some(value.clone())
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"large")
            .unwrap(),
        Some(value)
    );
}

#[test]
fn historical_peak_and_lock_ownership_are_enforced() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store
        .with_store(|s| {
            s.put(b"deleted", b"value")?;
            s.delete(b"deleted")
        })
        .unwrap();
    let options = OnlineCompactionOptions {
        max_snapshot_keys: 0,
        ..Default::default()
    };
    assert!(
        store
            .compact_and_replace(&previous, &options)
            .unwrap_err()
            .to_string()
            .contains("historical")
    );
    store.with_store(|s| s.put(b"present", b"value")).unwrap();
    store
        .compact_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == copy::Point::BeforeExchange {
                    assert!(Store::open(&source).is_err());
                    assert!(Store::open(&previous).is_err());
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
        .unwrap();
    assert_eq!(
        store.with_store(|s| s.get(b"present")).unwrap(),
        Some(b"value".to_vec())
    );
}
