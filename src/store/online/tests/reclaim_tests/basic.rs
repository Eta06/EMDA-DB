use super::*;

#[test]
fn reclamation_removes_old_history_and_recovers_live_state() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    store.with_store(|db| db.delete(b"delete")).unwrap();
    let before = fs::metadata(source.join("records.log")).unwrap().len();
    let report = store
        .compact_and_reclaim(&previous, &Default::default())
        .unwrap();
    assert_eq!(report.final_log_bytes, before);
    assert!(report.compacted_log_bytes < before);
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|db| db.get(b"overwritten")).unwrap(),
        Some(b"39".to_vec())
    );
    assert_eq!(store.with_store(|db| db.get(b"delete")).unwrap(), None);
    store.with_store(|db| db.put(b"after", b"handoff")).unwrap();
    drop(store);
    let reopened = retry_lock(|| Store::open(&source)).unwrap();
    assert_eq!(reopened.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
    assert_eq!(reopened.get(b"after").unwrap(), Some(b"handoff".to_vec()));
    assert_eq!(reopened.get(b"delete").unwrap(), None);
}

#[test]
fn reclamation_supports_legacy_v2_and_v3_store_fixtures() {
    for version in ["legacy", "v2", "v3"] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        fs::create_dir(&source).unwrap();
        match version {
            "legacy" => fs::write(
                source.join("records.log"),
                include_bytes!("../../../../../tests/fixtures/db01-legacy-record.bin"),
            )
            .unwrap(),
            "v2" => {
                fs::write(
                    source.join("records.log"),
                    include_bytes!("../../../../../tests/fixtures/v2-batch-records.log"),
                )
                .unwrap();
                fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../../../../tests/fixtures/v2-batch-commit.state"),
                )
                .unwrap();
            }
            "v3" => {
                fs::write(
                    source.join("records.log"),
                    include_bytes!("../../../../../tests/fixtures/v3-mixed-records.log"),
                )
                .unwrap();
                fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../../../../tests/fixtures/v3-mixed-commit.state"),
                )
                .unwrap();
                rustix::fs::setxattr(
                    source.join("records.log"),
                    "user.db.commit.v3",
                    include_bytes!("../../../../../tests/fixtures/v3-mixed-metadata.state"),
                    rustix::fs::XattrFlags::empty(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let store = OnlineStore::open(&source).unwrap();
        store.with_store(|db| db.put(b"later", b"write")).unwrap();
        let expected = store.with_store(|db| db.scan(None, None, 100)).unwrap();
        store
            .compact_and_reclaim(&previous, &Default::default())
            .unwrap();
        assert!(!previous.exists());
        assert_eq!(
            store.with_store(|db| db.scan(None, None, 100)).unwrap(),
            expected,
            "{version}"
        );
        drop(store);
        assert_eq!(
            retry_lock(|| Store::open(&source))
                .unwrap()
                .scan(None, None, 100)
                .unwrap(),
            expected,
            "{version}"
        );
    }
}

#[test]
fn reclamation_preserves_document_indexes_and_raw_transaction_data() {
    use crate::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue};
    use serde_json::json;
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store
        .with_store(|raw| {
            let db = DocumentDatabase::new(raw);
            db.create_collection("c", CollectionSchema::default())?;
            db.create_index(
                "c",
                IndexDefinition {
                    name: "email".into(),
                    fields: vec![vec!["email".into()]],
                    unique: true,
                    partial: None,
                },
            )?;
            db.transaction(|tx| {
                tx.insert("c", "id", &json!({"email":"x", "items":[1, 2]}))?;
                tx.put_raw(b"receipt", b"committed")
            })
        })
        .unwrap();
    store
        .compact_and_reclaim(&previous, &Default::default())
        .unwrap();
    let verify = |raw: &Store| -> io::Result<()> {
        let db = DocumentDatabase::new(raw);
        assert_eq!(
            db.get("c", "id")?,
            Some(json!({"email":"x", "items":[1, 2]}))
        );
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"receipt"))?,
            Some(b"committed".to_vec())
        );
        let page = db.query_index(
            "c",
            "email",
            &IndexQuery {
                equal: vec![IndexValue::String("x".into())],
                ..Default::default()
            },
        )?;
        assert_eq!(page.documents.len(), 1);
        assert_eq!(page.documents[0].id, "id");
        assert_eq!(db.verify_collection("c")?.index_keys, 2);
        Ok(())
    };
    store.with_store(verify).unwrap();
    drop(store);
    let recovered = retry_lock(|| Store::open(&source)).unwrap();
    verify(&recovered).unwrap();
}

#[test]
fn reclamation_keeps_per_open_limits_enforced_after_handoff() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store =
        OnlineStore::open_with_resource_limits(&source, Some(64 * 1024), Some(2), Some(2)).unwrap();
    store
        .with_store(|db| {
            db.put(b"a", b"1")?;
            db.put(b"b", b"2")
        })
        .unwrap();
    store
        .compact_and_reclaim(&previous, &Default::default())
        .unwrap();
    assert!(store.with_store(|db| db.put(b"c", b"3")).is_err());
    store
        .with_store(|db| {
            db.put(b"a", b"new")?;
            db.delete(b"b")?;
            db.put(b"c", b"3")
        })
        .unwrap();
    assert_eq!(
        store.with_store(|db| db.get(b"a")).unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        store.with_store(|db| db.get(b"c")).unwrap(),
        Some(b"3".to_vec())
    );
}
