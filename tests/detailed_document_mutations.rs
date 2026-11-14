use database_engine::{
    BulkOperation, BulkResult, CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate,
    FieldCondition, IndexDefinition, Store, TransactionFailure,
};
use serde_json::json;

mod support;
use support::retry_lock;

fn exercise(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "x".into(),
            fields: vec![vec!["x".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.insert_detailed("c", "a", &json!({"x":1})).unwrap();
    assert!(matches!(
        db.insert_detailed("c", "a", &json!({"x":2})),
        Err(TransactionFailure::Callback(_))
    ));
    assert!(matches!(
        db.insert_detailed("c", "b", &json!({"x":1})),
        Err(TransactionFailure::Callback(_))
    ));
    assert_eq!(db.get("c", "b").unwrap(), None);
    assert!(matches!(
        db.replace_detailed("c", "missing", &json!({"x":2}), false),
        Err(TransactionFailure::Callback(_))
    ));
    db.replace_detailed("c", "a", &json!({"x":2}), false)
        .unwrap();
    let updates = [DocumentUpdate::Set {
        path: vec!["x".into()],
        value: json!(3),
    }];
    let conditions = [FieldCondition {
        path: vec!["x".into()],
        expected: Some(json!(99)),
    }];
    assert!(
        !db.update_if_detailed("c", "a", &conditions, &updates, false)
            .unwrap()
    );
    assert!(
        !db.update_if_detailed("c", "missing", &[], &updates, false)
            .unwrap()
    );
    assert!(
        db.update_if_detailed("c", "a", &[], &updates, false)
            .unwrap()
    );
    let four = json!({"x":4});
    let five = json!({"x":5});
    let failed_batch = [
        BulkOperation::Replace {
            collection: "c",
            id: "a",
            document: &four,
            upsert: false,
        },
        BulkOperation::Insert {
            collection: "c",
            id: "a",
            document: &five,
        },
    ];
    assert!(matches!(
        db.bulk_detailed(&failed_batch),
        Err(TransactionFailure::Callback(_))
    ));
    assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":3})));
    let batch = [
        BulkOperation::Replace {
            collection: "c",
            id: "a",
            document: &four,
            upsert: false,
        },
        BulkOperation::Insert {
            collection: "c",
            id: "b",
            document: &five,
        },
    ];
    assert_eq!(
        db.bulk_detailed(&batch).unwrap(),
        [BulkResult::Written, BulkResult::Written]
    );
    assert!(!db.delete_detailed("c", "missing").unwrap());
    assert!(db.delete_detailed("c", "b").unwrap());
    assert_eq!(db.get("c", "a").unwrap(), Some(four));
    assert_eq!(db.get("c", "b").unwrap(), None);
    db.verify_collection("c").unwrap();
}

fn check_rejected(db: &DocumentDatabase<'_>) {
    assert!(matches!(
        db.insert_detailed("c", "blocked", &json!({"x":6})),
        Err(TransactionFailure::Rejected(_))
    ));
}

#[test]
fn document_group_admission_rejection_preserves_documents_and_indexes() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "x".into(),
            fields: vec![vec!["x".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    let before = std::fs::read(root.path().join("records.log")).unwrap();
    store.set_group_commit_limits(0, 0).unwrap();
    assert!(matches!(
        db.insert_detailed("c", "blocked", &json!({"x":1})),
        Err(TransactionFailure::Rejected(_))
    ));
    assert_eq!(
        std::fs::read(root.path().join("records.log")).unwrap(),
        before
    );
    store
        .set_group_commit_limits(
            database_engine::MAX_GROUP_COMMIT_BUFFERED_BYTES,
            database_engine::MAX_GROUP_COMMIT_BUFFERED_RECORDS,
        )
        .unwrap();
    assert_eq!(db.get("c", "blocked").unwrap(), None);
    db.verify_collection("c").unwrap();
}

#[test]
fn common_document_outcomes_are_atomic_on_both_backends_and_formats() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for checkpoint_writer in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            if v2 {
                std::fs::create_dir(&path).unwrap();
                std::fs::write(
                    path.join("records.log"),
                    include_bytes!("fixtures/v2-empty-records.log"),
                )
                .unwrap();
                std::fs::write(
                    path.join("commit.state"),
                    include_bytes!("fixtures/v2-empty-commit.state"),
                )
                .unwrap();
            }
            let store = Store::open(&path).unwrap();
            let checkpoint = root.path().join("checkpoint");
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            if checkpoint_writer {
                drop(store);
                let store =
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
                let db = DocumentDatabase::with_checkpoint_store(&store);
                exercise(&db);
                store.set_active_transaction_limit(0).unwrap();
                check_rejected(&db);
                store
                    .set_active_transaction_limit(database_engine::MAX_ACTIVE_TRANSACTIONS)
                    .unwrap();
                assert_eq!(db.get("c", "blocked").unwrap(), None);
            } else {
                let db = DocumentDatabase::new(&store);
                exercise(&db);
                store.set_active_transaction_limit(0).unwrap();
                check_rejected(&db);
                store
                    .set_active_transaction_limit(database_engine::MAX_ACTIVE_TRANSACTIONS)
                    .unwrap();
                assert_eq!(db.get("c", "blocked").unwrap(), None);
                drop(store);
            }
            let recovered = retry_lock(|| Store::open(&path)).unwrap();
            let db = DocumentDatabase::new(&recovered);
            assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":4})));
            assert_eq!(db.get("c", "b").unwrap(), None);
            db.verify_collection("c").unwrap();
        }
    }
}
