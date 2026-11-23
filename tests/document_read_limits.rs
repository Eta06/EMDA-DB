use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    AuthorizedRetainedStore, FileRetainedAuthority, IndexLimits, ManagedRetainedStore,
    OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentReadLimits,
    DocumentTransactionMode, IndexDefinition, IndexQuery, IndexValue, RetainedExportLimits, Store,
    TransactionFailure,
};
use serde_json::json;
use std::io;

mod support;
use support::retry_lock;

fn is_read_limit(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidInput
        && error.to_string() == "document transaction read limit exceeded"
}

fn check_limits(db: &DocumentDatabase<'_>) {
    let mode = DocumentTransactionMode::ReadSet;
    let limits = |units, bytes| DocumentReadLimits {
        max_read_units: units,
        max_read_bytes: bytes,
    };

    db.transaction_with_read_limits(limits(2, 8), mode, None, None, |tx| {
        assert_eq!(tx.get_raw(b"key")?, Some(b"data".to_vec()));
        assert_eq!(tx.get_raw(b"z")?, None);
        Ok(())
    })
    .unwrap();

    for limits in [limits(2, 6), limits(0, 8)] {
        let result = db.transaction_with_read_limits(limits, mode, None, None, |tx| {
            tx.put_raw(b"receipt", b"not committed")?;
            assert!(is_read_limit(&tx.get_raw(b"key").unwrap_err()));
            assert!(is_read_limit(&tx.get_raw(b"z").unwrap_err()));
            Ok(())
        });
        assert!(
            matches!(result, Err(TransactionFailure::Callback(error)) if is_read_limit(&error))
        );
    }

    let page = IndexQuery {
        limit: 1,
        ..Default::default()
    };
    let result = db.transaction_with_read_limits(limits(4, usize::MAX), mode, None, None, |tx| {
        let first = tx.query_index("c", "score", &page)?;
        assert_eq!(first.documents[0].id, "a");
        let next = IndexQuery {
            after: first.next,
            ..page.clone()
        };
        assert!(is_read_limit(
            &tx.query_index("c", "score", &next).unwrap_err()
        ));
        Ok(())
    });
    assert!(matches!(result, Err(TransactionFailure::Callback(error)) if is_read_limit(&error)));

    let result = db.transaction_with_read_limits(limits(3, usize::MAX), mode, None, None, |tx| {
        tx.put_raw(b"receipt", b"not committed")?;
        let query = IndexQuery {
            limit: 2,
            ..Default::default()
        };
        // Metadata, the page request, and one index row fit. Reject the
        // whole page when admission of its second index row fails.
        assert!(is_read_limit(
            &tx.query_index("c", "score", &query).unwrap_err()
        ));
        Ok(())
    });
    assert!(matches!(result, Err(TransactionFailure::Callback(error)) if is_read_limit(&error)));

    let empty = IndexQuery {
        equal: vec![IndexValue::Integer(9)],
        ..Default::default()
    };
    db.transaction_with_read_limits(limits(2, usize::MAX), mode, None, None, |tx| {
        assert!(tx.query_index("c", "score", &empty)?.documents.is_empty());
        Ok(())
    })
    .unwrap();
    assert_eq!(db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(), None);
}

#[test]
fn cumulative_document_read_limits_apply_to_all_writable_backends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint_path = dir.path().join("checkpoint");
    let catalog = dir.path().join("retained");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "score".into(),
            fields: vec![vec!["score".into()]],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "a", &json!({"score": 1})).unwrap();
    db.insert("c", "b", &json!({"score": 2})).unwrap();
    store.put(b"key", b"data").unwrap();
    check_limits(&db);
    let digest = store.checkpoint_to(&checkpoint_path).unwrap();

    let retained_limits = RetainedStoreLimits {
        base: Limits {
            file_bytes: 16384,
            entries: 32,
            key_bytes: 4096,
            work_bytes: 16384,
        },
        index: IndexLimits {
            blocks: 8,
            anchor_bytes: 4096,
        },
        suffix: SuffixLimits {
            bytes: 16384,
            operations: 32,
            work: 16384,
        },
        overlay: OverlayLimits {
            entries: 32,
            key_bytes: 4096,
        },
    };
    let export = RetainedExportLimits {
        max_log_bytes: 16384,
        max_live_keys: 32,
        max_live_key_bytes: 4096,
        max_output_bytes: 16384,
    };
    ManagedRetainedStore::copy_from_store(store, &path, &catalog, export, retained_limits).unwrap();
    let checkpoint_store =
        retry_lock(|| CheckpointStore::open(&path, &checkpoint_path, &digest)).unwrap();
    check_limits(&DocumentDatabase::with_checkpoint_store(&checkpoint_store));
    drop(checkpoint_store);
    let mut retained = ManagedRetainedStore::open(&catalog, retained_limits).unwrap();
    check_limits(&DocumentDatabase::with_managed_retained_store(
        &mut retained,
    ));

    let authorized = dir.path().join("authorized");
    let authority = dir.path().join("authority");
    type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;
    let initial = Catalog::prepare_from_store(
        retry_lock(|| Store::open(&path)).unwrap(),
        &path,
        &authorized,
        export,
        retained_limits,
    )
    .unwrap();
    let mut retained = Catalog::register_prepared(
        &authorized,
        retained_limits,
        FileRetainedAuthority::create(&authority).unwrap(),
        initial,
    )
    .unwrap();
    check_limits(&DocumentDatabase::with_authorized_retained_store(
        &mut retained,
    ));
    assert_eq!(
        FileRetainedAuthority::open(&authority)
            .unwrap()
            .read()
            .unwrap(),
        Some(initial)
    );
}
