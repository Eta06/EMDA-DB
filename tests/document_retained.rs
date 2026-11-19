use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    AuthorizedRetainedStore, FileRetainedAuthority, IndexLimits, ManagedRetainedStore,
    OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{
    CancellationToken, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentTransactionMode, IndexDefinition, IndexQuery, MaintenanceKey,
    RetainedExportLimits, Store, TransactionFailure,
};
use serde_json::json;
use std::io;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Instant;

const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 1_000_000,
        entries: 5_000,
        key_bytes: 250_000,
        work_bytes: 1_000_001,
    },
    index: IndexLimits {
        blocks: 32,
        anchor_bytes: 4096,
    },
    suffix: SuffixLimits {
        bytes: 1_000_000,
        operations: 5_000,
        work: 10_000_000,
    },
    overlay: OverlayLimits {
        entries: 5_000,
        key_bytes: 250_000,
    },
};

const EXPORT: RetainedExportLimits = RetainedExportLimits {
    max_log_bytes: 1_000_000,
    max_live_keys: 5_000,
    max_live_key_bytes: 250_000,
    max_output_bytes: 1_000_000,
};

fn seed(source: &Path) -> Store {
    let store = Store::open(source).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("items", CollectionSchema::default())
        .unwrap();
    db.insert("items", "a", &json!({"n": 1})).unwrap();
    db.insert("items", "z", &json!({"n": 10})).unwrap();
    db.create_index(
        "items",
        IndexDefinition {
            name: "score".into(),
            fields: vec![vec!["n".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    store
}

fn prepare(root: &Path) -> ManagedRetainedStore {
    let source = root.join("source");
    let catalog = root.join("catalog");
    ManagedRetainedStore::copy_from_store(seed(&source), &source, &catalog, EXPORT, LIMITS)
        .unwrap();
    ManagedRetainedStore::open_with_auto_reclaim(&catalog, LIMITS, NonZeroU64::MIN).unwrap()
}

#[test]
fn authorized_cutover_preserves_document_transactions_through_reclamation() {
    type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let authority = dir.path().join("authority");
    let initial = Catalog::cutover_from_store(seed(&source), &source, EXPORT, LIMITS).unwrap();
    let mut provider = FileRetainedAuthority::create(&authority).unwrap();
    assert!(provider.compare_and_swap(None, initial).unwrap());
    let open = || {
        Catalog::open_with_auto_reclaim(
            &source,
            LIMITS,
            FileRetainedAuthority::open(&authority).unwrap(),
            NonZeroU64::MIN,
        )
        .unwrap()
    };
    let mut store = open();
    {
        let db = DocumentDatabase::with_authorized_retained_store(&mut store);
        let token = CancellationToken::new();
        assert!(matches!(
            db.transaction_detailed(
                DocumentTransactionMode::ReadSetFifo,
                None,
                Some(&token),
                |tx| {
                    tx.replace("items", "a", &json!({"n": 0}), false)?;
                    tx.put_raw(b"cancelled", b"never committed")?;
                    token.cancel();
                    Ok(())
                }
            ),
            Err(TransactionFailure::Cancelled)
        ));
        assert_eq!(provider.read().unwrap(), Some(initial));

        db.transaction_with_read_set(|tx| {
            tx.delete("items", "a")?;
            tx.insert("items", "m", &json!({"n": 3}))?;
            tx.replace("items", "z", &json!({"n": 7}), false)?;
            tx.put_raw(b"receipt", b"committed")?;
            let rows = tx
                .query_index("items", "score", &IndexQuery::default())?
                .documents;
            assert_eq!(
                rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
                ["m", "z"]
            );
            assert_eq!(rows[1].document, json!({"n": 7}));
            let planned = tx.query_planned(
                "items",
                &DocumentQuery {
                    filter: DocumentFilter::Or(vec![
                        DocumentFilter::Equal {
                            path: vec!["n".into()],
                            value: json!(3),
                        },
                        DocumentFilter::Equal {
                            path: vec!["n".into()],
                            value: json!(7),
                        },
                    ]),
                    ..Default::default()
                },
            )?;
            assert_eq!(
                planned.access,
                DocumentQueryAccess::DisjunctionIndex {
                    indexes: vec!["score".into()],
                    probes: 2,
                }
            );
            assert_eq!(planned.result.documents, rows);
            Ok(())
        })
        .unwrap();
    }
    let committed = provider.read().unwrap().unwrap();
    assert_eq!(committed.revision, initial.revision + 2);
    assert!(committed.pending.is_none());
    assert_eq!(store.get(b"cancelled", 32).unwrap(), None);
    {
        let db = DocumentDatabase::with_authorized_retained_store(&mut store);
        db.replace("items", "z", &json!({"n": 8}), false).unwrap();
    }
    let rotated = provider.read().unwrap().unwrap();
    assert_ne!(rotated.committed.generation, committed.committed.generation);
    assert_eq!(rotated.revision, committed.revision + 4);
    drop(store);

    let mut store = open();
    let db = DocumentDatabase::with_authorized_retained_store(&mut store);
    assert_eq!(db.get("items", "a").unwrap(), None);
    assert_eq!(db.get("items", "m").unwrap(), Some(json!({"n": 3})));
    assert_eq!(db.get("items", "z").unwrap(), Some(json!({"n": 8})));
    assert!(db.insert("items", "duplicate", &json!({"n": 8})).is_err());
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"receipt"))
            .unwrap()
            .unwrap(),
        b"committed"
    );
    db.verify_collection("items").unwrap();

    let key = MaintenanceKey::generate().unwrap();
    let saved_key = key.to_bytes();
    db.start_index_build_guarded(
        "items",
        IndexDefinition {
            name: "guarded-score".into(),
            fields: vec![vec!["n".into()]],
            unique: false,
            partial: None,
        },
        &key,
    )
    .unwrap();
    assert!(
        !db.advance_index_build_guarded("items", "guarded-score", 1, &key)
            .unwrap()
            .complete
    );
    drop(store);

    let mut store = open();
    let db = DocumentDatabase::with_authorized_retained_store(&mut store);
    let key = MaintenanceKey::from_bytes(saved_key);
    assert!(
        !db.advance_index_build_guarded("items", "guarded-score", 1, &key)
            .unwrap()
            .complete
    );
    assert!(
        db.advance_index_build_guarded("items", "guarded-score", 1, &key)
            .unwrap()
            .complete
    );
    let page = db
        .query_index("items", "guarded-score", &IndexQuery::default())
        .unwrap();
    assert_eq!(page.documents.len(), 2);
    db.verify_collection("items").unwrap();
}

#[test]
fn indexed_document_transactions_survive_automatic_reclamation_and_bound_restore() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = prepare(dir.path());
    let first = store.generation_id().unwrap();
    {
        let db = DocumentDatabase::with_managed_retained_store(&mut store);
        db.replace("items", "z", &json!({"n": 5}), false).unwrap();
        db.transaction_with_read_set(|tx| {
            tx.delete("items", "a")?;
            tx.insert("items", "m", &json!({"n": 3}))?;
            tx.replace("items", "z", &json!({"n": 7}), false)?;
            tx.put_raw(b"receipt", b"committed")?;
            let page = tx.query_index("items", "score", &IndexQuery::default())?;
            assert_eq!(
                page.documents
                    .iter()
                    .map(|row| row.id.as_str())
                    .collect::<Vec<_>>(),
                ["m", "z"]
            );
            assert_eq!(page.documents[1].document, json!({"n": 7}));
            assert_eq!(tx.get("items", "a")?, None);
            Ok(())
        })
        .unwrap();
    }
    assert_ne!(store.generation_id().unwrap(), first);
    let backup = dir.path().join("backup");
    let expected = store.backup_to(&backup).unwrap();
    drop(store);
    let target = dir.path().join("restored");
    ManagedRetainedStore::restore_backup(&backup, &target, LIMITS, &expected).unwrap();
    let mut restored = ManagedRetainedStore::open(&target, LIMITS).unwrap();
    let db = DocumentDatabase::with_managed_retained_store(&mut restored);
    assert_eq!(db.get("items", "m").unwrap(), Some(json!({"n": 3})));
    assert_eq!(db.get("items", "z").unwrap(), Some(json!({"n": 7})));
    assert_eq!(db.get("items", "a").unwrap(), None);
    let page = db
        .query_index("items", "score", &IndexQuery::default())
        .unwrap();
    assert_eq!(page.documents.len(), 2);
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"receipt"))
            .unwrap()
            .unwrap(),
        b"committed"
    );
    db.verify_collection("items").unwrap();
}

#[test]
fn retained_document_controls_and_staging_failures_leave_no_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = prepare(dir.path());
    let db = DocumentDatabase::with_managed_retained_store(&mut store);
    let mode = DocumentTransactionMode::ReadSetFifo;
    let token = CancellationToken::new();
    assert!(matches!(
        db.transaction_detailed(mode, None, Some(&token), |tx| {
            tx.insert("items", "cancelled", &json!({"n": 2}))?;
            token.cancel();
            Ok(())
        }),
        Err(TransactionFailure::Cancelled)
    ));
    let token = CancellationToken::new();
    let result = db.transaction_controlled(&token, None, mode, |tx| {
        tx.put_raw(b"receipt", b"cancelled during callback")?;
        token.cancel();
        tx.get_raw(b"receipt")
    });
    assert!(matches!(
        result,
        Err(database_engine::ControlledTransactionError::Cancelled)
    ));
    let token = CancellationToken::new();
    let result = db.transaction_controlled(&token, None, mode, |_| {
        Err::<(), _>(io::Error::new(io::ErrorKind::Interrupted, "callback I/O"))
    });
    assert!(matches!(
        result,
        Err(database_engine::ControlledTransactionError::Operation(error))
            if error.kind() == io::ErrorKind::Interrupted
    ));
    assert!(matches!(
        db.transaction_detailed(mode, Some(Instant::now()), None, |_| panic!(
            "expired callback"
        )),
        Err::<(), _>(TransactionFailure::DeadlineExceeded)
    ));
    assert!(matches!(
        db.transaction_detailed(mode, None, None, |tx| {
            tx.put_raw(b"receipt", b"rejected")?;
            let _ = tx.insert("items", "duplicate", &json!({"n": 1}));
            Ok(())
        }),
        Err(TransactionFailure::Callback(_))
    ));
    db.transaction(|tx| {
        assert_eq!(
            db.get("items", "a").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(tx.get_raw(b"receipt")?, None);
        Ok(())
    })
    .unwrap();
    assert_eq!(db.get("items", "cancelled").unwrap(), None);
    assert_eq!(db.get("items", "duplicate").unwrap(), None);
    db.verify_collection("items").unwrap();
}
