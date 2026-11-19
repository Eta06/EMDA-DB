use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    IndexLimits, ManagedRetainedStore, OverlayLimits, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{
    CancellationToken, CollectionSchema, DocumentDatabase, DocumentTransactionMode,
    IndexDefinition, IndexQuery, RetainedExportLimits, Store, TransactionFailure,
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

fn prepare(root: &Path) -> ManagedRetainedStore {
    let source = root.join("source");
    let catalog = root.join("catalog");
    let store = Store::open(&source).unwrap();
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
    ManagedRetainedStore::copy_from_store(
        store,
        &source,
        &catalog,
        RetainedExportLimits {
            max_log_bytes: 1_000_000,
            max_live_keys: 5_000,
            max_live_key_bytes: 250_000,
            max_output_bytes: 1_000_000,
        },
        LIMITS,
    )
    .unwrap();
    ManagedRetainedStore::open_with_auto_reclaim(&catalog, LIMITS, NonZeroU64::MIN).unwrap()
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
