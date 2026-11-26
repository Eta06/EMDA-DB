//! Request usage accounting agrees exactly with request admission.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentQuery, DocumentReadLimits,
    DocumentRequestLimits, DocumentRequestUsage, DocumentSnapshot, DocumentTransaction,
    DocumentTransactionLimits, DocumentTransactionMode, IndexDefinition, Store,
};
use serde_json::json;
use std::io;

mod support;
use support::retry_lock;

fn seed(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "by_n".into(),
            fields: vec![vec!["n".into()]],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    for index in 0..8 {
        db.insert(
            "c",
            &format!("d{index}"),
            &json!({"n": index, "pad": "x".repeat(40)}),
        )
        .unwrap();
    }
}

// Reads, an indexed update and an insert, returning the admitted usage.
fn request(tx: &mut DocumentTransaction<'_, '_>) -> io::Result<DocumentRequestUsage> {
    tx.get("c", "d1")?;
    tx.query(
        "c",
        &DocumentQuery {
            max_scanned: 16,
            ..Default::default()
        },
    )?;
    tx.replace("c", "d2", &json!({"n": 20, "pad": "y"}), false)?;
    tx.insert("c", "new", &json!({"n": 99}))?;
    Ok(tx.request_usage())
}

fn limits(usage: DocumentRequestUsage) -> DocumentRequestLimits {
    DocumentRequestLimits {
        transaction: DocumentTransactionLimits {
            reads: DocumentReadLimits {
                max_read_units: usage.read_units,
                max_read_bytes: usage.read_bytes,
            },
            max_validation_items: u64::MAX,
        },
        max_write_units: usage.write_units,
        max_write_bytes: usage.write_bytes,
    }
}

fn check(db: &DocumentDatabase<'_>) {
    // Measure an aborted request, then prove the measure is exact.
    let mut usage = None;
    let _ = db.transaction(|tx| -> io::Result<()> {
        usage = Some(request(tx)?);
        Err(io::Error::other("measure only"))
    });
    let usage = usage.unwrap();
    assert!(usage.read_units > 0 && usage.read_bytes > 0, "{usage:?}");
    assert!(usage.write_units >= 3 && usage.write_bytes > 0, "{usage:?}");
    let run = |limits| {
        db.transaction_with_request_limits(
            limits,
            DocumentTransactionMode::Conservative,
            None,
            None,
            |tx| {
                request(tx)?;
                Err::<(), _>(io::Error::other("rolled back"))
            },
        )
    };
    let refused = |result: Result<(), database_engine::TransactionFailure>| {
        let message = format!("{:?}", result.unwrap_err());
        assert!(message.contains("limit exceeded"), "{message}");
    };
    // Exactly the measured usage is admitted; the callback then aborts.
    let message = format!("{:?}", run(limits(usage)).unwrap_err());
    assert!(message.contains("rolled back"), "{message}");
    let mut less = limits(usage);
    less.transaction.reads.max_read_units -= 1;
    refused(run(less));
    let mut less = limits(usage);
    less.transaction.reads.max_read_bytes -= 1;
    refused(run(less));
    let mut less = limits(usage);
    less.max_write_units -= 1;
    refused(run(less));
    let mut less = limits(usage);
    less.max_write_bytes -= 1;
    refused(run(less));
    // Nothing from the aborted requests was committed.
    assert!(db.get("c", "new").unwrap().is_none());
}

#[test]
fn request_usage_matches_admission_on_both_writable_backends() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let store = Store::open(&source).unwrap();
    let db = DocumentDatabase::new(&store);
    seed(&db);
    check(&db);
    let checkpoint = root.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(db);
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
    check(&DocumentDatabase::with_checkpoint_store(&store));
}

#[test]
fn snapshot_usage_matches_its_read_limits() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("store")).unwrap();
    seed(&DocumentDatabase::new(&store));
    let reads = |documents: &DocumentSnapshot<'_>| -> io::Result<()> {
        documents.get("c", "d3")?;
        documents.query(
            "c",
            &DocumentQuery {
                max_scanned: 16,
                ..Default::default()
            },
        )?;
        Ok(())
    };
    let snapshot = store.snapshot(1024, 1 << 20).unwrap();
    let unlimited = DocumentSnapshot::new(&snapshot);
    reads(&unlimited).unwrap();
    let usage = unlimited.request_usage();
    assert_eq!((usage.write_units, usage.write_bytes), (0, 0));
    let exact = DocumentSnapshot::new(&snapshot).with_read_limits(DocumentReadLimits {
        max_read_units: usage.read_units,
        max_read_bytes: usage.read_bytes,
    });
    reads(&exact).unwrap();
    assert_eq!(exact.request_usage(), usage);
    let short = DocumentSnapshot::new(&snapshot).with_read_limits(DocumentReadLimits {
        max_read_units: usage.read_units,
        max_read_bytes: usage.read_bytes - 1,
    });
    assert!(reads(&short).is_err());
}
