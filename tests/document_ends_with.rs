//! Case-sensitive UTF-8 suffix filtering through scans, planned residuals
//! and transactions on both writable backends.
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, IndexDefinition, IndexValue, Store,
};
use serde_json::json;
use std::io::ErrorKind;

mod support;
use support::retry_lock;

fn ends_with(suffix: &str) -> DocumentFilter {
    DocumentFilter::EndsWith {
        path: vec!["name".into()],
        suffix: suffix.into(),
    }
}

fn ids(db: &DocumentDatabase<'_>, filter: DocumentFilter) -> Vec<String> {
    let query = DocumentQuery {
        filter,
        max_scanned: 64,
        ..Default::default()
    };
    db.query("c", &query)
        .unwrap()
        .documents
        .into_iter()
        .map(|entry| entry.id)
        .collect()
}

fn exercise(db: &DocumentDatabase<'_>) {
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
    for (id, name, score) in [
        ("a", json!("café"), 1),
        ("b", json!("CAFÉ"), 2),
        ("c", json!("au lait café"), 3),
        ("d", json!(""), 4),
        ("e", json!(5), 5),
        ("f", json!(null), 6),
        ("g", json!(["café"]), 7),
    ] {
        db.insert("c", id, &json!({"name": name, "score": score}))
            .unwrap();
    }
    db.insert("c", "h", &json!({"score": 8})).unwrap();
    // Case-sensitive, by UTF-8 characters; only strings match.
    assert_eq!(ids(db, ends_with("é")), ["a", "c"]);
    assert_eq!(ids(db, ends_with("É")), ["b"]);
    assert_eq!(ids(db, ends_with("fé")), ["a", "c"]);
    // An empty suffix matches every string, as an empty prefix does.
    assert_eq!(ids(db, ends_with("")), ["a", "b", "c", "d"]);
    // A negated suffix also selects missing, null and non-string values.
    assert_eq!(
        ids(db, DocumentFilter::Not(Box::new(ends_with("é")))),
        ["b", "d", "e", "f", "g", "h"]
    );
    // The operand shares the 256-byte string cap.
    let long = DocumentQuery {
        filter: ends_with(&"x".repeat(257)),
        max_scanned: 64,
        ..Default::default()
    };
    assert_eq!(
        db.query("c", &long).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    // A suffix alone claims no index; a range conjunct plans the query and
    // the suffix is a residual filter.
    let planned = |filter: DocumentFilter| {
        db.query_planned(
            "c",
            &DocumentQuery {
                filter,
                max_scanned: 64,
                ..Default::default()
            },
        )
        .unwrap()
    };
    assert_eq!(
        planned(ends_with("é")).access,
        DocumentQueryAccess::CollectionScan
    );
    let residual = DocumentFilter::And(vec![
        DocumentFilter::Compare {
            path: vec!["score".into()],
            comparison: Comparison::GreaterOrEqual,
            value: IndexValue::Integer(2),
        },
        ends_with("é"),
    ]);
    let result = planned(residual.clone());
    assert_eq!(
        result.access,
        DocumentQueryAccess::RangeIndex {
            index: "score".into()
        }
    );
    let planned_ids = result
        .result
        .documents
        .into_iter()
        .map(|entry| entry.id)
        .collect::<Vec<_>>();
    assert_eq!(planned_ids, ["c"]);
    assert_eq!(ids(db, residual.clone()), ["c"]);
    // Transactions use the same evaluator.
    let in_transaction = db
        .transaction(|tx| {
            tx.query(
                "c",
                &DocumentQuery {
                    filter: residual,
                    max_scanned: 64,
                    ..Default::default()
                },
            )
        })
        .unwrap();
    assert_eq!(in_transaction.documents.len(), 1);
}

#[test]
fn suffix_filters_match_on_both_writable_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let store = Store::open(&source).unwrap();
        if checkpoint_mode {
            let checkpoint = root.path().join("checkpoint");
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            exercise(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            exercise(&DocumentDatabase::new(&store));
        }
    }
}
