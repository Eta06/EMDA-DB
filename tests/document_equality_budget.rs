//! Equality inputs and work share the document filter's resource budgets.
use database_engine::{
    AggregateRequest, AggregateStatsRequest, CheckpointStore, CollectionSchema, DocumentDatabase,
    DocumentFilter, DocumentQuery, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;

mod support;
use support::retry_lock;

fn path(name: &str) -> Vec<String> {
    vec![name.into()]
}

fn equal(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: path(name),
        value,
    }
}

fn query(filter: DocumentFilter) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned: 32,
        ..DocumentQuery::default()
    }
}

fn on_backends(mut action: impl FnMut(&DocumentDatabase<'_>)) {
    for checkpoint in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let index = root.path().join("index");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&index).unwrap();
        if checkpoint {
            drop(store);
            let store = retry_lock(|| CheckpointStore::open(&source, &index, &digest)).unwrap();
            action(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            action(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn equality_values_share_exact_encoded_limits_even_when_branches_do_not_run() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        // Each value encodes to exactly 1 MiB: quotes plus escaped newlines.
        let chunk = json!("\n".repeat(512 * 1024 - 1));
        assert_eq!(serde_json::to_vec(&chunk).unwrap().len(), 1024 * 1024);
        let exactly =
            DocumentFilter::And((0..4).map(|_| equal("missing", chunk.clone())).collect());
        assert!(
            db.query("c", &query(exactly.clone()))
                .unwrap()
                .documents
                .is_empty()
        );
        assert!(
            db.query_planned("c", &query(exactly.clone()))
                .unwrap()
                .result
                .documents
                .is_empty()
        );
        let too_large = DocumentFilter::Or(vec![
            DocumentFilter::All,
            exactly,
            equal("missing", Value::Null),
        ]);
        let oversized = query(too_large.clone());
        assert_eq!(
            db.query("c", &oversized).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            db.query_planned("c", &oversized).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            db.aggregate(
                "c",
                &AggregateRequest {
                    filter: too_large.clone(),
                    group_by: None,
                    sum: None,
                    max_scanned: 32
                }
            )
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            db.aggregate_stats(
                "c",
                &AggregateStatsRequest {
                    filter: too_large,
                    group_by: None,
                    fields: vec![path("n")],
                    max_scanned: 32
                }
            )
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            db.transaction(|tx| {
                tx.put_raw(b"rollback", b"not committed")?;
                tx.query("c", &oversized)?;
                Ok(())
            })
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);

        let full_membership = DocumentFilter::In {
            path: path("missing"),
            values: vec![Value::Null; 256],
        };
        assert!(
            db.query("c", &query(full_membership.clone()))
                .unwrap()
                .documents
                .is_empty()
        );
        let mixed = DocumentFilter::And(vec![full_membership, equal("missing", Value::Null)]);
        assert_eq!(
            db.query("c", &query(mixed)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn equality_and_membership_share_one_comparison_budget_and_abort_atomically() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        let mut tags = vec![json!(0); 4_000];
        *tags.last_mut().unwrap() = json!(1);
        for id in ["a", "b"] {
            db.insert("c", id, &json!({"n":0, "tags":tags})).unwrap();
        }
        let filter = |array_checks| {
            let mut filters = vec![equal("n", json!(0))];
            filters.extend((0..array_checks).map(|_| DocumentFilter::ArrayContains {
                path: path("tags"),
                value: json!(1),
            }));
            DocumentFilter::And(filters)
        };
        assert_eq!(
            db.query("c", &query(filter(124))).unwrap().documents.len(),
            2
        );
        let q = query(filter(125));
        for error in [
            db.query("c", &q).unwrap_err(),
            db.query_planned("c", &q).unwrap_err(),
        ] {
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert!(error.to_string().contains("comparison limit"));
        }
        let error = db
            .transaction(|tx| {
                tx.put_raw(b"work-rollback", b"not committed")?;
                tx.query("c", &q)?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"work-rollback")).unwrap(),
            None
        );
    });
}
