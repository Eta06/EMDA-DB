//! Queries and updates bound their exact JSON equality work.
use database_engine::{
    AggregateRequest, AggregateStatsRequest, CheckpointStore, CollectionSchema, DocumentDatabase,
    DocumentFilter, DocumentQuery, DocumentQueryAccess, DocumentUpdate, FieldCondition,
    IndexDefinition, Store,
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

fn create_indexed_collection(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "by_n".into(),
            fields: vec![path("n")],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
}

#[test]
fn nested_membership_work_limit_is_shared_by_scans_and_indexed_aggregation() {
    on_backends(|db| {
        create_indexed_collection(db);
        // Each nested comparison visits 125,000 child values.
        let payload = json!(vec![0; 125_000]);
        db.insert("c", "a", &json!({"n": 1, "payload": payload, "extra": [0]}))
            .unwrap();
        let mut filters = vec![equal("n", json!(1))];
        filters.extend((0..8).map(|_| DocumentFilter::In {
            path: path("payload"),
            values: vec![payload.clone()],
        }));
        let exact = query(DocumentFilter::And(filters.clone()));
        let scanned = db.query("c", &exact).unwrap();
        let planned = db.query_planned("c", &exact).unwrap();
        assert_eq!(scanned.documents.len(), 1);
        assert_eq!(scanned.documents, planned.result.documents);
        assert!(matches!(
            planned.access,
            DocumentQueryAccess::EqualityIndex { .. }
        ));

        filters.push(equal("extra", json!([0])));
        let exceeded = query(DocumentFilter::And(filters));
        let request = AggregateStatsRequest {
            filter: exceeded.filter.clone(),
            group_by: None,
            fields: vec![path("n")],
            max_scanned: 32,
        };
        for error in [
            db.query("c", &exceeded).unwrap_err(),
            db.query_planned("c", &exceeded).unwrap_err(),
            db.aggregate_stats("c", &request).unwrap_err(),
            db.aggregate_stats_planned("c", &request).unwrap_err(),
        ] {
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert_eq!(
                error.to_string(),
                "nested equality comparison limit exceeded"
            );
        }
    });
}

#[test]
fn repeated_string_equality_stops_at_the_shared_text_limit() {
    on_backends(|db| {
        create_indexed_collection(db);
        let text = json!("x".repeat(256 * 1024));
        let document = json!({"n": 1, "text": text});
        for id in 0..32 {
            db.insert("c", &format!("doc-{id:02}"), &document).unwrap();
        }
        let mut filters = vec![equal("n", json!(1))];
        filters.extend((0..8).map(|_| equal("text", text.clone())));
        let request = DocumentQuery {
            projection: Some(vec![path("n")]),
            max_scanned: 64,
            ..query(DocumentFilter::And(filters))
        };
        let planned = db.query_planned("c", &request).unwrap();
        assert!(matches!(
            planned.access,
            DocumentQueryAccess::EqualityIndex { .. }
        ));
        assert_eq!(planned.result.documents.len(), 32);
        assert_eq!(
            db.query("c", &request).unwrap().documents,
            planned.result.documents,
        );
        let error = db
            .transaction(|tx| {
                tx.insert("c", "overflow", &document)?;
                tx.put_raw(b"receipt", b"staged")?;
                tx.query_planned("c", &request)?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "filter equality text byte limit exceeded"
        );
        assert_eq!(db.get("c", "overflow").unwrap(), None);
        assert_eq!(db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(), None);
    });
}

#[test]
fn conditional_update_shares_nested_work_with_array_operators() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        let item = json!(vec![0; 15_625]);
        let original = json!({"expected": item, "items": [item]});
        db.insert("c", "a", &original).unwrap();

        let conditions = vec![
            FieldCondition {
                path: path("expected"),
                expected: Some(item.clone()),
            };
            63
        ];
        let add = DocumentUpdate::ArrayAddUnique {
            path: path("items"),
            value: item,
        };
        // 63 matching preconditions plus one matching array element visit
        // exactly 1,000,000 child values.
        assert!(
            db.update_if("c", "a", &conditions, std::slice::from_ref(&add), false)
                .unwrap()
        );
        let error = db
            .transaction(|tx| {
                tx.put_raw(b"rollback", b"value")?;
                tx.update_if(
                    "c",
                    "a",
                    &conditions,
                    &[
                        DocumentUpdate::Set {
                            path: path("changed"),
                            value: json!(true),
                        },
                        add.clone(),
                        add.clone(),
                    ],
                    false,
                )
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "nested equality comparison limit exceeded"
        );
        assert_eq!(db.get("c", "a").unwrap(), Some(original));
        assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);
    });
}

#[test]
fn repeated_array_removal_shares_the_text_comparison_limit() {
    on_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        let item = "x".repeat(32 * 1024);
        let original = json!({"items": vec![item.clone(); 31]});
        db.insert("c", "a", &original).unwrap();
        let mut near_match = item;
        near_match.pop();
        near_match.push('y');
        let remove = DocumentUpdate::ArrayRemove {
            path: path("items"),
            value: json!(near_match),
        };
        // 66 passes spend just under 64 MiB of logical text comparisons.
        let mut removals = vec![remove.clone(); 66];
        assert!(db.update_if("c", "a", &[], &removals, false).unwrap());
        removals.push(remove);
        let error = db.update_if("c", "a", &[], &removals, false).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "update equality text byte limit exceeded"
        );
        assert_eq!(db.get("c", "a").unwrap(), Some(original));
    });
}
