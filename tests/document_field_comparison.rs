use database_engine::{
    AggregateStatsRequest, CheckpointStore, CollectionSchema, Comparison, DocumentDatabase,
    DocumentFilter, DocumentQuery, DocumentQueryAccess, IndexDefinition, PartialFilter, Store,
};
use serde_json::json;
use std::{io, path::Path};

mod support;
use support::retry_lock;

fn fields(comparison: Comparison) -> DocumentFilter {
    DocumentFilter::CompareFields {
        left: vec!["usage".into()],
        comparison,
        right: vec!["policy".into(), "limit".into()],
    }
}

fn tenant_filter(comparison: DocumentFilter) -> DocumentFilter {
    DocumentFilter::And(vec![
        DocumentFilter::Equal {
            path: vec!["tenant".into()],
            value: json!("example"),
        },
        comparison,
    ])
}

fn query(filter: DocumentFilter) -> DocumentQuery {
    DocumentQuery {
        filter,
        ..Default::default()
    }
}

fn with_database(
    path: &Path,
    checkpoint: &Path,
    digest: &[u8; 32],
    checkpoint_backed: bool,
    action: impl FnOnce(&DocumentDatabase<'_>),
) {
    if checkpoint_backed {
        let store = retry_lock(|| CheckpointStore::open(path, checkpoint, digest)).unwrap();
        action(&DocumentDatabase::with_checkpoint_store(&store));
    } else {
        let store = retry_lock(|| Store::open(path)).unwrap();
        action(&DocumentDatabase::new(&store));
    }
}

#[test]
fn field_comparison_agrees_with_scan_and_survives_recovery() {
    for checkpoint_backed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "tenant_with_policy".into(),
                fields: vec![vec!["tenant".into()]],
                unique: false,
                partial: Some(PartialFilter::Exists {
                    path: vec!["policy".into()],
                    exists: true,
                }),
            },
        )
        .unwrap();
        for (id, document) in [
            (
                "a",
                json!({"tenant":"example","usage":2,"policy":{"limit":7},"bill":2}),
            ),
            (
                "b",
                json!({"tenant":"example","usage":10,"policy":{"limit":10}}),
            ),
            (
                "c",
                json!({"tenant":"example","usage":null,"policy":{"limit":false}}),
            ),
            ("d", json!({"tenant":"example","policy":{"limit":7}})),
            ("e", json!({"tenant":"example","usage":4})),
            (
                "f",
                json!({"tenant":"example","usage":2,"policy":{"limit":2.0}}),
            ),
            (
                "g",
                json!({"tenant":"example","usage":"λ","policy":{"limit":"中"}}),
            ),
            (
                "h",
                json!({"tenant":"example","usage":[1],"policy":{"limit":7}}),
            ),
            (
                "i",
                json!({"tenant":"example","usage":4,"policy":{"limit":"x".repeat(257)}}),
            ),
            (
                "j",
                json!({"tenant":"example","usage":5,"policy":{"limit":1}}),
            ),
            (
                "z",
                json!({"tenant":"other","usage":1,"policy":{"limit":8}}),
            ),
        ] {
            db.insert("c", id, &document).unwrap();
        }
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);

        let access = DocumentQueryAccess::PartialEqualityIndex {
            index: "tenant_with_policy".into(),
            fields: 1,
        };
        with_database(&path, &checkpoint, &digest, checkpoint_backed, |db| {
            for (comparison, ids) in [
                (Comparison::Less, &["a", "c", "g"][..]),
                (Comparison::LessOrEqual, &["a", "b", "c", "g"][..]),
                (Comparison::Greater, &["j"][..]),
                (Comparison::GreaterOrEqual, &["b", "j"][..]),
            ] {
                let request = query(tenant_filter(fields(comparison)));
                let scan = db.query("c", &request).unwrap();
                let planned = db.query_planned("c", &request).unwrap();
                assert_eq!(planned.access, access);
                assert_eq!(planned.result.documents, scan.documents);
                assert_eq!(
                    planned
                        .result
                        .documents
                        .iter()
                        .map(|row| row.id.as_str())
                        .collect::<Vec<_>>(),
                    ids,
                );
                assert_eq!(planned.result.plan.scanned, 9);
            }
            let request = query(tenant_filter(DocumentFilter::Not(Box::new(fields(
                Comparison::Less,
            )))));
            let planned = db.query_planned("c", &request).unwrap();
            assert_eq!(planned.access, DocumentQueryAccess::CollectionScan);
            assert_eq!(planned.result, db.query("c", &request).unwrap());
            assert!(planned.result.documents.iter().any(|row| row.id == "e"));

            let request = AggregateStatsRequest {
                filter: tenant_filter(fields(Comparison::Less)),
                group_by: None,
                fields: vec![vec!["bill".into()]],
                max_scanned: 16,
            };
            let planned = db.aggregate_stats_planned("c", &request).unwrap();
            assert_eq!(planned.access, access);
            assert_eq!(
                planned.result.groups,
                db.aggregate_stats("c", &request).unwrap().groups
            );
            assert_eq!(planned.result.groups[0].count, 3);
            assert_eq!(planned.result.groups[0].fields[0].sum, 2);
            assert_eq!(
                db.aggregate_stats_planned(
                    "c",
                    &AggregateStatsRequest {
                        max_scanned: 8,
                        ..request
                    }
                )
                .unwrap_err()
                .kind(),
                io::ErrorKind::InvalidInput
            );

            db.transaction_with_read_set(|tx| {
                tx.replace(
                    "c",
                    "e",
                    &json!({"tenant":"example","usage":4,"policy":{"limit":9}}),
                    false,
                )?;
                let planned =
                    tx.query_planned("c", &query(tenant_filter(fields(Comparison::Less))))?;
                assert_eq!(planned.access, access);
                assert!(planned.result.documents.iter().any(|row| row.id == "e"));
                Ok(())
            })
            .unwrap();
        });
        with_database(&path, &checkpoint, &digest, checkpoint_backed, |db| {
            let request = query(tenant_filter(fields(Comparison::Less)));
            let planned = db.query_planned("c", &request).unwrap();
            assert_eq!(planned.access, access);
            assert_eq!(
                planned.result.documents,
                db.query("c", &request).unwrap().documents
            );
            assert_eq!(
                planned
                    .result
                    .documents
                    .iter()
                    .map(|row| row.id.as_str())
                    .collect::<Vec<_>>(),
                ["a", "c", "e", "g"],
            );
        });
    }
}

#[test]
fn field_comparison_paths_are_validated_and_relative_to_array_elements() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("store")).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"samples":[null, {"usage":7,"policy":{"limit":4}}, {"usage":1,"policy":{"limit":8}}]})).unwrap();
    db.insert(
        "c",
        "b",
        &json!({"samples":[{"usage":1},{"policy":{"limit":8}}]}),
    )
    .unwrap();

    let request = query(DocumentFilter::ArrayElementMatch {
        path: vec!["samples".into()],
        filter: Box::new(fields(Comparison::Less)),
    });
    let result = db.query("c", &request).unwrap();
    assert_eq!(result.documents.len(), 1);
    assert_eq!(result.documents[0].id, "a");
    assert_eq!(db.query_planned("c", &request).unwrap().result, result);

    let mut invalid = request;
    let DocumentFilter::ArrayElementMatch { filter, .. } = &mut invalid.filter else {
        unreachable!()
    };
    let DocumentFilter::CompareFields { right, .. } = filter.as_mut() else {
        unreachable!()
    };
    right.clear();
    assert_eq!(
        db.query("c", &invalid).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        db.explain_query("c", &invalid).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}
