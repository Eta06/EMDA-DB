//! Same-element array filtering, shared budgets and transaction safety.
use database_engine::{
    AggregateRequest, CheckpointStore, CollectionSchema, Comparison, DocumentDatabase,
    DocumentFilter as F, DocumentQuery, DocumentQueryAccess, IndexDefinition, IndexValue, Store,
};
use serde_json::{Value, json};
use std::{io::ErrorKind, sync::Barrier};

mod support;
use support::retry_lock;

fn path(name: &str) -> Vec<String> {
    name.split('.').map(str::to_owned).collect()
}
fn eq(name: &str, value: Value) -> F {
    F::Equal {
        path: path(name),
        value,
    }
}
fn elem(name: &str, filter: F) -> F {
    F::ArrayElementMatch {
        path: path(name),
        filter: Box::new(filter),
    }
}
fn query(filter: F) -> DocumentQuery {
    DocumentQuery {
        filter,
        max_scanned: 32,
        ..Default::default()
    }
}
fn match_red_large() -> F {
    elem(
        "items",
        F::And(vec![
            eq("color", json!("red")),
            F::Compare {
                path: path("qty"),
                comparison: Comparison::GreaterOrEqual,
                value: IndexValue::Integer(5),
            },
        ]),
    )
}
fn ids(db: &DocumentDatabase<'_>, filter: F) -> Vec<String> {
    let q = query(filter);
    let scan = db.query("c", &q).unwrap();
    let planned = db.query_planned("c", &q).unwrap();
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
    scan.documents.into_iter().map(|d| d.id).collect()
}
fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    for (name, unique) in [("color", false), ("handle", true)] {
        db.create_index(
            "c",
            IndexDefinition {
                name: name.into(),
                fields: vec![path(name)],
                unique,
                partial: None,
            },
        )
        .unwrap();
    }
    for (id, doc) in [
        (
            "a",
            json!({"handle":"a","color":"red","items":[{"color":"red","qty":1},{"color":"blue","qty":9}],"amount":3}),
        ),
        (
            "b",
            json!({"handle":"b","color":"blue","items":[{"color":"red","qty":7,"meta":{"v":null}},{"color":"blue","qty":3}],"amount":5}),
        ),
        (
            "c",
            json!({"handle":"c","color":"red","items":[null,42,"x",{"color":"red","qty":8,"sub":[{"x":1},{"x":2}]},{}],"amount":7}),
        ),
        ("d", json!({"handle":"d","color":"red","items":[null,42]})),
        ("e", json!({"handle":"e","color":"red","items":[]})),
        ("m", json!({"handle":"m","color":"red"})),
        ("n", json!({"handle":"n","color":"red","items":null})),
        (
            "s",
            json!({"handle":"s","color":"red","items":{"color":"red"}}),
        ),
    ] {
        db.insert("c", id, &doc).unwrap();
    }
}
fn exercise(db: &DocumentDatabase<'_>) {
    assert_eq!(ids(db, match_red_large()), ["b", "c"]);
    assert_eq!(ids(db, elem("items", eq("meta.v", Value::Null))), ["b"]);
    assert_eq!(
        ids(db, elem("items", elem("sub", eq("x", json!(2))))),
        ["c"]
    );
    assert_eq!(
        ids(
            db,
            elem(
                "items",
                F::Not(Box::new(F::Exists {
                    path: path("color"),
                    exists: true
                }))
            )
        ),
        ["c"]
    );
    assert_eq!(ids(db, elem("items", F::All)), ["a", "b", "c"]);

    // The nested `color` does not constrain the root color index. In particular,
    // b's root color is blue even though a matching element has red color.
    let nested = db.query_planned("c", &query(match_red_large())).unwrap();
    assert_eq!(nested.access, DocumentQueryAccess::CollectionScan);
    let indexed = query(F::And(vec![eq("handle", json!("b")), match_red_large()]));
    let result = db.query_planned("c", &indexed).unwrap();
    assert_eq!(
        result.access,
        DocumentQueryAccess::UniqueEqualityIndex {
            index: "handle".into()
        }
    );
    assert_eq!(
        result.result.documents,
        db.query("c", &indexed).unwrap().documents
    );
    assert_eq!(result.result.documents[0].id, "b");
    let aggregate = db
        .aggregate(
            "c",
            &AggregateRequest {
                filter: match_red_large(),
                group_by: None,
                sum: Some(path("amount")),
                max_scanned: 32,
            },
        )
        .unwrap();
    assert_eq!(
        (aggregate.groups[0].count, aggregate.groups[0].sum),
        (2, Some(12))
    );
    assert_eq!(db.verify_collection("c").unwrap().documents, 8);
}

#[test]
fn same_element_semantics_planning_and_recovery_on_both_backends() {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            prepare(&db);
            exercise(&db);
        } else {
            let db = DocumentDatabase::new(&store);
            prepare(&db);
            exercise(&db);
            drop(store);
        }
        let recovered = retry_lock(|| Store::open(&source)).unwrap();
        exercise(&DocumentDatabase::new(&recovered));
    }
}

fn bounds(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert(
        "c",
        "a",
        &json!({"items":vec![Value::Null; 31_250], "one":[null]}),
    )
    .unwrap();
    let all_primitive = || F::Not(Box::new(elem("items", F::All)));
    assert_eq!(
        db.query(
            "c",
            &query(F::And((0..32).map(|_| all_primitive()).collect()))
        )
        .unwrap()
        .documents
        .len(),
        1
    );
    let excessive = || {
        let mut filters: Vec<_> = (0..32).map(|_| all_primitive()).collect();
        filters.push(F::Not(Box::new(elem("one", F::All))));
        query(F::And(filters))
    };
    let error = db.query("c", &excessive()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("element visit limit"));
    let err = db
        .transaction(|tx| {
            tx.put_raw(b"rollback", b"not committed")?;
            tx.query("c", &excessive())?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert_eq!(db.transaction(|tx| tx.get_raw(b"rollback")).unwrap(), None);

    let too_many = F::Or(vec![
        F::All,
        elem(
            "items",
            F::In {
                path: path("x"),
                values: vec![json!(0); 257],
            },
        ),
    ]);
    assert_eq!(
        db.query("c", &query(too_many)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let invalid = F::Or(vec![
        F::All,
        F::ArrayElementMatch {
            path: vec![],
            filter: Box::new(F::All),
        },
    ]);
    assert_eq!(
        db.query("c", &query(invalid)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let invalid_scalar = F::Or(vec![
        F::All,
        elem(
            "items",
            F::Compare {
                path: path("x"),
                comparison: Comparison::Greater,
                value: IndexValue::String("x".repeat(257)),
            },
        ),
    ]);
    assert_eq!(
        db.query("c", &query(invalid_scalar)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let mut too_deep = F::All;
    for _ in 0..17 {
        too_deep = elem("items", too_deep);
    }
    assert_eq!(
        db.query("c", &query(too_deep)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

fn phantom(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    let entered = Barrier::new(2);
    let changed = Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            entered.wait();
            db.insert("c", "new", &json!({"items":[{"color":"red","qty":7}]}))
                .unwrap();
            changed.wait();
        });
        let result = db.transaction_with_read_set(|tx| {
            assert!(
                tx.query("c", &query(match_red_large()))?
                    .documents
                    .is_empty()
            );
            entered.wait();
            changed.wait();
            tx.put_raw(b"phantom", b"not committed")?;
            Ok(())
        });
        assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
        writer.join().unwrap();
    });
    assert_eq!(db.transaction(|tx| tx.get_raw(b"phantom")).unwrap(), None);
}

#[test]
fn array_element_budget_and_phantom_rollback_on_both_backends() {
    for checkpoint_mode in [false, true] {
        for check in [bounds as fn(&DocumentDatabase<'_>), phantom] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("store");
            let checkpoint = root.path().join("checkpoint");
            let store = Store::open(&source).unwrap();
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            if checkpoint_mode {
                drop(store);
                let store =
                    retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
                check(&DocumentDatabase::with_checkpoint_store(&store));
            } else {
                check(&DocumentDatabase::new(&store));
            }
        }
    }
}
