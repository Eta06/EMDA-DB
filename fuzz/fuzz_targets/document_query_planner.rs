#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};
use std::path::Path;

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn db(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }
}
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], use_checkpoint: bool) -> Backend {
    if use_checkpoint {
        Backend::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(path).unwrap())
    }
}
fn equal(path: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: vec![path.into()],
        value,
    }
}
fn filter(byte: u8) -> (DocumentFilter, bool) {
    match byte % 10 {
        0 => (equal("email", json!("e0")), true),
        1 => (equal("email", Value::Null), true),
        2 => (equal("email", json!([1])), false),
        3 => (
            DocumentFilter::And(vec![
                equal("score", json!(i64::from(byte % 4))),
                equal("email", json!("e1")),
            ]),
            true,
        ),
        4 => (
            DocumentFilter::Or(vec![
                equal("email", json!("e0")),
                equal("email", json!("e1")),
            ]),
            false,
        ),
        5 => (
            DocumentFilter::Not(Box::new(equal("email", json!("e0")))),
            false,
        ),
        6 => (
            DocumentFilter::And(vec![
                equal("email", json!("e0")),
                DocumentFilter::Or(vec![DocumentFilter::All, equal("score", json!(2))]),
            ]),
            true,
        ),
        7 => (
            DocumentFilter::And(vec![
                DocumentFilter::Or(vec![equal("email", json!("e0")), DocumentFilter::All]),
                equal("score", json!(2)),
            ]),
            false,
        ),
        8 => (equal("score", json!(i64::from(byte % 4))), false),
        _ => (
            DocumentFilter::And(vec![
                equal("email", json!("e0")),
                equal("email", json!("e1")),
            ]),
            true,
        ),
    }
}
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let checkpoint_mode = data[0] & 1 != 0;
    let mut store = open(&path, &checkpoint, &digest, checkpoint_mode);
    store
        .db()
        .create_collection("c", CollectionSchema::default())
        .unwrap();
    for (name, unique) in [("email", true), ("score", false)] {
        store
            .db()
            .create_index(
                "c",
                IndexDefinition {
                    name: name.into(),
                    fields: vec![vec![name.into()]],
                    unique,
                    partial: None,
                },
            )
            .unwrap();
    }
    for (id, email, score) in [
        ("0", Some(json!("e0")), 0),
        ("1", Some(json!("e1")), 1),
        ("2", Some(Value::Null), 2),
        ("3", None, 3),
    ] {
        let mut document = json!({"score":score});
        if let Some(email) = email {
            document["email"] = email;
        }
        store.db().insert("c", id, &document).unwrap();
    }
    for bytes in data[1..].chunks_exact(2) {
        if bytes[0] % 11 == 0 {
            drop(store);
            store = open(&path, &checkpoint, &digest, checkpoint_mode);
        }
        if bytes[0] % 7 == 0 {
            store
                .db()
                .replace(
                    "c",
                    "0",
                    &json!({"email":"e0", "score":bytes[1] % 4}),
                    false,
                )
                .unwrap();
        }
        let (filter, indexed) = filter(bytes[0]);
        let query = DocumentQuery {
            filter,
            max_scanned: 16,
            limit: 1 + usize::from(bytes[1] % 4),
            offset: usize::from(bytes[1] % 3),
            sort: (bytes[1] & 1 != 0).then(|| DocumentSort {
                path: vec!["score".into()],
                descending: bytes[1] & 2 != 0,
            }),
            projection: (bytes[1] & 4 != 0).then(|| vec![vec!["email".into()]]),
        };
        let scan = store.db().query("c", &query).unwrap();
        let planned = store.db().query_planned("c", &query).unwrap();
        assert_eq!(
            planned.access,
            if indexed {
                DocumentQueryAccess::UniqueEqualityIndex {
                    index: "email".into(),
                }
            } else if matches!(bytes[0] % 10, 7 | 8) {
                DocumentQueryAccess::EqualityIndex {
                    index: "score".into(),
                }
            } else {
                DocumentQueryAccess::CollectionScan
            }
        );
        assert_eq!(planned.result.documents, scan.documents);
        assert_eq!(planned.result.plan.matched, scan.plan.matched);
        assert_eq!(planned.result.plan.returned, scan.plan.returned);
        assert_eq!(planned.result.plan.sorted, scan.plan.sorted);
    }
});
