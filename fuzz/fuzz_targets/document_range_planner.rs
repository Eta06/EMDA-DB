#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, IndexValue, Store,
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
fn value(byte: u8) -> IndexValue {
    match byte % 7 {
        0 => IndexValue::Null,
        1 => IndexValue::Boolean(byte & 8 != 0),
        2 | 3 => IndexValue::Integer(i64::from(byte % 9) - 4),
        4 => IndexValue::String("a".into()),
        5 => IndexValue::String("a\0z".into()),
        _ => IndexValue::String("z".into()),
    }
}
fn json_value(value: &IndexValue) -> Value {
    match value {
        IndexValue::Null => Value::Null,
        IndexValue::Boolean(v) => json!(v),
        IndexValue::Integer(v) => json!(v),
        IndexValue::String(v) => json!(v),
    }
}
fn comparison(byte: u8) -> Comparison {
    match byte % 4 {
        0 => Comparison::Less,
        1 => Comparison::LessOrEqual,
        2 => Comparison::Greater,
        _ => Comparison::GreaterOrEqual,
    }
}
fn predicate(name: &str, byte: u8, bound: u8) -> DocumentFilter {
    DocumentFilter::Compare {
        path: vec![name.into()],
        comparison: comparison(byte),
        value: value(bound),
    }
}
fn document(id: u8, byte: u8) -> Value {
    let mut doc = json!({"handle":format!("id{id}"), "tags":[id % 3,"shared"]});
    if byte & 8 != 0 {
        doc["score"] = json_value(&value(byte));
    }
    doc
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
    for (name, unique) in [("score", false), ("handle", true)] {
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
    for id in 0_u8..6 {
        store
            .db()
            .insert(
                "c",
                &id.to_string(),
                &document(id, id.wrapping_mul(7).wrapping_add(8)),
            )
            .unwrap();
    }
    for bytes in data[1..].chunks_exact(4) {
        let id = bytes[1] % 8;
        match bytes[0] % 6 {
            0 | 1 => {
                store
                    .db()
                    .replace("c", &id.to_string(), &document(id, bytes[2]), true)
                    .unwrap();
            }
            2 => {
                store.db().delete("c", &id.to_string()).unwrap();
            }
            3 => {
                drop(store);
                store = open(&path, &checkpoint, &digest, checkpoint_mode);
            }
            _ => {}
        }
        let first = predicate("score", bytes[0], bytes[2]);
        let second = predicate("score", bytes[1], bytes[3]);
        let (filter, expected_access) = match bytes[0] % 8 {
            0 => (
                first,
                DocumentQueryAccess::RangeIndex {
                    index: "score".into(),
                },
            ),
            1 => (
                DocumentFilter::And(vec![first, second]),
                DocumentQueryAccess::RangeIndex {
                    index: "score".into(),
                },
            ),
            2 => (
                DocumentFilter::Or(vec![first, second]),
                DocumentQueryAccess::CollectionScan,
            ),
            3 => (
                DocumentFilter::Not(Box::new(first)),
                DocumentQueryAccess::CollectionScan,
            ),
            4 => (
                DocumentFilter::And(vec![
                    first,
                    DocumentFilter::Equal {
                        path: vec!["handle".into()],
                        value: json!(format!("id{id}")),
                    },
                ]),
                DocumentQueryAccess::UniqueEqualityIndex {
                    index: "handle".into(),
                },
            ),
            5 => (
                DocumentFilter::And(vec![
                    first,
                    DocumentFilter::ArrayContains {
                        path: vec!["tags".into()],
                        value: json!("shared"),
                    },
                ]),
                DocumentQueryAccess::RangeIndex {
                    index: "score".into(),
                },
            ),
            6 => (
                DocumentFilter::And(vec![
                    DocumentFilter::Or(vec![first, DocumentFilter::All]),
                    second,
                ]),
                DocumentQueryAccess::RangeIndex {
                    index: "score".into(),
                },
            ),
            _ => (
                predicate("other", bytes[0], bytes[2]),
                DocumentQueryAccess::CollectionScan,
            ),
        };
        let request = DocumentQuery {
            filter,
            max_scanned: 16,
            limit: 1 + usize::from(bytes[3] % 5),
            offset: usize::from(bytes[2] % 4),
            sort: (bytes[3] & 4 != 0).then(|| DocumentSort {
                path: vec!["handle".into()],
                descending: bytes[3] & 8 != 0,
            }),
            projection: (bytes[3] & 16 != 0)
                .then(|| vec![vec!["handle".into()], vec!["score".into()]]),
        };
        let scan = store.db().query("c", &request).unwrap();
        let planned = store.db().query_planned("c", &request).unwrap();
        assert_eq!(planned.access, expected_access);
        assert_eq!(planned.result.documents, scan.documents);
        assert_eq!(planned.result.plan.matched, scan.plan.matched);
        assert_eq!(planned.result.plan.returned, scan.plan.returned);
        assert_eq!(planned.result.plan.sorted, scan.plan.sorted);
    }
});
