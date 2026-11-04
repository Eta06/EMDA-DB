#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, Comparison, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, DocumentSort, IndexDefinition, IndexValue, PartialFilter, Store,
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
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], checkpoint_mode: bool) -> Backend {
    if checkpoint_mode {
        Backend::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(path).unwrap())
    }
}
fn eq(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: vec![name.into()],
        value,
    }
}
fn exists(name: &str, present: bool) -> DocumentFilter {
    DocumentFilter::Exists {
        path: vec![name.into()],
        exists: present,
    }
}
fn compare(name: &str, comparison: Comparison, value: IndexValue) -> DocumentFilter {
    DocumentFilter::Compare {
        path: vec![name.into()],
        comparison,
        value,
    }
}
fn document(id: u8, byte: u8) -> Value {
    let mut doc = json!({"id":format!("id{id}")});
    if byte & 1 != 0 {
        doc["tenant"] = json!(if byte & 2 != 0 { "t" } else { "u" });
    }
    match (byte >> 2) % 6 {
        0 => {}
        1 => doc["score"] = Value::Null,
        2 => doc["score"] = json!(false),
        3 => doc["score"] = json!(i64::from(byte % 5) - 2),
        4 => doc["score"] = json!("s"),
        _ => doc["score"] = json!(true),
    }
    if byte & 16 != 0 {
        doc["suffix"] = if byte & 32 != 0 {
            Value::Null
        } else {
            json!(true)
        };
    }
    match byte % 4 {
        0 => {}
        1 => doc["active"] = json!(true),
        2 => doc["active"] = json!(false),
        _ => doc["active"] = Value::Null,
    }
    doc
}
fn access(prefix: usize, partial: bool) -> DocumentQueryAccess {
    if partial {
        DocumentQueryAccess::PartialCompoundRangeIndex {
            index: "i".into(),
            equality_fields: prefix,
        }
    } else {
        DocumentQueryAccess::CompoundRangeIndex {
            index: "i".into(),
            equality_fields: prefix,
        }
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
    let partial = data[0] & 2 != 0;
    let mut store = open(&path, &checkpoint, &digest, checkpoint_mode);
    store
        .db()
        .create_collection("c", CollectionSchema::default())
        .unwrap();
    store
        .db()
        .create_index(
            "c",
            IndexDefinition {
                name: "i".into(),
                fields: vec![
                    vec!["tenant".into()],
                    vec!["score".into()],
                    vec!["suffix".into()],
                ],
                unique: false,
                partial: partial.then(|| PartialFilter::Equals {
                    path: vec!["active".into()],
                    value: IndexValue::Boolean(true),
                }),
            },
        )
        .unwrap();
    for id in 0..6 {
        store
            .db()
            .insert("c", &id.to_string(), &document(id, id * 13 + 3))
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
        let mode = bytes[0] % 10;
        let bound = match bytes[2] % 4 {
            0 => IndexValue::Null,
            1 => IndexValue::Boolean(false),
            2 => IndexValue::Integer(i64::from(bytes[3] % 5) - 2),
            _ => IndexValue::String("s".into()),
        };
        let score_range = compare("score", Comparison::GreaterOrEqual, bound);
        let mut filters = vec![eq("tenant", json!("t")), score_range.clone()];
        let expected = match mode {
            0 => {
                filters.push(exists("suffix", true));
                access(1, partial)
            }
            1 => DocumentQueryAccess::CollectionScan,
            2 => {
                filters.push(exists("suffix", true));
                if partial {
                    DocumentQueryAccess::CollectionScan
                } else {
                    access(1, false)
                }
            }
            3 => {
                filters.push(DocumentFilter::Or(vec![
                    exists("suffix", true),
                    DocumentFilter::All,
                ]));
                DocumentQueryAccess::CollectionScan
            }
            4 => {
                filters.push(DocumentFilter::Not(Box::new(exists("suffix", false))));
                DocumentQueryAccess::CollectionScan
            }
            5 => {
                filters = vec![
                    compare(
                        "tenant",
                        Comparison::GreaterOrEqual,
                        IndexValue::String("t".into()),
                    ),
                    exists("score", true),
                    exists("suffix", true),
                ];
                access(0, partial)
            }
            6 => {
                filters.push(eq("suffix", Value::Null));
                access(1, partial)
            }
            7 => {
                filters.push(compare(
                    "suffix",
                    Comparison::GreaterOrEqual,
                    IndexValue::Null,
                ));
                access(1, partial)
            }
            8 => {
                filters = vec![
                    eq("tenant", json!("t")),
                    DocumentFilter::Or(vec![score_range, DocumentFilter::All]),
                    exists("suffix", true),
                ];
                DocumentQueryAccess::CollectionScan
            }
            _ => {
                filters = vec![
                    eq("tenant", json!("t")),
                    DocumentFilter::Not(Box::new(score_range)),
                    exists("suffix", true),
                ];
                DocumentQueryAccess::CollectionScan
            }
        };
        if partial && mode != 2 {
            filters.push(eq("active", json!(true)));
        }
        let query = DocumentQuery {
            filter: DocumentFilter::And(filters),
            max_scanned: 16,
            limit: 1 + usize::from(bytes[3] % 5),
            offset: usize::from(bytes[2] % 3),
            sort: (bytes[3] & 4 != 0).then(|| DocumentSort {
                path: vec!["score".into()],
                descending: bytes[3] & 8 != 0,
            }),
            projection: (bytes[3] & 16 != 0)
                .then(|| vec![vec!["id".into()], vec!["tenant".into()]]),
        };
        let scan = store.db().query("c", &query).unwrap();
        let planned = store.db().query_planned("c", &query).unwrap();
        assert_eq!(planned.access, expected);
        assert_eq!(planned.result.documents, scan.documents);
        assert_eq!(planned.result.plan.matched, scan.plan.matched);
        assert_eq!(planned.result.plan.returned, scan.plan.returned);
        assert_eq!(planned.result.plan.sorted, scan.plan.sorted);
    }
});
