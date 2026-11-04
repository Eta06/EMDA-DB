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
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], use_checkpoint: bool) -> Backend {
    if use_checkpoint {
        Backend::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(path).unwrap())
    }
}
fn equal(name: &str, value: Value) -> DocumentFilter {
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
fn and(filters: Vec<DocumentFilter>) -> DocumentFilter {
    DocumentFilter::And(filters)
}
fn document(id: u8, byte: u8) -> Value {
    let mut doc = json!({"id":format!("id{id}"), "tags":[id % 3,"shared"]});
    if byte & 8 != 0 {
        doc["tenant"] = json!("t");
    }
    if byte & 16 != 0 {
        doc["group"] = if byte & 32 != 0 {
            json!("g")
        } else {
            Value::Null
        };
    }
    match byte % 5 {
        0 => {}
        1 => doc["active"] = Value::Null,
        2 => doc["active"] = json!(true),
        3 => doc["active"] = json!(false),
        _ => doc["active"] = json!([1, 2]),
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
    let compound = data[0] & 2 != 0;
    let kind = (data[0] >> 2) % 4;
    let (partial, guard, opposite) = match kind {
        0 => (
            PartialFilter::Exists {
                path: vec!["active".into()],
                exists: true,
            },
            exists("active", true),
            exists("active", false),
        ),
        1 => (
            PartialFilter::Exists {
                path: vec!["active".into()],
                exists: false,
            },
            exists("active", false),
            exists("active", true),
        ),
        2 => (
            PartialFilter::Equals {
                path: vec!["active".into()],
                value: IndexValue::Boolean(true),
            },
            equal("active", json!(true)),
            equal("active", json!(false)),
        ),
        _ => (
            PartialFilter::Equals {
                path: vec!["active".into()],
                value: IndexValue::Null,
            },
            equal("active", Value::Null),
            equal("active", json!(false)),
        ),
    };
    let mut store = open(&path, &checkpoint, &digest, checkpoint_mode);
    store
        .db()
        .create_collection("c", CollectionSchema::default())
        .unwrap();
    let fields = if compound {
        vec![vec!["tenant".into()], vec!["group".into()]]
    } else {
        vec![vec!["group".into()]]
    };
    store
        .db()
        .create_index(
            "c",
            IndexDefinition {
                name: "i".into(),
                fields,
                unique: false,
                partial: Some(partial),
            },
        )
        .unwrap();
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
        let mut tuple = vec![equal(
            "group",
            if bytes[2] & 1 == 0 {
                json!("g")
            } else {
                Value::Null
            },
        )];
        if compound {
            tuple.push(equal("tenant", json!("t")));
        }
        let mode = bytes[0] % 8;
        let expected_access = match mode {
            0 | 5 | 7 => {
                tuple.push(if mode == 5 && kind == 0 {
                    equal("active", Value::Null)
                } else {
                    guard.clone()
                });
                if mode == 7 {
                    tuple.push(DocumentFilter::ArrayContains {
                        path: vec!["tags".into()],
                        value: json!("shared"),
                    });
                }
                DocumentQueryAccess::PartialEqualityIndex {
                    index: "i".into(),
                    fields: if compound { 2 } else { 1 },
                }
            }
            1 => DocumentQueryAccess::CollectionScan,
            2 => {
                tuple.push(opposite.clone());
                DocumentQueryAccess::CollectionScan
            }
            3 => {
                tuple.push(DocumentFilter::Or(vec![guard.clone(), DocumentFilter::All]));
                DocumentQueryAccess::CollectionScan
            }
            4 => {
                tuple.push(DocumentFilter::Not(Box::new(opposite.clone())));
                DocumentQueryAccess::CollectionScan
            }
            _ => {
                tuple = vec![
                    DocumentFilter::Compare {
                        path: vec!["group".into()],
                        comparison: Comparison::GreaterOrEqual,
                        value: IndexValue::String("g".into()),
                    },
                    guard.clone(),
                ];
                if compound {
                    DocumentQueryAccess::CollectionScan
                } else {
                    DocumentQueryAccess::PartialRangeIndex { index: "i".into() }
                }
            }
        };
        let request = DocumentQuery {
            filter: and(tuple),
            max_scanned: 16,
            limit: 1 + usize::from(bytes[3] % 5),
            offset: usize::from(bytes[2] % 4),
            sort: (bytes[3] & 4 != 0).then(|| DocumentSort {
                path: vec!["id".into()],
                descending: bytes[3] & 8 != 0,
            }),
            projection: (bytes[3] & 16 != 0).then(|| vec![vec!["id".into()], vec!["group".into()]]),
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
