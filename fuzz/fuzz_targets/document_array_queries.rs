#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, IndexDefinition, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};
use std::collections::BTreeMap;
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
fn value(byte: u8) -> Value {
    match byte % 8 {
        0 => Value::Null,
        1 => json!(true),
        2 => json!(i64::from(byte % 3)),
        3 => json!(1.0),
        4 => json!("a"),
        5 => json!({"x": byte % 3, "y": (byte / 3) % 3}),
        6 => json!([1, 2]),
        _ => json!({"y": byte % 3}),
    }
}
fn document(id: u8, byte: u8) -> Value {
    let mut doc = json!({"handle":format!("id{id}")});
    match byte % 4 {
        0 => {}
        1 => doc["tags"] = Value::Null,
        2 => doc["tags"] = json!("scalar"),
        _ => {
            let values = (0..(byte % 6))
                .map(|n| value(byte.wrapping_add(n)))
                .collect::<Vec<_>>();
            doc["tags"] = Value::Array(values);
        }
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
    store
        .db()
        .create_index(
            "c",
            IndexDefinition {
                name: "handle".into(),
                fields: vec![vec!["handle".into()]],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
    let mut model = BTreeMap::<u8, Value>::new();
    for id in 0..5 {
        let doc = document(id, id.wrapping_mul(7).wrapping_add(3));
        store.db().insert("c", &id.to_string(), &doc).unwrap();
        model.insert(id, doc);
    }
    for bytes in data[1..].chunks_exact(4) {
        let id = bytes[1] % 6;
        match bytes[0] % 5 {
            0 | 1 => {
                let doc = document(id, bytes[2]);
                store
                    .db()
                    .replace("c", &id.to_string(), &doc, true)
                    .unwrap();
                model.insert(id, doc);
            }
            2 => {
                store.db().delete("c", &id.to_string()).unwrap();
                model.remove(&id);
            }
            3 => {
                drop(store);
                store = open(&path, &checkpoint, &digest, checkpoint_mode);
            }
            _ => {}
        }
        let needles = (0..(bytes[3] % 4))
            .map(|n| value(bytes[2].wrapping_add(n)))
            .collect::<Vec<_>>();
        let needle = value(bytes[2]);
        let length = usize::from(bytes[2] % 6);
        let mode = bytes[0] % 6;
        let field = vec!["tags".into()];
        let mut filter = match mode {
            0 => DocumentFilter::ArrayContains {
                path: field,
                value: needle.clone(),
            },
            1 => DocumentFilter::ArrayContainsAny {
                path: field,
                values: needles.clone(),
            },
            2 => DocumentFilter::ArrayContainsAll {
                path: field,
                values: needles.clone(),
            },
            3 => DocumentFilter::ArrayLength {
                path: field,
                length,
            },
            4 => DocumentFilter::ArrayElementMatch {
                path: field,
                filter: Box::new(DocumentFilter::And(vec![
                    DocumentFilter::Equal {
                        path: vec!["x".into()],
                        value: json!(bytes[2] % 3),
                    },
                    DocumentFilter::Equal {
                        path: vec!["y".into()],
                        value: json!(bytes[3] % 3),
                    },
                ])),
            },
            _ => DocumentFilter::ArrayElementMatch {
                path: field,
                filter: Box::new(DocumentFilter::Not(Box::new(DocumentFilter::Exists {
                    path: vec!["x".into()],
                    exists: true,
                }))),
            },
        };
        let negate = bytes[3] & 4 != 0;
        if negate {
            filter = DocumentFilter::Not(Box::new(filter));
        }
        let indexed = bytes[3] & 8 != 0;
        if indexed {
            filter = DocumentFilter::And(vec![
                DocumentFilter::Equal {
                    path: vec!["handle".into()],
                    value: json!(format!("id{id}")),
                },
                filter,
            ]);
        }
        let query = DocumentQuery {
            filter,
            max_scanned: 16,
            ..Default::default()
        };
        let expected = model
            .iter()
            .filter_map(|(row_id, doc)| {
                let matches = match doc.get("tags").and_then(Value::as_array) {
                    None => false,
                    Some(array) => match mode {
                        0 => array.contains(&needle),
                        1 => needles.iter().any(|needle| array.contains(needle)),
                        2 => needles.iter().all(|needle| array.contains(needle)),
                        3 => array.len() == length,
                        4 => array.iter().any(|element| {
                            element.as_object().is_some_and(|object| {
                                object.get("x") == Some(&json!(bytes[2] % 3))
                                    && object.get("y") == Some(&json!(bytes[3] % 3))
                            })
                        }),
                        _ => array.iter().any(|element| {
                            element.as_object().is_some_and(|object| !object.contains_key("x"))
                        }),
                    },
                };
                ((matches ^ negate) && (!indexed || *row_id == id)).then(|| row_id.to_string())
            })
            .collect::<Vec<_>>();
        let scan = store.db().query("c", &query).unwrap();
        let planned = store.db().query_planned("c", &query).unwrap();
        let actual = scan
            .documents
            .iter()
            .map(|row| row.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(planned.result.documents, scan.documents);
        assert_eq!(planned.result.plan.matched, scan.plan.matched);
        assert_eq!(
            planned.access,
            if indexed {
                DocumentQueryAccess::UniqueEqualityIndex {
                    index: "handle".into(),
                }
            } else {
                DocumentQueryAccess::CollectionScan
            }
        );
    }
});
