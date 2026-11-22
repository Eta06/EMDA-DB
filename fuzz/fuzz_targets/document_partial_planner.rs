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
    let mut doc = json!({"id":format!("id{id}")});
    match (byte >> 5) % 4 {
        0 => {}
        1 => doc["tags"] = Value::Null,
        2 => doc["tags"] = json!([]),
        _ => doc["tags"] = json!([id % 3, "shared", {"x": id % 2}]),
    }
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
    match byte % 8 {
        0 => {}
        1 => doc["active"] = Value::Null,
        2 => doc["active"] = json!(true),
        3 => doc["active"] = json!(false),
        4 => doc["active"] = json!([1, 2]),
        5 => doc["active"] = json!(1),
        6 => doc["active"] = json!(1.0),
        _ => doc["active"] = json!(2),
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
    let kind = (data[0] >> 2) % 6;
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
        3 => (
            PartialFilter::Equals {
                path: vec!["active".into()],
                value: IndexValue::Null,
            },
            equal("active", Value::Null),
            equal("active", json!(false)),
        ),
        4 => (
            PartialFilter::Exists {
                path: vec!["tags".into()],
                exists: true,
            },
            DocumentFilter::ArrayElementMatch {
                path: vec!["tags".into()],
                filter: Box::new(equal("x", json!(1))),
            },
            exists("tags", false),
        ),
        _ => (
            PartialFilter::Equals {
                path: vec!["active".into()],
                value: IndexValue::Integer(1),
            },
            equal("active", json!(1)),
            equal("active", json!(1.0)),
        ),
    };
    let guard_value = match &partial {
        PartialFilter::Equals { value, .. } => Some(value.clone()),
        _ => None,
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
        let mode = bytes[0] % 18;
        let partial_equality = || DocumentQueryAccess::PartialEqualityIndex {
            index: "i".into(),
            fields: if compound { 2 } else { 1 },
        };
        let not = |filter| DocumentFilter::Not(Box::new(filter));
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
                tuple.push(not(opposite.clone()));
                if matches!(kind, 0 | 1 | 4) {
                    partial_equality()
                } else {
                    DocumentQueryAccess::CollectionScan
                }
            }
            11 if kind < 2 => {
                tuple.push(not(DocumentFilter::Or(vec![
                    opposite.clone(),
                    exists("group", false),
                ])));
                partial_equality()
            }
            12 if kind < 2 => {
                tuple.push(not(DocumentFilter::And(vec![
                    opposite.clone(),
                    exists("group", true),
                ])));
                DocumentQueryAccess::CollectionScan
            }
            13 => {
                tuple.push(not(not(guard.clone())));
                partial_equality()
            }
            14..=16 if guard_value.is_some() => {
                let bound = |comparison| DocumentFilter::Compare {
                    path: vec!["active".into()],
                    comparison,
                    value: guard_value.clone().unwrap(),
                };
                match mode {
                    14 => {
                        for predicate in &mut tuple {
                            *predicate = not(not(predicate.clone()));
                        }
                        tuple.push(not(DocumentFilter::Or(vec![
                            not(bound(Comparison::GreaterOrEqual)),
                            not(bound(Comparison::LessOrEqual)),
                        ])));
                        partial_equality()
                    }
                    15 => {
                        let DocumentFilter::Equal { value, .. } = &guard else {
                            unreachable!("typed guard");
                        };
                        let typed = bytes[3] & 1 == 0;
                        let second = if typed { json!("higher") } else { json!(1.0) };
                        tuple.push(bound(Comparison::LessOrEqual));
                        tuple.push(not(DocumentFilter::NotIn {
                            path: vec!["active".into()],
                            values: vec![value.clone(), second],
                        }));
                        if typed {
                            partial_equality()
                        } else {
                            DocumentQueryAccess::CollectionScan
                        }
                    }
                    _ => {
                        tuple.push(not(DocumentFilter::Or(vec![
                            bound(Comparison::Less),
                            bound(Comparison::Greater),
                        ])));
                        DocumentQueryAccess::CollectionScan
                    }
                }
            }
            17 => {
                tuple[0] = not(DocumentFilter::NotIn {
                    path: vec!["group".into()],
                    values: vec![json!("g"), Value::Null, json!("g")],
                });
                tuple.push(guard.clone());
                if compound {
                    DocumentQueryAccess::PartialCompoundMembershipIndex {
                        index: "i".into(),
                        fields: 2,
                        probes: 2,
                    }
                } else {
                    DocumentQueryAccess::PartialMembershipIndex {
                        index: "i".into(),
                        probes: 2,
                    }
                }
            }
            8 | 9 | 10 if guard_value.is_some() => {
                let bound = |comparison| DocumentFilter::Compare {
                    path: vec!["active".into()],
                    comparison,
                    value: guard_value.clone().unwrap(),
                };
                tuple.push(bound(Comparison::LessOrEqual));
                let alternate = match mode {
                    8 => and(vec![guard.clone(), exists("group", true)]),
                    9 => DocumentFilter::All,
                    _ => {
                        let DocumentFilter::Equal { value, .. } = &guard else {
                            unreachable!("typed guard");
                        };
                        DocumentFilter::In {
                            path: vec!["active".into()],
                            values: vec![value.clone(), value.clone()],
                        }
                    }
                };
                tuple.push(DocumentFilter::Or(vec![
                    bound(Comparison::GreaterOrEqual),
                    alternate,
                ]));
                if mode == 9 {
                    DocumentQueryAccess::CollectionScan
                } else {
                    DocumentQueryAccess::PartialEqualityIndex {
                        index: "i".into(),
                        fields: if compound { 2 } else { 1 },
                    }
                }
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
