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
        3 => {
            doc["score"] = json!(match byte >> 5 {
                4 => i64::MIN,
                5 => i64::MIN + 1,
                6 => i64::MAX - 1,
                7 => i64::MAX,
                _ => i64::from(byte % 5) - 2,
            })
        }
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
    match (byte / 7) % 4 {
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

fn check_complete_membership(store: &Backend, bytes: &[u8], partial: bool) {
    let mode = bytes[0] % 5;
    let prefixes = match mode {
        2 => 129,
        4 => 2,
        _ => 128,
    };
    let tenants = [json!("u"), json!("t")]
        .into_iter()
        .chain((2..prefixes).map(|ordinal| json!(format!("extra-{ordinal}"))))
        .collect();
    let span = if mode == 4 { 129 } else { 4 };
    let start = match bytes[2] % 4 {
        0 => i64::MIN,
        1 => i64::MAX - span,
        _ => i64::from(bytes[3] % 9) - span / 2,
    };
    let offsets: Vec<_> = match mode {
        0 => vec![1, 0],
        3 => vec![4, 2, 0],
        4 => (0..65).chain(66..=129).rev().collect(),
        _ => vec![4, 1, 0, 1],
    };
    let mut predicates = vec![
        DocumentFilter::In {
            path: vec!["tenant".into()],
            values: tenants,
        },
        DocumentFilter::In {
            path: vec!["score".into()],
            values: offsets
                .into_iter()
                .map(|offset| json!(start + offset))
                .collect(),
        },
    ];
    if partial {
        predicates.push(eq("active", json!(true)));
    }
    let expected = match (mode, partial) {
        (0, false) => DocumentQueryAccess::CompoundMembershipIndex {
            index: "i".into(),
            fields: 2,
            probes: 256,
        },
        (0, true) => DocumentQueryAccess::PartialCompoundMembershipIndex {
            index: "i".into(),
            fields: 2,
            probes: 256,
        },
        (1 | 4, false) => DocumentQueryAccess::CompoundMembershipRangeIndex {
            index: "i".into(),
            prefix_fields: 1,
            probes: 2 * prefixes,
        },
        (1 | 4, true) => DocumentQueryAccess::PartialCompoundMembershipRangeIndex {
            index: "i".into(),
            prefix_fields: 1,
            probes: 2 * prefixes,
        },
        _ => DocumentQueryAccess::CollectionScan,
    };
    let query = DocumentQuery {
        filter: DocumentFilter::And(predicates),
        max_scanned: 16,
        limit: 1 + usize::from(bytes[3] % 5),
        offset: usize::from(bytes[3] % 3),
        sort: (bytes[3] & 4 != 0).then(|| DocumentSort {
            path: vec!["score".into()],
            descending: bytes[3] & 8 != 0,
        }),
        projection: (bytes[3] & 16 != 0).then(|| vec![vec!["id".into()], vec!["tenant".into()]]),
    };
    let db = store.db();
    let scan = db.query("c", &query).unwrap();
    let planned = db.query_planned("c", &query).unwrap();
    assert_eq!(
        planned.access, expected,
        "complete membership mode={mode} partial={partial}"
    );
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
    assert_eq!(planned.result.plan.returned, scan.plan.returned);
    if expected != DocumentQueryAccess::CollectionScan {
        assert_eq!(planned.result.plan.scanned, scan.plan.matched);
    }
}

fn check_range_narrowed_tuple(store: &Backend, bytes: &[u8], partial: bool) {
    let start = match bytes[2] % 5 {
        0 => i64::MIN,
        1 => i64::MAX - 3,
        _ => i64::from(bytes[2] % 7) - 4,
    };
    let suffix = if bytes[3] & 1 == 0 {
        Value::Null
    } else {
        json!(true)
    };
    let mut filters = vec![
        DocumentFilter::In {
            path: vec!["tenant".into()],
            values: vec![json!("t"), json!("u")],
        },
        DocumentFilter::In {
            path: vec!["score".into()],
            values: vec![
                json!(start + 3),
                json!(start + 2),
                json!(start + 1),
                json!(start),
                json!("s"),
            ],
        },
        compare("score", Comparison::Greater, IndexValue::Integer(start)),
        compare("score", Comparison::Less, IndexValue::Integer(start + 3)),
        eq("suffix", suffix),
    ];
    if partial {
        filters.push(eq("active", json!(true)));
    }
    let query = DocumentQuery {
        filter: DocumentFilter::And(filters),
        max_scanned: 16,
        limit: 1 + usize::from(bytes[3] % 5),
        offset: usize::from(bytes[3] % 3),
        sort: Some(DocumentSort {
            path: vec!["score".into()],
            descending: bytes[3] & 2 != 0,
        }),
        projection: (bytes[3] & 4 != 0).then(|| vec![vec!["suffix".into()]]),
    };
    let db = store.db();
    let scan = db.query("c", &query).unwrap();
    let planned = db.query_planned("c", &query).unwrap();
    let expected = if partial {
        DocumentQueryAccess::PartialCompoundMembershipIndex {
            index: "i".into(),
            fields: 3,
            probes: 4,
        }
    } else {
        DocumentQueryAccess::CompoundMembershipIndex {
            index: "i".into(),
            fields: 3,
            probes: 4,
        }
    };
    assert_eq!(planned.access, expected);
    assert_eq!(planned.result.documents, scan.documents);
    assert_eq!(planned.result.plan.scanned, scan.plan.matched);
    assert_eq!(planned.result.plan.matched, scan.plan.matched);
    assert_eq!(planned.result.plan.returned, scan.plan.returned);
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
    let complete_membership = data[0] & 4 != 0;
    let mut store = open(&path, &checkpoint, &digest, checkpoint_mode);
    let mut fields = vec![vec!["tenant".into()], vec!["score".into()]];
    if !complete_membership {
        fields.push(vec!["suffix".into()]);
    }
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
                fields,
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
        if complete_membership {
            check_complete_membership(&store, bytes, partial);
            continue;
        }
        if bytes[0] >= 250 {
            check_range_narrowed_tuple(&store, bytes, partial);
            continue;
        }
        let mode = bytes[0] % 25;
        let bound = match bytes[2] % 4 {
            0 => IndexValue::Null,
            1 => IndexValue::Boolean(false),
            2 => IndexValue::Integer(i64::from(bytes[3] % 5) - 2),
            _ => IndexValue::String("s".into()),
        };
        let score_range = compare("score", Comparison::GreaterOrEqual, bound.clone());
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
                access(1, partial)
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
            9 => {
                filters = vec![
                    eq("tenant", json!("t")),
                    DocumentFilter::Not(Box::new(score_range)),
                    exists("suffix", true),
                ];
                DocumentQueryAccess::CollectionScan
            }
            10 => {
                filters[0] = DocumentFilter::In {
                    path: vec!["tenant".into()],
                    values: vec![json!("u"), json!("t"), json!("u")],
                };
                filters.push(exists("suffix", true));
                if partial {
                    DocumentQueryAccess::PartialCompoundMembershipRangeIndex {
                        index: "i".into(),
                        prefix_fields: 1,
                        probes: 2,
                    }
                } else {
                    DocumentQueryAccess::CompoundMembershipRangeIndex {
                        index: "i".into(),
                        prefix_fields: 1,
                        probes: 2,
                    }
                }
            }
            11 => {
                filters[0] = DocumentFilter::In {
                    path: vec!["tenant".into()],
                    values: vec![json!("t"), json!(["u"])],
                };
                filters.push(exists("suffix", true));
                DocumentQueryAccess::CollectionScan
            }
            12 => {
                filters[0] = DocumentFilter::In {
                    path: vec!["tenant".into()],
                    values: vec![json!("t"), json!("u")],
                };
                DocumentQueryAccess::CollectionScan
            }
            13 | 14 => {
                let probes = if mode == 13 {
                    1
                } else {
                    filters[0] = DocumentFilter::In {
                        path: vec!["tenant".into()],
                        values: vec![json!("t"), json!("u"), json!("t")],
                    };
                    2
                };
                filters[1] = exists("score", true);
                filters.push(exists("suffix", true));
                DocumentQueryAccess::CompoundPrefixIndex {
                    index: "i".into(),
                    prefix_fields: 1,
                    probes,
                    partial,
                }
            }
            15 => {
                filters[1] = DocumentFilter::Or(vec![exists("score", true), DocumentFilter::All]);
                filters.push(exists("suffix", true));
                DocumentQueryAccess::CollectionScan
            }
            16 => {
                let tenant = IndexValue::String("t".into());
                filters[0] = DocumentFilter::And(vec![
                    compare("tenant", Comparison::GreaterOrEqual, tenant.clone()),
                    compare("tenant", Comparison::LessOrEqual, tenant),
                ]);
                filters.push(exists("suffix", true));
                access(1, partial)
            }
            17..=19 => {
                if mode == 18 {
                    filters[1] = compare("score", Comparison::Greater, bound.clone());
                }
                filters.push(compare("score", Comparison::LessOrEqual, bound));
                filters.push(eq("suffix", Value::Null));
                match (mode, partial) {
                    (17, true) => DocumentQueryAccess::PartialEqualityIndex {
                        index: "i".into(),
                        fields: 3,
                    },
                    (17, false) => DocumentQueryAccess::CompoundEqualityIndex {
                        index: "i".into(),
                        fields: 3,
                    },
                    (18, _) => access(1, partial),
                    _ => {
                        filters[0] = DocumentFilter::In {
                            path: vec!["tenant".into()],
                            values: vec![json!("t"), json!("u")],
                        };
                        if partial {
                            DocumentQueryAccess::PartialCompoundMembershipIndex {
                                index: "i".into(),
                                fields: 3,
                                probes: 2,
                            }
                        } else {
                            DocumentQueryAccess::CompoundMembershipIndex {
                                index: "i".into(),
                                fields: 3,
                                probes: 2,
                            }
                        }
                    }
                }
            }
            20..=22 => {
                filters[0] =
                    DocumentFilter::Or(vec![eq("tenant", json!("t")), eq("tenant", json!("u"))]);
                let second = match (mode, bytes[3] % 3) {
                    (22, _) => json!(1.0),
                    (_, 0) => json!(true),
                    (_, 1) => json!(i64::from(bytes[3] % 5) - 2),
                    _ => json!("s"),
                };
                filters[1] = DocumentFilter::Or(vec![
                    eq("score", Value::Null),
                    DocumentFilter::In {
                        path: vec!["score".into()],
                        values: vec![second, Value::Null],
                    },
                ]);
                if mode == 21 {
                    filters.push(compare(
                        "suffix",
                        Comparison::GreaterOrEqual,
                        IndexValue::Null,
                    ));
                } else {
                    filters.push(eq("suffix", Value::Null));
                }
                match (mode, partial) {
                    (20, false) => DocumentQueryAccess::CompoundMembershipIndex {
                        index: "i".into(),
                        fields: 3,
                        probes: 4,
                    },
                    (20, true) => DocumentQueryAccess::PartialCompoundMembershipIndex {
                        index: "i".into(),
                        fields: 3,
                        probes: 4,
                    },
                    (21, false) => DocumentQueryAccess::CompoundMembershipRangeIndex {
                        index: "i".into(),
                        prefix_fields: 2,
                        probes: 4,
                    },
                    (21, true) => DocumentQueryAccess::PartialCompoundMembershipRangeIndex {
                        index: "i".into(),
                        prefix_fields: 2,
                        probes: 4,
                    },
                    _ => DocumentQueryAccess::DisjunctionIndex {
                        indexes: vec!["i".into()],
                        probes: 2,
                    },
                }
            }
            23 | 24 => {
                let first = match bytes[2] % 4 {
                    0 => i64::MIN,
                    1 => i64::MAX - 4,
                    _ => i64::from(bytes[3] % 7) - 5,
                };
                let upper = if mode == 23 {
                    Comparison::LessOrEqual
                } else {
                    Comparison::Less
                };
                filters = vec![
                    DocumentFilter::In {
                        path: vec!["tenant".into()],
                        values: vec![json!("u"), json!("t"), json!("u")],
                    },
                    DocumentFilter::In {
                        path: vec!["score".into()],
                        values: vec![
                            json!(first + 4),
                            json!(first),
                            json!(first + 1),
                            json!(first + 3),
                            json!(first),
                            Value::Null,
                        ],
                    },
                    compare(
                        "score",
                        Comparison::GreaterOrEqual,
                        IndexValue::Integer(first),
                    ),
                    compare("score", upper, IndexValue::Integer(first + 4)),
                    exists("suffix", true),
                ];
                if partial {
                    DocumentQueryAccess::PartialCompoundMembershipRangeIndex {
                        index: "i".into(),
                        prefix_fields: 1,
                        probes: 4,
                    }
                } else {
                    DocumentQueryAccess::CompoundMembershipRangeIndex {
                        index: "i".into(),
                        prefix_fields: 1,
                        probes: 4,
                    }
                }
            }
            _ => unreachable!("range model mode"),
        };
        if partial && mode != 2 {
            filters.push(eq("active", json!(true)));
        }
        let query = DocumentQuery {
            filter: DocumentFilter::And(filters),
            max_scanned: 16,
            limit: 1 + usize::from(bytes[3] % 5),
            offset: usize::from(bytes[3] % 3),
            sort: (bytes[3] & 4 != 0).then(|| DocumentSort {
                path: vec!["score".into()],
                descending: bytes[3] & 8 != 0,
            }),
            projection: (bytes[3] & 16 != 0)
                .then(|| vec![vec!["id".into()], vec!["tenant".into()]]),
        };
        let scan = store.db().query("c", &query).unwrap();
        let planned = store.db().query_planned("c", &query).unwrap();
        assert_eq!(
            planned.access, expected,
            "mode={mode} checkpoint={checkpoint_mode} partial={partial} query={query:?}"
        );
        assert_eq!(planned.result.documents, scan.documents);
        assert_eq!(planned.result.plan.matched, scan.plan.matched);
        assert_eq!(planned.result.plan.returned, scan.plan.returned);
        assert_eq!(planned.result.plan.sorted, scan.plan.sorted);
        if mode >= 23 {
            assert_eq!(planned.result.plan.scanned, scan.plan.matched);
        }
    }
});
