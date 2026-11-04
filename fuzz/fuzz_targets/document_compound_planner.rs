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
fn equal(name: &str, value: Value) -> DocumentFilter {
    DocumentFilter::Equal {
        path: vec![name.into()],
        value,
    }
}
fn inside(name: &str, first: Value, second: Value) -> (DocumentFilter, usize) {
    let distinct = if first == second { 1 } else { 2 };
    (
        DocumentFilter::In {
            path: vec![name.into()],
            values: vec![first, second],
        },
        distinct,
    )
}
fn and(filters: Vec<DocumentFilter>) -> DocumentFilter {
    DocumentFilter::And(filters)
}
fn value(byte: u8) -> Value {
    match byte % 5 {
        0 => Value::Null,
        1 => json!("t"),
        2 => json!("red"),
        3 => json!("a\u{0}z"),
        _ => json!("other"),
    }
}
fn document(id: u8, byte: u8) -> Value {
    let mut doc = json!({"email":format!("id{id}"), "tags":[id % 3,"shared"]});
    if byte & 8 != 0 {
        doc["tenant"] = value(byte);
    }
    if byte & 16 != 0 {
        doc["category"] = value(byte.wrapping_add(2));
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
    for (name, fields, unique) in [
        (
            "pair",
            vec![vec!["tenant".into()], vec!["category".into()]],
            false,
        ),
        (
            "tenant_email",
            vec![vec!["tenant".into()], vec!["email".into()]],
            true,
        ),
    ] {
        store
            .db()
            .create_index(
                "c",
                IndexDefinition {
                    name: name.into(),
                    fields,
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
        let tenant = equal("tenant", value(bytes[2]));
        let category = equal("category", value(bytes[3]));
        let email = equal("email", json!(format!("id{id}")));
        let (filter, expected_access) = match bytes[0] % 12 {
            0 => (
                and(vec![tenant, category]),
                DocumentQueryAccess::CompoundEqualityIndex {
                    index: "pair".into(),
                    fields: 2,
                },
            ),
            1 => (
                and(vec![tenant, email]),
                DocumentQueryAccess::UniqueCompoundEqualityIndex {
                    index: "tenant_email".into(),
                    fields: 2,
                },
            ),
            2 => (tenant, DocumentQueryAccess::CollectionScan),
            3 => (
                and(vec![
                    tenant,
                    DocumentFilter::Or(vec![category, DocumentFilter::All]),
                ]),
                DocumentQueryAccess::CollectionScan,
            ),
            4 => (
                DocumentFilter::Not(Box::new(and(vec![tenant, category]))),
                DocumentQueryAccess::CollectionScan,
            ),
            5 => (
                and(vec![tenant, category, email]),
                DocumentQueryAccess::UniqueCompoundEqualityIndex {
                    index: "tenant_email".into(),
                    fields: 2,
                },
            ),
            6 => (
                and(vec![
                    tenant,
                    category,
                    DocumentFilter::ArrayContains {
                        path: vec!["tags".into()],
                        value: json!("shared"),
                    },
                ]),
                DocumentQueryAccess::CompoundEqualityIndex {
                    index: "pair".into(),
                    fields: 2,
                },
            ),
            7 => (
                and(vec![
                    tenant,
                    category,
                    equal("category", value(bytes[3].wrapping_add(1))),
                ]),
                DocumentQueryAccess::CompoundEqualityIndex {
                    index: "pair".into(),
                    fields: 2,
                },
            ),
            8 => {
                let (members, probes) =
                    inside("tenant", value(bytes[2]), value(bytes[2].wrapping_add(1)));
                (
                    and(vec![members, category]),
                    DocumentQueryAccess::CompoundMembershipIndex {
                        index: "pair".into(),
                        fields: 2,
                        probes,
                    },
                )
            }
            9 => {
                let (members, probes) =
                    inside("category", value(bytes[3]), value(bytes[3].wrapping_add(1)));
                (
                    and(vec![tenant, members]),
                    DocumentQueryAccess::CompoundMembershipIndex {
                        index: "pair".into(),
                        fields: 2,
                        probes,
                    },
                )
            }
            10 => {
                let (tenants, first) =
                    inside("tenant", value(bytes[2]), value(bytes[2].wrapping_add(1)));
                let (categories, second) =
                    inside("category", value(bytes[3]), value(bytes[3].wrapping_add(1)));
                (
                    and(vec![tenants, categories]),
                    DocumentQueryAccess::CompoundMembershipIndex {
                        index: "pair".into(),
                        fields: 2,
                        probes: first * second,
                    },
                )
            }
            _ => {
                let (tenants, first) =
                    inside("tenant", value(bytes[2]), value(bytes[2].wrapping_add(1)));
                let (emails, second) = inside(
                    "email",
                    json!(format!("id{id}")),
                    json!(format!("id{}", id + 1)),
                );
                (
                    and(vec![tenants, emails]),
                    DocumentQueryAccess::CompoundMembershipIndex {
                        index: "tenant_email".into(),
                        fields: 2,
                        probes: first * second,
                    },
                )
            }
        };
        let request = DocumentQuery {
            filter,
            max_scanned: 16,
            limit: 1 + usize::from(bytes[3] % 5),
            offset: usize::from(bytes[2] % 4),
            sort: (bytes[3] & 4 != 0).then(|| DocumentSort {
                path: vec!["email".into()],
                descending: bytes[3] & 8 != 0,
            }),
            projection: (bytes[3] & 16 != 0)
                .then(|| vec![vec!["email".into()], vec!["tenant".into()]]),
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
