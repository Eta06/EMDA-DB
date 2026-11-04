#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery,
    DocumentQueryAccess, IndexDefinition, IndexValue, PartialFilter, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
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
fn index_value(byte: u8) -> Value {
    match byte % 4 {
        0 => Value::Null,
        1 => json!(false),
        2 => json!((byte / 4) % 12),
        _ => json!(format!("s{}", (byte / 4) % 4)),
    }
}
fn query_value(byte: u8) -> Value {
    match byte % 8 {
        0..=3 => index_value(byte),
        4 => json!({"nested":1}),
        5 => json!([1, 2]),
        6 => json!(1.0),
        _ => json!(true),
    }
}
fn indexable(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => true,
        Value::Number(number) => number.as_i64().is_some(),
        _ => false,
    }
}
fn document(id: u8, byte: u8, unique: bool) -> Value {
    let mut document = json!({"active": byte & 1 != 0});
    if byte & 2 == 0 {
        document["v"] = if unique {
            json!(id)
        } else {
            index_value(byte / 4)
        };
    }
    document
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
    let unique = data[0] & 2 != 0;
    let partial = data[0] & 4 != 0;
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
                name: "v".into(),
                fields: vec![vec!["v".into()]],
                unique,
                partial: partial.then(|| PartialFilter::Equals {
                    path: vec!["active".into()],
                    value: IndexValue::Boolean(true),
                }),
            },
        )
        .unwrap();
    let mut model = BTreeMap::new();
    for id in 0..6 {
        let doc = document(id, id * 4 + 1, unique);
        store.db().insert("c", &format!("{id:02}"), &doc).unwrap();
        model.insert(id, doc);
    }
    for bytes in data[1..].chunks_exact(6) {
        let id = bytes[1] % 12;
        match bytes[0] % 6 {
            0 | 1 => {
                let doc = document(id, bytes[2], unique);
                store
                    .db()
                    .replace("c", &format!("{id:02}"), &doc, true)
                    .unwrap();
                model.insert(id, doc);
            }
            2 => {
                store.db().delete("c", &format!("{id:02}")).unwrap();
                model.remove(&id);
            }
            3 => {
                drop(store);
                store = open(&path, &checkpoint, &digest, checkpoint_mode);
            }
            _ => {}
        }
        let values = vec![
            query_value(bytes[3]),
            query_value(bytes[4]),
            query_value(bytes[5]),
        ];
        let guarded = bytes[0] & 8 != 0;
        let negate = bytes[0] & 16 != 0;
        let wrapped = bytes[0] & 32 != 0;
        let predicate = DocumentFilter::In {
            path: vec!["v".into()],
            values: values.clone(),
        };
        let predicate = if negate {
            DocumentFilter::Not(Box::new(predicate))
        } else if wrapped {
            DocumentFilter::Or(vec![predicate])
        } else {
            predicate
        };
        let filter = if guarded {
            DocumentFilter::And(vec![
                predicate,
                DocumentFilter::Equal {
                    path: vec!["active".into()],
                    value: json!(true),
                },
            ])
        } else {
            predicate
        };
        let request = DocumentQuery {
            filter,
            offset: usize::from(bytes[2] % 3),
            limit: 1 + usize::from(bytes[1] % 6),
            max_scanned: 16,
            projection: (bytes[0] & 64 != 0).then(|| vec![vec!["v".into()]]),
            ..Default::default()
        };
        let matching = model
            .iter()
            .filter_map(|(row_id, doc)| {
                let member = doc.get("v").is_some_and(|actual| values.contains(actual));
                let selected = (member ^ negate) && (!guarded || doc["active"] == json!(true));
                selected.then(|| format!("{row_id:02}"))
            })
            .collect::<Vec<_>>();
        let expected = matching
            .iter()
            .skip(request.offset)
            .take(request.limit)
            .cloned()
            .collect::<Vec<_>>();
        let scan = store.db().query("c", &request).unwrap();
        let planned = store.db().query_planned("c", &request).unwrap();
        assert_eq!(
            scan.documents
                .iter()
                .map(|row| row.id.clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(scan.plan.matched, matching.len());
        assert_eq!(planned.result.documents, scan.documents);
        assert_eq!(planned.result.plan.matched, scan.plan.matched);
        let uses_index =
            !negate && !wrapped && values.iter().all(indexable) && (!partial || guarded);
        let distinct = values
            .iter()
            .map(|value| serde_json::to_string(value).unwrap())
            .collect::<BTreeSet<_>>()
            .len();
        let expected_access = if uses_index && partial {
            DocumentQueryAccess::PartialMembershipIndex {
                index: "v".into(),
                probes: distinct,
            }
        } else if uses_index {
            DocumentQueryAccess::MembershipIndex {
                index: "v".into(),
                probes: distinct,
            }
        } else {
            DocumentQueryAccess::CollectionScan
        };
        assert_eq!(planned.access, expected_access);
    }
});
