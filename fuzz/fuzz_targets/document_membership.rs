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
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], checkpoint_mode: bool) -> Backend {
    if checkpoint_mode {
        Backend::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(path).unwrap())
    }
}
fn value(byte: u8) -> Value {
    match byte % 8 {
        0 => Value::Null,
        1 => json!(false),
        2 => json!(i64::from(byte % 3)),
        3 => json!(1.0),
        4 => json!("a"),
        5 => json!([1, 2]),
        6 => json!({"x":1}),
        _ => json!(true),
    }
}
fn document(id: u8, byte: u8) -> Value {
    let mut doc = json!({"handle":format!("h{id}")});
    if byte % 5 != 0 {
        doc["v"] = value(byte / 5);
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
    for id in 0..6 {
        let doc = document(id, id * 11 + 1);
        store.db().insert("c", &id.to_string(), &doc).unwrap();
        model.insert(id, doc);
    }
    for bytes in data[1..].chunks_exact(4) {
        let id = bytes[1] % 8;
        match bytes[0] % 6 {
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
        let values = (0..(bytes[3] % 4))
            .map(|n| value(bytes[2].wrapping_add(n)))
            .collect::<Vec<_>>();
        let negative = bytes[3] & 4 != 0;
        let indexed = bytes[3] & 8 != 0;
        let predicate = if negative {
            DocumentFilter::NotIn {
                path: vec!["v".into()],
                values: values.clone(),
            }
        } else {
            DocumentFilter::In {
                path: vec!["v".into()],
                values: values.clone(),
            }
        };
        let filter = if indexed {
            DocumentFilter::And(vec![
                DocumentFilter::In {
                    path: vec!["handle".into()],
                    values: vec![json!(format!("h{id}"))],
                },
                predicate,
            ])
        } else {
            predicate
        };
        let offset = usize::from(bytes[2] % 3);
        let limit = 1 + usize::from(bytes[3] % 5);
        let request = DocumentQuery {
            filter,
            max_scanned: 16,
            offset,
            limit,
            projection: (bytes[3] & 16 != 0).then(|| vec![vec!["handle".into()], vec!["v".into()]]),
            ..Default::default()
        };
        let matching = model
            .iter()
            .filter_map(|(row_id, doc)| {
                let present_match = doc.get("v").is_some_and(|actual| values.contains(actual));
                ((present_match ^ negative) && (!indexed || *row_id == id))
                    .then(|| row_id.to_string())
            })
            .collect::<Vec<_>>();
        let expected = matching
            .iter()
            .skip(offset)
            .take(limit)
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
