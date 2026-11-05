#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery, Store,
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
fn open(source: &Path, checkpoint: &Path, digest: &[u8; 32], checkpoint_mode: bool) -> Backend {
    if checkpoint_mode {
        Backend::Checkpoint(CheckpointStore::open(source, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(source).unwrap())
    }
}
fn value(byte: u8) -> Value {
    match byte % 8 {
        0 => json!("x".repeat(1024 * 1024 - 2)),
        1 => json!("\n".repeat(512 * 1024 - 1)),
        2 => Value::Null,
        3 => json!({"nested": ["\0", "é", 42]}),
        4 => json!([byte, null, false]),
        _ => json!(byte),
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 3 || data.len() > 16 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let checkpoint = root.path().join("index");
    let initial = Store::open(&source).unwrap();
    initial.put(b"receipt", b"old").unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let checkpoint_mode = data[0] & 1 != 0;
    let store = open(&source, &checkpoint, &digest, checkpoint_mode);
    let db = store.db();
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"x":0})).unwrap();
    let mut bytes = 0_usize;
    let mut values = 0_usize;
    let mut children = Vec::new();
    if data[0] & 2 != 0 {
        let count = if data[0] & 4 == 0 { 255 } else { 256 };
        bytes += 4 * count;
        values += count;
        children.push(DocumentFilter::In {
            path: vec!["missing".into()],
            values: vec![Value::Null; count],
        });
    }
    for index in 0..(1 + usize::from(data[1] % 6)) {
        let control = data.get(index + 2).copied().unwrap_or(0);
        let value = value(control >> 2);
        bytes += serde_json::to_vec(&value).unwrap().len();
        values += 1;
        let filter = match control % 4 {
            0 => DocumentFilter::Equal {
                path: vec!["x".into()],
                value,
            },
            1 => DocumentFilter::ArrayContains {
                path: vec!["x".into()],
                value,
            },
            2 => DocumentFilter::In {
                path: vec!["x".into()],
                values: vec![value],
            },
            _ => DocumentFilter::Not(Box::new(DocumentFilter::Equal {
                path: vec!["x".into()],
                value,
            })),
        };
        children.push(filter);
    }
    let query = DocumentQuery {
        // The All branch makes the other predicates unreachable at evaluation;
        // their inputs must still be admitted before any staged commit.
        filter: DocumentFilter::Or(vec![DocumentFilter::All, DocumentFilter::And(children)]),
        ..Default::default()
    };
    let allowed = values <= 256 && bytes <= 4 * 1024 * 1024;
    let result = db.transaction(|tx| {
        tx.put_raw(b"receipt", b"new")?;
        let rows = tx.query("c", &query)?;
        assert_eq!(rows.documents.len(), 1);
        Ok(())
    });
    assert_eq!(result.is_ok(), allowed, "values={values}, bytes={bytes}");
    drop(store);
    let recovered = Store::open(&source).unwrap();
    assert_eq!(
        recovered.get(b"receipt").unwrap().as_deref(),
        Some(if allowed { &b"new"[..] } else { &b"old"[..] })
    );
});
