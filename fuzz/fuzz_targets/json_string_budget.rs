#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, FieldCondition,
    MAX_DOCUMENT_UPDATE_INPUT_BYTES, Store,
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

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 || data.len() > 128 {
        return;
    }
    let string = String::from_utf8_lossy(&data[1..]).into_owned();
    let value = match (data[0] >> 1) & 3 {
        0 => Value::String(string),
        1 => json!({"key": string, "nested": [null, true, false, -1, 1.25]}),
        2 => json!([string, {"escaped\"": "\0", "n": u64::MAX}]),
        _ => {
            let mut object = serde_json::Map::new();
            object.insert(string.clone(), json!([string, {}, []]));
            Value::Object(object)
        }
    };
    let encoded = serde_json::to_vec(&value).unwrap().len();
    assert!(encoded + 2 < 1024 * 1024);
    let full = Value::String("x".repeat(1024 * 1024 - 2));
    let remainder = Value::String("x".repeat(1024 * 1024 - encoded - 2));
    let mut conditions = (0..3)
        .map(|_| FieldCondition {
            path: vec!["missing".into()],
            expected: Some(full.clone()),
        })
        .collect::<Vec<_>>();
    conditions.push(FieldCondition {
        path: vec!["missing".into()],
        expected: Some(remainder),
    });
    let update = DocumentUpdate::SetIfMissing {
        path: vec!["x".into()],
        value,
    };
    assert_eq!(
        3 * 1024 * 1024 + (1024 * 1024 - encoded) + encoded,
        MAX_DOCUMENT_UPDATE_INPUT_BYTES
    );
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&source).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = open(&source, &checkpoint, &digest, data[0] & 1 != 0);
    let db = store.db();
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"x":0})).unwrap();
    assert!(
        !db.update_if("c", "a", &conditions, std::slice::from_ref(&update), false)
            .unwrap()
    );
    let result = db.transaction(|tx| {
        tx.put_raw(b"receipt", b"must-not-commit")?;
        tx.update_if(
            "c",
            "a",
            &conditions,
            &[
                update,
                DocumentUpdate::ArrayRemove {
                    path: vec!["missing".into()],
                    value: json!(0),
                },
            ],
            false,
        )?;
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(), None);
    assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":0})));
});
