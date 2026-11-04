#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, FieldCondition,
    IndexValue, MAX_DOCUMENT_UPDATE_INPUT_BYTES, Store,
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
    match byte % 16 {
        4 => return json!({"nested": ["\0", byte]}),
        5 => return json!([byte, null, {"key": "é"}]),
        6 => return json!({"": [], "\"": false}),
        _ => {}
    }
    let (length, character) = match byte % 16 {
        0 => (1024 * 1024 - 2, 'x'),
        1 => (174_763, '\0'),
        2 => (512 * 1024, 'y'),
        _ => (usize::from(byte % 64), 'z'),
    };
    Value::String(character.to_string().repeat(length))
}
fn encoded(value: &Value) -> usize {
    serde_json::to_vec(value).unwrap().len()
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 3 || data.len() > 16 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&source).unwrap();
    initial.put(b"receipt", b"old").unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = open(&source, &checkpoint, &digest, data[0] & 1 != 0);
    let db = store.db();
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"x":0})).unwrap();
    let condition_value = value(data[2]);
    let mut bytes = encoded(&condition_value);
    let conditions = [FieldCondition {
        path: vec!["absent".into()],
        expected: Some(condition_value),
    }];
    let mut updates = Vec::new();
    let count = 1 + usize::from(data[1] % 5);
    for index in 0..count {
        let control = data.get(3 + index).copied().unwrap_or(0);
        let update = match control % 4 {
            0 => {
                let value = value(control);
                bytes += encoded(&value);
                DocumentUpdate::SetIfMissing {
                    path: vec!["x".into()],
                    value,
                }
            }
            1 => {
                let value = value(control);
                bytes += encoded(&value);
                DocumentUpdate::Set {
                    path: vec!["x".into()],
                    value,
                }
            }
            2 => {
                let value = value(control);
                bytes += encoded(&value);
                DocumentUpdate::ArrayRemove {
                    path: vec!["missing".into()],
                    value,
                }
            }
            _ => {
                bytes += encoded(&Value::Null);
                DocumentUpdate::Min {
                    path: vec!["x".into()],
                    value: IndexValue::Null,
                }
            }
        };
        updates.push(update);
    }
    let result = db.transaction(|tx| {
        tx.put_raw(b"receipt", b"new")?;
        assert!(!tx.update_if("c", "a", &conditions, &updates, false)?);
        Ok(())
    });
    assert_eq!(result.is_ok(), bytes <= MAX_DOCUMENT_UPDATE_INPUT_BYTES);
    let receipt = db.transaction(|tx| tx.get_raw(b"receipt")).unwrap();
    assert_eq!(
        receipt,
        Some(if bytes <= MAX_DOCUMENT_UPDATE_INPUT_BYTES {
            b"new".to_vec()
        } else {
            b"old".to_vec()
        })
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":0})));
});
