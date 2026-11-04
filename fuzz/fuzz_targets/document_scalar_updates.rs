#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, IndexDefinition,
    IndexValue, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Map, Value, json};
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
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Scalar {
    Null,
    Boolean(bool),
    Integer(i64),
    String(String),
}
fn scalar(value: &Value) -> Scalar {
    match value {
        Value::Null => Scalar::Null,
        Value::Bool(v) => Scalar::Boolean(*v),
        Value::Number(v) => Scalar::Integer(v.as_i64().unwrap()),
        Value::String(v) => Scalar::String(v.clone()),
        _ => unreachable!(),
    }
}
fn to_json(value: &IndexValue) -> Value {
    match value {
        IndexValue::Null => Value::Null,
        IndexValue::Boolean(v) => json!(v),
        IndexValue::Integer(v) => json!(v),
        IndexValue::String(v) => json!(v),
    }
}
fn argument(byte: u8) -> IndexValue {
    match byte % 8 {
        0 => IndexValue::Null,
        1 => IndexValue::Boolean(false),
        2 => IndexValue::Boolean(true),
        3 => IndexValue::Integer(i64::MAX),
        4 => IndexValue::Integer(i64::MIN),
        5 => IndexValue::Integer(i64::from(byte as i8)),
        6 => IndexValue::String("a".into()),
        _ => IndexValue::String("z".into()),
    }
}
fn expected(document: &mut Map<String, Value>, update: &DocumentUpdate) -> Result<(), ()> {
    match update {
        DocumentUpdate::SetIfMissing { path, value } => {
            document
                .entry(path[0].clone())
                .or_insert_with(|| value.clone());
        }
        DocumentUpdate::Multiply { path, factor } => {
            let old = match document.get(&path[0]) {
                None => 0,
                Some(value) => value.as_i64().ok_or(())?,
            };
            document.insert(path[0].clone(), json!(old.checked_mul(*factor).ok_or(())?));
        }
        DocumentUpdate::Min { path, value } | DocumentUpdate::Max { path, value } => {
            let next = to_json(value);
            let replace = match document.get(&path[0]) {
                None => true,
                Some(old) => {
                    if matches!(update, DocumentUpdate::Min { .. }) {
                        scalar(&next) < scalar(old)
                    } else {
                        scalar(&next) > scalar(old)
                    }
                }
            };
            if replace {
                document.insert(path[0].clone(), next);
            }
        }
        DocumentUpdate::Rename { from, to } => {
            if from.starts_with(to) || to.starts_with(from) {
                return Err(());
            }
            if let Some(value) = document.remove(&from[0]) {
                document.insert(to[0].clone(), value);
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 128 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&source).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let checkpoint_mode = data[0] & 1 != 0;
    let mut store = open(&source, &checkpoint, &digest, checkpoint_mode);
    store
        .db()
        .create_collection("c", CollectionSchema::default())
        .unwrap();
    for name in ["x", "y"] {
        store
            .db()
            .create_index(
                "c",
                IndexDefinition {
                    name: name.into(),
                    fields: vec![vec![name.into()]],
                    unique: false,
                    partial: None,
                },
            )
            .unwrap();
    }
    store.db().insert("c", "a", &json!({})).unwrap();
    let mut model = Map::<String, Value>::new();
    let mut receipt: Option<Vec<u8>> = None;
    for (step, bytes) in data[1..].chunks_exact(3).enumerate() {
        if bytes[0] % 8 == 7 {
            drop(store);
            store = open(&source, &checkpoint, &digest, checkpoint_mode);
        } else {
            let path = vec![if bytes[1] & 1 == 0 { "x" } else { "y" }.to_owned()];
            let arg = argument(bytes[2]);
            let update = match bytes[0] % 8 {
                0 => DocumentUpdate::SetIfMissing {
                    path,
                    value: to_json(&arg),
                },
                1 => DocumentUpdate::Multiply {
                    path,
                    factor: [-2, -1, 0, 1, 2][usize::from(bytes[2] % 5)],
                },
                2 => DocumentUpdate::Min { path, value: arg },
                3 => DocumentUpdate::Max { path, value: arg },
                4 => DocumentUpdate::Rename {
                    from: vec!["x".into()],
                    to: vec!["y".into()],
                },
                5 => DocumentUpdate::Rename {
                    from: vec!["y".into()],
                    to: vec!["x".into()],
                },
                _ => DocumentUpdate::Rename {
                    from: vec!["x".into()],
                    to: vec!["x".into(), "nested".into()],
                },
            };
            let mut next = model.clone();
            let expected_ok = expected(&mut next, &update).is_ok();
            let next_receipt = vec![step as u8];
            let result = store.db().transaction(|tx| {
                tx.put_raw(b"receipt", &next_receipt)?;
                assert!(tx.update_if("c", "a", &[], &[update], false)?);
                Ok(())
            });
            assert_eq!(result.is_ok(), expected_ok);
            if expected_ok {
                model = next;
                receipt = Some(next_receipt);
            }
        }
        assert_eq!(
            store.db().get("c", "a").unwrap(),
            Some(Value::Object(model.clone()))
        );
        assert_eq!(
            store.db().transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
            receipt
        );
        assert_eq!(store.db().verify_collection("c").unwrap().documents, 1);
    }
});
