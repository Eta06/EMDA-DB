#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, IndexDefinition,
    IndexQuery, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};
use std::{io, path::Path};

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
fn path(name: &str) -> Vec<String> {
    vec![name.into()]
}
fn value(byte: u8) -> Value {
    match byte % 5 {
        0 => Value::Null,
        1 => json!(byte % 3),
        2 => json!(byte & 1 == 0),
        3 => json!({"nested": byte % 3}),
        _ => json!([byte % 3, null]),
    }
}
fn check(db: &DocumentDatabase<'_>, model: &[Value]) {
    assert_eq!(
        db.get("c", "id").unwrap(),
        Some(json!({"handle":"one", "items":model}))
    );
    assert_eq!(
        db.query_index("c", "handle", &IndexQuery::default())
            .unwrap()
            .documents
            .len(),
        1
    );
    assert_eq!(db.verify_collection("c").unwrap().documents, 1);
    assert_eq!(db.transaction(|tx| tx.get_raw(b"failed")).unwrap(), None);
}
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let store_path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&store_path).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let checkpoint_mode = data[0] & 1 != 0;
    let mut store = open(&store_path, &checkpoint, &digest, checkpoint_mode);
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
                fields: vec![path("handle")],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
    store
        .db()
        .insert("c", "id", &json!({"handle":"one", "items":[]}))
        .unwrap();
    let mut model = Vec::new();
    for bytes in data[1..].chunks_exact(2) {
        let item = value(bytes[1]);
        let array_path = path("items");
        let operation = match bytes[0] % 8 {
            0 => {
                model.push(item.clone());
                Some(DocumentUpdate::ArrayPush {
                    path: array_path,
                    value: item,
                })
            }
            1 => {
                if !model.contains(&item) {
                    model.push(item.clone());
                }
                Some(DocumentUpdate::ArrayAddUnique {
                    path: array_path,
                    value: item,
                })
            }
            2 => {
                model.retain(|existing| *existing != item);
                Some(DocumentUpdate::ArrayRemove {
                    path: array_path,
                    value: item,
                })
            }
            3 => {
                if !model.is_empty() {
                    model.remove(0);
                }
                Some(DocumentUpdate::ArrayPop {
                    path: array_path,
                    first: true,
                })
            }
            4 => {
                model.pop();
                Some(DocumentUpdate::ArrayPop {
                    path: array_path,
                    first: false,
                })
            }
            5 => {
                let result = store.db().transaction(|tx| {
                    tx.put_raw(b"failed", b"no")?;
                    tx.update_if(
                        "c",
                        "id",
                        &[],
                        &[DocumentUpdate::ArrayPush {
                            path: array_path,
                            value: item,
                        }],
                        false,
                    )?;
                    Err::<(), _>(io::Error::other("abort"))
                });
                assert!(result.is_err());
                None
            }
            6 => {
                let result = store.db().transaction(|tx| {
                    tx.put_raw(b"failed", b"no")?;
                    let error = tx
                        .update_if(
                            "c",
                            "id",
                            &[],
                            &[DocumentUpdate::ArrayPush {
                                path: path("handle"),
                                value: item,
                            }],
                            false,
                        )
                        .unwrap_err();
                    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                    Ok(())
                });
                assert!(result.is_err());
                None
            }
            _ => {
                drop(store);
                store = open(&store_path, &checkpoint, &digest, checkpoint_mode);
                None
            }
        };
        if let Some(operation) = operation {
            assert!(
                store
                    .db()
                    .update_if("c", "id", &[], &[operation], false)
                    .unwrap()
            );
        }
        check(&store.db(), &model);
    }
    drop(store);
    let recovered = Store::open(&store_path).unwrap();
    check(&DocumentDatabase::new(&recovered), &model);
});
