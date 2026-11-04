#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, IndexDefinition,
    IndexQuery, IndexValue, PartialFilter, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::json;
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
};

fn field(value: &str) -> Vec<String> {
    vec![value.to_owned()]
}
fn partial() -> IndexDefinition {
    IndexDefinition {
        name: "partial".into(),
        fields: vec![field("n")],
        unique: false,
        partial: Some(PartialFilter::Equals {
            path: field("active"),
            value: IndexValue::Boolean(true),
        }),
    }
}
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
        Backend::Checkpoint(
            CheckpointStore::open(path, checkpoint, digest).expect("open checkpoint"),
        )
    } else {
        Backend::Ordinary(Store::open(path).expect("open store"))
    }
}
fn check(db: &DocumentDatabase<'_>, model: &BTreeMap<String, (i64, bool)>, has_partial: bool) {
    let mut expected = model
        .iter()
        .map(|(id, (n, active))| (*n, id.clone(), *active))
        .collect::<Vec<_>>();
    expected.sort();
    let page = db
        .query_index(
            "c",
            "n",
            &IndexQuery {
                limit: 100,
                ..Default::default()
            },
        )
        .expect("index query");
    let found = page
        .documents
        .iter()
        .map(|row| {
            (
                row.document["n"].as_i64().expect("number"),
                row.id.clone(),
                row.document["active"].as_bool().expect("boolean"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(found, expected);
    if has_partial {
        let found = db
            .query_index(
                "c",
                "partial",
                &IndexQuery {
                    limit: 100,
                    ..Default::default()
                },
            )
            .expect("partial query");
        assert_eq!(
            found
                .documents
                .iter()
                .map(|row| row.id.clone())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .filter(|row| row.2)
                .map(|row| row.1.clone())
                .collect::<Vec<_>>()
        );
    }
    for (id, (n, active)) in model {
        assert_eq!(
            db.get("c", id).expect("get"),
            Some(json!({"n":n,"active":active}))
        );
    }
}
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 64 {
        return;
    }
    let use_checkpoint = data[0] & 1 == 1;
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("store");
    let checkpoint: PathBuf = root.path().join("checkpoint");
    let initial = Store::open(&path).expect("create");
    let digest = initial.checkpoint_to(&checkpoint).expect("checkpoint");
    drop(initial);
    let mut store = open(&path, &checkpoint, &digest, use_checkpoint);
    let db = store.db();
    db.create_collection("c", CollectionSchema::default())
        .expect("collection");
    db.create_index(
        "c",
        IndexDefinition {
            name: "n".into(),
            fields: vec![field("n")],
            unique: false,
            partial: None,
        },
    )
    .expect("index");
    let mut model = BTreeMap::<String, (i64, bool)>::new();
    let mut has_partial = false;
    for chunk in data[1..].chunks_exact(3) {
        let id = format!("id-{}", chunk[1] % 8);
        let value = (i64::from(chunk[2] % 9) - 4, chunk[2] & 1 == 1);
        match chunk[0] % 8 {
            0 | 1 => {
                store
                    .db()
                    .replace("c", &id, &json!({"n":value.0,"active":value.1}), true)
                    .expect("upsert");
                model.insert(id, value);
            }
            2 => {
                assert_eq!(
                    store.db().delete("c", &id).expect("delete"),
                    model.remove(&id).is_some()
                );
            }
            3 => {
                let exists = model.get_mut(&id);
                assert_eq!(
                    store
                        .db()
                        .update_if(
                            "c",
                            &id,
                            &[],
                            &[DocumentUpdate::Increment {
                                path: field("n"),
                                amount: 1
                            }],
                            false
                        )
                        .expect("increment"),
                    exists.is_some()
                );
                if let Some(entry) = exists {
                    entry.0 += 1;
                }
            }
            4 => {
                if has_partial {
                    store.db().drop_index("c", "partial").expect("drop index");
                } else {
                    store
                        .db()
                        .create_index("c", partial())
                        .expect("create partial");
                }
                has_partial = !has_partial;
            }
            5 => {
                drop(store);
                store = open(&path, &checkpoint, &digest, use_checkpoint);
            }
            6 => {
                let result = store.db().transaction(|tx| {
                    tx.put_raw(b"transient", b"should rollback")?;
                    let _ = tx.insert("c", &id, &json!({"n":0,"active":false}));
                    Err::<(), _>(io::Error::other("rollback"))
                });
                assert!(result.is_err());
                assert_eq!(
                    store
                        .db()
                        .transaction(|tx| tx.get_raw(b"transient"))
                        .expect("raw"),
                    None
                );
            }
            _ => {
                let result = store
                    .db()
                    .insert("c", &id, &json!({"n":value.0,"active":value.1}));
                if model.contains_key(&id) {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::AlreadyExists);
                } else {
                    result.expect("insert");
                    model.insert(id, value);
                }
            }
        }
        check(&store.db(), &model, has_partial);
    }
    drop(store);
    let store = open(&path, &checkpoint, &digest, use_checkpoint);
    check(&store.db(), &model, has_partial);
});
