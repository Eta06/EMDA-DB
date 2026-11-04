#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentQuery, FieldRule, FieldType, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::json;
use std::{collections::BTreeMap, io, path::Path};

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
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], mode: bool) -> Backend {
    if mode {
        Backend::Checkpoint(
            CheckpointStore::open(path, checkpoint, digest).expect("checkpoint store"),
        )
    } else {
        Backend::Ordinary(Store::open(path).expect("ordinary store"))
    }
}
fn schema() -> CollectionSchema {
    CollectionSchema {
        fields: vec![FieldRule {
            path: vec!["n".into()],
            field_type: FieldType::Integer,
            required: true,
            nullable: false,
        }],
    }
}
fn check(
    db: &DocumentDatabase<'_>,
    model: &BTreeMap<String, Option<i64>>,
    pending: bool,
    active: bool,
) {
    let result = db
        .query(
            "c",
            &DocumentQuery {
                max_scanned: 16,
                ..Default::default()
            },
        )
        .expect("query");
    let actual = result
        .documents
        .iter()
        .map(|row| {
            (
                row.id.clone(),
                row.document.get("n").and_then(|n| n.as_i64()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(&actual, model);
    assert_eq!(
        db.schema_migration("c").expect("migration").is_some(),
        pending
    );
    if active {
        assert!(model.values().all(Option::is_some));
    }
}
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 64 {
        return;
    }
    let mode = data[0] & 1 != 0;
    let root = tempfile::tempdir().expect("temp");
    let path = root.path().join("store");
    let checkpoint = root.path().join("base");
    let ordinary = Store::open(&path).expect("create");
    let digest = ordinary.checkpoint_to(&checkpoint).expect("checkpoint");
    drop(ordinary);
    let mut store = open(&path, &checkpoint, &digest, mode);
    store
        .db()
        .create_collection("c", CollectionSchema::default())
        .expect("collection");
    let mut model = BTreeMap::<String, Option<i64>>::new();
    let mut pending = false;
    let mut active = false;
    for chunk in data[1..].chunks_exact(3) {
        let id = format!("id-{}", chunk[1] % 8);
        let valid = chunk[2] & 1 != 0;
        let value = i64::from(chunk[2] % 16);
        match chunk[0] % 7 {
            0 | 1 => {
                let before = std::fs::read(path.join("records.log")).unwrap();
                let document = if valid {
                    json!({"n":value})
                } else {
                    json!({"other":true})
                };
                match store.db().replace("c", &id, &document, true) {
                    Ok(()) => {
                        assert!(valid || (!pending && !active));
                        model.insert(id, valid.then_some(value));
                    }
                    Err(error) => {
                        assert!(
                            !valid
                                && (pending || active)
                                && error.kind() == io::ErrorKind::InvalidInput
                        );
                        assert_eq!(std::fs::read(path.join("records.log")).unwrap(), before);
                    }
                }
            }
            2 => {
                let removed = store.db().delete("c", &id).expect("delete");
                assert_eq!(removed, model.remove(&id).is_some());
            }
            3 => {
                if !pending {
                    store
                        .db()
                        .start_schema_migration("c", schema())
                        .expect("start");
                    pending = true;
                } else {
                    store.db().cancel_schema_migration("c").expect("cancel");
                    pending = false;
                }
            }
            4 => {
                if pending {
                    let before = std::fs::read(path.join("records.log")).unwrap();
                    match store
                        .db()
                        .advance_schema_migration("c", usize::from(chunk[2] % 3 + 1))
                    {
                        Ok(step) => {
                            if step.complete {
                                pending = false;
                                active = true;
                            }
                        }
                        Err(error) => {
                            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                            assert!(model.values().any(Option::is_none));
                            assert_eq!(std::fs::read(path.join("records.log")).unwrap(), before);
                        }
                    }
                }
            }
            5 => {
                drop(store);
                store = open(&path, &checkpoint, &digest, mode);
            }
            _ => {}
        }
        check(&store.db(), &model, pending, active);
    }
    drop(store);
    let store = open(&path, &checkpoint, &digest, mode);
    check(&store.db(), &model, pending, active);
});
