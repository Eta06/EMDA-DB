#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentQuery, FieldRule, FieldType,
    IndexDefinition, IndexQuery, Store,
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
fn definition(unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: "idx".into(),
        fields: vec![vec!["n".into()]],
        unique,
        partial: None,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    None,
    Building,
    Dropping,
    Active,
}
fn check(
    db: &DocumentDatabase<'_>,
    model: &BTreeMap<String, i64>,
    state: State,
    unique: bool,
    schema_pending: bool,
) {
    assert_eq!(
        db.schema_migration("c").expect("schema status").is_some(),
        schema_pending
    );
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
        .map(|row| (row.id.clone(), row.document["n"].as_i64().unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(&actual, model);
    let jobs = db.index_jobs("c").expect("jobs");
    assert_eq!(
        jobs.len(),
        usize::from(matches!(state, State::Building | State::Dropping))
    );
    assert_eq!(
        db.indexes("c").expect("indexes").len(),
        usize::from(state == State::Active)
    );
    if state == State::Active {
        let mut expected = model
            .iter()
            .map(|(id, value)| (*value, id.clone()))
            .collect::<Vec<_>>();
        expected.sort();
        if unique {
            assert!(expected.windows(2).all(|items| items[0].0 != items[1].0));
        }
        let result = db
            .query_index("c", "idx", &IndexQuery::default())
            .expect("index query");
        let actual = result
            .documents
            .iter()
            .map(|row| (row.document["n"].as_i64().unwrap(), row.id.clone()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        db.verify_collection("c").expect("integrity");
    }
}
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 64 {
        return;
    }
    let mode = data[0] & 1 != 0;
    let unique = data[0] & 2 != 0;
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
    let mut model = BTreeMap::<String, i64>::new();
    let mut state = State::None;
    let mut schema_pending = false;
    for chunk in data[1..].chunks_exact(3) {
        let id = format!("id-{}", chunk[1] % 8);
        let value = i64::from(chunk[2] % 8);
        match chunk[0] % 10 {
            0 | 1 => {
                let before = std::fs::read(path.join("records.log")).expect("log");
                match store.db().replace("c", &id, &json!({"n":value}), true) {
                    Ok(()) => {
                        model.insert(id, value);
                    }
                    Err(error) => {
                        assert!(
                            unique
                                && matches!(state, State::Building | State::Active)
                                && error.kind() == io::ErrorKind::AlreadyExists
                        );
                        assert_eq!(std::fs::read(path.join("records.log")).unwrap(), before);
                    }
                }
            }
            2 => {
                let removed = store.db().delete("c", &id).expect("delete");
                assert_eq!(removed, model.remove(&id).is_some());
            }
            3 => match state {
                State::None => {
                    store
                        .db()
                        .start_index_build("c", definition(unique))
                        .expect("start build");
                    state = State::Building;
                }
                State::Building => {
                    store.db().cancel_index_build("c", "idx").expect("cancel");
                    state = State::Dropping;
                }
                State::Active => {
                    store.db().start_index_drop("c", "idx").expect("start drop");
                    state = State::Dropping;
                }
                State::Dropping => {}
            },
            4 => match state {
                State::Building => {
                    let before = std::fs::read(path.join("records.log")).unwrap();
                    match store
                        .db()
                        .advance_index_build("c", "idx", usize::from(chunk[2] % 3 + 1))
                    {
                        Ok(step) => {
                            if step.complete {
                                state = State::Active;
                            }
                        }
                        Err(error) => {
                            assert!(unique && error.kind() == io::ErrorKind::AlreadyExists);
                            assert_eq!(std::fs::read(path.join("records.log")).unwrap(), before);
                        }
                    }
                }
                State::Dropping => {
                    if store
                        .db()
                        .advance_index_drop("c", "idx", usize::from(chunk[2] % 3 + 1))
                        .expect("drop step")
                        .complete
                    {
                        state = State::None;
                    }
                }
                _ => {}
            },
            5 => {
                drop(store);
                store = open(&path, &checkpoint, &digest, mode);
            }
            6 => {
                if schema_pending {
                    store
                        .db()
                        .cancel_schema_migration("c")
                        .expect("cancel schema");
                    schema_pending = false;
                } else {
                    store
                        .db()
                        .start_schema_migration("c", schema())
                        .expect("start schema");
                    schema_pending = true;
                }
            }
            7 => {
                if schema_pending
                    && store
                        .db()
                        .advance_schema_migration("c", usize::from(chunk[2] % 3 + 1))
                        .expect("schema step")
                        .complete
                {
                    schema_pending = false;
                }
            }
            _ => {}
        }
        check(&store.db(), &model, state, unique, schema_pending);
    }
    drop(store);
    let store = open(&path, &checkpoint, &digest, mode);
    check(&store.db(), &model, state, unique, schema_pending);
});
