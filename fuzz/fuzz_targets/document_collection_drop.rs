#![no_main]
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, IndexDefinition, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::json;
use std::{collections::BTreeSet, io, path::Path};

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn db(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(s) => DocumentDatabase::new(s),
            Self::Checkpoint(s) => DocumentDatabase::with_checkpoint_store(s),
        }
    }
}
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], cp: bool) -> Backend {
    if cp {
        Backend::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(path).unwrap())
    }
}
fn create(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "i".into(),
            fields: vec![vec!["n".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Active,
    Dropping,
    Absent,
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
    let cp = data[0] & 1 != 0;
    let mut store = open(&path, &checkpoint, &digest, cp);
    create(&store.db());
    let mut phase = Phase::Active;
    let mut ids = BTreeSet::new();
    let mut remaining = 0;
    for input in &data[1..] {
        let id = (input / 8) % 8;
        match (phase, input % 8) {
            (Phase::Active, 0) => {
                store.db().start_collection_drop("c").unwrap();
                remaining = ids.len() * 3;
                phase = Phase::Dropping;
            }
            (Phase::Active, 1 | 2 | 3) => {
                store
                    .db()
                    .replace("c", &id.to_string(), &json!({"n":id}), true)
                    .unwrap();
                ids.insert(id);
            }
            (Phase::Active, 4) => {
                assert_eq!(
                    store.db().delete("c", &id.to_string()).unwrap(),
                    ids.remove(&id)
                );
            }
            (Phase::Active, 5) => {
                let result = store.db().transaction::<()>(|tx| {
                    tx.start_collection_drop("c")?;
                    Err(io::Error::other("abort"))
                });
                assert!(result.is_err());
                assert!(!store.db().collection_drop_in_progress("c").unwrap());
            }
            (Phase::Dropping, 0 | 1 | 2 | 3) => {
                let limit = usize::from((*input / 8) % 5) + 1;
                let step = store.db().advance_collection_drop("c", limit).unwrap();
                assert!(step.removed <= limit && step.removed <= remaining);
                remaining -= step.removed;
                if step.complete {
                    assert_eq!(remaining, 0);
                    assert_eq!(step.removed, 0);
                    phase = Phase::Absent;
                    ids.clear();
                } else {
                    assert!(step.removed > 0);
                }
            }
            (Phase::Dropping, 4) => {
                let result = store.db().transaction::<()>(|tx| {
                    tx.advance_collection_drop("c", 1)?;
                    Err(io::Error::other("abort"))
                });
                assert!(result.is_err());
                assert!(store.db().collection_drop_in_progress("c").unwrap());
            }
            (Phase::Dropping, 5) => assert_eq!(
                store.db().get("c", "0").unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            ),
            (Phase::Absent, 0 | 1 | 2) => {
                create(&store.db());
                phase = Phase::Active;
            }
            (_, 7) => {
                drop(store);
                store = open(&path, &checkpoint, &digest, cp);
            }
            _ => {}
        }
        match phase {
            Phase::Active => {
                assert!(!store.db().collection_drop_in_progress("c").unwrap());
                assert_eq!(
                    store.db().get("c", &id.to_string()).unwrap().is_some(),
                    ids.contains(&id)
                );
            }
            Phase::Dropping => assert!(store.db().collection_drop_in_progress("c").unwrap()),
            Phase::Absent => assert_eq!(
                store.db().get("c", "0").unwrap_err().kind(),
                io::ErrorKind::NotFound
            ),
        }
    }
    drop(store);
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    match phase {
        Phase::Active => assert_eq!(db.verify_collection("c").unwrap().index_keys, ids.len() * 2),
        Phase::Dropping => loop {
            let step = db.advance_collection_drop("c", 8).unwrap();
            remaining -= step.removed;
            if step.complete {
                assert_eq!(remaining, 0);
                break;
            }
        },
        Phase::Absent => assert_eq!(
            db.get("c", "0").unwrap_err().kind(),
            io::ErrorKind::NotFound
        ),
    }
});
