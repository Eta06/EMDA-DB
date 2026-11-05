//! Timed resumable deletion steps on ordinary and checkpoint-backed stores.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, IndexDefinition, Store,
};
use serde_json::json;
use std::{
    env,
    fs::File,
    io::{self, Read},
    path::PathBuf,
    time::Instant,
};

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

fn main() -> io::Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 4 {
        return Err(io::Error::other(
            "usage: collection_drop_bench <parent> <ordinary|checkpoint> <documents>",
        ));
    }
    let root = tempfile::tempdir_in(&args[1])?;
    let path: PathBuf = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let count: usize = args[3].parse().map_err(io::Error::other)?;
    if count == 0 || count > 4000 {
        return Err(io::Error::other("documents must be 1..=4000"));
    }
    let store = Store::open(&path)?;
    let mut magic = [0_u8; 8];
    File::open(path.join("records.log"))?.read_exact(&mut magic)?;
    if &magic != b"EMDALOG2" && &magic != b"EMDALOG3" {
        return Err(io::Error::other("unexpected log format"));
    }
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())?;
    db.create_index(
        "c",
        IndexDefinition {
            name: "idx".into(),
            fields: vec![vec!["n".into()]],
            unique: true,
            partial: None,
        },
    )?;
    for first in (0..count).step_by(32) {
        db.transaction(|tx| {
            for n in first..(first + 32).min(count) {
                tx.insert("c", &format!("id-{n:05}"), &json!({"n":n}))?;
            }
            Ok(())
        })?;
    }
    let mode = args[2].as_str();
    let store = match mode {
        "ordinary" => Backend::Ordinary(store),
        "checkpoint" => {
            let digest = store.checkpoint_to(&checkpoint)?;
            drop(store);
            Backend::Checkpoint(CheckpointStore::open(&path, &checkpoint, &digest)?)
        }
        _ => return Err(io::Error::other("unknown mode")),
    };
    let db = store.db();
    let log_before = std::fs::metadata(path.join("records.log"))?.len();
    db.start_collection_drop("c")?;
    let mut samples = Vec::new();
    let mut removed = 0;
    loop {
        let start = Instant::now();
        let step = db.advance_collection_drop("c", 128)?;
        let ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        samples.push(json!({"removed":step.removed,"complete":step.complete,"ns":ns}));
        removed += step.removed;
        if step.complete {
            break;
        }
    }
    let log_after = std::fs::metadata(path.join("records.log"))?.len();
    if removed != count * 3 {
        return Err(io::Error::other("incorrect key count"));
    }
    db.create_collection("c", CollectionSchema::default())?;
    db.create_index(
        "c",
        IndexDefinition {
            name: "idx".into(),
            fields: vec![vec!["n".into()]],
            unique: true,
            partial: None,
        },
    )?;
    db.insert("c", "recreated", &json!({"n":0}))?;
    drop(store);
    let reopened = Store::open(&path)?;
    let db = DocumentDatabase::new(&reopened);
    if db.get("c", "recreated")? != Some(json!({"n":0}))
        || db.verify_collection("c")?.index_keys != 2
    {
        return Err(io::Error::other("recovery verification failed"));
    }
    println!(
        "{}",
        json!({"mode":mode,"documents":count,"removed":removed,"log_format":std::str::from_utf8(&magic).unwrap(),"log_bytes_before":log_before,"log_bytes_after":log_after,"samples":samples,"recovery_verified":true})
    );
    Ok(())
}
