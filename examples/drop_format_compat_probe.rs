//! Compatibility probe for disposable stores; `resume` finishes an irreversible drop.
use database_engine::{CheckpointStore, CollectionSchema, DocumentDatabase, Store};
use serde_json::json;
use std::{env, io, path::Path};

fn main() -> io::Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 3 {
        return Err(io::Error::other("usage: probe <create|old|resume> <root>"));
    }
    let root = Path::new(&args[2]);
    let path = root.join("store");
    match args[1].as_str() {
        "create" => {
            let store = Store::open(&path)?;
            let db = DocumentDatabase::new(&store);
            db.create_collection("c", CollectionSchema::default())?;
            db.insert("c", "a", &json!({"n":1}))?;
            db.insert("c", "b", &json!({"n":2}))?;
            db.start_collection_drop("c")?;
            let checkpoint = root.join("checkpoint");
            let digest = store.checkpoint_to(&checkpoint)?;
            drop(store);
            let store = CheckpointStore::open(&path, checkpoint, &digest)?;
            let step =
                DocumentDatabase::with_checkpoint_store(&store).advance_collection_drop("c", 1)?;
            assert_eq!((step.removed, step.complete), (1, false));
            println!("checkpoint step committed");
        }
        "old" => {
            let store = Store::open(&path)?;
            let db = DocumentDatabase::new(&store);
            assert_eq!(
                db.get("c", "a").unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(
                db.collection_drop_in_progress("c").unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            println!("previous reader rejected version 4");
        }
        "resume" => {
            let store = Store::open(&path)?;
            let db = DocumentDatabase::new(&store);
            assert!(db.collection_drop_in_progress("c")?);
            let mut removed = 0;
            loop {
                let step = db.advance_collection_drop("c", 1)?;
                removed += step.removed;
                if step.complete {
                    break;
                }
            }
            assert_eq!(removed, 1);
            db.create_collection("c", CollectionSchema::default())?;
            assert!(db.get("c", "b")?.is_none());
            println!("current reader completed and recreated collection");
        }
        _ => return Err(io::Error::other("unknown mode")),
    }
    Ok(())
}
