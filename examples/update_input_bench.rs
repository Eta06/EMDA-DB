//! Compare conditional document-update input validation for identical inputs.
use database_engine::{CollectionSchema, DocumentDatabase, DocumentUpdate, Store};
use serde_json::{Value, json};
use std::{error::Error, path::Path, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err("usage: update_input_bench DIRECTORY SCENARIO CYCLES".into());
    }
    let cycles = args[3].parse::<usize>()?;
    if cycles == 0 || cycles > 100_000 {
        return Err("invalid cycle count".into());
    }
    let value = match args[2].as_str() {
        "small" => Value::String("x".repeat(64)),
        "large" => Value::String("x".repeat(1024 * 1024 - 2)),
        "escaped" => Value::String("\0".repeat(174_763)),
        "nested-small" => json!({"text":"x".repeat(32),"flag":true,"n":7}),
        "nested-large" => json!({"text":"x".repeat(1024*1024-100)}),
        "nested-wide" => json!((0..1024).collect::<Vec<i64>>()),
        _ => return Err("unknown scenario".into()),
    };
    let encoded_bytes = serde_json::to_vec(&value)?.len();
    let update = DocumentUpdate::SetIfMissing {
        path: vec!["x".into()],
        value,
    };
    let root = tempfile::tempdir_in(Path::new(&args[1]))?;
    let store = Store::open(root.path())?;
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())?;
    db.insert("c", "a", &json!({"x":0}))?;
    let mut samples = Vec::with_capacity(cycles);
    for iteration in 0..cycles + 5 {
        let start = Instant::now();
        assert!(db.update_if("c", "a", &[], std::slice::from_ref(&update), false)?);
        let elapsed = u64::try_from(start.elapsed().as_nanos())?;
        if iteration >= 5 {
            samples.push(elapsed);
        }
    }
    assert_eq!(db.get("c", "a")?, Some(json!({"x":0})));
    println!(
        "{}",
        serde_json::to_string(
            &json!({"scenario":args[2],"encoded_bytes":encoded_bytes,"cycles":cycles,"verified":true,"samples_ns":samples})
        )?
    );
    Ok(())
}
