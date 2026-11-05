//! Process-kill races across acknowledged and in-flight document transactions.
//! These tests do not simulate loss of OS/device caches or physical power.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue,
    Store,
};
use serde_json::json;
use std::{
    env, fs,
    io::{self, ErrorKind, Read, Write},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

mod support;
use support::retry_lock;

fn initialize(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "number".into(),
            fields: vec![vec!["n".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
}

fn child_work(db: &DocumentDatabase<'_>, signal: &Path) -> ! {
    for number in 0..3 {
        let id = format!("acked-{number}");
        let receipt = format!("receipt-{number}");
        db.transaction(|tx| {
            tx.insert("c", &id, &json!({"n":number}))?;
            tx.put_raw(receipt.as_bytes(), id.as_bytes())
        })
        .unwrap();
    }
    // All three transactions returned successfully before this signal. The
    // parent captures their log prefix while this child is paused on stdin.
    fs::write(signal, b"acknowledged").unwrap();
    let mut go = [0_u8; 1];
    io::stdin().read_exact(&mut go).unwrap();
    db.transaction(|tx| {
        tx.insert("c", "pending", &json!({"n":99}))?;
        tx.put_raw(b"pending-receipt", b"pending")
    })
    .unwrap();
    loop {
        thread::park();
    }
}

#[test]
fn document_kill_race_child() {
    let Ok(mode) = env::var("EMDA_DOCUMENT_KILL_CHILD") else {
        return;
    };
    let path = env::var_os("EMDA_DOCUMENT_KILL_PATH").unwrap();
    let signal = env::var_os("EMDA_DOCUMENT_KILL_SIGNAL").unwrap();
    if mode == "ordinary" {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        child_work(&DocumentDatabase::new(&store), Path::new(&signal));
    } else {
        let checkpoint = env::var_os("EMDA_DOCUMENT_KILL_CHECKPOINT").unwrap();
        let digest: [u8; 32] = fs::read(env::var_os("EMDA_DOCUMENT_KILL_DIGEST").unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        child_work(
            &DocumentDatabase::with_checkpoint_store(&store),
            Path::new(&signal),
        );
    }
}

fn verify_recovered(db: &DocumentDatabase<'_>) -> bool {
    for number in 0..3 {
        let id = format!("acked-{number}");
        let receipt = format!("receipt-{number}");
        assert_eq!(db.get("c", &id).unwrap(), Some(json!({"n":number})));
        assert_eq!(
            db.transaction(|tx| tx.get_raw(receipt.as_bytes())).unwrap(),
            Some(id.as_bytes().to_vec())
        );
        let entries = db
            .query_index(
                "c",
                "number",
                &IndexQuery {
                    equal: vec![IndexValue::Integer(number)],
                    ..Default::default()
                },
            )
            .unwrap()
            .documents;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, id);
    }
    let pending = db.get("c", "pending").unwrap();
    let receipt = db.transaction(|tx| tx.get_raw(b"pending-receipt")).unwrap();
    let indexed = db
        .query_index(
            "c",
            "number",
            &IndexQuery {
                equal: vec![IndexValue::Integer(99)],
                ..Default::default()
            },
        )
        .unwrap()
        .documents;
    match pending {
        None => {
            assert_eq!(receipt, None);
            assert!(indexed.is_empty());
        }
        Some(value) => {
            assert_eq!(value, json!({"n":99}));
            assert_eq!(receipt.as_deref(), Some(&b"pending"[..]));
            assert_eq!(indexed.len(), 1);
            assert_eq!(indexed[0].id, "pending");
        }
    }
    let integrity = db.verify_collection("c").unwrap();
    assert_eq!(integrity.documents, 3 + usize::from(receipt.is_some()));
    receipt.is_some()
}

#[test]
fn killed_document_writers_recover_acknowledged_transactions_or_fail_closed() {
    let repetitions = env::var("EMDA_DOCUMENT_KILL_REPETITIONS")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(2);
    assert!((1..=100).contains(&repetitions));
    for mode in ["ordinary", "checkpoint"] {
        let mut recovered_count = 0;
        let mut fail_closed_count = 0;
        let mut pending_present_count = 0;
        for _ in 0..repetitions {
            for delay in [0_u64, 50, 500, 2_000] {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("store");
                let checkpoint = root.path().join("checkpoint");
                let digest_file = root.path().join("digest");
                let signal = root.path().join("signal");
                let store = Store::open(&path).unwrap();
                initialize(&DocumentDatabase::new(&store));
                let digest = store.checkpoint_to(&checkpoint).unwrap();
                fs::write(&digest_file, digest).unwrap();
                drop(store);
                let mut child = Command::new(env::current_exe().unwrap())
                    .arg("--exact")
                    .arg("document_kill_race_child")
                    .env("EMDA_DOCUMENT_KILL_CHILD", mode)
                    .env("EMDA_DOCUMENT_KILL_PATH", &path)
                    .env("EMDA_DOCUMENT_KILL_SIGNAL", &signal)
                    .env("EMDA_DOCUMENT_KILL_CHECKPOINT", &checkpoint)
                    .env("EMDA_DOCUMENT_KILL_DIGEST", &digest_file)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .spawn()
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(20);
                while !signal.exists() {
                    if let Some(status) = child.try_wait().unwrap() {
                        panic!("child exited before acknowledgements: {status}");
                    }
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("child did not acknowledge");
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                let acknowledged_prefix = fs::read(path.join("records.log")).unwrap();
                child.stdin.take().unwrap().write_all(b"x").unwrap();
                if delay > 0 {
                    thread::sleep(Duration::from_micros(delay));
                }
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("child exited before interruption: {status}");
                }
                child.kill().unwrap();
                child.wait().unwrap();
                let interrupted = fs::read(path.join("records.log")).unwrap();
                assert!(interrupted.starts_with(&acknowledged_prefix));
                match retry_lock(|| Store::open(&path)) {
                    Ok(recovered) => {
                        let pending_present = verify_recovered(&DocumentDatabase::new(&recovered));
                        drop(recovered);
                        let checkpoint_recovered =
                            retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest))
                                .unwrap();
                        assert_eq!(
                            verify_recovered(&DocumentDatabase::with_checkpoint_store(
                                &checkpoint_recovered
                            )),
                            pending_present,
                            "full-log and checkpoint recovery disagree"
                        );
                        recovered_count += 1;
                        pending_present_count += usize::from(pending_present);
                    }
                    Err(error) => {
                        assert!(
                            matches!(
                                error.kind(),
                                ErrorKind::InvalidData | ErrorKind::UnexpectedEof
                            ),
                            "unexpected recovery error: {error}"
                        );
                        assert_eq!(
                            fs::read(path.join("records.log")).unwrap(),
                            interrupted,
                            "failed recovery changed the log"
                        );
                        fail_closed_count += 1;
                    }
                }
            }
        }
        println!(
            "mode={mode} repetitions={repetitions} recovered={recovered_count} pending_present={pending_present_count} failed_closed={fail_closed_count}"
        );
    }
}
