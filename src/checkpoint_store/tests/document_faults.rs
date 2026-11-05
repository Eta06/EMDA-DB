use super::*;
use crate::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue};
use serde_json::json;

#[cfg(target_os = "linux")]
#[test]
fn checkpoint_drop_cursor_and_raw_receipt_remain_atomic_at_every_v2_append_cut() {
    use crate::store::collection_drop_fault_tests::{prepare, step, verify_checkpoint_first};
    let length = {
        let (_root, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let db = DocumentDatabase::with_checkpoint_store(&store);
        prepare(&db);
        let log = path.join("records.log");
        let before = fs::metadata(&log).unwrap().len();
        step(&db).unwrap();
        usize::try_from(fs::metadata(log).unwrap().len() - before).unwrap()
    };
    eprintln!("checkpoint drop cursor append cut points: {}", length + 1);
    let faults = (0..=length).map(LogFault::Partial).chain([
        LogFault::SyncBefore,
        LogFault::SyncAfter,
        LogFault::SyncTimeout,
        LogFault::Panic(0),
        LogFault::Panic(length / 2),
        LogFault::Panic(length),
    ]);
    for fault in faults {
        let (_root, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        prepare(&DocumentDatabase::with_checkpoint_store(&store));
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            step(&DocumentDatabase::with_checkpoint_store(&store))
        }));
        assert!(!matches!(result, Ok(Ok(()))));
        drop(store);
        let log = path.join("records.log");
        let bytes = fs::read(&log).unwrap();
        match retry_lock(|| Store::open(&path)) {
            Ok(reopened) => verify_checkpoint_first(&reopened),
            Err(error) => {
                assert!(matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ));
                assert_eq!(fs::read(&log).unwrap(), bytes);
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn checkpoint_drop_cursor_and_raw_receipt_remain_atomic_at_every_v3_append_cut() {
    use crate::store::collection_drop_fault_tests::{prepare, step, verify_checkpoint_first};
    fn fixture_v3() -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::path::PathBuf,
        [u8; 32],
    ) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);
        assert_eq!(
            &fs::read(path.join("records.log")).unwrap()[..8],
            b"EMDALOG3"
        );
        (root, path, checkpoint, digest)
    }
    let length = {
        let (_root, path, checkpoint, digest) = fixture_v3();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let db = DocumentDatabase::with_checkpoint_store(&store);
        prepare(&db);
        let log = path.join("records.log");
        let before = fs::metadata(&log).unwrap().len();
        step(&db).unwrap();
        usize::try_from(fs::metadata(log).unwrap().len() - before).unwrap()
    };
    eprintln!(
        "V3 checkpoint drop cursor append cut points: {}",
        length + 1
    );
    let faults = (0..=length).map(LogFault::Partial).chain([
        LogFault::Panic(0),
        LogFault::Panic(length / 2),
        LogFault::Panic(length),
    ]);
    for fault in faults {
        let (_root, path, checkpoint, digest) = fixture_v3();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        prepare(&DocumentDatabase::with_checkpoint_store(&store));
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            step(&DocumentDatabase::with_checkpoint_store(&store))
        }));
        assert!(!matches!(result, Ok(Ok(()))));
        drop(store);
        let log = path.join("records.log");
        let bytes = fs::read(&log).unwrap();
        match retry_lock(|| Store::open(&path)) {
            Ok(reopened) => verify_checkpoint_first(&reopened),
            Err(error) => {
                assert!(matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ));
                assert_eq!(fs::read(&log).unwrap(), bytes);
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn later_checkpoint_drop_cursor_update_and_receipt_are_atomic_at_every_append_cut() {
    use crate::store::collection_drop_fault_tests::{prepare_second, step_second, verify_second};
    fn fixture_for(
        v3: bool,
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::path::PathBuf,
        [u8; 32],
    ) {
        if !v3 {
            return fixture();
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);
        assert_eq!(
            &fs::read(path.join("records.log")).unwrap()[..8],
            b"EMDALOG3"
        );
        (root, path, checkpoint, digest)
    }
    for v3 in [false, true] {
        let length = {
            let (_root, path, checkpoint, digest) = fixture_for(v3);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            prepare_second(&db);
            let log = path.join("records.log");
            let before = fs::metadata(&log).unwrap().len();
            step_second(&db).unwrap();
            usize::try_from(fs::metadata(log).unwrap().len() - before).unwrap()
        };
        eprintln!(
            "later checkpoint drop cursor append cuts, V3={v3}: {}",
            length + 1
        );
        let mut faults = (0..=length).map(LogFault::Partial).collect::<Vec<_>>();
        faults.extend([
            LogFault::Panic(0),
            LogFault::Panic(length / 2),
            LogFault::Panic(length),
        ]);
        if !v3 {
            faults.extend([
                LogFault::SyncBefore,
                LogFault::SyncAfter,
                LogFault::SyncTimeout,
            ]);
        }
        for fault in faults {
            let (_root, path, checkpoint, digest) = fixture_for(v3);
            let mut store =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            prepare_second(&DocumentDatabase::with_checkpoint_store(&store));
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(path.join("records.log"))
                .unwrap();
            store
                .log
                .replace_io_for_test(Box::new(FaultLog { file, fault }));
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                step_second(&DocumentDatabase::with_checkpoint_store(&store))
            }));
            assert!(!matches!(outcome, Ok(Ok(()))));
            drop(store);
            let log = path.join("records.log");
            let bytes = fs::read(&log).unwrap();
            match retry_lock(|| Store::open(&path)) {
                Ok(reopened) => verify_second(&reopened),
                Err(error) => {
                    assert!(matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                    ));
                    assert_eq!(fs::read(&log).unwrap(), bytes);
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn final_checkpoint_drop_cursor_removal_and_receipt_are_atomic_at_every_append_cut() {
    use crate::store::collection_drop_fault_tests::{prepare_final, step_final, verify_final};
    fn fixture_for(
        v3: bool,
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::path::PathBuf,
        [u8; 32],
    ) {
        if !v3 {
            return fixture();
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&path).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);
        assert_eq!(
            &fs::read(path.join("records.log")).unwrap()[..8],
            b"EMDALOG3"
        );
        (root, path, checkpoint, digest)
    }
    for v3 in [false, true] {
        let length = {
            let (_root, path, checkpoint, digest) = fixture_for(v3);
            let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let db = DocumentDatabase::with_checkpoint_store(&store);
            prepare_final(&db);
            let log = path.join("records.log");
            let before = fs::metadata(&log).unwrap().len();
            step_final(&db).unwrap();
            usize::try_from(fs::metadata(log).unwrap().len() - before).unwrap()
        };
        eprintln!(
            "final checkpoint cursor removal append cuts, V3={v3}: {}",
            length + 1
        );
        let mut faults = (0..=length).map(LogFault::Partial).collect::<Vec<_>>();
        faults.extend([
            LogFault::Panic(0),
            LogFault::Panic(length / 2),
            LogFault::Panic(length),
        ]);
        if !v3 {
            faults.extend([
                LogFault::SyncBefore,
                LogFault::SyncAfter,
                LogFault::SyncTimeout,
            ]);
        }
        for fault in faults {
            let (_root, path, checkpoint, digest) = fixture_for(v3);
            let mut store =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            prepare_final(&DocumentDatabase::with_checkpoint_store(&store));
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(path.join("records.log"))
                .unwrap();
            store
                .log
                .replace_io_for_test(Box::new(FaultLog { file, fault }));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                step_final(&DocumentDatabase::with_checkpoint_store(&store))
            }));
            assert!(!matches!(result, Ok(Ok(()))));
            drop(store);
            let log = path.join("records.log");
            let bytes = fs::read(&log).unwrap();
            match retry_lock(|| Store::open(&path)) {
                Ok(reopened) => verify_final(&reopened),
                Err(error) => {
                    assert!(matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                    ));
                    assert_eq!(fs::read(&log).unwrap(), bytes);
                }
            }
        }
    }
}

#[test]
fn partial_and_failed_document_commits_recover_atomically_or_fail_closed() {
    for fault in [
        LogFault::Partial(0),
        LogFault::Partial(1),
        LogFault::Partial(8),
        LogFault::Partial(30),
        LogFault::SyncBefore,
        LogFault::SyncAfter,
    ] {
        let (_root, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let db = DocumentDatabase::with_checkpoint_store(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "i".into(),
                fields: vec![vec!["x".into()]],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
        db.insert("c", "id", &json!({"x":1})).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let db = DocumentDatabase::with_checkpoint_store(&store);
        assert!(
            db.transaction(|tx| {
                tx.replace("c", "id", &json!({"x":2}), false)?;
                tx.put_raw(b"receipt", b"new")
            })
            .is_err()
        );
        assert!(store.get(b"base").is_err());
        drop(store);
        let bytes = fs::read(path.join("records.log")).unwrap();
        match retry_lock(|| Store::open(&path)) {
            Ok(reopened) => {
                let db = DocumentDatabase::new(&reopened);
                let value = db.get("c", "id").unwrap().unwrap()["x"].as_i64().unwrap();
                assert!(value == 1 || value == 2);
                assert_eq!(
                    db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
                    (value == 2).then(|| b"new".to_vec())
                );
                assert_eq!(db.verify_collection("c").unwrap().index_keys, 2);
                assert_eq!(
                    db.query_index(
                        "c",
                        "i",
                        &IndexQuery {
                            equal: vec![IndexValue::Integer(value)],
                            ..Default::default()
                        }
                    )
                    .unwrap()
                    .documents[0]
                        .id,
                    "id"
                );
            }
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )),
        }
        assert_eq!(fs::read(path.join("records.log")).unwrap(), bytes);
    }
}

#[test]
fn array_index_and_raw_write_recover_together_for_every_v2_append_cut() {
    use crate::document::array_fault_model::{prepare, update, verify};
    let length = {
        let (_root, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let db = DocumentDatabase::with_checkpoint_store(&store);
        prepare(&db);
        let log = path.join("records.log");
        let before = fs::metadata(&log).unwrap().len();
        update(&db).unwrap();
        usize::try_from(fs::metadata(log).unwrap().len() - before).unwrap()
    };
    assert!(length > 30);
    eprintln!("V2 array transaction append cut points: {}", length + 1);
    let mut faults = (0..=length).map(LogFault::Partial).collect::<Vec<_>>();
    faults.extend([
        LogFault::SyncBefore,
        LogFault::SyncAfter,
        LogFault::SyncTimeout,
        LogFault::Panic(0),
        LogFault::Panic(length / 2),
        LogFault::Panic(length),
    ]);
    for fault in faults {
        let (_root, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        prepare(&DocumentDatabase::with_checkpoint_store(&store));
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            update(&DocumentDatabase::with_checkpoint_store(&store))
        }));
        assert!(!matches!(outcome, Ok(Ok(()))));
        assert!(store.get(b"base").is_err());
        drop(store);
        let log = path.join("records.log");
        let bytes = fs::read(&log).unwrap();
        match retry_lock(|| Store::open(&path)) {
            Ok(reopened) => verify(&DocumentDatabase::new(&reopened)),
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )),
        }
        assert_eq!(fs::read(log).unwrap(), bytes);
    }
}
