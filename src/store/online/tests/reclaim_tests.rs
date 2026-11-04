use super::*;
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;

#[test]
fn reclamation_removes_old_history_and_recovers_live_state() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    store.with_store(|db| db.delete(b"delete")).unwrap();
    let before = fs::metadata(source.join("records.log")).unwrap().len();
    let report = store
        .compact_and_reclaim(&previous, &Default::default())
        .unwrap();
    assert_eq!(report.final_log_bytes, before);
    assert!(report.compacted_log_bytes < before);
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|db| db.get(b"overwritten")).unwrap(),
        Some(b"39".to_vec())
    );
    assert_eq!(store.with_store(|db| db.get(b"delete")).unwrap(), None);
    store.with_store(|db| db.put(b"after", b"handoff")).unwrap();
    drop(store);
    let reopened = retry_lock(|| Store::open(&source)).unwrap();
    assert_eq!(reopened.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
    assert_eq!(reopened.get(b"after").unwrap(), Some(b"handoff".to_vec()));
    assert_eq!(reopened.get(b"delete").unwrap(), None);
}

#[test]
fn every_reclamation_failure_keeps_the_synchronized_live_store_usable() {
    for fail_at in [
        reclaim::Point::BeforeMove,
        reclaim::Point::AfterMove,
        reclaim::Point::AfterRecords,
        reclaim::Point::AfterWitness,
        reclaim::Point::AfterLock,
        reclaim::Point::AfterRetiredSync,
        reclaim::Point::AfterRetiredRemoval,
        reclaim::Point::AfterStageSync,
        reclaim::Point::AfterStageRemoval,
        reclaim::Point::AfterParentSync,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = OnlineStore::open(&source).unwrap();
        insert_history(&store);
        let result = store.compact_reclaim_with_hooks(
            &previous,
            &Default::default(),
            |_| Ok(()),
            crate::directory::sync_existing,
            |point| {
                if point == fail_at {
                    return Err(io::Error::other("injected reclamation failure"));
                }
                Ok(())
            },
        );
        let Err(OnlineReclaimError::Reclamation { report, error }) = result else {
            panic!("expected reclamation failure");
        };
        assert!(report.compacted_log_bytes > 0);
        assert!(error.to_string().contains("injected reclamation failure"));
        if fail_at == reclaim::Point::BeforeMove {
            let old = retry_lock(|| Store::open(&previous)).unwrap();
            assert_eq!(old.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        } else {
            assert!(!previous.exists());
        }
        assert_eq!(
            store.with_store(|db| db.get(b"overwritten")).unwrap(),
            Some(b"39".to_vec())
        );
        store.with_store(|db| db.put(b"after", b"safe")).unwrap();
        crate::cleanup_abandoned_staging(root.path()).unwrap();
        drop(store);
        let live = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        assert_eq!(live.get(b"after").unwrap(), Some(b"safe".to_vec()));
    }
}

#[test]
fn handoff_failure_never_starts_reclamation() {
    for fail_at in [copy::Point::BeforeExchange, copy::Point::AfterExchange] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = OnlineStore::open(&source).unwrap();
        insert_history(&store);
        let called = std::sync::atomic::AtomicBool::new(false);
        let result = store.compact_reclaim_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == fail_at {
                    return Err(io::Error::other("injected handoff failure"));
                }
                Ok(())
            },
            crate::directory::sync_existing,
            |_| {
                called.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            },
        );
        assert!(matches!(result, Err(OnlineReclaimError::Compaction(_))));
        assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
        drop(store);
        for path in [&source, &previous] {
            let recovered = retry_lock(|| Store::open(path)).unwrap();
            assert_eq!(recovered.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        }
    }
}

#[test]
fn reads_continue_while_mutations_wait_and_the_new_previous_path_is_safe() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let worker_store = Arc::clone(&store);
    let old_path = previous.clone();
    let worker = thread::spawn(move || {
        worker_store.compact_reclaim_with_hooks(
            &old_path,
            &Default::default(),
            |_| Ok(()),
            crate::directory::sync_existing,
            |point| {
                if point == reclaim::Point::AfterMove {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let read_store = Arc::clone(&store);
    let reader = thread::spawn(move || {
        done_tx
            .send(read_store.with_store(|db| db.get(b"overwritten")))
            .unwrap()
    });
    let callback = done_rx.recv_timeout(WAIT);
    let (started_tx, started_rx) = mpsc::channel();
    let (write_tx, write_rx) = mpsc::channel();
    let write_store = Arc::clone(&store);
    let writer = thread::spawn(move || {
        started_tx.send(()).unwrap();
        write_tx
            .send(write_store.with_store(|db| db.put(b"live", b"still active")))
            .unwrap();
    });
    started_rx.recv_timeout(WAIT).unwrap();
    let write_before_cleanup = write_rx.recv_timeout(Duration::from_millis(100));
    let replacement = Store::open(&previous).unwrap();
    replacement.put(b"new", b"independent").unwrap();
    drop(replacement);
    resume_tx.send(()).unwrap();
    assert_eq!(callback.unwrap().unwrap(), Some(b"39".to_vec()));
    assert!(matches!(
        write_before_cleanup,
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    reader.join().unwrap();
    worker.join().unwrap().unwrap();
    write_rx.recv_timeout(WAIT).unwrap().unwrap();
    writer.join().unwrap();
    let replacement = retry_lock(|| Store::open(&previous)).unwrap();
    assert_eq!(
        replacement.get(b"new").unwrap(),
        Some(b"independent".to_vec())
    );
    assert_eq!(
        store.with_store(|db| db.get(b"live")).unwrap(),
        Some(b"still active".to_vec())
    );
}

#[test]
fn acknowledged_concurrent_increments_survive_repeated_reclamation_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let (source, _) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    let start = Arc::new(Barrier::new(5));
    let values = Arc::new(Mutex::new(Vec::new()));
    thread::scope(|scope| {
        for _ in 0..4 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            let values = Arc::clone(&values);
            scope.spawn(move || {
                start.wait();
                for _ in 0..64 {
                    let value = store
                        .with_store(|db| db.increment_i64(b"count", 1))
                        .unwrap();
                    values.lock().unwrap().push(value);
                }
            });
        }
        start.wait();
        for n in 0..4 {
            store
                .compact_and_reclaim(
                    root.path().join(format!("previous-{n}")),
                    &Default::default(),
                )
                .unwrap();
        }
    });
    let mut actual = values.lock().unwrap().clone();
    actual.sort_unstable();
    assert_eq!(actual, (1..=256).collect::<Vec<_>>());
    assert_eq!(
        store.with_store(|db| db.get(b"count")).unwrap(),
        Some(256_i64.to_le_bytes().to_vec())
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"count")
            .unwrap(),
        Some(256_i64.to_le_bytes().to_vec())
    );
}

#[test]
fn online_reclaim_kill_child() {
    use std::io::Write;
    let Ok(selected) = std::env::var("EMDA_ONLINE_RECLAIM_CHILD") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("EMDA_ONLINE_RECLAIM_ROOT").unwrap());
    let (source, previous) = paths(&root);
    let selected = match selected.as_str() {
        "before-move" => reclaim::Point::BeforeMove,
        "after-move" => reclaim::Point::AfterMove,
        "after-records" => reclaim::Point::AfterRecords,
        "after-lock" => reclaim::Point::AfterLock,
        "after-retired-sync" => reclaim::Point::AfterRetiredSync,
        "after-retired-removal" => reclaim::Point::AfterRetiredRemoval,
        "after-stage-sync" => reclaim::Point::AfterStageSync,
        "after-stage-removal" => reclaim::Point::AfterStageRemoval,
        "after-parent-sync" => reclaim::Point::AfterParentSync,
        _ => panic!("unknown hook"),
    };
    let store = retry_lock(|| OnlineStore::open(&source)).unwrap();
    let _ = store.compact_reclaim_with_hooks(
        &previous,
        &Default::default(),
        |_| Ok(()),
        crate::directory::sync_existing,
        |point| {
            if point == selected {
                let mut signal = fs::File::create(root.join("signal")).unwrap();
                signal.write_all(b"ready").unwrap();
                signal.sync_all().unwrap();
                loop {
                    thread::park();
                }
            }
            Ok(())
        },
    );
    panic!("kill hook did not run");
}

#[test]
fn process_kill_at_each_reclamation_boundary_preserves_acknowledged_data() {
    use std::process::{Command, Stdio};
    use std::time::Instant;
    for point in [
        "before-move",
        "after-move",
        "after-records",
        "after-lock",
        "after-retired-sync",
        "after-retired-removal",
        "after-stage-sync",
        "after-stage-removal",
        "after-parent-sync",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = OnlineStore::open(&source).unwrap();
        insert_history(&store);
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("store::online::tests::reclaim_tests::online_reclaim_kill_child")
            .env("EMDA_ONLINE_RECLAIM_CHILD", point)
            .env("EMDA_ONLINE_RECLAIM_ROOT", root.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + WAIT;
        while !root.path().join("signal").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before hook: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not reach hook {point}");
            }
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let live = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        assert_eq!(live.get(b"delete").unwrap(), Some(b"old".to_vec()));
        if point == "before-move" {
            let old = retry_lock(|| Store::open(&previous)).unwrap();
            assert_eq!(old.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        } else {
            assert!(!previous.exists());
        }
        crate::cleanup_abandoned_staging(root.path()).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        drop(live);
        assert_eq!(
            retry_lock(|| Store::open(&source))
                .unwrap()
                .get(b"overwritten")
                .unwrap(),
            Some(b"39".to_vec())
        );
    }
}

#[test]
fn panic_during_reclamation_does_not_disable_the_live_store() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.compact_reclaim_with_hooks(
            &previous,
            &Default::default(),
            |_| Ok(()),
            crate::directory::sync_existing,
            |point| {
                if point == reclaim::Point::AfterMove {
                    panic!("injected reclamation panic");
                }
                Ok(())
            },
        )
    }));
    assert!(outcome.is_err());
    assert_eq!(
        store.with_store(|db| db.get(b"overwritten")).unwrap(),
        Some(b"39".to_vec())
    );
    assert!(fs::read_dir(root.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .join("retired")
            .join("records.log")
            .exists()
    }));
    crate::cleanup_abandoned_staging(root.path()).unwrap();
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"overwritten")
            .unwrap(),
        Some(b"39".to_vec())
    );
}

#[test]
fn replacement_of_the_previous_path_does_not_delete_unrelated_data() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let moved = root.path().join("moved-original");
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    let result = store.compact_reclaim_with_hooks(
        &previous,
        &Default::default(),
        |_| Ok(()),
        crate::directory::sync_existing,
        |point| {
            if point == reclaim::Point::BeforeMove {
                fs::rename(&previous, &moved)?;
                fs::create_dir(&previous)?;
                fs::write(previous.join("unrelated"), b"keep")?;
            }
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(OnlineReclaimError::Reclamation { .. })
    ));
    assert_eq!(fs::read(previous.join("unrelated")).unwrap(), b"keep");
    assert_eq!(
        store.with_store(|db| db.get(b"overwritten")).unwrap(),
        Some(b"39".to_vec())
    );
    let original = retry_lock(|| Store::open(&moved)).unwrap();
    assert_eq!(original.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
}

#[test]
fn unexpected_files_in_retired_stage_are_retained_for_inspection() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    let mut unexpected = None;
    let result = store.compact_reclaim_with_hooks(
        &previous,
        &Default::default(),
        |_| Ok(()),
        crate::directory::sync_existing,
        |point| {
            if point == reclaim::Point::AfterMove {
                let stage = fs::read_dir(root.path())?
                    .map(|entry| entry.unwrap().path())
                    .find(|path| path.join("retired").is_dir())
                    .expect("retired stage");
                let path = stage.join("retired").join("unrelated");
                fs::write(&path, b"keep")?;
                unexpected = Some(path);
            }
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(OnlineReclaimError::Reclamation { .. })
    ));
    assert_eq!(fs::read(unexpected.unwrap()).unwrap(), b"keep");
    assert_eq!(
        store.with_store(|db| db.get(b"overwritten")).unwrap(),
        Some(b"39".to_vec())
    );
}

#[test]
fn reclamation_supports_legacy_v2_and_v3_store_fixtures() {
    for version in ["legacy", "v2", "v3"] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        fs::create_dir(&source).unwrap();
        match version {
            "legacy" => fs::write(
                source.join("records.log"),
                include_bytes!("../../../../tests/fixtures/db01-legacy-record.bin"),
            )
            .unwrap(),
            "v2" => {
                fs::write(
                    source.join("records.log"),
                    include_bytes!("../../../../tests/fixtures/v2-batch-records.log"),
                )
                .unwrap();
                fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../../../tests/fixtures/v2-batch-commit.state"),
                )
                .unwrap();
            }
            "v3" => {
                fs::write(
                    source.join("records.log"),
                    include_bytes!("../../../../tests/fixtures/v3-mixed-records.log"),
                )
                .unwrap();
                fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../../../tests/fixtures/v3-mixed-commit.state"),
                )
                .unwrap();
                rustix::fs::setxattr(
                    source.join("records.log"),
                    "user.db.commit.v3",
                    include_bytes!("../../../../tests/fixtures/v3-mixed-metadata.state"),
                    rustix::fs::XattrFlags::empty(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let store = OnlineStore::open(&source).unwrap();
        store.with_store(|db| db.put(b"later", b"write")).unwrap();
        let expected = store.with_store(|db| db.scan(None, None, 100)).unwrap();
        store
            .compact_and_reclaim(&previous, &Default::default())
            .unwrap();
        assert!(!previous.exists());
        assert_eq!(
            store.with_store(|db| db.scan(None, None, 100)).unwrap(),
            expected,
            "{version}"
        );
        drop(store);
        assert_eq!(
            retry_lock(|| Store::open(&source))
                .unwrap()
                .scan(None, None, 100)
                .unwrap(),
            expected,
            "{version}"
        );
    }
}

#[test]
fn unexpected_files_are_preserved_when_reclamation_unwinds() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.compact_reclaim_with_hooks(
            &previous,
            &Default::default(),
            |_| Ok(()),
            crate::directory::sync_existing,
            |point| {
                if point == reclaim::Point::AfterMove {
                    let stage = fs::read_dir(root.path())?
                        .map(|entry| entry.unwrap().path())
                        .find(|path| path.join("retired").is_dir())
                        .expect("retired stage");
                    fs::write(stage.join("retired").join("unrelated"), b"keep")?;
                    panic!("injected reclamation panic");
                }
                Ok(())
            },
        )
    }));
    assert!(outcome.is_err());
    let retained = fs::read_dir(root.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("retired").join("unrelated").exists())
        .expect("retained stage");
    assert_eq!(
        fs::read(retained.join("retired").join("unrelated")).unwrap(),
        b"keep"
    );
    assert_eq!(
        store.with_store(|db| db.get(b"overwritten")).unwrap(),
        Some(b"39".to_vec())
    );
}

#[test]
fn reclamation_preserves_document_indexes_and_raw_transaction_data() {
    use crate::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue};
    use serde_json::json;
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store
        .with_store(|raw| {
            let db = DocumentDatabase::new(raw);
            db.create_collection("c", CollectionSchema::default())?;
            db.create_index(
                "c",
                IndexDefinition {
                    name: "email".into(),
                    fields: vec![vec!["email".into()]],
                    unique: true,
                    partial: None,
                },
            )?;
            db.transaction(|tx| {
                tx.insert("c", "id", &json!({"email":"x", "items":[1, 2]}))?;
                tx.put_raw(b"receipt", b"committed")
            })
        })
        .unwrap();
    store
        .compact_and_reclaim(&previous, &Default::default())
        .unwrap();
    let verify = |raw: &Store| -> io::Result<()> {
        let db = DocumentDatabase::new(raw);
        assert_eq!(
            db.get("c", "id")?,
            Some(json!({"email":"x", "items":[1, 2]}))
        );
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"receipt"))?,
            Some(b"committed".to_vec())
        );
        let page = db.query_index(
            "c",
            "email",
            &IndexQuery {
                equal: vec![IndexValue::String("x".into())],
                ..Default::default()
            },
        )?;
        assert_eq!(page.documents.len(), 1);
        assert_eq!(page.documents[0].id, "id");
        assert_eq!(db.verify_collection("c")?.index_keys, 2);
        Ok(())
    };
    store.with_store(verify).unwrap();
    drop(store);
    let recovered = retry_lock(|| Store::open(&source)).unwrap();
    verify(&recovered).unwrap();
}

#[test]
fn reclamation_keeps_per_open_limits_enforced_after_handoff() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store =
        OnlineStore::open_with_resource_limits(&source, Some(64 * 1024), Some(2), Some(2)).unwrap();
    store
        .with_store(|db| {
            db.put(b"a", b"1")?;
            db.put(b"b", b"2")
        })
        .unwrap();
    store
        .compact_and_reclaim(&previous, &Default::default())
        .unwrap();
    assert!(store.with_store(|db| db.put(b"c", b"3")).is_err());
    store
        .with_store(|db| {
            db.put(b"a", b"new")?;
            db.delete(b"b")?;
            db.put(b"c", b"3")
        })
        .unwrap();
    assert_eq!(
        store.with_store(|db| db.get(b"a")).unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        store.with_store(|db| db.get(b"c")).unwrap(),
        Some(b"3".to_vec())
    );
}
