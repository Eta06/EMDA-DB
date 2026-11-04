use super::*;
use crate::test_support::retry_lock;
use crate::{
    BatchOperation, CollectionSchema, DocumentDatabase, IndexDefinition, IndexJobStatus, IndexQuery,
};
use serde_json::json;
use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(10);

fn paths(root: &Path) -> (PathBuf, PathBuf) {
    (root.join("live"), root.join("previous"))
}

fn insert_history(store: &OnlineStore) {
    store
        .with_store(|db| {
            for n in 0..40 {
                db.put(b"overwritten", n.to_string().as_bytes())?;
            }
            db.put(b"delete", b"old")?;
            Ok(())
        })
        .unwrap();
}

#[test]
fn catches_up_acknowledged_writes_and_retains_previous_store() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    let (stage_tx, stage_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let worker = Arc::clone(&store);
    let old_path = previous.clone();
    let thread = thread::spawn(move || {
        worker.compact_with_hooks(
            &old_path,
            &OnlineCompactionOptions::default(),
            |point| {
                if matches!(
                    point,
                    copy::Point::InitialCopy | copy::Point::BeforeFinalBarrier
                ) {
                    stage_tx.send(point).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
    });
    assert_eq!(
        stage_rx.recv_timeout(WAIT).unwrap(),
        copy::Point::InitialCopy
    );
    store
        .with_store(|db| {
            db.write_batch(&[
                BatchOperation::Put {
                    key: b"overwritten",
                    value: b"concurrent",
                },
                BatchOperation::Delete { key: b"delete" },
                BatchOperation::Put {
                    key: b"batch",
                    value: b"committed",
                },
            ])
        })
        .unwrap();
    resume_tx.send(()).unwrap();
    assert_eq!(
        stage_rx.recv_timeout(WAIT).unwrap(),
        copy::Point::BeforeFinalBarrier
    );
    store.with_store(|db| db.put(b"final", b"tail")).unwrap();
    resume_tx.send(()).unwrap();
    let report = thread.join().unwrap().unwrap();
    assert!(report.concurrent_replay_bytes > 0);
    assert!(report.final_replay_bytes > 0);
    assert!(report.compacted_log_bytes < report.final_log_bytes);
    store
        .with_store(|db| {
            assert_eq!(db.get(b"overwritten")?, Some(b"concurrent".to_vec()));
            assert_eq!(db.get(b"delete")?, None);
            assert_eq!(db.get(b"batch")?, Some(b"committed".to_vec()));
            assert_eq!(db.get(b"final")?, Some(b"tail".to_vec()));
            db.put(b"after", b"only new")
        })
        .unwrap();
    let old = retry_lock(|| Store::open(&previous)).unwrap();
    assert_eq!(
        old.get(b"overwritten").unwrap(),
        Some(b"concurrent".to_vec())
    );
    assert_eq!(old.get(b"final").unwrap(), Some(b"tail".to_vec()));
    assert_eq!(old.get(b"after").unwrap(), None);
    drop(old);
    drop(store);
    let reopened = retry_lock(|| Store::open(&source)).unwrap();
    assert_eq!(reopened.get(b"after").unwrap(), Some(b"only new".to_vec()));
}

#[test]
fn pending_document_jobs_continue_through_compaction() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store
        .with_store(|s| {
            let db = DocumentDatabase::new(s);
            db.create_collection("c", CollectionSchema::default())?;
            for (id, n) in [("a", 1), ("b", 2)] {
                db.insert("c", id, &json!({"n":n}))?;
            }
            db.start_index_build(
                "c",
                IndexDefinition {
                    name: "n".into(),
                    fields: vec![vec!["n".into()]],
                    unique: true,
                    partial: None,
                },
            )?;
            db.advance_index_build("c", "n", 1)?;
            Ok(())
        })
        .unwrap();
    store
        .compact_and_replace(&previous, &OnlineCompactionOptions::default())
        .unwrap();
    store.with_store(|s| {
        let db = DocumentDatabase::new(s);
        assert!(matches!(&db.index_jobs("c")?[0], IndexJobStatus::Building { after: Some(id), .. } if id == "a"));
        db.insert("c", "c", &json!({"n":3}))?;
        while !db.advance_index_build("c", "n", 1)?.complete {}
        assert_eq!(db.query_index("c", "n", &IndexQuery::default())?.documents.len(), 3);
        db.verify_collection("c")?;
        Ok(())
    }).unwrap();
}

#[test]
fn active_snapshot_finishes_before_handoff() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    store.with_store(|s| s.put(b"key", b"before")).unwrap();
    let (snap_tx, snap_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let reader_store = Arc::clone(&store);
    let reader = thread::spawn(move || {
        reader_store
            .with_store(|s| {
                let snapshot = s.snapshot(10, 100)?;
                snap_tx.send(()).unwrap();
                release_rx.recv_timeout(WAIT).unwrap();
                assert_eq!(snapshot.get(b"key")?, Some(b"before".to_vec()));
                Ok(())
            })
            .unwrap()
    });
    snap_rx.recv_timeout(WAIT).unwrap();
    store.with_store(|s| s.put(b"key", b"after")).unwrap();
    let (barrier_tx, barrier_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let compactor = Arc::clone(&store);
    let worker = thread::spawn(move || {
        let result = compactor.compact_with_hooks(
            &previous,
            &OnlineCompactionOptions::default(),
            |point| {
                if point == copy::Point::BeforeFinalBarrier {
                    barrier_tx.send(()).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        );
        done_tx.send(result).unwrap();
    });
    barrier_rx.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release_tx.send(()).unwrap();
    reader.join().unwrap();
    done_rx.recv_timeout(WAIT).unwrap().unwrap();
    worker.join().unwrap();
    assert_eq!(
        store.with_store(|s| s.get(b"key")).unwrap(),
        Some(b"after".to_vec())
    );
}

#[test]
fn suffix_limit_aborts_before_publication_and_keeps_source_usable() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    let options = OnlineCompactionOptions {
        catchup_rounds: 0,
        max_final_delta_bytes: 0,
        ..Default::default()
    };
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let worker_store = Arc::clone(&store);
    let old_path = previous.clone();
    let worker = thread::spawn(move || {
        worker_store.compact_with_hooks(
            &old_path,
            &options,
            |point| {
                if point == copy::Point::BeforeFinalBarrier {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    store.with_store(|s| s.put(b"late", b"write")).unwrap();
    resume_tx.send(()).unwrap();
    assert!(
        worker
            .join()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("suffix")
    );
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|s| s.get(b"late")).unwrap(),
        Some(b"write".to_vec())
    );
    store.with_store(|s| s.put(b"still", b"usable")).unwrap();
}

#[test]
fn configured_limits_and_unsafe_paths_fail_before_exchange() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store =
        OnlineStore::open_with_resource_limits(&source, Some(1_000_000), Some(2), Some(2)).unwrap();
    store
        .with_store(|s| {
            s.put(b"a", b"one")?;
            s.put(b"b", b"two")
        })
        .unwrap();
    let bad = OnlineCompactionOptions {
        max_snapshot_keys: 1,
        ..Default::default()
    };
    assert!(store.compact_and_replace(&previous, &bad).is_err());
    let bad = OnlineCompactionOptions {
        max_temporary_log_bytes: 1,
        ..Default::default()
    };
    assert!(store.compact_and_replace(&previous, &bad).is_err());
    let bad = OnlineCompactionOptions {
        catchup_rounds: 17,
        ..Default::default()
    };
    assert_eq!(
        store
            .compact_and_replace(&previous, &bad)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    fs::write(source.join("user-file"), b"keep").unwrap();
    assert_eq!(
        store
            .compact_and_replace(&previous, &Default::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    fs::remove_file(source.join("user-file")).unwrap();
    fs::create_dir(root.path().join("other")).unwrap();
    assert_eq!(
        store
            .compact_and_replace(
                root.path().join("other").join("previous"),
                &Default::default()
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    fs::create_dir(&previous).unwrap();
    assert_eq!(
        store
            .compact_and_replace(&previous, &Default::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    fs::remove_dir(&previous).unwrap();
    store
        .compact_and_replace(&previous, &Default::default())
        .unwrap();
    assert!(store.with_store(|s| s.put(b"c", b"limit")).is_err());
    assert_eq!(
        store.with_store(|s| s.get(b"a")).unwrap(),
        Some(b"one".to_vec())
    );
}

#[test]
fn nested_access_and_overlapping_compactions_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    store
        .with_store(|_| {
            assert_eq!(
                store.with_store(|_| Ok(())).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                store
                    .compact_and_replace(&previous, &Default::default())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            Ok(())
        })
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let other = Arc::clone(&store);
    let old = previous.clone();
    let worker = thread::spawn(move || {
        other.compact_with_hooks(
            &old,
            &Default::default(),
            |point| {
                if point == copy::Point::InitialCopy {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(
        store
            .compact_and_replace(root.path().join("other"), &Default::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    resume_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
}

#[test]
fn failure_injection_preserves_source_and_poisoning_after_exchange() {
    for fail_at in [
        copy::Point::InitialCopy,
        copy::Point::BeforeFinalBarrier,
        copy::Point::BeforeExchange,
        copy::Point::AfterExchange,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = OnlineStore::open(&source).unwrap();
        insert_history(&store);
        let result = store.compact_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == fail_at {
                    return Err(io::Error::other("injected"));
                }
                Ok(())
            },
            crate::directory::sync_existing,
        );
        assert!(result.is_err());
        if fail_at == copy::Point::AfterExchange {
            assert!(
                store
                    .with_store(|s| s.get(b"overwritten"))
                    .unwrap_err()
                    .to_string()
                    .contains("reopen")
            );
        } else {
            assert_eq!(
                store.with_store(|s| s.get(b"overwritten")).unwrap(),
                Some(b"39".to_vec())
            );
        }
        drop(store);
        let live = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        if previous.exists() {
            let old = retry_lock(|| Store::open(&previous)).unwrap();
            assert_eq!(old.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        }
    }
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store.with_store(|s| s.put(b"key", b"value")).unwrap();
    assert!(
        store
            .compact_with_hooks(
                &previous,
                &Default::default(),
                |_| Ok(()),
                |_| Err(io::Error::other("injected parent sync"))
            )
            .is_err()
    );
    assert!(store.with_store(|s| s.get(b"key")).is_err());
    drop(store);
    for path in [&source, &previous] {
        assert_eq!(
            retry_lock(|| Store::open(path))
                .unwrap()
                .get(b"key")
                .unwrap(),
            Some(b"value".to_vec())
        );
    }
}

#[test]
fn fixed_log_ranges_exclude_later_appends_and_reject_torn_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    store.put(b"a", b"one").unwrap();
    let first = copy::boundary(&store).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"b",
                value: b"two",
            },
            BatchOperation::Delete { key: b"a" },
        ])
        .unwrap();
    let second = copy::boundary(&store).unwrap();
    let mut initial = Vec::new();
    store
        .log
        .visit_synchronized_range(0, first, |key, location| {
            initial.push((key, location.is_some()));
            Ok(())
        })
        .unwrap();
    assert_eq!(initial, vec![(b"a".to_vec(), true)]);
    let mut tail = Vec::new();
    store
        .log
        .visit_synchronized_range(first, second, |key, location| {
            tail.push((key, location.is_some()));
            Ok(())
        })
        .unwrap();
    assert_eq!(tail, vec![(b"b".to_vec(), true), (b"a".to_vec(), false)]);
    assert!(
        store
            .log
            .visit_synchronized_range(first, second - 1, |_, _| Ok(()))
            .is_err()
    );
    assert!(
        store
            .log
            .visit_synchronized_range(second, first, |_, _| Ok(()))
            .is_err()
    );
    assert!(
        store
            .log
            .visit_synchronized_range(first, second + 1, |_, _| Ok(()))
            .is_err()
    );
}

#[test]
fn online_compaction_kill_child() {
    let Ok(selected) = std::env::var("EMDA_ONLINE_COMPACTION_CHILD") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("EMDA_ONLINE_COMPACTION_ROOT").unwrap());
    let (source, previous) = paths(&root);
    let store = OnlineStore::open(&source).unwrap();
    let selected = match selected.as_str() {
        "copy" => copy::Point::InitialCopy,
        "published" => copy::Point::BeforeExchange,
        "exchange" => copy::Point::AfterExchange,
        _ => panic!("unknown hook"),
    };
    let _ = store.compact_with_hooks(
        &previous,
        &Default::default(),
        |point| {
            if point == selected {
                let mut signal = File::create(root.join("signal")).unwrap();
                signal.write_all(b"ready").unwrap();
                signal.sync_all().unwrap();
                loop {
                    thread::park();
                }
            }
            Ok(())
        },
        crate::directory::sync_existing,
    );
    panic!("kill hook did not run");
}

#[test]
fn process_kill_at_copy_publication_and_exchange_recovers_complete_data() {
    for point in ["copy", "published", "exchange"] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = OnlineStore::open(&source).unwrap();
        insert_history(&store);
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("store::online::tests::online_compaction_kill_child")
            .env("EMDA_ONLINE_COMPACTION_CHILD", point)
            .env("EMDA_ONLINE_COMPACTION_ROOT", root.path())
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
                panic!("child did not reach hook");
            }
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let live = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        if point == "copy" {
            assert!(!previous.exists());
        } else {
            let old = retry_lock(|| Store::open(&previous)).unwrap();
            assert_eq!(old.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        }
        drop(live);
        let result = crate::cleanup_abandoned_staging(root.path()).unwrap();
        if point == "copy" {
            assert!(result.removed_payloads >= 1);
        }
    }
}

#[test]
fn staged_corruption_is_detected_before_exchange() {
    use std::os::unix::fs::FileExt;
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store.with_store(|s| s.put(b"key", b"intact")).unwrap();
    let result = store.compact_with_hooks(
        &previous,
        &Default::default(),
        |point| {
            if point == copy::Point::InitialCopy {
                let stage = fs::read_dir(root.path())?
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .unwrap()
                            .to_string_lossy()
                            .ends_with(".owner.data")
                    })
                    .expect("private stage directory");
                let file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(stage.join("records.log"))?;
                let offset = file.metadata()?.len() - 1;
                let mut byte = [0_u8];
                file.read_exact_at(&mut byte, offset)?;
                byte[0] ^= 1;
                file.write_all_at(&byte, offset)?;
            }
            Ok(())
        },
        crate::directory::sync_existing,
    );
    assert!(result.is_err());
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|s| s.get(b"key")).unwrap(),
        Some(b"intact".to_vec())
    );
}

#[test]
fn large_unbatchable_values_are_copied_and_reopened() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    let value = vec![0x5a; crate::record::MAX_VALUE];
    store.with_store(|s| s.put(b"large", &value)).unwrap();
    store
        .compact_and_replace(&previous, &Default::default())
        .unwrap();
    assert_eq!(
        store.with_store(|s| s.get(b"large")).unwrap(),
        Some(value.clone())
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"large")
            .unwrap(),
        Some(value)
    );
}

#[test]
fn historical_peak_and_lock_ownership_are_enforced() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store
        .with_store(|s| {
            s.put(b"deleted", b"value")?;
            s.delete(b"deleted")
        })
        .unwrap();
    let options = OnlineCompactionOptions {
        max_snapshot_keys: 0,
        ..Default::default()
    };
    assert!(
        store
            .compact_and_replace(&previous, &options)
            .unwrap_err()
            .to_string()
            .contains("historical")
    );
    store.with_store(|s| s.put(b"present", b"value")).unwrap();
    store
        .compact_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == copy::Point::BeforeExchange {
                    assert!(Store::open(&source).is_err());
                    assert!(Store::open(&previous).is_err());
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
        .unwrap();
    assert_eq!(
        store.with_store(|s| s.get(b"present")).unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn legacy_v2_and_v3_fixtures_compact_without_changing_live_data() {
    for version in ["legacy", "v2", "v3"] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        fs::create_dir(&source).unwrap();
        match version {
            "legacy" => fs::write(
                source.join("records.log"),
                include_bytes!("../../../tests/fixtures/db01-legacy-record.bin"),
            )
            .unwrap(),
            "v2" => {
                fs::write(
                    source.join("records.log"),
                    include_bytes!("../../../tests/fixtures/v2-batch-records.log"),
                )
                .unwrap();
                fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../../tests/fixtures/v2-batch-commit.state"),
                )
                .unwrap();
            }
            "v3" => {
                fs::write(
                    source.join("records.log"),
                    include_bytes!("../../../tests/fixtures/v3-mixed-records.log"),
                )
                .unwrap();
                fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../../tests/fixtures/v3-mixed-commit.state"),
                )
                .unwrap();
                rustix::fs::setxattr(
                    source.join("records.log"),
                    "user.db.commit.v3",
                    include_bytes!("../../../tests/fixtures/v3-mixed-metadata.state"),
                    rustix::fs::XattrFlags::empty(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let store = OnlineStore::open(&source).unwrap();
        store.with_store(|s| s.put(b"later", b"write")).unwrap();
        let expected = store.with_store(|s| s.scan(None, None, 100)).unwrap();
        store
            .compact_and_replace(&previous, &Default::default())
            .unwrap();
        assert_eq!(
            store.with_store(|s| s.scan(None, None, 100)).unwrap(),
            expected,
            "{version}"
        );
        assert_eq!(
            retry_lock(|| Store::open(&previous))
                .unwrap()
                .scan(None, None, 100)
                .unwrap(),
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
fn source_prefix_corruption_does_not_publish_a_copy_or_repair_data() {
    use std::os::unix::fs::FileExt;
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store
        .with_store(|s| {
            s.put(b"key", b"obsolete")?;
            s.put(b"key", b"latest")
        })
        .unwrap();
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(source.join("records.log"))
        .unwrap();
    let offset = crate::witness::LOG_HEADER_SIZE as u64 + crate::record::HEADER_SIZE as u64;
    let mut byte = [0_u8];
    file.read_exact_at(&mut byte, offset).unwrap();
    byte[0] ^= 1;
    file.write_all_at(&byte, offset).unwrap();
    let corrupted = fs::read(source.join("records.log")).unwrap();
    assert!(
        store
            .compact_and_replace(&previous, &Default::default())
            .is_err()
    );
    assert!(!previous.exists());
    assert_eq!(fs::read(source.join("records.log")).unwrap(), corrupted);
    drop(store);
    assert!(retry_lock(|| Store::open(&source)).is_err());
}

#[test]
fn transaction_finishes_before_handoff_and_its_commit_is_replayed() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    store.with_store(|s| s.put(b"account", b"old")).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let tx_store = Arc::clone(&store);
    let transaction = thread::spawn(move || {
        tx_store
            .with_store(|s| {
                s.transaction_with_read_set(|tx| {
                    assert_eq!(tx.get(b"account")?, Some(b"old".to_vec()));
                    tx.put(b"account", b"new")?;
                    tx.put(b"audit", b"committed")?;
                    started_tx.send(()).unwrap();
                    finish_rx.recv_timeout(WAIT).unwrap();
                    Ok(())
                })
            })
            .unwrap()
    });
    started_rx.recv_timeout(WAIT).unwrap();
    let (barrier_tx, barrier_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let compactor = Arc::clone(&store);
    let worker = thread::spawn(move || {
        let result = compactor.compact_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == copy::Point::BeforeFinalBarrier {
                    barrier_tx.send(()).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        );
        done_tx.send(result).unwrap();
    });
    barrier_rx.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    finish_tx.send(()).unwrap();
    transaction.join().unwrap();
    let report = done_rx.recv_timeout(WAIT).unwrap().unwrap();
    assert!(report.final_replay_bytes > 0);
    worker.join().unwrap();
    store
        .with_store(|s| {
            assert_eq!(s.get(b"account")?, Some(b"new".to_vec()));
            assert_eq!(s.get(b"audit")?, Some(b"committed".to_vec()));
            Ok(())
        })
        .unwrap();
}

#[test]
fn target_quota_failure_during_catchup_keeps_later_source_write() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    store.with_store(|s| s.put(b"a", b"1")).unwrap();
    let options = OnlineCompactionOptions {
        max_temporary_log_bytes: 128,
        ..Default::default()
    };
    let later = vec![7; 256];
    let result = store.compact_with_hooks(
        &previous,
        &options,
        |point| {
            if point == copy::Point::InitialCopy {
                store.with_store(|s| s.put(b"later", &later))?;
            }
            Ok(())
        },
        crate::directory::sync_existing,
    );
    assert!(result.is_err());
    assert!(!previous.exists());
    assert_eq!(store.with_store(|s| s.get(b"later")).unwrap(), Some(later));
}
