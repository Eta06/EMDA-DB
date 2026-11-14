use super::*;
use std::sync::mpsc;
use std::thread;

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
fn detailed_online_handoff_classifies_publication_and_exchange_boundaries() {
    use crate::OnlineHandoffFailure;
    for failure in [
        "preflight",
        "copy",
        "barrier",
        "candidate",
        "exchange",
        "sync",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = OnlineStore::open(&source).unwrap();
        insert_history(&store);
        let original = fs::read(source.join("records.log")).unwrap();
        let destination = if failure == "preflight" {
            root.path().join("missing/previous")
        } else {
            previous.clone()
        };
        let result = store.compact_with_hooks_detailed(
            &destination,
            &Default::default(),
            |point| {
                let reject = match failure {
                    "copy" => point == copy::Point::InitialCopy,
                    "barrier" => point == copy::Point::BeforeFinalBarrier,
                    "candidate" => point == copy::Point::BeforeExchange,
                    "exchange" => point == copy::Point::AfterExchange,
                    _ => false,
                };
                if reject {
                    Err(io::Error::other("injected handoff failure"))
                } else {
                    Ok(())
                }
            },
            |parent| {
                if failure == "sync" {
                    Err(io::Error::other("injected parent sync failure"))
                } else {
                    crate::directory::sync_existing(parent)
                }
            },
        );
        match failure {
            "preflight" | "copy" | "barrier" => {
                assert!(matches!(result, Err(OnlineHandoffFailure::NotPublished(_))))
            }
            "candidate" => assert!(matches!(
                result,
                Err(OnlineHandoffFailure::CandidateOutcomeUnknown(_))
            )),
            _ => assert!(matches!(
                result,
                Err(OnlineHandoffFailure::ExchangeOutcomeUnknown(_))
            )),
        }
        assert_eq!(
            fs::read(source.join("records.log")).unwrap() == original,
            !matches!(failure, "exchange" | "sync")
        );
        if matches!(failure, "exchange" | "sync") {
            assert!(store.with_store(|db| db.get(b"overwritten")).is_err());
            assert!(
                store
                    .with_store(|db| db.put(b"after", b"rejected"))
                    .is_err()
            );
        } else {
            assert_eq!(
                store.with_store(|db| db.get(b"overwritten")).unwrap(),
                Some(b"39".to_vec())
            );
        }
        drop(store);
        let live = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        assert_eq!(live.get(b"after").unwrap(), None);
        if destination.exists() {
            let old = retry_lock(|| Store::open(&destination)).unwrap();
            assert_eq!(old.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
            assert_eq!(
                fs::read(destination.join("records.log")).unwrap() == original,
                matches!(failure, "exchange" | "sync")
            );
        } else {
            assert!(matches!(failure, "preflight" | "copy" | "barrier"));
        }
    }
}

#[test]
fn detailed_prepublication_failure_does_not_exclude_concurrent_writes() {
    use crate::OnlineHandoffFailure;
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let worker_store = Arc::clone(&store);
    let target = previous.clone();
    let worker = thread::spawn(move || {
        worker_store.compact_with_hooks_detailed(
            &target,
            &Default::default(),
            |point| {
                if point == copy::Point::BeforeFinalBarrier {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                    return Err(io::Error::other("injected prepublication failure"));
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    store
        .with_store(|db| db.put(b"independent", b"acknowledged"))
        .unwrap();
    resume_tx.send(()).unwrap();
    assert!(matches!(
        worker.join().unwrap(),
        Err(OnlineHandoffFailure::NotPublished(_))
    ));
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|db| db.get(b"independent")).unwrap(),
        Some(b"acknowledged".to_vec())
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"independent")
            .unwrap(),
        Some(b"acknowledged".to_vec())
    );
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

#[test]
fn uncertain_source_commit_during_copy_never_publishes_a_compacted_store() {
    use crate::store::metadata_tests::{Fault, store as fault_store};
    use std::sync::atomic::{AtomicBool, Ordering};
    for fault in [
        Fault::Read,
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncTimeout,
        Fault::SyncAfter,
        Fault::Panic,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let armed = Arc::new(AtomicBool::new(false));
        let (entered, _) = mpsc::channel();
        let (_, resume) = mpsc::channel();
        let source_store = fault_store(&source, fault, Arc::clone(&armed), entered, resume);
        source_store.put(b"safe", b"committed").unwrap();
        let online = OnlineStore {
            path: fs::canonicalize(&source).unwrap(),
            state: RwLock::new(State {
                store: Arc::new(source_store),
                failed: false,
            }),
            maintenance: Mutex::new(()),
            reclamation_worker: std::sync::atomic::AtomicBool::new(false),
        };
        let result = online.compact_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == copy::Point::InitialCopy {
                    armed.store(true, Ordering::Release);
                    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        online.with_store(|s| s.put(b"uncertain", b"value"))
                    }));
                    assert!(!matches!(failed, Ok(Ok(()))));
                    armed.store(false, Ordering::Release);
                }
                Ok(())
            },
            crate::directory::sync_existing,
        );
        assert!(result.is_err());
        assert!(!previous.exists());
        assert!(online.with_store(|s| s.get(b"safe")).is_err());
        drop(online);
        let recovered = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(recovered.get(b"safe").unwrap(), Some(b"committed".to_vec()));
        assert!(matches!(
            recovered.get(b"uncertain").unwrap().as_deref(),
            None | Some(b"value")
        ));
    }
}

#[test]
fn partial_source_append_at_every_offset_aborts_handoff_without_truncation() {
    use crate::store::fault_tests::{Fault, open_fault_store};
    let length = crate::record::encode(crate::record::Operation::Put, b"uncertain", b"value")
        .unwrap()
        .len();
    for offset in 0..length {
        let root = tempfile::tempdir().unwrap();
        let source = tempfile::Builder::new()
            .prefix("live-")
            .tempdir_in(root.path())
            .unwrap();
        let previous = root.path().join("previous");
        let fault = Arc::new(Mutex::new(None));
        let source_store = open_fault_store(&source, Arc::clone(&fault)).unwrap();
        source_store.put(b"safe", b"committed").unwrap();
        let online = OnlineStore {
            path: fs::canonicalize(source.path()).unwrap(),
            state: RwLock::new(State {
                store: Arc::new(source_store),
                failed: false,
            }),
            maintenance: Mutex::new(()),
            reclamation_worker: std::sync::atomic::AtomicBool::new(false),
        };
        let result = online.compact_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == copy::Point::InitialCopy {
                    *fault.lock().unwrap() = Some(Fault::PartialAppend(offset));
                    assert!(
                        online
                            .with_store(|s| s.put(b"uncertain", b"value"))
                            .is_err()
                    );
                }
                Ok(())
            },
            crate::directory::sync_existing,
        );
        assert!(result.is_err(), "offset {offset}");
        assert!(!previous.exists());
        assert!(online.with_store(|s| s.get(b"safe")).is_err());
        drop(online);
        let path = source.path().join("records.log");
        let bytes = fs::read(&path).unwrap();
        let reopened = retry_lock(|| Store::open(source.path()));
        if offset == 0 {
            assert_eq!(
                reopened.unwrap().get(b"safe").unwrap(),
                Some(b"committed".to_vec())
            );
        } else {
            assert!(
                matches!(reopened, Err(ref error) if error.kind() == io::ErrorKind::InvalidData),
                "offset {offset}"
            );
        }
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn partial_v2_and_v3_source_appends_abort_handoff_without_truncation() {
    use crate::store::fault_tests::{Fault, open_versioned_fault_store};
    let length = crate::record::encode(crate::record::Operation::Put, b"uncertain", b"value")
        .unwrap()
        .len();
    for version in ["v2", "v3"] {
        for offset in 0..length {
            let root = tempfile::tempdir().unwrap();
            let (source, previous) = paths(root.path());
            if version == "v2" {
                fs::create_dir(&source).unwrap();
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
            let fault = Arc::new(Mutex::new(None));
            let source_store = open_versioned_fault_store(&source, Arc::clone(&fault)).unwrap();
            source_store.put(b"safe", b"committed").unwrap();
            let online = OnlineStore {
                path: fs::canonicalize(&source).unwrap(),
                state: RwLock::new(State {
                    store: Arc::new(source_store),
                    failed: false,
                }),
                maintenance: Mutex::new(()),
                reclamation_worker: std::sync::atomic::AtomicBool::new(false),
            };
            let result = online.compact_with_hooks(
                &previous,
                &Default::default(),
                |point| {
                    if point == copy::Point::InitialCopy {
                        *fault.lock().unwrap() = Some(Fault::PartialAppend(offset));
                        assert!(
                            online
                                .with_store(|s| s.put(b"uncertain", b"value"))
                                .is_err()
                        );
                    }
                    Ok(())
                },
                crate::directory::sync_existing,
            );
            assert!(result.is_err(), "{version} offset {offset}");
            assert!(!previous.exists());
            assert!(online.with_store(|s| s.get(b"safe")).is_err());
            drop(online);
            let path = source.join("records.log");
            let bytes = fs::read(&path).unwrap();
            let reopened = retry_lock(|| Store::open(&source));
            if offset == 0 {
                assert_eq!(
                    reopened.unwrap().get(b"safe").unwrap(),
                    Some(b"committed".to_vec())
                );
            } else {
                assert!(
                    matches!(reopened, Err(ref error) if error.kind() == io::ErrorKind::InvalidData),
                    "{version} offset {offset}"
                );
            }
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
    }
}
