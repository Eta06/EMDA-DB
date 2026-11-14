use super::*;
use crate::OnlineHandoffFailure;
use std::sync::atomic::{AtomicBool, Ordering};

fn prepared(force_v2: bool) -> (tempfile::TempDir, PathBuf, PathBuf, OnlineStore) {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    if force_v2 {
        fs::create_dir(&source).unwrap();
        fs::write(
            source.join("records.log"),
            include_bytes!("../../../../../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            source.join("commit.state"),
            include_bytes!("../../../../../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    (root, source, previous, store)
}

#[test]
fn detailed_reclaim_classifies_handoff_and_never_reclaims_on_failure() {
    for force_v2 in [false, true] {
        for failure in [
            "preflight",
            "copy",
            "barrier",
            "candidate",
            "exchange",
            "sync",
        ] {
            let (root, source, previous, store) = prepared(force_v2);
            let original = fs::read(source.join("records.log")).unwrap();
            let destination = if failure == "preflight" {
                root.path().join("missing/previous")
            } else {
                previous.clone()
            };
            let called = AtomicBool::new(false);
            let result = store.compact_reclaim_with_hooks_detailed(
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
                        Err(io::Error::other("injected exchange sync failure"))
                    } else {
                        crate::directory::sync_existing(parent)
                    }
                },
                |_| {
                    called.store(true, Ordering::Relaxed);
                    Ok(())
                },
            );
            match failure {
                "preflight" | "copy" | "barrier" => assert!(matches!(
                    result,
                    Err(OnlineReclaimFailure::Compaction(
                        OnlineHandoffFailure::NotPublished(_)
                    ))
                )),
                "candidate" => assert!(matches!(
                    result,
                    Err(OnlineReclaimFailure::Compaction(
                        OnlineHandoffFailure::CandidateOutcomeUnknown(_)
                    ))
                )),
                _ => assert!(matches!(
                    result,
                    Err(OnlineReclaimFailure::Compaction(
                        OnlineHandoffFailure::ExchangeOutcomeUnknown(_)
                    ))
                )),
            }
            assert!(!called.load(Ordering::Relaxed));
            let exchanged = matches!(failure, "exchange" | "sync");
            if exchanged {
                assert!(store.with_store(|db| db.get(b"overwritten")).is_err());
                assert!(
                    store
                        .with_store(|db| db.put(b"unexpected", b"value"))
                        .is_err()
                );
            } else {
                assert_eq!(
                    store.with_store(|db| db.get(b"overwritten")).unwrap(),
                    Some(b"39".to_vec())
                );
            }
            if exchanged {
                assert_eq!(fs::read(previous.join("records.log")).unwrap(), original);
            } else {
                assert_eq!(fs::read(source.join("records.log")).unwrap(), original);
            }
            drop(store);
            for path in [&source, &previous] {
                if path.exists() {
                    let recovered = retry_lock(|| Store::open(path)).unwrap();
                    assert_eq!(recovered.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
                    assert_eq!(recovered.get(b"unexpected").unwrap(), None);
                }
            }
        }
    }
}

#[test]
fn detailed_reclaim_reports_successful_handoff_before_cleanup_failures() {
    for force_v2 in [false, true] {
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
            let (_root, source, previous, store) = prepared(force_v2);
            let result = store.compact_reclaim_with_hooks_detailed(
                &previous,
                &Default::default(),
                |_| Ok(()),
                crate::directory::sync_existing,
                |point| {
                    if point == fail_at {
                        Err(io::Error::other("injected reclamation failure"))
                    } else {
                        Ok(())
                    }
                },
            );
            let Err(OnlineReclaimFailure::Reclamation { report, error }) = result else {
                panic!("expected a classified reclamation failure");
            };
            assert!(report.compacted_log_bytes > 0);
            assert!(error.to_string().contains("injected reclamation failure"));
            assert_eq!(
                store.with_store(|db| db.get(b"overwritten")).unwrap(),
                Some(b"39".to_vec())
            );
            store.with_store(|db| db.put(b"after", b"safe")).unwrap();
            retry_fixture_cleanup(&store).unwrap();
            drop(store);
            let reopened = retry_lock(|| Store::open(&source)).unwrap();
            assert_eq!(reopened.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
            assert_eq!(reopened.get(b"after").unwrap(), Some(b"safe".to_vec()));
        }
    }
}

#[test]
fn detailed_reclaim_public_api_rejects_invalid_options_and_succeeds() {
    for force_v2 in [false, true] {
        let (_root, source, previous, store) = prepared(force_v2);
        let invalid = OnlineCompactionOptions {
            catchup_rounds: 17,
            ..Default::default()
        };
        assert!(matches!(
            store.compact_and_reclaim_detailed(&previous, &invalid),
            Err(OnlineReclaimFailure::Compaction(
                OnlineHandoffFailure::NotPublished(_)
            ))
        ));
        assert!(!previous.exists());
        let report = store
            .compact_and_reclaim_detailed(&previous, &Default::default())
            .unwrap();
        assert!(report.compacted_log_bytes > 0);
        assert!(!previous.exists());
        drop(store);
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
fn rejected_detailed_reclaim_preserves_a_concurrent_acknowledged_write() {
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
    let worker_previous = previous.clone();
    let worker = thread::spawn(move || {
        worker_store.compact_reclaim_with_hooks_detailed(
            &worker_previous,
            &options,
            |point| {
                if point == copy::Point::BeforeFinalBarrier {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
            |_| panic!("reclamation must not start after a rejected handoff"),
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    store
        .with_store(|db| db.put(b"late", b"acknowledged"))
        .unwrap();
    resume_tx.send(()).unwrap();
    assert!(matches!(
        worker.join().unwrap(),
        Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::NotPublished(_)
        ))
    ));
    assert!(!previous.exists());
    assert_eq!(
        store.with_store(|db| db.get(b"late")).unwrap(),
        Some(b"acknowledged".to_vec())
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"late")
            .unwrap(),
        Some(b"acknowledged".to_vec())
    );
}
