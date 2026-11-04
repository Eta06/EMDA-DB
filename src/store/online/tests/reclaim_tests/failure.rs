use super::*;

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
        let cleaned = store.cleanup_retired_staging(&Default::default()).unwrap();
        if fail_at != reclaim::Point::BeforeMove {
            assert!(cleaned.removed_markers >= 1);
            if !matches!(
                fail_at,
                reclaim::Point::AfterStageRemoval | reclaim::Point::AfterParentSync
            ) {
                assert!(cleaned.removed_payloads >= 1);
            }
        }
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
