use super::*;
use std::os::unix::fs::FileExt;

fn abandon(store: &OnlineStore, previous: &Path, point: reclaim::Point) {
    let result = store.compact_reclaim_with_hooks(
        previous,
        &Default::default(),
        |_| Ok(()),
        crate::directory::sync_existing,
        |at| {
            if at == point {
                Err(io::Error::other("injected cleanup interruption"))
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(
        result,
        Err(OnlineReclaimError::Reclamation { .. })
    ));
}

fn retired_path(root: &Path) -> PathBuf {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("retired").is_dir())
        .unwrap()
        .join("retired")
}

#[test]
fn scoped_cleanup_recovers_each_retired_stage_without_touching_live_data() {
    for point in [
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
        abandon(&store, &previous, point);
        store
            .with_store(|db| db.put(b"after", b"acknowledged"))
            .unwrap();
        let report = retry_fixture_cleanup(&store).unwrap();
        assert_eq!(report.removed_markers, 1, "{point:?}: {report:?}");
        assert_eq!(report.skipped, 0, "{point:?}");
        assert!(!previous.exists());
        assert_eq!(
            store.cleanup_retired_staging(&Default::default()).unwrap(),
            crate::StagingCleanup::default()
        );
        drop(store);
        let reopened = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(
            reopened.get(b"overwritten").unwrap().as_deref(),
            Some(&b"39"[..])
        );
        assert_eq!(
            reopened.get(b"after").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
    }
}

#[test]
fn scoped_cleanup_preserves_a_named_previous_store() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::BeforeMove);
    assert!(previous.exists());
    assert_eq!(
        store
            .cleanup_retired_staging(&Default::default())
            .unwrap()
            .removed_payloads,
        0
    );
    assert_eq!(
        retry_lock(|| Store::open(&previous))
            .unwrap()
            .get(b"overwritten")
            .unwrap()
            .as_deref(),
        Some(&b"39"[..])
    );
}

#[test]
fn scoped_cleanup_skips_a_locked_retired_store_until_it_is_released() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::AfterMove);
    let retired = retired_path(root.path());
    let held = retry_lock(|| Store::open(&retired)).unwrap();
    let report = store.cleanup_retired_staging(&Default::default()).unwrap();
    assert_eq!((report.removed_payloads, report.skipped), (0, 1));
    assert_eq!(
        held.get(b"overwritten").unwrap().as_deref(),
        Some(&b"39"[..])
    );
    drop(held);
    assert_eq!(retry_fixture_cleanup(&store).unwrap().removed_payloads, 1);
    assert!(!retired.exists());
}

#[test]
fn scoped_cleanup_does_not_remove_a_different_stores_retired_copy() {
    let root = tempfile::tempdir().unwrap();
    let first = OnlineStore::open(root.path().join("first")).unwrap();
    let second = OnlineStore::open(root.path().join("second")).unwrap();
    insert_history(&first);
    insert_history(&second);
    abandon(
        &first,
        &root.path().join("first-previous"),
        reclaim::Point::AfterMove,
    );
    let first_retired = retired_path(root.path());
    abandon(
        &second,
        &root.path().join("second-previous"),
        reclaim::Point::AfterMove,
    );
    let second_retired = fs::read_dir(root.path())
        .unwrap()
        .map(|entry| entry.unwrap().path().join("retired"))
        .find(|path| path.is_dir() && *path != first_retired)
        .unwrap();
    let report = retry_fixture_cleanup(&first).unwrap();
    assert_eq!(
        (
            report.removed_payloads,
            report.removed_markers,
            report.skipped
        ),
        (1, 1, 0)
    );
    assert!(!first_retired.exists());
    assert!(second_retired.exists());
    assert_eq!(
        retry_lock(|| Store::open(&second_retired))
            .unwrap()
            .get(b"overwritten")
            .unwrap(),
        Some(b"39".to_vec())
    );
    assert_eq!(retry_fixture_cleanup(&second).unwrap().removed_payloads, 1);
    assert!(!second_retired.exists());
}

#[test]
fn scoped_cleanup_preserves_retired_copy_if_current_log_is_corrupt() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::AfterMove);
    let retired = retired_path(root.path());
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(source.join("records.log"))
        .unwrap();
    let offset = file.metadata().unwrap().len() - 1;
    let mut byte = [0];
    file.read_exact_at(&mut byte, offset).unwrap();
    byte[0] ^= 1;
    file.write_all_at(&byte, offset).unwrap();
    file.sync_all().unwrap();
    assert_eq!(
        crate::cleanup_abandoned_staging(root.path())
            .unwrap()
            .skipped,
        1
    );
    assert!(retired.exists());
    assert_eq!(
        retry_fixture_cleanup(&store).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(retired.exists());
    let predecessor = retry_lock(|| Store::open(&retired)).unwrap();
    assert_eq!(
        predecessor.get(b"overwritten").unwrap().as_deref(),
        Some(&b"39"[..])
    );
    drop(predecessor);
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source)).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn scoped_cleanup_keeps_retired_copy_when_verification_limits_are_too_small() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::AfterMove);
    let retired = retired_path(root.path());
    let options = OnlineCompactionOptions {
        max_snapshot_keys: 0,
        ..Default::default()
    };
    let error = retry_fixture_cleanup_with_options(&store, &options).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(
        error
            .to_string()
            .contains("snapshot key count limit exceeded")
    );
    assert!(retired.exists());
    assert_eq!(retry_fixture_cleanup(&store).unwrap().removed_payloads, 1);
}

#[test]
fn scoped_cleanup_skips_a_lock_only_retired_stage_until_release() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = OnlineStore::open(&source).unwrap();
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::AfterWitness);
    let retired = retired_path(root.path());
    let held = retry_lock(|| {
        let file = fs::File::open(retired.join("store.lock"))?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        Ok(file)
    })
    .unwrap();
    let report = store.cleanup_retired_staging(&Default::default()).unwrap();
    assert_eq!((report.removed_payloads, report.skipped), (0, 1));
    assert!(retired.exists());
    drop(held);
    let report = retry_fixture_cleanup(&store).unwrap();
    assert_eq!((report.removed_payloads, report.removed_markers), (1, 1));
    assert!(!retired.exists());
}

#[test]
fn worker_cleans_matching_retired_stages_and_reports_counts() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::AfterMove);
    let retired = retired_path(root.path());
    let worker = store
        .start_reclamation_worker_with_retired_cleanup(
            &previous,
            Duration::from_secs(3600),
            u64::MAX,
            1,
            Default::default(),
        )
        .unwrap();
    let status = request_checks_until(&worker, |_| {
        worker.retired_cleanup_status().unwrap().removed_markers == 1
    });
    assert!(status.last_error.is_none());
    let cleanup = worker.retired_cleanup_status().unwrap();
    assert!(cleanup.checks >= 1);
    assert_eq!((cleanup.removed_payloads, cleanup.removed_markers), (1, 1));
    assert!(!retired.exists());
    worker.stop().unwrap();
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"overwritten")
            .unwrap()
            .as_deref(),
        Some(&b"39"[..])
    );
}

#[test]
fn worker_defers_cleanup_while_parent_or_maintenance_lock_is_busy() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::AfterMove);
    let retired = retired_path(root.path());
    let active = crate::staging::Stage::directory(root.path()).unwrap();
    let worker = store
        .start_reclamation_worker_with_retired_cleanup(
            &previous,
            Duration::from_secs(3600),
            u64::MAX,
            1,
            Default::default(),
        )
        .unwrap();
    worker.request_check().unwrap();
    wait_for(&worker, |s| s.checks >= 1);
    assert_eq!(worker.retired_cleanup_status().unwrap().deferred, 1);
    assert!(retired.exists());
    drop(active);
    let maintenance = store.maintenance.lock().unwrap();
    worker.request_check().unwrap();
    wait_for(&worker, |s| s.checks >= 2);
    assert_eq!(worker.retired_cleanup_status().unwrap().deferred, 2);
    drop(maintenance);
    request_checks_until(&worker, |_| {
        worker.retired_cleanup_status().unwrap().removed_payloads == 1
    });
    assert_eq!(worker.retired_cleanup_status().unwrap().removed_payloads, 1);
    assert!(!retired.exists());
    assert!(worker.stop().unwrap().last_error.is_none());
}

#[test]
fn worker_does_not_compact_while_matching_retired_data_is_locked() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::AfterMove);
    let retired = retired_path(root.path());
    let held = retry_lock(|| Store::open(&retired)).unwrap();
    let worker = store
        .start_reclamation_worker_with_retired_cleanup(
            &previous,
            Duration::from_secs(3600),
            1,
            1,
            Default::default(),
        )
        .unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.checks >= 1);
    assert_eq!(status.handoffs, 0);
    assert_eq!(worker.retired_cleanup_status().unwrap().skipped, 1);
    drop(held);
    let status = request_checks_until(&worker, |status| status.handoffs == 1);
    assert!(status.last_error.is_none());
    assert_eq!(status.handoffs, 1);
    assert_eq!(worker.retired_cleanup_status().unwrap().removed_payloads, 1);
    worker.stop().unwrap();
}

#[test]
fn worker_stops_and_preserves_retired_copy_when_current_verification_fails() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    abandon(&store, &previous, reclaim::Point::AfterMove);
    let retired = retired_path(root.path());
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(source.join("records.log"))
        .unwrap();
    let offset = file.metadata().unwrap().len() - 1;
    let mut byte = [0];
    file.read_exact_at(&mut byte, offset).unwrap();
    byte[0] ^= 1;
    file.write_all_at(&byte, offset).unwrap();
    file.sync_all().unwrap();
    let worker = store
        .start_reclamation_worker_with_retired_cleanup(
            &previous,
            Duration::from_secs(3600),
            u64::MAX,
            1,
            Default::default(),
        )
        .unwrap();
    let status = request_checks_until(&worker, |status| status.stopped);
    assert_eq!(
        status.last_error.unwrap().stage,
        OnlineReclamationFailureStage::Reclamation
    );
    assert!(retired.exists());
    worker.stop().unwrap();
    let predecessor = retry_lock(|| Store::open(&retired)).unwrap();
    assert_eq!(
        predecessor.get(b"overwritten").unwrap().as_deref(),
        Some(&b"39"[..])
    );
}
