use super::*;
use std::os::unix::fs::symlink;

fn orphan(mut stage: Stage) -> PathBuf {
    let path = stage.payload.clone();
    stage.created = false;
    stage.marker.take().unwrap().keep().unwrap();
    drop(stage);
    path
}

// A private, single-stage fixture can briefly retain a retired-store lock
// across another test's fork/exec. Retry only its no-progress skipped report;
// an unexpected result or a skip that persists past retry_lock's deadline
// still fails the caller's assertions.
fn retry_fixture_retired_cleanup(parent: &Path, id: &[u8; 16]) -> io::Result<StagingCleanup> {
    crate::test_support::retry_lock(|| {
        let report = cleanup_retired(parent, id, || Ok(()))?;
        if report.removed_payloads == 0 && report.removed_markers == 0 && report.skipped == 1 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(report)
    })
}

#[test]
fn busy_retired_fixture_is_preserved_until_its_lock_is_released() {
    let directory = tempfile::tempdir().unwrap();
    let id = [8_u8; 16];
    let stage = Stage::retired_directory(directory.path(), &id).unwrap();
    fs::create_dir(stage.path().join("retired")).unwrap();
    let lock_path = stage.path().join("retired/store.lock");
    fs::write(&lock_path, b"lock").unwrap();
    fs::write(stage.path().join("retired/records.log"), b"old").unwrap();
    let path = orphan(stage);
    let lock = File::open(lock_path).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    let skipped =
        crate::test_support::retry_lock(|| cleanup_retired(directory.path(), &id, || Ok(())))
            .unwrap();
    assert_eq!((skipped.removed_markers, skipped.skipped), (0, 1));
    assert!(path.exists());
    let release = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        drop(lock);
    });
    let report = retry_fixture_retired_cleanup(directory.path(), &id).unwrap();
    release.join().unwrap();
    assert_eq!((report.removed_markers, report.skipped), (1, 0));
    assert!(!path.exists());
}

#[test]
fn active_operations_block_cleanup_and_orphans_can_be_removed() {
    let directory = tempfile::tempdir().unwrap();
    let (file_stage, mut file) = Stage::file(directory.path()).unwrap();
    file.write_all(b"sensitive backup").unwrap();
    let dir_stage = Stage::directory(directory.path()).unwrap();
    fs::write(dir_stage.path().join("data"), b"sensitive store").unwrap();
    assert_eq!(
        cleanup_abandoned_staging(directory.path())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    let file_path = orphan(file_stage);
    assert_eq!(
        cleanup_abandoned_staging(directory.path())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(file_path.exists());
    let dir_path = orphan(dir_stage);
    drop(file);
    assert_eq!(
        cleanup_abandoned_staging(directory.path()).unwrap(),
        StagingCleanup {
            removed_payloads: 2,
            removed_markers: 2,
            skipped: 0,
        }
    );
    assert!(!file_path.exists());
    assert!(!dir_path.exists());
    assert_eq!(
        cleanup_abandoned_staging(directory.path()).unwrap(),
        StagingCleanup::default()
    );
}

#[test]
fn selective_retired_cleanup_matches_identity_and_ignores_other_stages() {
    use std::cell::Cell;
    let directory = tempfile::tempdir().unwrap();
    let id = [1_u8; 16];
    let other_id = [2_u8; 16];
    let first = Stage::retired_directory(directory.path(), &id).unwrap();
    fs::create_dir(first.path().join("retired")).unwrap();
    fs::write(first.path().join("retired/store.lock"), []).unwrap();
    fs::write(first.path().join("retired/records.log"), b"retired").unwrap();
    let first = orphan(first);
    let second = orphan(Stage::retired_directory(directory.path(), &other_id).unwrap());
    let generic = orphan(Stage::directory(directory.path()).unwrap());
    let checks = Cell::new(0);
    let report = cleanup_retired(directory.path(), &id, || {
        checks.set(checks.get() + 1);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        (
            report.removed_payloads,
            report.removed_markers,
            report.skipped
        ),
        (1, 1, 0)
    );
    assert_eq!(checks.get(), 1);
    assert!(!first.exists());
    assert!(second.exists());
    assert!(generic.exists());
    let generic_report = cleanup_abandoned_staging(directory.path()).unwrap();
    assert_eq!(
        (generic_report.removed_payloads, generic_report.skipped),
        (1, 1)
    );
    assert!(second.exists());
    assert_eq!(
        cleanup_retired(directory.path(), &other_id, || Ok(()))
            .unwrap()
            .removed_payloads,
        1
    );
}

#[test]
fn selective_retired_cleanup_preserves_corrupt_markers_busy_locks_and_unknown_files() {
    let directory = tempfile::tempdir().unwrap();
    let id = [3_u8; 16];
    let malformed = Stage::retired_directory(directory.path(), &id).unwrap();
    let marker = malformed.marker.as_ref().unwrap().path().to_path_buf();
    let malformed_path = orphan(malformed);
    let mut bytes = fs::read(&marker).unwrap();
    bytes[RETIRED_MARKER.len()] ^= 1;
    fs::write(&marker, bytes).unwrap();
    let unknown = Stage::retired_directory(directory.path(), &id).unwrap();
    fs::write(unknown.path().join("unexpected"), b"keep").unwrap();
    let unknown_path = orphan(unknown);
    let busy = Stage::retired_directory(directory.path(), &id).unwrap();
    fs::create_dir(busy.path().join("retired")).unwrap();
    fs::write(busy.path().join("retired/store.lock"), []).unwrap();
    let busy_path = orphan(busy);
    let lock = File::open(busy_path.join("retired/store.lock")).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    let report = cleanup_retired(directory.path(), &id, || {
        panic!("unsafe stages must not be verified or removed")
    })
    .unwrap();
    assert_eq!(
        (
            report.removed_payloads,
            report.removed_markers,
            report.skipped
        ),
        (0, 0, 3)
    );
    assert!(malformed_path.exists());
    assert!(unknown_path.exists());
    assert!(busy_path.exists());
    drop(lock);
    let report = cleanup_retired(directory.path(), &id, || Ok(())).unwrap();
    assert_eq!(
        (
            report.removed_payloads,
            report.removed_markers,
            report.skipped
        ),
        (1, 1, 2)
    );
    assert!(!busy_path.exists());
}

#[test]
fn selective_retired_cleanup_never_deletes_if_verification_fails() {
    let directory = tempfile::tempdir().unwrap();
    let id = [4_u8; 16];
    let stage = orphan(Stage::retired_directory(directory.path(), &id).unwrap());
    let error = cleanup_retired(directory.path(), &id, || {
        Err(io::Error::other("injected verification failure"))
    })
    .unwrap_err();
    assert!(error.to_string().contains("injected verification failure"));
    assert!(stage.exists());
    assert_eq!(
        cleanup_retired(directory.path(), &id, || Ok(()))
            .unwrap()
            .removed_payloads,
        1
    );
}

#[test]
fn selective_retired_cleanup_respects_parent_lock_and_reports_sync_failure() {
    let directory = tempfile::tempdir().unwrap();
    let id = [5_u8; 16];
    let active = Stage::retired_directory(directory.path(), &id).unwrap();
    assert_eq!(
        cleanup_retired(directory.path(), &id, || Ok(()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    let path = orphan(active);
    let error = cleanup_selected_with_sync(
        directory.path(),
        Some(&id),
        |_| Err(io::Error::other("injected directory sync failure")),
        || Ok(()),
    )
    .unwrap_err();
    assert!(error.to_string().contains("may be incomplete"));
    assert!(!path.exists());
    assert_eq!(
        cleanup_retired(directory.path(), &id, || Ok(())).unwrap(),
        StagingCleanup::default()
    );
}

#[test]
fn selective_cleanup_avoids_sync_when_nothing_is_removed_and_classifies_only_its_lock_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let id = [6_u8; 16];
    let result = cleanup_selected_with_sync(
        directory.path(),
        Some(&id),
        |_| panic!("nothing changed"),
        || panic!("nothing to verify"),
    )
    .unwrap();
    assert_eq!(result, StagingCleanup::default());
    assert!(!is_retired_parent_busy(&io::Error::new(
        io::ErrorKind::WouldBlock,
        "unrelated I/O"
    )));
}

#[test]
fn retired_stage_removal_resumes_after_every_injected_interruption() {
    for selected in [
        RetiredRemovalPoint::Records,
        RetiredRemovalPoint::Witness,
        RetiredRemovalPoint::Lock,
        RetiredRemovalPoint::RetiredSync,
        RetiredRemovalPoint::RetiredDirectory,
        RetiredRemovalPoint::StageSync,
        RetiredRemovalPoint::StageDirectory,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let id = [9_u8; 16];
        let stage = Stage::retired_directory(directory.path(), &id).unwrap();
        fs::create_dir(stage.path().join("retired")).unwrap();
        for name in ["records.log", "commit.state", "store.lock"] {
            fs::write(stage.path().join("retired").join(name), b"old").unwrap();
        }
        let path = orphan(stage);
        let lock = lock_safe_retired_payload(&path).unwrap().unwrap();
        assert!(
            remove_retired_stage(&path, |point| {
                if point == selected {
                    Err(io::Error::other("interrupted"))
                } else {
                    Ok(())
                }
            })
            .is_err(),
            "{selected:?}"
        );
        drop(lock);
        let report = retry_fixture_retired_cleanup(directory.path(), &id).unwrap();
        assert_eq!(
            (report.removed_markers, report.skipped),
            (1, 0),
            "{selected:?}"
        );
        assert!(!path.exists());
    }
}

#[test]
fn retired_cleanup_kill_child() {
    let Ok(selected) = std::env::var("EMDA_RETIRED_CLEANUP_POINT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("EMDA_RETIRED_CLEANUP_ROOT").unwrap());
    let stage = fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("retired").is_dir())
        .unwrap();
    let _parent = ParentLock::exclusive(&root).unwrap();
    let _lock = lock_safe_retired_payload(&stage).unwrap().unwrap();
    let _ = remove_retired_stage(&stage, |point| {
        if format!("{point:?}") == selected {
            let mut signal = File::create(root.join("signal")).unwrap();
            signal.write_all(b"ready").unwrap();
            signal.sync_all().unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok(())
    });
    panic!("cleanup kill hook did not run");
}

#[test]
fn retired_stage_removal_resumes_after_process_kill_at_every_boundary() {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    for point in [
        "Records",
        "Witness",
        "Lock",
        "RetiredSync",
        "RetiredDirectory",
        "StageSync",
        "StageDirectory",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let id = [9_u8; 16];
        let stage = Stage::retired_directory(directory.path(), &id).unwrap();
        fs::create_dir(stage.path().join("retired")).unwrap();
        for name in ["records.log", "commit.state", "store.lock"] {
            fs::write(stage.path().join("retired").join(name), b"old").unwrap();
        }
        let path = orphan(stage);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("staging::tests::retired_cleanup_kill_child")
            .env("EMDA_RETIRED_CLEANUP_POINT", point)
            .env("EMDA_RETIRED_CLEANUP_ROOT", directory.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !directory.path().join("signal").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child timed out at {point}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let report = retry_fixture_retired_cleanup(directory.path(), &id).unwrap();
        assert_eq!((report.removed_markers, report.skipped), (1, 0), "{point}");
        assert!(!path.exists());
    }
}

#[test]
fn cleanup_preserves_unknown_legacy_and_symlink_entries() {
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("sentinel"), b"preserve").unwrap();
    let legacy = ["backup", "restore", "upgrade", "index", "compact"]
        .map(|kind| directory.path().join(format!(".db-{kind}-old")));
    for path in &legacy {
        fs::write(path, b"old data").unwrap();
    }
    let legacy_link = directory.path().join(".db-upgrade-link");
    symlink(outside.path(), &legacy_link).unwrap();
    let legacy_dir = directory.path().join(".db-restore-directory");
    fs::create_dir(&legacy_dir).unwrap();
    fs::write(legacy_dir.join("records.log"), b"preserve this copy").unwrap();
    let bogus = directory
        .path()
        .join(".emda-stage-v1-f-1234567890123456.owner");
    fs::write(&bogus, b"unknown").unwrap();
    fs::write(payload_path(&bogus), b"unowned").unwrap();
    let link_marker = directory
        .path()
        .join(".emda-stage-v1-d-1234567890123456.owner");
    fs::write(&link_marker, DIR_MARKER).unwrap();
    symlink(outside.path(), payload_path(&link_marker)).unwrap();
    let marker_link = directory
        .path()
        .join(".emda-stage-v1-f-abcdefghijklmnop.owner");
    symlink(&bogus, &marker_link).unwrap();
    let report = cleanup_abandoned_staging(directory.path()).unwrap();
    assert_eq!(report.removed_payloads, 0);
    assert_eq!(report.removed_markers, 0);
    assert_eq!(report.skipped, 3);
    for path in &legacy {
        assert_eq!(fs::read(path).unwrap(), b"old data");
    }
    assert!(
        fs::symlink_metadata(&legacy_link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read(legacy_dir.join("records.log")).unwrap(),
        b"preserve this copy"
    );
    assert_eq!(fs::read(payload_path(&bogus)).unwrap(), b"unowned");
    assert_eq!(
        fs::read(outside.path().join("sentinel")).unwrap(),
        b"preserve"
    );
    assert!(link_marker.exists());
}

#[test]
fn cleanup_removes_orphan_marker_and_reports_sync_failure() {
    let directory = tempfile::tempdir().unwrap();
    let (stage, file) = Stage::file(directory.path()).unwrap();
    let payload = orphan(stage);
    drop(file);
    fs::remove_file(payload).unwrap();
    let error = cleanup_with_sync(directory.path(), |_| {
        Err(io::Error::other(
            "injected directory synchronization failure",
        ))
    })
    .unwrap_err();
    assert!(error.to_string().contains("may be incomplete"));
    assert_eq!(
        cleanup_abandoned_staging(directory.path()).unwrap(),
        StagingCleanup::default()
    );
}

#[test]
fn cleanup_never_follows_links_inside_a_staging_directory() {
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("sentinel"), b"preserve").unwrap();
    let stage = Stage::directory(directory.path()).unwrap();
    symlink(outside.path(), stage.path().join("link")).unwrap();
    orphan(stage);
    let report = cleanup_abandoned_staging(directory.path()).unwrap();
    assert_eq!(report.removed_payloads, 1);
    assert_eq!(
        fs::read(outside.path().join("sentinel")).unwrap(),
        b"preserve"
    );
}

#[test]
fn cleanup_only_removes_the_staging_name_of_a_published_hard_link() {
    let directory = tempfile::tempdir().unwrap();
    let (stage, mut file) = Stage::file(directory.path()).unwrap();
    file.write_all(b"published data").unwrap();
    let published = directory.path().join("published");
    fs::hard_link(stage.path(), &published).unwrap();
    let payload = orphan(stage);
    drop(file);
    let report = cleanup_abandoned_staging(directory.path()).unwrap();
    assert_eq!((report.removed_payloads, report.removed_markers), (1, 1));
    assert!(!payload.exists());
    assert_eq!(fs::read(published).unwrap(), b"published data");
}

#[test]
fn ownership_sync_failures_stop_before_any_payload_is_created() {
    use std::cell::RefCell;
    for fail_at in 0..2 {
        let directory = tempfile::tempdir().unwrap();
        let calls = RefCell::new(Vec::new());
        let result = Stage::new_with_sync(
            directory.path(),
            Kind::File,
            |file| {
                calls.borrow_mut().push("marker");
                if fail_at == 0 {
                    Err(io::Error::other("injected marker sync failure"))
                } else {
                    file.sync_all()
                }
            },
            |_| {
                calls.borrow_mut().push("parent");
                Err(io::Error::other("injected parent sync failure"))
            },
        );
        assert!(result.is_err());
        assert_eq!(
            *calls.borrow(),
            if fail_at == 0 {
                vec!["marker"]
            } else {
                vec!["marker", "parent"]
            }
        );
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        assert_eq!(
            cleanup_abandoned_staging(directory.path()).unwrap(),
            StagingCleanup::default()
        );
    }
}

#[test]
fn parent_lock_release_is_not_delayed_by_a_duplicate_descriptor() {
    let directory = tempfile::tempdir().unwrap();
    let guard = ParentLock::shared(directory.path()).unwrap();
    let duplicate = guard.0.try_clone().unwrap();
    assert_eq!(
        ParentLock::exclusive(directory.path())
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(guard);
    let exclusive = ParentLock::exclusive(directory.path()).unwrap();
    drop(duplicate);
    drop(exclusive);
}
