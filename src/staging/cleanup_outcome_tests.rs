use super::*;
use std::cell::Cell;
use std::os::unix::fs::symlink;

fn orphan_file(parent: &Path) -> (PathBuf, PathBuf) {
    let (stage, mut file) = Stage::file(parent).unwrap();
    file.write_all(b"abandoned").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
    let payload = stage.payload.clone();
    stage.retain_payload().unwrap();
    (marker, payload)
}

#[test]
fn detailed_cleanup_reports_no_removal_on_lock_or_pre_removal_failure() {
    let root = tempfile::tempdir().unwrap();
    let (stage, file) = Stage::file(root.path()).unwrap();
    let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
    let payload = stage.payload.clone();
    assert!(matches!(
        cleanup_abandoned_staging_detailed(root.path()),
        Err(StagingCleanupFailure::NotStarted(error)) if error.kind() == io::ErrorKind::WouldBlock
    ));
    assert!(marker.exists() && payload.exists());
    drop(file);
    stage.retain_payload().unwrap();
    let error = cleanup_detailed_with_hooks(root.path(), File::sync_all, File::sync_all, || {
        Err(io::Error::other("injected before removal"))
    });
    assert!(matches!(error, Err(StagingCleanupFailure::NotStarted(_))));
    assert!(marker.exists() && payload.exists());
    let report = cleanup_abandoned_staging_detailed(root.path()).unwrap();
    assert_eq!((report.removed_payloads, report.removed_markers), (1, 1));
    assert!(!marker.exists() && !payload.exists());
}

#[test]
fn failed_payload_sync_distinguishes_prior_removal_and_keeps_the_marker() {
    for payload_present in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (marker, payload) = orphan_file(root.path());
        if !payload_present {
            fs::remove_file(&payload).unwrap();
        }
        let error = cleanup_detailed_with_hooks(
            root.path(),
            |_| Err(io::Error::other("injected payload sync failure")),
            File::sync_all,
            || Ok(()),
        );
        if payload_present {
            assert!(matches!(
                error,
                Err(StagingCleanupFailure::OutcomeUnknown(_))
            ));
        } else {
            assert!(matches!(error, Err(StagingCleanupFailure::NotStarted(_))));
        }
        assert!(marker.exists());
        assert!(!payload.exists());
        let report = cleanup_abandoned_staging_detailed(root.path()).unwrap();
        assert_eq!((report.removed_payloads, report.removed_markers), (0, 1));
        assert!(!marker.exists());
    }
}

#[test]
fn failed_final_sync_reports_unknown_after_completed_removals() {
    let root = tempfile::tempdir().unwrap();
    let (marker, payload) = orphan_file(root.path());
    let unrelated = root.path().join("unrelated");
    fs::write(&unrelated, b"keep").unwrap();
    let error = cleanup_detailed_with_hooks(
        root.path(),
        File::sync_all,
        |_| Err(io::Error::other("injected final sync failure")),
        || Ok(()),
    );
    assert!(matches!(
        error,
        Err(StagingCleanupFailure::OutcomeUnknown(_))
    ));
    assert!(!marker.exists() && !payload.exists());
    assert_eq!(fs::read(&unrelated).unwrap(), b"keep");
    assert_eq!(
        cleanup_abandoned_staging_detailed(root.path()).unwrap(),
        StagingCleanup::default()
    );
}

#[test]
fn failure_after_one_stage_reports_unknown_and_remaining_stage_is_retryable() {
    let root = tempfile::tempdir().unwrap();
    let first = orphan_file(root.path());
    let second = orphan_file(root.path());
    let calls = Cell::new(0);
    let error = cleanup_detailed_with_hooks(root.path(), File::sync_all, File::sync_all, || {
        let count = calls.get();
        calls.set(count + 1);
        if count == 2 {
            Err(io::Error::other("injected after one stage"))
        } else {
            Ok(())
        }
    });
    assert!(matches!(
        error,
        Err(StagingCleanupFailure::OutcomeUnknown(_))
    ));
    assert_eq!(
        usize::from(first.0.exists()) + usize::from(second.0.exists()),
        1
    );
    assert_eq!(
        usize::from(first.1.exists()) + usize::from(second.1.exists()),
        1
    );
    let report = cleanup_abandoned_staging_detailed(root.path()).unwrap();
    assert_eq!((report.removed_payloads, report.removed_markers), (1, 1));
}

#[test]
fn retired_marker_sync_failure_before_removal_is_not_started() {
    let root = tempfile::tempdir().unwrap();
    let id = [8; 16];
    let stage = Stage::retired_directory(root.path(), &id).unwrap();
    let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
    fs::remove_dir(stage.path()).unwrap();
    stage.retain_payload().unwrap();
    let failure = cleanup_retired_detailed(
        root.path(),
        &id,
        || panic!("no removal should be attempted"),
        |_| Err(io::Error::other("injected marker-only sync failure")),
        File::sync_all,
    );
    assert!(
        matches!(failure, Err(StagingCleanupFailure::NotStarted(error)) if error.to_string().contains("injected"))
    );
    assert!(marker.exists());
    let report =
        cleanup_retired_detailed(root.path(), &id, || Ok(()), File::sync_all, File::sync_all)
            .unwrap();
    assert_eq!((report.removed_payloads, report.removed_markers), (0, 1));
}

#[test]
fn detailed_cleanup_keeps_retired_and_symlink_payloads_untouched() {
    let root = tempfile::tempdir().unwrap();
    let (marker, payload) = orphan_file(root.path());
    let retired = Stage::retired_directory(root.path(), &[7; 16]).unwrap();
    let retired_marker = retired.marker.as_ref().unwrap().path().to_path_buf();
    let retired_payload = retired.payload.clone();
    retired.retain_payload().unwrap();
    let target = root.path().join("target");
    fs::write(&target, b"preserve").unwrap();
    let symlink_marker = root.path().join(".emda-stage-v1-f-abcdefghijklmnop.owner");
    fs::write(&symlink_marker, FILE_MARKER).unwrap();
    let symlink_payload = payload_path(&symlink_marker);
    symlink(&target, &symlink_payload).unwrap();
    let report = cleanup_abandoned_staging_detailed(root.path()).unwrap();
    assert_eq!((report.removed_payloads, report.removed_markers), (1, 1));
    assert!(report.skipped >= 2);
    assert!(!marker.exists() && !payload.exists());
    assert!(retired_marker.exists() && retired_payload.exists());
    assert!(
        symlink_marker.exists()
            && fs::symlink_metadata(&symlink_payload)
                .unwrap()
                .file_type()
                .is_symlink()
    );
    assert_eq!(fs::read(target).unwrap(), b"preserve");
}
