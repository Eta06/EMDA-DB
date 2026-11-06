//! Ownership markers must outlive unsynchronized payload removal.
use super::*;
use std::cell::Cell;
use std::io::BufRead;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn orphan(mut stage: Stage) -> (PathBuf, PathBuf) {
    let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
    let payload = stage.payload.clone();
    stage.created = false;
    stage.marker.take().unwrap().keep().unwrap();
    drop(stage);
    (marker, payload)
}

#[test]
fn drop_retains_marker_when_payload_absence_cannot_be_synchronized() {
    for kind in ["file", "directory", "published"] {
        let root = tempfile::tempdir().unwrap();
        let mut stage = match kind {
            "file" => {
                let (stage, file) = Stage::file(root.path()).unwrap();
                drop(file);
                stage
            }
            _ => Stage::directory(root.path()).unwrap(),
        };
        let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
        let payload = stage.payload.clone();
        let published = root.path().join("published");
        if kind == "published" {
            fs::write(stage.path().join("value"), b"keep").unwrap();
            stage.publish(&published).unwrap();
        }
        stage.cleanup_on_drop_with_sync(|_| {
            assert!(!payload.exists());
            assert!(marker.exists());
            Err(io::Error::other("injected parent sync failure"))
        });
        drop(stage);
        assert!(marker.exists(), "{kind}: ownership marker was lost");
        assert!(!payload.exists());
        let report = cleanup_abandoned_staging(root.path()).unwrap();
        assert_eq!((report.removed_payloads, report.removed_markers), (0, 1));
        if kind == "published" {
            assert_eq!(fs::read(published.join("value")).unwrap(), b"keep");
        }
    }
}

#[test]
fn synchronized_publication_avoids_an_unnecessary_second_parent_sync() {
    for kind in ["file", "directory"] {
        let root = tempfile::tempdir().unwrap();
        let mut stage = if kind == "file" {
            let (stage, file) = Stage::file(root.path()).unwrap();
            drop(file);
            stage
        } else {
            Stage::directory(root.path()).unwrap()
        };
        let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
        let destination = root.path().join("published");
        stage.publish(&destination).unwrap();
        File::open(root.path()).unwrap().sync_all().unwrap();
        stage.publication_synchronized();
        stage.cleanup_on_drop_with_sync(|_| panic!("publication was already synchronized"));
        assert!(!marker.exists());
        assert!(destination.exists());
    }
}

#[test]
fn synchronized_file_replacement_avoids_a_duplicate_parent_sync() {
    let root = tempfile::tempdir().unwrap();
    let (mut stage, mut file) = Stage::file(root.path()).unwrap();
    file.write_all(b"new").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
    let destination = root.path().join("selection");
    fs::write(&destination, b"old").unwrap();
    stage.replace_file(&destination).unwrap();
    assert_eq!(fs::read(&destination).unwrap(), b"new");
    File::open(root.path()).unwrap().sync_all().unwrap();
    stage.publication_synchronized();
    stage.cleanup_on_drop_with_sync(|_| panic!("replacement was already synchronized"));
    assert!(!marker.exists());
    assert_eq!(fs::read(destination).unwrap(), b"new");
}

#[test]
fn failed_replacement_sync_retains_the_marker_for_retry() {
    let root = tempfile::tempdir().unwrap();
    let (mut stage, mut file) = Stage::file(root.path()).unwrap();
    file.write_all(b"new").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
    let destination = root.path().join("selection");
    fs::write(&destination, b"old").unwrap();
    stage.replace_file(&destination).unwrap();
    stage.cleanup_on_drop_with_sync(|_| Err(io::Error::other("injected parent sync failure")));
    drop(stage);
    assert!(marker.exists());
    assert_eq!(fs::read(&destination).unwrap(), b"new");
    let report = cleanup_abandoned_staging(root.path()).unwrap();
    assert_eq!((report.removed_payloads, report.removed_markers), (0, 1));
    assert_eq!(fs::read(destination).unwrap(), b"new");
}

#[test]
fn file_replacement_rejects_a_different_parent_or_non_file_stage() {
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let (mut file_stage, file) = Stage::file(root.path()).unwrap();
    drop(file);
    let original = file_stage.path().to_path_buf();
    assert_eq!(
        file_stage
            .replace_file(&other.path().join("selection"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        file_stage.replace_file(&original).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(original.exists());
    let mut directory_stage = Stage::directory(root.path()).unwrap();
    assert_eq!(
        directory_stage
            .replace_file(&root.path().join("selection"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(directory_stage.path().is_dir());
}

#[test]
fn lingering_published_hard_link_is_synced_after_its_later_removal() {
    let root = tempfile::tempdir().unwrap();
    let (mut stage, mut file) = Stage::file(root.path()).unwrap();
    file.write_all(b"published").unwrap();
    drop(file);
    let marker = stage.marker.as_ref().unwrap().path().to_path_buf();
    let payload = stage.payload.clone();
    let destination = root.path().join("published");
    // Model the tempfile hard-link fallback retaining its old name.
    fs::hard_link(&payload, &destination).unwrap();
    stage.published = true;
    File::open(root.path()).unwrap().sync_all().unwrap();
    stage.publication_synchronized();
    let called = Cell::new(false);
    stage.cleanup_on_drop_with_sync(|directory| {
        assert!(!payload.exists());
        assert!(marker.exists());
        called.set(true);
        directory.sync_all()
    });
    assert!(called.get());
    assert!(!marker.exists());
    assert_eq!(fs::read(destination).unwrap(), b"published");
}

#[test]
fn cleanup_syncs_absent_payload_before_removing_marker_and_then_syncs_again() {
    for missing in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (stage, file) = Stage::file(root.path()).unwrap();
        drop(file);
        let (marker, payload) = orphan(stage);
        if missing {
            fs::remove_file(&payload).unwrap();
        }
        let phase = Cell::new(0);
        let report = cleanup_selected_with_syncs(
            root.path(),
            None,
            |directory| {
                assert_eq!(phase.get(), 0);
                assert!(!payload.exists());
                assert!(marker.exists());
                phase.set(1);
                directory.sync_all()
            },
            |directory| {
                assert_eq!(phase.get(), 1);
                assert!(!payload.exists());
                assert!(!marker.exists());
                phase.set(2);
                directory.sync_all()
            },
            || Ok(()),
        )
        .unwrap();
        assert_eq!(phase.get(), 2);
        assert_eq!(report.removed_payloads, usize::from(!missing));
        assert_eq!(report.removed_markers, 1);
    }
}

#[test]
fn failed_payload_sync_keeps_marker_for_generic_and_retired_cleanup() {
    for retired in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let id = [7_u8; 16];
        let stage = if retired {
            Stage::retired_directory(root.path(), &id).unwrap()
        } else {
            Stage::directory(root.path()).unwrap()
        };
        let (marker, payload) = orphan(stage);
        let error = cleanup_selected_with_syncs(
            root.path(),
            retired.then_some(&id),
            |_| {
                assert!(!payload.exists());
                assert!(marker.exists());
                Err(io::Error::other("injected payload sync failure"))
            },
            File::sync_all,
            || Ok(()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("marker retained"));
        assert!(marker.exists());
        assert!(!payload.exists());
        let report = if retired {
            cleanup_retired(root.path(), &id, || Ok(())).unwrap()
        } else {
            cleanup_abandoned_staging(root.path()).unwrap()
        };
        assert_eq!((report.removed_payloads, report.removed_markers), (0, 1));
    }
}

#[test]
fn generic_cleanup_does_not_sync_an_unchanged_directory() {
    let root = tempfile::tempdir().unwrap();
    let untouched = root.path().join("user-file");
    fs::write(&untouched, b"keep").unwrap();
    let report = cleanup_with_sync(root.path(), |_| panic!("directory was unchanged")).unwrap();
    assert_eq!(report, StagingCleanup::default());
    assert_eq!(fs::read(untouched).unwrap(), b"keep");
}

#[test]
fn interruption_child() {
    let Ok(root) = std::env::var("EMDA_STAGING_ORDER_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let mode = std::env::var("EMDA_STAGING_ORDER_MODE").unwrap();
    let (mut stage, file) = Stage::file(root).unwrap();
    drop(file);
    let pause = |directory: &File| -> io::Result<()> {
        directory.sync_all()?;
        println!("STAGING_PAYLOAD_REMOVAL_SYNCED");
        std::io::stdout().flush()?;
        loop {
            std::thread::park();
        }
    };
    match mode.as_str() {
        "drop" => stage.cleanup_on_drop_with_sync(pause),
        "cleanup" => {
            orphan(stage);
            cleanup_selected_with_syncs(root, None, pause, File::sync_all, || Ok(())).unwrap();
        }
        _ => panic!("unknown interruption mode"),
    }
}

#[test]
fn process_kill_after_payload_sync_leaves_a_recoverable_marker() {
    for mode in ["drop", "cleanup"] {
        let root = tempfile::tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "staging::ordering_tests::interruption_child",
                "--nocapture",
            ])
            .env("EMDA_STAGING_ORDER_ROOT", root.path())
            .env("EMDA_STAGING_ORDER_MODE", mode)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(output)
                .lines()
                .map_while(Result::ok)
            {
                if line.contains("STAGING_PAYLOAD_REMOVAL_SYNCED") {
                    let _ = sender.send(());
                    break;
                }
            }
        });
        let reached = receiver.recv_timeout(Duration::from_secs(20));
        let busy = reached
            .is_ok()
            .then(|| cleanup_abandoned_staging(root.path()));
        let killed = child.kill();
        child.wait().unwrap();
        reached.expect("child did not reach synchronized payload removal");
        killed.expect("could not terminate child");
        assert!(matches!(busy, Some(Err(ref error)) if error.kind() == io::ErrorKind::WouldBlock));
        let report = cleanup_abandoned_staging(root.path()).unwrap();
        assert_eq!((report.removed_payloads, report.removed_markers), (0, 1));
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
