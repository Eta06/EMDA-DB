use super::*;
use crate::Store;

#[test]
fn staging_lock_release_is_not_delayed_by_a_duplicate_descriptor() {
    let directory = tempfile::tempdir().unwrap();
    let mut lock = StagingLock::create(directory.path()).unwrap();
    let inherited = lock.file.try_clone().unwrap();
    let contender = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join("store.lock"))
        .unwrap();
    assert!(fs2::FileExt::try_lock_exclusive(&contender).is_err());
    lock.release().unwrap();
    fs2::FileExt::try_lock_exclusive(&contender).unwrap();
    drop(lock);
    drop(inherited);
    let second = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join("store.lock"))
        .unwrap();
    assert!(fs2::FileExt::try_lock_exclusive(&second).is_err());
}

fn pause_at_boundary() -> io::Result<()> {
    println!("BACKUP_CRASH_BOUNDARY_REACHED");
    std::io::stdout().flush()?;
    loop {
        std::thread::park();
    }
}

#[test]
fn publication_crash_child() {
    let Ok(parent) = std::env::var("EMDA_BACKUP_CRASH_PATH") else {
        return;
    };
    let parent = Path::new(&parent);
    let source = parent.join("source");
    let backup = parent.join("backup");
    let restored = parent.join("restored");
    let upgraded = parent.join("upgraded");
    let mode = std::env::var("EMDA_BACKUP_CRASH_STAGE").unwrap();
    let result = match mode.as_str() {
        "backup-staged" => {
            let (log, _) = Log::open(&source, false).unwrap();
            create_with_hooks(&log, &backup, |_| Ok(()), pause_at_boundary)
        }
        "backup-published" => {
            let (log, _) = Log::open(&source, false).unwrap();
            create_with_sync(&log, &backup, |_| pause_at_boundary())
        }
        "restore-staged" => restore_with_hooks(&backup, &restored, |_| Ok(()), pause_at_boundary),
        "restore-published" => restore_with_sync(&backup, &restored, |_| pause_at_boundary()),
        "upgrade-staged" => {
            let (log, _) = Log::open(&source, false).unwrap();
            upgrade_format_with_hooks(&log, &upgraded, |_| Ok(()), pause_at_boundary)
        }
        "upgrade-published" => {
            let (log, _) = Log::open(&source, false).unwrap();
            upgrade_format_with_hooks(&log, &upgraded, |_| pause_at_boundary(), || Ok(()))
        }
        _ => panic!("unknown test stage"),
    };
    result.unwrap();
}

fn kill_at_boundary(parent: &Path, mode: &str) {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "backup::publication_tests::publication_crash_child",
            "--nocapture",
        ])
        .env("EMDA_BACKUP_CRASH_PATH", parent)
        .env("EMDA_BACKUP_CRASH_STAGE", mode)
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
            if line.contains("BACKUP_CRASH_BOUNDARY_REACHED") {
                let _ = sender.send(());
                break;
            }
        }
    });
    let reached = receiver.recv_timeout(Duration::from_secs(20));
    let busy = crate::cleanup_abandoned_staging(parent);
    child.kill().unwrap();
    child.wait().unwrap();
    reached.expect("child did not reach the requested backup or restore boundary");
    assert_eq!(busy.unwrap_err().kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn process_kill_at_backup_publication_boundaries_preserves_source() {
    for mode in ["backup-staged", "backup-published"] {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source");
        let backup = directory.path().join("backup");
        let source = Store::open(&source_path).unwrap();
        source.put(b"a", b"one").unwrap();
        source.put(b"b", b"two").unwrap();
        let before = std::fs::read(source_path.join("records.log")).unwrap();
        drop(source);
        kill_at_boundary(directory.path(), mode);
        assert_eq!(
            std::fs::read(source_path.join("records.log")).unwrap(),
            before
        );
        let source = Store::open(&source_path).unwrap();
        if mode == "backup-staged" {
            assert!(!backup.exists());
            assert!(std::fs::read_dir(directory.path()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".emda-stage-v1-f-")
            }));
            let cleaned = crate::cleanup_abandoned_staging(directory.path()).unwrap();
            assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (1, 1));
            source.backup(&backup).unwrap();
        } else {
            assert!(backup.is_file());
            let cleaned = crate::cleanup_abandoned_staging(directory.path()).unwrap();
            assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (0, 1));
        }
        let restored = directory.path().join("restored");
        Store::restore(&backup, &restored).unwrap();
        let copy = Store::open(&restored).unwrap();
        let expected = vec![Some(b"one".to_vec()), Some(b"two".to_vec())];
        assert_eq!(source.get_many(&[b"a", b"b"]).unwrap(), expected);
        assert_eq!(copy.get_many(&[b"a", b"b"]).unwrap(), expected);
    }
}

#[test]
fn process_kill_at_restore_publication_boundaries_preserves_backup() {
    for mode in ["restore-staged", "restore-published"] {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source");
        let backup = directory.path().join("backup");
        let restored = directory.path().join("restored");
        let source = Store::open(&source_path).unwrap();
        source.put(b"a", b"one").unwrap();
        source.put(b"b", b"two").unwrap();
        source.backup(&backup).unwrap();
        let before_source = std::fs::read(source_path.join("records.log")).unwrap();
        let before_backup = std::fs::read(&backup).unwrap();
        drop(source);
        kill_at_boundary(directory.path(), mode);
        assert_eq!(
            std::fs::read(source_path.join("records.log")).unwrap(),
            before_source
        );
        assert_eq!(std::fs::read(&backup).unwrap(), before_backup);
        if mode == "restore-staged" {
            assert!(!restored.exists());
            assert!(std::fs::read_dir(directory.path()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".emda-stage-v1-d-")
            }));
            let cleaned = crate::cleanup_abandoned_staging(directory.path()).unwrap();
            assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (1, 1));
            Store::restore(&backup, &restored).unwrap();
        } else {
            let cleaned = crate::cleanup_abandoned_staging(directory.path()).unwrap();
            assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (0, 1));
        }
        let source = Store::open(&source_path).unwrap();
        let copy = Store::open(&restored).unwrap();
        let expected = vec![Some(b"one".to_vec()), Some(b"two".to_vec())];
        assert_eq!(source.get_many(&[b"a", b"b"]).unwrap(), expected);
        assert_eq!(copy.get_many(&[b"a", b"b"]).unwrap(), expected);
    }
}

#[test]
fn process_kill_at_upgrade_publication_boundaries_preserves_v2_and_v3_sources() {
    for mode in ["upgrade-staged", "upgrade-published"] {
        for v2 in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let source_path = directory.path().join("source");
            let upgraded = directory.path().join("upgraded");
            if v2 {
                std::fs::create_dir(&source_path).unwrap();
                std::fs::write(
                    source_path.join("records.log"),
                    include_bytes!("../../tests/fixtures/v2-batch-records.log"),
                )
                .unwrap();
                std::fs::write(
                    source_path.join("commit.state"),
                    include_bytes!("../../tests/fixtures/v2-batch-commit.state"),
                )
                .unwrap();
                std::fs::write(source_path.join("store.lock"), []).unwrap();
            } else {
                let source = Store::open(&source_path).unwrap();
                source.put(b"alpha", b"one").unwrap();
                source.put(b"beta", b"two").unwrap();
            }
            let before_log = std::fs::read(source_path.join("records.log")).unwrap();
            let before_witness = std::fs::read(source_path.join("commit.state")).unwrap();
            kill_at_boundary(directory.path(), mode);
            assert_eq!(
                std::fs::read(source_path.join("records.log")).unwrap(),
                before_log
            );
            assert_eq!(
                std::fs::read(source_path.join("commit.state")).unwrap(),
                before_witness
            );
            let source = Store::open(&source_path).unwrap();
            if mode == "upgrade-staged" {
                assert!(!upgraded.exists());
                assert!(std::fs::read_dir(directory.path()).unwrap().any(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".emda-stage-v1-d-")
                }));
                let cleaned = crate::cleanup_abandoned_staging(directory.path()).unwrap();
                assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (1, 1));
                source.upgrade_format(&upgraded).unwrap();
            } else {
                let cleaned = crate::cleanup_abandoned_staging(directory.path()).unwrap();
                assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (0, 1));
            }
            let copy = Store::open(&upgraded).unwrap();
            let expected = vec![Some(b"one".to_vec()), Some(b"two".to_vec())];
            assert_eq!(source.get_many(&[b"alpha", b"beta"]).unwrap(), expected);
            assert_eq!(copy.get_many(&[b"alpha", b"beta"]).unwrap(), expected);
        }
    }
}

#[test]
fn restore_parent_sync_failure_reports_uncertain_publication() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("source")).unwrap();
    store.put(b"key", b"durable").unwrap();
    let snapshot = dir.path().join("backup");
    store.backup(&snapshot).unwrap();
    let restored = dir.path().join("restored");
    let error = restore_with_sync(&snapshot, &restored, |_| {
        Err(io::Error::other("injected directory sync failure"))
    })
    .unwrap_err();
    assert!(error.to_string().contains("restore may exist"));
    assert_eq!(
        Store::open(&restored)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"durable"[..])
    );
    assert!(
        matches!(Store::restore(&snapshot, &restored), Err(e) if e.kind() == io::ErrorKind::AlreadyExists)
    );
}

#[test]
fn backup_parent_sync_failure_reports_uncertain_publication() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source");
    let store = Store::open(&source_path).unwrap();
    store.put(b"key", b"durable").unwrap();
    drop(store);
    let (log, _) = Log::open(&source_path, false).unwrap();
    let snapshot = dir.path().join("backup");
    let error = create_with_sync(&log, &snapshot, |_| {
        Err(io::Error::other("injected directory sync failure"))
    })
    .unwrap_err();
    assert!(error.to_string().contains("backup may exist"));
    let restored = dir.path().join("restored");
    Store::restore(&snapshot, &restored).unwrap();
    assert_eq!(
        Store::open(restored)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"durable"[..])
    );
    assert!(matches!(create(&log, &snapshot), Err(e) if e.kind() == io::ErrorKind::AlreadyExists));
}

#[test]
fn upgrade_parent_sync_failure_reports_uncertain_publication() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source");
    let source = Store::open(&source_path).unwrap();
    source.put(b"key", b"durable").unwrap();
    drop(source);
    let (log, _) = Log::open(&source_path, false).unwrap();
    let upgraded = directory.path().join("upgraded");
    let error = upgrade_format_with_hooks(
        &log,
        &upgraded,
        |_| Err(io::Error::other("injected parent sync failure")),
        || Ok(()),
    )
    .unwrap_err();
    assert!(error.to_string().contains("upgraded store may exist"));
    assert_eq!(
        Store::open(&upgraded)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"durable"[..])
    );
    assert!(
        matches!(upgrade_format(&log, &upgraded), Err(error) if error.kind() == io::ErrorKind::AlreadyExists)
    );
}

#[test]
fn every_backup_restore_and_upgrade_ancestor_sync_failure_is_uncertain() {
    let probe = tempfile::tempdir().unwrap();
    let count = std::fs::canonicalize(probe.path())
        .unwrap()
        .ancestors()
        .count();
    drop(probe);
    for mode in ["backup", "restore", "upgrade"] {
        for fail_at in 0..count {
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("source");
            let source = Store::open(&source_path).unwrap();
            source.put(b"key", b"durable").unwrap();
            let before = std::fs::read(source_path.join("records.log")).unwrap();
            let backup = dir.path().join("backup");
            if mode == "restore" {
                source.backup(&backup).unwrap();
            }
            drop(source);
            let (log, _) = Log::open(&source_path, false).unwrap();
            let destination = dir.path().join("destination");
            let mut seen = 0;
            let sync = |parent: &Path| {
                crate::directory::sync_existing_with(parent, |directory| {
                    let index = seen;
                    seen += 1;
                    if index == fail_at {
                        Err(io::Error::other("injected ancestor sync"))
                    } else {
                        File::open(directory)?.sync_all()
                    }
                })
            };
            let result = match mode {
                "backup" => create_with_sync(&log, &destination, sync),
                "restore" => restore_with_sync(&backup, &destination, sync),
                "upgrade" => upgrade_format_with_hooks(&log, &destination, sync, || Ok(())),
                _ => unreachable!(),
            };
            assert!(result.unwrap_err().to_string().contains("may exist"));
            assert_eq!(seen, fail_at + 1);
            assert!(destination.exists());
            assert_eq!(
                std::fs::read(source_path.join("records.log")).unwrap(),
                before
            );
            let copy_path = if mode == "backup" {
                let restored = dir.path().join("verify");
                Store::restore(&destination, &restored).unwrap();
                restored
            } else {
                destination
            };
            let copy = Store::open(&copy_path).unwrap();
            assert_eq!(copy.get(b"key").unwrap().as_deref(), Some(&b"durable"[..]));
        }
    }
}
