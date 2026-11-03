use super::*;
use crate::Store;

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
    child.kill().unwrap();
    child.wait().unwrap();
    reached.expect("child did not reach the requested backup or restore boundary");
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
                    .starts_with(".db-backup-")
            }));
            source.backup(&backup).unwrap();
        } else {
            assert!(backup.is_file());
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
                    .starts_with(".db-restore-")
            }));
            Store::restore(&backup, &restored).unwrap();
        }
        let source = Store::open(&source_path).unwrap();
        let copy = Store::open(&restored).unwrap();
        let expected = vec![Some(b"one".to_vec()), Some(b"two".to_vec())];
        assert_eq!(source.get_many(&[b"a", b"b"]).unwrap(), expected);
        assert_eq!(copy.get_many(&[b"a", b"b"]).unwrap(), expected);
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
