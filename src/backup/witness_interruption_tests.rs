//! Interrupt witness creation inside unpublished restore and upgrade stages.
use super::*;
use crate::Store;
use crate::test_support::retry_lock;
use rustix::fs::getxattr;
use rustix::io::Errno;
use std::io::BufRead;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const POINTS: [witness::CreationPoint; 4] = [
    witness::CreationPoint::FileCreated,
    witness::CreationPoint::Written,
    witness::CreationPoint::FileSynced,
    witness::CreationPoint::DirectorySynced,
];

fn point_name(point: witness::CreationPoint) -> &'static str {
    match point {
        witness::CreationPoint::FileCreated => "file-created",
        witness::CreationPoint::Written => "written",
        witness::CreationPoint::FileSynced => "file-synced",
        witness::CreationPoint::DirectorySynced => "directory-synced",
    }
}

fn selected_point(name: &str) -> witness::CreationPoint {
    POINTS
        .into_iter()
        .find(|point| point_name(*point) == name)
        .expect("known witness creation point")
}

fn invoke(
    root: &Path,
    operation: &str,
    hook: impl FnMut(witness::CreationPoint) -> io::Result<()>,
) -> io::Result<()> {
    match operation {
        "restore" => restore_with_witness_hooks(
            &root.join("backup"),
            &root.join("result"),
            |_| Ok(()),
            || Ok(()),
            |_| Ok(()),
            hook,
        ),
        "upgrade" => {
            let (source, _) = Log::open(&root.join("source"), false)?;
            upgrade_format_with_witness_hooks(
                &source,
                &root.join("result"),
                |_| Ok(()),
                || Ok(()),
                hook,
            )
        }
        _ => panic!("unknown operation"),
    }
}

fn metadata(path: &Path) -> Option<Vec<u8>> {
    let mut bytes = [0_u8; 36];
    match getxattr(path, "user.db.commit.v3", &mut bytes[..]) {
        Ok(length) => Some(bytes[..length].to_vec()),
        Err(Errno::NODATA | Errno::NOTSUP | Errno::NOSYS) => None,
        Err(error) => panic!("read source metadata: {error}"),
    }
}

pub(super) struct SourceSnapshot {
    log: Vec<u8>,
    witness: Vec<u8>,
    metadata: Option<Vec<u8>>,
    backup: Vec<u8>,
}

pub(super) fn prepare(root: &Path, v2: bool) -> SourceSnapshot {
    let source = root.join("source");
    if v2 {
        std::fs::create_dir(&source).unwrap();
        std::fs::write(
            source.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap();
        std::fs::write(
            source.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let store = retry_lock(|| Store::open(&source)).unwrap();
    store
        .put(b"large", &vec![0x5a; BUFFER_SIZE * 2 + 17])
        .unwrap();
    store.put(b"a", b"one").unwrap();
    store
        .write_batch(&[
            crate::BatchOperation::Put {
                key: b"b",
                value: b"two",
            },
            crate::BatchOperation::Delete { key: b"a" },
        ])
        .unwrap();
    store.backup(root.join("backup")).unwrap();
    drop(store);
    let log = std::fs::read(source.join("records.log")).unwrap();
    assert_eq!(&log[..8], if v2 { b"EMDALOG2" } else { b"EMDALOG3" });
    SourceSnapshot {
        log,
        witness: std::fs::read(source.join("commit.state")).unwrap(),
        metadata: metadata(&source.join("records.log")),
        backup: std::fs::read(root.join("backup")).unwrap(),
    }
}

pub(super) fn assert_preserved_and_retry(root: &Path, operation: &str, before: &SourceSnapshot) {
    let source = root.join("source");
    assert!(!root.join("result").exists());
    assert_eq!(
        std::fs::read(source.join("records.log")).unwrap(),
        before.log
    );
    assert_eq!(
        std::fs::read(source.join("commit.state")).unwrap(),
        before.witness
    );
    assert_eq!(metadata(&source.join("records.log")), before.metadata);
    assert_eq!(std::fs::read(root.join("backup")).unwrap(), before.backup);
    let source = retry_lock(|| Store::open(&source)).unwrap();
    match operation {
        "restore" => Store::restore(root.join("backup"), root.join("result")).unwrap(),
        "upgrade" => source.upgrade_format(root.join("result")).unwrap(),
        _ => unreachable!(),
    }
    let result = retry_lock(|| Store::open(root.join("result"))).unwrap();
    let expected = vec![None, Some(b"two".to_vec())];
    assert_eq!(source.get_many(&[b"a", b"b"]).unwrap(), expected);
    assert_eq!(result.get_many(&[b"a", b"b"]).unwrap(), expected);
    assert_eq!(
        result.get(b"large").unwrap(),
        Some(vec![0x5a; BUFFER_SIZE * 2 + 17])
    );
    result.put(b"after", b"retry").unwrap();
    drop(result);
    let reopened = retry_lock(|| Store::open(root.join("result"))).unwrap();
    assert_eq!(
        reopened.get(b"after").unwrap().as_deref(),
        Some(&b"retry"[..])
    );
}

#[test]
fn interruption_child() {
    let Ok(root) = std::env::var("EMDA_WITNESS_STAGE_ROOT") else {
        return;
    };
    let operation = std::env::var("EMDA_WITNESS_STAGE_OPERATION").unwrap();
    let point = selected_point(&std::env::var("EMDA_WITNESS_STAGE_POINT").unwrap());
    invoke(Path::new(&root), &operation, |current| {
        if current == point {
            println!("WITNESS_CREATION_BOUNDARY_REACHED");
            std::io::stdout().flush()?;
            loop {
                std::thread::park();
            }
        }
        Ok(())
    })
    .unwrap();
}

fn kill_at(root: &Path, operation: &str, point: witness::CreationPoint) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "backup::witness_interruption_tests::interruption_child",
            "--nocapture",
        ])
        .env("EMDA_WITNESS_STAGE_ROOT", root)
        .env("EMDA_WITNESS_STAGE_OPERATION", operation)
        .env("EMDA_WITNESS_STAGE_POINT", point_name(point))
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
            if line.contains("WITNESS_CREATION_BOUNDARY_REACHED") {
                let _ = sender.send(());
                break;
            }
        }
    });
    let reached = receiver.recv_timeout(Duration::from_secs(20));
    let busy = reached
        .is_ok()
        .then(|| crate::cleanup_abandoned_staging(root));
    let killed = child.kill();
    child.wait().unwrap();
    reached.expect("child did not reach witness creation point");
    killed.expect("could not terminate child");
    assert!(matches!(busy, Some(Err(ref error)) if error.kind() == io::ErrorKind::WouldBlock));
}

#[test]
fn killed_witness_creation_never_publishes_partial_restore_or_upgrade() {
    for operation in ["restore", "upgrade"] {
        for v2 in [false, true] {
            for point in POINTS {
                let root = tempfile::tempdir().unwrap();
                let before = prepare(root.path(), v2);
                kill_at(root.path(), operation, point);
                assert!(
                    !root.path().join("result").exists(),
                    "{operation} {v2} {point:?}"
                );
                let cleanup = crate::cleanup_abandoned_staging(root.path()).unwrap();
                assert_eq!((cleanup.removed_payloads, cleanup.removed_markers), (1, 1));
                assert_preserved_and_retry(root.path(), operation, &before);
            }
        }
    }
}

#[test]
fn errors_during_witness_creation_leave_source_and_backup_reusable() {
    for operation in ["restore", "upgrade"] {
        for v2 in [false, true] {
            for point in POINTS {
                let root = tempfile::tempdir().unwrap();
                let before = prepare(root.path(), v2);
                let result = invoke(root.path(), operation, |current| {
                    if current == point {
                        Err(io::Error::other("injected witness creation error"))
                    } else {
                        Ok(())
                    }
                });
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("injected witness creation error"),
                    "{operation} {v2} {point:?}"
                );
                let cleanup = crate::cleanup_abandoned_staging(root.path()).unwrap();
                assert_eq!((cleanup.removed_payloads, cleanup.removed_markers), (0, 0));
                assert_preserved_and_retry(root.path(), operation, &before);
            }
        }
    }
}
