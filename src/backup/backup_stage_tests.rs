//! Interrupt backup construction before a verified file is published.
use super::*;
use crate::Store;
use crate::test_support::retry_lock;
use rustix::fs::getxattr;
use rustix::io::Errno;
use std::io::BufRead;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const POINTS: [BackupCreationPoint; 6] = [
    BackupCreationPoint::HeaderWritten,
    BackupCreationPoint::DataChunk,
    BackupCreationPoint::DataCopied,
    BackupCreationPoint::FooterWritten,
    BackupCreationPoint::FileSynced,
    BackupCreationPoint::Verified,
];

fn name(point: BackupCreationPoint) -> &'static str {
    match point {
        BackupCreationPoint::HeaderWritten => "header-written",
        BackupCreationPoint::DataChunk => "first-data-chunk",
        BackupCreationPoint::DataCopied => "data-copied",
        BackupCreationPoint::FooterWritten => "footer-written",
        BackupCreationPoint::FileSynced => "file-synced",
        BackupCreationPoint::Verified => "verified",
    }
}

fn data() -> Vec<u8> {
    vec![0x5a; BUFFER_SIZE * 2 + 17]
}

fn metadata(path: &Path) -> Option<Vec<u8>> {
    let mut bytes = [0_u8; 36];
    match getxattr(path, "user.db.commit.v3", &mut bytes[..]) {
        Ok(length) => Some(bytes[..length].to_vec()),
        Err(Errno::NODATA | Errno::NOTSUP | Errno::NOSYS) => None,
        Err(error) => panic!("read source metadata: {error}"),
    }
}

struct SourceSnapshot {
    log: Vec<u8>,
    witness: Vec<u8>,
    metadata: Option<Vec<u8>>,
}

fn prepare(root: &Path, v2: bool) -> SourceSnapshot {
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
    store.put(b"large", &data()).unwrap();
    store.put(b"keep", b"acknowledged").unwrap();
    drop(store);
    let log = std::fs::read(source.join("records.log")).unwrap();
    assert_eq!(&log[..8], if v2 { b"EMDALOG2" } else { b"EMDALOG3" });
    SourceSnapshot {
        log,
        witness: std::fs::read(source.join("commit.state")).unwrap(),
        metadata: metadata(&source.join("records.log")),
    }
}

fn invoke(root: &Path, hook: impl FnMut(BackupCreationPoint) -> io::Result<()>) -> io::Result<()> {
    let (source, _) = Log::open(&root.join("source"), false)?;
    create_with_stage_hooks(&source, &root.join("backup"), |_| Ok(()), || Ok(()), hook)
}

fn assert_preserved_and_retry(root: &Path, before: &SourceSnapshot) {
    let path = root.join("source");
    assert!(!root.join("backup").exists());
    assert_eq!(std::fs::read(path.join("records.log")).unwrap(), before.log);
    assert_eq!(
        std::fs::read(path.join("commit.state")).unwrap(),
        before.witness
    );
    assert_eq!(metadata(&path.join("records.log")), before.metadata);
    let source = retry_lock(|| Store::open(&path)).unwrap();
    source.backup(root.join("backup")).unwrap();
    Store::restore(root.join("backup"), root.join("restored")).unwrap();
    let restored = retry_lock(|| Store::open(root.join("restored"))).unwrap();
    assert_eq!(source.get(b"large").unwrap(), Some(data()));
    assert_eq!(restored.get(b"large").unwrap(), Some(data()));
    assert_eq!(
        restored.get(b"keep").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}

#[test]
fn interruption_child() {
    let Ok(root) = std::env::var("EMDA_BACKUP_STAGE_ROOT") else {
        return;
    };
    let requested = std::env::var("EMDA_BACKUP_STAGE_POINT").unwrap();
    invoke(Path::new(&root), |point| {
        if name(point) == requested {
            println!("BACKUP_STAGE_BOUNDARY_REACHED");
            std::io::stdout().flush()?;
            loop {
                std::thread::park();
            }
        }
        Ok(())
    })
    .unwrap();
}

fn kill_at(root: &Path, point: BackupCreationPoint) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "backup::backup_stage_tests::interruption_child",
            "--nocapture",
        ])
        .env("EMDA_BACKUP_STAGE_ROOT", root)
        .env("EMDA_BACKUP_STAGE_POINT", name(point))
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
            if line.contains("BACKUP_STAGE_BOUNDARY_REACHED") {
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
    reached.expect("child did not reach backup construction point");
    killed.expect("could not terminate child");
    assert!(matches!(busy, Some(Err(ref error)) if error.kind() == io::ErrorKind::WouldBlock));
}

#[test]
fn killed_backup_construction_never_publishes_a_partial_file() {
    for v2 in [false, true] {
        for point in POINTS {
            let root = tempfile::tempdir().unwrap();
            let before = prepare(root.path(), v2);
            kill_at(root.path(), point);
            assert!(!root.path().join("backup").exists(), "{v2} {point:?}");
            if point == BackupCreationPoint::DataChunk {
                let payload = std::fs::read_dir(root.path())
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .unwrap()
                            .to_string_lossy()
                            .ends_with(".owner.data")
                    })
                    .expect("partial backup payload");
                assert_eq!(
                    std::fs::metadata(payload).unwrap().len(),
                    HEADER_SIZE + BUFFER_SIZE as u64
                );
                assert!(before.log.len() > BUFFER_SIZE);
            }
            let cleanup = crate::cleanup_abandoned_staging(root.path()).unwrap();
            assert_eq!((cleanup.removed_payloads, cleanup.removed_markers), (1, 1));
            assert_preserved_and_retry(root.path(), &before);
        }
    }
}

#[test]
fn backup_construction_errors_preserve_source_and_allow_retry() {
    for v2 in [false, true] {
        for point in POINTS {
            let root = tempfile::tempdir().unwrap();
            let before = prepare(root.path(), v2);
            let result = invoke(root.path(), |current| {
                if current == point {
                    Err(io::Error::other("injected backup construction failure"))
                } else {
                    Ok(())
                }
            });
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected backup construction failure"),
                "{v2} {point:?}"
            );
            let cleanup = crate::cleanup_abandoned_staging(root.path()).unwrap();
            assert_eq!((cleanup.removed_payloads, cleanup.removed_markers), (0, 0));
            assert_preserved_and_retry(root.path(), &before);
        }
    }
}
