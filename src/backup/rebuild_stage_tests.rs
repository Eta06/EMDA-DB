//! Interrupt restore and upgrade while they construct their private log copies.
use super::witness_interruption_tests::{assert_preserved_and_retry, prepare};
use super::*;
use std::io::BufRead;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const RESTORE_POINTS: [RebuildPoint; 8] = [
    RebuildPoint::LogCreated,
    RebuildPoint::DataChunk,
    RebuildPoint::DataCopied,
    RebuildPoint::LogSynced,
    RebuildPoint::LogVerified,
    RebuildPoint::WitnessInstalled,
    RebuildPoint::DirectorySynced,
    RebuildPoint::LockReleased,
];
const UPGRADE_POINTS: [RebuildPoint; 10] = [
    RebuildPoint::LogCreated,
    RebuildPoint::HeaderWritten,
    RebuildPoint::DataChunk,
    RebuildPoint::DataCopied,
    RebuildPoint::MetadataPrepared,
    RebuildPoint::LogSynced,
    RebuildPoint::LogVerified,
    RebuildPoint::WitnessInstalled,
    RebuildPoint::LockSynced,
    RebuildPoint::DirectorySynced,
];

fn name(point: RebuildPoint) -> &'static str {
    match point {
        RebuildPoint::LogCreated => "log-created",
        RebuildPoint::HeaderWritten => "header-written",
        RebuildPoint::DataChunk => "first-data-chunk",
        RebuildPoint::DataCopied => "data-copied",
        RebuildPoint::MetadataPrepared => "metadata-prepared",
        RebuildPoint::LogSynced => "log-synced",
        RebuildPoint::LogVerified => "log-verified",
        RebuildPoint::WitnessInstalled => "witness-installed",
        RebuildPoint::LockSynced => "lock-synced",
        RebuildPoint::DirectorySynced => "directory-synced",
        RebuildPoint::LockReleased => "lock-released",
    }
}

fn invoke(
    root: &Path,
    operation: &str,
    hook: impl FnMut(RebuildPoint) -> io::Result<()>,
) -> io::Result<()> {
    match operation {
        "restore" => restore_with_stage_hooks(
            &root.join("backup"),
            &root.join("result"),
            |_| Ok(()),
            || Ok(()),
            |_| Ok(()),
            |_| Ok(()),
            hook,
        ),
        "upgrade" => {
            let (source, _) = Log::open(&root.join("source"), false)?;
            upgrade_format_with_stage_hooks(
                &source,
                &root.join("result"),
                |_| Ok(()),
                || Ok(()),
                |_| Ok(()),
                hook,
            )
        }
        _ => panic!("unknown operation"),
    }
}

#[test]
fn interruption_child() {
    let Ok(root) = std::env::var("EMDA_REBUILD_STAGE_ROOT") else {
        return;
    };
    let operation = std::env::var("EMDA_REBUILD_STAGE_OPERATION").unwrap();
    let requested = std::env::var("EMDA_REBUILD_STAGE_POINT").unwrap();
    invoke(Path::new(&root), &operation, |point| {
        if name(point) == requested {
            println!("REBUILD_STAGE_BOUNDARY_REACHED");
            std::io::stdout().flush()?;
            loop {
                std::thread::park();
            }
        }
        Ok(())
    })
    .unwrap();
}

fn kill_at(root: &Path, operation: &str, point: RebuildPoint) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "backup::rebuild_stage_tests::interruption_child",
            "--nocapture",
        ])
        .env("EMDA_REBUILD_STAGE_ROOT", root)
        .env("EMDA_REBUILD_STAGE_OPERATION", operation)
        .env("EMDA_REBUILD_STAGE_POINT", name(point))
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
            if line.contains("REBUILD_STAGE_BOUNDARY_REACHED") {
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
    reached.expect("child did not reach rebuild construction point");
    killed.expect("could not terminate child");
    assert!(matches!(busy, Some(Err(ref error)) if error.kind() == io::ErrorKind::WouldBlock));
}

#[test]
fn killed_rebuilds_never_publish_partial_restore_or_upgrade() {
    for (operation, points) in [
        ("restore", &RESTORE_POINTS[..]),
        ("upgrade", &UPGRADE_POINTS[..]),
    ] {
        for v2 in [false, true] {
            for &point in points {
                let root = tempfile::tempdir().unwrap();
                let before = prepare(root.path(), v2);
                kill_at(root.path(), operation, point);
                assert!(
                    !root.path().join("result").exists(),
                    "{operation} {v2} {point:?}"
                );
                if point == RebuildPoint::DataChunk {
                    let payload = std::fs::read_dir(root.path())
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .find(|path| {
                            path.file_name()
                                .unwrap()
                                .to_string_lossy()
                                .ends_with(".owner.data")
                        })
                        .expect("partial rebuild directory");
                    let header = if operation == "upgrade" {
                        witness::LOG_HEADER_SIZE
                    } else {
                        0
                    };
                    assert_eq!(
                        std::fs::metadata(payload.join("records.log"))
                            .unwrap()
                            .len(),
                        (header + BUFFER_SIZE) as u64
                    );
                }
                let cleanup = crate::cleanup_abandoned_staging(root.path()).unwrap();
                assert_eq!((cleanup.removed_payloads, cleanup.removed_markers), (1, 1));
                assert_preserved_and_retry(root.path(), operation, &before);
            }
        }
    }
}

#[test]
fn rebuild_errors_leave_source_and_backup_reusable() {
    for (operation, points) in [
        ("restore", &RESTORE_POINTS[..]),
        ("upgrade", &UPGRADE_POINTS[..]),
    ] {
        for v2 in [false, true] {
            for &point in points {
                let root = tempfile::tempdir().unwrap();
                let before = prepare(root.path(), v2);
                let result = invoke(root.path(), operation, |current| {
                    if current == point {
                        Err(io::Error::other("injected rebuild construction failure"))
                    } else {
                        Ok(())
                    }
                });
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("injected rebuild construction failure"),
                    "{operation} {v2} {point:?}"
                );
                let cleanup = crate::cleanup_abandoned_staging(root.path()).unwrap();
                assert_eq!((cleanup.removed_payloads, cleanup.removed_markers), (0, 0));
                assert_preserved_and_retry(root.path(), operation, &before);
            }
        }
    }
}
