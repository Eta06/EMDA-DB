//! Interrupt restore and upgrade while they construct their private log copies.
use super::witness_interruption_tests::{SOURCE_V2_CASES, assert_preserved_and_retry, prepare};
use super::*;
use std::io::BufRead;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const RESTORE_POINTS: [RebuildPoint; 9] = [
    RebuildPoint::LogCreated,
    RebuildPoint::DataChunk,
    RebuildPoint::DataCopied,
    RebuildPoint::LogSynced,
    RebuildPoint::LogVerified,
    RebuildPoint::WitnessInstalled,
    RebuildPoint::LockSynced,
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
            (&root.join("backup"), FullLogRestoreLimits::default()),
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

fn staged_directory(root: &Path) -> std::path::PathBuf {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".owner.data")
        })
        .expect("private rebuild directory")
}

#[test]
fn killed_rebuilds_never_publish_partial_restore_or_upgrade() {
    for (operation, points) in [
        ("restore", &RESTORE_POINTS[..]),
        ("upgrade", &UPGRADE_POINTS[..]),
    ] {
        for &v2 in SOURCE_V2_CASES {
            for &point in points {
                let root = tempfile::tempdir().unwrap();
                let before = prepare(root.path(), v2);
                kill_at(root.path(), operation, point);
                assert!(
                    !root.path().join("result").exists(),
                    "{operation} {v2} {point:?}"
                );
                if point == RebuildPoint::DataChunk {
                    let payload = staged_directory(root.path());
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
        for &v2 in SOURCE_V2_CASES {
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

#[test]
fn changed_staged_log_identity_or_length_prevents_publication() {
    for &v2 in SOURCE_V2_CASES {
        for operation in ["restore", "upgrade"] {
            for point in [RebuildPoint::LogVerified, RebuildPoint::DirectorySynced] {
                for replace in [false, true] {
                    let root = tempfile::tempdir().unwrap();
                    let before = prepare(root.path(), v2);
                    let error = invoke(root.path(), operation, |current| {
                        if current == point {
                            let log = staged_directory(root.path()).join("records.log");
                            if replace {
                                let replacement = log.with_extension("replacement");
                                std::fs::copy(&log, &replacement)?;
                                std::fs::rename(replacement, log)?;
                            } else {
                                OpenOptions::new()
                                    .append(true)
                                    .open(log)?
                                    .write_all(b"tail")?;
                            }
                        }
                        Ok(())
                    })
                    .unwrap_err();
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                    assert!(
                        error
                            .to_string()
                            .contains("staged log identity or length changed")
                    );
                    let cleanup = crate::cleanup_abandoned_staging(root.path()).unwrap();
                    assert_eq!((cleanup.removed_payloads, cleanup.removed_markers), (0, 0));
                    assert_preserved_and_retry(root.path(), operation, &before);
                }
            }
        }
    }
}
