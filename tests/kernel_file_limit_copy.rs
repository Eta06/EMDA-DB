//! Kernel-enforced file-size failures during copy publication on Linux.
//! These disposable-process checks do not simulate physical ENOSPC or power loss.
#![cfg(target_os = "linux")]

use database_engine::{Store, cleanup_abandoned_staging};
use rustix::fs::getxattr;
use rustix::process::{Resource, Rlimit, Signal, getrlimit, setrlimit};
use std::fs;
use std::io::{self, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::Command;

const XATTR: &str = "user.db.commit.v3";
const READY: &str = "EMDA_COPY_FILE_LIMIT_READY";
const LARGE_VALUE: &[u8] = &[0x6b; 4096];

#[derive(Clone, Copy, Debug)]
enum Format {
    V2,
    V3,
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Backup,
    Restore,
    Upgrade,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::Upgrade => "upgrade",
        }
    }
}

fn witness(path: &Path, format: Format) -> Vec<u8> {
    match format {
        Format::V2 => fs::read(path.join("commit.state")).unwrap(),
        Format::V3 => {
            let mut bytes = [0; 36];
            assert_eq!(
                getxattr(path.join("records.log"), XATTR, &mut bytes[..]).unwrap(),
                bytes.len()
            );
            bytes.to_vec()
        }
    }
}

fn set_limit(limit: u64) {
    let core = getrlimit(Resource::Core);
    setrlimit(
        Resource::Core,
        Rlimit {
            current: Some(0),
            maximum: core.maximum,
        },
    )
    .unwrap();
    let file = getrlimit(Resource::Fsize);
    assert!(file.maximum.is_none_or(|maximum| limit <= maximum));
    setrlimit(
        Resource::Fsize,
        Rlimit {
            current: Some(limit),
            maximum: file.maximum,
        },
    )
    .unwrap();
}

fn verify(store: &Store) {
    assert_eq!(
        store.get(b"safe").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(store.get(b"large").unwrap().as_deref(), Some(LARGE_VALUE));
}

#[test]
fn kernel_file_limit_copy_child() {
    let Some(root) = std::env::var_os("EMDA_COPY_LIMIT_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let operation = std::env::var("EMDA_COPY_LIMIT_OPERATION").unwrap();
    let source = if operation == "restore" {
        None
    } else {
        Some(Store::open(root.join("source")).unwrap())
    };
    let limit = std::env::var("EMDA_COPY_LIMIT_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    set_limit(limit);
    println!("{READY}");
    io::stdout().flush().unwrap();
    let result = match operation.as_str() {
        "backup" => source.as_ref().unwrap().backup(root.join("failed-backup")),
        "restore" => Store::restore(root.join("backup"), root.join("failed-store")),
        "upgrade" => source
            .as_ref()
            .unwrap()
            .upgrade_format(root.join("failed-store")),
        _ => panic!("unknown operation"),
    };
    // An inherited ignored SIGXFSZ lets the caller observe the kernel error.
    let error = result.unwrap_err();
    let file_too_large = io::Error::from_raw_os_error(27).to_string();
    assert!(
        error.raw_os_error() == Some(27) || error.to_string().contains(&file_too_large),
        "{error:?}"
    );
    if let Some(source) = source {
        verify(&source);
    }
}

#[test]
fn file_size_failures_never_publish_partial_copies_and_allow_retry() {
    let mut outcomes = [0_usize; 2];
    for format in [Format::V2, Format::V3] {
        for operation in [Operation::Backup, Operation::Restore, Operation::Upgrade] {
            for limit in [128_u64, 512, 2048] {
                for ignore_signal in [false, true] {
                    let root = tempfile::tempdir().unwrap();
                    let source = root.path().join("source");
                    let backup = root.path().join("backup");
                    fs::create_dir(&source).unwrap();
                    if matches!(format, Format::V2) {
                        fs::write(
                            source.join("records.log"),
                            include_bytes!("fixtures/v2-empty-records.log"),
                        )
                        .unwrap();
                        fs::write(
                            source.join("commit.state"),
                            include_bytes!("fixtures/v2-empty-commit.state"),
                        )
                        .unwrap();
                    }
                    let store = Store::open(&source).unwrap();
                    store.put(b"safe", b"acknowledged").unwrap();
                    store.put(b"large", LARGE_VALUE).unwrap();
                    store.backup(&backup).unwrap();
                    drop(store);
                    let log_path = source.join("records.log");
                    let before = fs::read(&log_path).unwrap();
                    let anchor = fs::read(source.join("commit.state")).unwrap();
                    let marker = witness(&source, format);
                    let backup_before = fs::read(&backup).unwrap();
                    assert_eq!(
                        &before[..8],
                        match format {
                            Format::V2 => b"EMDALOG2",
                            Format::V3 => b"EMDALOG3",
                        }
                    );
                    assert!(before.len() as u64 > limit && backup_before.len() as u64 > limit);
                    let executable = std::env::current_exe().unwrap();
                    let mut command = if ignore_signal {
                        let mut shell = Command::new("sh");
                        shell.args(["-c", "trap '' XFSZ; exec \"$@\"", "copy-limit-child"]);
                        shell.arg(&executable);
                        shell
                    } else {
                        Command::new(&executable)
                    };
                    let output = command
                        .args(["--exact", "kernel_file_limit_copy_child", "--nocapture"])
                        .env("EMDA_COPY_LIMIT_ROOT", root.path())
                        .env("EMDA_COPY_LIMIT_OPERATION", operation.name())
                        .env("EMDA_COPY_LIMIT_BYTES", limit.to_string())
                        .output()
                        .unwrap();
                    let context = format!(
                        "{format:?} {operation:?} limit={limit} ignore_signal={ignore_signal} stdout={} stderr={}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    assert!(
                        String::from_utf8_lossy(&output.stdout).contains(READY),
                        "{context}"
                    );
                    assert!(
                        output.status.success()
                            || output.status.signal() == Some(Signal::XFSZ.as_raw()),
                        "{context}"
                    );
                    if ignore_signal {
                        assert!(output.status.success(), "{context}");
                    }
                    outcomes[usize::from(output.status.success())] += 1;
                    assert!(!root.path().join("failed-backup").exists(), "{context}");
                    assert!(!root.path().join("failed-store").exists(), "{context}");
                    assert_eq!(fs::read(&log_path).unwrap(), before, "{context}");
                    assert_eq!(
                        fs::read(source.join("commit.state")).unwrap(),
                        anchor,
                        "{context}"
                    );
                    assert_eq!(witness(&source, format), marker, "{context}");
                    assert_eq!(fs::read(&backup).unwrap(), backup_before, "{context}");
                    cleanup_abandoned_staging(root.path()).unwrap();
                    let retry = root.path().join("retry");
                    match operation {
                        Operation::Backup => {
                            let store = Store::open(&source).unwrap();
                            store.backup(&retry).unwrap();
                            drop(store);
                            let restored = root.path().join("restored");
                            Store::restore(&retry, &restored).unwrap();
                            verify(&Store::open(restored).unwrap());
                        }
                        Operation::Restore => {
                            Store::restore(&backup, &retry).unwrap();
                            verify(&Store::open(&retry).unwrap());
                        }
                        Operation::Upgrade => {
                            let store = Store::open(&source).unwrap();
                            store.upgrade_format(&retry).unwrap();
                            drop(store);
                            verify(&Store::open(&retry).unwrap());
                        }
                    }
                    assert_eq!(fs::read(&log_path).unwrap(), before, "{context}");
                    assert_eq!(
                        fs::read(source.join("commit.state")).unwrap(),
                        anchor,
                        "{context}"
                    );
                    assert_eq!(witness(&source, format), marker, "{context}");
                    assert_eq!(fs::read(&backup).unwrap(), backup_before, "{context}");
                }
            }
        }
    }
    eprintln!(
        "copy file-size cases: signaled={} returned EFBIG={}",
        outcomes[0], outcomes[1]
    );
}
