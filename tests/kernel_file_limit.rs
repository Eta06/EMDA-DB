//! Kernel-enforced file-size failures on disposable V2/V3 stores.
//! This is not a physical disk-full or power-loss test.
#![cfg(target_os = "linux")]

use database_engine::{CheckpointStore, Store};
use rustix::fs::getxattr;
use rustix::process::{Resource, Rlimit, Signal, getrlimit, setrlimit};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::Command;

const XATTR: &str = "user.db.commit.v3";
const CHILD_READY: &str = "EMDA_FILE_LIMIT_READY";

#[derive(Clone, Copy, Debug)]
enum Format {
    V2,
    V3,
}

#[derive(Clone, Copy, Debug)]
enum Writer {
    Ordinary,
    Checkpoint,
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

fn hex(token: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in token {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}

fn parse_hex(hex: &str) -> [u8; 32] {
    assert_eq!(hex.len(), 64);
    let mut token = [0; 32];
    for (index, byte) in token.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap();
    }
    token
}

enum Engine {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl Engine {
    fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], writer: Writer) -> io::Result<Self> {
        match writer {
            Writer::Ordinary => Store::open(path).map(Self::Ordinary),
            Writer::Checkpoint => {
                CheckpointStore::open(path, checkpoint, digest).map(Self::Checkpoint)
            }
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        match self {
            Self::Ordinary(store) => store.put(key, value),
            Self::Checkpoint(store) => store.put(key, value),
        }
    }

    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(store) => store.get(key),
            Self::Checkpoint(store) => store.get(key),
        }
    }
}

#[test]
fn kernel_file_limit_child() {
    let Some(root) = std::env::var_os("EMDA_FILE_LIMIT_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let digest = parse_hex(&std::env::var("EMDA_FILE_LIMIT_DIGEST").unwrap());
    let writer = match std::env::var("EMDA_FILE_LIMIT_WRITER").unwrap().as_str() {
        "ordinary" => Writer::Ordinary,
        "checkpoint" => Writer::Checkpoint,
        _ => panic!("unknown writer"),
    };
    let store = Engine::open(
        &root.join("store"),
        &root.join("checkpoint"),
        &digest,
        writer,
    )
    .unwrap();
    let old_core = getrlimit(Resource::Core);
    setrlimit(
        Resource::Core,
        Rlimit {
            current: Some(0),
            maximum: old_core.maximum,
        },
    )
    .unwrap();
    let old_limit = getrlimit(Resource::Fsize);
    let limit = std::env::var("EMDA_FILE_LIMIT_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    assert!(old_limit.maximum.is_none_or(|maximum| limit <= maximum));
    setrlimit(
        Resource::Fsize,
        Rlimit {
            current: Some(limit),
            maximum: old_limit.maximum,
        },
    )
    .unwrap();
    println!("{CHILD_READY}");
    io::stdout().flush().unwrap();
    // Depending on inherited signal disposition, the kernel may terminate the
    // process with SIGXFSZ or return EFBIG. If returned, the open handle must
    // fail closed until recovery; the parent checks the on-disk state in both.
    let error = store.put(b"failed", &vec![0x5a; 4096]).unwrap_err();
    let file_too_large = io::Error::from_raw_os_error(27).to_string();
    assert!(
        error.raw_os_error() == Some(27) || error.to_string().contains(&file_too_large),
        "{error:?}"
    );
    assert!(store.get(b"safe").is_err());
}

#[test]
fn kernel_file_size_failures_preserve_witness_and_prior_bytes() {
    let mut outcomes = [0_usize; 2];
    for format in [Format::V2, Format::V3] {
        for writer in [Writer::Ordinary, Writer::Checkpoint] {
            for extra_bytes in [0_u64, 1, 8, 32, 2048] {
                for ignore_signal in [false, true] {
                    let root = tempfile::tempdir().unwrap();
                    let path = root.path().join("store");
                    let checkpoint = root.path().join("checkpoint");
                    fs::create_dir(&path).unwrap();
                    if matches!(format, Format::V2) {
                        fs::write(
                            path.join("records.log"),
                            include_bytes!("fixtures/v2-empty-records.log"),
                        )
                        .unwrap();
                        fs::write(
                            path.join("commit.state"),
                            include_bytes!("fixtures/v2-empty-commit.state"),
                        )
                        .unwrap();
                    }
                    let store = Store::open(&path).unwrap();
                    store.put(b"safe", b"acknowledged").unwrap();
                    let digest = store.checkpoint_to(&checkpoint).unwrap();
                    drop(store);
                    let log_path = path.join("records.log");
                    let before = fs::read(&log_path).unwrap();
                    let marker = witness(&path, format);
                    let anchor = fs::read(path.join("commit.state")).unwrap();
                    assert_eq!(
                        &before[..8],
                        match format {
                            Format::V2 => b"EMDALOG2",
                            Format::V3 => b"EMDALOG3",
                        }
                    );
                    let executable = std::env::current_exe().unwrap();
                    let mut command = if ignore_signal {
                        // Exercise the returned EFBIG path even when SIGXFSZ would
                        // otherwise terminate the child before it sees the error.
                        let mut shell = Command::new("sh");
                        shell.args(["-c", "trap '' XFSZ; exec \"$@\"", "file-limit-child"]);
                        shell.arg(&executable);
                        shell
                    } else {
                        Command::new(&executable)
                    };
                    let output = command
                        .args(["--exact", "kernel_file_limit_child", "--nocapture"])
                        .env("EMDA_FILE_LIMIT_ROOT", root.path())
                        .env("EMDA_FILE_LIMIT_DIGEST", hex(&digest))
                        .env(
                            "EMDA_FILE_LIMIT_WRITER",
                            match writer {
                                Writer::Ordinary => "ordinary",
                                Writer::Checkpoint => "checkpoint",
                            },
                        )
                        .env(
                            "EMDA_FILE_LIMIT_BYTES",
                            (before.len() as u64 + extra_bytes).to_string(),
                        )
                        .output()
                        .unwrap();
                    let context = format!(
                        "{format:?} {writer:?} extra={extra_bytes} ignore_signal={ignore_signal} stdout={} stderr={}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    assert!(
                        String::from_utf8_lossy(&output.stdout).contains(CHILD_READY),
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
                    let after = fs::read(&log_path).unwrap();
                    assert_eq!(
                        after.len() as u64,
                        before.len() as u64 + extra_bytes,
                        "{context}"
                    );
                    assert_eq!(&after[..before.len()], before, "{context}");
                    assert_eq!(witness(&path, format), marker, "{context}");
                    assert_eq!(
                        fs::read(path.join("commit.state")).unwrap(),
                        anchor,
                        "{context}"
                    );
                    for recovery in [Writer::Ordinary, Writer::Checkpoint] {
                        match Engine::open(&path, &checkpoint, &digest, recovery) {
                            Ok(store) if extra_bytes == 0 => {
                                assert_eq!(
                                    store.get(b"safe").unwrap().as_deref(),
                                    Some(&b"acknowledged"[..]),
                                    "{context}"
                                );
                                assert_eq!(store.get(b"failed").unwrap(), None, "{context}");
                            }
                            Err(error)
                                if extra_bytes > 0
                                    && error.kind() == io::ErrorKind::InvalidData => {}
                            Ok(_) => panic!("unexpectedly opened partial record: {context}"),
                            Err(error) => panic!("unexpected recovery error {error:?}: {context}"),
                        }
                        assert_eq!(fs::read(&log_path).unwrap(), after, "{context}");
                        assert_eq!(witness(&path, format), marker, "{context}");
                        assert_eq!(
                            fs::read(path.join("commit.state")).unwrap(),
                            anchor,
                            "{context}"
                        );
                    }
                }
            }
        }
    }
    eprintln!(
        "file-size cases: signaled={} returned EFBIG={}",
        outcomes[0], outcomes[1]
    );
}
