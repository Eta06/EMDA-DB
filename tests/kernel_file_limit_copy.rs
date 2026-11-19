//! Kernel-enforced file-size failures during copy publication on Linux.
//! These disposable-process checks do not simulate physical ENOSPC or power loss.
#![cfg(target_os = "linux")]

use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    AuthorizedRetainedStore, Expected, FileRetainedAuthority, IndexLimits, ManagedRetainedStore,
    OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixExpected, SuffixLimits,
};
use database_engine::{PublicationFailure, RetainedExportLimits, Store, cleanup_abandoned_staging};
use rustix::fs::getxattr;
use rustix::process::{Resource, Rlimit, Signal, getrlimit, setrlimit};
use std::fs;
use std::io::{self, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;

mod kernel_file_limit_common;
mod support;

use support::retry_lock;

const XATTR: &str = "user.db.commit.v3";
const READY: &str = "EMDA_COPY_FILE_LIMIT_READY";
const LARGE_VALUE: &[u8] = &[0x6b; 4096];
const EXPORT: RetainedExportLimits = RetainedExportLimits {
    max_log_bytes: 8192,
    max_live_keys: 16,
    max_live_key_bytes: 128,
    max_output_bytes: 8192,
};
const RETAINED: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 8192,
        entries: 16,
        key_bytes: 128,
        work_bytes: 8193,
    },
    index: IndexLimits {
        blocks: 4,
        anchor_bytes: 32,
    },
    suffix: SuffixLimits {
        bytes: 8192,
        operations: 32,
        work: 4096,
    },
    overlay: OverlayLimits {
        entries: 16,
        key_bytes: 128,
    },
};
type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;

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
    RetainedCutover,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::Upgrade => "upgrade",
            Self::RetainedCutover => "retained-cutover",
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
fn kernel_retained_restore_child() {
    let Some(root) = std::env::var_os("EMDA_RETAINED_RESTORE_LIMIT_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let binding = fs::read(root.join("expected")).unwrap();
    let expected = SuffixExpected {
        candidate: Expected {
            header: binding[..112].try_into().unwrap(),
            witness: binding[112..144].try_into().unwrap(),
            base_digest: binding[144..176].try_into().unwrap(),
        },
        end: u64::from_le_bytes(binding[176..184].try_into().unwrap()),
        digest: binding[184..].try_into().unwrap(),
    };
    set_limit(
        std::env::var("EMDA_COPY_LIMIT_BYTES")
            .unwrap()
            .parse()
            .unwrap(),
    );
    println!("{READY}");
    io::stdout().flush().unwrap();
    let result = ManagedRetainedStore::restore_backup(
        root.join("retained-backup"),
        root.join("restored"),
        RETAINED,
        &expected,
    );
    assert!(
        matches!(
            result,
            Err(PublicationFailure::NotPublished(ref cause)) if cause.raw_os_error() == Some(27)
        ),
        "{result:?}"
    );
}

#[test]
fn retained_restore_file_size_failures_preserve_backup_and_allow_retry() {
    let root = tempfile::tempdir().unwrap();
    let original = root.path().join("source");
    let source = Store::open(&original).unwrap();
    source.put(b"safe", b"acknowledged").unwrap();
    let catalog = root.path().join("catalog");
    ManagedRetainedStore::copy_from_store(source, &original, &catalog, EXPORT, RETAINED).unwrap();
    let mut store = ManagedRetainedStore::open(&catalog, RETAINED).unwrap();
    store.put(b"large", LARGE_VALUE).unwrap();
    let backup = root.path().join("retained-backup");
    let expected = store.backup_to(&backup).unwrap();
    let binding = [
        expected.candidate.header.as_slice(),
        &expected.candidate.witness,
        &expected.candidate.base_digest,
        &expected.end.to_le_bytes(),
        &expected.digest,
    ]
    .concat();
    fs::write(root.path().join("expected"), &binding).unwrap();
    let suffix = fs::read(backup.join("suffix")).unwrap();
    let base = fs::read(backup.join("candidate/base")).unwrap();
    let old_log = fs::read(catalog.join("generation-a/records.log")).unwrap();
    let old_witness = fs::read(catalog.join("generation-a/commit.state")).unwrap();
    let old_selection = fs::read(catalog.join("active.retained")).unwrap();
    let destination = root.path().join("restored");
    let normal_entries = fs::read_dir(root.path()).unwrap().count();
    for limit in [1024, 2048, 3072] {
        assert!(base.len() < limit && limit < suffix.len());
        for ignore_signal in [false, true] {
            let mut command = kernel_file_limit_common::child_command(
                "kernel_retained_restore_child",
                ignore_signal,
            )
            .unwrap();
            command
                .env("EMDA_RETAINED_RESTORE_LIMIT_ROOT", root.path())
                .env("EMDA_COPY_LIMIT_BYTES", limit.to_string());
            let output = kernel_file_limit_common::output_with_timeout(&mut command).unwrap();
            let details = format!("limit={limit} ignore_signal={ignore_signal} {output:?}");
            assert!(
                String::from_utf8_lossy(&output.stdout).contains(READY),
                "{details}"
            );
            if ignore_signal {
                assert!(output.status.success(), "{details}");
            } else {
                assert_eq!(
                    output.status.signal(),
                    Some(Signal::XFSZ.as_raw()),
                    "{details}"
                );
            }
            assert!(!destination.exists(), "{details}");
            cleanup_abandoned_staging(root.path()).unwrap();
            assert_eq!(
                fs::read_dir(root.path()).unwrap().count(),
                normal_entries,
                "{details}"
            );
            assert_eq!(fs::read(backup.join("suffix")).unwrap(), suffix);
            assert_eq!(fs::read(backup.join("candidate/base")).unwrap(), base);
            assert_eq!(fs::read(root.path().join("expected")).unwrap(), binding);
            assert_eq!(
                fs::read(catalog.join("generation-a/records.log")).unwrap(),
                old_log
            );
            assert_eq!(
                fs::read(catalog.join("generation-a/commit.state")).unwrap(),
                old_witness
            );
            assert_eq!(
                fs::read(catalog.join("active.retained")).unwrap(),
                old_selection
            );
        }
    }

    let generation =
        ManagedRetainedStore::restore_backup(&backup, &destination, RETAINED, &expected).unwrap();
    let restored = ManagedRetainedStore::open(&destination, RETAINED).unwrap();
    assert_eq!(restored.generation_id().unwrap(), generation);
    assert_eq!(
        restored.scan(None, None, 8).unwrap(),
        store.scan(None, None, 8).unwrap()
    );
}

#[test]
fn kernel_file_limit_copy_child() {
    let Some(root) = std::env::var_os("EMDA_COPY_LIMIT_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let operation = std::env::var("EMDA_COPY_LIMIT_OPERATION").unwrap();
    let mut source = if operation == "restore" {
        None
    } else {
        Some(retry_lock(|| Store::open(root.join("source"))).unwrap())
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
        "retained-cutover" => Catalog::cutover_from_store(
            source.take().unwrap(),
            root.join("source"),
            EXPORT,
            RETAINED,
        )
        .map(|_| ())
        .map_err(|error| match error {
            PublicationFailure::NotPublished(error) => error,
            other => panic!("cutover published before its export drained: {other:?}"),
        }),
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
    } else if operation == "retained-cutover" {
        verify(&Store::open(root.join("source")).unwrap());
    }
}

#[test]
fn file_size_failures_never_publish_partial_copies_and_allow_retry() {
    let mut outcomes = [0_usize; 2];
    for format in [Format::V2, Format::V3] {
        for operation in [
            Operation::Backup,
            Operation::Restore,
            Operation::Upgrade,
            Operation::RetainedCutover,
        ] {
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
                    let store = retry_lock(|| Store::open(&source)).unwrap();
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
                    let mut command = kernel_file_limit_common::child_command(
                        "kernel_file_limit_copy_child",
                        ignore_signal,
                    )
                    .unwrap();
                    command
                        .env("EMDA_COPY_LIMIT_ROOT", root.path())
                        .env("EMDA_COPY_LIMIT_OPERATION", operation.name())
                        .env("EMDA_COPY_LIMIT_BYTES", limit.to_string());
                    let output =
                        kernel_file_limit_common::output_with_timeout(&mut command).unwrap();
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
                            let store = retry_lock(|| Store::open(&source)).unwrap();
                            store.backup(&retry).unwrap();
                            drop(store);
                            let restored = root.path().join("restored");
                            Store::restore(&retry, &restored).unwrap();
                            verify(&retry_lock(|| Store::open(&restored)).unwrap());
                        }
                        Operation::Restore => {
                            Store::restore(&backup, &retry).unwrap();
                            verify(&retry_lock(|| Store::open(&retry)).unwrap());
                        }
                        Operation::Upgrade => {
                            let store = retry_lock(|| Store::open(&source)).unwrap();
                            store.upgrade_format(&retry).unwrap();
                            drop(store);
                            verify(&retry_lock(|| Store::open(&retry)).unwrap());
                        }
                        Operation::RetainedCutover => {
                            // This fixture's export fits in the output buffer
                            // but exceeds the kernel file limit at the drain.
                            cleanup_abandoned_staging(&source).unwrap();
                            assert_eq!(fs::read_dir(&source).unwrap().count(), 3, "{context}");
                            let store = Store::open(&source).unwrap();
                            let initial =
                                Catalog::cutover_from_store(store, &source, EXPORT, RETAINED)
                                    .unwrap();
                            let mut authority =
                                FileRetainedAuthority::create(root.path().join("authority"))
                                    .unwrap();
                            assert!(authority.compare_and_swap(None, initial).unwrap());
                            let mut reopened = Catalog::open(&source, RETAINED, authority).unwrap();
                            assert_eq!(
                                reopened.get(b"safe", 12).unwrap().as_deref(),
                                Some(b"acknowledged".as_slice())
                            );
                            assert_eq!(
                                reopened
                                    .get(b"large", LARGE_VALUE.len())
                                    .unwrap()
                                    .as_deref(),
                                Some(LARGE_VALUE)
                            );
                            assert_eq!(fs::read(&backup).unwrap(), backup_before, "{context}");
                            continue;
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
