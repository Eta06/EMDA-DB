//! Real kernel ENOSPC on an exclusively owned, tiny disposable tmpfs.
//! Run only through the documented disposable-container procedure.
#![cfg(target_os = "linux")]

use database_engine::{CheckpointStore, Store, cleanup_abandoned_staging};
use rustix::fs::{getxattr, statfs};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const TMPFS_MAGIC: i64 = 0x0102_1994;
const XATTR: &str = "user.db.commit.v3";
const LARGE_VALUE: &[u8] = &[0x73; 256 * 1024];

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

#[derive(Clone, Copy, Debug)]
enum CopyOperation {
    Backup,
    Restore,
    Upgrade,
}

fn disposable_root() -> PathBuf {
    assert_eq!(
        std::env::var("EMDA_ENOSPC_CONFIRM").as_deref(),
        Ok("disposable-tmpfs-only")
    );
    let root =
        PathBuf::from(std::env::var_os("EMDA_ENOSPC_ROOT").expect("missing disposable tmpfs root"));
    let root = root.canonicalize().unwrap();
    let info = statfs(&root).unwrap();
    assert_eq!(info.f_type as i64, TMPFS_MAGIC, "test requires tmpfs");
    let capacity = info.f_blocks.checked_mul(info.f_bsize as u64).unwrap();
    assert!(
        (2 * 1024 * 1024..=16 * 1024 * 1024).contains(&capacity),
        "unexpected tmpfs capacity"
    );
    assert_eq!(
        fs::read_dir(&root).unwrap().count(),
        0,
        "tmpfs must be empty and exclusive"
    );
    root
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

fn prepare_source(path: &Path, format: Format) -> Store {
    fs::create_dir(path).unwrap();
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
    let store = Store::open(path).unwrap();
    store.put(b"safe", b"acknowledged").unwrap();
    assert_eq!(
        &fs::read(path.join("records.log")).unwrap()[..8],
        match format {
            Format::V2 => b"EMDALOG2",
            Format::V3 => b"EMDALOG3",
        }
    );
    store
}

// The enclosing container has a dedicated tmpfs. The guard above prevents this
// loop from filling ordinary workspace storage or a shared large tmpfs.
fn fill_until_enospc(path: &Path) -> usize {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    let block = [0x7d; 64 * 1024];
    let mut total = 0;
    loop {
        match file.write(&block) {
            Ok(0) => panic!("filler wrote zero bytes before ENOSPC"),
            Ok(count) => total += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.raw_os_error() == Some(28) => break,
            Err(error) => panic!("unexpected filler error: {error}"),
        }
    }
    assert!(total > 0);
    drop(file);
    total
}

fn assert_enospc(error: &io::Error) {
    let expected = io::Error::from_raw_os_error(28).to_string();
    assert!(
        error.raw_os_error() == Some(28) || error.to_string().contains(&expected),
        "unexpected error: {error:?}"
    );
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

fn check_write(root: &Path, format: Format, writer: Writer) {
    let case = tempfile::tempdir_in(root).unwrap();
    let source = case.path().join("source");
    let checkpoint = case.path().join("checkpoint");
    let store = prepare_source(&source, format);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = Engine::open(&source, &checkpoint, &digest, writer).unwrap();
    let log_path = source.join("records.log");
    let before = fs::read(&log_path).unwrap();
    let marker = witness(&source, format);
    let anchor = fs::read(source.join("commit.state")).unwrap();
    let filler = case.path().join("filler");
    let filled = fill_until_enospc(&filler);
    let error = store.put(b"failed", &vec![0x4a; 16 * 1024]).unwrap_err();
    assert_enospc(&error);
    assert!(store.get(b"safe").is_err());
    let after = fs::read(&log_path).unwrap();
    assert!(after.len() >= before.len() && after.len() < before.len() + 16 * 1024);
    assert_eq!(&after[..before.len()], before);
    assert_eq!(witness(&source, format), marker);
    assert_eq!(fs::read(source.join("commit.state")).unwrap(), anchor);
    fs::remove_file(filler).unwrap();
    drop(store);
    for recovery in [Writer::Ordinary, Writer::Checkpoint] {
        match Engine::open(&source, &checkpoint, &digest, recovery) {
            Ok(reopened) if after.len() == before.len() => {
                assert_eq!(
                    reopened.get(b"safe").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                assert_eq!(reopened.get(b"failed").unwrap(), None);
            }
            Err(error)
                if after.len() > before.len() && error.kind() == io::ErrorKind::InvalidData => {}
            Ok(_) => panic!("partial record unexpectedly reopened"),
            Err(error) => panic!("unexpected recovery error: {error:?}"),
        }
        assert_eq!(fs::read(&log_path).unwrap(), after);
        assert_eq!(witness(&source, format), marker);
        assert_eq!(fs::read(source.join("commit.state")).unwrap(), anchor);
    }
    eprintln!(
        "write {format:?}/{writer:?}: filler={filled} appended={}",
        after.len() - before.len()
    );
}

fn verify_copy(path: &Path) {
    let store = Store::open(path).unwrap();
    assert_eq!(
        store.get(b"safe").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(store.get(b"large").unwrap().as_deref(), Some(LARGE_VALUE));
}

fn check_copy(root: &Path, format: Format, operation: CopyOperation) {
    let case = tempfile::tempdir_in(root).unwrap();
    let source = case.path().join("source");
    let backup = case.path().join("backup");
    let reserve = case.path().join("reserve");
    let filler = case.path().join("filler");
    let store = prepare_source(&source, format);
    store.put(b"large", LARGE_VALUE).unwrap();
    store.backup(&backup).unwrap();
    drop(store);
    let log_path = source.join("records.log");
    let before = fs::read(&log_path).unwrap();
    let marker = witness(&source, format);
    let anchor = fs::read(source.join("commit.state")).unwrap();
    let backup_before = fs::read(&backup).unwrap();
    fs::write(&reserve, vec![0x52; 64 * 1024]).unwrap();
    let filled = fill_until_enospc(&filler);
    fs::remove_file(reserve).unwrap();
    let failed = case.path().join("failed");
    let result = match operation {
        CopyOperation::Backup => Store::open(&source).unwrap().backup(&failed),
        CopyOperation::Restore => Store::restore(&backup, &failed),
        CopyOperation::Upgrade => Store::open(&source).unwrap().upgrade_format(&failed),
    };
    assert_enospc(&result.unwrap_err());
    assert!(!failed.exists());
    assert_eq!(fs::read(&log_path).unwrap(), before);
    assert_eq!(fs::read(source.join("commit.state")).unwrap(), anchor);
    assert_eq!(witness(&source, format), marker);
    assert_eq!(fs::read(&backup).unwrap(), backup_before);
    fs::remove_file(filler).unwrap();
    cleanup_abandoned_staging(case.path()).unwrap();
    let retry = case.path().join("retry");
    match operation {
        CopyOperation::Backup => {
            Store::open(&source).unwrap().backup(&retry).unwrap();
            let restored = case.path().join("restored");
            Store::restore(&retry, &restored).unwrap();
            verify_copy(&restored);
        }
        CopyOperation::Restore => {
            Store::restore(&backup, &retry).unwrap();
            verify_copy(&retry);
        }
        CopyOperation::Upgrade => {
            Store::open(&source)
                .unwrap()
                .upgrade_format(&retry)
                .unwrap();
            verify_copy(&retry);
        }
    }
    assert_eq!(fs::read(&log_path).unwrap(), before);
    assert_eq!(fs::read(source.join("commit.state")).unwrap(), anchor);
    assert_eq!(witness(&source, format), marker);
    assert_eq!(fs::read(&backup).unwrap(), backup_before);
    eprintln!("copy {format:?}/{operation:?}: filler={filled} failed safely and retry verified");
}

#[test]
#[ignore = "requires an exclusively owned, tiny disposable tmpfs; see docs/FOUNDATION_FAILURE_MATRIX.md"]
fn actual_tmpfs_enospc_preserves_source_and_recovery_evidence() {
    let root = disposable_root();
    for format in [Format::V2, Format::V3] {
        for writer in [Writer::Ordinary, Writer::Checkpoint] {
            check_write(&root, format, writer);
        }
        for operation in [
            CopyOperation::Backup,
            CopyOperation::Restore,
            CopyOperation::Upgrade,
        ] {
            check_copy(&root, format, operation);
        }
    }
    assert_eq!(fs::read_dir(root).unwrap().count(), 0);
}
