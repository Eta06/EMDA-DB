use database_engine::Store;
use sha2::{Digest, Sha256};
use std::fs;
use std::io;

#[test]
fn interrupted_empty_unwitnessed_log_is_not_opened_as_legacy() {
    for existing_lock in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("records.log"), []).unwrap();
        if existing_lock {
            fs::write(path.join("store.lock"), []).unwrap();
        }
        let result = Store::open(&path);
        assert!(matches!(result, Err(ref error) if error.kind() == io::ErrorKind::InvalidData));
        assert_eq!(fs::read(path.join("records.log")).unwrap(), b"");
        assert!(!path.join("commit.state").exists());
    }
}

#[test]
fn every_incomplete_versioned_header_fails_closed_without_rewriting_it() {
    let v2 = *include_bytes!("fixtures/v2-empty-records.log");
    let mut v3 = v2;
    v3[..8].copy_from_slice(b"EMDALOG3");
    let checksum = crc32fast::hash(&v3[..24]);
    v3[24..].copy_from_slice(&checksum.to_le_bytes());
    for header in [v2, v3] {
        for length in 0..=header.len() {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            fs::create_dir(&path).unwrap();
            fs::write(path.join("store.lock"), []).unwrap();
            fs::write(path.join("records.log"), &header[..length]).unwrap();
            assert!(Store::open(&path).is_err(), "header prefix {length}");
            assert_eq!(
                fs::read(path.join("records.log")).unwrap(),
                header[..length]
            );
            assert!(!path.join("commit.state").exists());
        }
    }
}

#[test]
fn explicitly_upgrade_verified_empty_legacy_without_changing_source_log() {
    for existing_lock in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("records.log"), []).unwrap();
        if existing_lock {
            fs::write(source.join("store.lock"), []).unwrap();
        }
        Store::upgrade_empty_legacy(&source, &destination).unwrap();
        assert_eq!(fs::read(source.join("records.log")).unwrap(), b"");
        assert!(!source.join("commit.state").exists());
        let target = Store::open(&destination).unwrap();
        target.transaction(|tx| tx.put(b"key", b"value")).unwrap();
        drop(target);
        let recovered = Store::open(&destination).unwrap();
        assert_eq!(
            recovered.get(b"key").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}

#[test]
fn empty_legacy_upgrade_refuses_nonempty_missing_or_witnessed_sources() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("missing");
    let output = root.path().join("output");
    assert!(Store::upgrade_empty_legacy(&missing, &output).is_err());
    assert!(!missing.exists() && !output.exists());

    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    let legacy = include_bytes!("fixtures/db01-legacy-record.bin");
    fs::write(source.join("records.log"), legacy).unwrap();
    assert!(Store::upgrade_empty_legacy(&source, &output).is_err());
    assert_eq!(fs::read(source.join("records.log")).unwrap(), legacy);
    assert!(!output.exists());

    fs::write(source.join("records.log"), []).unwrap();
    fs::write(source.join("commit.state"), b"do not replace").unwrap();
    assert!(Store::upgrade_empty_legacy(&source, &output).is_err());
    assert_eq!(fs::read(source.join("records.log")).unwrap(), b"");
    assert_eq!(
        fs::read(source.join("commit.state")).unwrap(),
        b"do not replace"
    );
    assert!(!output.exists());
}

#[test]
fn empty_legacy_upgrade_respects_the_existing_store_lock() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let destination = root.path().join("destination");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("records.log"), []).unwrap();
    let lock = fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(source.join("store.lock"))
        .unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    assert_eq!(
        Store::upgrade_empty_legacy(&source, &destination)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(!destination.exists());
    assert_eq!(fs::read(source.join("records.log")).unwrap(), b"");
    fs2::FileExt::unlock(&lock).unwrap();
    Store::upgrade_empty_legacy(&source, &destination).unwrap();
    assert!(Store::open(&destination).is_ok());
}

#[test]
fn empty_legacy_upgrade_never_replaces_an_existing_destination() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let destination = root.path().join("destination");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("records.log"), []).unwrap();
    fs::create_dir(&destination).unwrap();
    fs::write(destination.join("sentinel"), b"keep").unwrap();
    assert_eq!(
        Store::upgrade_empty_legacy(&source, &destination)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(source.join("records.log")).unwrap(), b"");
    assert_eq!(fs::read(destination.join("sentinel")).unwrap(), b"keep");
}

#[test]
fn dangling_witness_entry_never_permits_creation_or_legacy_fallback() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    for mode in ["missing-log", "empty-log", "legacy-log"] {
        let path = root.path().join(mode);
        let output = root.path().join(format!("{mode}-upgrade"));
        fs::create_dir(&path).unwrap();
        if mode == "empty-log" {
            fs::write(path.join("records.log"), []).unwrap();
        } else if mode == "legacy-log" {
            fs::write(
                path.join("records.log"),
                include_bytes!("fixtures/db01-legacy-record.bin"),
            )
            .unwrap();
        }
        symlink("missing-witness", path.join("commit.state")).unwrap();
        let before = fs::read(path.join("records.log")).ok();
        assert!(
            matches!(Store::open(&path), Err(ref error) if error.kind() == io::ErrorKind::InvalidData)
        );
        assert_eq!(fs::read(path.join("records.log")).ok(), before);
        assert!(
            fs::symlink_metadata(path.join("commit.state"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        if mode == "empty-log" {
            assert!(Store::upgrade_empty_legacy(&path, &output).is_err());
            assert!(!output.exists());
            assert_eq!(fs::read(path.join("records.log")).unwrap(), b"");
        }
        if mode == "legacy-log" {
            fs::remove_file(path.join("commit.state")).unwrap();
            let legacy = Store::open(&path).unwrap();
            assert_eq!(
                legacy.get(b"legacy").unwrap().as_deref(),
                Some(&b"persisted"[..])
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn empty_legacy_upgrade_refuses_existing_metadata_witness() {
    use rustix::fs::{XattrFlags, getxattr, setxattr};
    use rustix::io::Errno;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let destination = root.path().join("destination");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("records.log"), []).unwrap();
    match setxattr(
        source.join("records.log"),
        "user.db.commit.v3",
        b"existing",
        XattrFlags::empty(),
    ) {
        Ok(()) => {}
        Err(Errno::NOTSUP | Errno::NOSYS) => return,
        Err(error) => panic!("set metadata witness: {error}"),
    }
    assert_eq!(
        Store::upgrade_empty_legacy(&source, &destination)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert!(!destination.exists());
    assert_eq!(fs::read(source.join("records.log")).unwrap(), b"");
    let mut actual = [0_u8; 32];
    let length = getxattr(
        source.join("records.log"),
        "user.db.commit.v3",
        &mut actual[..],
    )
    .unwrap();
    assert_eq!(&actual[..length], b"existing");
}

#[test]
fn restoring_an_empty_legacy_backup_creates_a_witnessed_empty_store() {
    let root = tempfile::tempdir().unwrap();
    let backup = root.path().join("backup");
    let target = root.path().join("restored");
    let mut bytes = b"DBBKP001".to_vec();
    bytes.extend_from_slice(b"DBBEND01");
    bytes.extend_from_slice(&0_u64.to_le_bytes());
    bytes.extend_from_slice(&Sha256::digest([]));
    fs::write(&backup, &bytes).unwrap();
    let existing = root.path().join("existing");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("sentinel"), b"keep").unwrap();
    assert_eq!(
        Store::restore(&backup, &existing).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(existing.join("sentinel")).unwrap(), b"keep");
    Store::restore(&backup, &target).unwrap();
    assert_eq!(fs::read(&backup).unwrap(), bytes);
    assert!(fs::metadata(target.join("records.log")).unwrap().len() > 0);
    assert!(target.join("commit.state").exists());
    let store = Store::open(&target).unwrap();
    assert_eq!(store.get(b"key").unwrap(), None);
    store.transaction(|tx| tx.put(b"key", b"value")).unwrap();
    drop(store);
    assert_eq!(
        Store::open(&target)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"value"[..])
    );
}
