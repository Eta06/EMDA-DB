//! End-to-end checks for the independently synchronized commit boundary.
use database_engine::Store;
use std::fs::{self, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use tempfile::tempdir;

// Keep the full V2 regression suite against persistent pre-optimization files.
fn open_v2(path: impl AsRef<Path>) -> io::Result<Store> {
    let path = path.as_ref();
    if !path.join("records.log").exists()
        && !path.join("store.lock").exists()
        && !path.join("commit.state").exists()
    {
        fs::create_dir_all(path)?;
        fs::write(
            path.join("records.log"),
            include_bytes!("fixtures/v2-empty-records.log"),
        )?;
        fs::write(
            path.join("commit.state"),
            include_bytes!("fixtures/v2-empty-commit.state"),
        )?;
    }
    Store::open(path)
}

fn copy_store(source: &Path, destination: &Path) {
    fs::copy(source.join("records.log"), destination.join("records.log")).unwrap();
    fs::copy(
        source.join("commit.state"),
        destination.join("commit.state"),
    )
    .unwrap();
}

fn invalid(store: io::Result<Store>) {
    assert!(matches!(store, Err(ref error) if error.kind() == io::ErrorKind::InvalidData));
}

#[test]
fn truncation_at_every_acknowledged_record_boundary_is_detected_without_repair() {
    let source = tempdir().unwrap();
    let mut boundaries = vec![0];
    {
        let store = open_v2(source.path()).unwrap();
        boundaries.push(
            fs::metadata(source.path().join("records.log"))
                .unwrap()
                .len(),
        );
        for index in 0..4 {
            store.put(&[index + 1], &[index; 32]).unwrap();
            boundaries.push(
                fs::metadata(source.path().join("records.log"))
                    .unwrap()
                    .len(),
            );
        }
    }
    boundaries.pop(); // Keep only shorter, previously valid boundaries.
    for boundary in boundaries {
        let dir = tempdir().unwrap();
        copy_store(source.path(), dir.path());
        let path = dir.path().join("records.log");
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(boundary)
            .unwrap();
        let log_before = fs::read(&path).unwrap();
        let witness_before = fs::read(dir.path().join("commit.state")).unwrap();
        invalid(open_v2(dir.path()));
        assert_eq!(fs::read(&path).unwrap(), log_before);
        assert_eq!(
            fs::read(dir.path().join("commit.state")).unwrap(),
            witness_before
        );
    }
}

#[test]
fn missing_corrupt_truncated_or_wrong_witness_is_rejected() {
    let source = tempdir().unwrap();
    {
        let store = open_v2(source.path()).unwrap();
        store.put(b"key", b"value").unwrap();
    }
    let original = fs::read(source.path().join("commit.state")).unwrap();
    for index in 0..original.len() {
        let dir = tempdir().unwrap();
        copy_store(source.path(), dir.path());
        let path = dir.path().join("commit.state");
        let mut damaged = original.clone();
        damaged[index] ^= 1;
        fs::write(&path, &damaged).unwrap();
        invalid(open_v2(dir.path()));
        assert_eq!(fs::read(&path).unwrap(), damaged);
    }
    for length in (0..original.len()).chain([original.len() + 1]) {
        let dir = tempdir().unwrap();
        copy_store(source.path(), dir.path());
        let path = dir.path().join("commit.state");
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(length as u64)
            .unwrap();
        invalid(open_v2(dir.path()));
    }
    let dir = tempdir().unwrap();
    copy_store(source.path(), dir.path());
    fs::remove_file(dir.path().join("commit.state")).unwrap();
    invalid(open_v2(dir.path()));

    // A valid checksum cannot make a boundary inside a record acceptable.
    let dir = tempdir().unwrap();
    copy_store(source.path(), dir.path());
    let path = dir.path().join("commit.state");
    let mut damaged = original;
    damaged[24..32].copy_from_slice(&29_u64.to_le_bytes());
    let checksum = crc32fast::hash(&damaged[..32]);
    damaged[32..].copy_from_slice(&checksum.to_le_bytes());
    fs::write(path, damaged).unwrap();
    invalid(open_v2(dir.path()));
}

#[test]
fn witness_from_another_database_cannot_be_substituted() {
    let first = tempdir().unwrap();
    let second = tempdir().unwrap();
    for dir in [&first, &second] {
        let store = open_v2(dir.path()).unwrap();
        store.put(b"same", b"value").unwrap();
    }
    let witness_path = second.path().join("commit.state");
    let mut different = fs::read(&witness_path).unwrap();
    different[8] ^= 1;
    let checksum = crc32fast::hash(&different[..32]);
    different[32..].copy_from_slice(&checksum.to_le_bytes());
    fs::write(witness_path, different).unwrap();
    fs::copy(
        second.path().join("commit.state"),
        first.path().join("commit.state"),
    )
    .unwrap();
    invalid(open_v2(first.path()));
}

#[test]
fn single_byte_mutations_throughout_a_versioned_log_fail_without_modification() {
    let source = tempdir().unwrap();
    {
        let store = open_v2(source.path()).unwrap();
        store.put(b"alpha", b"first value").unwrap();
        store.put(b"beta", b"second value").unwrap();
        store.delete(b"alpha").unwrap();
    }
    let original = fs::read(source.path().join("records.log")).unwrap();
    for offset in 0..original.len() {
        let dir = tempdir().unwrap();
        copy_store(source.path(), dir.path());
        let path = dir.path().join("records.log");
        let mut damaged = original.clone();
        damaged[offset] ^= 1;
        fs::write(&path, &damaged).unwrap();
        invalid(open_v2(dir.path()));
        assert_eq!(fs::read(path).unwrap(), damaged, "byte {offset}");
    }
}

#[test]
fn crash_boundary_model_never_loses_a_prior_acknowledged_value_silently() {
    let source = tempdir().unwrap();
    let (old_log, old_witness, new_log, new_witness);
    {
        let store = open_v2(source.path()).unwrap();
        store.put(b"acknowledged", b"one").unwrap();
        old_log = fs::read(source.path().join("records.log")).unwrap();
        old_witness = fs::read(source.path().join("commit.state")).unwrap();
        store.put(b"next", b"two").unwrap();
        new_log = fs::read(source.path().join("records.log")).unwrap();
        new_witness = fs::read(source.path().join("commit.state")).unwrap();
    }
    let partial_log = &new_log[..new_log.len() - 1];
    for persisted_log in [old_log.as_slice(), new_log.as_slice(), partial_log] {
        for split in 0..=new_witness.len() {
            // Model a torn replacement: the new prefix reached storage and
            // the remaining bytes still contain the previous witness.
            let mut persisted_witness = old_witness.clone();
            persisted_witness[..split].copy_from_slice(&new_witness[..split]);
            let dir = tempdir().unwrap();
            fs::write(dir.path().join("records.log"), persisted_log).unwrap();
            fs::write(dir.path().join("commit.state"), &persisted_witness).unwrap();
            let opened = open_v2(dir.path());
            if let Ok(store) = opened {
                assert_ne!(
                    persisted_log, partial_log,
                    "partial log accepted at witness split {split}"
                );
                assert_eq!(
                    store.get(b"acknowledged").unwrap().as_deref(),
                    Some(&b"one"[..])
                );
                let expected = if persisted_log == new_log {
                    Some(&b"two"[..])
                } else {
                    None
                };
                assert_eq!(store.get(b"next").unwrap().as_deref(), expected);
            } else if split == new_witness.len() && persisted_log == new_log {
                panic!("fully synchronized state did not reopen");
            }
            assert_eq!(
                fs::read(dir.path().join("records.log")).unwrap(),
                persisted_log
            );
        }
    }
}

#[test]
fn complete_unacknowledged_tail_is_synced_and_adopted_before_reopen() {
    let dir = tempdir().unwrap();
    let old_witness;
    let old_len;
    {
        let store = open_v2(dir.path()).unwrap();
        store.put(b"first", b"one").unwrap();
        old_witness = fs::read(dir.path().join("commit.state")).unwrap();
        old_len = fs::metadata(dir.path().join("records.log")).unwrap().len();
        store.put(b"second", b"two").unwrap();
    }
    // Model a crash after the second complete log append but before publishing
    // its witness; an unacknowledged complete record may be recovered.
    fs::write(dir.path().join("commit.state"), &old_witness).unwrap();
    let reopened = open_v2(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"second").unwrap().as_deref(),
        Some(&b"two"[..])
    );
    drop(reopened);
    assert_ne!(
        fs::read(dir.path().join("commit.state")).unwrap(),
        old_witness
    );
    OpenOptions::new()
        .write(true)
        .open(dir.path().join("records.log"))
        .unwrap()
        .set_len(old_len)
        .unwrap();
    invalid(open_v2(dir.path()));
}

#[test]
fn tampering_with_the_live_log_or_witness_blocks_writes_and_backups() {
    for damage_witness in [false, true] {
        let dir = tempdir().unwrap();
        let store = open_v2(dir.path()).unwrap();
        store.put(b"first", b"one").unwrap();
        let earlier = fs::metadata(dir.path().join("records.log")).unwrap().len();
        store.put(b"second", b"two").unwrap();
        if damage_witness {
            let mut file = OpenOptions::new()
                .write(true)
                .open(dir.path().join("commit.state"))
                .unwrap();
            file.seek(SeekFrom::Start(3)).unwrap();
            file.write_all(b"x").unwrap();
        } else {
            OpenOptions::new()
                .write(true)
                .open(dir.path().join("records.log"))
                .unwrap()
                .set_len(earlier)
                .unwrap();
        }
        let backup = dir.path().join("snapshot");
        assert!(store.backup(&backup).is_err());
        assert!(!backup.exists());
        assert!(store.put(b"later", b"value").is_err());
        assert!(store.get(b"first").is_err());
    }
}

#[test]
fn restored_backup_has_a_witness_and_detects_later_log_truncation() {
    let dir = tempdir().unwrap();
    let store = open_v2(dir.path().join("source")).unwrap();
    store.put(b"first", b"one").unwrap();
    store.put(b"second", b"two").unwrap();
    let backup = dir.path().join("backup");
    store.backup(&backup).unwrap();
    let restored = dir.path().join("restored");
    Store::restore(&backup, &restored).unwrap();
    assert!(restored.join("commit.state").exists());
    let log = OpenOptions::new()
        .write(true)
        .open(restored.join("records.log"))
        .unwrap();
    log.set_len(28).unwrap();
    invalid(open_v2(restored));
}

#[test]
fn legacy_db01_log_remains_usable_and_backup_restores_legacy_format() {
    let dir = tempdir().unwrap();
    let legacy = dir.path().join("legacy");
    fs::create_dir(&legacy).unwrap();
    fs::write(
        legacy.join("records.log"),
        include_bytes!("fixtures/db01-legacy-record.bin"),
    )
    .unwrap();
    let store = open_v2(&legacy).unwrap();
    assert_eq!(
        store.get(b"legacy").unwrap().as_deref(),
        Some(&b"persisted"[..])
    );
    store.put(b"new", b"value").unwrap();
    assert!(!legacy.join("commit.state").exists());
    let backup = dir.path().join("backup");
    store.backup(&backup).unwrap();
    let restored = dir.path().join("restored");
    Store::restore(&backup, &restored).unwrap();
    assert!(!restored.join("commit.state").exists());
    let store = open_v2(restored).unwrap();
    assert_eq!(
        store.get(b"legacy").unwrap().as_deref(),
        Some(&b"persisted"[..])
    );
    assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
}

#[test]
fn incomplete_unacknowledged_tail_is_rejected_without_changing_log_or_witness() {
    let dir = tempdir().unwrap();
    {
        let store = open_v2(dir.path()).unwrap();
        store.put(b"first", b"one").unwrap();
    }
    let log_path = dir.path().join("records.log");
    OpenOptions::new()
        .append(true)
        .open(&log_path)
        .unwrap()
        .write_all(b"DB01\x01")
        .unwrap();
    let log_before = fs::read(&log_path).unwrap();
    let witness_before = fs::read(dir.path().join("commit.state")).unwrap();
    invalid(open_v2(dir.path()));
    assert_eq!(fs::read(log_path).unwrap(), log_before);
    assert_eq!(
        fs::read(dir.path().join("commit.state")).unwrap(),
        witness_before
    );
}

#[test]
fn legacy_upgrade_is_a_verified_non_overwriting_copy_with_a_new_boundary() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(
        source.join("records.log"),
        include_bytes!("fixtures/db01-legacy-record.bin"),
    )
    .unwrap();
    let store = open_v2(&source).unwrap();
    store.put(b"second", b"value").unwrap();
    let original = fs::read(source.join("records.log")).unwrap();
    let target = dir.path().join("upgraded");
    store.upgrade_legacy(&target).unwrap();
    assert_eq!(fs::read(source.join("records.log")).unwrap(), original);
    assert!(!source.join("commit.state").exists());
    let upgraded = open_v2(&target).unwrap();
    assert_eq!(
        upgraded.get(b"legacy").unwrap().as_deref(),
        Some(&b"persisted"[..])
    );
    assert_eq!(
        upgraded.get(b"second").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    assert_eq!(
        upgraded
            .upgrade_legacy(dir.path().join("again"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    drop(upgraded);
    assert_eq!(
        store.upgrade_legacy(&target).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    OpenOptions::new()
        .write(true)
        .open(target.join("records.log"))
        .unwrap()
        .set_len(28)
        .unwrap();
    invalid(open_v2(target));
}

#[test]
fn legacy_upgrade_refuses_corrupt_source_without_publishing_destination() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(
        source.join("records.log"),
        include_bytes!("fixtures/db01-legacy-record.bin"),
    )
    .unwrap();
    let store = open_v2(&source).unwrap();
    let mut log = OpenOptions::new()
        .write(true)
        .open(source.join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start(23)).unwrap();
    log.write_all(b"X").unwrap();
    let target = dir.path().join("upgraded");
    assert!(
        matches!(store.upgrade_legacy(&target), Err(ref error) if error.kind() == io::ErrorKind::InvalidData)
    );
    assert!(!target.exists());
}
