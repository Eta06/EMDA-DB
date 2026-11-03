use database_engine::Store;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

#[test]
fn backup_and_restore_preserve_a_point_in_time() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("live")).unwrap();
    store.put(b"name", b"before").unwrap();
    store.put(b"empty", b"").unwrap();
    store.put(b"removed", b"obsolete").unwrap();
    store.delete(b"removed").unwrap();
    let backup = dir.path().join("snapshot.dbbackup");
    store.backup(&backup).unwrap();
    // Backups are full-log copies, so deleted historical values remain present.
    assert!(
        fs::read(&backup)
            .unwrap()
            .windows(b"obsolete".len())
            .any(|window| window == b"obsolete")
    );
    store.put(b"name", b"after").unwrap();
    store.put(b"new", b"later").unwrap();

    let restored_path = dir.path().join("restored");
    Store::restore(&backup, &restored_path).unwrap();
    let restored = Store::open(restored_path).unwrap();
    assert_eq!(
        restored.get(b"name").unwrap().as_deref(),
        Some(&b"before"[..])
    );
    assert_eq!(restored.get(b"empty").unwrap().as_deref(), Some(&b""[..]));
    assert_eq!(restored.get(b"removed").unwrap(), None);
    assert_eq!(restored.get(b"new").unwrap(), None);
    restored.put(b"usable", b"yes").unwrap();
    drop(restored);
    assert_eq!(
        Store::open(dir.path().join("restored"))
            .unwrap()
            .get(b"usable")
            .unwrap()
            .as_deref(),
        Some(&b"yes"[..])
    );
}

#[test]
fn empty_store_backup_is_restorable() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("live")).unwrap();
    let backup = dir.path().join("empty.backup");
    store.backup(&backup).unwrap();
    let target = dir.path().join("restored");
    Store::restore(&backup, &target).unwrap();
    assert_eq!(Store::open(target).unwrap().get(b"missing").unwrap(), None);
}

#[test]
fn backup_refuses_to_overwrite_existing_file_or_symlink() {
    use std::os::unix::fs::symlink;
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("live")).unwrap();
    store.put(b"key", b"value").unwrap();
    let existing = dir.path().join("existing");
    fs::write(&existing, b"keep me").unwrap();
    assert_eq!(
        store.backup(&existing).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(&existing).unwrap(), b"keep me");
    let link = dir.path().join("link");
    symlink(&existing, &link).unwrap();
    assert_eq!(
        store.backup(&link).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(existing).unwrap(), b"keep me");
}

#[test]
fn restore_refuses_to_replace_an_existing_directory() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("live")).unwrap();
    let backup = dir.path().join("snapshot");
    store.backup(&backup).unwrap();
    let target = dir.path().join("existing");
    fs::create_dir(&target).unwrap();
    fs::write(target.join("sentinel"), b"keep me").unwrap();
    assert_eq!(
        Store::restore(&backup, &target).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(target.join("sentinel")).unwrap(), b"keep me");
    let empty = dir.path().join("empty-existing");
    fs::create_dir(&empty).unwrap();
    assert_eq!(
        Store::restore(&backup, &empty).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert!(fs::read_dir(empty).unwrap().next().is_none());
}

#[test]
fn corrupt_or_truncated_backup_is_rejected_before_publication() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("live")).unwrap();
    store.put(b"first", b"one").unwrap();
    store.put(b"second", b"two").unwrap();
    let backup = dir.path().join("snapshot");
    store.backup(&backup).unwrap();
    let original = fs::read(&backup).unwrap();
    let mut bad_header = original.clone();
    bad_header[0] ^= 1;
    let mut bad_data = original.clone();
    bad_data[30] ^= 1;
    let mut truncated = original.clone();
    truncated.pop();
    let mut missing_complete_record = original.clone();
    // Header (8) plus first record (17 header + 5 key + 3 value + 4 checksum).
    let first_record_end = 8 + 28 + 17 + 5 + 3 + 4;
    let footer_start = missing_complete_record.len() - 48;
    missing_complete_record.drain(first_record_end..footer_start);
    for (index, bytes) in [bad_header, bad_data, truncated, missing_complete_record]
        .iter()
        .enumerate()
    {
        let input = dir.path().join(format!("bad-{index}"));
        let target = dir.path().join(format!("target-{index}"));
        fs::write(&input, bytes).unwrap();
        assert_eq!(
            Store::restore(&input, &target).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert!(!target.exists());
    }
}

#[test]
fn backup_with_recomputed_outer_hash_still_checks_record_integrity() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("live")).unwrap();
    store.put(b"key", b"value").unwrap();
    let backup = dir.path().join("snapshot");
    store.backup(&backup).unwrap();
    let mut bytes = fs::read(&backup).unwrap();
    bytes[8 + 28 + 17 + 3] ^= 1;
    let footer = bytes.len() - 48;
    let digest = Sha256::digest(&bytes[8..footer]);
    bytes[footer + 16..].copy_from_slice(&digest);
    fs::write(&backup, bytes).unwrap();
    let target = dir.path().join("restored");
    assert_eq!(
        Store::restore(&backup, &target).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(!target.exists());
}

#[test]
fn corrupt_live_log_is_not_published_as_a_backup() {
    let dir = tempdir().unwrap();
    let live = dir.path().join("live");
    let store = Store::open(&live).unwrap();
    store.put(b"key", b"value").unwrap();
    let mut log = OpenOptions::new()
        .write(true)
        .open(live.join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start(28 + 20)).unwrap();
    log.write_all(b"X").unwrap();
    log.sync_all().unwrap();
    let backup = dir.path().join("snapshot");
    assert_eq!(
        store.backup(&backup).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(!backup.exists());
}

#[test]
fn backup_during_concurrent_updates_has_a_complete_value() {
    let dir = tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("live")).unwrap());
    store.put(b"shared", &vec![0; 4096]).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let writer_store = Arc::clone(&store);
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        for value in 1..=25 {
            writer_store.put(b"shared", &vec![value; 4096]).unwrap();
        }
    });
    barrier.wait();
    let backup = dir.path().join("snapshot");
    store.backup(&backup).unwrap();
    writer.join().unwrap();
    let target = dir.path().join("restored");
    Store::restore(&backup, &target).unwrap();
    let value = Store::open(target)
        .unwrap()
        .get(b"shared")
        .unwrap()
        .unwrap();
    assert_eq!(value.len(), 4096);
    assert!(value.iter().all(|byte| *byte == value[0]));
    assert!(value[0] <= 25);
}

#[test]
fn backup_rejects_corruption_beyond_the_recovery_buffer() {
    let dir = tempdir().unwrap();
    let live = dir.path().join("live");
    let store = Store::open(&live).unwrap();
    store.put(b"key", &vec![0x5a; 70_000]).unwrap();
    let mut log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(live.join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start((28 + 17 + 3 + 65_700) as u64))
        .unwrap();
    log.write_all(&[0x10]).unwrap();
    log.sync_all().unwrap();
    let destination = dir.path().join("snapshot");
    assert_eq!(
        store.backup(&destination).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(!destination.exists());
}
