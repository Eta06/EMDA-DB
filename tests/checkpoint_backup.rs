use database_engine::{BatchOperation, CheckpointStore, Store};
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;

#[test]
fn checkpoint_store_backup_restores_a_verified_point_in_time() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"base", b"old").unwrap();
    ordinary.put(b"delete", b"old").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"base",
                value: b"new",
            },
            BatchOperation::Delete { key: b"delete" },
            BatchOperation::Put {
                key: b"add",
                value: b"value",
            },
        ])
        .unwrap();
    let rotated = dir.path().join("rotated-checkpoint");
    store.rollover_to(&rotated).unwrap();
    store.put(b"after-rollover", b"present").unwrap();
    let backup = dir.path().join("backup");
    store.backup(&backup).unwrap();
    store.put(b"base", b"later").unwrap();
    assert!(store.backup(&backup).is_err());
    let restored = dir.path().join("restored");
    Store::restore(&backup, &restored).unwrap();
    let copy = Store::open(&restored).unwrap();
    assert_eq!(copy.get(b"base").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(copy.get(b"delete").unwrap(), None);
    assert_eq!(copy.get(b"add").unwrap().as_deref(), Some(&b"value"[..]));
    assert_eq!(
        copy.get(b"after-rollover").unwrap().as_deref(),
        Some(&b"present"[..])
    );
    assert_eq!(store.get(b"base").unwrap().as_deref(), Some(&b"later"[..]));
}

#[test]
fn checkpoint_store_backup_rejects_corrupt_obsolete_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"key", b"old").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store.put(b"key", b"new").unwrap();
    let log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    let mut byte = [0];
    log.read_exact_at(&mut byte, 28).unwrap();
    byte[0] ^= 0x40;
    log.write_all_at(&byte, 28).unwrap();
    let destination = dir.path().join("bad-backup");
    assert!(store.backup(&destination).is_err());
    assert!(!destination.exists());
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
    drop(store);
    assert!(Store::open(&path).is_err());
    assert!(fs::metadata(path.join("records.log")).unwrap().len() > 28);
}
