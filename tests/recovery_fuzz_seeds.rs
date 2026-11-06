//! Validate that the long recovery-fuzzer seeds reach successful replay.
#![cfg(target_os = "linux")]

use database_engine::{BatchOperation, CheckpointStore, Store};
use rustix::fs::{XattrFlags, getxattr, setxattr};
use std::fs::{self, OpenOptions};
use std::io::Write;

const XATTR: &str = "user.db.commit.v3";

fn verify_log_seed(data: &[u8], expected: &[(&[u8], &[u8])]) {
    assert!(data[0] & 0x80 != 0);
    let witness_len = usize::from(data[0] & 0x3f);
    let mut position = 1 + witness_len;
    let metadata = if data[0] & 0x40 != 0 {
        let len = usize::from(data[position]);
        position += 1;
        let bytes = &data[position..position + len];
        position += len;
        Some(bytes)
    } else {
        None
    };
    let root = tempfile::tempdir().unwrap();
    let log = root.path().join("records.log");
    fs::write(&log, &data[position..]).unwrap();
    fs::write(root.path().join("commit.state"), &data[1..1 + witness_len]).unwrap();
    if let Some(bytes) = metadata {
        setxattr(&log, XATTR, bytes, XattrFlags::empty()).unwrap();
    }
    let store = Store::open(root.path()).unwrap();
    for &(key, value) in expected {
        assert_eq!(store.get(key).unwrap().as_deref(), Some(value));
    }
}

#[test]
fn long_log_seeds_replay_as_valid_v2_and_v3_stores() {
    verify_log_seed(
        include_bytes!("../fuzz/seeds/long-v2-batches"),
        &[(b"alpha", b"one"), (b"beta", b"two")],
    );
    verify_log_seed(
        include_bytes!("../fuzz/seeds/long-v3-records"),
        &[(b"probe", b"fuzz value")],
    );
}

fn verify_checkpoint_seed(data: &[u8], metadata: bool) {
    assert_eq!(data.len(), 256);
    assert_eq!(data[0] & 0x89, if metadata { 0x89 } else { 0x09 });
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    fs::create_dir(&path).unwrap();
    if !metadata {
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
    let log = path.join("records.log");
    assert_eq!(
        &fs::read(&log).unwrap()[..8],
        if metadata { b"EMDALOG3" } else { b"EMDALOG2" }
    );
    store.put(b"base", b"persisted").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let witness = fs::read(path.join("commit.state")).unwrap();
    let mut old_metadata = [0_u8; 36];
    if metadata {
        assert_eq!(getxattr(&log, XATTR, &mut old_metadata[..]).unwrap(), 36);
    }
    store.put(b"tail", b"first").unwrap();
    store
        .write_batch(&[
            BatchOperation::Delete { key: b"base" },
            BatchOperation::Put {
                key: b"tail",
                value: b"updated",
            },
            BatchOperation::Put {
                key: b"new",
                value: b"value",
            },
        ])
        .unwrap();
    drop(store);
    if metadata {
        setxattr(&log, XATTR, &old_metadata, XattrFlags::REPLACE).unwrap();
    } else {
        fs::write(path.join("commit.state"), witness).unwrap();
    }
    OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(&data[4..])
        .unwrap();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    assert_eq!(store.get(b"base").unwrap(), None);
    assert_eq!(
        store.get(b"tail").unwrap().as_deref(),
        Some(&b"updated"[..])
    );
    assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
    assert_eq!(
        store.get(b"probe").unwrap().as_deref(),
        Some(&b"fuzz value"[..])
    );
    drop(store);
    let ordinary = Store::open(&path).unwrap();
    assert_eq!(
        ordinary.get(b"probe").unwrap().as_deref(),
        Some(&b"fuzz value"[..])
    );
}

#[test]
fn long_checkpoint_seeds_are_complete_adoptable_tails() {
    verify_checkpoint_seed(
        include_bytes!("../fuzz/checkpoint-recovery-seeds/long-v2-complete-tail"),
        false,
    );
    verify_checkpoint_seed(
        include_bytes!("../fuzz/checkpoint-recovery-seeds/long-v3-complete-tail"),
        true,
    );
}
