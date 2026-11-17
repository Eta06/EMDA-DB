//! Large DB02 tail adoption must not publish a partial or rejected batch.
#![cfg(target_os = "linux")]

mod support;
use database_engine::{BatchOperation, CheckpointStore, Store};
use rustix::fs::{XattrFlags, getxattr, setxattr};
use std::fs;
use std::path::Path;
use support::retry_lock;

const XATTR: &str = "user.db.commit.v3";

fn witness(path: &Path, v3: bool) -> Vec<u8> {
    if v3 {
        let mut bytes = [0; 36];
        let n = getxattr(path.join("records.log"), XATTR, &mut bytes[..]).unwrap();
        bytes[..n].to_vec()
    } else {
        fs::read(path.join("commit.state")).unwrap()
    }
}

fn set_witness(path: &Path, v3: bool, bytes: &[u8]) {
    if v3 {
        setxattr(path.join("records.log"), XATTR, bytes, XattrFlags::REPLACE).unwrap();
    } else {
        fs::write(path.join("commit.state"), bytes).unwrap();
    }
}

#[test]
fn large_unacknowledged_batch_is_adopted_or_rejected_without_advancing_witness() {
    for v3 in [false, true] {
        for checkpoint_mode in [false, true] {
            for scenario in ["valid", "corrupt", "truncated", "quota"] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("store");
                let checkpoint = dir.path().join("checkpoint");
                if !v3 {
                    fs::create_dir(&path).unwrap();
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
                store.put(b"base", b"acknowledged").unwrap();
                let digest = store.checkpoint_to(&checkpoint).unwrap();
                let previous = witness(&path, v3);
                let large = vec![0x75; 65_600];
                store
                    .write_batch(&[
                        BatchOperation::Put {
                            key: b"large",
                            value: &large,
                        },
                        BatchOperation::Put {
                            key: b"second",
                            value: b"value",
                        },
                    ])
                    .unwrap();
                drop(store);
                set_witness(&path, v3, &previous);
                let log_path = path.join("records.log");
                let mut bytes = fs::read(&log_path).unwrap();
                if scenario == "corrupt" {
                    let start = bytes
                        .windows(large.len())
                        .position(|window| window == large)
                        .unwrap();
                    bytes[start + 65_536] ^= 1;
                } else if scenario == "truncated" {
                    bytes.pop();
                }
                if scenario == "corrupt" || scenario == "truncated" {
                    fs::write(&log_path, &bytes).unwrap();
                }
                let rejected = if checkpoint_mode {
                    match retry_lock(|| {
                        CheckpointStore::open_with_limits(
                            &path,
                            &checkpoint,
                            &digest,
                            1_000_000,
                            if scenario == "quota" { 1 } else { 8 },
                            1_000,
                        )
                    }) {
                        Ok(store) => {
                            assert_eq!(scenario, "valid");
                            assert_eq!(store.get(b"large").unwrap(), Some(large.clone()));
                            assert_eq!(
                                store.get(b"second").unwrap().as_deref(),
                                Some(&b"value"[..])
                            );
                            false
                        }
                        Err(error) => {
                            assert_ne!(scenario, "valid", "{error}");
                            assert_eq!(
                                error.kind(),
                                if scenario == "quota" {
                                    std::io::ErrorKind::InvalidInput
                                } else {
                                    std::io::ErrorKind::InvalidData
                                },
                                "{error}"
                            );
                            true
                        }
                    }
                } else {
                    match retry_lock(|| {
                        Store::open_with_resource_limits(
                            &path,
                            None,
                            if scenario == "quota" { Some(2) } else { None },
                            None,
                        )
                    }) {
                        Ok(store) => {
                            assert_eq!(scenario, "valid");
                            assert_eq!(store.get(b"large").unwrap(), Some(large.clone()));
                            assert_eq!(
                                store.get(b"second").unwrap().as_deref(),
                                Some(&b"value"[..])
                            );
                            false
                        }
                        Err(error) => {
                            assert_ne!(scenario, "valid", "{error}");
                            assert_eq!(
                                error.kind(),
                                if scenario == "quota" {
                                    std::io::ErrorKind::InvalidInput
                                } else {
                                    std::io::ErrorKind::InvalidData
                                },
                                "{error}"
                            );
                            true
                        }
                    }
                };
                assert_eq!(fs::read(&log_path).unwrap(), bytes);
                if rejected {
                    assert_eq!(
                        witness(&path, v3),
                        previous,
                        "v3={v3} checkpoint={checkpoint_mode} scenario={scenario}"
                    );
                } else {
                    assert_ne!(witness(&path, v3), previous);
                    let reopened = retry_lock(|| Store::open(&path)).unwrap();
                    assert_eq!(
                        reopened.get(b"base").unwrap().as_deref(),
                        Some(&b"acknowledged"[..])
                    );
                    assert_eq!(reopened.get(b"large").unwrap(), Some(large));
                    assert_eq!(
                        reopened.get(b"second").unwrap().as_deref(),
                        Some(&b"value"[..])
                    );
                }
            }
        }
    }
}
