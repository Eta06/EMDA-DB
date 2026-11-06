//! V3 initialization and recovery must not accept a damaged identity anchor.
#![cfg(target_os = "linux")]

use database_engine::Store;
use rustix::fs::{XattrFlags, getxattr, setxattr};
use std::fs;
use std::io;
use std::path::Path;

const METADATA: &str = "user.db.commit.v3";

fn marker(path: &Path) -> Vec<u8> {
    let mut bytes = [0; 36];
    assert_eq!(
        getxattr(path.join("records.log"), METADATA, &mut bytes[..]).unwrap(),
        36
    );
    bytes.to_vec()
}

fn install(path: &Path, log: &[u8], anchor: Option<&[u8]>, boundary: &[u8]) {
    fs::write(path.join("records.log"), log).unwrap();
    setxattr(
        path.join("records.log"),
        METADATA,
        boundary,
        XattrFlags::empty(),
    )
    .unwrap();
    if let Some(anchor) = anchor {
        fs::write(path.join("commit.state"), anchor).unwrap();
    }
}

fn reject_without_modifying(path: &Path, name: &str) {
    let before_log = fs::read(path.join("records.log")).unwrap();
    let before_anchor = fs::read(path.join("commit.state")).ok();
    let before_boundary = marker(path);
    for _ in 0..2 {
        let result = Store::open(path);
        assert!(
            matches!(result, Err(ref error) if error.kind() == io::ErrorKind::InvalidData),
            "{name}: damaged anchor was accepted"
        );
        assert_eq!(
            fs::read(path.join("records.log")).unwrap(),
            before_log,
            "{name}"
        );
        assert_eq!(
            fs::read(path.join("commit.state")).ok(),
            before_anchor,
            "{name}"
        );
        assert_eq!(marker(path), before_boundary, "{name}");
    }
}

#[test]
fn missing_torn_corrupt_and_mismatched_v3_anchors_fail_closed() {
    let source = tempfile::tempdir().unwrap();
    let store = Store::open(source.path()).unwrap();
    store.put(b"acknowledged", b"value").unwrap();
    drop(store);
    let log = fs::read(source.path().join("records.log")).unwrap();
    assert_eq!(&log[..8], b"EMDALOG3", "test needs a V3-capable filesystem");
    let boundary = marker(source.path());
    let anchor = fs::read(source.path().join("commit.state")).unwrap();
    assert_eq!(anchor.len(), 36);

    let mut cases = vec![("missing".to_owned(), None)];
    for length in 0..anchor.len() {
        cases.push((format!("short-{length}"), Some(anchor[..length].to_vec())));
    }
    let mut longer = anchor.clone();
    longer.push(0);
    cases.push(("long".into(), Some(longer)));
    for index in 0..anchor.len() {
        let mut corrupt = anchor.clone();
        corrupt[index] ^= 1;
        cases.push((format!("bit-flip-{index}"), Some(corrupt)));
    }
    for field in ["magic", "identity", "offset"] {
        let mut corrupt = anchor.clone();
        match field {
            "magic" => corrupt[0] ^= 1,
            "identity" => corrupt[8] ^= 1,
            "offset" => corrupt[24..32].copy_from_slice(&29_u64.to_le_bytes()),
            _ => unreachable!(),
        }
        let checksum = crc32fast::hash(&corrupt[..32]);
        corrupt[32..].copy_from_slice(&checksum.to_le_bytes());
        cases.push((format!("valid-checksum-{field}"), Some(corrupt)));
    }
    for (name, anchor) in cases {
        let target = tempfile::tempdir().unwrap();
        install(target.path(), &log, anchor.as_deref(), &boundary);
        reject_without_modifying(target.path(), &name);
    }
}
