//! V3 crash states, rollback detection, and compatibility of the one-fsync path.
#![cfg(target_os = "linux")]
use database_engine::Store;
use rustix::fs::{XattrFlags, getxattr, removexattr, setxattr};
use std::fs::{self, OpenOptions};
use std::io;
use std::path::Path;
use tempfile::tempdir;

const NAME: &str = "user.db.commit.v3";

fn marker(dir: &Path) -> Vec<u8> {
    let mut bytes = [0; 36];
    assert_eq!(
        getxattr(dir.join("records.log"), NAME, &mut bytes[..]).unwrap(),
        36
    );
    bytes.to_vec()
}

fn install(dir: &Path, log: &[u8], anchor: &[u8], boundary: Option<&[u8]>) {
    fs::write(dir.join("records.log"), log).unwrap();
    fs::write(dir.join("commit.state"), anchor).unwrap();
    if let Some(boundary) = boundary {
        setxattr(dir.join("records.log"), NAME, boundary, XattrFlags::empty()).unwrap();
    }
}

fn invalid(dir: &Path) {
    let log = fs::read(dir.join("records.log")).unwrap();
    let anchor = fs::read(dir.join("commit.state")).unwrap();
    assert!(matches!(Store::open(dir), Err(ref e) if e.kind() == io::ErrorKind::InvalidData));
    assert_eq!(fs::read(dir.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(dir.join("commit.state")).unwrap(), anchor);
}

#[test]
fn new_stores_use_metadata_and_each_acknowledgement_advances_it() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let anchor = fs::read(dir.path().join("commit.state")).unwrap();
    assert_eq!(
        &fs::read(dir.path().join("records.log")).unwrap()[..8],
        b"EMDALOG3"
    );
    for i in 0..5 {
        store.put(&[i + 1], &[i; 32]).unwrap();
        let boundary = marker(dir.path());
        assert_eq!(
            u64::from_le_bytes(boundary[24..32].try_into().unwrap()),
            fs::metadata(dir.path().join("records.log")).unwrap().len()
        );
        assert_eq!(fs::read(dir.path().join("commit.state")).unwrap(), anchor);
    }
}

#[test]
fn every_shorter_log_length_and_byte_corruption_is_rejected() {
    let source = tempdir().unwrap();
    {
        let store = Store::open(source.path()).unwrap();
        store.put(b"first", b"one").unwrap();
        store.put(b"second", b"two").unwrap();
        store.delete(b"first").unwrap();
    }
    let log = fs::read(source.path().join("records.log")).unwrap();
    let anchor = fs::read(source.path().join("commit.state")).unwrap();
    let boundary = marker(source.path());
    for length in 0..log.len() {
        let dir = tempdir().unwrap();
        install(dir.path(), &log[..length], &anchor, Some(&boundary));
        invalid(dir.path());
    }
    for byte in 0..log.len() {
        let dir = tempdir().unwrap();
        let mut corrupt = log.clone();
        corrupt[byte] ^= 1;
        install(dir.path(), &corrupt, &anchor, Some(&boundary));
        invalid(dir.path());
    }
}

#[test]
fn missing_corrupt_wrong_identity_nonboundary_and_short_metadata_fail_closed() {
    let source = tempdir().unwrap();
    {
        let store = Store::open(source.path()).unwrap();
        store.put(b"key", b"value").unwrap();
    }
    let log = fs::read(source.path().join("records.log")).unwrap();
    let anchor = fs::read(source.path().join("commit.state")).unwrap();
    let boundary = marker(source.path());
    let mut cases = vec![None];
    for length in 0..36 {
        cases.push(Some(boundary[..length].to_vec()));
    }
    cases.push(Some(vec![0; 37]));
    for byte in 0..36 {
        let mut corrupt = boundary.clone();
        corrupt[byte] ^= 1;
        cases.push(Some(corrupt));
    }
    for wrong_identity in [false, true] {
        let mut corrupt = boundary.clone();
        if wrong_identity {
            corrupt[8] ^= 1;
        } else {
            corrupt[24..32].copy_from_slice(&29_u64.to_le_bytes());
        }
        let checksum = crc32fast::hash(&corrupt[..32]);
        corrupt[32..].copy_from_slice(&checksum.to_le_bytes());
        cases.push(Some(corrupt));
    }
    for case in cases {
        let dir = tempdir().unwrap();
        install(dir.path(), &log, &anchor, case.as_deref());
        invalid(dir.path());
    }
}

#[test]
fn all_new_record_prefixes_and_torn_metadata_states_preserve_acknowledged_data_or_fail() {
    let source = tempdir().unwrap();
    let store = Store::open(source.path()).unwrap();
    store.put(b"safe", b"committed").unwrap();
    let old_log = fs::read(source.path().join("records.log")).unwrap();
    let old_marker = marker(source.path());
    store.put(b"next", b"unacknowledged").unwrap();
    let new_log = fs::read(source.path().join("records.log")).unwrap();
    let new_marker = marker(source.path());
    let anchor = fs::read(source.path().join("commit.state")).unwrap();
    drop(store);
    for length in old_log.len()..=new_log.len() {
        for split in 0..=36 {
            let mut torn = old_marker.clone();
            torn[..split].copy_from_slice(&new_marker[..split]);
            let dir = tempdir().unwrap();
            install(dir.path(), &new_log[..length], &anchor, Some(&torn));
            match Store::open(dir.path()) {
                Ok(store) => {
                    assert!(length == old_log.len() || length == new_log.len());
                    assert_eq!(
                        store.get(b"safe").unwrap().as_deref(),
                        Some(&b"committed"[..])
                    );
                    assert_eq!(
                        store.get(b"next").unwrap().as_deref(),
                        if length == new_log.len() {
                            Some(&b"unacknowledged"[..])
                        } else {
                            None
                        }
                    );
                    // Adoption must install a boundary protecting the recovered tail.
                    assert_eq!(
                        u64::from_le_bytes(marker(dir.path())[24..32].try_into().unwrap()),
                        length as u64
                    );
                }
                Err(error) => {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                    assert!(!(length == new_log.len() && (split == 0 || split == 36)));
                    assert_eq!(marker(dir.path()), torn);
                }
            }
            assert_eq!(
                fs::read(dir.path().join("records.log")).unwrap(),
                new_log[..length]
            );
        }
    }
}

#[test]
fn rolling_back_only_log_bytes_cannot_hide_an_acknowledged_write() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"key", b"one").unwrap();
    let earlier = fs::read(dir.path().join("records.log")).unwrap();
    store.put(b"key", b"two").unwrap();
    drop(store);
    fs::write(dir.path().join("records.log"), earlier).unwrap();
    invalid(dir.path());
}

#[test]
fn live_metadata_or_log_damage_blocks_writes_and_backup() {
    for fault in 0..4 {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put(b"key", b"one").unwrap();
        let path = dir.path().join("records.log");
        match fault {
            0 => removexattr(&path, NAME).unwrap(),
            1 => setxattr(&path, NAME, &[0; 36], XattrFlags::REPLACE).unwrap(),
            2 => OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(28)
                .unwrap(),
            _ => fs::write(dir.path().join("commit.state"), [0; 36]).unwrap(),
        }
        let backup = dir.path().join("backup");
        assert!(store.backup(&backup).is_err());
        assert!(!backup.exists());
        assert!(store.put(b"key", b"two").is_err());
        assert!(store.get(b"key").is_err());
    }
}

#[test]
fn restore_recreates_metadata_and_protects_the_restored_boundary() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("source");
    let store = Store::open(&source).unwrap();
    store.put(b"key", b"value").unwrap();
    let backup = dir.path().join("backup");
    store.backup(&backup).unwrap();
    let target = dir.path().join("target");
    Store::restore(&backup, &target).unwrap();
    assert_eq!(marker(&source), marker(&target));
    let reopened = Store::open(&target).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    drop(reopened);
    OpenOptions::new()
        .write(true)
        .open(target.join("records.log"))
        .unwrap()
        .set_len(28)
        .unwrap();
    invalid(&target);
}

#[test]
fn v2_and_legacy_upgrade_to_v3_without_modifying_source_or_replacing_destination() {
    for legacy in [false, true] {
        let dir = tempdir().unwrap();
        let source = dir.path().join("source");
        fs::create_dir(&source).unwrap();
        if legacy {
            fs::write(
                source.join("records.log"),
                include_bytes!("fixtures/db01-legacy-record.bin"),
            )
            .unwrap();
        } else {
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
        let store = Store::open(&source).unwrap();
        store.put(b"key", b"first").unwrap();
        store.put(b"key", b"second").unwrap();
        store.put(b"gone", b"delete me").unwrap();
        store.delete(b"gone").unwrap();
        let original = fs::read(source.join("records.log")).unwrap();
        let target = dir.path().join("target");
        store.upgrade_format(&target).unwrap();
        assert_eq!(fs::read(source.join("records.log")).unwrap(), original);
        assert_eq!(
            store.upgrade_format(&target).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        let upgraded = Store::open(&target).unwrap();
        assert_eq!(
            &fs::read(target.join("records.log")).unwrap()[..8],
            b"EMDALOG3"
        );
        assert_eq!(
            upgraded.get(b"key").unwrap().as_deref(),
            Some(&b"second"[..])
        );
        assert_eq!(upgraded.get(b"gone").unwrap(), None);
        if legacy {
            assert_eq!(
                upgraded.get(b"legacy").unwrap().as_deref(),
                Some(&b"persisted"[..])
            );
        }
        upgraded.put(b"key", b"third").unwrap();
        assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"second"[..]));
        let again = dir.path().join("again");
        upgraded.upgrade_format(&again).unwrap();
        assert_eq!(
            Store::open(&again).unwrap().get(b"key").unwrap().as_deref(),
            Some(&b"third"[..])
        );
    }
}

#[test]
fn upgrade_refuses_corrupt_source_identity_and_record_bytes() {
    for byte in [8, 28 + 17 + 3] {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put(b"key", b"value").unwrap();
        let path = dir.path().join("records.log");
        let mut bytes = fs::read(&path).unwrap();
        bytes[byte] ^= 1;
        fs::write(&path, &bytes).unwrap();
        let destination = dir.path().join("upgrade");
        assert!(store.upgrade_format(&destination).is_err());
        assert!(!destination.exists());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn metadata_without_a_valid_v3_header_never_opens_as_legacy_or_v2() {
    let dir = tempdir().unwrap();
    drop(Store::open(dir.path()).unwrap());
    let path = dir.path().join("records.log");
    // Even if the external anchor was lost during interrupted initialization,
    // an existing metadata witness prevents an empty-file legacy downgrade.
    fs::remove_file(dir.path().join("commit.state")).unwrap();
    fs::write(&path, []).unwrap();
    assert!(
        matches!(Store::open(dir.path()), Err(ref e) if e.kind() == io::ErrorKind::InvalidData)
    );
    assert_eq!(fs::read(&path).unwrap(), b"");
}

#[test]
fn replay_key_limit_does_not_advance_witness_for_unacknowledged_tail() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"first", b"one").unwrap();
    let first_boundary = marker(dir.path());
    store.put(b"second", b"two").unwrap();
    drop(store);
    let log = fs::read(dir.path().join("records.log")).unwrap();
    setxattr(
        dir.path().join("records.log"),
        NAME,
        &first_boundary,
        XattrFlags::empty(),
    )
    .unwrap();
    let error = match Store::open_with_limits(dir.path(), Some((log.len() - 1) as u64), None) {
        Ok(_) => panic!("length limit must reject the unacknowledged tail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(marker(dir.path()), first_boundary);
    assert_eq!(fs::read(dir.path().join("records.log")).unwrap(), log);
    let error = match Store::open_with_limits(dir.path(), None, Some(1)) {
        Ok(_) => panic!("limit must reject the unacknowledged second key"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(marker(dir.path()), first_boundary);
    assert_eq!(fs::read(dir.path().join("records.log")).unwrap(), log);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"first").unwrap().as_deref(),
        Some(&b"one"[..])
    );
    assert_eq!(
        reopened.get(b"second").unwrap().as_deref(),
        Some(&b"two"[..])
    );
    assert_ne!(marker(dir.path()), first_boundary);
}

#[test]
fn atomic_batch_torn_tails_and_corruption_fail_closed_at_every_byte() {
    use database_engine::BatchOperation as Op;
    let source = tempdir().unwrap();
    let store = Store::open(source.path()).unwrap();
    store.put(b"a", b"old-a").unwrap();
    store.put(b"b", b"old-b").unwrap();
    let before = fs::read(source.path().join("records.log")).unwrap();
    let old_boundary = marker(source.path());
    let anchor = fs::read(source.path().join("commit.state")).unwrap();
    store
        .write_batch(&[
            Op::Put {
                key: b"a",
                value: b"new-a",
            },
            Op::Put {
                key: b"b",
                value: b"new-b",
            },
        ])
        .unwrap();
    let after = fs::read(source.path().join("records.log")).unwrap();
    let new_boundary = marker(source.path());
    assert_eq!(&after[before.len()..before.len() + 4], b"DB02");
    drop(store);
    for length in before.len() + 1..after.len() {
        let dir = tempdir().unwrap();
        install(dir.path(), &after[..length], &anchor, Some(&old_boundary));
        invalid(dir.path());
        assert_eq!(marker(dir.path()), old_boundary, "truncation at {length}");
    }
    for position in before.len()..after.len() {
        let dir = tempdir().unwrap();
        let mut corrupt = after.clone();
        corrupt[position] ^= 1;
        install(dir.path(), &corrupt, &anchor, Some(&new_boundary));
        invalid(dir.path());
        assert_eq!(marker(dir.path()), new_boundary, "corruption at {position}");
    }
    let dir = tempdir().unwrap();
    install(dir.path(), &after, &anchor, Some(&old_boundary));
    let recovered = Store::open(dir.path()).unwrap();
    assert_eq!(
        recovered.get_many(&[b"a", b"b"]).unwrap(),
        vec![Some(b"new-a".to_vec()), Some(b"new-b".to_vec())]
    );
    assert_eq!(marker(dir.path()), new_boundary);
}

#[test]
fn batch_nested_corruption_is_rejected_even_with_recomputed_outer_checksum() {
    use database_engine::BatchOperation as Op;
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let start = fs::metadata(dir.path().join("records.log")).unwrap().len() as usize;
    store
        .write_batch(&[Op::Put {
            key: b"a",
            value: b"value",
        }])
        .unwrap();
    drop(store);
    let path = dir.path().join("records.log");
    let mut log = fs::read(&path).unwrap();
    // DB02 header + sentinel key + count + nested DB01 header + nested key.
    let nested_value = start + 17 + 1 + 4 + 17 + 1;
    log[nested_value] ^= 1;
    let footer = log.len() - 4;
    let checksum = crc32fast::hash(&log[start..footer]);
    log[footer..].copy_from_slice(&checksum.to_le_bytes());
    fs::write(&path, &log).unwrap();
    assert!(
        matches!(Store::open(dir.path()), Err(ref e) if e.kind() == io::ErrorKind::InvalidData)
    );
    assert_eq!(fs::read(path).unwrap(), log);
}
