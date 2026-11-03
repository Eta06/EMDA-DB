use database_engine::{BatchOperation as Op, Store};
use std::fs;
use std::io::ErrorKind;
use tempfile::tempdir;

#[test]
fn checkpoint_reopens_with_suffix_including_atomic_batch_and_transaction() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old").unwrap();
    store.put(b"deleted", b"old").unwrap();
    let checkpoint = dir.path().join("index");
    let token = store.checkpoint_to(&checkpoint).unwrap();
    store
        .write_batch(&[
            Op::Put {
                key: b"a",
                value: b"new",
            },
            Op::Delete { key: b"deleted" },
            Op::Put {
                key: b"b",
                value: b"value",
            },
        ])
        .unwrap();
    store
        .transaction(|tx| {
            tx.put(b"c", b"transaction")?;
            Ok(())
        })
        .unwrap();
    let expected = store.get_many(&[b"a", b"deleted", b"b", b"c"]).unwrap();
    drop(store);
    let mut wrong_token = token;
    wrong_token[0] ^= 1;
    assert!(Store::open_with_checkpoint(&path, &checkpoint, &wrong_token).is_err());
    let recovered = Store::open_with_checkpoint(&path, &checkpoint, &token).unwrap();
    assert_eq!(
        recovered.get_many(&[b"a", b"deleted", b"b", b"c"]).unwrap(),
        expected
    );
    recovered.put(b"after", b"write").unwrap();
    drop(recovered);
    assert_eq!(
        Store::open_with_checkpoint(&path, &checkpoint, &token)
            .unwrap()
            .get(b"after")
            .unwrap()
            .as_deref(),
        Some(&b"write"[..])
    );
}

#[test]
fn checkpoint_corruption_or_truncation_never_changes_source_and_full_replay_still_works() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"b", b"two").unwrap();
    let checkpoint = dir.path().join("checkpoint");
    let token = store.checkpoint_to(&checkpoint).unwrap();
    let original = fs::read(&checkpoint).unwrap();
    let log = fs::read(path.join("records.log")).unwrap();
    let witness = fs::read(path.join("commit.state")).unwrap();
    drop(store);
    let bad = dir.path().join("bad");
    for length in 0..original.len() {
        fs::write(&bad, &original[..length]).unwrap();
        assert!(
            Store::open_with_checkpoint(&path, &bad, &token).is_err(),
            "truncation at {length}"
        );
        assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness);
    }
    for index in 0..original.len() {
        let mut bytes = original.clone();
        bytes[index] ^= 1;
        fs::write(&bad, bytes).unwrap();
        assert!(
            Store::open_with_checkpoint(&path, &bad, &token).is_err(),
            "corruption at {index}"
        );
        assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness);
    }
    assert_eq!(
        Store::open(&path).unwrap().get(b"a").unwrap().as_deref(),
        Some(&b"one"[..])
    );
}

#[test]
fn unrelated_checkpoint_and_existing_destination_are_rejected() {
    let dir = tempdir().unwrap();
    let first = Store::open(dir.path().join("first")).unwrap();
    first.put(b"key", b"one").unwrap();
    let checkpoint = dir.path().join("index");
    let token = first.checkpoint_to(&checkpoint).unwrap();
    assert_eq!(
        first.checkpoint_to(&checkpoint).unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    let other_path = dir.path().join("second");
    let second = Store::open(&other_path).unwrap();
    second.put(b"key", b"two").unwrap();
    drop(second);
    assert!(Store::open_with_checkpoint(other_path, &checkpoint, &token).is_err());
}

#[test]
fn checkpoint_detects_corruption_in_an_obsolete_log_record() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"old").unwrap();
    store.put(b"key", b"new").unwrap();
    let checkpoint = dir.path().join("index");
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let mut log = fs::OpenOptions::new()
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start(28 + 17 + 3)).unwrap();
    log.write_all(b"X").unwrap();
    log.sync_all().unwrap();
    assert!(Store::open_with_checkpoint(&path, &checkpoint, &token).is_err());
    assert!(Store::open(&path).is_err());
}

#[test]
fn checkpoint_respects_historical_key_peak_and_live_log_limits() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"1").unwrap();
    store.put(b"b", b"2").unwrap();
    store.delete(b"a").unwrap();
    let checkpoint = dir.path().join("index");
    let token = store.checkpoint_to(&checkpoint).unwrap();
    let prefix = fs::read(path.join("records.log")).unwrap();
    store.put(b"c", b"3").unwrap();
    let full = fs::read(path.join("records.log")).unwrap();
    drop(store);
    assert!(
        Store::open_with_checkpoint_and_limits(&path, &checkpoint, &token, None, Some(1)).is_err()
    );
    assert!(
        Store::open_with_checkpoint_and_limits(
            &path,
            &checkpoint,
            &token,
            Some(prefix.len() as u64),
            Some(2)
        )
        .is_err()
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), full);
    let reopened = Store::open_with_checkpoint_and_limits(
        &path,
        &checkpoint,
        &token,
        Some(full.len() as u64 + 100),
        Some(2),
    )
    .unwrap();
    assert_eq!(
        reopened.get_many(&[b"a", b"b", b"c"]).unwrap(),
        vec![None, Some(b"2".to_vec()), Some(b"3".to_vec())]
    );
    assert!(reopened.put(b"d", b"4").is_err());
    reopened.delete(b"b").unwrap();
    reopened.put(b"d", b"4").unwrap();
}

#[test]
fn checkpoint_open_does_not_create_a_missing_store() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("missing");
    assert!(
        Store::open_with_checkpoint(&missing, dir.path().join("missing-index"), &[0; 32]).is_err()
    );
    assert!(!missing.exists());
}

#[test]
fn checkpoint_refuses_corrupt_live_log_without_publishing() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"old").unwrap();
    store.put(b"key", b"new").unwrap();
    let mut log = fs::OpenOptions::new()
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start(28 + 17 + 3)).unwrap();
    log.write_all(b"X").unwrap();
    log.sync_all().unwrap();
    let checkpoint = dir.path().join("index");
    assert!(store.checkpoint_to(&checkpoint).is_err());
    assert!(!checkpoint.exists());
}

#[test]
fn unknown_checkpoint_codec_version_is_rejected_even_with_valid_file_checksum() {
    use sha2::{Digest, Sha256};
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    let checkpoint = dir.path().join("index");
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let mut bytes = fs::read(&checkpoint).unwrap();
    bytes[32..36].copy_from_slice(&3_u32.to_le_bytes());
    let footer = bytes.len() - 32;
    let digest = Sha256::digest(&bytes[..footer]);
    bytes[footer..].copy_from_slice(&digest);
    let forged_token: [u8; 32] = digest.into();
    fs::write(&checkpoint, bytes).unwrap();
    assert!(Store::open_with_checkpoint(&path, &checkpoint, &token).is_err());
    assert!(Store::open_with_checkpoint(&path, &checkpoint, &forged_token).is_err());
    assert_eq!(
        Store::open(&path).unwrap().get(b"key").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn checkpoint_carries_historical_peak_inside_atomic_batches() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let store = Store::open(&path).unwrap();
    store
        .write_batch(&[
            Op::Put {
                key: b"a",
                value: b"1",
            },
            Op::Put {
                key: b"b",
                value: b"2",
            },
            Op::Delete { key: b"a" },
        ])
        .unwrap();
    let checkpoint = dir.path().join("index");
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    assert!(Store::open_with_limits(&path, None, Some(1)).is_err());
    assert!(
        Store::open_with_checkpoint_and_limits(&path, &checkpoint, &token, None, Some(1)).is_err()
    );
    let recovered =
        Store::open_with_checkpoint_and_limits(&path, &checkpoint, &token, None, Some(2)).unwrap();
    assert_eq!(
        recovered.get_many(&[b"a", b"b"]).unwrap(),
        vec![None, Some(b"2".to_vec())]
    );
}

#[test]
fn empty_and_v2_batch_stores_support_verified_checkpoints() {
    let dir = tempdir().unwrap();
    let empty_path = dir.path().join("empty");
    let empty = Store::open(&empty_path).unwrap();
    let empty_index = dir.path().join("empty-index");
    let empty_token = empty.checkpoint_to(&empty_index).unwrap();
    drop(empty);
    assert!(
        Store::open_with_checkpoint_and_limits(
            &empty_path,
            &empty_index,
            &empty_token,
            None,
            Some(0)
        )
        .unwrap()
        .scan(None, None, 1)
        .unwrap()
        .is_empty()
    );

    let v2_path = dir.path().join("v2");
    fs::create_dir(&v2_path).unwrap();
    fs::write(
        v2_path.join("records.log"),
        include_bytes!("fixtures/v2-batch-records.log"),
    )
    .unwrap();
    fs::write(
        v2_path.join("commit.state"),
        include_bytes!("fixtures/v2-batch-commit.state"),
    )
    .unwrap();
    let v2 = Store::open(&v2_path).unwrap();
    let v2_index = dir.path().join("v2-index");
    let v2_token = v2.checkpoint_to(&v2_index).unwrap();
    drop(v2);
    let reopened = Store::open_with_checkpoint(&v2_path, &v2_index, &v2_token).unwrap();
    assert_eq!(
        reopened.get_many(&[b"alpha", b"beta"]).unwrap(),
        vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
    );
}

#[test]
fn checkpoint_rejects_legacy_and_does_not_replace_a_symlink() {
    use std::os::unix::fs::symlink;
    let dir = tempdir().unwrap();
    let legacy_path = dir.path().join("legacy");
    fs::create_dir(&legacy_path).unwrap();
    fs::write(
        legacy_path.join("records.log"),
        include_bytes!("fixtures/db01-legacy-record.bin"),
    )
    .unwrap();
    let legacy = Store::open(&legacy_path).unwrap();
    let target = dir.path().join("index");
    assert_eq!(
        legacy.checkpoint_to(&target).unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert!(!target.exists());
    drop(legacy);
    assert_eq!(
        Store::open_with_checkpoint(&legacy_path, &target, &[0; 32])
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );

    let store = Store::open(dir.path().join("live")).unwrap();
    store.put(b"key", b"value").unwrap();
    let existing = dir.path().join("existing");
    fs::write(&existing, b"preserve").unwrap();
    let link = dir.path().join("link");
    symlink(&existing, &link).unwrap();
    assert_eq!(
        store.checkpoint_to(&link).unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    assert!(fs::symlink_metadata(link).unwrap().file_type().is_symlink());
    assert_eq!(fs::read(existing).unwrap(), b"preserve");
}

#[test]
fn trusted_digest_rejects_forged_index_even_if_its_internal_checksum_is_recomputed() {
    use sha2::{Digest, Sha256};
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"b", b"two").unwrap();
    let checkpoint = dir.path().join("index");
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let mut bytes = fs::read(&checkpoint).unwrap();
    let first = 68 + 2 + 1; // header + key-length + key 'a'
    let second = first + 16 + 2 + 1;
    let second_location = bytes[second..second + 16].to_vec();
    bytes[first..first + 16].copy_from_slice(&second_location);
    let footer = bytes.len() - 32;
    let forged_digest = Sha256::digest(&bytes[..footer]);
    bytes[footer..].copy_from_slice(&forged_digest);
    fs::write(&checkpoint, bytes).unwrap();
    assert!(Store::open_with_checkpoint(&path, &checkpoint, &token).is_err());
    assert_eq!(
        Store::open(&path).unwrap().get_many(&[b"a", b"b"]).unwrap(),
        vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
    );
}

#[test]
fn persistent_index_fixture_matches_current_format_and_opens_v2_batch_store() {
    let dir = tempdir().unwrap();
    fs::write(
        dir.path().join("records.log"),
        include_bytes!("fixtures/v2-batch-records.log"),
    )
    .unwrap();
    fs::write(
        dir.path().join("commit.state"),
        include_bytes!("fixtures/v2-batch-commit.state"),
    )
    .unwrap();
    let checkpoint = dir.path().join("index");
    let store = Store::open(dir.path()).unwrap();
    let token = store.checkpoint_to(&checkpoint).unwrap();
    assert_eq!(&token, include_bytes!("fixtures/v2-batch-index.sha256"));
    assert_eq!(
        fs::read(&checkpoint).unwrap(),
        include_bytes!("fixtures/v2-batch-index.checkpoint")
    );
    drop(store);
    let reopened =
        Store::open_with_checkpoint_and_limits(dir.path(), &checkpoint, &token, None, Some(2))
            .unwrap();
    assert_eq!(
        reopened.get_many(&[b"alpha", b"beta"]).unwrap(),
        vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
    );
}
