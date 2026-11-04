use database_engine::{BatchOperation, CheckpointStore, Store};
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;

fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    for key in [b"a", b"c", b"z"] {
        store.put(key, b"base").unwrap();
    }
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    (dir, path, checkpoint, digest)
}

#[test]
fn streamed_rollover_clears_overlay_after_reopen_and_preserves_historical_peak() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"b",
                value: b"one",
            },
            BatchOperation::Put {
                key: b"d",
                value: b"two",
            },
            BatchOperation::Delete { key: b"c" },
        ])
        .unwrap();
    let expected = store.scan(None, None, 20).unwrap();
    let next = dir.path().join("next");
    let next_digest = store.checkpoint_to(&next).unwrap();
    assert!(store.tail_bytes().unwrap() > 0);
    assert_eq!(store.overlay_keys().unwrap(), 3);
    assert_eq!(store.scan(None, None, 20).unwrap(), expected);
    drop(store);

    // The ordered batch briefly had five live keys, even though it ended with four.
    assert!(
        Store::open_with_checkpoint_and_limits(&path, &next, &next_digest, None, Some(4)).is_err()
    );
    let ordinary =
        Store::open_with_checkpoint_and_limits(&path, &next, &next_digest, None, Some(5)).unwrap();
    assert_eq!(ordinary.scan(None, None, 20).unwrap(), expected);
    let reference = dir.path().join("reference");
    assert_eq!(ordinary.checkpoint_to(&reference).unwrap(), next_digest);
    assert_eq!(fs::read(&reference).unwrap(), fs::read(&next).unwrap());
    drop(ordinary);

    let store = CheckpointStore::open_with_limits(&path, &next, &next_digest, 1000, 0, 0).unwrap();
    assert_eq!(store.tail_bytes().unwrap(), 0);
    assert_eq!(store.overlay_keys().unwrap(), 0);
    assert_eq!(store.scan(None, None, 20).unwrap(), expected);
    assert!(store.put(b"a", b"full").is_err());
    drop(store);
    let store = CheckpointStore::open(&path, &next, &next_digest).unwrap();
    store.delete(b"a").unwrap();
    let third = dir.path().join("third");
    let third_digest = store.checkpoint_to(&third).unwrap();
    drop(store);
    assert!(
        Store::open_with_checkpoint_and_limits(&path, &third, &third_digest, None, Some(4))
            .is_err()
    );
    let ordinary =
        Store::open_with_checkpoint_and_limits(&path, &third, &third_digest, None, Some(5))
            .unwrap();
    assert_eq!(ordinary.get(b"a").unwrap(), None);
    assert_eq!(ordinary.get(b"b").unwrap().as_deref(), Some(&b"one"[..]));
}

#[test]
fn rollover_rejects_checkpoint_prefix_and_suffix_corruption_without_publication() {
    for target in ["checkpoint", "prefix", "suffix"] {
        let (dir, path, checkpoint, digest) = fixture();
        let prefix = fs::metadata(path.join("records.log")).unwrap().len();
        let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
        store.put(b"tail", b"value").unwrap();
        let (damaged, offset) = match target {
            "checkpoint" => (checkpoint.clone(), 20),
            "prefix" => (path.join("records.log"), 28),
            _ => (path.join("records.log"), prefix),
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&damaged)
            .unwrap();
        let mut byte = [0];
        file.read_exact_at(&mut byte, offset).unwrap();
        byte[0] ^= 0x40;
        file.write_all_at(&byte, offset).unwrap();
        let next = dir.path().join("must-not-exist");
        assert!(
            store.checkpoint_to(&next).is_err(),
            "{target} corruption was accepted"
        );
        assert!(!next.exists());
        drop(store);
        if target == "checkpoint" {
            assert_eq!(
                Store::open(&path).unwrap().get(b"tail").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        } else {
            assert!(Store::open(&path).is_err());
        }
    }
}

#[test]
fn rollover_never_replaces_an_existing_destination() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let destination = dir.path().join("occupied");
    fs::write(&destination, b"keep").unwrap();
    assert!(store.checkpoint_to(&destination).is_err());
    assert_eq!(fs::read(&destination).unwrap(), b"keep");
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"base"[..]));
}

#[test]
fn rollover_merges_multiple_blocks_with_shared_key_prefixes() {
    use std::collections::BTreeMap;
    fn key(number: u32) -> Vec<u8> {
        let mut key = b"a-long-shared-prefix".to_vec();
        key.extend_from_slice(&number.to_be_bytes());
        key
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("base");
    let store = Store::open(&path).unwrap();
    let keys = (0..2000).map(|i| key(i * 2)).collect::<Vec<_>>();
    let mut expected = BTreeMap::new();
    for chunk in keys.chunks(500) {
        let operations = chunk
            .iter()
            .map(|key| BatchOperation::Put { key, value: b"old" })
            .collect::<Vec<_>>();
        store.write_batch(&operations).unwrap();
        for key in chunk {
            expected.insert(key.clone(), b"old".to_vec());
        }
    }
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let mut changes = Vec::new();
    for i in 0..500 {
        changes.push((key(i * 2), None));
        changes.push((key(i * 2 + 1), Some(b"inserted".to_vec())));
        changes.push((key((i + 1000) * 2), Some(b"updated".to_vec())));
    }
    for chunk in changes.chunks(500) {
        let operations = chunk
            .iter()
            .map(|(key, value)| match value {
                Some(value) => BatchOperation::Put { key, value },
                None => BatchOperation::Delete { key },
            })
            .collect::<Vec<_>>();
        store.write_batch(&operations).unwrap();
        for (key, value) in chunk {
            if let Some(value) = value {
                expected.insert(key.clone(), value.clone());
            } else {
                expected.remove(key);
            }
        }
    }
    let next = dir.path().join("next");
    let next_digest = store.checkpoint_to(&next).unwrap();
    drop(store);
    let ordinary = Store::open(&path).unwrap();
    let reference = dir.path().join("reference");
    assert_eq!(ordinary.checkpoint_to(&reference).unwrap(), next_digest);
    assert_eq!(fs::read(&reference).unwrap(), fs::read(&next).unwrap());
    drop(ordinary);
    let recovered = CheckpointStore::open(&path, &next, &next_digest).unwrap();
    assert_eq!(recovered.tail_bytes().unwrap(), 0);
    let mut actual = Vec::new();
    let mut after = None::<Vec<u8>>;
    loop {
        let page = recovered.scan(after.as_deref(), None, 37).unwrap();
        if page.is_empty() {
            break;
        }
        after = Some(page.last().unwrap().0.clone());
        actual.extend(page);
    }
    assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
}
