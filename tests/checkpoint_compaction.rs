use database_engine::{BatchOperation, COMPACTED_CHECKPOINT_FILE, CheckpointStore, Store};
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;

#[test]
fn compacted_copy_has_a_verified_checkpoint_and_preserves_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    for _ in 0..100 {
        ordinary.put(b"key", &[7; 512]).unwrap();
    }
    ordinary.put(b"deleted", b"old-secret-marker").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let source = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    source
        .write_batch(&[
            BatchOperation::Put {
                key: b"key",
                value: b"new",
            },
            BatchOperation::Delete { key: b"deleted" },
            BatchOperation::Put {
                key: b"added",
                value: b"value",
            },
        ])
        .unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    let destination = dir.path().join("compacted");
    let target_digest = source.compact_to(&destination).unwrap();
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    source.put(b"later", b"source-only").unwrap();
    let compacted_log = fs::read(destination.join("records.log")).unwrap();
    assert!(compacted_log.len() < before.len());
    assert!(
        !compacted_log
            .windows(b"old-secret-marker".len())
            .any(|window| window == b"old-secret-marker")
    );
    let target = CheckpointStore::open(
        &destination,
        destination.join(COMPACTED_CHECKPOINT_FILE),
        &target_digest,
    )
    .unwrap();
    assert_eq!(target.tail_bytes().unwrap(), 0);
    assert_eq!(target.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(target.get(b"deleted").unwrap(), None);
    assert_eq!(
        target.get(b"added").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    assert_eq!(target.get(b"later").unwrap(), None);
    target.put(b"target-only", b"ok").unwrap();
    assert_eq!(source.get(b"target-only").unwrap(), None);
    drop(target);
    let ordinary_target = Store::open(&destination).unwrap();
    assert_eq!(
        ordinary_target.get(b"target-only").unwrap().as_deref(),
        Some(&b"ok"[..])
    );
}

#[test]
fn compaction_handles_an_empty_store_and_rejects_existing_destinations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let source = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let occupied = dir.path().join("occupied");
    fs::write(&occupied, b"keep").unwrap();
    assert!(source.compact_to(&occupied).is_err());
    assert_eq!(fs::read(&occupied).unwrap(), b"keep");
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&occupied, &link).unwrap();
    assert!(source.compact_to(&link).is_err());
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read(&occupied).unwrap(), b"keep");
    let destination = dir.path().join("empty-copy");
    let new_digest = source.compact_to(&destination).unwrap();
    let target = CheckpointStore::open_with_limits(
        &destination,
        destination.join(COMPACTED_CHECKPOINT_FILE),
        &new_digest,
        0,
        0,
        0,
    )
    .unwrap();
    assert!(target.scan(None, None, 10).unwrap().is_empty());
}

#[test]
fn compaction_copies_a_value_larger_than_the_normal_group_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    let value = vec![0x5a; 5 * 1024 * 1024];
    ordinary.put(b"large", &value).unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let source = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let destination = dir.path().join("copy");
    let target_digest = source.compact_to(&destination).unwrap();
    let copy = CheckpointStore::open(
        &destination,
        destination.join(COMPACTED_CHECKPOINT_FILE),
        &target_digest,
    )
    .unwrap();
    assert_eq!(copy.get(b"large").unwrap(), Some(value));
}

#[test]
fn compaction_rejects_corrupt_source_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"key", b"old").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let source = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    source.put(b"key", b"new").unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    let mut byte = [0];
    file.read_exact_at(&mut byte, 28).unwrap();
    byte[0] ^= 0x40;
    file.write_all_at(&byte, 28).unwrap();
    let destination = dir.path().join("must-not-exist");
    assert!(source.compact_to(&destination).is_err());
    assert!(!destination.exists());
}

#[test]
fn compaction_streams_a_large_checkpoint_and_reopens_without_an_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    let keys = (0_u32..20_000).map(|n| n.to_be_bytes()).collect::<Vec<_>>();
    for chunk in keys.chunks(500) {
        let batch = chunk
            .iter()
            .map(|key| BatchOperation::Put {
                key,
                value: b"value",
            })
            .collect::<Vec<_>>();
        ordinary.write_batch(&batch).unwrap();
    }
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let source = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let destination = dir.path().join("compacted");
    let target_digest = source.compact_to(&destination).unwrap();
    let target = CheckpointStore::open_with_limits(
        &destination,
        destination.join(COMPACTED_CHECKPOINT_FILE),
        &target_digest,
        0,
        0,
        0,
    )
    .unwrap();
    assert_eq!(target.tail_bytes().unwrap(), 0);
    for number in [0_u32, 1, 999, 10_000, 19_999] {
        assert_eq!(
            target.get(&number.to_be_bytes()).unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}
