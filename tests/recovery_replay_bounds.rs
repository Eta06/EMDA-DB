mod support;

use database_engine::{
    BatchOperation, CheckpointStore, MAX_ATOMIC_BATCH_BYTES, RecoveredCheckpointView, Store,
};
use std::fs;
use std::io::ErrorKind;
use support::retry_lock;
use tempfile::tempdir;

const RECOVERY_READ_BUFFER_BYTES: usize = 64 * 1024;

#[test]
fn maximum_encoded_batch_replays_on_both_paths() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"preserved").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    // DB02 payload: four-byte count and a DB01 child (17-byte header,
    // key, value and four-byte checksum).
    let key = b"maximum";
    let value = vec![0x63; MAX_ATOMIC_BATCH_BYTES - 4 - 17 - key.len() - 4];
    store
        .write_batch(&[BatchOperation::Put { key, value: &value }])
        .unwrap();
    drop(store);
    let before = fs::read(path.join("records.log")).unwrap();
    let ordinary = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(ordinary.get(key).unwrap(), Some(value.clone()));
    drop(ordinary);
    let checkpoint_store =
        retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(checkpoint_store.get(key).unwrap(), Some(value));
    assert_eq!(
        checkpoint_store.get(b"base").unwrap().as_deref(),
        Some(&b"preserved"[..])
    );
    drop(checkpoint_store);
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
}

#[test]
fn reopen_replays_puts_deletes_and_batches_larger_than_the_recovery_buffer() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let sizes = [
        RECOVERY_READ_BUFFER_BYTES - 1,
        RECOVERY_READ_BUFFER_BYTES,
        RECOVERY_READ_BUFFER_BYTES + 1,
        70_000,
    ];
    {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        store.put(b"pre", b"small").unwrap();
        store.put(b"empty", b"").unwrap();
        for (index, size) in sizes.iter().copied().enumerate() {
            let key = [b'a' + u8::try_from(index).unwrap()];
            store.put(&key, &vec![key[0]; size]).unwrap();
        }
        store.delete(b"a").unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"batch",
                    value: &vec![0x7e; RECOVERY_READ_BUFFER_BYTES + 17],
                },
                BatchOperation::Delete { key: b"b" },
            ])
            .unwrap();
        store.put(b"tail", b"after").unwrap();
    }
    let before = fs::read(path.join("records.log")).unwrap();
    let store = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(store.get(b"pre").unwrap().as_deref(), Some(&b"small"[..]));
    assert_eq!(store.get(b"empty").unwrap().as_deref(), Some(&b""[..]));
    assert_eq!(store.get(b"a").unwrap(), None);
    assert_eq!(store.get(b"b").unwrap(), None);
    assert_eq!(store.get(b"c").unwrap().unwrap(), vec![b'c'; sizes[2]]);
    assert_eq!(store.get(b"d").unwrap().unwrap(), vec![b'd'; sizes[3]]);
    assert_eq!(
        store.get(b"batch").unwrap().unwrap(),
        vec![0x7e; RECOVERY_READ_BUFFER_BYTES + 17]
    );
    assert_eq!(store.get(b"tail").unwrap().as_deref(), Some(&b"after"[..]));
    drop(store);
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
}

#[test]
fn corruption_past_the_recovery_buffer_fails_closed_and_preserves_the_log() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let value = vec![0x5a_u8; RECOVERY_READ_BUFFER_BYTES + 90];
    {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        store.put(b"early", b"kept").unwrap();
        store.put(b"wide", &value).unwrap();
        store.put(b"late", b"also").unwrap();
    }
    let original = fs::read(path.join("records.log")).unwrap();
    for offset_in_run in [3_usize, RECOVERY_READ_BUFFER_BYTES] {
        let mut damaged = original.clone();
        let start = damaged
            .windows(value.len())
            .position(|window| window == value)
            .expect("stored value");
        damaged[start + offset_in_run] ^= 1;
        fs::write(path.join("records.log"), &damaged).unwrap();
        let error = match retry_lock(|| Store::open(&path)) {
            Ok(_store) => panic!("corrupt log opened at value offset {offset_in_run}"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::InvalidData, "{offset_in_run}");
        assert!(
            error.to_string().contains("record checksum mismatch"),
            "{error}"
        );
        assert_eq!(fs::read(path.join("records.log")).unwrap(), damaged);
    }
    fs::write(path.join("records.log"), &original).unwrap();
    let store = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(store.get(b"wide").unwrap().unwrap(), value);
    assert_eq!(store.get(b"late").unwrap().as_deref(), Some(&b"also"[..]));
}

#[test]
fn checkpoint_suffix_replay_rejects_corruption_past_the_recovery_buffer() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let checkpoint = dir.path().join("index.checkpoint");
    let value = vec![0xab_u8; RECOVERY_READ_BUFFER_BYTES + 64];
    let digest = {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        store.put(b"base", b"kept").unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        store.put(b"suffix", &value).unwrap();
        digest
    };
    let opened = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(opened.get(b"base").unwrap().as_deref(), Some(&b"kept"[..]));
    assert_eq!(opened.get(b"suffix").unwrap().unwrap(), value);
    drop(opened);

    let original = fs::read(path.join("records.log")).unwrap();
    let witness = fs::read(path.join("commit.state")).unwrap();
    let mut damaged = original.clone();
    let start = damaged
        .windows(value.len())
        .position(|window| window == value)
        .expect("suffix value");
    damaged[start + RECOVERY_READ_BUFFER_BYTES] ^= 1;
    fs::write(path.join("records.log"), &damaged).unwrap();
    let error = match retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)) {
        Ok(_store) => panic!("corrupt checkpoint suffix opened"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("record checksum mismatch"),
        "{error}"
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), damaged);
    assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness);
    match retry_lock(|| Store::open(&path)) {
        Ok(_store) => panic!("corrupt log opened through ordinary recovery"),
        Err(error) => assert_eq!(error.kind(), ErrorKind::InvalidData),
    }

    fs::write(path.join("records.log"), &original).unwrap();
    let opened = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(opened.get(b"suffix").unwrap().unwrap(), value);
    assert_eq!(opened.get(b"base").unwrap().as_deref(), Some(&b"kept"[..]));
}

#[test]
fn backup_and_recovered_view_reject_corrupt_streamed_suffix() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let value = vec![0xbc_u8; RECOVERY_READ_BUFFER_BYTES + 64];
    let store = retry_lock(|| Store::open(&path)).unwrap();
    store.put(b"base", b"kept").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    store.put(b"suffix", &value).unwrap();
    store.backup(dir.path().join("valid-backup")).unwrap();
    drop(store);
    let view = retry_lock(|| RecoveredCheckpointView::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(view.get(b"suffix").unwrap().unwrap(), value);
    drop(view);

    let store = retry_lock(|| Store::open(&path)).unwrap();
    let log = path.join("records.log");
    let original = fs::read(&log).unwrap();
    let mut damaged = original.clone();
    let start = damaged
        .windows(value.len())
        .position(|window| window == value)
        .unwrap();
    damaged[start + RECOVERY_READ_BUFFER_BYTES] ^= 1;
    fs::write(&log, &damaged).unwrap();
    let backup = dir.path().join("invalid-backup");
    assert!(store.backup(&backup).is_err());
    assert!(!backup.exists());
    drop(store);
    assert!(retry_lock(|| RecoveredCheckpointView::open(&path, &checkpoint, &digest)).is_err());
    assert_eq!(fs::read(&log).unwrap(), damaged);
    fs::write(&log, original).unwrap();
    let view = retry_lock(|| RecoveredCheckpointView::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(view.get(b"suffix").unwrap().unwrap(), value);
}

#[test]
fn reopen_streams_a_multi_child_batch_and_rejects_a_late_corrupt_byte() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let mut value = b"BATCH-VALUE-MARKER!!".to_vec();
    value.extend((0..RECOVERY_READ_BUFFER_BYTES).map(|index| (index % 251) as u8));
    {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        store.put(b"early", b"kept").unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"wide",
                    value: &value,
                },
                BatchOperation::Put {
                    key: b"small",
                    value: b"child",
                },
                BatchOperation::Delete { key: b"early" },
            ])
            .unwrap();
        store.put(b"tail", b"after").unwrap();
    }
    let original = fs::read(path.join("records.log")).unwrap();
    let store = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(store.get(b"early").unwrap(), None);
    assert_eq!(store.get(b"wide").unwrap().unwrap(), value);
    assert_eq!(store.get(b"small").unwrap().as_deref(), Some(&b"child"[..]));
    assert_eq!(store.get(b"tail").unwrap().as_deref(), Some(&b"after"[..]));
    drop(store);

    let mut damaged = original.clone();
    let start = damaged
        .windows(value.len())
        .position(|window| window == value.as_slice())
        .expect("stored batch value");
    damaged[start + RECOVERY_READ_BUFFER_BYTES] ^= 1;
    fs::write(path.join("records.log"), &damaged).unwrap();
    let error = match retry_lock(|| Store::open(&path)) {
        Ok(_store) => panic!("corrupt batch opened"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("record checksum mismatch"),
        "{error}"
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), damaged);

    fs::write(path.join("records.log"), &original).unwrap();
    let store = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(store.get(b"wide").unwrap().unwrap(), value);
    assert_eq!(store.get(b"small").unwrap().as_deref(), Some(&b"child"[..]));
    assert_eq!(store.get(b"tail").unwrap().as_deref(), Some(&b"after"[..]));
    assert_eq!(fs::read(path.join("records.log")).unwrap(), original);
}

#[test]
fn streamed_batch_replay_enforces_ordered_live_key_quotas() {
    let dir = tempdir().unwrap();
    let peak = dir.path().join("peak");
    let flat = dir.path().join("flat");
    {
        let store = retry_lock(|| Store::open(&peak)).unwrap();
        store.put(b"k", b"1").unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"aa",
                    value: b"2",
                },
                BatchOperation::Put {
                    key: b"b",
                    value: b"3",
                },
                BatchOperation::Delete { key: b"aa" },
            ])
            .unwrap();
    }
    {
        let store = retry_lock(|| Store::open(&flat)).unwrap();
        store.put(b"k", b"1").unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"aa",
                    value: b"2",
                },
                BatchOperation::Delete { key: b"aa" },
                BatchOperation::Put {
                    key: b"b",
                    value: b"3",
                },
            ])
            .unwrap();
    }
    let peak_log = fs::read(peak.join("records.log")).unwrap();
    let flat_log = fs::read(flat.join("records.log")).unwrap();

    let error = match retry_lock(|| Store::open_with_limits(&peak, None, Some(2))) {
        Ok(_store) => panic!("batch peak of three keys opened"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("recovery live key limit"),
        "{error}"
    );
    assert_eq!(fs::read(peak.join("records.log")).unwrap(), peak_log);
    let opened = retry_lock(|| Store::open_with_limits(&flat, None, Some(2))).unwrap();
    assert_eq!(opened.get(b"k").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(opened.get(b"b").unwrap().as_deref(), Some(&b"3"[..]));
    assert_eq!(opened.get(b"aa").unwrap(), None);
    drop(opened);

    let error = match retry_lock(|| Store::open_with_resource_limits(&peak, None, None, Some(3))) {
        Ok(_store) => panic!("batch byte peak opened"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("recovery live key byte limit"),
        "{error}"
    );
    assert_eq!(fs::read(peak.join("records.log")).unwrap(), peak_log);
    let opened =
        retry_lock(|| Store::open_with_resource_limits(&flat, None, None, Some(3))).unwrap();
    assert_eq!(opened.get(b"aa").unwrap(), None);
    assert_eq!(opened.get(b"b").unwrap().as_deref(), Some(&b"3"[..]));
    assert_eq!(fs::read(flat.join("records.log")).unwrap(), flat_log);
}

#[test]
fn checkpoint_suffix_replays_a_streamed_batch_and_rejects_late_corruption() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("live");
    let checkpoint = dir.path().join("index.checkpoint");
    let mut value = b"SUFFIX-BATCH-MARKER!".to_vec();
    value.extend((0..RECOVERY_READ_BUFFER_BYTES + 24).map(|index| (index % 251) as u8));
    let digest = {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        store.put(b"base", b"kept").unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"suffix",
                    value: &value,
                },
                BatchOperation::Delete { key: b"base" },
            ])
            .unwrap();
        digest
    };
    let opened = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(opened.get(b"base").unwrap(), None);
    assert_eq!(opened.get(b"suffix").unwrap().unwrap(), value);
    drop(opened);

    let original = fs::read(path.join("records.log")).unwrap();
    let witness = fs::read(path.join("commit.state")).unwrap();
    let mut damaged = original.clone();
    let start = damaged
        .windows(value.len())
        .position(|window| window == value.as_slice())
        .expect("suffix batch value");
    damaged[start + RECOVERY_READ_BUFFER_BYTES] ^= 1;
    fs::write(path.join("records.log"), &damaged).unwrap();
    let error = match retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)) {
        Ok(_store) => panic!("corrupt batch suffix opened"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("record checksum mismatch"),
        "{error}"
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), damaged);
    assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness);

    fs::write(path.join("records.log"), &original).unwrap();
    let opened = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(opened.get(b"suffix").unwrap().unwrap(), value);
    assert_eq!(opened.get(b"base").unwrap(), None);
}
