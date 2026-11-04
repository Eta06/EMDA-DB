use database_engine::{
    BatchOperation as Op, CheckpointView, MAX_RECOVERED_VIEW_TAIL_BYTES,
    MAX_RECOVERED_VIEW_TAIL_KEY_BYTES, MAX_RECOVERED_VIEW_TAIL_KEYS, RecoveredCheckpointView,
    Store,
};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;

#[test]
fn recovered_view_merges_suffix_updates_tombstones_and_ordered_pages() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    for key in [b"b", b"d", b"f"] {
        store.put(key, b"old").unwrap();
    }
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"b", b"updated").unwrap();
    store.put(b"c", b"three").unwrap();
    store.delete(b"d").unwrap();
    store
        .write_batch(&[
            Op::Put {
                key: b"e",
                value: b"five",
            },
            Op::Put {
                key: b"f",
                value: b"six",
            },
            Op::Put {
                key: b"g",
                value: b"seven",
            },
            Op::Put {
                key: b"z",
                value: b"temporary",
            },
            Op::Delete { key: b"z" },
        ])
        .unwrap();
    store.transaction(|tx| tx.put(b"h", b"eight")).unwrap();
    drop(store);
    let view = RecoveredCheckpointView::open(&path, &checkpoint, &digest).unwrap();
    assert!(view.tail_bytes() > 0);
    assert_eq!(view.checkpoint_block_count(), 1);
    assert_eq!(view.overlay_keys(), 9);
    assert_eq!(view.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(view.get(b"b").unwrap().as_deref(), Some(&b"updated"[..]));
    assert_eq!(view.get(b"d").unwrap(), None);
    assert_eq!(view.get(b"z").unwrap(), None);
    assert!(Store::open(&path).is_err());
    let second = RecoveredCheckpointView::open(&path, &checkpoint, &digest).unwrap();
    drop(second);
    let expected = vec![
        (b"a".to_vec(), b"one".to_vec()),
        (b"b".to_vec(), b"updated".to_vec()),
        (b"c".to_vec(), b"three".to_vec()),
        (b"e".to_vec(), b"five".to_vec()),
        (b"f".to_vec(), b"six".to_vec()),
        (b"g".to_vec(), b"seven".to_vec()),
        (b"h".to_vec(), b"eight".to_vec()),
    ];
    assert_eq!(view.scan(None, None, 20).unwrap(), expected);
    assert_eq!(
        view.scan(Some(b"b"), Some(b"g"), 20).unwrap(),
        expected[2..5]
    );
    assert!(view.scan(Some(b"g"), Some(b"b"), 20).unwrap().is_empty());
    assert_eq!(
        view.scan(None, None, 0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let mut after = None::<Vec<u8>>;
    let mut pages = Vec::new();
    loop {
        let page = view.scan(after.as_deref(), None, 2).unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().map(|entry| entry.0.clone());
        pages.extend(page);
    }
    assert_eq!(pages, expected);
    drop(view);
    assert_eq!(
        Store::open(&path).unwrap().scan(None, None, 20).unwrap(),
        expected
    );
}

#[test]
fn recovered_view_enforces_suffix_limits_without_publishing_partial_results() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let prefix = fs::metadata(path.join("records.log")).unwrap().len();
    store.put(b"a", b"one").unwrap();
    store.put(b"bb", b"two").unwrap();
    store.delete(b"zzz").unwrap();
    drop(store);
    let tail = fs::metadata(path.join("records.log")).unwrap().len() - prefix;
    for (bytes, keys, key_bytes) in [(tail - 1, 3, 6), (tail, 2, 6), (tail, 3, 5)] {
        let result = RecoveredCheckpointView::open_with_limits(
            &path,
            &checkpoint,
            &digest,
            bytes,
            keys,
            key_bytes,
        );
        assert!(matches!(result, Err(ref error) if error.kind() == io::ErrorKind::InvalidInput));
    }
    let view =
        RecoveredCheckpointView::open_with_limits(&path, &checkpoint, &digest, tail, 3, 6).unwrap();
    assert_eq!(view.overlay_keys(), 3);
    assert_eq!(
        view.scan(None, None, 10).unwrap(),
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"bb".to_vec(), b"two".to_vec())
        ]
    );
    drop(view);
    for (bytes, keys, key_bytes) in [
        (MAX_RECOVERED_VIEW_TAIL_BYTES + 1, 0, 0),
        (0, MAX_RECOVERED_VIEW_TAIL_KEYS + 1, 0),
        (0, 0, MAX_RECOVERED_VIEW_TAIL_KEY_BYTES + 1),
    ] {
        let result = RecoveredCheckpointView::open_with_limits(
            &path,
            &checkpoint,
            &digest,
            bytes,
            keys,
            key_bytes,
        );
        assert!(matches!(result, Err(ref error) if error.kind() == io::ErrorKind::InvalidInput));
    }
}

#[test]
fn recovered_view_accepts_empty_suffix_with_zero_limits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"value").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view =
        RecoveredCheckpointView::open_with_limits(&path, &checkpoint, &digest, 0, 0, 0).unwrap();
    assert_eq!(view.tail_bytes(), 0);
    assert_eq!(view.overlay_keys(), 0);
    assert_eq!(view.get(b"base").unwrap().as_deref(), Some(&b"value"[..]));
}

#[test]
fn recovered_view_rejects_an_incomplete_tail_without_changing_the_log() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"value").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let log = path.join("records.log");
    let mut file = OpenOptions::new().append(true).open(&log).unwrap();
    file.write_all(b"DB").unwrap();
    file.sync_all().unwrap();
    let before = fs::read(&log).unwrap();
    assert!(RecoveredCheckpointView::open(&path, &checkpoint, &digest).is_err());
    assert_eq!(fs::read(&log).unwrap(), before);
    let historical = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
    assert!(historical.has_log_tail());
    assert_eq!(
        historical.get(b"base").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    drop(historical);
    assert!(Store::open(&path).is_err());
    assert_eq!(fs::read(log).unwrap(), before);
}

#[cfg(target_os = "linux")]
#[test]
fn recovered_view_rejects_unacknowledged_tail_until_normal_recovery() {
    use rustix::fs::{XattrFlags, getxattr, setxattr};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let log = path.join("records.log");
    let mut old_boundary = [0_u8; 36];
    assert_eq!(
        getxattr(&log, "user.db.commit.v3", &mut old_boundary[..]).unwrap(),
        36
    );
    store.put(b"b", b"tail").unwrap();
    drop(store);
    setxattr(
        &log,
        "user.db.commit.v3",
        &old_boundary,
        XattrFlags::REPLACE,
    )
    .unwrap();
    let before = fs::read(&log).unwrap();
    assert!(RecoveredCheckpointView::open(&path, &checkpoint, &digest).is_err());
    assert_eq!(fs::read(&log).unwrap(), before);
    let historical = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
    assert!(historical.has_log_tail());
    assert_eq!(historical.get(b"b").unwrap(), None);
    drop(historical);
    let recovered = Store::open(&path).unwrap();
    assert_eq!(recovered.get(b"b").unwrap().as_deref(), Some(&b"tail"[..]));
    drop(recovered);
    let view = RecoveredCheckpointView::open(&path, &checkpoint, &digest).unwrap();
    assert_eq!(view.get(b"b").unwrap().as_deref(), Some(&b"tail"[..]));
}

#[test]
fn recovered_view_detects_every_single_byte_corruption_in_small_suffix() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"old").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let log = path.join("records.log");
    let prefix = fs::metadata(&log).unwrap().len() as usize;
    store
        .write_batch(&[
            Op::Put {
                key: b"base",
                value: b"new",
            },
            Op::Put {
                key: b"extra",
                value: b"value",
            },
            Op::Delete { key: b"missing" },
        ])
        .unwrap();
    drop(store);
    let original = fs::read(&log).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log)
        .unwrap();
    for offset in prefix..original.len() {
        file.write_all_at(&[original[offset] ^ 1], offset as u64)
            .unwrap();
        assert!(
            RecoveredCheckpointView::open(&path, &checkpoint, &digest).is_err(),
            "offset {offset}"
        );
        file.write_all_at(&original[offset..offset + 1], offset as u64)
            .unwrap();
    }
    file.sync_all().unwrap();
    let view = RecoveredCheckpointView::open(&path, &checkpoint, &digest).unwrap();
    assert_eq!(view.get(b"base").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn recovered_view_pagination_preserves_entries_across_the_response_byte_cap() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    let large = vec![0x5a; 16 * 1024 * 1024];
    store.put(b"a", &large).unwrap();
    store.put(b"c", &large).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    store.put(b"b", b"small").unwrap();
    drop(store);
    let view = RecoveredCheckpointView::open(&path, &checkpoint, &digest).unwrap();
    let first = view.scan(None, None, 10).unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].0, b"a");
    assert_eq!(first[1], (b"b".to_vec(), b"small".to_vec()));
    let second = view.scan(Some(b"b"), None, 10).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, b"c");
    assert_eq!(second[0].1, large);
}

#[test]
fn recovered_view_replays_suffix_on_persistent_v2_fixture() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    fs::write(
        path.join("records.log"),
        include_bytes!("fixtures/v2-batch-records.log"),
    )
    .unwrap();
    fs::write(
        path.join("commit.state"),
        include_bytes!("fixtures/v2-batch-commit.state"),
    )
    .unwrap();
    fs::write(path.join("store.lock"), []).unwrap();
    let checkpoint = path.join("checkpoint");
    fs::write(
        &checkpoint,
        include_bytes!("fixtures/v2-batch-index.checkpoint"),
    )
    .unwrap();
    let digest = include_bytes!("fixtures/v2-batch-index.sha256");
    let store = Store::open(path).unwrap();
    store.delete(b"alpha").unwrap();
    store.put(b"gamma", b"three").unwrap();
    drop(store);
    let view = RecoveredCheckpointView::open(path, &checkpoint, digest).unwrap();
    assert_eq!(
        view.scan(None, None, 10).unwrap(),
        vec![
            (b"beta".to_vec(), b"two".to_vec()),
            (b"gamma".to_vec(), b"three".to_vec()),
        ]
    );
}

#[test]
fn recovered_view_detects_changed_checkpoint_blocks_and_suffix_values_after_open() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"base").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let suffix_offset = fs::metadata(path.join("records.log")).unwrap().len();
    store.put(b"b", b"tail").unwrap();
    drop(store);
    let view = RecoveredCheckpointView::open(&path, &checkpoint, &digest).unwrap();
    let checkpoint_file = OpenOptions::new().write(true).open(&checkpoint).unwrap();
    checkpoint_file.write_all_at(b"z", 68 + 2).unwrap();
    checkpoint_file.sync_all().unwrap();
    assert!(view.get(b"a").is_err());
    assert!(view.scan(None, None, 10).is_err());
    let log = OpenOptions::new()
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    log.write_all_at(b"X", suffix_offset + 17 + 1).unwrap();
    log.sync_all().unwrap();
    assert!(view.get(b"b").is_err());
}
