use database_engine::{BatchOperation as Op, CheckpointView, Store};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

#[test]
fn historical_view_is_stable_and_uses_shared_advisory_lock() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("index");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old-a").unwrap();
    store.put(b"b", b"old-b").unwrap();
    let token = store.checkpoint_to(&checkpoint).unwrap();
    store
        .write_batch(&[
            Op::Put {
                key: b"a",
                value: b"new-a",
            },
            Op::Delete { key: b"b" },
            Op::Put {
                key: b"c",
                value: b"new-c",
            },
        ])
        .unwrap();
    // A cooperating writer prevents a historical view from acquiring its lock.
    assert!(CheckpointView::open(&path, &checkpoint, &token).is_err());
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    assert_eq!(view.len(), 2);
    assert!(view.has_log_tail());
    assert_eq!(view.get(b"a").unwrap().as_deref(), Some(&b"old-a"[..]));
    assert_eq!(view.get(b"b").unwrap().as_deref(), Some(&b"old-b"[..]));
    assert_eq!(view.get(b"c").unwrap(), None);
    assert_eq!(
        view.scan(Some(b"a"), None, 1).unwrap(),
        vec![(b"b".to_vec(), b"old-b".to_vec())]
    );
    assert!(view.scan(Some(b"b"), None, 1).unwrap().is_empty());
    let other = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    assert!(Store::open(&path).is_err());
    drop(other);
    drop(view);
    assert_eq!(
        Store::open(&path).unwrap().get(b"a").unwrap().as_deref(),
        Some(&b"new-a"[..])
    );
}

#[test]
fn view_detects_checkpoint_block_corruption_after_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("index");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old-a").unwrap();
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    let mut cp = OpenOptions::new().write(true).open(&checkpoint).unwrap();
    cp.seek(SeekFrom::Start(68 + 2)).unwrap();
    cp.write_all(b"z").unwrap();
    cp.sync_all().unwrap();
    assert!(view.get(b"a").is_err());
    drop(view);
    assert!(CheckpointView::open(&path, &checkpoint, &token).is_err());
    // The regular store does not depend on the checkpoint.
    assert_eq!(
        Store::open(&path).unwrap().get(b"a").unwrap().as_deref(),
        Some(&b"old-a"[..])
    );
}

#[test]
fn view_detects_log_value_corruption_after_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("index");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old-a").unwrap();
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    // Simulate non-cooperating corruption after opening the shared view.
    let mut log = OpenOptions::new()
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start(28 + 17 + 1)).unwrap();
    log.write_all(b"X").unwrap();
    log.sync_all().unwrap();
    assert_eq!(
        view.get(b"a").unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    drop(view);
    assert!(Store::open(&path).is_err());
}

#[test]
fn empty_view_and_bad_digest_leave_store_unchanged() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("index");
    let store = Store::open(&path).unwrap();
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let original_log = fs::read(path.join("records.log")).unwrap();
    assert!(CheckpointView::open(&path, &checkpoint, &[42; 32]).is_err());
    let view = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    assert!(view.is_empty());
    assert_eq!(view.block_count(), 0);
    assert!(!view.has_log_tail());
    assert_eq!(view.get(b"a").unwrap(), None);
    assert!(view.scan(None, None, 1).unwrap().is_empty());
    assert!(view.scan(None, None, 0).is_err());
    drop(view);
    assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
    assert!(Store::open(&path).is_ok());
}

#[test]
fn view_reads_persistent_v2_checkpoint_fixture() {
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
    fs::write(dir.path().join("store.lock"), []).unwrap();
    let checkpoint = dir.path().join("index");
    fs::write(
        &checkpoint,
        include_bytes!("fixtures/v2-batch-index.checkpoint"),
    )
    .unwrap();
    let token = include_bytes!("fixtures/v2-batch-index.sha256");
    let view = CheckpointView::open(dir.path(), &checkpoint, token).unwrap();
    assert_eq!(view.len(), 2);
    assert_eq!(view.get(b"alpha").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(view.get(b"beta").unwrap().as_deref(), Some(&b"two"[..]));
}

#[test]
fn historical_view_reports_but_does_not_replay_a_corrupt_tail() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("index");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    let view = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    assert!(view.has_log_tail());
    assert_eq!(view.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
    drop(view);
    assert!(Store::open(&path).is_err());
}

#[test]
fn large_index_uses_multiple_verified_blocks_and_supports_cross_block_scans() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("index");
    let store = Store::open(&path).unwrap();
    let keys = (0..1100u32)
        .map(|i| {
            let mut key = i.to_be_bytes().to_vec();
            key.resize(1020, 7);
            key
        })
        .collect::<Vec<_>>();
    for chunk in keys.chunks(500) {
        let ops = chunk
            .iter()
            .map(|key| Op::Put {
                key,
                value: &key[..4],
            })
            .collect::<Vec<_>>();
        store.write_batch(&ops).unwrap();
    }
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    assert!(view.block_count() >= 2);
    assert_eq!(view.len(), 1100);
    for i in [0, 500, 1000, 1099] {
        assert_eq!(view.get(&keys[i]).unwrap().as_deref(), Some(&keys[i][..4]));
    }
    // Each entry occupies 1,038 bytes: the first block ends after key 30.
    let page = view.scan(Some(&keys[28]), Some(&keys[34]), 10).unwrap();
    assert_eq!(page.len(), 5);
    for (entry, key) in page.iter().zip(&keys[29..34]) {
        assert_eq!(&entry.0, key);
        assert_eq!(entry.1, key[..4]);
    }
}

#[test]
fn short_and_shared_prefixes_choose_the_correct_verified_block() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("index");
    let store = Store::open(&path).unwrap();
    let prefix = [b'q'; 16];
    let keys = (0..5000_u32)
        .map(|number| [prefix.as_slice(), &number.to_be_bytes()].concat())
        .collect::<Vec<_>>();
    store.put(b"q", b"short").unwrap();
    store.put(&prefix[..15], b"shorter").unwrap();
    store.put(&prefix, b"exact").unwrap();
    for chunk in keys.chunks(500) {
        let ops = chunk
            .iter()
            .map(|key| Op::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        store.write_batch(&ops).unwrap();
    }
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    assert!(view.block_count() >= 6);
    assert_eq!(view.get(b"q").unwrap().as_deref(), Some(&b"short"[..]));
    assert_eq!(
        view.get(&prefix[..15]).unwrap().as_deref(),
        Some(&b"shorter"[..])
    );
    assert_eq!(view.get(&prefix).unwrap().as_deref(), Some(&b"exact"[..]));
    for index in [0, 1, 858, 859, 860, 861, 862, 863, 2000, 4999] {
        assert_eq!(view.get(&keys[index]).unwrap().as_deref(), Some(&b"v"[..]));
    }
    let missing = [prefix.as_slice(), &9000_u32.to_be_bytes()].concat();
    assert_eq!(view.get(&missing).unwrap(), None);
    let page = view.scan(Some(&keys[857]), Some(&keys[866]), 20).unwrap();
    assert_eq!(
        page.iter().map(|entry| &entry.0).collect::<Vec<_>>(),
        keys[858..866].iter().collect::<Vec<_>>()
    );
}

#[test]
fn ambiguous_prefix_search_checks_block_integrity_before_branching() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("index");
    let store = Store::open(&path).unwrap();
    let prefix = [b'x'; 16];
    let keys = (0..5000_u32)
        .map(|number| [prefix.as_slice(), &number.to_be_bytes()].concat())
        .collect::<Vec<_>>();
    for chunk in keys.chunks(500) {
        let operations = chunk
            .iter()
            .map(|key| Op::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        store.write_batch(&operations).unwrap();
    }
    let token = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &token).unwrap();
    assert_eq!(view.block_count(), 6);
    let entries_per_block = database_engine::MAX_CHECKPOINT_VIEW_BLOCK_BYTES / (18 + 20);
    let mut cp = OpenOptions::new().write(true).open(&checkpoint).unwrap();
    cp.seek(SeekFrom::Start(
        (68 + 3 * entries_per_block * (18 + 20) + 2) as u64,
    ))
    .unwrap();
    cp.write_all(b"z").unwrap();
    cp.sync_all().unwrap();
    assert_eq!(
        view.get(&keys[4000]).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}
