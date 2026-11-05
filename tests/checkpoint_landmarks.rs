//! Sparse lookup offsets must preserve full-block verification and ordering.
use database_engine::{
    BatchOperation, CheckpointStore, CheckpointView, RecoveredCheckpointView, Store,
};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{ErrorKind, Write},
    os::unix::fs::FileExt,
};

mod support;
use support::retry_lock;

fn key(number: u32) -> Vec<u8> {
    let mut value = b"a-long-shared-key-prefix-".to_vec();
    value.extend_from_slice(&number.to_be_bytes());
    value.resize(100 + (number as usize % 29), b'x');
    value
}

fn prepare(store: &Store, count: u32) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut model = BTreeMap::new();
    for first in (0..count).step_by(100) {
        let rows = (first..(first + 100).min(count))
            .map(|n| (key(n), n.to_le_bytes().to_vec()))
            .collect::<Vec<_>>();
        let operations = rows
            .iter()
            .map(|(k, v)| BatchOperation::Put { key: k, value: v })
            .collect::<Vec<_>>();
        store.write_batch(&operations).unwrap();
        model.extend(rows);
    }
    model
}

fn check(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    get: impl Fn(&[u8]) -> std::io::Result<Option<Vec<u8>>>,
) {
    for (key, value) in model {
        assert_eq!(get(key).unwrap().as_deref(), Some(value.as_slice()));
        let mut absent = key.clone();
        absent.push(0);
        assert_eq!(get(&absent).unwrap(), None);
    }
    assert_eq!(get(b"0").unwrap(), None);
    assert_eq!(get(b"z").unwrap(), None);
}

#[test]
fn sparse_landmarks_preserve_present_absent_and_paged_reads() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    let model = prepare(&initial, 640);
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let view = retry_lock(|| CheckpointView::open(&path, &checkpoint, &digest)).unwrap();
    assert!(view.block_count() > 1);
    check(&model, |key| view.get(key));
    assert_eq!(
        view.scan(None, None, 640).unwrap(),
        model
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    );
    drop(view);
    let recovered =
        retry_lock(|| RecoveredCheckpointView::open(&path, &checkpoint, &digest)).unwrap();
    check(&model, |key| recovered.get(key));
    drop(recovered);
    let writable = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    check(&model, |key| writable.get(key));
}

#[test]
fn sparse_search_still_detects_corruption_elsewhere_in_the_same_block() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    prepare(&store, 160);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = retry_lock(|| CheckpointView::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(view.block_count(), 1);
    assert!(view.get(&key(0)).unwrap().is_some());
    let bytes = std::fs::read(&checkpoint).unwrap();
    let target = key(140);
    let offset = bytes
        .windows(target.len())
        .position(|window| window == target)
        .unwrap()
        + target.len()
        - 1;
    let mut file = OpenOptions::new().write(true).open(&checkpoint).unwrap();
    file.write_all_at(&[bytes[offset] ^ 1], offset as u64)
        .unwrap();
    file.flush().unwrap();
    assert_eq!(
        view.get(&key(0)).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}
