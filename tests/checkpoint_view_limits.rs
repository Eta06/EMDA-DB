//! Per-open checkpoint view admission must never change recovered data.
use database_engine::{
    CheckpointStore, CheckpointStoreLimits, CheckpointView, CheckpointViewLimits,
    MAX_CHECKPOINT_VIEW_BLOCKS, MAX_CHECKPOINT_VIEW_FILE_BYTES, Store,
};
use std::fs;
use std::io::ErrorKind;

mod support;
use support::retry_lock;

#[test]
fn view_rejects_file_and_directory_limits_before_returning_a_view() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let store = Store::open(&path).unwrap();
    for number in 0..80 {
        let key = format!("{number:03}{}", "x".repeat(950));
        store.put(key.as_bytes(), b"value").unwrap();
    }
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let original_log = fs::read(path.join("records.log")).unwrap();
    let file_bytes = fs::metadata(&checkpoint).unwrap().len();
    let view = retry_lock(|| CheckpointView::open(&path, &checkpoint, &digest)).unwrap();
    let blocks = view.block_count();
    assert!(blocks >= 2);
    drop(view);
    for limits in [
        CheckpointViewLimits {
            max_file_bytes: file_bytes - 1,
            max_blocks: blocks,
        },
        CheckpointViewLimits {
            max_file_bytes: file_bytes,
            max_blocks: blocks - 1,
        },
        CheckpointViewLimits {
            max_file_bytes: MAX_CHECKPOINT_VIEW_FILE_BYTES + 1,
            max_blocks: blocks,
        },
        CheckpointViewLimits {
            max_file_bytes: file_bytes,
            max_blocks: MAX_CHECKPOINT_VIEW_BLOCKS + 1,
        },
    ] {
        let error =
            retry_lock(|| CheckpointView::open_with_limits(&path, &checkpoint, &digest, limits))
                .err()
                .expect("limit must reject opening");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
    }
    let limits = CheckpointViewLimits {
        max_file_bytes: file_bytes,
        max_blocks: blocks,
    };
    let view = retry_lock(|| CheckpointView::open_with_limits(&path, &checkpoint, &digest, limits))
        .unwrap();
    assert_eq!(view.len(), 80);
    assert_eq!(view.block_count(), blocks);
    assert_eq!(
        view.get(format!("000{}", "x".repeat(950)).as_bytes())
            .unwrap()
            .as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn empty_checkpoint_can_use_zero_block_directory_capacity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let store = Store::open(&path).unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let limits = CheckpointViewLimits {
        max_file_bytes: fs::metadata(&checkpoint).unwrap().len(),
        max_blocks: 0,
    };
    let view = retry_lock(|| CheckpointView::open_with_limits(&path, &checkpoint, &digest, limits))
        .unwrap();
    assert_eq!(view.len(), 0);
    assert_eq!(view.block_count(), 0);
}

#[test]
fn explicit_rollover_cannot_adopt_an_oversized_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"before").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let limits = CheckpointStoreLimits {
        checkpoint_view: CheckpointViewLimits {
            max_file_bytes: fs::metadata(&checkpoint).unwrap().len(),
            max_blocks: 1,
        },
        ..Default::default()
    };
    let store =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .unwrap();
    assert_eq!(store.limits(), limits);
    let snapshot = store.snapshot().unwrap();
    store.put(b"new-key", b"acknowledged").unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    let destination = root.path().join("too-large");
    let error = store.rollover_to(&destination).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(destination.exists());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(
        store.get(b"new-key").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(snapshot.get(b"new-key").unwrap(), None);
    drop(snapshot);
    drop(store);
    let store =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .unwrap();
    assert_eq!(
        store.get(b"new-key").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    drop(store);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.rollover_to(root.path().join("larger")).unwrap();
    assert_eq!(
        store.get(b"new-key").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}

#[test]
fn rollover_cannot_adopt_an_index_exceeding_the_block_limit() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let initial = Store::open(&path).unwrap();
    for number in 0..30 {
        initial
            .put(
                format!("{number:03}{}", "x".repeat(950)).as_bytes(),
                b"value",
            )
            .unwrap();
    }
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let limits = CheckpointStoreLimits {
        checkpoint_view: CheckpointViewLimits {
            max_blocks: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let store =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .unwrap();
    for number in 30..40 {
        store
            .put(format!("{number:03}{}", "x".repeat(950)).as_bytes(), b"new")
            .unwrap();
    }
    let before = fs::read(path.join("records.log")).unwrap();
    let destination = root.path().join("too-many-blocks");
    assert_eq!(
        store.rollover_to(&destination).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(destination.exists());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(
        store
            .get(format!("039{}", "x".repeat(950)).as_bytes())
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
    drop(store);
    let store =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .unwrap();
    assert_eq!(
        store
            .get(format!("039{}", "x".repeat(950)).as_bytes())
            .unwrap()
            .as_deref(),
        Some(&b"new"[..])
    );
}

#[test]
fn automatic_rollover_limit_rejection_keeps_selection_and_acknowledged_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"before").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let limits = CheckpointStoreLimits {
        max_tail_keys: 1,
        checkpoint_view: CheckpointViewLimits {
            max_file_bytes: fs::metadata(&checkpoint).unwrap().len(),
            max_blocks: 1,
        },
        ..Default::default()
    };
    let store =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .unwrap();
    store.rollover_managed(root.path().join("managed")).unwrap();
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"one", b"acknowledged").unwrap();
    let selection = fs::read(path.join("checkpoint.selection")).unwrap();
    let log = fs::read(path.join("records.log")).unwrap();
    assert_eq!(
        store.put(b"two", b"rejected").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        fs::read(path.join("checkpoint.selection")).unwrap(),
        selection
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
    assert_eq!(
        store.get(b"one").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(store.get(b"two").unwrap(), None);
    drop(store);
    let store = retry_lock(|| CheckpointStore::open_managed_with_options(&path, limits)).unwrap();
    assert_eq!(store.limits(), limits);
    assert_eq!(
        store.get(b"one").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(store.get(b"two").unwrap(), None);
}

#[cfg(target_os = "linux")]
#[test]
fn compaction_handoff_preserves_checkpoint_view_limits() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"before").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let limits = CheckpointStoreLimits {
        checkpoint_view: CheckpointViewLimits {
            max_file_bytes: 1024,
            max_blocks: 1,
        },
        ..Default::default()
    };
    let store =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .unwrap();
    store.put(b"new", b"acknowledged").unwrap();
    let (store, _) = store
        .compact_and_replace(root.path().join("previous"))
        .unwrap();
    assert_eq!(store.limits(), limits);
    assert_eq!(
        store.get(b"new").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    store.rollover_to(root.path().join("next-index")).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn over_limit_compaction_candidate_does_not_replace_the_source() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let initial = Store::open(&path).unwrap();
    initial.put(b"base", b"before").unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let limits = CheckpointStoreLimits {
        checkpoint_view: CheckpointViewLimits {
            max_file_bytes: fs::metadata(&checkpoint).unwrap().len(),
            max_blocks: 1,
        },
        ..Default::default()
    };
    let store =
        retry_lock(|| CheckpointStore::open_with_options(&path, &checkpoint, &digest, limits))
            .unwrap();
    store.put(b"new", b"acknowledged").unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    let previous = root.path().join("previous");
    let error = store
        .compact_and_replace(&previous)
        .err()
        .expect("candidate must exceed its cap");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert!(previous.exists());
    let original = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        original.get(b"new").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    let candidate = retry_lock(|| Store::open(&previous)).unwrap();
    assert_eq!(
        candidate.get(b"new").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}
