use database_engine::{
    CheckpointView, CheckpointViewLimits, MAX_RECOVERED_VIEW_TAIL_KEYS, RecoveredCheckpointView,
    RecoveredViewLimits, Store,
};
use std::fs;
use std::io::{self, Write};

fn invalid<T>(result: io::Result<T>, message: &str) {
    match result {
        Err(error) => {
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains(message), "{error}");
        }
        Ok(_) => panic!("expected {message}"),
    }
}

#[test]
fn historical_view_enforces_the_whole_log_limit_before_checkpoint_access() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let missing = directory.path().join("missing");
    let store = Store::open(&path).unwrap();
    store.put(b"before", b"base").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    store.put(b"after", b"suffix").unwrap();
    drop(store);
    let log_path = path.join("records.log");
    let before = fs::read(&log_path).unwrap();
    let length = before.len() as u64;

    invalid(
        CheckpointView::open_with_recovery_limits(
            &path,
            &missing,
            &digest,
            CheckpointViewLimits::default(),
            Some(length - 1),
        ),
        "whole-log byte limit",
    );
    assert!(matches!(
        CheckpointView::open_with_recovery_limits(
            &path, &missing, &digest, CheckpointViewLimits::default(), Some(length),
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound
    ));
    let historical = CheckpointView::open_with_recovery_limits(
        &path,
        &checkpoint,
        &digest,
        CheckpointViewLimits::default(),
        Some(length),
    )
    .unwrap();
    assert_eq!(
        historical.get(b"before").unwrap().as_deref(),
        Some(&b"base"[..])
    );
    assert_eq!(historical.get(b"after").unwrap(), None);
    assert!(historical.has_log_tail());
    assert!(Store::open(&path).is_err());
    drop(historical);
    assert_eq!(fs::read(&log_path).unwrap(), before);
    assert_eq!(
        Store::open(&path)
            .unwrap()
            .get(b"after")
            .unwrap()
            .as_deref(),
        Some(&b"suffix"[..])
    );
}

#[test]
fn recovered_view_combines_log_checkpoint_and_suffix_admission() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let prefix = fs::metadata(path.join("records.log")).unwrap().len();
    store.put(b"a", b"new").unwrap();
    store.put(b"b", b"second").unwrap();
    drop(store);
    let length = fs::metadata(path.join("records.log")).unwrap().len();
    let checkpoint_length = fs::metadata(&checkpoint).unwrap().len();
    let limits = RecoveredViewLimits {
        max_log_bytes: Some(length),
        max_tail_bytes: length - prefix,
        max_tail_keys: 2,
        max_tail_key_bytes: 2,
        checkpoint_view: CheckpointViewLimits {
            max_file_bytes: checkpoint_length,
            max_blocks: 1,
        },
    };
    let view =
        RecoveredCheckpointView::open_with_options(&path, &checkpoint, &digest, limits).unwrap();
    assert_eq!(
        view.scan(None, None, 10).unwrap(),
        vec![
            (b"a".to_vec(), b"new".to_vec()),
            (b"b".to_vec(), b"second".to_vec())
        ]
    );
    drop(view);

    invalid(
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                max_log_bytes: Some(length - 1),
                ..limits
            },
        ),
        "whole-log byte limit",
    );
    invalid(
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                max_tail_bytes: length - prefix - 1,
                ..limits
            },
        ),
        "suffix byte limit",
    );
    invalid(
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                max_tail_keys: 1,
                ..limits
            },
        ),
        "suffix key limit",
    );
    invalid(
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                max_tail_key_bytes: 1,
                ..limits
            },
        ),
        "suffix key byte limit",
    );
    invalid(
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                checkpoint_view: CheckpointViewLimits {
                    max_file_bytes: checkpoint_length - 1,
                    ..limits.checkpoint_view
                },
                ..limits
            },
        ),
        "checkpoint",
    );
    invalid(
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                checkpoint_view: CheckpointViewLimits {
                    max_blocks: 0,
                    ..limits.checkpoint_view
                },
                ..limits
            },
        ),
        "view blocks",
    );
    invalid(
        RecoveredCheckpointView::open_with_options(
            &path,
            &checkpoint,
            &digest,
            RecoveredViewLimits {
                max_tail_keys: MAX_RECOVERED_VIEW_TAIL_KEYS + 1,
                ..limits
            },
        ),
        "supported maximum",
    );

    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(reopened.get(b"b").unwrap().as_deref(), Some(&b"second"[..]));
}

#[test]
fn log_cap_includes_unacknowledged_bytes_and_never_adopts_a_partial_tail() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"base").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let log = path.join("records.log");
    let acknowledged_length = fs::metadata(&log).unwrap().len();
    let mut file = fs::OpenOptions::new().append(true).open(&log).unwrap();
    file.write_all(b"incomplete").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let bytes = fs::read(&log).unwrap();
    invalid(
        CheckpointView::open_with_recovery_limits(
            &path,
            &checkpoint,
            &digest,
            CheckpointViewLimits::default(),
            Some(acknowledged_length),
        ),
        "whole-log byte limit",
    );
    let historical = CheckpointView::open_with_recovery_limits(
        &path,
        &checkpoint,
        &digest,
        CheckpointViewLimits::default(),
        Some(bytes.len() as u64),
    )
    .unwrap();
    assert_eq!(historical.get(b"a").unwrap().as_deref(), Some(&b"base"[..]));
    assert!(historical.has_log_tail());
    drop(historical);
    let result = RecoveredCheckpointView::open_with_options(
        &path,
        &checkpoint,
        &digest,
        RecoveredViewLimits {
            max_log_bytes: Some(bytes.len() as u64),
            ..Default::default()
        },
    );
    assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidData));
    assert_eq!(fs::read(&log).unwrap(), bytes);
    assert!(Store::open(&path).is_err());
}

#[test]
fn whole_log_limit_applies_to_persistent_v2_checkpoint_fixture() {
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
    let log_length = fs::metadata(path.join("records.log")).unwrap().len();
    let limits = RecoveredViewLimits {
        max_log_bytes: Some(log_length),
        ..Default::default()
    };
    let view =
        RecoveredCheckpointView::open_with_options(path, &checkpoint, digest, limits).unwrap();
    assert_eq!(view.get(b"alpha").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(view.get(b"beta").unwrap().as_deref(), Some(&b"two"[..]));
    drop(view);
    invalid(
        RecoveredCheckpointView::open_with_options(
            path,
            &checkpoint,
            digest,
            RecoveredViewLimits {
                max_log_bytes: Some(log_length - 1),
                ..limits
            },
        ),
        "whole-log byte limit",
    );
}
