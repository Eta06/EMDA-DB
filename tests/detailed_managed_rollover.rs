use database_engine::{CheckpointStore, CheckpointView, ManagedRolloverFailure, Store};
use std::fs;

mod support;
use support::retry_lock;

#[test]
fn detailed_managed_rollover_preserves_selection_and_reopens_both_formats() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        if v2 {
            fs::create_dir(&path).unwrap();
            fs::write(
                path.join("records.log"),
                include_bytes!("fixtures/v2-empty-records.log"),
            )
            .unwrap();
            fs::write(
                path.join("commit.state"),
                include_bytes!("fixtures/v2-empty-commit.state"),
            )
            .unwrap();
        }
        let store = Store::open(&path).unwrap();
        store.put(b"safe", b"value").unwrap();
        let checkpoint = root.path().join("base");
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.put(b"tail", b"value").unwrap();
        assert!(matches!(
            store.rollover_managed_detailed(root.path().join("missing/next")),
            Err(ManagedRolloverFailure::NotPublished(_))
        ));
        let existing = root.path().join("existing");
        fs::write(&existing, b"preserve").unwrap();
        assert!(matches!(
            store.rollover_managed_detailed(&existing),
            Err(ManagedRolloverFailure::CheckpointOutcomeUnknown(_))
        ));
        assert_eq!(fs::read(&existing).unwrap(), b"preserve");
        assert!(!path.join("checkpoint.selection").exists());
        let selected = root.path().join("selected");
        let selected_digest = store.rollover_managed_detailed(&selected).unwrap();
        let old_selection = fs::read(path.join("checkpoint.selection")).unwrap();
        assert!(matches!(
            store.rollover_managed_detailed(&existing),
            Err(ManagedRolloverFailure::CheckpointOutcomeUnknown(_))
        ));
        assert_eq!(
            fs::read(path.join("checkpoint.selection")).unwrap(),
            old_selection
        );
        assert_eq!(store.get(b"tail").unwrap().as_deref(), Some(&b"value"[..]));
        drop(store);
        let view = retry_lock(|| CheckpointView::open(&path, &selected, &selected_digest)).unwrap();
        assert_eq!(view.get(b"tail").unwrap().as_deref(), Some(&b"value"[..]));
        drop(view);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"safe").unwrap().as_deref(),
            Some(&b"value"[..])
        );
        assert_eq!(
            reopened.get(b"tail").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}
