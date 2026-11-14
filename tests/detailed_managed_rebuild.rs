use database_engine::{CheckpointStore, CheckpointView, ManagedRebuildFailure, Store};
use std::error::Error;
use std::fs;

#[test]
fn external_caller_can_classify_rebuild_failures_and_reopen_the_selection() {
    for v2 in [true, false] {
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
        let initial = Store::open(&path).unwrap();
        initial.put(b"safe", b"acknowledged").unwrap();
        drop(initial);

        let missing = root.path().join("missing/checkpoint");
        let error =
            CheckpointStore::rebuild_managed_selection_detailed(&path, &missing).unwrap_err();
        assert!(matches!(error, ManagedRebuildFailure::NotPublished(_)));
        assert!(error.source().is_some());
        assert!(!missing.exists());

        let first = root.path().join("first");
        let digest = CheckpointStore::rebuild_managed_selection_detailed(&path, &first).unwrap();
        let view = CheckpointView::open(&path, &first, &digest).unwrap();
        assert_eq!(
            view.get(b"safe").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        drop(view);
        let store = CheckpointStore::open_managed(&path).unwrap();
        store.put(b"tail", b"value").unwrap();
        drop(store);

        let selection = fs::read(path.join("checkpoint.selection")).unwrap();
        let existing = root.path().join("existing");
        fs::write(&existing, b"preserve").unwrap();
        assert!(matches!(
            CheckpointStore::rebuild_managed_selection_detailed(&path, &existing),
            Err(ManagedRebuildFailure::CheckpointOutcomeUnknown(_))
        ));
        assert_eq!(fs::read(&existing).unwrap(), b"preserve");
        assert_eq!(
            fs::read(path.join("checkpoint.selection")).unwrap(),
            selection
        );
        let next = root.path().join("next");
        assert!(matches!(
            CheckpointStore::rebuild_managed_selection_with_limits_detailed(
                &path,
                &next,
                Some(1),
                None,
                None
            ),
            Err(ManagedRebuildFailure::NotPublished(_))
        ));
        assert!(!next.exists());
        CheckpointStore::rebuild_managed_selection_with_limits_detailed(
            &path, &next, None, None, None,
        )
        .unwrap();
        let reopened = CheckpointStore::open_managed(&path).unwrap();
        assert_eq!(
            reopened.get(b"tail").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}
