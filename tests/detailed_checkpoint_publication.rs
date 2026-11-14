use database_engine::{CheckpointStore, CheckpointView, PublicationFailure, Store};
use std::fs;

mod support;
use support::retry_lock;

#[test]
fn detailed_checkpoint_copy_preserves_data_and_existing_destinations() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for checkpoint_writer in [false, true] {
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
            store.put(b"safe", b"yes").unwrap();
            let base = root.path().join("base");
            let base_digest = store.checkpoint_to(&base).unwrap();
            let destination = root.path().join("copy");
            let missing = root.path().join("missing").join("copy");
            let digest = if checkpoint_writer {
                drop(store);
                let store =
                    retry_lock(|| CheckpointStore::open(&path, &base, &base_digest)).unwrap();
                store.put(b"new", b"value").unwrap();
                assert!(matches!(
                    store.checkpoint_to_detailed(&missing),
                    Err(PublicationFailure::NotPublished(_))
                ));
                let digest = store.checkpoint_to_detailed(&destination).unwrap();
                let saved = fs::read(&destination).unwrap();
                assert!(matches!(
                    store.checkpoint_to_detailed(&destination),
                    Err(PublicationFailure::OutcomeUnknown(_))
                ));
                assert_eq!(fs::read(&destination).unwrap(), saved);
                assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
                digest
            } else {
                store.put(b"new", b"value").unwrap();
                assert!(matches!(
                    store.checkpoint_to_detailed(&missing),
                    Err(PublicationFailure::NotPublished(_))
                ));
                let digest = store.checkpoint_to_detailed(&destination).unwrap();
                let saved = fs::read(&destination).unwrap();
                assert!(matches!(
                    store.checkpoint_to_detailed(&destination),
                    Err(PublicationFailure::OutcomeUnknown(_))
                ));
                assert_eq!(fs::read(&destination).unwrap(), saved);
                assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
                drop(store);
                digest
            };
            assert!(!missing.exists());
            let view = retry_lock(|| CheckpointView::open(&path, &destination, &digest)).unwrap();
            assert_eq!(view.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
            assert_eq!(view.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
            drop(view);
            let recovered =
                retry_lock(|| CheckpointStore::open(&path, &destination, &digest)).unwrap();
            assert_eq!(
                recovered.get(b"new").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        }
    }
}
