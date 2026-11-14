use database_engine::{COMPACTED_CHECKPOINT_FILE, CheckpointStore, PublicationFailure, Store};
use std::fs;

mod support;
use support::retry_lock;

#[test]
fn detailed_compaction_copies_current_data_without_overwriting_destinations() {
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
            store.put(b"deleted", b"old").unwrap();
            store.delete(b"deleted").unwrap();
            let checkpoint = root.path().join("checkpoint");
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            let destination = root.path().join("copy");
            let missing = root.path().join("missing").join("copy");
            let copy_digest = if checkpoint_writer {
                drop(store);
                let store =
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
                store.put(b"new", b"value").unwrap();
                assert!(matches!(
                    store.compact_to_detailed(&missing),
                    Err(PublicationFailure::NotPublished(_))
                ));
                let digest = store.compact_to_detailed(&destination).unwrap();
                let saved = fs::read(destination.join("records.log")).unwrap();
                assert!(matches!(
                    store.compact_to_detailed(&destination),
                    Err(PublicationFailure::OutcomeUnknown(_))
                ));
                assert_eq!(fs::read(destination.join("records.log")).unwrap(), saved);
                assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
                Some(digest)
            } else {
                store.put(b"new", b"value").unwrap();
                assert!(matches!(
                    store.compact_to_detailed(&missing),
                    Err(PublicationFailure::NotPublished(_))
                ));
                store.compact_to_detailed(&destination).unwrap();
                let saved = fs::read(destination.join("records.log")).unwrap();
                assert!(matches!(
                    store.compact_to_detailed(&destination),
                    Err(PublicationFailure::OutcomeUnknown(_))
                ));
                assert_eq!(fs::read(destination.join("records.log")).unwrap(), saved);
                assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
                drop(store);
                None
            };
            assert!(!missing.exists());
            let copy = retry_lock(|| Store::open(&destination)).unwrap();
            assert_eq!(copy.get(b"safe").unwrap().as_deref(), Some(&b"yes"[..]));
            assert_eq!(copy.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
            assert_eq!(copy.get(b"deleted").unwrap(), None);
            drop(copy);
            if let Some(copy_digest) = copy_digest {
                let compacted_checkpoint = destination.join(COMPACTED_CHECKPOINT_FILE);
                let copy = retry_lock(|| {
                    CheckpointStore::open(&destination, &compacted_checkpoint, &copy_digest)
                })
                .unwrap();
                assert_eq!(copy.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
                assert_eq!(copy.get(b"deleted").unwrap(), None);
            }
        }
    }
}
