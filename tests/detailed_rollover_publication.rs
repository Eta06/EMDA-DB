use database_engine::{CheckpointStore, PublicationFailure, Store};
use std::fs;

mod support;
use support::retry_lock;

#[test]
fn detailed_rollover_preserves_old_view_on_rejection_and_adopts_on_success() {
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
        store.put(b"new", b"yes").unwrap();
        let old_tail = store.tail_bytes().unwrap();
        let missing = root.path().join("missing").join("next");
        assert!(matches!(
            store.rollover_to_detailed(&missing),
            Err(PublicationFailure::NotPublished(_))
        ));
        let existing = root.path().join("existing");
        fs::write(&existing, b"do not overwrite").unwrap();
        assert!(matches!(
            store.rollover_to_detailed(&existing),
            Err(PublicationFailure::OutcomeUnknown(_))
        ));
        assert_eq!(fs::read(&existing).unwrap(), b"do not overwrite");
        assert_eq!(store.tail_bytes().unwrap(), old_tail);
        let next = root.path().join("next");
        let next_digest = store.rollover_to_detailed(&next).unwrap();
        assert_eq!(store.tail_bytes().unwrap(), 0);
        assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"yes"[..]));
        drop(store);
        let recovered = retry_lock(|| CheckpointStore::open(&path, &next, &next_digest)).unwrap();
        assert_eq!(
            recovered.get(b"safe").unwrap().as_deref(),
            Some(&b"value"[..])
        );
        assert_eq!(recovered.get(b"new").unwrap().as_deref(), Some(&b"yes"[..]));
    }
}
