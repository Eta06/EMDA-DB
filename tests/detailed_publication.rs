use database_engine::{CheckpointStore, PublicationFailure, Store};
use std::fs;

#[test]
fn detailed_backup_and_restore_preserve_existing_paths_and_recover_data() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let backup = root.path().join("ordinary.backup");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();

    let missing_parent = root.path().join("missing").join("backup");
    assert!(matches!(
        store.backup_detailed(&missing_parent),
        Err(PublicationFailure::NotPublished(_))
    ));
    assert!(!missing_parent.exists());
    store.backup_detailed(&backup).unwrap();
    let saved = fs::read(&backup).unwrap();
    assert!(matches!(
        store.backup_detailed(&backup),
        Err(PublicationFailure::OutcomeUnknown(_))
    ));
    assert_eq!(fs::read(&backup).unwrap(), saved);
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);

    let restored = root.path().join("restored");
    assert!(matches!(
        Store::restore_detailed(root.path().join("missing.backup"), &restored),
        Err(PublicationFailure::NotPublished(_))
    ));
    assert!(!restored.exists());
    Store::restore_detailed(&backup, &restored).unwrap();
    assert_eq!(
        Store::open(&restored)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"value"[..])
    );
    assert!(matches!(
        Store::restore_detailed(&backup, &restored),
        Err(PublicationFailure::OutcomeUnknown(_))
    ));
    assert_eq!(
        Store::open(&restored)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"value"[..])
    );

    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let checkpoint_backup = root.path().join("checkpoint.backup");
    assert!(matches!(
        store.backup_detailed(&missing_parent),
        Err(PublicationFailure::NotPublished(_))
    ));
    store.backup_detailed(&checkpoint_backup).unwrap();
    let saved = fs::read(&checkpoint_backup).unwrap();
    assert!(matches!(
        store.backup_detailed(&checkpoint_backup),
        Err(PublicationFailure::OutcomeUnknown(_))
    ));
    assert_eq!(fs::read(&checkpoint_backup).unwrap(), saved);
    let restored = root.path().join("checkpoint-restored");
    Store::restore_detailed(&checkpoint_backup, &restored).unwrap();
    assert_eq!(
        Store::open(&restored)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn corrupt_backup_fails_before_this_call_publishes() {
    let root = tempfile::tempdir().unwrap();
    let backup = root.path().join("corrupt");
    let restored = root.path().join("restored");
    fs::write(&backup, b"not a backup").unwrap();
    assert!(matches!(
        Store::restore_detailed(&backup, &restored),
        Err(PublicationFailure::NotPublished(_))
    ));
    assert!(!restored.exists());
    assert_eq!(fs::read(backup).unwrap(), b"not a backup");
}
