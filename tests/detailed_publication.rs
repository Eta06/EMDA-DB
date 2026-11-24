use database_engine::{CheckpointStore, PublicationFailure, Store};
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::symlink;

#[test]
fn occupied_copy_destinations_reject_before_source_reads_or_staging() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("store");
    let store = Store::open(&source).unwrap();
    store.put(b"key", b"value").unwrap();
    let log = fs::read(source.join("records.log")).unwrap();
    let witness = fs::read(source.join("commit.state")).unwrap();
    let absent = root.path().join("absent");
    let file = root.path().join("existing-file");
    let directory = root.path().join("existing-directory");
    let link = root.path().join("existing-link");
    fs::write(&file, b"original").unwrap();
    fs::create_dir(&directory).unwrap();
    symlink(&absent, &link).unwrap();

    // An operation that reaches staging would contend for this lock.
    let parent = fs::File::open(root.path()).unwrap();
    fs2::FileExt::try_lock_exclusive(&parent).unwrap();
    let entries = fs::read_dir(root.path()).unwrap().count();
    for destination in [&file, &directory, &link] {
        for failure in [
            store.backup_detailed(destination),
            Store::restore_detailed(&absent, destination),
            store.upgrade_format_detailed(destination),
        ] {
            assert!(
                matches!(&failure, Err(PublicationFailure::NotPublished(error))
                    if error.kind() == ErrorKind::AlreadyExists),
                "{failure:?}"
            );
        }
    }
    assert_eq!(fs::read(&file).unwrap(), b"original");
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
    assert_eq!(fs::read_link(&link).unwrap(), absent);
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), entries);
    assert_eq!(fs::read(source.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(source.join("commit.state")).unwrap(), witness);

    assert!(matches!(
        Store::restore_detailed(&file, absent.join("restored")),
        Err(PublicationFailure::NotPublished(error)) if error.kind() == ErrorKind::NotFound
    ));
}

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
        Err(PublicationFailure::NotPublished(error)) if error.kind() == std::io::ErrorKind::AlreadyExists
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
        Err(PublicationFailure::NotPublished(error)) if error.kind() == std::io::ErrorKind::AlreadyExists
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
        Err(PublicationFailure::NotPublished(error)) if error.kind() == std::io::ErrorKind::AlreadyExists
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
