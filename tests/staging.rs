use database_engine::{Store, cleanup_abandoned_staging};
use std::io;

#[test]
fn all_publication_apis_reject_the_reserved_staging_namespace() {
    let parent = tempfile::tempdir().unwrap();
    let store = Store::open(parent.path().join("source")).unwrap();
    store.put(b"key", b"value").unwrap();
    let backup = parent.path().join("backup");
    store.backup(&backup).unwrap();
    let reserved = parent.path().join(".emda-stage-v1-user-destination");
    assert_eq!(
        store.backup(&reserved).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        store.checkpoint_to(&reserved).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        store.compact_to(&reserved).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        store.upgrade_format(&reserved).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        Store::restore(&backup, &reserved).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(!reserved.exists());
    std::fs::create_dir(&reserved).unwrap();
    let nested = reserved.join("nested");
    assert_eq!(
        store.backup(&nested).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        cleanup_abandoned_staging(&reserved).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        cleanup_abandoned_staging(parent.path())
            .unwrap()
            .removed_markers,
        0
    );
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
}
