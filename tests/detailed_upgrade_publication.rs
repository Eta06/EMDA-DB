use database_engine::{PublicationFailure, Store};
use std::fs;

mod support;
use support::retry_lock;

#[test]
fn detailed_format_upgrade_preserves_source_and_existing_destination() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source");
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
        let source_bytes = fs::read(path.join("records.log")).unwrap();
        let destination = root.path().join("copy");
        let missing = root.path().join("missing").join("copy");
        assert!(matches!(
            store.upgrade_format_detailed(&missing),
            Err(PublicationFailure::NotPublished(_))
        ));
        assert!(matches!(
            store.upgrade_legacy_detailed(root.path().join("not-legacy")),
            Err(PublicationFailure::NotPublished(_))
        ));
        store.upgrade_format_detailed(&destination).unwrap();
        let copy_bytes = fs::read(destination.join("records.log")).unwrap();
        assert!(matches!(
            store.upgrade_format_detailed(&destination),
            Err(PublicationFailure::OutcomeUnknown(_))
        ));
        assert_eq!(
            fs::read(destination.join("records.log")).unwrap(),
            copy_bytes
        );
        assert_eq!(fs::read(path.join("records.log")).unwrap(), source_bytes);
        assert_eq!(store.get(b"safe").unwrap().as_deref(), Some(&b"value"[..]));
        let copy = retry_lock(|| Store::open(&destination)).unwrap();
        assert_eq!(copy.get(b"safe").unwrap().as_deref(), Some(&b"value"[..]));
    }
}

#[test]
fn detailed_legacy_upgrade_classifies_empty_and_nonempty_sources() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("legacy");
    fs::create_dir(&source).unwrap();
    let bytes = include_bytes!("fixtures/db01-legacy-record.bin");
    fs::write(source.join("records.log"), bytes).unwrap();
    let store = Store::open(&source).unwrap();
    let missing = root.path().join("missing").join("copy");
    assert!(matches!(
        store.upgrade_legacy_detailed(&missing),
        Err(PublicationFailure::NotPublished(_))
    ));
    let target = root.path().join("copy");
    store.upgrade_legacy_detailed(&target).unwrap();
    assert!(matches!(
        store.upgrade_legacy_detailed(&target),
        Err(PublicationFailure::OutcomeUnknown(_))
    ));
    assert_eq!(fs::read(source.join("records.log")).unwrap(), bytes);
    let copy = retry_lock(|| Store::open(&target)).unwrap();
    assert_eq!(
        copy.get(b"legacy").unwrap().as_deref(),
        Some(&b"persisted"[..])
    );
    drop(copy);
    drop(store);
    assert!(matches!(
        Store::upgrade_empty_legacy_detailed(&source, root.path().join("nonempty")),
        Err(PublicationFailure::NotPublished(_))
    ));
    let empty = root.path().join("empty");
    fs::create_dir(&empty).unwrap();
    fs::write(empty.join("records.log"), []).unwrap();
    let empty_target = root.path().join("empty-copy");
    Store::upgrade_empty_legacy_detailed(&empty, &empty_target).unwrap();
    assert!(matches!(
        Store::upgrade_empty_legacy_detailed(&empty, &empty_target),
        Err(PublicationFailure::OutcomeUnknown(_))
    ));
    assert!(fs::read(empty.join("records.log")).unwrap().is_empty());
    assert_eq!(
        retry_lock(|| Store::open(&empty_target))
            .unwrap()
            .get(b"legacy")
            .unwrap(),
        None
    );
}
