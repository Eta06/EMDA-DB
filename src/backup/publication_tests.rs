use super::*;
use crate::Store;

#[test]
fn restore_parent_sync_failure_reports_uncertain_publication() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("source")).unwrap();
    store.put(b"key", b"durable").unwrap();
    let snapshot = dir.path().join("backup");
    store.backup(&snapshot).unwrap();
    let restored = dir.path().join("restored");
    let error = restore_with_sync(&snapshot, &restored, |_| {
        Err(io::Error::other("injected directory sync failure"))
    })
    .unwrap_err();
    assert!(error.to_string().contains("restore may exist"));
    assert_eq!(
        Store::open(&restored)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"durable"[..])
    );
    assert!(
        matches!(Store::restore(&snapshot, &restored), Err(e) if e.kind() == io::ErrorKind::AlreadyExists)
    );
}

#[test]
fn backup_parent_sync_failure_reports_uncertain_publication() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source");
    let store = Store::open(&source_path).unwrap();
    store.put(b"key", b"durable").unwrap();
    drop(store);
    let (log, _) = Log::open(&source_path, false).unwrap();
    let snapshot = dir.path().join("backup");
    let error = create_with_sync(&log, &snapshot, |_| {
        Err(io::Error::other("injected directory sync failure"))
    })
    .unwrap_err();
    assert!(error.to_string().contains("backup may exist"));
    let restored = dir.path().join("restored");
    Store::restore(&snapshot, &restored).unwrap();
    assert_eq!(
        Store::open(restored)
            .unwrap()
            .get(b"key")
            .unwrap()
            .as_deref(),
        Some(&b"durable"[..])
    );
    assert!(matches!(create(&log, &snapshot), Err(e) if e.kind() == io::ErrorKind::AlreadyExists));
}
