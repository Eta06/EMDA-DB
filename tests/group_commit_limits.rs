//! Configurable ordinary-store group-commit admission preserves atomicity.
use database_engine::{
    BatchOperation, MAX_GROUP_COMMIT_BUFFERED_BYTES, MAX_GROUP_COMMIT_BUFFERED_RECORDS, Store,
};
use std::{fs, io::ErrorKind};

mod support;
use support::retry_lock;

#[test]
fn lower_limits_reject_before_submission_and_reset_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"old").unwrap();
    let initial = store.group_commit_status().unwrap();
    assert_eq!(
        (initial.limit_bytes, initial.limit_records),
        (
            MAX_GROUP_COMMIT_BUFFERED_BYTES,
            MAX_GROUP_COMMIT_BUFFERED_RECORDS
        )
    );
    assert_eq!(
        (
            initial.reserved_bytes,
            initial.reserved_records,
            initial.waiting_writers
        ),
        (0, 0, 0)
    );
    assert!(!initial.failed);
    assert_eq!(
        store
            .set_group_commit_limits(MAX_GROUP_COMMIT_BUFFERED_BYTES + 1, 1)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store
            .set_group_commit_limits(1, MAX_GROUP_COMMIT_BUFFERED_RECORDS + 1)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(store.group_commit_status().unwrap(), initial);

    store.set_group_commit_limits(0, 0).unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    assert_eq!(
        store.put(b"b", b"new").unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    assert_eq!(
        store
            .compare_exchange(b"a", Some(b"old"), Some(b"new"))
            .unwrap_err()
            .kind(),
        ErrorKind::WouldBlock
    );
    assert_eq!(
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"a",
                    value: b"new"
                },
                BatchOperation::Put {
                    key: b"b",
                    value: b"new"
                }
            ])
            .unwrap_err()
            .kind(),
        ErrorKind::WouldBlock
    );
    assert_eq!(
        store
            .transaction(|tx| tx.put(b"b", b"new"))
            .unwrap_err()
            .kind(),
        ErrorKind::WouldBlock
    );
    assert_eq!(
        store.transaction(|tx| tx.get(b"a")).unwrap().as_deref(),
        Some(&b"old"[..])
    );
    assert!(
        !store
            .compare_exchange(b"a", Some(b"different"), Some(b"new"))
            .unwrap()
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(store.get(b"b").unwrap(), None);
    drop(store);

    let reopened = retry_lock(|| Store::open(&path)).unwrap();
    let status = reopened.group_commit_status().unwrap();
    assert_eq!(
        (status.limit_bytes, status.limit_records),
        (
            MAX_GROUP_COMMIT_BUFFERED_BYTES,
            MAX_GROUP_COMMIT_BUFFERED_RECORDS
        )
    );
    assert_eq!(reopened.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(reopened.get(b"b").unwrap(), None);
    reopened.put(b"b", b"after reopen").unwrap();
}

#[test]
fn admission_rejection_releases_a_log_quota_reservation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let store = Store::open(&path).unwrap();
    let before = fs::metadata(path.join("records.log")).unwrap().len();
    store.put(b"x", b"y").unwrap();
    let current = fs::metadata(path.join("records.log")).unwrap().len();
    let record_bytes = current - before;
    drop(store);
    let store = retry_lock(|| Store::open_with_log_limit(&path, current + record_bytes)).unwrap();
    store.set_group_commit_limits(0, 0).unwrap();
    assert_eq!(
        store.put(b"x", b"z").unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    store
        .set_group_commit_limits(
            MAX_GROUP_COMMIT_BUFFERED_BYTES,
            MAX_GROUP_COMMIT_BUFFERED_RECORDS,
        )
        .unwrap();
    store.put(b"x", b"z").unwrap();
    assert_eq!(
        fs::metadata(path.join("records.log")).unwrap().len(),
        current + record_bytes
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&path))
            .unwrap()
            .get(b"x")
            .unwrap()
            .as_deref(),
        Some(&b"z"[..])
    );
}

#[cfg(target_os = "linux")]
#[test]
fn compact_handoff_preserves_limits_but_a_later_reopen_resets_them() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("live");
    let previous = root.path().join("previous");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"durable").unwrap();
    store.set_group_commit_limits(0, 0).unwrap();
    let store = store.compact_and_replace(&path, &previous).unwrap();
    let status = store.group_commit_status().unwrap();
    assert_eq!((status.limit_bytes, status.limit_records), (0, 0));
    assert_eq!(
        store.put(b"b", b"rejected").unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"durable"[..]));
    drop(store);
    let reopened = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        reopened.group_commit_status().unwrap().limit_records,
        MAX_GROUP_COMMIT_BUFFERED_RECORDS
    );
    reopened.put(b"b", b"allowed").unwrap();
}
