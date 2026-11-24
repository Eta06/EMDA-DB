use super::*;
use crate::{CheckpointView, PublicationFailure, test_support::retry_lock};

#[test]
fn ordinary_checkpoint_scan_limit_admits_exact_reads_and_still_checks_history() {
    use crate::log_io::replay_fault::ReplaceAfterRead;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let store = Store::open(&path).unwrap();
    let start = store.log.len().unwrap();
    store.put(b"key", b"old").unwrap();
    let end = store.log.len().unwrap();
    store.put(b"key", b"new").unwrap();
    let log_path = path.join("records.log");
    let log_bytes = std::fs::read(&log_path).unwrap();
    let target = root.path().join("target");
    let exact = 2 * store.log.len().unwrap();
    drop(store);
    let replacement = record::encode(Operation::Put, b"key", b"bad").unwrap();
    assert_eq!(end - start, replacement.len() as u64);
    let fired = Arc::new(AtomicBool::new(false));
    let store = retry_lock(|| {
        Store::open_using(&path, |path, initialize| {
            let (mut log, index) = Log::open(path, initialize)?;
            log.replace_io_for_test(Box::new(ReplaceAfterRead {
                file: OpenOptions::new().read(true).write(true).open(&log_path)?,
                observed: 0..log.len()?,
                replace_at: start,
                replacement: replacement.clone(),
                fired: Arc::clone(&fired),
            }));
            Ok((log, index))
        })
    })
    .unwrap();
    let refused = store
        .checkpoint_to_with_scan_limit(&target, exact - 1)
        .unwrap_err();
    assert!(matches!(refused, PublicationFailure::NotPublished(error)
        if error.kind() == io::ErrorKind::InvalidInput
            && error.to_string() == "checkpoint log scan byte limit exceeded"));
    assert!(!fired.load(Ordering::Relaxed));
    assert!(!target.exists());
    assert_eq!(std::fs::read(&log_path).unwrap(), log_bytes);

    let corrupt = store
        .checkpoint_to_with_scan_limit(&target, exact)
        .unwrap_err();
    assert!(matches!(corrupt, PublicationFailure::NotPublished(error)
        if error.kind() == io::ErrorKind::InvalidData
            && error.to_string() == "checkpoint does not match log prefix"));
    assert!(fired.load(Ordering::Relaxed));
    assert!(!target.exists());
    std::fs::write(&log_path, log_bytes).unwrap();
    let digest = store.checkpoint_to_with_scan_limit(&target, exact).unwrap();
    drop(store);
    let view = retry_lock(|| CheckpointView::open(&path, &target, &digest)).unwrap();
    assert_eq!(view.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn published_checkpoint_sync_failure_is_reported_as_unknown() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    let trusted_digest = store.checkpoint_to(root.path().join("reference")).unwrap();
    let target = root.path().join("target");
    let result = store.checkpoint_to_detailed_using(
        &target,
        |_| Err(io::Error::other("injected parent sync failure")),
        Some(2 * store.log.len().unwrap()),
    );
    assert!(matches!(result, Err(PublicationFailure::OutcomeUnknown(_))));
    assert!(target.exists());
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
    drop(store);
    let view = retry_lock(|| CheckpointView::open(&path, &target, &trusted_digest)).unwrap();
    assert_eq!(view.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
}
