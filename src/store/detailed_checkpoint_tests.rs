use super::*;
use crate::{CheckpointView, PublicationFailure, test_support::retry_lock};

#[test]
fn published_checkpoint_sync_failure_is_reported_as_unknown() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    let trusted_digest = store.checkpoint_to(root.path().join("reference")).unwrap();
    let target = root.path().join("target");
    let result = store.checkpoint_to_detailed_with_sync(&target, |_| {
        Err(io::Error::other("injected parent sync failure"))
    });
    assert!(matches!(result, Err(PublicationFailure::OutcomeUnknown(_))));
    assert!(target.exists());
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
    drop(store);
    let view = retry_lock(|| CheckpointView::open(&path, &target, &trusted_digest)).unwrap();
    assert_eq!(view.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
}
