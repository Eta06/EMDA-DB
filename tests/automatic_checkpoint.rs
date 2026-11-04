//! Public API coverage for persistent automatic checkpoints.
use database_engine::{CheckpointStore, Store};

#[test]
fn automatic_checkpoints_are_persistent_and_keep_snapshot_reads_stable() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let checkpoint = root.path().join("initial");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"base", b"value").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);

    let store =
        CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 1024, 1, 64).unwrap();
    store.rollover_managed(root.path().join("managed")).unwrap();
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"b", b"two").unwrap();
    let snapshot = store.snapshot().unwrap();
    store.put(b"c", b"three").unwrap();
    store.put(b"d", b"four").unwrap();
    assert_eq!(snapshot.get(b"b").unwrap().as_deref(), Some(&b"two"[..]));
    assert_eq!(snapshot.get(b"c").unwrap(), None);
    drop(snapshot);
    store.disable_automatic_checkpoints().unwrap();
    drop(store);

    let store =
        CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None).unwrap();
    assert!(!store.automatic_checkpoints_enabled().unwrap());
    assert_eq!(store.get(b"d").unwrap().as_deref(), Some(&b"four"[..]));
    assert!(store.put(b"e", b"five").is_err());
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"e", b"five").unwrap();
    assert_eq!(store.get(b"e").unwrap().as_deref(), Some(&b"five"[..]));
}
