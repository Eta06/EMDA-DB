//! Public API coverage for persistent automatic checkpoints.
use database_engine::{
    COMPACTED_CHECKPOINT_FILE, CheckpointSnapshot, CheckpointStore, Store, is_transaction_conflict,
};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

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

fn automatic_slot(store: &Path, slot: &str) -> PathBuf {
    store
        .join(".emda-auto-checkpoints-v1")
        .join(slot)
        .join(COMPACTED_CHECKPOINT_FILE)
}

fn snapshot_pages(snapshot: &CheckpointSnapshot<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let page = snapshot.scan(after.as_deref(), None, 1).unwrap();
        if page.is_empty() {
            break;
        }
        assert_eq!(page.len(), 1);
        after = Some(page[0].0.clone());
        pages.extend(page);
    }
    pages
}

fn assert_captured_view(snapshot: &CheckpointSnapshot<'_>) {
    assert_eq!(
        snapshot
            .get_many(&[b"a", b"c", b"m", b"m", b"z", b"p", b"s1", b"s2"])
            .unwrap(),
        vec![
            Some(b"A".to_vec()),
            None,
            Some(b"M".to_vec()),
            Some(b"M".to_vec()),
            Some(b"Z".to_vec()),
            None,
            None,
            None,
        ]
    );
    // `c` is present in the captured checkpoint and hidden only by the copied
    // tombstone. Exclusive bounds omit `a` and `m`, so a lost tombstone is visible.
    assert!(
        snapshot
            .scan(Some(b"a"), Some(b"m"), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        snapshot_pages(snapshot),
        vec![
            (b"a".to_vec(), b"A".to_vec()),
            (b"m".to_vec(), b"M".to_vec()),
            (b"x".to_vec(), b"X".to_vec()),
            (b"y".to_vec(), b"Y".to_vec()),
            (b"z".to_vec(), b"Z".to_vec()),
        ]
    );
}

#[test]
fn snapshot_and_read_set_survive_automatic_slot_unlink_and_recovery() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let checkpoint = root.path().join("initial");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"a", b"A").unwrap();
    ordinary.put(b"c", b"C").unwrap();
    ordinary.put(b"m", b"M").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);

    // One overlay key of room lets the tombstone and `z` fill the suffix.
    // The next new keys must roll the two automatic slots forward.
    let store =
        CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 4096, 2, 64).unwrap();
    store.rollover_managed(root.path().join("managed")).unwrap();
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"x", b"X").unwrap();
    store.put(b"y", b"Y").unwrap();
    store.put(b"z", b"Z").unwrap();
    store.delete(b"c").unwrap();

    let slot0 = automatic_slot(&path, "slot-0");
    let slot1 = automatic_slot(&path, "slot-1");
    assert!(slot0.is_file());
    assert!(!slot1.exists());
    let original_slot = std::fs::read(&slot0).unwrap();
    let original_inode = std::fs::metadata(&slot0).unwrap().ino();
    let snapshot = store.snapshot().unwrap();
    assert_captured_view(&snapshot);

    store
        .transaction_with_read_set(|transaction| {
            assert_eq!(transaction.get(b"m")?.as_deref(), Some(&b"M"[..]));
            assert_eq!(transaction.get(b"c")?, None);
            assert_eq!(transaction.get(b"z")?.as_deref(), Some(&b"Z"[..]));
            // Exclusive range `(a, n)` records only `m`. The tombstone keeps `c`
            // out of that range; `p`, `q`, and `r` sort at or after `n`.
            assert_eq!(
                transaction.scan(Some(b"a"), Some(b"n"), 10)?,
                vec![(b"m".to_vec(), b"M".to_vec())]
            );
            store.put(b"p", b"P")?;
            store.put(b"q", b"Q")?;
            store.put(b"r", b"R")?;
            let replaced = std::fs::read(&slot0).unwrap();
            assert_ne!(replaced, original_slot);
            assert_ne!(std::fs::metadata(&slot0).unwrap().ino(), original_inode);
            assert!(slot1.is_file());
            // Revalidation reads the replacement checkpoint after slot-0 is unlinked.
            assert_eq!(transaction.get(b"m")?.as_deref(), Some(&b"M"[..]));
            assert_eq!(transaction.get(b"c")?, None);
            assert_eq!(transaction.get(b"z")?.as_deref(), Some(&b"Z"[..]));
            assert_eq!(
                transaction.scan(Some(b"a"), Some(b"n"), 1)?,
                vec![(b"m".to_vec(), b"M".to_vec())]
            );
            transaction.put(b"s1", b"one")?;
            transaction.put(b"s2", b"two")?;
            Ok(())
        })
        .unwrap();
    assert_captured_view(&snapshot);
    assert_eq!(store.get(b"s1").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(store.get(b"s2").unwrap().as_deref(), Some(&b"two"[..]));
    assert_eq!(store.get(b"c").unwrap(), None);

    store.put(b"c", b"C2").unwrap();
    assert_captured_view(&snapshot);
    assert_eq!(store.get(b"c").unwrap().as_deref(), Some(&b"C2"[..]));

    let conflict = store.transaction_with_read_set(|transaction| {
        assert_eq!(transaction.get(b"m")?.as_deref(), Some(&b"M"[..]));
        store.put(b"m", b"M2")?;
        transaction.put(b"must-not", b"no")?;
        Ok(())
    });
    assert!(is_transaction_conflict(&conflict.unwrap_err()));
    assert_captured_view(&snapshot);
    assert_eq!(store.get(b"m").unwrap().as_deref(), Some(&b"M2"[..]));
    assert_eq!(store.get(b"must-not").unwrap(), None);

    drop(snapshot);
    drop(store);
    let recovered =
        CheckpointStore::open_managed_with_resource_limits(&path, 4096, 2, 64, None).unwrap();
    assert!(recovered.automatic_checkpoints_enabled().unwrap());
    for (key, expected) in [
        (b"a".as_slice(), Some(&b"A"[..])),
        (b"c".as_slice(), Some(&b"C2"[..])),
        (b"m".as_slice(), Some(&b"M2"[..])),
        (b"x".as_slice(), Some(&b"X"[..])),
        (b"y".as_slice(), Some(&b"Y"[..])),
        (b"z".as_slice(), Some(&b"Z"[..])),
        (b"p".as_slice(), Some(&b"P"[..])),
        (b"q".as_slice(), Some(&b"Q"[..])),
        (b"r".as_slice(), Some(&b"R"[..])),
        (b"s1".as_slice(), Some(&b"one"[..])),
        (b"s2".as_slice(), Some(&b"two"[..])),
        (b"must-not".as_slice(), None),
    ] {
        assert_eq!(recovered.get(key).unwrap().as_deref(), expected);
    }
}
