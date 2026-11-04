use super::*;

#[test]
fn automatic_rollover_reuses_two_slots_and_preserves_live_snapshots_and_recovery() {
    let (_dir, path, store) = managed_fixture(1024, 1, 64);
    assert!(!store.automatic_checkpoints_enabled().unwrap());
    store.enable_automatic_checkpoints().unwrap();
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"b", b"two").unwrap();
    let snapshot = store.snapshot().unwrap();
    store.put(b"c", b"three").unwrap();
    store.put(b"d", b"four").unwrap();
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(snapshot.get(b"b").unwrap().as_deref(), Some(&b"two"[..]));
    assert_eq!(snapshot.get(b"c").unwrap(), None);
    assert_eq!(snapshot.get(b"d").unwrap(), None);
    let root = crate::checkpoint_store::automatic::root(&path);
    let mut names = fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["owner", "slot-0", "slot-1"]);
    drop(snapshot);
    drop(store);
    let reopened =
        retry_lock(|| CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None))
            .unwrap();
    assert!(reopened.automatic_checkpoints_enabled().unwrap());
    for (key, expected) in [
        (b"a".as_slice(), b"one".as_slice()),
        (b"b", b"two"),
        (b"c", b"three"),
        (b"d", b"four"),
    ] {
        assert_eq!(reopened.get(key).unwrap().as_deref(), Some(expected));
    }
    reopened.put(b"e", b"five").unwrap();
    assert_eq!(reopened.get(b"e").unwrap().as_deref(), Some(&b"five"[..]));
}

#[test]
fn automatic_rollover_handles_suffix_byte_and_key_byte_limits() {
    for (max_tail, max_key_bytes, first, second) in [
        (60, 64, b"a".as_slice(), b"a".as_slice()),
        (1024, 2, b"a".as_slice(), b"bb".as_slice()),
    ] {
        let (_dir, path, store) = managed_fixture(max_tail, 8, max_key_bytes);
        store.enable_automatic_checkpoints().unwrap();
        store.put(first, &[7; 10]).unwrap();
        store.put(second, &[8; 10]).unwrap();
        assert!(
            crate::checkpoint_store::automatic::root(&path)
                .join("slot-0")
                .is_dir()
        );
        assert_eq!(store.get(second).unwrap().as_deref(), Some(&[8; 10][..]));
    }
}

#[test]
fn impossible_batch_and_whole_log_limit_do_not_create_a_checkpoint() {
    let (_dir, path, store) = managed_fixture(1024, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"one").unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    let batch = [
        BatchOperation::Put {
            key: b"b",
            value: b"two",
        },
        BatchOperation::Put {
            key: b"c",
            value: b"three",
        },
    ];
    assert!(store.write_batch(&batch).is_err());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    let root = crate::checkpoint_store::automatic::root(&path);
    assert!(!root.join("slot-0").exists());
    drop(store);
    let max_log = before.len() as u64;
    let reopened = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, Some(max_log))
    })
    .unwrap();
    assert!(reopened.put(b"b", b"two").is_err());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert!(!root.join("slot-0").exists());
}

#[test]
fn automatic_checkpoints_require_managed_selection_and_protect_reserved_paths() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        store.enable_automatic_checkpoints().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(!crate::checkpoint_store::automatic::root(&path).exists());
    store.rollover_managed(dir.path().join("managed")).unwrap();
    store.enable_automatic_checkpoints().unwrap();
    let root = crate::checkpoint_store::automatic::root(&path);
    assert!(store.checkpoint_to(&root).is_err());
    assert!(store.rollover_managed(root.join("other")).is_err());
    assert!(store.backup(root.join("other")).is_err());
    assert!(store.compact_to(root.join("other")).is_err());
    assert_eq!(
        store.get(b"base").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}

#[test]
fn automatic_rollover_preserves_conditional_and_transaction_semantics() {
    let (_dir, _path, store) = managed_fixture(4096, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"one").unwrap();
    assert!(store.compare_exchange(b"b", None, Some(b"two")).unwrap());
    assert!(
        !store
            .compare_exchange(b"a", Some(b"incorrect"), Some(b"bad"))
            .unwrap()
    );
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"one"[..]));
            tx.put(b"c", b"three")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"two"[..]));
    assert_eq!(store.get(b"c").unwrap().as_deref(), Some(&b"three"[..]));
}

#[test]
fn automatic_rollover_serializes_concurrent_writes() {
    use std::sync::Arc;
    let (_dir, path, store) = managed_fixture(4096, 2, 64);
    store.enable_automatic_checkpoints().unwrap();
    let store = Arc::new(store);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for sequence in 0..8 {
                    let key = format!("{worker}:{sequence}");
                    store.put(key.as_bytes(), key.as_bytes()).unwrap();
                }
            });
        }
    });
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    for worker in 0..4 {
        for sequence in 0..8 {
            let key = format!("{worker}:{sequence}");
            assert_eq!(
                reopened.get(key.as_bytes()).unwrap().as_deref(),
                Some(key.as_bytes())
            );
        }
    }
}

#[test]
fn automatic_checkpoint_configuration_can_be_disabled_and_reenabled_across_reopens() {
    let (_dir, path, store) = managed_fixture(1024, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"first").unwrap();
    store.disable_automatic_checkpoints().unwrap();
    store.disable_automatic_checkpoints().unwrap();
    assert!(!store.automatic_checkpoints_enabled().unwrap());
    let before = fs::read(path.join("records.log")).unwrap();
    assert!(store.put(b"b", b"blocked").is_err());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    drop(store);
    let store =
        retry_lock(|| CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None))
            .unwrap();
    assert!(!store.automatic_checkpoints_enabled().unwrap());
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"b", b"accepted").unwrap();
    drop(store);
    let store =
        retry_lock(|| CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None))
            .unwrap();
    assert!(store.automatic_checkpoints_enabled().unwrap());
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"first"[..]));
    assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"accepted"[..]));
}
