#![no_main]
use database_engine::{CheckpointStore, ManagedRolloverFailure, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

fn check(store: &CheckpointStore, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let expected = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(store.scan(None, None, 100).expect("scan"), expected);
    for index in 0..8 {
        let key = vec![b'a' + index];
        assert_eq!(store.get(&key).expect("read"), model.get(&key).cloned());
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 128 {
        return;
    }
    let root = tempfile::tempdir().expect("temporary directory");
    let path = root.path().join("store");
    let initial = root.path().join("initial");
    let ordinary = Store::open(&path).expect("create");
    let mut model = BTreeMap::new();
    ordinary.put(b"a", b"base").expect("base");
    model.insert(b"a".to_vec(), b"base".to_vec());
    let digest = ordinary.checkpoint_to(&initial).expect("checkpoint");
    drop(ordinary);
    let mut store = CheckpointStore::open(&path, &initial, &digest).expect("open");
    let mut generation = 0_usize;
    let mut peak_keys = model.len();
    if data.first().is_some_and(|byte| byte & 1 != 0) {
        store
            .rollover_managed_detailed(root.path().join("managed-0"))
            .expect("first detailed managed selection");
    } else {
        store
            .rollover_managed(root.path().join("managed-0"))
            .expect("first managed selection");
    }
    let selection = path.join("checkpoint.selection");
    for bytes in data.chunks_exact(3) {
        let key = vec![b'a' + bytes[1] % 8];
        match bytes[0] % 10 {
            0 => {
                let value = vec![bytes[2]; usize::from(bytes[2] % 5)];
                store.put(&key, &value).expect("put");
                model.insert(key, value);
            }
            1 => {
                store.delete(&key).expect("delete");
                model.remove(&key);
            }
            2 => {
                generation += 1;
                let before = model.clone();
                let snapshot = store.snapshot().expect("snapshot");
                let next = root.path().join(format!("managed-{generation}"));
                if bytes[2] & 1 != 0 {
                    store
                        .rollover_managed_detailed(next)
                        .expect("detailed rollover");
                } else {
                    store.rollover_managed(next).expect("rollover");
                }
                assert_eq!(
                    snapshot.scan(None, None, 100).expect("snapshot scan"),
                    before.into_iter().collect::<Vec<_>>()
                );
            }
            3 => {
                check(&store, &model);
                drop(store);
                store = CheckpointStore::open_managed(&path).expect("reopen");
            }
            4 => {
                check(&store, &model);
                drop(store);
                let original = std::fs::read(&selection).expect("selection");
                let mut corrupt = original.clone();
                let index = usize::from(bytes[1]) % corrupt.len();
                corrupt[index] ^= (bytes[2] | 1).max(1);
                std::fs::write(&selection, &corrupt).expect("corrupt selection");
                assert!(CheckpointStore::open_managed(&path).is_err());
                assert!(CheckpointStore::open(&path, &initial, &digest).is_err());
                let rejected = root.path().join(format!("rejected-{generation}"));
                assert!(CheckpointStore::rebuild_managed_selection(&path, &rejected).is_err());
                assert!(!rejected.exists());
                std::fs::write(&selection, original).expect("restore selection");
                store = CheckpointStore::open_managed(&path).expect("reopen valid selection");
            }
            5 => {
                assert!(store.rollover_to(root.path().join("unmanaged")).is_err());
            }
            6 => {
                check(&store, &model);
                drop(store);
                generation += 1;
                CheckpointStore::rebuild_managed_selection(
                    &path,
                    root.path().join(format!("managed-{generation}")),
                )
                .expect("rebuild selection");
                store = CheckpointStore::open_managed(&path).expect("reopen rebuilt selection");
                assert_eq!(store.overlay_keys().expect("empty suffix"), 0);
            }
            7 => {
                check(&store, &model);
                drop(store);
                generation += 1;
                let next = root.path().join(format!("managed-{generation}"));
                let selection_before = std::fs::read(&selection).expect("selection");
                let log_length = std::fs::metadata(path.join("records.log"))
                    .expect("log metadata")
                    .len();
                let denied = bytes[2] & 1 != 0;
                let cap = if denied { peak_keys - 1 } else { peak_keys };
                // All generated keys have one byte, so the count and
                // key-byte historical peaks are equal in this model.
                let result = CheckpointStore::rebuild_managed_selection_with_limits(
                    &path,
                    &next,
                    Some(log_length),
                    Some(cap),
                    Some(cap),
                );
                if denied {
                    assert!(result.is_err());
                    assert!(!next.exists());
                    assert_eq!(
                        std::fs::read(&selection).expect("selection"),
                        selection_before
                    );
                } else {
                    result.expect("limited rebuild");
                }
                store = CheckpointStore::open_managed(&path).expect("reopen after limited rebuild");
                if !denied {
                    assert_eq!(store.overlay_keys().expect("empty suffix"), 0);
                }
            }
            8 => {
                generation += 1;
                let blocked = root.path().join(format!("blocked-{generation}"));
                std::fs::write(&blocked, b"pre-existing checkpoint").expect("sentinel");
                let previous = std::fs::read(&selection).expect("selection");
                let snapshot = store.snapshot().expect("snapshot before collision");
                assert!(matches!(
                    store.rollover_managed_detailed(&blocked),
                    Err(ManagedRolloverFailure::CheckpointOutcomeUnknown(_))
                ));
                assert_eq!(
                    std::fs::read(&blocked).expect("sentinel after"),
                    b"pre-existing checkpoint"
                );
                assert_eq!(
                    std::fs::read(&selection).expect("selection after"),
                    previous
                );
                assert_eq!(
                    snapshot
                        .scan(None, None, 100)
                        .expect("snapshot after collision"),
                    model
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<Vec<_>>()
                );
                drop(snapshot);
                if bytes[1] & 1 != 0 {
                    drop(store);
                    store = CheckpointStore::open_managed(&path).expect("reopen after collision");
                }
            }
            _ => {
                generation += 1;
                let missing = root
                    .path()
                    .join(format!("missing-{generation}"))
                    .join("checkpoint");
                let previous = std::fs::read(&selection).expect("selection");
                assert!(matches!(
                    store.rollover_managed_detailed(&missing),
                    Err(ManagedRolloverFailure::NotPublished(_))
                ));
                assert!(!missing.exists());
                assert_eq!(
                    std::fs::read(&selection).expect("selection after"),
                    previous
                );
                if bytes[1] & 1 != 0 {
                    drop(store);
                    store = CheckpointStore::open_managed(&path).expect("reopen after rejection");
                }
            }
        }
        peak_keys = peak_keys.max(model.len());
        check(&store, &model);
    }
    drop(store);
    let reopened = CheckpointStore::open_managed(&path).expect("final recovery");
    check(&reopened, &model);
});
