#![no_main]
use database_engine::{CheckpointStore, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

fn check(store: &CheckpointStore, model: &BTreeMap<Vec<u8>, Vec<u8>>, enabled: bool) {
    assert_eq!(
        store.scan(None, None, 100).expect("scan"),
        model
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        store
            .automatic_checkpoints_enabled()
            .expect("configuration"),
        enabled
    );
}
fn check_old(store: &Store, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    assert_eq!(
        store.scan(None, None, 100).expect("old scan"),
        model
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    );
}
fuzz_target!(|data: &[u8]| {
    if data.len() > 64 {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let source = root.path().join("live");
    let base = root.path().join("base");
    let ordinary = Store::open(&source).expect("open");
    ordinary.put(b"z", b"base").expect("base");
    let digest = ordinary.checkpoint_to(&base).expect("checkpoint");
    drop(ordinary);
    let mut store = CheckpointStore::open_with_limits(&source, &base, &digest, 4096, 2, 64)
        .expect("checkpoint store");
    store
        .rollover_managed(root.path().join("managed"))
        .expect("managed");
    store.enable_automatic_checkpoints().expect("enable");
    let mut enabled = true;
    let mut model = BTreeMap::new();
    model.insert(b"z".to_vec(), b"base".to_vec());
    let mut old = Vec::new();
    for chunk in data.chunks_exact(3) {
        let key = vec![b'a' + chunk[1] % 6];
        let value = vec![chunk[2]; usize::from(chunk[2] % 5)];
        match chunk[0] % 5 {
            0 | 1 => {
                let before = std::fs::read(source.join("records.log")).expect("log");
                let result = if chunk[0] % 5 == 0 {
                    store.put(&key, &value)
                } else {
                    store.delete(&key)
                };
                if result.is_ok() {
                    if chunk[0] % 5 == 0 {
                        model.insert(key, value);
                    } else {
                        model.remove(&key);
                    }
                } else {
                    assert!(!enabled);
                    assert_eq!(
                        std::fs::read(source.join("records.log")).expect("log"),
                        before
                    );
                }
            }
            2 => {
                let previous = root.path().join(format!("previous-{}", old.len()));
                (store, _) = store
                    .compact_and_replace(&previous)
                    .expect("compact exchange");
                let old_store = Store::open(&previous).expect("open old");
                check_old(&old_store, &model);
                drop(old_store);
                old.push((previous, model.clone()));
            }
            3 => {
                if enabled {
                    store.disable_automatic_checkpoints().expect("disable");
                } else {
                    store.enable_automatic_checkpoints().expect("enable");
                }
                enabled = !enabled;
            }
            _ => {
                drop(store);
                store =
                    CheckpointStore::open_managed_with_resource_limits(&source, 4096, 2, 64, None)
                        .expect("reopen");
            }
        }
        check(&store, &model, enabled);
    }
    drop(store);
    let store = CheckpointStore::open_managed_with_resource_limits(&source, 4096, 2, 64, None)
        .expect("final reopen");
    check(&store, &model, enabled);
    for (path, model) in old {
        check_old(&Store::open(path).expect("old reopen"), &model);
    }
});
