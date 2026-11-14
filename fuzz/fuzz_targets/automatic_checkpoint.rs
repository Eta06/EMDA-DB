#![no_main]
use database_engine::{BatchOperation, CheckpointStore, PublicationFailure, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

fn key(byte: u8) -> Vec<u8> {
    vec![b'a' + byte % 8]
}
fn check(store: &CheckpointStore, model: &BTreeMap<Vec<u8>, Vec<u8>>, enabled: bool) {
    let expected = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(store.scan(None, None, 100).expect("scan"), expected);
    assert_eq!(
        store
            .automatic_checkpoints_enabled()
            .expect("configuration"),
        enabled
    );
    for number in 0..8 {
        let key = key(number);
        assert_eq!(store.get(&key).expect("read"), model.get(&key).cloned());
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 128 {
        return;
    }
    let max_keys = usize::from(data[0] % 3 + 1);
    let root = tempfile::tempdir().expect("temporary directory");
    let path = root.path().join("store");
    let initial = root.path().join("initial");
    if data[0] & 0x80 != 0 {
        std::fs::create_dir(&path).expect("V2 directory");
        std::fs::write(
            path.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .expect("V2 log");
        std::fs::write(
            path.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .expect("V2 witness");
    }
    let ordinary = Store::open(&path).expect("create");
    ordinary.put(b"z", b"base").expect("base");
    let digest = ordinary.checkpoint_to(&initial).expect("checkpoint");
    drop(ordinary);
    let mut store = CheckpointStore::open_with_limits(&path, &initial, &digest, 4096, max_keys, 64)
        .expect("open");
    if data[0] & 0x40 != 0 {
        assert!(matches!(
            store.enable_automatic_checkpoints_detailed(),
            Err(PublicationFailure::NotPublished(_))
        ));
        assert_eq!(
            store.get(b"z").expect("base after rejection"),
            Some(b"base".to_vec())
        );
    }
    store
        .rollover_managed(root.path().join("managed-0"))
        .expect("managed selection");
    store
        .enable_automatic_checkpoints()
        .expect("enable automatic checkpoints");
    let mut enabled = true;
    let mut model = BTreeMap::new();
    model.insert(b"z".to_vec(), b"base".to_vec());
    let mut generation = 0_usize;
    for bytes in data[1..].chunks_exact(3) {
        let first = key(bytes[1]);
        let second = key(bytes[2]);
        let value = vec![bytes[2]; usize::from(bytes[2] % 5)];
        match bytes[0] % 8 {
            0 | 1 => {
                let before = std::fs::read(path.join("records.log")).expect("log");
                let result = if bytes[0] % 8 == 0 {
                    store.put(&first, &value)
                } else {
                    store.delete(&first)
                };
                if result.is_ok() {
                    if bytes[0] % 8 == 0 {
                        model.insert(first, value);
                    } else {
                        model.remove(&first);
                    }
                } else {
                    assert!(
                        !enabled,
                        "enabled single-key write should fit after rollover"
                    );
                    assert_eq!(
                        std::fs::read(path.join("records.log")).expect("log"),
                        before
                    );
                }
            }
            2 => {
                let before = std::fs::read(path.join("records.log")).expect("log");
                let operations = [
                    BatchOperation::Put {
                        key: &first,
                        value: &value,
                    },
                    BatchOperation::Delete { key: &second },
                ];
                let result = store.write_batch(&operations);
                if result.is_ok() {
                    model.insert(first, value);
                    model.remove(&second);
                } else {
                    assert!(!enabled || (max_keys == 1 && first != second));
                    assert_eq!(
                        std::fs::read(path.join("records.log")).expect("log"),
                        before
                    );
                }
            }
            3 => {
                let before = model.clone();
                let snapshot = store.snapshot().expect("snapshot");
                let original_log = std::fs::read(path.join("records.log")).expect("log");
                let result = store.put(&first, &value);
                if result.is_ok() {
                    model.insert(first, value);
                } else {
                    assert!(!enabled);
                    assert_eq!(
                        std::fs::read(path.join("records.log")).expect("log"),
                        original_log
                    );
                }
                assert_eq!(
                    snapshot.scan(None, None, 100).expect("snapshot scan"),
                    before.into_iter().collect::<Vec<_>>()
                );
            }
            4 => {
                check(&store, &model, enabled);
                drop(store);
                store = CheckpointStore::open_managed_with_resource_limits(
                    &path, 4096, max_keys, 64, None,
                )
                .expect("reopen");
            }
            5 => {
                match (enabled, bytes[2] & 1 != 0) {
                    (true, false) => store.disable_automatic_checkpoints().expect("disable"),
                    (true, true) => store
                        .disable_automatic_checkpoints_detailed()
                        .expect("detailed disable"),
                    (false, false) => store.enable_automatic_checkpoints().expect("enable"),
                    (false, true) => store
                        .enable_automatic_checkpoints_detailed()
                        .expect("detailed enable"),
                }
                enabled = !enabled;
                if bytes[2] & 2 != 0 {
                    if enabled {
                        store
                            .enable_automatic_checkpoints_detailed()
                            .expect("idempotent enable");
                    } else {
                        store
                            .disable_automatic_checkpoints_detailed()
                            .expect("idempotent disable");
                    }
                }
            }
            6 => {
                generation += 1;
                store
                    .rollover_managed(root.path().join(format!("manual-{generation}")))
                    .expect("manual rollover");
            }
            _ => {
                let before = std::fs::read(path.join("records.log")).expect("log");
                let result = store.transaction_with_read_set(|transaction| {
                    let _ = transaction.get(&first)?;
                    transaction.put(&first, &value)?;
                    Ok(())
                });
                if result.is_ok() {
                    model.insert(first, value);
                } else {
                    assert!(!enabled);
                    assert_eq!(
                        std::fs::read(path.join("records.log")).expect("log"),
                        before
                    );
                }
            }
        }
        check(&store, &model, enabled);
    }
    drop(store);
    let reopened =
        CheckpointStore::open_managed_with_resource_limits(&path, 4096, max_keys, 64, None)
            .expect("final recovery");
    check(&reopened, &model, enabled);
});
