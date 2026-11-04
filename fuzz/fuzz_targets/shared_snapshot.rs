#![no_main]
use database_engine::{BatchOperation, Snapshot, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn check(snapshot: &Snapshot<'_>, model: &Model) {
    let expected = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        snapshot.scan(None, None, 100).expect("snapshot scan"),
        expected
    );
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("store");
    let store = Store::open(&path).expect("open");
    let mut model = Model::new();
    let mut snapshots = Vec::<(Snapshot<'_>, Model)>::new();
    for chunk in data.chunks_exact(3) {
        let key = vec![b'a' + chunk[1] % 8; 1 + usize::from(chunk[1] % 4)];
        let other = vec![b'a' + chunk[2] % 8; 1 + usize::from(chunk[2] % 4)];
        let value = vec![chunk[2]; usize::from(chunk[2] % 8)];
        match chunk[0] % 8 {
            0 | 1 => {
                store.put(&key, &value).expect("put");
                model.insert(key, value);
            }
            2 => {
                store.delete(&key).expect("delete");
                model.remove(&key);
            }
            3 => {
                store
                    .write_batch(&[
                        BatchOperation::Put {
                            key: &key,
                            value: &value,
                        },
                        BatchOperation::Delete { key: &other },
                        BatchOperation::Put {
                            key: &other,
                            value: b"batch",
                        },
                    ])
                    .expect("batch");
                model.insert(key, value);
                model.remove(&other);
                model.insert(other, b"batch".to_vec());
            }
            4 if snapshots.len() < 4 => {
                snapshots.push((store.snapshot(100, 1024).expect("snapshot"), model.clone()));
            }
            5 if !snapshots.is_empty() => {
                snapshots.remove(usize::from(chunk[1]) % snapshots.len());
            }
            6 => {
                store
                    .transaction(|tx| {
                        tx.put(&key, &value)?;
                        tx.delete(&other)
                    })
                    .expect("transaction");
                model.insert(key, value);
                model.remove(&other);
            }
            _ => {
                let max_keys = usize::from(chunk[1] % 10);
                let max_bytes = usize::from(chunk[2] % 35);
                let bytes = model.keys().map(Vec::len).sum::<usize>();
                let snapshot = store.snapshot(max_keys, max_bytes);
                if model.len() <= max_keys && bytes <= max_bytes {
                    check(&snapshot.expect("within limits"), &model);
                } else {
                    assert!(snapshot.is_err());
                }
            }
        }
        for (snapshot, expected) in &snapshots {
            check(snapshot, expected);
        }
        let current = store.snapshot(100, 1024).expect("current snapshot");
        check(&current, &model);
    }
    drop(snapshots);
    drop(store);
    let reopened = Store::open(&path).expect("reopen");
    check(
        &reopened.snapshot(100, 1024).expect("recovered snapshot"),
        &model,
    );
});
