#![no_main]
use database_engine::{BatchOperation, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

const KEYS: [&[u8]; 6] = [b"a", b"bb", b"ccc", b"dddd", b"eeeee", b"ffffff"];

fn apply(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, operation: &BatchOperation<'_>) {
    match operation {
        BatchOperation::Put { key, value } => {
            model.insert(key.to_vec(), value.to_vec());
        }
        BatchOperation::Delete { key } => {
            model.remove(*key);
        }
    }
}

fn live_bytes(model: &BTreeMap<Vec<u8>, Vec<u8>>) -> usize {
    model.keys().map(Vec::len).sum()
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 128 {
        return;
    }
    let cap = usize::from(data[0] % 25);
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path();
    let mut store = Store::open_with_resource_limits(path, None, None, Some(cap)).expect("open");
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for &byte in &data[1..] {
        let first = KEYS[usize::from(byte & 7) % KEYS.len()];
        let second = KEYS[usize::from((byte >> 3) & 7) % KEYS.len()];
        let value = [byte];
        if byte >> 6 == 3 {
            drop(store);
            store = Store::open_with_resource_limits(path, None, None, Some(cap)).expect("reopen");
        } else {
            let operations = match byte >> 6 {
                0 => vec![BatchOperation::Put {
                    key: first,
                    value: &value,
                }],
                1 => vec![BatchOperation::Delete { key: first }],
                _ if byte & 32 == 0 => vec![
                    BatchOperation::Put {
                        key: first,
                        value: &value,
                    },
                    BatchOperation::Delete { key: second },
                ],
                _ => vec![
                    BatchOperation::Delete { key: first },
                    BatchOperation::Put {
                        key: second,
                        value: &value,
                    },
                ],
            };
            let mut proposed = model.clone();
            let mut allowed = true;
            for operation in &operations {
                apply(&mut proposed, operation);
                allowed &= live_bytes(&proposed) <= cap;
            }
            let before = std::fs::read(path.join("records.log")).expect("read log");
            let result = if operations.len() == 1 {
                match &operations[0] {
                    BatchOperation::Put { key, value } => store.put(key, value),
                    BatchOperation::Delete { key } => store.delete(key),
                }
            } else {
                store.write_batch(&operations)
            };
            if allowed {
                result.expect("admissible mutation");
                model = proposed;
            } else {
                let error = result.expect_err("over-limit mutation must fail");
                assert!(error.to_string().contains("live key byte limit"));
                assert_eq!(
                    std::fs::read(path.join("records.log")).expect("read log"),
                    before
                );
            }
        }
        assert_eq!(
            store.scan(None, None, 10).expect("scan"),
            model
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        );
    }
    drop(store);
    let store =
        Store::open_with_resource_limits(path, None, None, Some(cap)).expect("final reopen");
    assert_eq!(
        store.scan(None, None, 10).expect("recovered scan"),
        model.into_iter().collect::<Vec<_>>()
    );
});
