#![no_main]
use database_engine::{BatchOperation, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

const KEYS: [&[u8]; 4] = [b"a", b"bb", b"ccc", b"dddd"];
const HEADER_AND_CHECKSUM: u64 = 17 + 4;

fn record_bytes(key: &[u8], value: &[u8]) -> u64 {
    HEADER_AND_CHECKSUM + key.len() as u64 + value.len() as u64
}

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

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 128 {
        return;
    }
    // Both current witnessed log headers are 28 bytes.
    let cap = 28 + u64::from(data[0]);
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path();
    let mut store = Store::open_with_log_limit(path, cap).expect("open");
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for &byte in &data[1..] {
        let first = KEYS[usize::from(byte & 3)];
        let second = KEYS[usize::from((byte >> 2) & 3)];
        let value = [byte];
        if byte >> 6 == 3 {
            drop(store);
            store = Store::open_with_log_limit(path, cap).expect("reopen");
        } else {
            let operations = match byte >> 6 {
                0 => vec![BatchOperation::Put {
                    key: first,
                    value: &value,
                }],
                1 => vec![BatchOperation::Delete { key: first }],
                _ => vec![
                    BatchOperation::Put {
                        key: first,
                        value: &value,
                    },
                    BatchOperation::Delete { key: second },
                ],
            };
            let bytes = if operations.len() == 1 {
                match &operations[0] {
                    BatchOperation::Put { key, value } => record_bytes(key, value),
                    BatchOperation::Delete { key } => record_bytes(key, &[]),
                }
            } else {
                // Outer DB02 record has a one-byte key and a four-byte count.
                HEADER_AND_CHECKSUM
                    + 1
                    + 4
                    + record_bytes(first, &value)
                    + record_bytes(second, &[])
            };
            let before = std::fs::read(path.join("records.log")).expect("read log");
            let allowed = before.len() as u64 + bytes <= cap;
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
                for operation in &operations {
                    apply(&mut model, operation);
                }
                assert_eq!(
                    std::fs::metadata(path.join("records.log"))
                        .expect("log metadata")
                        .len(),
                    before.len() as u64 + bytes
                );
            } else {
                let error = result.expect_err("over-limit mutation must fail");
                assert!(error.to_string().contains("log size limit"));
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
    let store = Store::open_with_log_limit(path, cap).expect("final reopen");
    assert_eq!(
        store.scan(None, None, 10).expect("recovered scan"),
        model.into_iter().collect::<Vec<_>>()
    );
});
