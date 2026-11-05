#![no_main]
use database_engine::{
    BatchOperation, MAX_GROUP_COMMIT_BUFFERED_BYTES, MAX_GROUP_COMMIT_BUFFERED_RECORDS, Store,
};
use libfuzzer_sys::fuzz_target;
use std::{collections::BTreeMap, io::ErrorKind};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut store = Store::open(&path).unwrap();
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut admitted = true;
    for chunk in data.chunks(3) {
        let flag = chunk[0];
        let key = vec![chunk.get(1).copied().unwrap_or(0) % 4];
        let other = vec![(key[0] + 1) % 4];
        let value = vec![flag; usize::from(chunk.get(2).copied().unwrap_or(0) % 32)];
        match flag % 8 {
            0 => {
                // Every generated record/batch is much smaller than 4096 bytes.
                let bytes = if flag & 8 == 0 { 0 } else { 4096 };
                let records = if flag & 16 == 0 { 0 } else { 4 };
                store.set_group_commit_limits(bytes, records).unwrap();
                admitted = bytes != 0 && records != 0;
            }
            1 => {
                let before = store.group_commit_status().unwrap();
                assert_eq!(
                    store
                        .set_group_commit_limits(MAX_GROUP_COMMIT_BUFFERED_BYTES + 1, 1)
                        .unwrap_err()
                        .kind(),
                    ErrorKind::InvalidInput
                );
                assert_eq!(
                    store
                        .set_group_commit_limits(1, MAX_GROUP_COMMIT_BUFFERED_RECORDS + 1)
                        .unwrap_err()
                        .kind(),
                    ErrorKind::InvalidInput
                );
                assert_eq!(store.group_commit_status().unwrap(), before);
            }
            2 => {
                let result = store.put(&key, &value);
                assert_eq!(result.is_ok(), admitted);
                if admitted {
                    model.insert(key, value);
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            3 => {
                let result = store.delete(&key);
                assert_eq!(result.is_ok(), admitted);
                if admitted {
                    model.remove(&key);
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            4 => {
                let result = store.write_batch(&[
                    BatchOperation::Put {
                        key: &key,
                        value: &value,
                    },
                    BatchOperation::Delete { key: &other },
                ]);
                assert_eq!(result.is_ok(), admitted);
                if admitted {
                    model.insert(key, value);
                    model.remove(&other);
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            5 => {
                let result = store.transaction(|tx| {
                    tx.put(&key, &value)?;
                    tx.delete(&other)
                });
                assert_eq!(result.is_ok(), admitted);
                if admitted {
                    model.insert(key, value);
                    model.remove(&other);
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            6 => {
                let expected = model.get(&key).map(Vec::as_slice);
                let result = store.compare_exchange(&key, expected, Some(&value));
                assert_eq!(result.is_ok(), admitted);
                if admitted {
                    assert!(result.unwrap());
                    model.insert(key, value);
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
                }
            }
            _ => {
                drop(store);
                store = Store::open(&path).unwrap();
                admitted = true;
                let status = store.group_commit_status().unwrap();
                assert_eq!(
                    (status.limit_bytes, status.limit_records),
                    (
                        MAX_GROUP_COMMIT_BUFFERED_BYTES,
                        MAX_GROUP_COMMIT_BUFFERED_RECORDS
                    )
                );
            }
        }
        let status = store.group_commit_status().unwrap();
        assert_eq!(
            (
                status.reserved_bytes,
                status.reserved_records,
                status.waiting_writers
            ),
            (0, 0, 0)
        );
        assert!(!status.failed);
        for key in 0..4 {
            assert_eq!(store.get(&[key]).unwrap(), model.get(&vec![key]).cloned());
        }
    }
    drop(store);
    let recovered = Store::open(&path).unwrap();
    for key in 0..4 {
        assert_eq!(
            recovered.get(&[key]).unwrap(),
            model.get(&vec![key]).cloned()
        );
    }
});
