use database_engine::{BatchOperation as Op, Store};
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn byte_limit_counts_live_keys_and_reclaims_deleted_space() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open_with_resource_limits(directory.path(), None, None, Some(6)).unwrap();
    store.put(b"ab", b"old").unwrap();
    store.put(b"cde", b"value").unwrap();
    store.put(b"ab", b"updated").unwrap();
    store.delete(b"missing").unwrap();
    assert!(store.put(b"zz", b"value").is_err());
    store.put(b"f", b"value").unwrap();
    assert!(store.compare_exchange(b"z", None, Some(b"value")).is_err());
    store.delete(b"cde").unwrap();
    assert_eq!(store.increment_i64(b"ccc", 1).unwrap(), 1);
    assert!(store.put(b"z", b"value").is_err());
    assert!(
        store
            .compare_exchange(b"ccc", Some(&1_i64.to_le_bytes()), None)
            .unwrap()
    );
    store.put(b"zzz", b"value").unwrap();
    drop(store);
    let reopened = Store::open_with_resource_limits(directory.path(), None, None, Some(6)).unwrap();
    assert_eq!(
        reopened.get(b"ab").unwrap().as_deref(),
        Some(&b"updated"[..])
    );
    assert_eq!(
        reopened.get(b"zzz").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn zero_byte_limit_and_combined_limits_reject_before_writing() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open_with_resource_limits(directory.path(), None, Some(1), Some(0)).unwrap();
    let before = std::fs::read(directory.path().join("records.log")).unwrap();
    assert!(store.put(b"a", b"value").is_err());
    assert!(
        store
            .write_batch(&[Op::Put {
                key: b"a",
                value: b"value"
            }])
            .is_err()
    );
    assert_eq!(
        std::fs::read(directory.path().join("records.log")).unwrap(),
        before
    );
    store.delete(b"absent").unwrap();
    drop(store);
    let store = Store::open_with_resource_limits(directory.path(), None, Some(1), Some(2)).unwrap();
    store.put(b"aa", b"value").unwrap();
    assert!(store.put(b"b", b"value").is_err());
    assert!(
        store
            .write_batch(&[
                Op::Delete { key: b"aa" },
                Op::Put {
                    key: b"bbb",
                    value: b"value"
                }
            ])
            .is_err()
    );
    assert_eq!(store.get(b"aa").unwrap().as_deref(), Some(&b"value"[..]));
}

#[test]
fn concurrent_writers_cannot_oversubscribe_live_key_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        Arc::new(Store::open_with_resource_limits(directory.path(), None, None, Some(6)).unwrap());
    let barrier = Barrier::new(17);
    let winners = thread::scope(|scope| {
        let mut workers = Vec::new();
        for id in 0..16_u8 {
            let (store, barrier) = (&store, &barrier);
            workers.push(scope.spawn(move || {
                barrier.wait();
                store.put(&[id + 1, 0xff], b"value").is_ok()
            }));
        }
        barrier.wait();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|won| *won)
            .count()
    });
    assert_eq!(winners, 3);
    drop(store);
    let store = Store::open_with_resource_limits(directory.path(), None, None, Some(6)).unwrap();
    assert_eq!(store.scan(None, None, 10).unwrap().len(), 3);
}

#[test]
fn ordered_batch_intermediate_states_and_transaction_replacements_respect_limit() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open_with_resource_limits(directory.path(), None, None, Some(4)).unwrap();
    store.put(b"a", b"1").unwrap();
    store.put(b"bc", b"2").unwrap();
    let before = std::fs::read(directory.path().join("records.log")).unwrap();
    assert!(
        store
            .write_batch(&[
                Op::Put {
                    key: b"de",
                    value: b"3"
                },
                Op::Delete { key: b"bc" },
            ])
            .is_err()
    );
    assert_eq!(
        std::fs::read(directory.path().join("records.log")).unwrap(),
        before
    );
    store
        .transaction(|tx| {
            tx.put(b"def", b"3")?;
            tx.delete(b"bc")
        })
        .unwrap();
    assert!(store.put(b"b", b"oversized").is_err());
    store
        .write_batch(&[
            Op::Delete { key: b"def" },
            Op::Put {
                key: b"xy",
                value: b"4",
            },
            Op::Put {
                key: b"xy",
                value: b"5",
            },
        ])
        .unwrap();
    store.put(b"z", b"6").unwrap();
    assert!(store.put(b"w", b"7").is_err());
    drop(store);
    let store = Store::open_with_resource_limits(directory.path(), None, None, Some(4)).unwrap();
    assert_eq!(store.get(b"xy").unwrap().as_deref(), Some(&b"5"[..]));
    assert_eq!(store.get(b"z").unwrap().as_deref(), Some(&b"6"[..]));
}

#[test]
fn recovery_enforces_historical_byte_peak_and_compaction_can_reduce_it() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let store = Store::open(&source).unwrap();
    store.put(b"aaaa", b"value").unwrap();
    store.put(b"bbb", b"value").unwrap();
    store.delete(b"aaaa").unwrap();
    drop(store);
    let before = std::fs::read(source.join("records.log")).unwrap();
    let result = Store::open_with_resource_limits(&source, None, None, Some(3));
    assert!(
        matches!(result, Err(ref error) if error.to_string().contains("recovery live key byte limit"))
    );
    assert_eq!(std::fs::read(source.join("records.log")).unwrap(), before);
    let store = Store::open(&source).unwrap();
    let compacted = directory.path().join("compacted");
    store.compact_to(&compacted).unwrap();
    let compacted = Store::open_with_resource_limits(compacted, None, None, Some(3)).unwrap();
    assert_eq!(
        compacted.get(b"bbb").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn recovery_enforces_byte_peak_inside_an_atomic_batch() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    store
        .write_batch(&[
            Op::Put {
                key: b"temp",
                value: b"value",
            },
            Op::Delete { key: b"temp" },
        ])
        .unwrap();
    drop(store);
    let before = std::fs::read(directory.path().join("records.log")).unwrap();
    assert!(Store::open_with_resource_limits(directory.path(), None, None, Some(0)).is_err());
    assert_eq!(
        std::fs::read(directory.path().join("records.log")).unwrap(),
        before
    );
    assert!(Store::open_with_resource_limits(directory.path(), None, None, Some(4)).is_ok());
}

#[cfg(target_os = "linux")]
#[test]
fn rejected_recovery_does_not_adopt_an_unacknowledged_tail() {
    use rustix::fs::{XattrFlags, getxattr, setxattr};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let store = Store::open(path).unwrap();
    store.put(b"a", b"safe").unwrap();
    let log = path.join("records.log");
    let mut old_boundary = [0_u8; 36];
    assert_eq!(
        getxattr(&log, "user.db.commit.v3", &mut old_boundary[..]).unwrap(),
        36
    );
    store.put(b"long", b"tail").unwrap();
    drop(store);
    setxattr(
        &log,
        "user.db.commit.v3",
        &old_boundary,
        XattrFlags::REPLACE,
    )
    .unwrap();
    let before = std::fs::read(&log).unwrap();
    assert!(Store::open_with_resource_limits(path, None, None, Some(1)).is_err());
    assert_eq!(std::fs::read(&log).unwrap(), before);
    let mut after_boundary = [0_u8; 36];
    assert_eq!(
        getxattr(&log, "user.db.commit.v3", &mut after_boundary[..]).unwrap(),
        36
    );
    assert_eq!(after_boundary, old_boundary);
    let recovered = Store::open(path).unwrap();
    assert_eq!(
        recovered.get(b"long").unwrap().as_deref(),
        Some(&b"tail"[..])
    );
}

#[test]
fn mixed_mutations_match_a_logical_key_byte_reference_model() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let mut store = Store::open_with_resource_limits(path, None, None, Some(24)).unwrap();
    let keys = [b"a".as_slice(), b"bb", b"ccc", b"dddd", b"eeeee", b"ffffff"];
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut random = 0x6a09e667f3bcc909_u64;
    for iteration in 0..512 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let first = keys[random as usize % keys.len()];
        let second = keys[(random >> 8) as usize % keys.len()];
        let value = [(random >> 16) as u8];
        let operations = if random & 1 == 0 {
            vec![
                Op::Put {
                    key: first,
                    value: &value,
                },
                Op::Delete { key: second },
            ]
        } else {
            vec![
                Op::Delete { key: first },
                Op::Put {
                    key: second,
                    value: &value,
                },
            ]
        };
        let mut proposed = model.clone();
        let mut allowed = true;
        for operation in &operations {
            match operation {
                Op::Put { key, value } => {
                    proposed.insert(key.to_vec(), value.to_vec());
                }
                Op::Delete { key } => {
                    proposed.remove(*key);
                }
            }
            allowed &= proposed.keys().map(Vec::len).sum::<usize>() <= 24;
        }
        let result = store.write_batch(&operations);
        assert_eq!(result.is_ok(), allowed, "iteration {iteration}");
        if allowed {
            model = proposed;
        }
        assert_eq!(
            store.scan(None, None, 10).unwrap(),
            model
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        );
        if iteration % 64 == 63 {
            drop(store);
            store = Store::open_with_resource_limits(path, None, None, Some(24)).unwrap();
        }
    }
}
