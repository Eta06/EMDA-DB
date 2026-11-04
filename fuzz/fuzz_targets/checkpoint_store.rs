#![no_main]
use database_engine::{BatchOperation, CheckpointStore, Store};
use libfuzzer_sys::fuzz_target;
use std::collections::{BTreeMap, BTreeSet};

fn key(byte: u8) -> Vec<u8> {
    let index = byte % 8;
    vec![b'a' + index; usize::from(index % 4) + 1]
}

fn check(
    store: &CheckpointStore,
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    touched: &BTreeSet<Vec<u8>>,
    tail: u64,
) {
    let actual = store.scan(None, None, 100).expect("scan");
    let expected = model
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(store.overlay_keys().expect("overlay size"), touched.len());
    assert_eq!(store.tail_bytes().expect("suffix length"), tail);
    for index in 0..8 {
        let key = key(index);
        assert_eq!(
            store.get(&key).expect("point read"),
            model.get(&key).cloned()
        );
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 3 || data.len() > 128 {
        return;
    }
    let max_tail = u64::from(data[0]) * 4;
    let max_keys = usize::from(data[1] % 9);
    let max_key_bytes = usize::from(data[2] % 25);
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("store");
    let mut checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).expect("create store");
    let mut model = BTreeMap::new();
    for index in 0..3 {
        let key = key(index);
        ordinary.put(&key, b"base").expect("base write");
        model.insert(key, b"base".to_vec());
    }
    let mut digest = ordinary.checkpoint_to(&checkpoint).expect("checkpoint");
    drop(ordinary);
    let mut store = CheckpointStore::open_with_limits(
        &path,
        &checkpoint,
        &digest,
        max_tail,
        max_keys,
        max_key_bytes,
    )
    .expect("open");
    let mut touched = BTreeSet::<Vec<u8>>::new();
    let mut tail = 0_u64;
    let mut peak = model.len();
    let mut generation = 0_usize;
    for bytes in data[3..].chunks_exact(4) {
        let first = key(bytes[1]);
        let second = key(bytes[2]);
        let value = vec![bytes[3]; usize::from(bytes[3] % 5)];
        let kind = bytes[0] % 7;
        if kind == 4 {
            check(&store, &model, &touched, tail);
            drop(store);
            store = CheckpointStore::open_with_limits(
                &path,
                &checkpoint,
                &digest,
                max_tail,
                max_keys,
                max_key_bytes,
            )
            .expect("reopen");
            continue;
        }
        if kind == 5 {
            check(&store, &model, &touched, tail);
            generation += 1;
            let next = dir.path().join(format!("checkpoint-{generation}"));
            let next_digest = store.checkpoint_to(&next).expect("stream checkpoint");
            drop(store);
            if peak > 0 {
                assert!(
                    Store::open_with_checkpoint_and_limits(
                        &path,
                        &next,
                        &next_digest,
                        None,
                        Some(peak - 1)
                    )
                    .is_err()
                );
            }
            let ordinary = Store::open_with_checkpoint_and_limits(
                &path,
                &next,
                &next_digest,
                None,
                Some(peak),
            )
            .expect("checkpoint and historical peak");
            let expected = model
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>();
            assert_eq!(
                ordinary.scan(None, None, 100).expect("checkpoint scan"),
                expected
            );
            drop(ordinary);
            checkpoint = next;
            digest = next_digest;
            touched.clear();
            tail = 0;
            store = CheckpointStore::open_with_limits(
                &path,
                &checkpoint,
                &digest,
                max_tail,
                max_keys,
                max_key_bytes,
            )
            .expect("reopen streamed checkpoint");
            continue;
        }
        if kind == 6 {
            check(&store, &model, &touched, tail);
            generation += 1;
            let next = dir.path().join(format!("checkpoint-{generation}"));
            digest = store.rollover_to(&next).expect("live rollover");
            checkpoint = next;
            touched.clear();
            tail = 0;
            check(&store, &model, &touched, tail);
            continue;
        }
        let mut next_touched = touched.clone();
        next_touched.insert(first.clone());
        if kind >= 2 {
            next_touched.insert(second.clone());
        }
        let encoded = match kind {
            0 => 21 + first.len() + value.len(),
            1 => 21 + first.len(),
            2 => 26 + (21 + first.len() + value.len()) + (21 + second.len()),
            _ => 26 + (21 + first.len()) + (21 + second.len() + value.len()),
        } as u64;
        let allowed = tail + encoded <= max_tail
            && next_touched.len() <= max_keys
            && next_touched.iter().map(Vec::len).sum::<usize>() <= max_key_bytes;
        let result = match kind {
            0 => store.put(&first, &value),
            1 => store.delete(&first),
            2 => store.write_batch(&[
                BatchOperation::Put {
                    key: &first,
                    value: &value,
                },
                BatchOperation::Delete { key: &second },
            ]),
            _ => store.write_batch(&[
                BatchOperation::Delete { key: &first },
                BatchOperation::Put {
                    key: &second,
                    value: &value,
                },
            ]),
        };
        assert_eq!(
            result.is_ok(),
            allowed,
            "admission mismatch: {:?}",
            result.err()
        );
        if allowed {
            touched = next_touched;
            tail += encoded;
            match kind {
                0 => {
                    model.insert(first, value);
                }
                1 => {
                    model.remove(&first);
                }
                2 => {
                    model.insert(first, value);
                    peak = peak.max(model.len());
                    model.remove(&second);
                }
                _ => {
                    model.remove(&first);
                    model.insert(second, value);
                }
            }
            peak = peak.max(model.len());
        }
        check(&store, &model, &touched, tail);
    }
    drop(store);
    let ordinary = Store::open(&path).expect("full replay");
    let expected = model.into_iter().collect::<Vec<_>>();
    assert_eq!(
        ordinary.scan(None, None, 100).expect("full replay scan"),
        expected
    );
});
