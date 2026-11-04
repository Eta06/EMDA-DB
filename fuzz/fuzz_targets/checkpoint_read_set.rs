#![no_main]
use database_engine::{
    CheckpointStore, CheckpointTransaction, CheckpointTransactionMode, Store, TimedTransactionError,
};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::io::{self, ErrorKind};
use std::time::{Duration, Instant};

type Model = BTreeMap<u8, (u64, u8)>;

fn matches(
    points: &BTreeMap<u8, Option<(u64, u8)>>,
    scan_generation: Option<u64>,
    live: &Model,
    generation: u64,
) -> bool {
    scan_generation.is_none_or(|seen| seen == generation)
        && points
            .iter()
            .all(|(key, observed)| *observed == live.get(key).copied())
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 256 {
        return;
    }
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("store");
    let checkpoint = directory.path().join("checkpoint");
    let ordinary = Store::open(&path).expect("create store");
    let mut live = Model::new();
    let mut generation = 0_u64;
    for key in b'a'..=b'd' {
        ordinary.put(&[key], &[key]).expect("initial write");
        generation += 1;
        live.insert(key, (generation, key));
    }
    let digest = ordinary.checkpoint_to(&checkpoint).expect("checkpoint");
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).expect("checkpoint store");
    let mut staged = BTreeMap::<u8, Option<u8>>::new();
    let mut points = BTreeMap::<u8, Option<(u64, u8)>>::new();
    let mut scan_generation = None;
    let mut conflict = false;
    let action = |tx: &mut CheckpointTransaction<'_>| -> io::Result<()> {
        for &byte in data {
            let key = b'a' + (byte & 7);
            let kind = byte >> 5;
            if matches!(kind, 0 | 1 | 2 | 5 | 6 | 7)
                && !matches(&points, scan_generation, &live, generation)
            {
                let result = match kind {
                    0 | 6 => tx.get(&[key]).map(|_| ()),
                    1 => tx.put(&[key], &[byte]),
                    2 => tx.delete(&[key]),
                    _ => tx.scan(None, None, 4).map(|_| ()),
                };
                let error = result.expect_err("stale observation must fail before a new result");
                assert_eq!(error.kind(), ErrorKind::WouldBlock);
                conflict = true;
                return Err(error);
            }
            match kind {
                0 | 6 => {
                    let expected = if let Some(value) = staged.get(&key) {
                        *value
                    } else {
                        let observed = live.get(&key).copied();
                        points.entry(key).or_insert(observed);
                        observed.map(|entry| entry.1)
                    };
                    assert_eq!(
                        tx.get(&[key])?.as_deref(),
                        expected.as_ref().map(std::slice::from_ref)
                    );
                }
                1 => {
                    tx.put(&[key], &[byte])?;
                    staged.insert(key, Some(byte));
                }
                2 => {
                    tx.delete(&[key])?;
                    staged.insert(key, None);
                }
                3 => {
                    store.put(&[key], &[byte])?;
                    generation += 1;
                    live.insert(key, (generation, byte));
                }
                4 => {
                    store.delete(&[key])?;
                    generation += 1;
                    live.remove(&key);
                }
                5 | 7 => {
                    let after = (byte & 8 != 0).then_some(b'b');
                    let before = (byte & 16 != 0).then_some(b'g');
                    let limit = usize::from(byte & 3) + 1;
                    let mut view = live
                        .iter()
                        .map(|(&k, &(_, value))| (k, value))
                        .collect::<BTreeMap<_, _>>();
                    for (&k, &value) in &staged {
                        match value {
                            Some(value) => {
                                view.insert(k, value);
                            }
                            None => {
                                view.remove(&k);
                            }
                        }
                    }
                    let expected = view
                        .into_iter()
                        .filter(|(k, _)| {
                            after.is_none_or(|bound| *k > bound)
                                && before.is_none_or(|bound| *k < bound)
                        })
                        .take(limit)
                        .map(|(k, value)| (vec![k], vec![value]))
                        .collect::<Vec<_>>();
                    let lower = after.as_ref().map(std::slice::from_ref);
                    let upper = before.as_ref().map(std::slice::from_ref);
                    assert_eq!(tx.scan(lower, upper, limit)?, expected);
                    scan_generation = Some(generation);
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    };
    let result = match data[0] & 3 {
        0 => store.transaction_with_read_set(action),
        1 => store.transaction_with_read_set_fifo(action),
        mode => store
            .transaction_until(
                Instant::now() + Duration::from_secs(60),
                if mode == 2 {
                    CheckpointTransactionMode::ReadSet
                } else {
                    CheckpointTransactionMode::ReadSetFifo
                },
                action,
            )
            .map_err(|error| match error {
                TimedTransactionError::Operation(error) => error,
                TimedTransactionError::DeadlineExceeded => {
                    panic!("generous fuzz deadline unexpectedly expired")
                }
            }),
    };
    if !conflict && matches(&points, scan_generation, &live, generation) {
        result.expect("matching observations must commit");
        for (key, value) in staged {
            match value {
                Some(value) => {
                    live.insert(key, (0, value));
                }
                None => {
                    live.remove(&key);
                }
            }
        }
    } else {
        assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
    }
    for key in b'a'..=b'h' {
        assert_eq!(
            store.get(&[key]).expect("live read").as_deref(),
            live.get(&key).map(|entry| std::slice::from_ref(&entry.1))
        );
    }
    drop(store);
    let recovered = Store::open(&path).expect("full recovery");
    for key in b'a'..=b'h' {
        assert_eq!(
            recovered.get(&[key]).expect("recovered read").as_deref(),
            live.get(&key).map(|entry| std::slice::from_ref(&entry.1))
        );
    }
});
