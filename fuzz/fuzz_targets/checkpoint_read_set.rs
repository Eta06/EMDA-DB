#![no_main]
use database_engine::{
    CheckpointStore, CheckpointTransaction, CheckpointTransactionMode, Store, TimedTransactionError,
};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::io::{self, ErrorKind};
use std::time::{Duration, Instant};

type Model = BTreeMap<u8, (u64, u8)>;
type RangeObservation = (Option<u8>, Option<u8>, Model);

fn physical_key(key: u8) -> Vec<u8> {
    vec![key; usize::from(key % 3) + 1]
}

fn within(key: u8, after: Option<u8>, before: Option<u8>) -> bool {
    after.is_none_or(|bound| key > bound) && before.is_none_or(|bound| key < bound)
}

fn matches(
    points: &BTreeMap<u8, Option<(u64, u8)>>,
    ranges: &[RangeObservation],
    live: &Model,
) -> bool {
    points
        .iter()
        .all(|(key, observed)| *observed == live.get(key).copied())
        && ranges.iter().all(|(after, before, observed)| {
            observed.iter().eq(live
                .iter()
                .filter(|(key, _)| within(**key, *after, *before)))
        })
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
        ordinary
            .put(&physical_key(key), &[key])
            .expect("initial write");
        generation += 1;
        live.insert(key, (generation, key));
    }
    let digest = ordinary.checkpoint_to(&checkpoint).expect("checkpoint");
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).expect("checkpoint store");
    let mut staged = BTreeMap::<u8, Option<u8>>::new();
    let mut points = BTreeMap::<u8, Option<(u64, u8)>>::new();
    let mut ranges = Vec::<RangeObservation>::new();
    let mut conflict = false;
    let action = |tx: &mut CheckpointTransaction<'_>| -> io::Result<()> {
        for &byte in data {
            let key = b'a' + (byte & 7);
            let kind = byte >> 5;
            if matches!(kind, 0 | 1 | 2 | 5 | 6 | 7) && !matches(&points, &ranges, &live) {
                let result = match kind {
                    0 | 6 => tx.get(&physical_key(key)).map(|_| ()),
                    1 => tx.put(&physical_key(key), &[byte]),
                    2 => tx.delete(&physical_key(key)),
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
                        tx.get(&physical_key(key))?.as_deref(),
                        expected.as_ref().map(std::slice::from_ref)
                    );
                }
                1 => {
                    tx.put(&physical_key(key), &[byte])?;
                    staged.insert(key, Some(byte));
                }
                2 => {
                    tx.delete(&physical_key(key))?;
                    staged.insert(key, None);
                }
                3 => {
                    store.put(&physical_key(key), &[byte])?;
                    generation += 1;
                    live.insert(key, (generation, byte));
                }
                4 => {
                    store.delete(&physical_key(key))?;
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
                        .map(|(k, value)| (physical_key(k), vec![value]))
                        .collect::<Vec<_>>();
                    let lower = after.map(physical_key);
                    let upper = before.map(physical_key);
                    assert_eq!(
                        tx.scan(lower.as_deref(), upper.as_deref(), limit)?,
                        expected
                    );
                    let observed = live
                        .iter()
                        .filter(|(key, _)| within(**key, after, before))
                        .map(|(&key, &value)| (key, value))
                        .collect();
                    ranges.push((after, before, observed));
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
    if !conflict && matches(&points, &ranges, &live) {
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
            store.get(&physical_key(key)).expect("live read").as_deref(),
            live.get(&key).map(|entry| std::slice::from_ref(&entry.1))
        );
    }
    drop(store);
    let recovered = Store::open(&path).expect("full recovery");
    for key in b'a'..=b'h' {
        assert_eq!(
            recovered
                .get(&physical_key(key))
                .expect("recovered read")
                .as_deref(),
            live.get(&key).map(|entry| std::slice::from_ref(&entry.1))
        );
    }
});
