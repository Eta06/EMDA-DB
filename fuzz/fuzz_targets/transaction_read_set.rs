#![no_main]
use database_engine::{Store, Transaction};
use libfuzzer_sys::fuzz_target;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, ErrorKind};

type Model = BTreeMap<u8, (u64, u8)>;

fn within(key: u8, after: Option<u8>, before: Option<u8>) -> bool {
    after.is_none_or(|bound| key > bound) && before.is_none_or(|bound| key < bound)
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 256 {
        return;
    }
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path()).expect("open store");
    let mut live = Model::new();
    let mut version = 0_u64;
    for key in b'a'..=b'd' {
        store.put(&[key], &[key]).expect("initial write");
        version += 1;
        live.insert(key, (version, key));
    }
    let initial = live.clone();
    let mut staged = BTreeMap::<u8, Option<u8>>::new();
    let mut points = BTreeSet::new();
    let mut ranges = Vec::new();
    let action = |tx: &mut Transaction<'_>| -> io::Result<()> {
        for &byte in data {
            let key = b'a' + (byte & 7);
            match byte >> 5 {
                0 | 6 => {
                    let expected = if let Some(value) = staged.get(&key) {
                        *value
                    } else {
                        points.insert(key);
                        initial.get(&key).map(|entry| entry.1)
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
                    version += 1;
                    live.insert(key, (version, byte));
                }
                4 => {
                    store.delete(&[key])?;
                    version += 1;
                    live.remove(&key);
                }
                5 | 7 => {
                    let after = (byte & 8 != 0).then_some(b'b');
                    let before = (byte & 16 != 0).then_some(b'g');
                    let limit = usize::from(byte & 3) + 1;
                    ranges.push((after, before));
                    let mut view = initial
                        .iter()
                        .map(|(&key, &(_, value))| (key, value))
                        .collect::<BTreeMap<_, _>>();
                    for (&key, &value) in &staged {
                        match value {
                            Some(value) => {
                                view.insert(key, value);
                            }
                            None => {
                                view.remove(&key);
                            }
                        }
                    }
                    let expected = view
                        .into_iter()
                        .filter(|(key, _)| within(*key, after, before))
                        .take(limit)
                        .map(|(key, value)| (vec![key], vec![value]))
                        .collect::<Vec<_>>();
                    let lower = after.as_ref().map(std::slice::from_ref);
                    let upper = before.as_ref().map(std::slice::from_ref);
                    assert_eq!(tx.scan(lower, upper, limit)?, expected);
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    };
    let result = if data[0] & 1 == 0 {
        store.transaction_with_read_set(action)
    } else {
        store.transaction_with_read_set_fifo(action)
    };
    let points_match = points.iter().all(|key| initial.get(key) == live.get(key));
    let ranges_match = ranges.iter().all(|&(after, before)| {
        initial
            .iter()
            .filter(|(key, _)| within(**key, after, before))
            .eq(live.iter().filter(|(key, _)| within(**key, after, before)))
    });
    if points_match && ranges_match {
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
    let reopened = Store::open(directory.path()).expect("recover store");
    for key in b'a'..=b'h' {
        assert_eq!(
            reopened.get(&[key]).expect("recovered read").as_deref(),
            live.get(&key).map(|entry| std::slice::from_ref(&entry.1))
        );
    }
});
