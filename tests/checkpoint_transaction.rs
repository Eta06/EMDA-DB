use database_engine::{CheckpointStore, MAX_ATOMIC_BATCH_OPERATIONS, Store};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf, [u8; 32]) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    for (key, value) in [
        (b"a".as_slice(), b"one".as_slice()),
        (b"c".as_slice(), b"three".as_slice()),
        (b"e".as_slice(), b"five".as_slice()),
    ] {
        store.put(key, value).unwrap();
    }
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    (dir, path, checkpoint, digest)
}

#[test]
fn transaction_reads_merged_pages_rolls_back_and_recovers_atomic_writes() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let log = path.join("records.log");
    let initial = fs::read(&log).unwrap();
    let result: io::Result<()> = store.transaction(|tx| {
        tx.put(b"a", b"uncommitted")?;
        tx.delete(b"c")?;
        assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"uncommitted"[..]));
        assert_eq!(tx.get(b"c")?, None);
        Err(io::Error::other("cancel"))
    });
    assert!(result.is_err());
    assert_eq!(fs::read(&log).unwrap(), initial);
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));

    store
        .transaction(|tx| {
            tx.delete(b"a")?;
            tx.put(b"b", b"two")?;
            tx.put(b"c", b"updated")?;
            tx.put(b"d", b"four")?;
            tx.delete(b"e")?;
            tx.put(b"f", b"six")?;
            let first = tx.scan(None, None, 2)?;
            assert_eq!(
                first,
                vec![
                    (b"b".to_vec(), b"two".to_vec()),
                    (b"c".to_vec(), b"updated".to_vec())
                ]
            );
            let second = tx.scan(Some(&first[1].0), None, 2)?;
            assert_eq!(
                second,
                vec![
                    (b"d".to_vec(), b"four".to_vec()),
                    (b"f".to_vec(), b"six".to_vec())
                ]
            );
            assert!(tx.scan(Some(&second[1].0), None, 2)?.is_empty());
            assert_eq!(
                tx.scan(Some(b"b"), Some(b"f"), 8)?,
                vec![
                    (b"c".to_vec(), b"updated".to_vec()),
                    (b"d".to_vec(), b"four".to_vec())
                ]
            );
            assert!(tx.scan(Some(b"f"), Some(b"b"), 8)?.is_empty());
            assert_eq!(
                tx.scan(None, None, 0).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            Ok(())
        })
        .unwrap();
    let expected = vec![
        (b"b".to_vec(), b"two".to_vec()),
        (b"c".to_vec(), b"updated".to_vec()),
        (b"d".to_vec(), b"four".to_vec()),
        (b"f".to_vec(), b"six".to_vec()),
    ];
    assert_eq!(store.scan(None, None, 10).unwrap(), expected);
    drop(store);
    assert_eq!(
        Store::open(&path).unwrap().scan(None, None, 10).unwrap(),
        expected
    );
}

#[test]
fn any_intervening_write_conflicts_including_aba_and_phantoms() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let result = store.transaction(|tx| {
        assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"one"[..]));
        store.put(b"a", b"other")?;
        store.put(b"a", b"one")?;
        assert_eq!(tx.get(b"a").unwrap_err().kind(), io::ErrorKind::WouldBlock);
        // Even if a callback catches the read error, final validation rejects it.
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);

    let result = store.transaction(|tx| {
        tx.put(b"uncommitted", b"value")?;
        assert_eq!(tx.scan(Some(b"a"), Some(b"e"), 10)?.len(), 1);
        store.put(b"b", b"phantom")?;
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(store.get(b"uncommitted").unwrap(), None);
    assert_eq!(store.get(b"b").unwrap().as_deref(), Some(&b"phantom"[..]));

    // Switching to an equivalent checkpoint changes no logical state.
    let next = dir.path().join("next");
    let next_digest = store
        .transaction(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"one"[..]));
            let next_digest = store.rollover_to(&next)?;
            tx.put(b"after-rollover", b"ok")?;
            Ok(next_digest)
        })
        .unwrap();
    drop(store);
    let reopened = CheckpointStore::open(&path, &next, &next_digest).unwrap();
    assert_eq!(
        reopened.get(b"after-rollover").unwrap().as_deref(),
        Some(&b"ok"[..])
    );
}

#[test]
fn transaction_rejects_too_many_staged_writes_without_appending() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let log = path.join("records.log");
    let before = fs::read(&log).unwrap();
    let result = store.transaction(|tx| {
        for i in 0..=MAX_ATOMIC_BATCH_OPERATIONS {
            tx.put(format!("key-{i}").as_bytes(), b"value")?;
        }
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(fs::read(log).unwrap(), before);
}

#[test]
fn concurrent_transactions_transfer_without_losing_or_splitting_updates() {
    let (_dir, path, checkpoint, digest) = fixture();
    let store = Arc::new(CheckpointStore::open(&path, &checkpoint, &digest).unwrap());
    store.put(b"left", &500_i64.to_le_bytes()).unwrap();
    store.put(b"right", &500_i64.to_le_bytes()).unwrap();
    let start = Arc::new(Barrier::new(5));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            scope.spawn(move || {
                start.wait();
                for _ in 0..40 {
                    let mut committed = false;
                    for _ in 0..10_000 {
                        match store.transaction(|tx| {
                            let left = tx.get(b"left")?.unwrap();
                            let right = tx.get(b"right")?.unwrap();
                            let left = i64::from_le_bytes(left.try_into().unwrap());
                            let right = i64::from_le_bytes(right.try_into().unwrap());
                            tx.put(b"left", &(left - 1).to_le_bytes())?;
                            tx.put(b"right", &(right + 1).to_le_bytes())?;
                            Ok(())
                        }) {
                            Ok(()) => {
                                committed = true;
                                break;
                            }
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                            Err(error) => panic!("unexpected transaction error: {error}"),
                        }
                    }
                    assert!(committed, "transaction retry bound exceeded");
                }
            });
        }
        start.wait();
    });
    let expected = vec![
        Some(340_i64.to_le_bytes().to_vec()),
        Some(660_i64.to_le_bytes().to_vec()),
    ];
    assert_eq!(store.get_many(&[b"left", b"right"]).unwrap(), expected);
    drop(store);
    assert_eq!(
        Store::open(&path)
            .unwrap()
            .get_many(&[b"left", b"right"])
            .unwrap(),
        expected
    );
}

#[test]
fn checkpoint_transaction_handles_more_keys_than_ordinary_snapshot_limit() {
    use database_engine::{BatchOperation, MAX_SNAPSHOT_KEYS};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large-store");
    let checkpoint = dir.path().join("large-checkpoint");
    let ordinary = Store::open(&path).unwrap();
    let keys = (0..MAX_SNAPSHOT_KEYS + 16)
        .map(|index| format!("key-{index:05}").into_bytes())
        .collect::<Vec<_>>();
    for chunk in keys.chunks(1024) {
        let operations = chunk
            .iter()
            .map(|key| BatchOperation::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        ordinary.write_batch(&operations).unwrap();
    }
    assert!(ordinary.transaction(|_| Ok(())).is_err());
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    let last = keys.last().unwrap();
    store
        .transaction(|tx| {
            assert_eq!(tx.get(last)?.as_deref(), Some(&b"v"[..]));
            tx.put(last, b"updated")?;
            let page = tx.scan(Some(&keys[keys.len() - 3]), None, 5)?;
            assert_eq!(page.len(), 2);
            assert_eq!(page[1], (last.clone(), b"updated".to_vec()));
            Ok(())
        })
        .unwrap();
    assert_eq!(store.get(last).unwrap().as_deref(), Some(&b"updated"[..]));
}

#[test]
fn transaction_scan_paginates_when_staged_and_base_values_reach_byte_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large-values");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    let value = vec![1_u8; 11 * 1024 * 1024];
    for key in [b"a", b"b", b"c"] {
        ordinary.put(key, &value).unwrap();
    }
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    store
        .transaction(|tx| {
            tx.put(b"c", &value)?;
            let first = tx.scan(None, None, 10)?;
            assert_eq!(first.len(), 2);
            assert_eq!(&first[0].0, b"a");
            assert_eq!(&first[1].0, b"b");
            let second = tx.scan(Some(b"b"), None, 10)?;
            assert_eq!(second.len(), 1);
            assert_eq!(&second[0].0, b"c");
            assert_eq!(second[0].1, value);
            Ok(())
        })
        .unwrap();
}
