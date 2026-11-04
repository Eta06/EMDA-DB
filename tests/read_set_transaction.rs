use database_engine::{MAX_TRANSACTION_READ_CONSTRAINTS, Store};
use std::io::ErrorKind;
use std::sync::Barrier;
use std::thread;

#[test]
fn unrelated_writes_can_commit_but_observed_changes_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    store.put(b"a", b"old").unwrap();
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
            store.put(b"unrelated", b"other")?;
            tx.put(b"a", b"committed")
        })
        .unwrap();
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"committed"[..]));

    let conflict = store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"committed"[..]));
            store.put(b"a", b"temporary")?;
            store.put(b"a", b"committed")?;
            tx.put(b"side", b"discard")
        })
        .unwrap_err();
    assert_eq!(conflict.kind(), ErrorKind::WouldBlock);
    assert_eq!(store.get(b"side").unwrap(), None);

    let conflict = store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"missing")?, None);
            store.put(b"missing", b"new")?;
            tx.put(b"side", b"discard")
        })
        .unwrap_err();
    assert_eq!(conflict.kind(), ErrorKind::WouldBlock);
    assert_eq!(store.get(b"side").unwrap(), None);
}

#[test]
fn restored_absence_can_serialize_at_validation_and_staged_reads_are_local() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"missing")?, None);
            store.put(b"missing", b"temporary")?;
            store.delete(b"missing")?;
            tx.put(b"result", b"ok")
        })
        .unwrap();
    assert_eq!(store.get(b"result").unwrap().as_deref(), Some(&b"ok"[..]));
    store
        .transaction_with_read_set(|tx| {
            tx.put(b"key", b"staged")?;
            assert_eq!(tx.get(b"key")?.as_deref(), Some(&b"staged"[..]));
            store.put(b"key", b"intervening")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"staged"[..]));
}

#[test]
fn ranges_detect_phantoms_beyond_a_limited_page_but_exclude_endpoints() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    for key in [b"a", b"c", b"e"] {
        store.put(key, b"old").unwrap();
    }
    let conflict = store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.scan(Some(b"a"), Some(b"e"), 1)?[0].0, b"c");
            store.put(b"d", b"phantom")?;
            tx.put(b"result", b"discard")
        })
        .unwrap_err();
    assert_eq!(conflict.kind(), ErrorKind::WouldBlock);
    assert_eq!(store.get(b"result").unwrap(), None);
    store
        .transaction_with_read_set(|tx| {
            let values = tx.scan(Some(b"a"), Some(b"e"), 1)?;
            assert_eq!(values[0].0, b"c");
            store.put(b"a", b"endpoint")?;
            store.put(b"e", b"endpoint")?;
            store.put(b"z", b"outside")?;
            tx.put(b"result", b"committed")
        })
        .unwrap();
    assert_eq!(
        store.get(b"result").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
}

#[test]
fn concurrent_write_skew_is_rejected_with_read_set_validation() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    store.put(b"a", b"1").unwrap();
    store.put(b"b", b"1").unwrap();
    let barrier = Barrier::new(2);
    let results = thread::scope(|scope| {
        [b"a".as_slice(), b"b".as_slice()]
            .into_iter()
            .map(|key| {
                let (store, barrier) = (&store, &barrier);
                scope.spawn(move || {
                    store.transaction_with_read_set(|tx| {
                        assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"1"[..]));
                        assert_eq!(tx.get(b"b")?.as_deref(), Some(&b"1"[..]));
                        barrier.wait();
                        tx.put(key, b"0")
                    })
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(error) if error.kind() == ErrorKind::WouldBlock))
            .count(),
        1
    );
    assert!(
        store
            .get_many(&[b"a", b"b"])
            .unwrap()
            .contains(&Some(b"1".to_vec()))
    );
}

#[test]
fn read_set_count_limit_deduplicates_reads_and_rejects_before_returning_data() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    store
        .transaction_with_read_set(|tx| {
            for _ in 0..MAX_TRANSACTION_READ_CONSTRAINTS + 1 {
                assert_eq!(tx.get(b"same")?, None);
            }
            for index in 0..MAX_TRANSACTION_READ_CONSTRAINTS - 1 {
                assert_eq!(tx.get(&(index as u32).to_be_bytes())?, None);
            }
            let error = tx.get(b"excess").unwrap_err();
            assert!(error.to_string().contains("read-set limit"));
            tx.put(b"marker", b"ok")
        })
        .unwrap();
    assert_eq!(store.get(b"marker").unwrap().as_deref(), Some(&b"ok"[..]));
}

#[test]
fn read_set_byte_limit_rejects_new_range_before_reading() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    store
        .transaction_with_read_set(|tx| {
            for index in 0..=1024_u32 {
                let mut lower = vec![1; 1024];
                lower[1..5].copy_from_slice(&index.to_be_bytes());
                let upper = vec![2; 1024];
                let result = tx.scan(Some(&lower), Some(&upper), 1);
                if index < 1024 {
                    assert!(result?.is_empty());
                } else {
                    assert!(result.unwrap_err().to_string().contains("read-set limit"));
                }
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn validation_work_limit_rejects_without_publishing_staged_writes() {
    use database_engine::BatchOperation;
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let keys = (0..1000_u16)
        .map(|index| [b'k', (index >> 8) as u8, index as u8])
        .collect::<Vec<_>>();
    let operations = keys
        .iter()
        .map(|key| BatchOperation::Put { key, value: b"v" })
        .collect::<Vec<_>>();
    store.write_batch(&operations).unwrap();
    let run = |ranges: u16, marker: &[u8]| {
        store.transaction_with_read_set(|tx| {
            for index in 0..ranges {
                let lower = [b'a', (index >> 8) as u8, index as u8];
                assert_eq!(tx.scan(Some(&lower), None, 1)?.len(), 1);
            }
            store.put(b"0", b"outside")?;
            tx.put(marker, b"staged")
        })
    };
    run(65, b"ok-marker").unwrap();
    let error = run(66, b"z-failed").unwrap_err();
    assert!(error.to_string().contains("validation work limit"));
    assert_eq!(
        store.get(b"ok-marker").unwrap().as_deref(),
        Some(&b"staged"[..])
    );
    assert_eq!(store.get(b"z-failed").unwrap(), None);
}
