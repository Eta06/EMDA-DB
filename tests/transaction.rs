use database_engine::{BatchOperation as Op, Store};
use std::io::{self, ErrorKind};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

#[test]
fn transaction_reads_own_writes_and_commits_atomically_after_callback() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"a", b"old").unwrap();
    store.put(b"removed", b"old").unwrap();
    let result = store
        .transaction(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
            tx.put(b"a", b"first")?;
            tx.put(b"a", b"last")?;
            tx.delete(b"removed")?;
            tx.put(b"empty", b"")?;
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"last"[..]));
            assert_eq!(tx.get(b"removed")?, None);
            assert_eq!(tx.get(b"empty")?.as_deref(), Some(&b""[..]));
            assert_eq!(store.get(b"a")?.as_deref(), Some(&b"old"[..]));
            Ok(17)
        })
        .unwrap();
    assert_eq!(result, 17);
    let expected = vec![Some(b"last".to_vec()), None, Some(vec![])];
    assert_eq!(
        store.get_many(&[b"a", b"removed", b"empty"]).unwrap(),
        expected
    );
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get_many(&[b"a", b"removed", b"empty"])
            .unwrap(),
        expected
    );
}

#[test]
fn callback_error_and_panic_discard_staged_writes_and_release_resources() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"key", b"old").unwrap();
    let path = dir.path().join("records.log");
    let before = std::fs::read(&path).unwrap();
    let result: io::Result<()> = store.transaction(|tx| {
        tx.put(b"key", b"new")?;
        Err(io::Error::other("abort"))
    });
    assert!(result.is_err());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: io::Result<()> = store.transaction(|tx| {
            tx.put(b"key", b"new")?;
            panic!("abort callback")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"old"[..]));
    store.transaction(|tx| tx.put(b"key", b"after")).unwrap();
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"after"[..]));
}

#[test]
fn conflict_on_unrelated_write_does_not_publish_staged_changes_or_retry_callback() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"key", b"old").unwrap();
    let mut calls = 0;
    let error = store
        .transaction(|tx| {
            calls += 1;
            assert_eq!(tx.get(b"key")?.as_deref(), Some(&b"old"[..]));
            tx.put(b"key", b"staged")?;
            store.put(b"unrelated", b"concurrent")?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert_eq!(calls, 1);
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(
        store.get(b"unrelated").unwrap().as_deref(),
        Some(&b"concurrent"[..])
    );
}

#[test]
fn concurrent_write_skew_attempt_has_at_most_one_committing_transaction() {
    let dir = tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    store
        .write_batch(&[
            Op::Put {
                key: b"a",
                value: b"1",
            },
            Op::Put {
                key: b"b",
                value: b"1",
            },
        ])
        .unwrap();
    let barrier = Barrier::new(2);
    let outcomes = thread::scope(|scope| {
        let mut workers = Vec::new();
        for key in [b"a".as_slice(), b"b".as_slice()] {
            let (store, barrier) = (&store, &barrier);
            workers.push(scope.spawn(move || {
                store.transaction(|tx| {
                    assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"1"[..]));
                    assert_eq!(tx.get(b"b")?.as_deref(), Some(&b"1"[..]));
                    barrier.wait();
                    tx.put(key, b"0")?;
                    Ok(())
                })
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, Err(e) if e.kind() == ErrorKind::WouldBlock))
            .count(),
        1
    );
    let values = store.get_many(&[b"a", b"b"]).unwrap();
    assert!(values.contains(&Some(b"1".to_vec())));
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get_many(&[b"a", b"b"])
            .unwrap(),
        values
    );
}

#[test]
fn transaction_replaces_key_at_quota_and_checks_staged_size_without_partial_commit() {
    let dir = tempdir().unwrap();
    let store = Store::open_with_limits(dir.path(), None, Some(1)).unwrap();
    store.put(b"a", b"old").unwrap();
    store
        .transaction(|tx| {
            tx.put(b"b", b"new")?;
            tx.delete(b"a")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store.get_many(&[b"a", b"b"]).unwrap(),
        vec![None, Some(b"new".to_vec())]
    );
    let err = store
        .transaction(|tx| {
            tx.put(b"c", b"too-many")?;
            Ok(())
        })
        .unwrap_err();
    assert!(err.to_string().contains("key limit"));
    assert_eq!(store.get(b"c").unwrap(), None);
    let too_large = vec![0; 16 * 1024 * 1024];
    store
        .transaction(|tx| {
            assert_eq!(
                tx.put(b"b", &too_large).unwrap_err().kind(),
                ErrorKind::InvalidInput
            );
            tx.put(b"b", b"still-valid")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store.get(b"b").unwrap().as_deref(),
        Some(&b"still-valid"[..])
    );
}

#[test]
fn transaction_scan_merges_staged_changes_and_detects_phantoms() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    for key in [b"a", b"c", b"e"] {
        store.put(key, b"old").unwrap();
    }
    store
        .transaction(|tx| {
            tx.delete(b"a")?;
            tx.put(b"b", b"insert")?;
            tx.put(b"c", b"new")?;
            tx.put(b"d", b"insert")?;
            let first = tx.scan(None, None, 2)?;
            assert_eq!(
                first,
                vec![
                    (b"b".to_vec(), b"insert".to_vec()),
                    (b"c".to_vec(), b"new".to_vec())
                ]
            );
            let second = tx.scan(Some(&first[1].0), None, 2)?;
            assert_eq!(
                second,
                vec![
                    (b"d".to_vec(), b"insert".to_vec()),
                    (b"e".to_vec(), b"old".to_vec())
                ]
            );
            assert!(tx.scan(Some(&second[1].0), None, 2)?.is_empty());
            assert_eq!(
                tx.scan(Some(b"b"), Some(b"e"), 5)?,
                vec![
                    (b"c".to_vec(), b"new".to_vec()),
                    (b"d".to_vec(), b"insert".to_vec())
                ]
            );
            Ok(())
        })
        .unwrap();
    let error = store
        .transaction(|tx| {
            let _ = tx.scan(Some(b"a"), Some(b"z"), 10)?;
            store.put(b"phantom", b"external")?;
            tx.put(b"staged", b"discard")?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert_eq!(store.get(b"staged").unwrap(), None);
}

#[test]
fn retried_concurrent_transactions_do_not_lose_updates() {
    let dir = tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    store.put(b"counter", &0_i64.to_le_bytes()).unwrap();
    let barrier = Barrier::new(9);
    thread::scope(|scope| {
        for _ in 0..8 {
            let (store, barrier) = (&store, &barrier);
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..20 {
                    loop {
                        match store.transaction(|tx| {
                            let current: [u8; 8] = tx.get(b"counter")?.unwrap().try_into().unwrap();
                            tx.put(b"counter", &(i64::from_le_bytes(current) + 1).to_le_bytes())
                        }) {
                            Ok(()) => break,
                            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                                thread::yield_now()
                            }
                            Err(error) => panic!("unexpected transaction error: {error}"),
                        }
                    }
                }
            });
        }
        barrier.wait();
    });
    assert_eq!(
        store.get(b"counter").unwrap().as_deref(),
        Some(&160_i64.to_le_bytes()[..])
    );
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get(b"counter")
            .unwrap()
            .as_deref(),
        Some(&160_i64.to_le_bytes()[..])
    );
}

#[test]
fn aba_and_read_only_concurrent_mutations_trigger_conflicts() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"a", b"original").unwrap();
    let error = store
        .transaction(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"original"[..]));
            store.put(b"a", b"temporary")?;
            store.put(b"a", b"original")?;
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"original"[..]));
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    let error = store
        .transaction(|tx| {
            assert_eq!(tx.get(b"absent")?, None);
            store.delete(b"absent")?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
}

#[test]
fn transaction_crash_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("EMDA_TX_CRASH_PATH") else {
        return;
    };
    let store = Store::open(path).unwrap();
    store
        .transaction(|tx| {
            tx.put(b"a", b"committed")?;
            tx.put(b"b", b"committed")?;
            Ok(())
        })
        .unwrap();
    println!("TRANSACTION_ACKNOWLEDGED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn acknowledged_transaction_survives_process_kill() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    let dir = tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "transaction_crash_child", "--nocapture"])
        .env("EMDA_TX_CRASH_PATH", dir.path())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in std::io::BufReader::new(output)
            .lines()
            .map_while(Result::ok)
        {
            if line.contains("TRANSACTION_ACKNOWLEDGED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let ack = rx.recv_timeout(Duration::from_secs(20));
    child.kill().unwrap();
    child.wait().unwrap();
    ack.expect("transaction process did not acknowledge");
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get_many(&[b"a", b"b"])
            .unwrap(),
        vec![Some(b"committed".to_vec()), Some(b"committed".to_vec())]
    );
}

#[test]
fn transaction_snapshot_limit_rejects_before_callback_and_keeps_store_usable() {
    use std::cell::Cell;
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let keys = (0..=database_engine::MAX_SNAPSHOT_KEYS)
        .map(|i| (i as u32).to_be_bytes())
        .collect::<Vec<_>>();
    for chunk in keys.chunks(database_engine::MAX_ATOMIC_BATCH_OPERATIONS) {
        let ops = chunk
            .iter()
            .map(|key| Op::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        store.write_batch(&ops).unwrap();
    }
    let called = Cell::new(false);
    let error = store
        .transaction(|_| {
            called.set(true);
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("snapshot key count"));
    assert!(!called.get());
    assert_eq!(store.get(&keys[0]).unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn transaction_log_limit_rejects_without_partial_publication() {
    let dir = tempdir().unwrap();
    let store = Store::open_with_log_limit(dir.path(), 28).unwrap();
    let before = std::fs::read(dir.path().join("records.log")).unwrap();
    let error = store
        .transaction(|tx| {
            tx.put(b"a", b"1")?;
            tx.put(b"b", b"2")?;
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("log size limit"));
    assert_eq!(
        std::fs::read(dir.path().join("records.log")).unwrap(),
        before
    );
    assert_eq!(store.get_many(&[b"a", b"b"]).unwrap(), vec![None, None]);
}
