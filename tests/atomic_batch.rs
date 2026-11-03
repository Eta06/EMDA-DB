use database_engine::{BatchOperation as Op, MAX_ATOMIC_BATCH_OPERATIONS, Store};
use std::fs;
use std::io::ErrorKind;
use tempfile::tempdir;

#[test]
fn batch_applies_ordered_puts_and_deletes_and_survives_recovery_backup_and_compaction() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("live")).unwrap();
    store.put(b"a", b"old").unwrap();
    store.put(b"remove", b"old").unwrap();
    let snapshot = store.snapshot(2, 7).unwrap();
    store
        .write_batch(&[
            Op::Put {
                key: b"a",
                value: b"first",
            },
            Op::Delete { key: b"remove" },
            Op::Put {
                key: b"b",
                value: b"",
            },
            Op::Put {
                key: b"a",
                value: b"last",
            },
            Op::Delete { key: b"missing" },
        ])
        .unwrap();
    let expected = vec![Some(b"last".to_vec()), None, Some(vec![])];
    assert_eq!(store.get_many(&[b"a", b"remove", b"b"]).unwrap(), expected);
    assert_eq!(snapshot.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
    assert_eq!(snapshot.get(b"b").unwrap(), None);
    drop(snapshot);
    let backup = dir.path().join("backup");
    store.backup(&backup).unwrap();
    let restored = dir.path().join("restored");
    Store::restore(&backup, &restored).unwrap();
    assert_eq!(
        Store::open(&restored)
            .unwrap()
            .get_many(&[b"a", b"remove", b"b"])
            .unwrap(),
        expected
    );
    let compacted = dir.path().join("compacted");
    store.compact_to(&compacted).unwrap();
    assert_eq!(
        Store::open(&compacted)
            .unwrap()
            .get_many(&[b"a", b"remove", b"b"])
            .unwrap(),
        expected
    );
    drop(store);
    assert_eq!(
        Store::open(dir.path().join("live"))
            .unwrap()
            .get_many(&[b"a", b"remove", b"b"])
            .unwrap(),
        expected
    );
}

#[test]
fn invalid_batches_and_quota_rejection_do_not_append_anything() {
    let dir = tempdir().unwrap();
    let store = Store::open_with_limits(dir.path(), None, Some(2)).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"b", b"two").unwrap();
    let path = dir.path().join("records.log");
    let before = fs::read(&path).unwrap();
    assert_eq!(
        store.write_batch(&[]).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store
            .write_batch(&[Op::Put {
                key: b"",
                value: b"bad"
            }])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    let many = vec![Op::Delete { key: b"a" }; MAX_ATOMIC_BATCH_OPERATIONS + 1];
    assert_eq!(
        store.write_batch(&many).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    // Intermediate states count: create before delete would exceed the cap.
    assert!(
        store
            .write_batch(&[
                Op::Put {
                    key: b"c",
                    value: b"three"
                },
                Op::Delete { key: b"a" }
            ])
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    store
        .write_batch(&[
            Op::Delete { key: b"a" },
            Op::Put {
                key: b"c",
                value: b"three",
            },
        ])
        .unwrap();
    drop(store);
    let recovered = Store::open_with_limits(dir.path(), None, Some(2)).unwrap();
    assert_eq!(
        recovered.get_many(&[b"a", b"b", b"c"]).unwrap(),
        vec![None, Some(b"two".to_vec()), Some(b"three".to_vec())]
    );
}

#[test]
fn atomic_batch_respects_log_limit_and_rejects_legacy_format() {
    let dir = tempdir().unwrap();
    let store = Store::open_with_log_limit(dir.path().join("limited"), 28).unwrap();
    assert!(
        store
            .write_batch(&[Op::Put {
                key: b"a",
                value: b"1"
            }])
            .is_err()
    );
    assert_eq!(
        fs::metadata(dir.path().join("limited/records.log"))
            .unwrap()
            .len(),
        28
    );
    let legacy = dir.path().join("legacy");
    fs::create_dir(&legacy).unwrap();
    fs::write(
        legacy.join("records.log"),
        include_bytes!("fixtures/db01-legacy-record.bin"),
    )
    .unwrap();
    let store = Store::open(&legacy).unwrap();
    let before = fs::read(legacy.join("records.log")).unwrap();
    assert_eq!(
        store
            .write_batch(&[Op::Delete { key: b"x" }])
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(fs::read(legacy.join("records.log")).unwrap(), before);
}

#[test]
fn persistent_v2_batch_fixture_reopens_and_remains_writable() {
    let dir = tempdir().unwrap();
    fs::write(
        dir.path().join("records.log"),
        include_bytes!("fixtures/v2-batch-records.log"),
    )
    .unwrap();
    fs::write(
        dir.path().join("commit.state"),
        include_bytes!("fixtures/v2-batch-commit.state"),
    )
    .unwrap();
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(
        store.get_many(&[b"alpha", b"beta"]).unwrap(),
        vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
    );
    store
        .write_batch(&[
            Op::Delete { key: b"alpha" },
            Op::Put {
                key: b"beta",
                value: b"new",
            },
        ])
        .unwrap();
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get_many(&[b"alpha", b"beta"])
            .unwrap(),
        vec![None, Some(b"new".to_vec())]
    );
}

#[test]
fn concurrent_readers_never_observe_partial_batch() {
    use std::sync::{Arc, Barrier};
    use std::thread;
    let dir = tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    store
        .write_batch(&[
            Op::Put {
                key: b"a",
                value: &[0],
            },
            Op::Put {
                key: b"b",
                value: &[0],
            },
        ])
        .unwrap();
    let barrier = Barrier::new(5);
    thread::scope(|scope| {
        let (store_write, barrier_write) = (&store, &barrier);
        scope.spawn(move || {
            barrier_write.wait();
            for i in 1..=100u8 {
                store_write
                    .write_batch(&[
                        Op::Put {
                            key: b"a",
                            value: &[i],
                        },
                        Op::Put {
                            key: b"b",
                            value: &[i],
                        },
                    ])
                    .unwrap();
            }
        });
        for _ in 0..3 {
            let (store_read, barrier_read) = (&store, &barrier);
            scope.spawn(move || {
                barrier_read.wait();
                for _ in 0..500 {
                    let pair = store_read.get_many(&[b"a", b"b"]).unwrap();
                    assert_eq!(pair[0], pair[1]);
                }
            });
        }
        barrier.wait();
    });
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get_many(&[b"a", b"b"])
            .unwrap(),
        vec![Some(vec![100]), Some(vec![100])]
    );
}

#[test]
fn batch_crash_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("EMDA_BATCH_CRASH_PATH") else {
        return;
    };
    let store = Store::open(path).unwrap();
    for i in 0..25u8 {
        store
            .write_batch(&[
                Op::Put {
                    key: b"a",
                    value: &[i],
                },
                Op::Put {
                    key: b"b",
                    value: &[i],
                },
            ])
            .unwrap();
    }
    println!("BATCH_ACKNOWLEDGED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn acknowledged_batch_survives_process_kill() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    let dir = tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "batch_crash_child", "--nocapture"])
        .env("EMDA_BATCH_CRASH_PATH", dir.path())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(output)
            .lines()
            .map_while(Result::ok)
        {
            if line.contains("BATCH_ACKNOWLEDGED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let ack = rx.recv_timeout(Duration::from_secs(20));
    child.kill().unwrap();
    child.wait().unwrap();
    ack.expect("batch process did not acknowledge");
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .get_many(&[b"a", b"b"])
            .unwrap(),
        vec![Some(vec![24]), Some(vec![24])]
    );
}

#[test]
fn encoded_batch_byte_limit_rejects_oversized_payload_before_append() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let before = fs::read(dir.path().join("records.log")).unwrap();
    let value = vec![7; database_engine::MAX_ATOMIC_BATCH_BYTES];
    assert_eq!(
        store
            .write_batch(&[Op::Put {
                key: b"key",
                value: &value
            }])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(fs::read(dir.path().join("records.log")).unwrap(), before);
    assert_eq!(store.get(b"key").unwrap(), None);
}
