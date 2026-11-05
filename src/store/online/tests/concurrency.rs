use super::*;
use crate::BatchOperation;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[test]
fn handoff_keeps_a_transaction_limit_changed_during_copy() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    store
        .with_store(|db| {
            db.put(b"key", b"value")?;
            db.set_active_direct_write_limit(5)?;
            db.set_group_commit_limits(4096, 4)?;
            db.set_transaction_staging_limit(512)?;
            db.set_active_transaction_limit(8)
        })
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker = Arc::clone(&store);
    let thread = thread::spawn(move || {
        worker.compact_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == copy::Point::BeforeFinalBarrier {
                    ready_tx.send(()).unwrap();
                    release_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    store
        .with_store(|db| {
            db.set_active_direct_write_limit(2)?;
            db.set_group_commit_limits(2048, 2)?;
            db.set_transaction_staging_limit(128)?;
            db.set_active_transaction_limit(3)
        })
        .unwrap();
    release_tx.send(()).unwrap();
    thread.join().unwrap().unwrap();
    assert_eq!(
        store
            .with_store(|db| Ok(db.direct_write_status().limit))
            .unwrap(),
        2
    );
    let group = store.with_store(|db| db.group_commit_status()).unwrap();
    assert_eq!((group.limit_bytes, group.limit_records), (2048, 2));
    assert_eq!(
        store
            .with_store(|db| Ok(db.transaction_budget_status()?.limit_bytes))
            .unwrap(),
        128
    );
    assert_eq!(
        store
            .with_store(|db| Ok(db.transaction_budget_status()?.active_limit))
            .unwrap(),
        3
    );
}

#[test]
fn catches_up_acknowledged_writes_and_retains_previous_store() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    insert_history(&store);
    let (stage_tx, stage_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let worker = Arc::clone(&store);
    let old_path = previous.clone();
    let thread = thread::spawn(move || {
        worker.compact_with_hooks(
            &old_path,
            &OnlineCompactionOptions::default(),
            |point| {
                if matches!(
                    point,
                    copy::Point::InitialCopy | copy::Point::BeforeFinalBarrier
                ) {
                    stage_tx.send(point).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
    });
    assert_eq!(
        stage_rx.recv_timeout(WAIT).unwrap(),
        copy::Point::InitialCopy
    );
    store
        .with_store(|db| {
            db.write_batch(&[
                BatchOperation::Put {
                    key: b"overwritten",
                    value: b"concurrent",
                },
                BatchOperation::Delete { key: b"delete" },
                BatchOperation::Put {
                    key: b"batch",
                    value: b"committed",
                },
            ])
        })
        .unwrap();
    resume_tx.send(()).unwrap();
    assert_eq!(
        stage_rx.recv_timeout(WAIT).unwrap(),
        copy::Point::BeforeFinalBarrier
    );
    store.with_store(|db| db.put(b"final", b"tail")).unwrap();
    resume_tx.send(()).unwrap();
    let report = thread.join().unwrap().unwrap();
    assert!(report.concurrent_replay_bytes > 0);
    assert!(report.final_replay_bytes > 0);
    assert!(report.compacted_log_bytes < report.final_log_bytes);
    store
        .with_store(|db| {
            assert_eq!(db.get(b"overwritten")?, Some(b"concurrent".to_vec()));
            assert_eq!(db.get(b"delete")?, None);
            assert_eq!(db.get(b"batch")?, Some(b"committed".to_vec()));
            assert_eq!(db.get(b"final")?, Some(b"tail".to_vec()));
            db.put(b"after", b"only new")
        })
        .unwrap();
    let old = retry_lock(|| Store::open(&previous)).unwrap();
    assert_eq!(
        old.get(b"overwritten").unwrap(),
        Some(b"concurrent".to_vec())
    );
    assert_eq!(old.get(b"final").unwrap(), Some(b"tail".to_vec()));
    assert_eq!(old.get(b"after").unwrap(), None);
    drop(old);
    drop(store);
    let reopened = retry_lock(|| Store::open(&source)).unwrap();
    assert_eq!(reopened.get(b"after").unwrap(), Some(b"only new".to_vec()));
}

#[test]
fn active_snapshot_finishes_before_handoff() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    store.with_store(|s| s.put(b"key", b"before")).unwrap();
    let (snap_tx, snap_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let reader_store = Arc::clone(&store);
    let reader = thread::spawn(move || {
        reader_store
            .with_store(|s| {
                let snapshot = s.snapshot(10, 100)?;
                snap_tx.send(()).unwrap();
                release_rx.recv_timeout(WAIT).unwrap();
                assert_eq!(snapshot.get(b"key")?, Some(b"before".to_vec()));
                Ok(())
            })
            .unwrap()
    });
    snap_rx.recv_timeout(WAIT).unwrap();
    store.with_store(|s| s.put(b"key", b"after")).unwrap();
    let (barrier_tx, barrier_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let compactor = Arc::clone(&store);
    let worker = thread::spawn(move || {
        let result = compactor.compact_with_hooks(
            &previous,
            &OnlineCompactionOptions::default(),
            |point| {
                if point == copy::Point::BeforeFinalBarrier {
                    barrier_tx.send(()).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        );
        done_tx.send(result).unwrap();
    });
    barrier_rx.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release_tx.send(()).unwrap();
    reader.join().unwrap();
    done_rx.recv_timeout(WAIT).unwrap().unwrap();
    worker.join().unwrap();
    assert_eq!(
        store.with_store(|s| s.get(b"key")).unwrap(),
        Some(b"after".to_vec())
    );
}

#[test]
fn nested_access_and_overlapping_compactions_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    store
        .with_store(|_| {
            assert_eq!(
                store.with_store(|_| Ok(())).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                store
                    .compact_and_replace(&previous, &Default::default())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            Ok(())
        })
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let other = Arc::clone(&store);
    let old = previous.clone();
    let worker = thread::spawn(move || {
        other.compact_with_hooks(
            &old,
            &Default::default(),
            |point| {
                if point == copy::Point::InitialCopy {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
    });
    ready_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(
        store
            .compact_and_replace(root.path().join("other"), &Default::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    resume_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
}

#[test]
fn transaction_finishes_before_handoff_and_its_commit_is_replayed() {
    let root = tempfile::tempdir().unwrap();
    let (source, previous) = paths(root.path());
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    store.with_store(|s| s.put(b"account", b"old")).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let tx_store = Arc::clone(&store);
    let transaction = thread::spawn(move || {
        tx_store
            .with_store(|s| {
                s.transaction_with_read_set(|tx| {
                    assert_eq!(tx.get(b"account")?, Some(b"old".to_vec()));
                    tx.put(b"account", b"new")?;
                    tx.put(b"audit", b"committed")?;
                    started_tx.send(()).unwrap();
                    finish_rx.recv_timeout(WAIT).unwrap();
                    Ok(())
                })
            })
            .unwrap()
    });
    started_rx.recv_timeout(WAIT).unwrap();
    let (barrier_tx, barrier_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let compactor = Arc::clone(&store);
    let worker = thread::spawn(move || {
        let result = compactor.compact_with_hooks(
            &previous,
            &Default::default(),
            |point| {
                if point == copy::Point::BeforeFinalBarrier {
                    barrier_tx.send(()).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        );
        done_tx.send(result).unwrap();
    });
    barrier_rx.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    finish_tx.send(()).unwrap();
    transaction.join().unwrap();
    let report = done_rx.recv_timeout(WAIT).unwrap().unwrap();
    assert!(report.final_replay_bytes > 0);
    worker.join().unwrap();
    store
        .with_store(|s| {
            assert_eq!(s.get(b"account")?, Some(b"new".to_vec()));
            assert_eq!(s.get(b"audit")?, Some(b"committed".to_vec()));
            Ok(())
        })
        .unwrap();
}

#[test]
fn concurrent_acknowledged_increments_survive_repeated_handoffs() {
    use std::sync::Barrier;
    const WRITERS: usize = 4;
    const PER_WRITER: usize = 32;
    for _ in 0..8 {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = Arc::new(OnlineStore::open(&source).unwrap());
        let start = Arc::new(Barrier::new(WRITERS + 1));
        let (progress_tx, progress_rx) = mpsc::channel();
        let mut writers = Vec::new();
        for _ in 0..WRITERS {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            let progress = progress_tx.clone();
            writers.push(thread::spawn(move || {
                start.wait();
                let mut observed = Vec::new();
                for index in 0..PER_WRITER {
                    observed.push(
                        store
                            .with_store(|s| s.increment_i64(b"counter", 1))
                            .unwrap(),
                    );
                    if index == 0 {
                        progress.send(()).unwrap();
                    }
                }
                observed
            }));
        }
        let compactor = Arc::clone(&store);
        let old_path = previous.clone();
        let worker = thread::spawn(move || {
            compactor.compact_with_hooks(
                &old_path,
                &Default::default(),
                |point| {
                    if point == copy::Point::InitialCopy {
                        start.wait();
                        for _ in 0..WRITERS {
                            progress_rx.recv_timeout(WAIT).unwrap();
                        }
                    }
                    Ok(())
                },
                crate::directory::sync_existing,
            )
        });
        let report = worker.join().unwrap().unwrap();
        assert!(report.concurrent_replay_bytes > 0);
        let mut observed = writers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        observed.sort_unstable();
        assert_eq!(
            observed,
            (1..=(WRITERS * PER_WRITER) as i64).collect::<Vec<_>>()
        );
        let expected = ((WRITERS * PER_WRITER) as i64).to_le_bytes().to_vec();
        assert_eq!(
            store.with_store(|s| s.get(b"counter")).unwrap(),
            Some(expected.clone())
        );
        let old = retry_lock(|| Store::open(&previous)).unwrap();
        let old_value: [u8; 8] = old.get(b"counter").unwrap().unwrap().try_into().unwrap();
        assert!(
            (WRITERS as i64..=(WRITERS * PER_WRITER) as i64)
                .contains(&i64::from_le_bytes(old_value))
        );
        drop(old);
        drop(store);
        assert_eq!(
            retry_lock(|| Store::open(&source))
                .unwrap()
                .get(b"counter")
                .unwrap(),
            Some(expected)
        );
    }
}
