use super::*;
use crate::test_support::retry_lock;
use std::fs;
use std::sync::{Barrier, mpsc};
use std::time::Instant;

const WAIT: Duration = Duration::from_secs(10);

fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf, Arc<OnlineStore>) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let previous = root.path().join("previous");
    let store = Arc::new(OnlineStore::open(&source).unwrap());
    (root, source, previous, store)
}

fn wait_for(
    worker: &OnlineReclamationWorker,
    predicate: impl Fn(&OnlineReclamationWorkerStatus) -> bool,
) -> OnlineReclamationWorkerStatus {
    let deadline = Instant::now() + WAIT;
    loop {
        let status = worker.status();
        if predicate(&status) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "reclamation worker timed out: {status:?}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn start(
    store: &Arc<OnlineStore>,
    previous: &Path,
    interval: Duration,
    minimum_log: u64,
    minimum_growth: u64,
) -> OnlineReclamationWorker {
    store
        .start_reclamation_worker(
            previous,
            interval,
            minimum_log,
            minimum_growth,
            Default::default(),
        )
        .unwrap()
}

#[test]
fn requested_checks_respect_thresholds_and_recover_all_writes() {
    let (_root, source, previous, store) = fixture();
    let minimum_log = store.reclamation_observation().unwrap().bytes + 1;
    let worker = start(
        &store,
        &previous,
        Duration::from_secs(3600),
        minimum_log,
        400,
    );
    worker.request_check().unwrap();
    assert_eq!(wait_for(&worker, |s| s.checks >= 1).reclaimed, 0);
    store
        .with_store(|db| {
            for n in 0..20 {
                db.put(b"key", n.to_string().as_bytes())?;
            }
            Ok(())
        })
        .unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.reclaimed >= 1);
    assert_eq!(status.handoffs, 1);
    assert!(status.last_report.is_some());
    assert!(!previous.exists());
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.checks >= 3);
    assert_eq!(status.reclaimed, 1);
    store.with_store(|db| db.put(b"small", b"a")).unwrap();
    worker.request_check().unwrap();
    assert_eq!(wait_for(&worker, |s| s.checks >= 4).reclaimed, 1);
    store.with_store(|db| db.put(b"large", &[42; 500])).unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.reclaimed >= 2);
    assert_eq!(status.handoffs, 2);
    assert!(worker.stop().unwrap().stopped);
    drop(store);
    let reopened = retry_lock(|| Store::open(&source)).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"19".to_vec()));
    assert_eq!(reopened.get(b"small").unwrap(), Some(b"a".to_vec()));
    assert_eq!(reopened.get(b"large").unwrap(), Some(vec![42; 500]));
}

#[test]
fn periodic_checks_run_once_for_unchanged_data_and_lease_is_released() {
    let (_root, _source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"value")).unwrap();
    let worker = start(&store, &previous, Duration::from_millis(20), 1, 1);
    assert!(
        matches!(store.start_reclamation_worker(&previous, Duration::from_secs(1), 1, 1, Default::default()), Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists)
    );
    let status = wait_for(&worker, |s| s.checks >= 4);
    assert_eq!((status.handoffs, status.reclaimed), (1, 1));
    assert!(status.last_error.is_none());
    assert!(worker.stop().unwrap().stopped);
    let replacement = start(&store, &previous, Duration::from_secs(3600), 1, 1);
    drop(replacement);
    assert_eq!(Arc::strong_count(&store), 1);
}

#[test]
fn invalid_configuration_and_occupied_paths_are_rejected() {
    let (root, _source, previous, store) = fixture();
    for (interval, log, growth) in [
        (Duration::ZERO, 1, 1),
        (Duration::from_micros(999), 1, 1),
        (Duration::from_secs(86_401), 1, 1),
        (Duration::from_secs(1), 0, 1),
        (Duration::from_secs(1), 1, 0),
    ] {
        assert!(
            matches!(store.start_reclamation_worker(&previous, interval, log, growth, Default::default()), Err(ref e) if e.kind() == io::ErrorKind::InvalidInput)
        );
    }
    let invalid = OnlineCompactionOptions {
        catchup_rounds: 17,
        ..Default::default()
    };
    assert!(
        matches!(store.start_reclamation_worker(&previous, Duration::from_secs(1), 1, 1, invalid), Err(ref e) if e.kind() == io::ErrorKind::InvalidInput)
    );
    let other = tempfile::tempdir().unwrap();
    assert!(
        matches!(store.start_reclamation_worker(other.path().join("other"), Duration::from_secs(1), 1, 1, Default::default()), Err(ref e) if e.kind() == io::ErrorKind::InvalidInput)
    );
    fs::write(&previous, b"untouched").unwrap();
    assert!(
        matches!(store.start_reclamation_worker(&previous, Duration::from_secs(1), 1, 1, Default::default()), Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists)
    );
    assert_eq!(fs::read(&previous).unwrap(), b"untouched");
    let nested = store.with_store(|_| {
        assert!(matches!(store.start_reclamation_worker(root.path().join("nested"), Duration::from_secs(1), 1, 1, Default::default()), Err(ref e) if e.kind() == io::ErrorKind::InvalidInput));
        Ok(())
    });
    nested.unwrap();
}

#[test]
fn maintenance_contention_defers_then_reclaims_without_losing_writes() {
    let (_root, source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"safe")).unwrap();
    let before = fs::read(source.join("records.log")).unwrap();
    let worker = start(&store, &previous, Duration::from_secs(3600), 1, 1);
    let held = store.maintenance.lock().unwrap();
    let error = store
        .compact_and_reclaim(&previous, &Default::default())
        .unwrap_err();
    assert!(
        matches!(error, OnlineReclaimError::Compaction(ref error) if error.kind() == io::ErrorKind::WouldBlock)
    );
    for count in 1..=3 {
        worker.request_check().unwrap();
        let status = wait_for(&worker, |s| s.checks >= count);
        assert!(!status.stopped);
        assert!(status.last_error.is_none());
        assert_eq!((status.handoffs, status.reclaimed), (0, 0));
        assert_eq!(worker.busy_checks(), count);
        assert_eq!(fs::read(source.join("records.log")).unwrap(), before);
        assert!(!previous.exists());
    }
    drop(held);
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.reclaimed == 1 || s.stopped);
    assert!(!status.stopped, "{status:?}");
    assert_eq!(worker.busy_checks(), 3);
    assert_eq!((status.handoffs, status.reclaimed), (1, 1));
    worker.stop().unwrap();
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"safe".to_vec())
    );
}

#[test]
fn periodic_worker_retries_after_contention_clears() {
    let (_root, source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"safe")).unwrap();
    let held = store.maintenance.lock().unwrap();
    let worker = start(&store, &previous, Duration::from_millis(20), 1, 1);
    let status = wait_for(&worker, |s| s.checks >= 2);
    assert!(!status.stopped);
    assert!(worker.busy_checks() >= 2);
    drop(held);
    let status = wait_for(&worker, |s| s.reclaimed == 1 || s.stopped);
    assert!(!status.stopped, "{status:?}");
    worker.stop().unwrap();
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"safe".to_vec())
    );
}

#[test]
fn deferred_worker_reobserves_store_after_a_concurrent_manual_handoff() {
    let (root, source, previous, store) = fixture();
    let manual_previous = root.path().join("manual-previous");
    store
        .with_store(|db| db.put(b"first", b"acknowledged"))
        .unwrap();
    let worker = start(&store, &previous, Duration::from_secs(3600), 1, 1);
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let manual_store = Arc::clone(&store);
    let manual = thread::spawn(move || {
        manual_store.compact_with_hooks(
            &manual_previous,
            &Default::default(),
            |point| {
                if point == super::super::copy::Point::InitialCopy {
                    started_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                }
                Ok(())
            },
            crate::directory::sync_existing,
        )
    });
    started_rx.recv_timeout(WAIT).unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.checks >= 1);
    assert!(!status.stopped);
    assert_eq!(worker.busy_checks(), 1);
    store
        .with_store(|db| db.put(b"during", b"acknowledged"))
        .unwrap();
    resume_tx.send(()).unwrap();
    manual.join().unwrap().unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.reclaimed == 1 || s.stopped);
    assert!(!status.stopped, "{status:?}");
    worker.stop().unwrap();
    drop(store);
    let reopened = retry_lock(|| Store::open(&source)).unwrap();
    assert_eq!(
        reopened.get(b"first").unwrap(),
        Some(b"acknowledged".to_vec())
    );
    assert_eq!(
        reopened.get(b"during").unwrap(),
        Some(b"acknowledged".to_vec())
    );
}

#[test]
fn stopping_a_deferred_worker_does_not_wait_for_the_maintenance_lock() {
    let (_root, source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"safe")).unwrap();
    let held = store.maintenance.lock().unwrap();
    let worker = start(&store, &previous, Duration::from_secs(3600), 1, 1);
    worker.request_check().unwrap();
    wait_for(&worker, |s| s.checks >= 1);
    assert_eq!(worker.busy_checks(), 1);
    let status = worker.stop().unwrap();
    assert!(status.stopped);
    assert_eq!((status.handoffs, status.reclaimed), (0, 0));
    drop(held);
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"safe".to_vec())
    );
}

#[test]
fn newly_occupied_path_stops_without_retry_or_deletion() {
    let (_root, source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"safe")).unwrap();
    let worker = start(&store, &previous, Duration::from_secs(3600), 1, 1);
    fs::write(&previous, b"keep").unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.stopped);
    assert_eq!(
        status.last_error.unwrap().stage,
        OnlineReclamationFailureStage::Compaction
    );
    assert_eq!(
        (status.handoffs, status.reclaimed, worker.busy_checks()),
        (0, 0, 0)
    );
    assert_eq!(
        worker.request_check().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(fs::read(&previous).unwrap(), b"keep");
    worker.stop().unwrap();
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"safe".to_vec())
    );
}

#[test]
fn unrelated_would_block_error_is_not_safely_retryable() {
    let (_root, _source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"safe")).unwrap();
    let schedule = Schedule {
        previous,
        interval: Duration::from_secs(1),
        minimum_log_bytes: 1,
        minimum_growth_bytes: 1,
        options: Default::default(),
    };
    let outcome = check_with(&store, &schedule, None, || {
        Err(OnlineReclaimError::Compaction(io::Error::new(
            io::ErrorKind::WouldBlock,
            "injected I/O failure",
        )))
    });
    let Outcome::Failed(failure) = outcome else {
        panic!("ambiguous I/O errors must stop the worker")
    };
    assert_eq!(failure.stage, OnlineReclamationFailureStage::Compaction);
    assert_eq!(failure.error.kind(), io::ErrorKind::WouldBlock);
    assert!(failure.report.is_none());
}

#[test]
fn would_block_during_handoff_stops_with_both_copies_preserved() {
    for fail_at in [
        super::super::copy::Point::BeforeExchange,
        super::super::copy::Point::AfterExchange,
    ] {
        let (_root, source, previous, store) = fixture();
        store.with_store(|db| db.put(b"key", b"safe")).unwrap();
        let schedule = Schedule {
            previous: previous.clone(),
            interval: Duration::from_secs(1),
            minimum_log_bytes: 1,
            minimum_growth_bytes: 1,
            options: Default::default(),
        };
        let outcome = check_with(&store, &schedule, None, || {
            store.compact_reclaim_with_hooks(
                &previous,
                &schedule.options,
                |point| {
                    if point == fail_at {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "injected handoff failure",
                        ));
                    }
                    Ok(())
                },
                crate::directory::sync_existing,
                |_| Ok(()),
            )
        });
        let Outcome::Failed(failure) = outcome else {
            panic!("handoff failures must stop the worker")
        };
        assert_eq!(failure.stage, OnlineReclamationFailureStage::Compaction);
        assert_eq!(failure.error.kind(), io::ErrorKind::WouldBlock);
        assert!(failure.report.is_none());
        if fail_at == super::super::copy::Point::AfterExchange {
            assert!(store.with_store(|db| db.get(b"key")).is_err());
        } else {
            assert_eq!(
                store.with_store(|db| db.get(b"key")).unwrap(),
                Some(b"safe".to_vec())
            );
        }
        drop(store);
        for path in [&source, &previous] {
            assert_eq!(
                retry_lock(|| Store::open(path))
                    .unwrap()
                    .get(b"key")
                    .unwrap(),
                Some(b"safe".to_vec())
            );
        }
    }
}

#[test]
fn stop_waits_for_an_in_flight_check() {
    let (_root, _source, previous, store) = fixture();
    let worker = start(&store, &previous, Duration::from_secs(3600), 1, 1);
    let held = store.state.write().unwrap();
    worker.request_check().unwrap();
    wait_for(&worker, |s| s.in_flight);
    let (sender, receiver) = mpsc::channel();
    let stopper = thread::spawn(move || sender.send(worker.stop()).unwrap());
    assert!(matches!(
        receiver.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    drop(held);
    assert!(receiver.recv_timeout(WAIT).unwrap().unwrap().stopped);
    stopper.join().unwrap();
}

#[test]
fn cleanup_error_is_reported_as_a_completed_handoff_and_old_data_is_preserved() {
    let (root, source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"safe")).unwrap();
    let schedule = Schedule {
        previous: previous.clone(),
        interval: Duration::from_secs(1),
        minimum_log_bytes: 1,
        minimum_growth_bytes: 1,
        options: Default::default(),
    };
    let result = check_with(&store, &schedule, None, || {
        store.compact_reclaim_with_hooks(
            &previous,
            &schedule.options,
            |_| Ok(()),
            crate::directory::sync_existing,
            |point| {
                if point == super::super::reclaim::Point::BeforeMove {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "injected cleanup failure",
                    ));
                }
                Ok(())
            },
        )
    });
    let Outcome::Failed(failure) = result else {
        panic!("expected cleanup error");
    };
    assert_eq!(failure.stage, OnlineReclamationFailureStage::Reclamation);
    assert_eq!(failure.error.kind(), io::ErrorKind::WouldBlock);
    assert!(failure.report.is_some());
    assert!(!failure.reclaimed);
    assert!(
        failure
            .error
            .to_string()
            .contains("injected cleanup failure")
    );
    assert_eq!(
        retry_lock(|| Store::open(&previous))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"safe".to_vec())
    );
    assert_eq!(
        store.with_store(|db| db.get(b"key")).unwrap(),
        Some(b"safe".to_vec())
    );
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"safe".to_vec())
    );
    drop(root);
}

#[test]
fn worker_status_distinguishes_incomplete_cleanup_and_panics() {
    for panic_during_check in [false, true] {
        let (_root, _source, previous, store) = fixture();
        store.with_store(|db| db.put(b"key", b"safe")).unwrap();
        let control = Arc::new(Control::default());
        let thread_control = Arc::clone(&control);
        let thread_store = Arc::clone(&store);
        let schedule = Schedule {
            previous: previous.clone(),
            interval: Duration::from_secs(3600),
            minimum_log_bytes: 1,
            minimum_growth_bytes: 1,
            options: Default::default(),
        };
        let thread = thread::spawn(move || {
            guarded_run(&thread_control, || {
                run_with(
                    &thread_store,
                    &thread_control,
                    &schedule,
                    |store, schedule, baseline| {
                        if panic_during_check {
                            panic!("injected worker panic");
                        }
                        check_with(store, schedule, baseline, || {
                            store.compact_reclaim_with_hooks(
                                &schedule.previous,
                                &schedule.options,
                                |_| Ok(()),
                                crate::directory::sync_existing,
                                |point| {
                                    if point == super::super::reclaim::Point::BeforeMove {
                                        return Err(io::Error::other("injected cleanup failure"));
                                    }
                                    Ok(())
                                },
                            )
                        })
                    },
                );
            });
        });
        let worker = OnlineReclamationWorker {
            control,
            thread: Some(thread),
        };
        worker.request_check().unwrap();
        let status = wait_for(&worker, |status| status.stopped);
        assert!(!status.in_flight);
        assert_eq!(status.reclaimed, 0);
        if panic_during_check {
            assert_eq!(
                status.last_error.unwrap().stage,
                OnlineReclamationFailureStage::Panic
            );
            assert_eq!(status.handoffs, 0);
            assert!(status.last_report.is_none());
        } else {
            assert_eq!(
                status.last_error.unwrap().stage,
                OnlineReclamationFailureStage::Reclamation
            );
            assert_eq!(status.handoffs, 1);
            assert!(status.last_report.is_some());
            assert_eq!(
                retry_lock(|| Store::open(&previous))
                    .unwrap()
                    .get(b"key")
                    .unwrap(),
                Some(b"safe".to_vec())
            );
        }
        worker.stop().unwrap();
    }
}

#[test]
fn successful_cleanup_followed_by_observation_error_preserves_the_report() {
    let (_root, _source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"safe")).unwrap();
    let schedule = Schedule {
        previous: previous.clone(),
        interval: Duration::from_secs(1),
        minimum_log_bytes: 1,
        minimum_growth_bytes: 1,
        options: Default::default(),
    };
    let result = check_with(&store, &schedule, None, || {
        let report = store.compact_and_reclaim(&previous, &schedule.options)?;
        store.state.write().unwrap().failed = true;
        Ok(report)
    });
    let Outcome::Failed(failure) = result else {
        panic!("expected observation error");
    };
    assert_eq!(failure.stage, OnlineReclamationFailureStage::Observation);
    assert!(failure.reclaimed);
    assert!(failure.report.is_some());
    assert!(!previous.exists());
    store.state.write().unwrap().failed = false;
    assert_eq!(
        store.with_store(|db| db.get(b"key")).unwrap(),
        Some(b"safe".to_vec())
    );
}

#[test]
fn external_handoff_changes_the_growth_baseline_identity() {
    let (root, _source, previous, store) = fixture();
    store.with_store(|db| db.put(b"key", b"safe")).unwrap();
    let worker = start(&store, &previous, Duration::from_secs(3600), 1, 10_000);
    worker.request_check().unwrap();
    wait_for(&worker, |s| s.reclaimed >= 1);
    store
        .compact_and_replace(root.path().join("manual-previous"), &Default::default())
        .unwrap();
    worker.request_check().unwrap();
    let status = wait_for(&worker, |s| s.reclaimed >= 2 || s.stopped);
    assert!(status.last_error.is_none(), "{status:?}");
    assert_eq!(status.reclaimed, 2);
    worker.stop().unwrap();
    assert_eq!(
        store.with_store(|db| db.get(b"key")).unwrap(),
        Some(b"safe".to_vec())
    );
}

#[test]
fn concurrent_acknowledged_writes_survive_scheduled_reclamation() {
    let (_root, source, previous, store) = fixture();
    let worker = start(&store, &previous, Duration::from_millis(5), 1, 1);
    let start_barrier = Arc::new(Barrier::new(5));
    let mut writers = Vec::new();
    for _ in 0..4 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&start_barrier);
        writers.push(thread::spawn(move || {
            barrier.wait();
            (0..40)
                .map(|_| {
                    store
                        .with_store(|db| db.increment_i64(b"count", 1))
                        .unwrap()
                })
                .collect::<Vec<_>>()
        }));
    }
    start_barrier.wait();
    let mut values = writers
        .into_iter()
        .flat_map(|writer| writer.join().unwrap())
        .collect::<Vec<_>>();
    wait_for(&worker, |s| s.reclaimed >= 1 || s.stopped);
    let status = worker.stop().unwrap();
    assert!(status.last_error.is_none(), "{status:?}");
    assert!(status.reclaimed >= 1);
    values.sort_unstable();
    assert_eq!(values, (1..=160).collect::<Vec<_>>());
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"count")
            .unwrap(),
        Some(160_i64.to_le_bytes().to_vec())
    );
}
