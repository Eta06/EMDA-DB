//! Scheduled online checkpoint reclamation. These run on a live OS; they are
//! not physical power-loss tests.
use super::super::tests::{
    Fixture, Model, captured, fixture, live, parent_entries, reopened, write,
};
use super::*;
use crate::OnlineReclamationWorkerStatus;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

const INTERVAL: Duration = Duration::from_millis(5);

fn wait_for(
    worker: &OnlineReclamationWorker,
    what: &str,
    done: impl Fn(&OnlineReclamationWorkerStatus, &OnlineReclamationWorker) -> bool,
) -> OnlineReclamationWorkerStatus {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = worker.status();
        if done(&status, worker) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {status:?}"
        );
        thread::sleep(Duration::from_millis(2));
    }
}

fn start(
    online: &Arc<OnlineCheckpointStore>,
    fixture: &Fixture,
    growth: u64,
) -> OnlineReclamationWorker {
    online
        .start_reclamation_worker(&fixture.previous, INTERVAL, 1, growth, Default::default())
        .unwrap()
}

#[test]
fn scheduled_handoff_keeps_callbacks_snapshots_and_acknowledged_data() {
    for budgeted in [false, true] {
        let (fixture, online, mut model) = fixture(false);
        if budgeted {
            online.set_disk_budget(Some(u64::MAX)).unwrap();
        }
        let online = Arc::new(online);
        let before = model.clone();
        let snapshot = online.snapshot().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (online, stop) = (Arc::clone(&online), Arc::clone(&stop));
            thread::spawn(move || {
                let mut written = Model::new();
                let mut index = 0_u32;
                while !stop.load(Ordering::Acquire) || index < 20 {
                    let key = format!("scheduled-{}", index % 256).into_bytes();
                    let value = index.to_le_bytes().to_vec();
                    online.with_store(|store| store.put(&key, &value)).unwrap();
                    written.insert(key, value);
                    index += 1;
                }
                written
            })
        };
        let worker = start(&online, &fixture, u64::MAX);
        let status = wait_for(&worker, "a scheduled handoff", |status, _| {
            status.reclaimed >= 1
        });
        assert!(status.last_error.is_none(), "{status:?}");
        stop.store(true, Ordering::Release);
        model.extend(writer.join().unwrap());
        let status = worker.stop().unwrap();
        assert_eq!((status.handoffs, status.reclaimed), (1, 1), "{status:?}");
        assert!(status.stopped && status.last_error.is_none());
        assert_eq!(captured(&snapshot), before);
        assert_eq!(live(&online), model);
        assert!(!fixture.previous.exists());
        assert_eq!(parent_entries(&fixture), ["base", "live", "managed"]);
        drop(snapshot);
        if budgeted {
            let budget = online.disk_budget_status().unwrap().unwrap();
            assert_eq!(
                (budget.pinned_bytes, budget.charged),
                (0, budget.live_bytes)
            );
        }
        drop(online);
        assert_eq!(reopened(&fixture.source), model, "budgeted {budgeted}");
    }
}

#[test]
fn growth_hysteresis_coalesces_requests_and_one_worker_per_handle() {
    let (fixture, online, mut model) = fixture(false);
    let online = Arc::new(online);
    let growth = 4096;
    let worker = start(&online, &fixture, growth);
    let error = online
        .start_reclamation_worker(&fixture.previous, INTERVAL, 1, 1, Default::default())
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    wait_for(&worker, "the first handoff", |status, _| {
        status.handoffs == 1
    });
    // Without growth, repeated and coalesced requests only check.
    let checks = worker.status().checks;
    for _ in 0..5 {
        worker.request_check().unwrap();
    }
    wait_for(&worker, "unchanged checks", |status, _| {
        status.checks >= checks + 3
    });
    assert_eq!(worker.status().handoffs, 1);
    let mut index = 0;
    // Grow the log by more than the threshold, then a request compacts again.
    let before = online.with_store(|store| store.log.len()).unwrap();
    while online.with_store(|store| store.log.len()).unwrap() < before + growth {
        write(
            &online,
            &mut model,
            format!("grow-{index}").as_bytes(),
            &[7; 64],
        );
        index += 1;
    }
    worker.request_check().unwrap();
    let status = wait_for(&worker, "the growth handoff", |status, _| {
        status.handoffs == 2
    });
    assert!(status.last_error.is_none(), "{status:?}");
    worker.stop().unwrap();
    // The lease is released: a new worker can start.
    start(&online, &fixture, growth).stop().unwrap();
    assert_eq!(live(&online), model);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn defers_while_a_replaced_generation_is_still_held_by_a_snapshot() {
    let (fixture, online, mut model) = fixture(false);
    let online = Arc::new(online);
    let worker = start(&online, &fixture, 1);
    wait_for(&worker, "the first handoff", |status, _| {
        status.handoffs == 1
    });
    worker.stop().unwrap();
    let snapshot = online.snapshot().unwrap();
    // Replace the snapshot's generation by hand; it stays pinned.
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    let worker = start(&online, &fixture, 1);
    write(&online, &mut model, b"growth", b"while pinned");
    worker.request_check().unwrap();
    wait_for(&worker, "a pinned deferral", |_, worker| {
        worker.deferred_checks() >= 2
    });
    assert_eq!(worker.status().handoffs, 0);
    drop(snapshot);
    worker.request_check().unwrap();
    let status = wait_for(&worker, "the handoff after release", |status, _| {
        status.handoffs == 1
    });
    assert!(status.last_error.is_none(), "{status:?}");
    worker.stop().unwrap();
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn defers_when_the_disk_budget_cannot_fit_a_replacement() {
    let (fixture, online, mut model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    let charged = online.disk_budget_status().unwrap().unwrap().charged;
    online.set_disk_budget(Some(charged + 4096)).unwrap();
    let online = Arc::new(online);
    let worker = start(&online, &fixture, 1);
    wait_for(&worker, "budget deferrals", |_, worker| {
        worker.deferred_checks() >= 3
    });
    let status = worker.status();
    assert!(
        status.handoffs == 0 && status.last_error.is_none(),
        "{status:?}"
    );
    assert!(!fixture.previous.exists());
    assert_eq!(parent_entries(&fixture), ["base", "live", "managed"]);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    worker.request_check().unwrap();
    wait_for(
        &worker,
        "the handoff after raising the limit",
        |status, _| status.handoffs == 1,
    );
    worker.stop().unwrap();
    write(&online, &mut model, b"after", b"budget");
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn stops_without_deleting_an_interrupted_candidate() {
    let (fixture, online, model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    let online = Arc::new(online);
    // Waits a day unless asked, so the interruption happens between checks.
    let worker = online
        .start_reclamation_worker(&fixture.previous, MAX_INTERVAL, 1, 1, Default::default())
        .unwrap();
    let interrupted = online.compact_reclaim_detailed_with_hooks(
        &fixture.previous,
        &Default::default(),
        (
            |point| {
                if point == super::super::Point::InitialCopy {
                    Err(io::Error::other("injected"))
                } else {
                    Ok(())
                }
            },
            crate::directory::sync_existing,
        ),
        |_| Ok(()),
    );
    assert!(interrupted.is_err());
    let candidate = crate::staging::tree_bytes(&fixture.previous).unwrap();
    assert!(candidate > 0);
    worker.request_check().unwrap();
    let status = wait_for(&worker, "the worker to stop", |status, _| status.stopped);
    let error = status.last_error.expect("stopped with an error");
    assert_eq!(error.stage, OnlineReclamationFailureStage::Compaction);
    assert!(error.message.contains("previous-store path"), "{error:?}");
    assert_eq!(
        crate::staging::tree_bytes(&fixture.previous).unwrap(),
        candidate
    );
    assert_eq!(
        online.disk_budget_status().unwrap().unwrap().pending,
        DiskBudgetPending::Compaction
    );
    drop(worker);
    // A new worker refuses to start over the leftover.
    let refused = online
        .start_reclamation_worker(&fixture.previous, INTERVAL, 1, 1, Default::default())
        .err()
        .unwrap();
    assert_eq!(refused.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(live(&online), model);
}

#[test]
fn uncertain_publication_and_reclamation_failures_stop_the_check() {
    let (fixture, online, _model) = fixture(false);
    let schedule = Schedule {
        previous: fixture.previous.clone(),
        interval: INTERVAL,
        minimum_log_bytes: 1,
        minimum_growth_bytes: 1,
        options: Default::default(),
    };
    let uncertain = check(&online, &schedule, None, |_, _| {
        Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::ExchangeOutcomeUnknown(io::Error::other("injected exchange")),
        ))
    });
    let Outcome::Failed(failure) = uncertain else {
        panic!("uncertain exchange did not stop the worker");
    };
    assert_eq!(failure.stage, OnlineReclamationFailureStage::Compaction);
    assert!(failure.error.to_string().contains("reconcile both paths"));
    let full = check(&online, &schedule, None, |_, _| {
        Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::NotPublished(crate::checkpoint_store::disk_budget::exceeded()),
        ))
    });
    assert!(matches!(full, Outcome::Deferred));
    let busy = check(&online, &schedule, None, |_, _| {
        Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::NotPublished(io::Error::new(
                io::ErrorKind::WouldBlock,
                super::super::MaintenanceBusy,
            )),
        ))
    });
    assert!(matches!(busy, Outcome::MaintenanceBusy));
}

#[test]
fn a_paused_manual_reservation_is_never_reconciled_by_the_worker() {
    use std::sync::mpsc::channel;
    let (fixture, online, model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    let online = Arc::new(online);
    let start_charge = online.disk_budget_status().unwrap().unwrap().charged;
    let (reserved_tx, reserved_rx) = channel();
    let (release_tx, release_rx) = channel::<()>();
    let manual = {
        let (online, previous) = (Arc::clone(&online), fixture.previous.clone());
        thread::spawn(move || {
            online.compact_reclaim_detailed_with_hooks(
                &previous,
                &Default::default(),
                (
                    |point| {
                        if point == super::super::Point::AfterReservation {
                            // The reservation is durable; nothing is allocated yet.
                            reserved_tx.send(()).unwrap();
                            release_rx.recv().unwrap();
                        }
                        Ok(())
                    },
                    crate::directory::sync_existing,
                ),
                |_| Ok(()),
            )
        })
    };
    reserved_rx.recv().unwrap();
    let held = online.disk_budget_status().unwrap().unwrap();
    assert_eq!(held.pending, DiskBudgetPending::Compaction);
    assert!(held.maintenance_reserved > 0);
    assert!(!fixture.previous.exists(), "no candidate exists yet");
    // The scheduled path, a worker check and public reconciliation all wait
    // for the manual operation's lease instead of clearing its reservation.
    let scheduled = online.compact_reclaim_scheduled(
        &fixture.previous,
        &Default::default(),
        (|_| Ok(()), crate::directory::sync_existing),
    );
    assert!(matches!(
        &scheduled,
        Err(OnlineReclaimFailure::Compaction(OnlineHandoffFailure::NotPublished(error)))
            if error.get_ref().is_some_and(|inner| inner.is::<super::super::MaintenanceBusy>())
    ));
    let worker = online
        .start_reclamation_worker(&fixture.previous, MAX_INTERVAL, 1, 1, Default::default())
        .unwrap();
    worker.request_check().unwrap();
    wait_for(&worker, "a busy check", |_, worker| {
        worker.busy_checks() >= 1
    });
    assert_eq!(
        online.reconcile_disk_budget().err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(online.disk_budget_status().unwrap().unwrap(), held);
    worker.stop().unwrap();
    release_tx.send(()).unwrap();
    manual.join().unwrap().unwrap();
    let after = online.disk_budget_status().unwrap().unwrap();
    assert_eq!(after.pending, DiskBudgetPending::None);
    assert!(after.charged < start_charge);
    assert_eq!(live(&online), model);
}

#[test]
fn a_manual_handoff_between_preflight_and_compaction_defers_under_the_lease() {
    let (fixture, online, model) = fixture(false);
    let online = Arc::new(online);
    let schedule = Schedule {
        previous: fixture.previous.clone(),
        interval: INTERVAL,
        minimum_log_bytes: 1,
        minimum_growth_bytes: 1,
        options: Default::default(),
    };
    let snapshot = online.snapshot().unwrap();
    let mut manual = None;
    // The worker's preflight sees nothing retired; a manual handoff then
    // replaces the snapshot's generation before the scheduled compaction.
    let outcome = check(&online, &schedule, None, |store, schedule| {
        manual = Some(
            store
                .compact_and_reclaim(&schedule.previous, &Default::default())
                .unwrap(),
        );
        store.compact_scheduled(schedule)
    });
    assert!(manual.is_some());
    assert!(matches!(outcome, Outcome::Deferred));
    assert!(!fixture.previous.exists());
    // Exactly one retired generation is held, by the snapshot.
    assert_eq!(online.retired.lock().unwrap().len(), 1);
    drop(snapshot);
    let outcome = check(&online, &schedule, None, |store, schedule| {
        store.compact_scheduled(schedule)
    });
    assert!(matches!(outcome, Outcome::Reclaimed(..)));
    assert_eq!(live(&online), model);
}
