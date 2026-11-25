//! The maintenance-wide source read ceiling of one online checkpoint
//! handoff: the initial pass, copied values, catch-up and final replay
//! ranges with their value rereads, final comparison source values and the
//! final source confirmation. Logical bytes only.
use super::*;
use crate::DiskBudgetPending;

fn values(model: &Model) -> u64 {
    model.values().map(|value| value.len() as u64).sum()
}

fn log_len(fixture: &Fixture) -> u64 {
    fs::metadata(fixture.source.join("records.log"))
        .unwrap()
        .len()
}

fn state(fixture: &Fixture) -> (Vec<String>, Vec<u8>, Vec<u8>, Vec<u8>) {
    (
        parent_entries(fixture),
        fs::read(fixture.source.join("records.log")).unwrap(),
        fs::read(fixture.source.join("commit.state")).unwrap(),
        fs::read(
            fixture
                .source
                .join(super::super::super::disk_budget::BUDGET_FILE),
        )
        .unwrap(),
    )
}

fn is_limit(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidInput
        && error.to_string() == "online maintenance source read byte limit exceeded"
}

#[test]
fn refusals_before_publication_leave_source_quota_and_namespace_unchanged() {
    for (label, margin) in [("initial pass", 0), ("copied values", 1)] {
        let (fixture, online, model) = fixture(false);
        online.set_disk_budget(Some(u64::MAX)).unwrap();
        // One short of the initial pass, or of the pass plus every value.
        let limit = log_len(&fixture) + margin * values(&model) - 1;
        online
            .set_maintenance_source_read_limit(Some(limit))
            .unwrap();
        let before = state(&fixture);
        let charged = online.disk_budget_status().unwrap();
        let result = online.compact_and_reclaim_detailed(&fixture.previous, &Default::default());
        assert!(
            matches!(&result, Err(OnlineReclaimFailure::Compaction(
                OnlineHandoffFailure::NotPublished(error))) if is_limit(error)),
            "{label}: {result:?}"
        );
        assert_eq!(state(&fixture), before, "{label}");
        assert_eq!(online.disk_budget_status().unwrap(), charged, "{label}");
        // Clearing the limit restores the default behavior.
        online.set_maintenance_source_read_limit(None).unwrap();
        online
            .compact_and_reclaim(&fixture.previous, &Default::default())
            .unwrap();
        assert_eq!(live(&online), model, "{label}");
    }
}

const WRITES: usize = 5;

// Runs a handoff with writes caught up during the copy. Returns the result,
// the expected total read charge and the model of acknowledged values.
fn concurrent_handoff(
    limit: Option<u64>,
) -> (
    Fixture,
    OnlineCheckpointStore,
    Result<OnlineCompactionReport, OnlineReclaimFailure>,
    u64,
    Model,
) {
    let (fixture, online, mut model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    online.set_maintenance_source_read_limit(limit).unwrap();
    let initial = log_len(&fixture);
    let copied = values(&model);
    let mut written = 0_u64;
    let mut final_boundary = 0_u64;
    let result = online.compact_reclaim_detailed_with_hooks(
        &fixture.previous,
        &Default::default(),
        (
            |point| {
                if point == Point::InitialCopy {
                    for index in 0..WRITES {
                        let key = format!("concurrent-{index}").into_bytes();
                        let value = vec![b'v'; 32];
                        online.with_store(|store| store.put(&key, &value))?;
                        written += value.len() as u64;
                        model.insert(key, value);
                    }
                }
                if point == Point::BeforeFinalBarrier {
                    // After catch-up; the final replay must read this one.
                    let value = vec![b'f'; 32];
                    online.with_store(|store| store.put(b"final-write", &value))?;
                    written += value.len() as u64;
                    model.insert(b"final-write".to_vec(), value);
                    final_boundary = log_len(&fixture);
                }
                Ok(())
            },
            crate::directory::sync_existing,
        ),
        |_| Ok(()),
    );
    // Initial pass + copied values + the catch-up and final replay ranges,
    // which together cover the log after the capture + the values reread in
    // both phases + the final source confirmation + final comparison values.
    let total =
        initial + copied + (final_boundary - initial) + written + final_boundary + values(&model);
    (fixture, online, result, total, model)
}

#[test]
fn a_concurrent_handoff_is_charged_exactly_and_refused_one_short() {
    let (_, _, unlimited, total, _) = concurrent_handoff(None);
    unlimited.unwrap();
    let (fixture, online, exact, exact_total, model) = concurrent_handoff(Some(total));
    assert_eq!(exact_total, total, "fixtures are deterministic");
    let report = exact.unwrap();
    // Both replay call sites read and were charged.
    assert!(report.concurrent_replay_bytes > 0, "{report:?}");
    assert!(report.final_replay_bytes > 0, "{report:?}");
    assert_eq!(live(&online), model);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);

    // The last charge is a final-comparison source value under the barrier,
    // after the candidate was published: the outcome is classified, the
    // source stays live with every acknowledged write, and the candidate and
    // its reservation remain for reconciliation.
    let (fixture, online, short, _, model) = concurrent_handoff(Some(total - 1));
    assert!(
        matches!(&short, Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::CandidateOutcomeUnknown(error))) if is_limit(error)),
        "{short:?}"
    );
    assert_eq!(live(&online), model);
    assert!(fixture.previous.exists());
    assert_eq!(
        online.disk_budget_status().unwrap().unwrap().pending,
        DiskBudgetPending::Compaction
    );
    fs::remove_dir_all(&fixture.previous).unwrap();
    crate::cleanup_abandoned_staging(fixture.root.path()).unwrap();
    online.reconcile_disk_budget().unwrap();
    online.set_maintenance_source_read_limit(None).unwrap();
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn a_scheduled_handoff_stops_the_worker_on_the_ceiling() {
    let (fixture, online, model) = fixture(false);
    online.set_maintenance_source_read_limit(Some(1)).unwrap();
    let online = Arc::new(online);
    let worker = online
        .start_reclamation_worker(
            &fixture.previous,
            Duration::from_millis(5),
            1,
            1,
            Default::default(),
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !worker.status().stopped {
        assert!(Instant::now() < deadline, "worker did not stop");
        thread::sleep(Duration::from_millis(2));
    }
    let status = worker.stop().unwrap();
    let error = status.last_error.expect("stopped on the ceiling");
    assert_eq!(
        error.stage,
        crate::OnlineReclamationFailureStage::Compaction
    );
    assert!(
        error.message.contains("source read byte limit"),
        "{error:?}"
    );
    assert_eq!(status.handoffs, 0);
    assert!(!fixture.previous.exists());
    assert_eq!(live(&online), model);
}
