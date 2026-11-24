//! Disk-budget admission for managed online checkpoint maintenance. Byte
//! counts are logical file lengths; these are not filesystem-allocation or
//! physical power-loss tests.
use super::*;
use crate::{DiskBudgetPending, OnlineReclaimFailure};

fn record_bytes(key: &[u8], value: &[u8]) -> u64 {
    (crate::record::HEADER_SIZE + key.len() + value.len() + crate::record::CHECKSUM_SIZE) as u64
}

fn status(online: &OnlineCheckpointStore) -> crate::DiskBudgetStatus {
    online
        .disk_budget_status()
        .unwrap()
        .expect("budget installed")
}

fn budgeted(limit: u64) -> (Fixture, OnlineCheckpointStore, Model) {
    let (fixture, online, model) = fixture(false);
    online.set_disk_budget(Some(limit)).unwrap();
    (fixture, online, model)
}

// Logical bytes of everything the store owns or may still own.
fn observed(fixture: &Fixture) -> u64 {
    let parent = fixture.root.path();
    let mut total = crate::staging::tree_bytes(&fixture.source).unwrap()
        + crate::staging::tree_bytes(&fixture.previous).unwrap();
    for entry in fs::read_dir(parent).unwrap() {
        let name = entry.unwrap().file_name();
        if name.to_string_lossy().starts_with(".emda-stage-v1-") {
            total += crate::staging::tree_bytes(&parent.join(name)).unwrap();
        }
    }
    total + crate::staging::tree_bytes(&parent.join("managed")).unwrap()
}

fn is_full(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::StorageFull
}

#[test]
fn exact_limit_admits_and_the_next_byte_is_refused_across_reopen() {
    let (fixture, online, mut model) = budgeted(u64::MAX);
    let charged = status(&online).charged;
    assert_eq!(status(&online).live_bytes, charged);
    assert_eq!(charged, observed(&fixture) + 4096, "spare record length");
    let step = record_bytes(b"k", b"v");
    online.set_disk_budget(Some(charged + step)).unwrap();
    write(&online, &mut model, b"k", b"v");
    assert_eq!(status(&online).charged, charged + step);
    let error = online
        .with_store(|store| store.put(b"x", b"y"))
        .unwrap_err();
    assert!(is_full(&error), "{error}");
    assert_eq!(live(&online), model);
    // A lower limit than the charge is refused and leaves the old one.
    let error = online.set_disk_budget(Some(charged)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    drop(online);
    let reopened = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    let after = status(&reopened);
    assert_eq!(
        (after.limit, after.charged),
        (charged + step, charged + step)
    );
    assert!(is_full(
        &reopened
            .with_store(|store| store.put(b"x", b"y"))
            .unwrap_err()
    ));
    // The plain checkpoint-store entry point enforces the same record.
    drop(reopened);
    let plain = retry_lock(|| CheckpointStore::open_managed(&fixture.source)).unwrap();
    assert!(is_full(&plain.put(b"x", b"y").unwrap_err()));
    drop(plain);
    let reopened = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    reopened.set_disk_budget(Some(charged + 2 * step)).unwrap();
    write(&reopened, &mut model, b"x", b"y");
    assert_eq!(live(&reopened), model);
}

#[test]
fn unbudgeted_writers_and_unreserved_allocations_are_refused() {
    let (fixture, online, model) = budgeted(u64::MAX);
    let unsupported = |error: io::Error| assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    unsupported(
        online
            .with_store(|store| store.rollover_managed(fixture.root.path().join("other")))
            .unwrap_err(),
    );
    unsupported(
        online
            .with_store(CheckpointStore::enable_automatic_checkpoints)
            .unwrap_err(),
    );
    unsupported(
        online
            .compact_and_replace(&fixture.previous, &Default::default())
            .unwrap_err(),
    );
    assert!(!fixture.previous.exists());
    drop(online);
    // Ordinary opens and managed rebuilds check the record under the lock.
    unsupported(retry_lock(|| Store::open(&fixture.source)).err().unwrap());
    let rebuilt = CheckpointStore::rebuild_managed_selection(
        &fixture.source,
        fixture.root.path().join("rebuilt"),
    );
    unsupported(rebuilt.unwrap_err());
    let blocking = retry_lock(|| CheckpointStore::open_managed(&fixture.source)).unwrap();
    unsupported(
        blocking
            .compact_and_replace(&fixture.previous)
            .err()
            .unwrap(),
    );
    let reopened = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert_eq!(live(&reopened), model);
}

#[test]
fn over_limit_compaction_is_refused_before_anything_is_published() {
    let (fixture, online, model) = budgeted(u64::MAX);
    let charged = status(&online).charged;
    online.set_disk_budget(Some(charged + 1024)).unwrap();
    let result = online.compact_and_reclaim_detailed(&fixture.previous, &Default::default());
    assert!(
        matches!(
            &result,
            Err(OnlineReclaimFailure::Compaction(OnlineHandoffFailure::NotPublished(error)))
                if is_full(error)
        ),
        "{result:?}"
    );
    assert!(!fixture.previous.exists());
    let after = status(&online);
    assert_eq!(
        (after.charged, after.pending),
        (charged, DiskBudgetPending::None)
    );
    assert_eq!(parent_entries(&fixture), ["base", "live", "managed"]);
    assert_eq!(live(&online), model);
}

#[test]
fn peak_charge_bounds_files_through_compaction_retirement_and_release() {
    let (fixture, online, mut model) = budgeted(u64::MAX);
    let start = status(&online).charged;
    let snapshot = online.snapshot().unwrap();
    let before = model.clone();
    let mut samples = Vec::new();
    let mut sample = |phase: &str, online: &OnlineCheckpointStore| {
        let budget = online
            .with_store(|store| store.disk_budget_status())
            .unwrap()
            .unwrap();
        let files = observed(&fixture);
        assert!(budget.charged >= files, "{phase}: {budget:?} < {files}");
        samples.push((phase.to_string(), budget.charged, files));
    };
    let options = OnlineCheckpointCompactionOptions {
        max_catchup_bytes: 1 << 20,
        ..Default::default()
    };
    let report = online
        .compact_reclaim_detailed_with_hooks(
            &fixture.previous,
            &options,
            (
                |point| {
                    if matches!(point, Point::InitialCopy | Point::BeforeFinalBarrier) {
                        let key = format!("during-{point:?}").into_bytes();
                        online.with_store(|store| store.put(&key, b"value"))?;
                        model.insert(key, b"value".to_vec());
                        sample(&format!("{point:?}"), &online);
                    }
                    Ok(())
                },
                crate::directory::sync_existing,
            ),
            |point| {
                if point == RetirePoint::AfterMoveParentSync {
                    // Writers are excluded here; read the shared accounting.
                    let budget = online
                        .state
                        .read()
                        .unwrap()
                        .store
                        .disk_budget()
                        .unwrap()
                        .unwrap()
                        .status()
                        .unwrap();
                    assert!(budget.charged >= observed(&fixture));
                    assert_eq!(budget.pending, DiskBudgetPending::Retirement);
                }
                Ok(())
            },
        )
        .unwrap();
    assert!(report.compacted_log_bytes < report.final_log_bytes);
    let pinned = status(&online);
    assert_eq!(pinned.pending, DiskBudgetPending::None);
    assert_eq!(pinned.maintenance_reserved, 0);
    assert!(pinned.pinned_bytes > 0, "{pinned:?}");
    // The view still reads the unlinked generation, which stays charged.
    assert_eq!(captured(&snapshot), before);
    drop(snapshot);
    let released = status(&online);
    assert_eq!(released.pinned_bytes, 0);
    assert_eq!(released.charged, released.live_bytes);
    assert_eq!(
        released.charged,
        observed(&fixture)
            - crate::staging::tree_bytes(&fixture.root.path().join("managed")).unwrap()
            + 4096
    );
    assert!(released.charged < start, "{start} -> {released:?}");
    assert_eq!(samples.len(), 2, "{samples:?}");
    write(&online, &mut model, b"after", b"release");
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn concurrent_appends_are_admitted_against_the_reservation() {
    let (fixture, online, mut model) = budgeted(u64::MAX);
    let step = record_bytes(b"c", b"v");
    let mut outcomes = Vec::new();
    let options = OnlineCheckpointCompactionOptions {
        max_catchup_bytes: 4 * step,
        ..Default::default()
    };
    let result = online.compact_reclaim_detailed_with_hooks(
        &fixture.previous,
        &options,
        (
            |point| {
                if point == Point::InitialCopy {
                    // Leave room for exactly one more append.
                    let charged = online
                        .with_store(|store| store.disk_budget_status())?
                        .unwrap()
                        .charged;
                    let budget = online.state.read().unwrap().store.disk_budget()?.unwrap();
                    budget.set_limit(charged + step)?;
                    outcomes.push(online.with_store(|store| store.put(b"c", b"v")).is_ok());
                    let second = online.with_store(|store| store.put(b"d", b"v"));
                    outcomes.push(second.is_err_and(|error| is_full(&error)));
                }
                Ok(())
            },
            crate::directory::sync_existing,
        ),
        |_| Ok(()),
    );
    // The reserved replacement and the admitted append fit; retirement
    // then frees the previous generation.
    result.unwrap();
    assert_eq!(outcomes, [true, true]);
    model.insert(b"c".to_vec(), b"v".to_vec());
    assert_eq!(live(&online), model);
    let after = status(&online);
    assert!(after.charged <= after.limit);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn interrupted_compaction_keeps_its_reservation_until_reconciled() {
    let (fixture, online, mut model) = budgeted(u64::MAX);
    let start = status(&online).charged;
    let result = online.compact_reclaim_detailed_with_hooks(
        &fixture.previous,
        &Default::default(),
        (
            |point| {
                if point == Point::InitialCopy {
                    Err(io::Error::other("injected"))
                } else {
                    Ok(())
                }
            },
            crate::directory::sync_existing,
        ),
        |_| Ok(()),
    );
    assert!(matches!(
        result,
        Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::CandidateOutcomeUnknown(_)
        ))
    ));
    let held = status(&online);
    assert_eq!(held.pending, DiskBudgetPending::Compaction);
    assert!(held.maintenance_reserved > 0);
    assert_eq!(held.charged, start + held.maintenance_reserved);
    assert!(held.charged >= observed(&fixture) + 4096);
    drop(online);
    // The durable record keeps the charge after reopen.
    let online = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert_eq!(status(&online), held);
    let kept = online.reconcile_disk_budget().unwrap().unwrap();
    assert_eq!(kept.pending, DiskBudgetPending::Compaction);
    fs::remove_dir_all(&fixture.previous).unwrap();
    let released = online.reconcile_disk_budget().unwrap().unwrap();
    assert_eq!(released.pending, DiskBudgetPending::None);
    assert_eq!(released.charged, start);
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    write(&online, &mut model, b"after", b"retry");
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn failed_retirement_stays_charged_until_scoped_cleanup() {
    let (fixture, online, model) = budgeted(u64::MAX);
    let result = online.compact_reclaim_detailed_with_hooks(
        &fixture.previous,
        &Default::default(),
        (|_| Ok(()), crate::directory::sync_existing),
        |point| {
            if point == RetirePoint::AfterCheckpointFiles {
                Err(io::Error::other("injected"))
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(
        result,
        Err(OnlineReclaimFailure::Reclamation { .. })
    ));
    let held = status(&online);
    assert_eq!(held.pending, DiskBudgetPending::Retirement);
    assert!(held.retired_bytes > 0);
    assert!(held.charged >= observed(&fixture));
    drop(online);
    let online = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert_eq!(status(&online).retired_bytes, held.retired_bytes);
    online.cleanup_retired_staging_detailed().unwrap();
    let released = status(&online);
    assert_eq!(
        (released.pending, released.retired_bytes),
        (DiskBudgetPending::None, 0)
    );
    assert_eq!(released.charged, released.live_bytes);
    assert_eq!(live(&online), model);
}

const BUDGET_KILL_POINT: &str = "EMDA_ONLINE_BUDGET_KILL_POINT";

#[test]
fn online_budget_kill_child() {
    let Ok(selected) = std::env::var(BUDGET_KILL_POINT) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT).unwrap());
    let online = retry_lock(|| OnlineCheckpointStore::open_managed(root.join("live"))).unwrap();
    let pause = |name: String| {
        if name == selected {
            fs::write(root.join("signal"), b"").unwrap();
            thread::sleep(Duration::from_secs(60));
        }
    };
    let _ = online.compact_reclaim_detailed_with_hooks(
        &root.join("previous"),
        &Default::default(),
        (
            |point| {
                pause(format!("{point:?}"));
                Ok(())
            },
            crate::directory::sync_existing,
        ),
        |point| {
            pause(format!("{point:?}"));
            Ok(())
        },
    );
    panic!("kill point {selected} was not reached");
}

#[test]
fn process_kill_keeps_charges_attributable_after_reopen() {
    for (point, pending) in [
        ("InitialCopy", DiskBudgetPending::Compaction),
        ("BeforeExchange", DiskBudgetPending::Compaction),
        ("AfterExchange", DiskBudgetPending::Retirement),
        ("AfterMove", DiskBudgetPending::Retirement),
        ("AfterRecords", DiskBudgetPending::Retirement),
        ("AfterParentSync", DiskBudgetPending::Retirement),
    ] {
        let (fixture, online, model) = budgeted(u64::MAX);
        drop(online);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("checkpoint_store::online::tests::budget::online_budget_kill_child")
            .env(BUDGET_KILL_POINT, point)
            .env(CHILD_ROOT, fixture.root.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        while !fixture.root.path().join("signal").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before {point}: {status}");
            }
            assert!(Instant::now() < deadline, "child did not reach {point}");
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        fs::remove_file(fixture.root.path().join("signal")).unwrap();
        let online = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
        assert_eq!(live(&online), model, "{point}");
        let held = status(&online);
        assert_eq!(held.pending, pending, "{point}");
        // Everything still on disk for this store is charged.
        assert!(
            held.charged
                >= observed(&fixture)
                    - crate::staging::tree_bytes(&fixture.root.path().join("managed")).unwrap(),
            "{point}: {held:?}"
        );
        // Reconcile only after the leftovers are gone.
        if fixture.previous.exists() {
            assert_eq!(
                online.reconcile_disk_budget().unwrap().unwrap().pending,
                pending
            );
            fs::remove_dir_all(&fixture.previous).unwrap();
        }
        online.cleanup_retired_staging_detailed().unwrap();
        crate::cleanup_abandoned_staging(fixture.root.path()).unwrap();
        let released = online.reconcile_disk_budget().unwrap().unwrap();
        assert_eq!(released.pending, DiskBudgetPending::None, "{point}");
        assert_eq!(released.charged, released.live_bytes, "{point}");
        assert_eq!(
            parent_entries(&fixture),
            ["base", "live", "managed"],
            "{point}"
        );
        drop(online);
        assert_eq!(reopened(&fixture.source), model, "{point}");
    }
}

#[test]
fn retired_generation_charge_outlives_its_descriptors() {
    let (fixture, online, _model) = budgeted(u64::MAX);
    let retired = online.state.read().unwrap().store.generation_token();
    let snapshot = online.snapshot().unwrap();
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    // Only the owned snapshot keeps the reclaimed generation alive.
    assert!(retired.upgrade().is_some());
    let pinned = status(&online).pinned_bytes;
    assert!(pinned > 0);
    // An online snapshot's cancel drops its captured view and then its store
    // reference, the last owner of the reclaimed generation.
    let mut snapshot = snapshot;
    snapshot.cancel();
    assert!(retired.upgrade().is_none());
    assert_eq!(status(&online).pinned_bytes, 0);
    // The token is the store's last field and the snapshot releases its view
    // before its store reference, so a dead token means the generation's
    // log, checkpoint and lock descriptors were already dropped.
    drop(snapshot);
}
