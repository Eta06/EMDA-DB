//! Online checkpoint compaction and reclamation tests. These run on a
//! running OS with in-process hooks and process kills. They are not
//! physical power-loss tests.
use super::*;
use crate::test_support::retry_lock;
use crate::{OnlineHandoffFailure, Store};
use std::collections::BTreeMap;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

struct Fixture {
    root: tempfile::TempDir,
    source: PathBuf,
    previous: PathBuf,
}

// A managed checkpoint store with history: overwrites and deletes in the
// suffix, optionally with automatic checkpoints in owned slots.
fn fixture(automatic: bool) -> (Fixture, OnlineCheckpointStore, Model) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let previous = root.path().join("previous");
    let base = root.path().join("base");
    let ordinary = Store::open(&source).unwrap();
    let mut model = Model::new();
    for index in 0..64_u32 {
        let key = format!("key-{index:03}").into_bytes();
        ordinary.put(&key, b"base").unwrap();
        model.insert(key, b"base".to_vec());
    }
    let digest = ordinary.checkpoint_to(&base).unwrap();
    drop(ordinary);
    let store = retry_lock(|| CheckpointStore::open(&source, &base, &digest)).unwrap();
    store.rollover_managed(root.path().join("managed")).unwrap();
    if automatic {
        store.enable_automatic_checkpoints().unwrap();
    }
    for round in 0..3_u32 {
        for index in (0..64_u32).step_by(3) {
            let key = format!("key-{index:03}").into_bytes();
            let value = format!("round-{round}").into_bytes();
            store.put(&key, &value).unwrap();
            model.insert(key, value);
        }
    }
    for index in (1..64_u32).step_by(7) {
        let key = format!("key-{index:03}").into_bytes();
        store.delete(&key).unwrap();
        model.remove(&key);
    }
    let online = OnlineCheckpointStore::from_store(store).unwrap();
    (
        Fixture {
            root,
            source,
            previous,
        },
        online,
        model,
    )
}

fn scan_all(read: impl Fn(Option<&[u8]>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>>) -> Model {
    let mut all = Model::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page = read(after.as_deref()).unwrap();
        let Some((last, _)) = page.last() else {
            return all;
        };
        after = Some(last.clone());
        all.extend(page);
    }
}

fn live(online: &OnlineCheckpointStore) -> Model {
    scan_all(|after| online.with_store(|store| store.scan(after, None, MAX_SCAN_ENTRIES)))
}

fn captured(snapshot: &OnlineCheckpointSnapshot) -> Model {
    scan_all(|after| {
        snapshot
            .as_snapshot()
            .unwrap()
            .scan(after, None, MAX_SCAN_ENTRIES)
    })
}

fn reopened(path: &Path) -> Model {
    let store = retry_lock(|| CheckpointStore::open_managed(path)).unwrap();
    scan_all(|after| store.scan(after, None, MAX_SCAN_ENTRIES))
}

fn write(online: &OnlineCheckpointStore, model: &mut Model, key: &[u8], value: &[u8]) {
    online.with_store(|store| store.put(key, value)).unwrap();
    model.insert(key.to_vec(), value.to_vec());
}

fn parent_entries(fixture: &Fixture) -> Vec<String> {
    let mut names = fs::read_dir(fixture.root.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
fn handoff_catches_up_concurrent_writes_and_keeps_open_snapshots() {
    for automatic in [false, true] {
        let (fixture, online, mut model) = fixture(automatic);
        let before = model.clone();
        let snapshot = online.snapshot().unwrap();
        let online = Arc::new(online);
        let stop = Arc::new(AtomicBool::new(false));
        // An independent writer and an atomic counter run throughout.
        let writer = {
            let (online, stop) = (Arc::clone(&online), Arc::clone(&stop));
            thread::spawn(move || {
                let mut written = Model::new();
                let mut increments = 0_i64;
                let mut index = 0_u32;
                while !stop.load(Ordering::Acquire) || index < 50 {
                    let key = format!("concurrent-{index:05}").into_bytes();
                    let value = format!("value-{index}").into_bytes();
                    online.with_store(|store| store.put(&key, &value)).unwrap();
                    written.insert(key, value);
                    online
                        .with_store(|store| store.increment_i64(b"counter", 1))
                        .unwrap();
                    increments += 1;
                    index += 1;
                }
                (written, increments)
            })
        };
        let mut hooked = Model::new();
        let report = online
            .compact_internal(
                &fixture.previous,
                &OnlineCheckpointCompactionOptions::default(),
                (
                    |point| {
                        let key = format!("hook-{point:?}").into_bytes();
                        if matches!(point, Point::InitialCopy | Point::BeforeFinalBarrier) {
                            online.with_store(|store| store.put(&key, b"hooked"))?;
                            hooked.insert(key, b"hooked".to_vec());
                        }
                        Ok(())
                    },
                    crate::directory::sync_existing,
                ),
                None::<fn(RetirePoint) -> io::Result<()>>,
                None,
            )
            .unwrap()
            .0;
        stop.store(true, Ordering::Release);
        let (written, increments) = writer.join().unwrap();
        model.extend(hooked);
        model.extend(written);
        model.insert(b"counter".to_vec(), increments.to_le_bytes().to_vec());
        assert!(report.concurrent_replay_bytes > 0, "{report:?}");
        assert!(
            report.compacted_log_bytes < report.final_log_bytes,
            "{report:?}"
        );
        assert_eq!(live(&online), model);
        assert_eq!(captured(&snapshot), before);
        // Further writes go to the replacement.
        write(&online, &mut model, b"after", b"handoff");
        drop(snapshot);
        drop(online);
        assert_eq!(reopened(&fixture.source), model);
        let reopened_store = retry_lock(|| CheckpointStore::open_managed(&fixture.source)).unwrap();
        assert_eq!(reopened_store.read().unwrap().automatic, automatic);
        assert_eq!(
            reopened_store
                .read()
                .unwrap()
                .selection
                .as_ref()
                .unwrap()
                .checkpoint,
            fs::canonicalize(&fixture.source)
                .unwrap()
                .join(crate::COMPACTED_CHECKPOINT_FILE)
        );
        drop(reopened_store);
        // The previous generation is complete up to its final boundary.
        let previous = retry_lock(|| Store::open(&fixture.previous)).unwrap();
        assert_eq!(previous.get(b"after").unwrap(), None);
        assert_eq!(
            previous.get(b"hook-InitialCopy").unwrap(),
            Some(b"hooked".to_vec())
        );
    }
}

#[test]
fn reclamation_releases_the_retired_generation_after_snapshots_drop() {
    for automatic in [false, true] {
        let (fixture, online, mut model) = fixture(automatic);
        let before = model.clone();
        let snapshot = online.snapshot().unwrap();
        let budget = online
            .with_store(|store| store.snapshot_budget_status())
            .unwrap();
        assert_eq!(budget.active, 1);
        write(&online, &mut model, b"new", b"value");
        online
            .compact_and_reclaim_detailed(
                &fixture.previous,
                &OnlineCheckpointCompactionOptions::default(),
            )
            .unwrap();
        assert!(!fixture.previous.exists());
        assert_eq!(parent_entries(&fixture), ["base", "live", "managed"]);
        // The retired generation is unlinked but still readable by its view,
        // and its reservation is shared with the replacement.
        assert_eq!(captured(&snapshot), before);
        let budget = online
            .with_store(|store| store.snapshot_budget_status())
            .unwrap();
        assert_eq!(budget.active, 1);
        drop(snapshot);
        let budget = online
            .with_store(|store| store.snapshot_budget_status())
            .unwrap();
        assert_eq!(budget.active, 0);
        assert_eq!(live(&online), model);
        // A second generation compacts the first replacement.
        write(&online, &mut model, b"second", b"generation");
        online
            .compact_and_reclaim(&fixture.previous, &Default::default())
            .unwrap();
        assert_eq!(parent_entries(&fixture), ["base", "live", "managed"]);
        drop(online);
        assert_eq!(reopened(&fixture.source), model);
    }
}

#[test]
fn failures_before_exchange_leave_the_source_live() {
    for point in [
        Point::InitialCopy,
        Point::BeforeFinalBarrier,
        Point::SelectionBeforeRename,
        Point::SelectionBeforeSync,
        Point::BeforeExchange,
    ] {
        let (fixture, online, mut model) = fixture(true);
        let result = crate::publication_outcome::track_online_handoff(|tracker| {
            online.compact_internal(
                &fixture.previous,
                &Default::default(),
                (
                    |reached| {
                        if reached == point {
                            Err(io::Error::other("injected"))
                        } else {
                            Ok(())
                        }
                    },
                    crate::directory::sync_existing,
                ),
                None::<fn(RetirePoint) -> io::Result<()>>,
                Some(tracker),
            )
        });
        assert!(
            matches!(
                result,
                Err(OnlineHandoffFailure::CandidateOutcomeUnknown(_))
            ),
            "{point:?}"
        );
        // The handle stays usable on the unchanged source.
        write(&online, &mut model, b"after-failure", b"accepted");
        assert_eq!(live(&online), model, "{point:?}");
        // Reconcile by removing the unselected candidate, then retry.
        fs::remove_dir_all(&fixture.previous).unwrap();
        online
            .compact_and_reclaim(&fixture.previous, &Default::default())
            .unwrap();
        drop(online);
        assert_eq!(reopened(&fixture.source), model, "{point:?}");
    }
}

#[test]
fn rejected_attempts_do_not_publish() {
    let (fixture, online, model) = fixture(false);
    fs::create_dir(&fixture.previous).unwrap();
    let result = online.compact_and_replace_detailed(&fixture.previous, &Default::default());
    assert!(matches!(result, Err(OnlineHandoffFailure::NotPublished(_))));
    fs::remove_dir(&fixture.previous).unwrap();
    let tight = OnlineCheckpointCompactionOptions {
        copy_limits: CheckpointCompactionLimits {
            max_source_log_read_bytes: Some(1),
            max_target_log_bytes: None,
        },
        ..Default::default()
    };
    let result = online.compact_and_replace_detailed(&fixture.previous, &tight);
    assert!(matches!(result, Err(OnlineHandoffFailure::NotPublished(_))));
    assert!(!fixture.previous.exists());
    assert_eq!(live(&online), model);
}

#[test]
fn final_delta_limit_rejects_handoff_before_exchange() {
    let (fixture, online, mut model) = fixture(false);
    let options = OnlineCheckpointCompactionOptions {
        catchup_rounds: 0,
        max_final_delta_bytes: 0,
        ..Default::default()
    };
    let mut writes = Model::new();
    let result = crate::publication_outcome::track_online_handoff(|tracker| {
        online.compact_internal(
            &fixture.previous,
            &options,
            (
                |point| {
                    if point == Point::InitialCopy {
                        online.with_store(|store| store.put(b"late", b"write"))?;
                        writes.insert(b"late".to_vec(), b"write".to_vec());
                    }
                    Ok(())
                },
                crate::directory::sync_existing,
            ),
            None::<fn(RetirePoint) -> io::Result<()>>,
            Some(tracker),
        )
    });
    assert!(matches!(
        result,
        Err(OnlineHandoffFailure::CandidateOutcomeUnknown(_))
    ));
    model.extend(writes);
    assert_eq!(live(&online), model);
}

#[test]
fn failure_after_exchange_poisons_the_handle_and_reopens_complete() {
    let (fixture, online, model) = fixture(true);
    let result = crate::publication_outcome::track_online_handoff(|tracker| {
        online.compact_internal(
            &fixture.previous,
            &Default::default(),
            (
                |_| Ok(()),
                |_: &Path| Err(io::Error::other("injected sync")),
            ),
            None::<fn(RetirePoint) -> io::Result<()>>,
            Some(tracker),
        )
    });
    assert!(matches!(
        result,
        Err(OnlineHandoffFailure::ExchangeOutcomeUnknown(_))
    ));
    assert!(online.with_store(|_| Ok(())).is_err());
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
    assert_eq!(
        scan_all(|after| {
            retry_lock(|| Store::open(&fixture.previous))?.scan(after, None, MAX_SCAN_ENTRIES)
        }),
        model
    );
}

#[test]
fn retirement_failures_keep_the_replacement_and_scoped_cleanup_finishes() {
    for automatic in [false, true] {
        for point in [
            RetirePoint::BeforeMove,
            RetirePoint::AfterMove,
            RetirePoint::AfterMoveParentSync,
            RetirePoint::AfterAutomaticRoot,
            RetirePoint::AfterCheckpointFiles,
            RetirePoint::AfterRecords,
            RetirePoint::AfterLock,
            RetirePoint::AfterRetiredRemoval,
        ] {
            let (fixture, online, mut model) = fixture(automatic);
            let result = online.compact_reclaim_detailed_with_hooks(
                &fixture.previous,
                &Default::default(),
                (|_| Ok(()), crate::directory::sync_existing),
                |reached| {
                    if reached == point {
                        Err(io::Error::other("injected"))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(
                matches!(result, Err(OnlineReclaimFailure::Reclamation { .. })),
                "{automatic} {point:?}"
            );
            write(&online, &mut model, b"after", b"reclamation failure");
            if point == RetirePoint::BeforeMove {
                assert!(fixture.previous.exists());
                fs::remove_dir_all(&fixture.previous).unwrap();
            } else {
                assert!(!fixture.previous.exists());
                online.cleanup_retired_staging_detailed().unwrap();
            }
            assert_eq!(
                parent_entries(&fixture),
                ["base", "live", "managed"],
                "{automatic} {point:?}"
            );
            assert_eq!(live(&online), model);
            drop(online);
            assert_eq!(reopened(&fixture.source), model, "{automatic} {point:?}");
        }
    }
}

#[test]
fn scoped_cleanup_resumes_partial_automatic_root_and_rejects_unknown_entries() {
    let (fixture, online, model) = fixture(true);
    let result = online.compact_reclaim_detailed_with_hooks(
        &fixture.previous,
        &Default::default(),
        (|_| Ok(()), crate::directory::sync_existing),
        |reached| {
            if reached == RetirePoint::AfterMoveParentSync {
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
    let stage = fs::read_dir(fixture.root.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("retired").is_dir())
        .expect("retained retired stage");
    let automatic_root = automatic::root(&stage.join("retired"));
    // Simulate an interruption midway through automatic-root removal.
    let slot = automatic_root.join("slot-0");
    if slot.exists() {
        fs::remove_dir_all(&slot).unwrap();
    }
    fs::remove_file(automatic_root.join("owner")).unwrap();
    // An unexpected entry keeps the stage preserved.
    fs::write(automatic_root.join("unexpected"), b"keep").unwrap();
    let cleanup = online.cleanup_retired_staging_detailed().unwrap();
    assert_eq!(cleanup.removed_payloads, 0);
    assert!(automatic_root.join("unexpected").exists());
    fs::remove_file(automatic_root.join("unexpected")).unwrap();
    let cleanup = online.cleanup_retired_staging_detailed().unwrap();
    assert_eq!(cleanup.removed_payloads, 1);
    assert_eq!(parent_entries(&fixture), ["base", "live", "managed"]);
    assert_eq!(live(&online), model);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

const CHILD_POINT: &str = "EMDA_ONLINE_CHECKPOINT_KILL_POINT";
const CHILD_ROOT: &str = "EMDA_ONLINE_CHECKPOINT_KILL_ROOT";

#[test]
fn online_checkpoint_kill_child() {
    let Ok(selected) = std::env::var(CHILD_POINT) else {
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
fn process_kill_at_handoff_and_retirement_boundaries_preserves_acknowledged_data() {
    for point in [
        "InitialCopy",
        "BeforeExchange",
        "AfterExchange",
        "BeforeMove",
        "AfterMove",
        "AfterMoveParentSync",
        "AfterAutomaticRoot",
        "AfterCheckpointFiles",
        "AfterRecords",
        "AfterLock",
        "AfterRetiredRemoval",
        "AfterParentSync",
    ] {
        let (fixture, online, model) = fixture(true);
        drop(online);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("checkpoint_store::online::tests::online_checkpoint_kill_child")
            .env(CHILD_POINT, point)
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
        // Every acknowledged value is recoverable at the original path.
        let online = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
        assert_eq!(live(&online), model, "{point}");
        if matches!(point, "InitialCopy" | "BeforeExchange") {
            // An unselected candidate remains; the source never changed.
            fs::remove_dir_all(&fixture.previous).unwrap();
        } else if point == "AfterExchange" || point == "BeforeMove" {
            // The complete previous generation remains at its path.
            let previous = retry_lock(|| Store::open(&fixture.previous)).unwrap();
            assert_eq!(
                scan_all(|after| previous.scan(after, None, MAX_SCAN_ENTRIES)),
                model
            );
            drop(previous);
            fs::remove_dir_all(&fixture.previous).unwrap();
            // A kill before the move can leave an empty owned stage.
            online.cleanup_retired_staging_detailed().unwrap();
        } else {
            assert!(!fixture.previous.exists(), "{point}");
            online.cleanup_retired_staging_detailed().unwrap();
        }
        assert_eq!(
            parent_entries(&fixture),
            ["base", "live", "managed"],
            "{point}"
        );
        online
            .compact_and_reclaim(&fixture.previous, &Default::default())
            .unwrap();
        drop(online);
        assert_eq!(reopened(&fixture.source), model, "{point}");
    }
}

mod budget;
