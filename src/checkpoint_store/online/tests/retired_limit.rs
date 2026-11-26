//! A per-handle limit on retired log history that owned snapshots pin across
//! online checkpoint handoffs. Byte counts are logical log lengths.
use super::*;
use crate::{OnlineHandoffFailure, OnlineReclaimFailure};

fn log_len(fixture: &Fixture) -> u64 {
    fs::metadata(fixture.source.join("records.log"))
        .unwrap()
        .len()
}

fn is_retired_limit(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        && error
            .get_ref()
            .is_some_and(|cause| cause.is::<super::super::RetiredSnapshotLimit>())
}

#[test]
fn a_held_snapshot_is_admitted_exactly_and_refused_before_any_write() {
    let (fixture, online, mut model) = fixture(false);
    let captured_model = model.clone();
    let snapshot = online.snapshot().unwrap();
    let pinned = log_len(&fixture);
    online
        .set_retired_snapshot_log_limit(Some(pinned - 1))
        .unwrap();
    let before = parent_entries(&fixture);
    let log = fs::read(fixture.source.join("records.log")).unwrap();
    let result = online.compact_and_reclaim_detailed(&fixture.previous, &Default::default());
    assert!(
        matches!(&result, Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::NotPublished(error))) if is_retired_limit(error)),
        "{result:?}"
    );
    // Nothing was read into a candidate, reserved or published.
    assert_eq!(parent_entries(&fixture), before);
    assert_eq!(fs::read(fixture.source.join("records.log")).unwrap(), log);
    assert_eq!(online.retired_snapshot_log_bytes().unwrap(), 0);
    // The exact limit admits the handoff; the snapshot keeps its view.
    online.set_retired_snapshot_log_limit(Some(pinned)).unwrap();
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    assert_eq!(online.retired_snapshot_log_bytes().unwrap(), pinned);
    write(&online, &mut model, b"after", b"handoff");
    assert_eq!(captured(&snapshot), captured_model);
    // A limit below the pinned history is refused and leaves the old one.
    let error = online
        .set_retired_snapshot_log_limit(Some(pinned - 1))
        .unwrap_err();
    assert!(is_retired_limit(&error), "{error}");
    // Releasing the last view releases its history.
    drop(snapshot);
    assert_eq!(online.retired_snapshot_log_bytes().unwrap(), 0);
    online.set_retired_snapshot_log_limit(Some(0)).unwrap();
    // With nothing pinned a zero limit still admits handoffs.
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    assert_eq!(live(&online), model);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn pinned_generations_accumulate_across_handoffs_and_release_independently() {
    let (fixture, online, mut model) = fixture(false);
    let first = online.snapshot().unwrap();
    let first_bytes = log_len(&fixture);
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    write(&online, &mut model, b"second", b"generation");
    let second_model = model.clone();
    let second = online.snapshot().unwrap();
    let second_bytes = log_len(&fixture);
    // Both generations would be pinned after the next handoff.
    online
        .set_retired_snapshot_log_limit(Some(first_bytes + second_bytes - 1))
        .unwrap();
    let error = online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap_err();
    assert!(
        matches!(&error, crate::OnlineReclaimError::Compaction(error) if is_retired_limit(error)),
        "{error}"
    );
    online
        .set_retired_snapshot_log_limit(Some(first_bytes + second_bytes))
        .unwrap();
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    assert_eq!(
        online.retired_snapshot_log_bytes().unwrap(),
        first_bytes + second_bytes
    );
    // Each generation is released by its own last view.
    drop(first);
    assert_eq!(online.retired_snapshot_log_bytes().unwrap(), second_bytes);
    assert_eq!(captured(&second), second_model);
    drop(second);
    assert_eq!(online.retired_snapshot_log_bytes().unwrap(), 0);
    // The scheduled-worker deferral sees the registry empty again.
    assert!(!online.scheduled_deferral_locked().unwrap());
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn a_snapshot_taken_during_the_copy_is_refused_under_the_final_barrier() {
    let (fixture, online, mut model) = fixture(false);
    online.set_retired_snapshot_log_limit(Some(0)).unwrap();
    let mut during = None;
    let result = online.compact_reclaim_detailed_with_hooks(
        &fixture.previous,
        &Default::default(),
        (
            |point| {
                if point == Point::InitialCopy {
                    // Concurrent readers and writers keep using the source.
                    during = Some(online.snapshot()?);
                    online.with_store(|store| store.put(b"during", b"copy"))?;
                }
                Ok(())
            },
            crate::directory::sync_existing,
        ),
        |_| Ok(()),
    );
    model.insert(b"during".to_vec(), b"copy".to_vec());
    assert!(
        matches!(&result, Err(OnlineReclaimFailure::Compaction(
            OnlineHandoffFailure::CandidateOutcomeUnknown(error))) if is_retired_limit(error)),
        "{result:?}"
    );
    // The source stayed live with every acknowledged write; the unselected
    // candidate remains for cleanup, and the view is unaffected.
    assert_eq!(live(&online), model);
    assert!(fixture.previous.exists());
    let snapshot = during.take().unwrap();
    assert_eq!(captured(&snapshot).get(&b"during"[..]), None);
    drop(snapshot);
    // Reopen after the interrupted handoff, clean up, and hand off again.
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
    fs::remove_dir_all(&fixture.previous).unwrap();
    crate::cleanup_abandoned_staging(fixture.root.path()).unwrap();
    let online = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    online.set_retired_snapshot_log_limit(Some(0)).unwrap();
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    assert!(!fixture.previous.exists());
    assert_eq!(live(&online), model);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn readers_and_writers_continue_while_a_pinned_handoff_runs() {
    let (fixture, online, mut model) = fixture(false);
    let online = Arc::new(online);
    let captured_model = model.clone();
    let snapshot = online.snapshot().unwrap();
    online
        .set_retired_snapshot_log_limit(Some(u64::MAX))
        .unwrap();
    let stop = std::sync::atomic::AtomicBool::new(false);
    let written = std::thread::scope(|scope| {
        // The pinned view is read throughout the handoff.
        let reader = scope.spawn(|| {
            let mut reads = 0_u32;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                assert_eq!(captured(&snapshot), captured_model);
                reads += 1;
            }
            reads
        });
        let writer = scope.spawn(|| {
            let mut written = Vec::new();
            for index in 0..32_u32 {
                let key = format!("concurrent-{index:02}").into_bytes();
                online.with_store(|store| store.put(&key, b"w")).unwrap();
                written.push(key);
            }
            written
        });
        online
            .compact_and_reclaim(&fixture.previous, &Default::default())
            .unwrap();
        let written = writer.join().unwrap();
        stop.store(true, std::sync::atomic::Ordering::Release);
        assert!(reader.join().unwrap() > 0);
        written
    });
    for key in written {
        model.insert(key, b"w".to_vec());
    }
    assert!(online.retired_snapshot_log_bytes().unwrap() > 0);
    assert_eq!(live(&online), model);
    assert_eq!(captured(&snapshot), captured_model);
    drop(snapshot);
    assert_eq!(online.retired_snapshot_log_bytes().unwrap(), 0);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}
