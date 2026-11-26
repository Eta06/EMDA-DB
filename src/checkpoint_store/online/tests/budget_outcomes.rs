//! Classified outcomes of disk-budget record publication: install, limit
//! change and removal.
use super::*;
use crate::PublicationFailure;
use crate::checkpoint_store::disk_budget::{BUDGET_FILE, FAIL_RECORD_DIRECTORY_SYNC, read};

fn with_failed_sync<T>(action: impl FnOnce() -> T) -> T {
    FAIL_RECORD_DIRECTORY_SYNC.set(true);
    let result = action();
    FAIL_RECORD_DIRECTORY_SYNC.set(false);
    result
}

fn charged(online: &OnlineCheckpointStore) -> u64 {
    online.disk_budget_status().unwrap().unwrap().charged
}

fn would_block(failure: &PublicationFailure) -> bool {
    matches!(failure, PublicationFailure::NotPublished(error)
        if error.kind() == io::ErrorKind::WouldBlock)
}

#[test]
fn refusals_before_publication_leave_the_record_and_the_handle_healthy() {
    let (fixture, online, mut model) = fixture(false);
    // An install below the projected charge publishes nothing.
    let failure = online.set_disk_budget_detailed(Some(1)).unwrap_err();
    assert!(would_block(&failure), "{failure:?}");
    assert!(read(&fixture.source).unwrap().is_none());
    write(&online, &mut model, b"healthy", b"after install refusal");
    online.set_disk_budget_detailed(Some(u64::MAX)).unwrap();
    let limit = charged(&online) + 4096;
    online.set_disk_budget_detailed(Some(limit)).unwrap();
    // A limit below the charge keeps the old limit.
    let failure = online.set_disk_budget_detailed(Some(1)).unwrap_err();
    assert!(would_block(&failure), "{failure:?}");
    assert_eq!(read(&fixture.source).unwrap().unwrap().limit, limit);
    // A pinned retired generation blocks removal before the record changes.
    let snapshot = online.snapshot().unwrap();
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    let failure = online.set_disk_budget_detailed(None).unwrap_err();
    assert!(would_block(&failure), "{failure:?}");
    assert!(read(&fixture.source).unwrap().is_some());
    drop(snapshot);
    online.set_disk_budget_detailed(None).unwrap();
    assert!(read(&fixture.source).unwrap().is_none());
    assert!(online.disk_budget_status().unwrap().is_none());
    write(&online, &mut model, b"healthy", b"after removal");
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn an_uncertain_install_or_limit_change_fences_until_reopen_loads_the_record() {
    let (fixture, online, model) = fixture(false);
    let failure = with_failed_sync(|| online.set_disk_budget_detailed(Some(u64::MAX))).unwrap_err();
    assert!(
        matches!(failure, PublicationFailure::OutcomeUnknown(_)),
        "{failure:?}"
    );
    // The rename happened, so a reopen loads the record; this handle is fenced.
    assert!(online.with_store(|store| store.put(b"x", b"y")).is_err());
    drop(online);
    let online = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert_eq!(live(&online), model);
    let limit = charged(&online) + 8192;
    let failure = with_failed_sync(|| online.set_disk_budget_detailed(Some(limit))).unwrap_err();
    assert!(
        matches!(failure, PublicationFailure::OutcomeUnknown(_)),
        "{failure:?}"
    );
    // Either limit may be durable, so the handle admits nothing more.
    assert!(online.with_store(|store| store.put(b"x", b"y")).is_err());
    drop(online);
    let online = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert_eq!(online.disk_budget_status().unwrap().unwrap().limit, limit);
    assert_eq!(live(&online), model);
}

#[test]
fn an_uncertain_removal_keeps_enforcing_the_budget_until_reopen() {
    let (fixture, online, mut model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    let limit = charged(&online) + 64;
    online.set_disk_budget(Some(limit)).unwrap();
    let failure = with_failed_sync(|| online.set_disk_budget_detailed(None)).unwrap_err();
    assert!(
        matches!(failure, PublicationFailure::OutcomeUnknown(_)),
        "{failure:?}"
    );
    // The record is unlinked, but this handle still enforces the old limit.
    assert!(!fixture.source.join(BUDGET_FILE).exists());
    assert_eq!(online.disk_budget_status().unwrap().unwrap().limit, limit);
    let large = vec![b'v'; 128];
    let error = online
        .with_store(|store| store.put(b"large", &large))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull);
    // A reopen finds no record and no budget.
    drop(online);
    let online = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert!(online.disk_budget_status().unwrap().is_none());
    write(&online, &mut model, b"large", &large);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}
