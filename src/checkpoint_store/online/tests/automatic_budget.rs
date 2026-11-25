//! Automatic checkpoint rollover under a persistent disk budget. Byte
//! counts are logical file lengths, computed here from the file formats.
use super::*;
use std::collections::BTreeMap;

const SLOT_OWNER_BYTES: u64 = b"EMDA-AUTO-CHECKPOINT-SLOT-V1\n".len() as u64;
const SELECTION_FIXED_BYTES: u64 = 8 + 4 + 32 + 32;
const RECORD: u64 = crate::checkpoint_store::disk_budget::RECORD_BYTES as u64;

fn status(online: &OnlineCheckpointStore) -> crate::DiskBudgetStatus {
    online.disk_budget_status().unwrap().expect("budget")
}

fn record_bytes(key: &[u8], value: &[u8]) -> u64 {
    (crate::record::HEADER_SIZE + key.len() + value.len() + crate::record::CHECKSUM_SIZE) as u64
}

// Every file and directory under `root`, with file contents.
fn tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let mut all = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                all.insert(path.clone(), None);
                pending.push(path);
            } else {
                all.insert(path.clone(), Some(fs::read(&path).unwrap()));
            }
        }
    }
    all
}

fn slot_index(fixture: &Fixture, slot: usize) -> PathBuf {
    fs::canonicalize(&fixture.source)
        .unwrap()
        .join(".emda-auto-checkpoints-v1")
        .join(format!("slot-{slot}"))
        .join(crate::COMPACTED_CHECKPOINT_FILE)
}

fn selected(fixture: &Fixture) -> PathBuf {
    crate::checkpoint_store::selection::load(&fixture.source)
        .unwrap()
        .unwrap()
        .checkpoint
}

// Charged bytes measured from disk for an internally selected store.
fn measured(fixture: &Fixture) -> u64 {
    crate::staging::tree_bytes(&fixture.source).unwrap() + RECORD
}

// A budgeted automatic store whose suffix holds one key, so the second new
// key of each round rolls over.
fn budgeted_automatic() -> (Fixture, OnlineCheckpointStore, Model) {
    let (fixture, online, model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    online
        .with_store(CheckpointStore::enable_automatic_checkpoints)
        .unwrap();
    online
        .with_store(|store| {
            store
                .automatic_rollover(store.write()?)
                .map(drop)
                .map_err(|failure| failure.into_io_error())
        })
        .unwrap();
    drop(online);
    let store = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(&fixture.source, 1 << 20, 1, 1024, None)
    })
    .unwrap();
    (
        fixture,
        OnlineCheckpointStore::from_store(store).unwrap(),
        model,
    )
}

// The peak an automatic rollover before appending `key` must admit.
fn rollover_peak(
    fixture: &Fixture,
    online: &OnlineCheckpointStore,
    model: &Model,
    key: &[u8],
    value: &[u8],
) -> u64 {
    let current = selected(fixture);
    let next = slot_index(fixture, usize::from(current == slot_index(fixture, 0)));
    let key_bytes = model.keys().map(|key| key.len() as u64).sum();
    let checkpoint = crate::checkpoint::file_bytes(model.len() as u64, key_bytes).unwrap();
    let credit = crate::staging::tree_bytes(next.parent().unwrap()).unwrap();
    status(online).charged - credit
        + checkpoint
        + crate::staging::Stage::DIRECTORY_MARKER_BYTES
        + SLOT_OWNER_BYTES
        + crate::staging::Stage::FILE_MARKER_BYTES
        + SELECTION_FIXED_BYTES
        + next.as_os_str().len() as u64
        + record_bytes(key, value)
}

#[test]
fn rollover_peaks_are_exact_refused_before_any_write_and_reopen_measured() {
    let (fixture, online, mut model) = budgeted_automatic();
    for round in 0..3_u32 {
        online.set_disk_budget(Some(u64::MAX)).unwrap();
        let filler = format!("filler-{round}").into_bytes();
        write(&online, &mut model, &filler, b"f");
        let key = format!("rollover-{round}").into_bytes();
        let peak = rollover_peak(&fixture, &online, &model, &key, b"v");
        let before_selection = selected(&fixture);
        let charged = status(&online).charged;
        online.set_disk_budget(Some(peak - 1)).unwrap();
        let files = tree(fixture.root.path());
        let error = online
            .with_store(|store| store.put(&key, b"v"))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::StorageFull, "{error}");
        // Nothing was removed, staged, published or charged.
        assert_eq!(tree(fixture.root.path()), files, "round {round}");
        let after = status(&online);
        assert_eq!((after.charged, after.automatic_reserved), (charged, 0));
        assert_eq!(live(&online), model, "the store stays readable");
        online.set_disk_budget(Some(peak)).unwrap();
        write(&online, &mut model, &key, b"v");
        assert_ne!(selected(&fixture), before_selection);
        let after = status(&online);
        assert_eq!(after.automatic_reserved, 0);
        assert_eq!(after.charged, measured(&fixture), "round {round}");
        assert!(after.charged <= peak);
        assert_eq!(
            fs::metadata(selected(&fixture)).unwrap().len(),
            crate::checkpoint::file_bytes(
                model.len() as u64 - 1,
                model.keys().map(|k| k.len() as u64).sum::<u64>() - key.len() as u64
            )
            .unwrap()
        );
    }
    let charged = status(&online).charged;
    drop(online);
    let reopened = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert_eq!(status(&reopened).charged, charged);
    assert_eq!(live(&reopened), model);
}

#[test]
fn a_pinned_slot_is_never_reused_under_a_budget() {
    let (fixture, online, mut model) = budgeted_automatic();
    // Slot 0 is selected; protection is off by default.
    let snapshot = online.snapshot().unwrap();
    write(&online, &mut model, b"a", b"1");
    write(&online, &mut model, b"b", b"2"); // rolls over into slot 1
    assert_eq!(selected(&fixture), slot_index(&fixture, 1));
    // The next rollover needs slot 0, which the snapshot still reads.
    let files = tree(fixture.root.path());
    let error = online
        .with_store(|store| store.put(b"c", b"3"))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock, "{error}");
    assert_eq!(tree(fixture.root.path()), files);
    assert_eq!(status(&online).charged, measured(&fixture));
    assert_eq!(captured(&snapshot).get(&b"a"[..]), None);
    drop(snapshot);
    write(&online, &mut model, b"c", b"3");
    assert_eq!(selected(&fixture), slot_index(&fixture, 0));
    assert_eq!(status(&online).charged, measured(&fixture));
    assert_eq!(live(&online), model);
}

#[test]
fn a_deselected_external_checkpoint_stays_charged_while_a_snapshot_reads_it() {
    let (fixture, online, mut model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    online
        .with_store(CheckpointStore::enable_automatic_checkpoints)
        .unwrap();
    let external = selected(&fixture);
    assert!(!external.starts_with(fs::canonicalize(&fixture.source).unwrap()));
    let external_bytes = crate::staging::tree_bytes(&external).unwrap();
    let snapshot = online.snapshot().unwrap();
    online
        .with_store(|store| {
            store
                .automatic_rollover(store.write()?)
                .map(drop)
                .map_err(|failure| failure.into_io_error())
        })
        .unwrap();
    assert_eq!(selected(&fixture), slot_index(&fixture, 0));
    let held = status(&online);
    assert_eq!(held.pinned_bytes, external_bytes);
    assert_eq!(held.charged, measured(&fixture) + external_bytes);
    assert_eq!(captured(&snapshot), model);
    drop(snapshot);
    let released = status(&online);
    assert_eq!(
        (released.pinned_bytes, released.charged),
        (0, measured(&fixture))
    );
    write(&online, &mut model, b"after", b"release");
    assert_eq!(live(&online), model);
}

#[test]
fn enabling_without_headroom_is_refused_without_fencing_or_files() {
    let (fixture, online, mut model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    let charged = status(&online).charged;
    let step = record_bytes(b"k", b"v");
    online.set_disk_budget(Some(charged + step)).unwrap();
    let files = tree(fixture.root.path());
    let error = online
        .with_store(CheckpointStore::enable_automatic_checkpoints)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull, "{error}");
    assert_eq!(tree(fixture.root.path()), files);
    assert!(!crate::checkpoint_store::automatic::root(&fixture.source).exists());
    assert!(
        !online
            .with_store(CheckpointStore::automatic_checkpoints_enabled)
            .unwrap()
    );
    let after = status(&online);
    assert_eq!((after.charged, after.automatic_reserved), (charged, 0));
    // The healthy store keeps reading and appending within the old limit.
    assert_eq!(live(&online), model);
    write(&online, &mut model, b"k", b"v");
    assert_eq!(status(&online).charged, charged + step);
    // A predictably over-budget append is refused with nothing charged.
    let error = online
        .with_store(|store| store.put(b"x", b"y"))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull);
    assert_eq!(status(&online).charged, charged + step);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    online
        .with_store(CheckpointStore::enable_automatic_checkpoints)
        .unwrap();
    assert_eq!(status(&online).charged, measured_external(&fixture));
    drop(online);
    let reopened = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert_eq!(live(&reopened), model);
}

fn measured_external(fixture: &Fixture) -> u64 {
    measured(fixture) + crate::staging::tree_bytes(&selected(fixture)).unwrap()
}

// Bytes a configuration change reserves: a directory stage marker, a file
// stage marker and two copies of the longer root marker.
fn configuration_bytes() -> u64 {
    crate::staging::Stage::DIRECTORY_MARKER_BYTES
        + crate::staging::Stage::FILE_MARKER_BYTES
        + 2 * b"EMDA-AUTO-CHECKPOINT-ROOT-V1:OFF\n".len() as u64
}

fn stages(directory: &Path) -> Vec<String> {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with(".emda-stage-v1-"))
        .collect()
}

#[test]
fn a_killed_first_enable_stage_is_cleaned_before_the_budget_is_measured() {
    let (fixture, online, model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    let charged = status(&online).charged;
    drop(online);
    // What a first enable killed before publishing its root leaves: a
    // marked directory stage in the store directory holding the root marker.
    let mut stage = crate::staging::Stage::directory(&fixture.source).unwrap();
    fs::write(
        stage.path().join("owner"),
        b"EMDA-AUTO-CHECKPOINT-ROOT-V1:ON\n",
    )
    .unwrap();
    stage.preserve_payload_on_drop();
    drop(stage);
    assert_eq!(stages(&fixture.source).len(), 2, "payload and marker");
    let unknown = fixture.source.join("unknown-entry");
    fs::write(&unknown, b"kept").unwrap();
    let reopened = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    assert!(stages(&fixture.source).is_empty());
    assert_eq!(fs::read(&unknown).unwrap(), b"kept");
    let unknown_bytes = 4;
    assert_eq!(status(&reopened).charged, charged + unknown_bytes);
    // Exactly the configuration's reservation is enough to enable.
    reopened
        .set_disk_budget(Some(charged + unknown_bytes + configuration_bytes() - 1))
        .unwrap();
    let error = reopened
        .with_store(CheckpointStore::enable_automatic_checkpoints)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull, "{error}");
    reopened
        .set_disk_budget(Some(charged + unknown_bytes + configuration_bytes()))
        .unwrap();
    reopened
        .with_store(CheckpointStore::enable_automatic_checkpoints)
        .unwrap();
    assert_eq!(status(&reopened).charged, measured_external(&fixture));
    assert_eq!(live(&reopened), model);
}

#[test]
fn automatic_and_online_reservations_share_one_headroom() {
    let (fixture, online, _model) = fixture(false);
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    let charged = status(&online).charged;
    online.set_disk_budget(Some(charged + 1000)).unwrap();
    let budget = online
        .with_store(CheckpointStore::disk_budget)
        .unwrap()
        .unwrap();
    let automatic = budget.reserve_automatic(600, 0, None).unwrap();
    assert_eq!(status(&online).automatic_reserved, 600);
    // An online replacement cannot spend the automatic share.
    let refused = budget
        .reserve_compaction(&fixture.previous, 401)
        .unwrap_err();
    assert_eq!(refused.kind(), io::ErrorKind::StorageFull);
    budget.reserve_compaction(&fixture.previous, 400).unwrap();
    drop(automatic);
    // And an automatic rollover cannot spend the online reservation.
    let refused = budget.reserve_automatic(601, 0, None).err().unwrap();
    assert_eq!(refused.kind(), io::ErrorKind::StorageFull);
    drop(budget.reserve_automatic(600, 0, None).unwrap());
    assert_eq!(status(&online).automatic_reserved, 0);
    assert!(budget.reconcile().unwrap());
}

#[test]
fn a_quota_installed_on_an_automatic_store_survives_compaction_rollover_and_reopen() {
    let (fixture, online, mut model) = fixture(true);
    let overlay = online
        .with_store(|store| Ok(store.read()?.view.overlay.len()))
        .unwrap();
    drop(online);
    // A suffix key limit just above the fixture's, so later writes roll over.
    let keys = overlay + 4;
    let store = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(
            &fixture.source,
            1 << 20,
            keys,
            1 << 16,
            None,
        )
    })
    .unwrap();
    let online = OnlineCheckpointStore::from_store(store).unwrap();
    // Automatic first, then the quota: the slots root and the external
    // selection are measured.
    assert!(
        online
            .with_store(CheckpointStore::automatic_checkpoints_enabled)
            .unwrap()
    );
    online.set_disk_budget(Some(u64::MAX)).unwrap();
    assert_eq!(status(&online).charged, measured_external(&fixture));
    // Online compaction and reclamation under the quota.
    online
        .compact_and_reclaim(&fixture.previous, &Default::default())
        .unwrap();
    assert!(!fixture.previous.exists());
    assert!(selected(&fixture).starts_with(fs::canonicalize(&fixture.source).unwrap()));
    let after = status(&online);
    assert_eq!(after.pending, crate::DiskBudgetPending::None);
    assert_eq!(after.charged, measured(&fixture));
    assert!(
        online
            .with_store(CheckpointStore::automatic_checkpoints_enabled)
            .unwrap()
    );
    // Fill the exchanged generation's empty suffix to its key limit.
    for index in 0..keys {
        write(
            &online,
            &mut model,
            format!("fill-{index:03}").as_bytes(),
            b"f",
        );
    }
    let compacted = selected(&fixture);
    // The next new key rolls over into slot 0 under an exact cap.
    let peak = rollover_peak(&fixture, &online, &model, b"trigger", b"v");
    online.set_disk_budget(Some(peak - 1)).unwrap();
    let charged = status(&online).charged;
    let files = tree(fixture.root.path());
    let error = online
        .with_store(|store| store.put(b"trigger", b"v"))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::StorageFull, "{error}");
    assert_eq!(tree(fixture.root.path()), files);
    assert_eq!(status(&online).charged, charged);
    online.set_disk_budget(Some(peak)).unwrap();
    write(&online, &mut model, b"trigger", b"v");
    assert_eq!(selected(&fixture), slot_index(&fixture, 0));
    assert_ne!(selected(&fixture), compacted);
    let rolled = status(&online);
    assert_eq!(rolled.charged, measured(&fixture));
    assert!(rolled.charged <= peak);
    drop(online);
    // The limit, the charge and the exact data persist.
    let reopened = retry_lock(|| OnlineCheckpointStore::open_managed(&fixture.source)).unwrap();
    let persisted = status(&reopened);
    assert_eq!((persisted.limit, persisted.charged), (peak, rolled.charged));
    assert_eq!(live(&reopened), model);
    assert!(
        reopened
            .with_store(CheckpointStore::automatic_checkpoints_enabled)
            .unwrap()
    );
}
