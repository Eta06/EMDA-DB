//! Public checks for bounded offline managed-checkpoint rebuild.
use database_engine::{
    BatchOperation, CheckpointStore, ManagedRebuildFailure, Store, cleanup_abandoned_staging,
};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::thread;
use std::time::Duration;

fn retry<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let mut last = None;
    for _ in 0..100 {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                last = Some(error);
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(error),
        }
    }
    Err(last.expect("retry observed a lock conflict"))
}

fn witness_bytes(path: &Path) -> Vec<u8> {
    let log = fs::read(path.join("records.log")).unwrap();
    if log.starts_with(b"EMDALOG3") {
        let file = File::open(path.join("records.log")).unwrap();
        let mut buffer = vec![0_u8; 128];
        let count = rustix::fs::fgetxattr(&file, "user.db.commit.v3", &mut buffer).unwrap();
        buffer[..count].to_vec()
    } else {
        fs::read(path.join("commit.state")).unwrap()
    }
}

fn restore_witness(path: &Path, bytes: &[u8]) {
    let log = fs::read(path.join("records.log")).unwrap();
    if log.starts_with(b"EMDALOG3") {
        let file = OpenOptions::new()
            .write(true)
            .open(path.join("records.log"))
            .unwrap();
        rustix::fs::fsetxattr(
            &file,
            "user.db.commit.v3",
            bytes,
            rustix::fs::XattrFlags::REPLACE,
        )
        .unwrap();
        file.sync_all().unwrap();
    } else {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path.join("commit.state"))
            .unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }
}

fn open_store(path: &Path, v2: bool) -> Store {
    if v2 {
        fs::create_dir(path).unwrap();
        fs::write(
            path.join("records.log"),
            include_bytes!("fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            path.join("commit.state"),
            include_bytes!("fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    Store::open(path).unwrap()
}

fn bounded_detailed(
    path: &Path,
    destination: &Path,
    base: &Path,
    digest: &[u8; 32],
    chunk_keys: usize,
    spill_bytes: u64,
    operations: u64,
) -> Result<[u8; 32], ManagedRebuildFailure> {
    CheckpointStore::rebuild_managed_selection_bounded_detailed(
        path,
        destination,
        base,
        digest,
        chunk_keys,
        8192,
        spill_bytes,
        2,
        operations,
    )
}

fn assert_no_stages(path: &Path) {
    let cleaned = cleanup_abandoned_staging(path).unwrap();
    assert_eq!(cleaned.removed_markers, 0, "{}", path.display());
}

#[test]
fn bounded_rebuild_recovers_a_suffix_over_the_configured_overlay_limit() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("base");
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"kept").unwrap();
    drop(store);
    let digest = CheckpointStore::rebuild_managed_selection(&path, &base).unwrap();
    let store = retry(|| Store::open(&path)).unwrap();
    for id in 0..40 {
        store
            .put(format!("key-{id:03}").as_bytes(), b"value")
            .unwrap();
    }
    drop(store);
    let before = fs::read(path.join("records.log")).unwrap();
    let witness = witness_bytes(&path);
    assert!(
        CheckpointStore::open_managed_with_resource_limits(&path, 1 << 20, 4, 1024, None).is_err()
    );
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(witness_bytes(&path), witness);
    let destination = root.path().join("rebuilt");
    bounded_detailed(&path, &destination, &base, &digest, 3, 1 << 20, 100).unwrap();
    let rebuilt =
        retry(|| CheckpointStore::open_managed_with_resource_limits(&path, 1 << 20, 4, 1024, None))
            .unwrap();
    assert_eq!(rebuilt.overlay_keys().unwrap(), 0);
    assert_eq!(rebuilt.get(b"base").unwrap().as_deref(), Some(&b"kept"[..]));
    for id in 0..40 {
        assert_eq!(
            rebuilt
                .get(format!("key-{id:03}").as_bytes())
                .unwrap()
                .as_deref(),
            Some(&b"value"[..])
        );
    }
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
}

#[test]
fn bounded_rebuild_matches_full_replay_peak_batches_and_value_offsets() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let base = root.path().join("base");
        let store = open_store(&path, v2);
        store.put(b"a", b"a").unwrap();
        store.put(b"b", b"b").unwrap();
        store.put(b"c", b"c").unwrap();
        store.delete(b"c").unwrap();
        let digest = store.checkpoint_to(&base).unwrap();
        drop(store);

        let store = retry(|| Store::open(&path)).unwrap();
        store.put(b"d", b"d").unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"e",
                    value: b"first",
                },
                BatchOperation::Put {
                    key: b"f",
                    value: b"f",
                },
                BatchOperation::Delete { key: b"d" },
                BatchOperation::Put {
                    key: b"e",
                    value: b"second",
                },
                BatchOperation::Delete { key: b"missing" },
                BatchOperation::Put {
                    key: b"b",
                    value: b"replaced",
                },
            ])
            .unwrap();
        store.put(b"g", b"g").unwrap();
        store.delete(b"g").unwrap();
        store.put(b"c", b"returned").unwrap();
        store.delete(b"e").unwrap();
        store.delete(b"f").unwrap();
        store.delete(b"c").unwrap();
        drop(store);

        let destination = root.path().join("rebuilt");
        let rebuilt = bounded_detailed(&path, &destination, &base, &digest, 1, 32 << 20, 10_000)
            .unwrap_or_else(|error| panic!("v2={v2}: {error}"));
        let reopened =
            retry(|| CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None))
                .unwrap();
        assert_eq!(reopened.overlay_keys().unwrap(), 0);
        assert_eq!(reopened.get(b"a").unwrap().as_deref(), Some(&b"a"[..]));
        assert_eq!(
            reopened.get(b"b").unwrap().as_deref(),
            Some(&b"replaced"[..])
        );
        assert!(reopened.get(b"c").unwrap().is_none());
        assert!(reopened.get(b"d").unwrap().is_none());
        assert!(reopened.get(b"e").unwrap().is_none());
        assert!(reopened.get(b"f").unwrap().is_none());
        assert!(reopened.get(b"g").unwrap().is_none());
        assert!(reopened.get(b"missing").unwrap().is_none());
        drop(reopened);

        let reference_path = root.path().join("reference");
        let reference = retry(|| Store::open(&path)).unwrap();
        let reference_digest = reference.checkpoint_to(&reference_path).unwrap();
        drop(reference);
        assert_eq!(rebuilt, reference_digest, "v2={v2}");
        assert_eq!(
            fs::read(&destination).unwrap(),
            fs::read(&reference_path).unwrap(),
            "v2={v2}"
        );
        let header = fs::read(&destination).unwrap();
        assert_eq!(&header[..8], b"EMDAIDX1");
        let count = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let peak = u64::from_le_bytes(header[24..32].try_into().unwrap());
        assert!(
            peak > count,
            "v2={v2}: historical key-count peak {peak} must exceed final count {count}"
        );
        assert_no_stages(&path);
        assert_no_stages(root.path());
    }
}

#[test]
fn empty_suffix_can_rebuild_without_spill_files() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("base");
    let store = Store::open(&path).unwrap();
    store.put(b"only", b"value").unwrap();
    let digest = store.checkpoint_to(&base).unwrap();
    drop(store);
    let destination = root.path().join("rebuilt");
    let rebuilt = CheckpointStore::rebuild_managed_selection_bounded(
        &path,
        &destination,
        &base,
        &digest,
        1,
        1024,
        0,
        2,
        0,
    )
    .unwrap();
    assert_eq!(fs::read(&destination).unwrap(), fs::read(&base).unwrap());
    assert_eq!(rebuilt, digest);
    assert_no_stages(&path);
}

#[test]
fn missing_untrusted_and_conflicting_inputs_fail_before_a_new_selection() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("base");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"value").unwrap();
    let digest = store.checkpoint_to(&base).unwrap();
    drop(store);
    let base_bytes = fs::read(&base).unwrap();

    let missing = root.path().join("absent");
    let destination = root.path().join("dest");
    let error =
        bounded_detailed(&path, &destination, &missing, &digest, 1, 1 << 20, 100).unwrap_err();
    assert!(matches!(error, ManagedRebuildFailure::NotPublished(_)));
    assert!(!destination.exists());

    let mut wrong = digest;
    wrong[0] ^= 1;
    let error = bounded_detailed(&path, &destination, &base, &wrong, 1, 1 << 20, 100).unwrap_err();
    assert!(matches!(error, ManagedRebuildFailure::NotPublished(_)));
    assert!(!destination.exists());
    assert_eq!(fs::read(&base).unwrap(), base_bytes);

    let link = root.path().join("linked-base");
    std::os::unix::fs::symlink(&base, &link).unwrap();
    let error = bounded_detailed(&path, &destination, &link, &digest, 1, 1 << 20, 100).unwrap_err();
    assert!(matches!(error, ManagedRebuildFailure::NotPublished(_)));
    assert!(!destination.exists());

    let log_path = path.join("records.log");
    let log_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)
        .unwrap();
    let flipped = log_file.metadata().unwrap().len() - 1;
    let mut byte = [0_u8; 1];
    log_file.read_exact_at(&mut byte, flipped).unwrap();
    byte[0] ^= 1;
    log_file.write_all_at(&byte, flipped).unwrap();
    log_file.sync_all().unwrap();
    let error = bounded_detailed(&path, &destination, &base, &digest, 1, 1 << 20, 100).unwrap_err();
    assert!(matches!(error, ManagedRebuildFailure::NotPublished(_)));
    assert!(!destination.exists());
    assert_eq!(fs::read(&base).unwrap(), base_bytes);
    byte[0] ^= 1;
    log_file.write_all_at(&byte, flipped).unwrap();
    log_file.sync_all().unwrap();

    fs::write(&destination, b"keep").unwrap();
    let error = bounded_detailed(&path, &destination, &base, &digest, 1, 1 << 20, 100).unwrap_err();
    assert!(matches!(
        error,
        ManagedRebuildFailure::CheckpointOutcomeUnknown(_)
    ));
    assert_eq!(fs::read(&destination).unwrap(), b"keep");
    assert!(!path.join("checkpoint.selection").exists());
}

#[test]
fn limits_locks_and_bad_selection_do_not_publish() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("base");
    let store = Store::open(&path).unwrap();
    store.put(b"a", b"a").unwrap();
    let digest = store.checkpoint_to(&base).unwrap();
    drop(store);
    let held = retry(|| Store::open(&path)).unwrap();
    let destination = root.path().join("blocked");
    let error = bounded_detailed(&path, &destination, &base, &digest, 1, 1 << 20, 10).unwrap_err();
    match error {
        ManagedRebuildFailure::NotPublished(error) => {
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        }
        other => panic!("active store was not excluded: {other}"),
    }
    assert!(!destination.exists());
    drop(held);

    let store = retry(|| Store::open(&path)).unwrap();
    store.put(b"b", b"b").unwrap();
    store.put(b"c", b"c").unwrap();
    drop(store);
    let error = bounded_detailed(&path, &destination, &base, &digest, 1, 1, 100).unwrap_err();
    assert!(
        matches!(error, ManagedRebuildFailure::NotPublished(ref inner) if inner.to_string().contains("spill"))
    );
    assert!(!destination.exists());
    assert_no_stages(&path);

    let error = bounded_detailed(&path, &destination, &base, &digest, 1, 1 << 20, 1).unwrap_err();
    assert!(
        matches!(error, ManagedRebuildFailure::NotPublished(ref inner) if inner.to_string().contains("operation"))
    );
    assert!(!destination.exists());

    let error = CheckpointStore::rebuild_managed_selection_bounded_detailed(
        &path,
        &destination,
        &base,
        &digest,
        1,
        1024,
        1 << 20,
        1,
        10,
    )
    .unwrap_err();
    assert!(matches!(error, ManagedRebuildFailure::NotPublished(_)));

    let selection = path.join("checkpoint.selection");
    fs::write(&selection, b"not a selection").unwrap();
    let error = bounded_detailed(&path, &destination, &base, &digest, 8, 1 << 20, 100).unwrap_err();
    assert!(matches!(error, ManagedRebuildFailure::NotPublished(_)));
    assert_eq!(fs::read(&selection).unwrap(), b"not a selection");
    assert!(!destination.exists());
}

#[test]
fn complete_unacknowledged_suffix_is_adopted_and_a_torn_suffix_is_not() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("base");
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"acked").unwrap();
    let digest = store.checkpoint_to(&base).unwrap();
    drop(store);
    let witness = witness_bytes(&path);
    let store = Store::open(&path).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"p",
                value: b"one",
            },
            BatchOperation::Delete { key: b"safe" },
            BatchOperation::Put {
                key: b"p",
                value: b"two",
            },
        ])
        .unwrap();
    store.put(b"q", b"q").unwrap();
    drop(store);
    let complete_log = fs::read(path.join("records.log")).unwrap();
    restore_witness(&path, &witness);

    let torn = root.path().join("torn-store");
    copy_store(&path, &torn);
    let mut truncated = fs::read(torn.join("records.log")).unwrap();
    truncated.pop().unwrap();
    fs::write(torn.join("records.log"), &truncated).unwrap();
    restore_witness(&torn, &witness);
    let torn_dest = root.path().join("torn-checkpoint");
    let error = bounded_detailed(&torn, &torn_dest, &base, &digest, 1, 1 << 20, 100).unwrap_err();
    assert!(matches!(error, ManagedRebuildFailure::NotPublished(_)));
    assert!(!torn_dest.exists());
    assert_eq!(fs::read(torn.join("records.log")).unwrap(), truncated);
    assert_eq!(witness_bytes(&torn), witness);

    let destination = root.path().join("rebuilt");
    let rebuilt = bounded_detailed(&path, &destination, &base, &digest, 1, 1 << 20, 100).unwrap();
    assert_ne!(witness_bytes(&path), witness);
    assert_eq!(fs::read(path.join("records.log")).unwrap(), complete_log);
    let reopened = retry(|| CheckpointStore::open_managed(&path)).unwrap();
    assert!(reopened.get(b"safe").unwrap().is_none());
    assert_eq!(reopened.get(b"p").unwrap().as_deref(), Some(&b"two"[..]));
    assert_eq!(reopened.get(b"q").unwrap().as_deref(), Some(&b"q"[..]));
    drop(reopened);
    let reference = root.path().join("reference");
    let store = retry(|| Store::open(&path)).unwrap();
    let reference_digest = store.checkpoint_to(&reference).unwrap();
    drop(store);
    assert_eq!(rebuilt, reference_digest);
    assert_eq!(
        fs::read(&destination).unwrap(),
        fs::read(&reference).unwrap()
    );
}

fn copy_store(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for name in ["records.log", "commit.state", "store.lock"] {
        let from = source.join(name);
        if from.exists() {
            fs::copy(&from, destination.join(name)).unwrap();
        }
    }
    if fs::read(source.join("records.log"))
        .unwrap()
        .starts_with(b"EMDALOG3")
    {
        let from = File::open(source.join("records.log")).unwrap();
        let mut buffer = vec![0_u8; 128];
        let count = rustix::fs::fgetxattr(&from, "user.db.commit.v3", &mut buffer).unwrap();
        let to = File::open(destination.join("records.log")).unwrap();
        if rustix::fs::fsetxattr(
            &to,
            "user.db.commit.v3",
            &buffer[..count],
            rustix::fs::XattrFlags::CREATE,
        )
        .is_err()
        {
            rustix::fs::fsetxattr(
                &to,
                "user.db.commit.v3",
                &buffer[..count],
                rustix::fs::XattrFlags::REPLACE,
            )
            .unwrap();
        }
    }
}
