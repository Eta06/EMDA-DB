//! Public checks for bounded offline managed-checkpoint rebuild.
use database_engine::{
    BatchOperation, BoundedFullRebuildLimits, BoundedSuffixRebuildLimits, CheckpointStore,
    CheckpointViewLimits, ManagedRebuildFailure, Store, cleanup_abandoned_staging,
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
fn occupied_bounded_rebuild_destinations_leave_the_pending_tail_unadopted() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let base = root.path().join("base");
        let reference = root.path().join("reference");
        let store = open_store(&path, v2);
        store.put(b"base", b"acknowledged").unwrap();
        drop(store);
        let digest = retry(|| CheckpointStore::rebuild_managed_selection(&path, &base)).unwrap();
        let before = witness_bytes(&path);
        let selection_path = path.join("checkpoint.selection");
        let previous_selection = fs::read(&selection_path).unwrap();
        let base_bytes = fs::read(&base).unwrap();
        let store = retry(|| Store::open(&path)).unwrap();
        store.put(b"tail", b"pending").unwrap();
        let expected_witness = witness_bytes(&path);
        let expected_digest = store.checkpoint_to(&reference).unwrap();
        drop(store);
        restore_witness(&path, &before);
        let log_path = path.join("records.log");
        let log = fs::read(&log_path).unwrap();
        let anchor = fs::read(path.join("commit.state")).unwrap();

        let file = root.path().join("occupied-file");
        let directory = root.path().join("occupied-directory");
        let dangling = root.path().join("occupied-symlink");
        let missing_target = root.path().join("missing-target");
        fs::write(&file, b"preserve file").unwrap();
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("keep"), b"preserve directory").unwrap();
        std::os::unix::fs::symlink(&missing_target, &dangling).unwrap();
        let full_limits = BoundedFullRebuildLimits {
            max_log_bytes: log.len() as u64,
            max_operations: 2,
            max_live_keys: 2,
            max_live_key_bytes: 8,
            max_chunk_keys: 1,
            max_chunk_key_bytes: 4,
            max_spill_bytes: 4096,
            max_merge_fan_in: 2,
        };
        for destination in [&file, &directory, &dangling] {
            for full in [true, false] {
                let result = if full {
                    CheckpointStore::rebuild_managed_selection_full_bounded(
                        &path,
                        destination,
                        full_limits,
                    )
                } else {
                    bounded_detailed(&path, destination, &base, &digest, 1, 4096, 2)
                };
                assert_eq!(
                    witness_bytes(&path),
                    before,
                    "occupied destination adopted the tail: {result:?}"
                );
                assert!(
                    matches!(result, Err(ManagedRebuildFailure::NotPublished(error))
                    if error.kind() == io::ErrorKind::AlreadyExists)
                );
                assert_eq!(fs::read(&selection_path).unwrap(), previous_selection);
                assert_eq!(fs::read(&log_path).unwrap(), log);
                assert_eq!(fs::read(path.join("commit.state")).unwrap(), anchor);
                assert_eq!(fs::read(&base).unwrap(), base_bytes);
                assert_no_stages(&path);
                assert_no_stages(root.path());
            }
        }
        assert_eq!(fs::read(&file).unwrap(), b"preserve file");
        assert_eq!(
            fs::read(directory.join("keep")).unwrap(),
            b"preserve directory"
        );
        assert_eq!(fs::read_link(&dangling).unwrap(), missing_target);
        assert!(!missing_target.exists());

        let destination = root.path().join("available");
        let rebuilt = bounded_detailed(&path, &destination, &base, &digest, 1, 4096, 2).unwrap();
        assert_eq!(rebuilt, expected_digest);
        assert_eq!(
            fs::read(&destination).unwrap(),
            fs::read(&reference).unwrap()
        );
        assert_eq!(witness_bytes(&path), expected_witness);
        let reopened = retry(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"base").unwrap(),
            Some(b"acknowledged".to_vec())
        );
        assert_eq!(reopened.get(b"tail").unwrap(), Some(b"pending".to_vec()));
    }
}

#[test]
fn full_bounded_rebuild_admits_history_before_adopting_a_complete_tail() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let reference = root.path().join("reference");
        let destination = root.path().join("rebuilt");
        let missing = root.path().join("old-checkpoint");
        let store = open_store(&path, v2);
        store.put(b"abc", b"old").unwrap();
        drop(store);
        retry(|| CheckpointStore::rebuild_managed_selection(&path, &missing)).unwrap();
        let selection = path.join("checkpoint.selection");
        let original_selection = fs::read(&selection).unwrap();
        fs::remove_file(missing).unwrap();
        let store = retry(|| Store::open(&path)).unwrap();
        let previous_witness = witness_bytes(&path);
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"longkey",
                    value: b"temporary",
                },
                BatchOperation::Put {
                    key: b"abc",
                    value: b"retained",
                },
                BatchOperation::Delete { key: b"longkey" },
                BatchOperation::Delete { key: b"missing" },
            ])
            .unwrap();
        let expected_witness = witness_bytes(&path);
        let expected_digest = store.checkpoint_to(&reference).unwrap();
        drop(store);

        let log_path = path.join("records.log");
        let log = fs::read(&log_path).unwrap();
        restore_witness(&path, &previous_witness);
        let exact = BoundedFullRebuildLimits {
            max_log_bytes: log.len() as u64,
            max_operations: 5,
            max_live_keys: 2,
            max_live_key_bytes: 10,
            max_chunk_keys: 1,
            max_chunk_key_bytes: 7,
            max_spill_bytes: 4096,
            max_merge_fan_in: 2,
        };
        for (limits, message) in [
            (
                BoundedFullRebuildLimits {
                    max_log_bytes: exact.max_log_bytes - 1,
                    ..exact
                },
                "log exceeds",
            ),
            (
                BoundedFullRebuildLimits {
                    max_operations: 4,
                    ..exact
                },
                "operation limit",
            ),
            (
                BoundedFullRebuildLimits {
                    max_live_keys: 1,
                    ..exact
                },
                "historical live key limit",
            ),
            (
                BoundedFullRebuildLimits {
                    max_live_key_bytes: 9,
                    ..exact
                },
                "historical live key-byte limit",
            ),
            (
                BoundedFullRebuildLimits {
                    max_spill_bytes: 1,
                    ..exact
                },
                "spill byte limit",
            ),
        ] {
            let result = CheckpointStore::rebuild_managed_selection_full_bounded(
                &path,
                &destination,
                limits,
            );
            assert!(
                matches!(&result, Err(ManagedRebuildFailure::NotPublished(error))
                if error.kind() == io::ErrorKind::InvalidInput && error.to_string().contains(message)),
                "{result:?}"
            );
            assert!(!destination.exists());
            assert_eq!(fs::read(&selection).unwrap(), original_selection);
            assert_eq!(witness_bytes(&path), previous_witness);
            assert_eq!(fs::read(&log_path).unwrap(), log);
            assert_no_stages(&path);
        }

        let checkpoint_limits = CheckpointViewLimits {
            max_file_bytes: reference.metadata().unwrap().len(),
            max_blocks: 1,
        };
        for (rejected, message) in [
            (
                CheckpointViewLimits {
                    max_file_bytes: checkpoint_limits.max_file_bytes - 1,
                    ..checkpoint_limits
                },
                "checkpoint exceeds configured file size limit",
            ),
            (
                CheckpointViewLimits {
                    max_blocks: 0,
                    ..checkpoint_limits
                },
                "checkpoint has too many view blocks",
            ),
        ] {
            let result =
                CheckpointStore::rebuild_managed_selection_full_bounded_with_checkpoint_limits(
                    &path,
                    &destination,
                    exact,
                    rejected,
                );
            assert!(
                matches!(&result, Err(ManagedRebuildFailure::NotPublished(error))
                if error.kind() == io::ErrorKind::InvalidInput && error.to_string() == message),
                "{result:?}"
            );
            assert!(!destination.exists());
            assert_eq!(fs::read(&selection).unwrap(), original_selection);
            assert_eq!(witness_bytes(&path), previous_witness);
            assert_eq!(fs::read(&log_path).unwrap(), log);
            assert_no_stages(&path);
        }

        fs::write(&log_path, &log[..log.len() - 1]).unwrap();
        let torn =
            CheckpointStore::rebuild_managed_selection_full_bounded(&path, &destination, exact);
        assert!(matches!(torn, Err(ManagedRebuildFailure::NotPublished(_))));
        assert!(!destination.exists());
        assert_eq!(fs::read(&selection).unwrap(), original_selection);
        assert_eq!(witness_bytes(&path), previous_witness);
        fs::write(&log_path, &log).unwrap();

        let actual =
            CheckpointStore::rebuild_managed_selection_full_bounded_with_checkpoint_limits(
                &path,
                &destination,
                exact,
                checkpoint_limits,
            )
            .unwrap();
        assert_eq!(actual, expected_digest);
        assert_eq!(
            fs::read(&destination).unwrap(),
            fs::read(reference).unwrap()
        );
        assert_eq!(witness_bytes(&path), expected_witness);
        assert_ne!(fs::read(&selection).unwrap(), original_selection);
        assert_eq!(fs::read(&log_path).unwrap(), log);
        let reopened =
            retry(|| CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None))
                .unwrap();
        assert_eq!(reopened.overlay_keys().unwrap(), 0);
        assert_eq!(
            reopened.get(b"abc").unwrap().as_deref(),
            Some(&b"retained"[..])
        );
        assert!(reopened.get(b"longkey").unwrap().is_none());
        drop(reopened);
        assert_no_stages(&path);
    }
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
    assert!(matches!(error, ManagedRebuildFailure::NotPublished(error)
        if error.kind() == io::ErrorKind::AlreadyExists));
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
fn trusted_base_and_output_limits_are_admitted_before_tail_adoption() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let base = root.path().join("base");
        let reference = root.path().join("reference");
        let destination = root.path().join("rebuilt");
        let store = open_store(&path, v2);
        store.put(b"base", b"old").unwrap();
        drop(store);
        let digest = retry(|| CheckpointStore::rebuild_managed_selection(&path, &base)).unwrap();
        let previous_witness = witness_bytes(&path);
        let store = retry(|| Store::open(&path)).unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"base",
                    value: b"new",
                },
                BatchOperation::Put {
                    key: b"suffix",
                    value: b"added",
                },
            ])
            .unwrap();
        let expected = store.checkpoint_to(&reference).unwrap();
        drop(store);
        let committed_witness = witness_bytes(&path);
        restore_witness(&path, &previous_witness);

        let log = fs::read(path.join("records.log")).unwrap();
        let anchor = fs::read(path.join("commit.state")).unwrap();
        let selection = fs::read(path.join("checkpoint.selection")).unwrap();
        let base_file = fs::read(&base).unwrap();
        let limits = BoundedSuffixRebuildLimits {
            max_log_bytes: log.len() as u64,
            max_operations: 2,
            max_chunk_keys: 1,
            max_chunk_key_bytes: 6,
            max_spill_bytes: 4096,
            max_merge_fan_in: 2,
            base_checkpoint: CheckpointViewLimits {
                max_file_bytes: base_file.len() as u64,
                max_blocks: 1,
            },
            output_checkpoint: CheckpointViewLimits {
                max_file_bytes: reference.metadata().unwrap().len(),
                max_blocks: 1,
            },
        };
        let rebuild = |limits| {
            CheckpointStore::rebuild_managed_selection_bounded_with_limits(
                &path,
                &destination,
                &base,
                &digest,
                limits,
            )
        };
        for (rejected, message) in [
            (
                BoundedSuffixRebuildLimits {
                    max_log_bytes: limits.max_log_bytes - 1,
                    ..limits
                },
                "existing log exceeds configured limit",
            ),
            (
                BoundedSuffixRebuildLimits {
                    max_operations: 1,
                    ..limits
                },
                "rebuild operation limit exceeded",
            ),
            (
                BoundedSuffixRebuildLimits {
                    base_checkpoint: CheckpointViewLimits {
                        max_file_bytes: limits.base_checkpoint.max_file_bytes - 1,
                        ..limits.base_checkpoint
                    },
                    ..limits
                },
                "checkpoint exceeds configured file size limit",
            ),
            (
                BoundedSuffixRebuildLimits {
                    base_checkpoint: CheckpointViewLimits {
                        max_blocks: 0,
                        ..limits.base_checkpoint
                    },
                    ..limits
                },
                "checkpoint has too many view blocks",
            ),
            (
                BoundedSuffixRebuildLimits {
                    output_checkpoint: CheckpointViewLimits {
                        max_file_bytes: limits.output_checkpoint.max_file_bytes - 1,
                        ..limits.output_checkpoint
                    },
                    ..limits
                },
                "checkpoint exceeds configured file size limit",
            ),
            (
                BoundedSuffixRebuildLimits {
                    output_checkpoint: CheckpointViewLimits {
                        max_blocks: 0,
                        ..limits.output_checkpoint
                    },
                    ..limits
                },
                "checkpoint has too many view blocks",
            ),
        ] {
            let result = rebuild(rejected);
            assert!(
                matches!(&result, Err(ManagedRebuildFailure::NotPublished(error))
                    if error.kind() == io::ErrorKind::InvalidInput && error.to_string() == message),
                "{result:?}"
            );
            assert!(!destination.exists());
            assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
            assert_eq!(fs::read(path.join("commit.state")).unwrap(), anchor);
            assert_eq!(
                fs::read(path.join("checkpoint.selection")).unwrap(),
                selection
            );
            assert_eq!(fs::read(&base).unwrap(), base_file);
            assert_eq!(witness_bytes(&path), previous_witness);
            assert_no_stages(&path);
            assert_no_stages(root.path());
        }

        assert_eq!(rebuild(limits).unwrap(), expected);
        assert_eq!(
            fs::read(&destination).unwrap(),
            fs::read(&reference).unwrap()
        );
        assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
        assert_eq!(witness_bytes(&path), committed_witness);
        let reopened = retry(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"base").unwrap().as_deref(),
            Some(b"new".as_slice())
        );
        assert_eq!(
            reopened.get(b"suffix").unwrap().as_deref(),
            Some(b"added".as_slice())
        );
    }
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

#[test]
fn torn_tail_after_complete_record_preserves_existing_selection_and_can_retry() {
    for v2 in [true, false] {
        check_torn_tail(v2, false);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn v3_torn_tail_preserves_metadata_witness_and_anchor() {
    check_torn_tail(false, true);
}

fn check_torn_tail(v2: bool, require_v3: bool) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("base");
    let store = open_store(&path, v2);
    store.put(b"base", b"value").unwrap();
    drop(store);
    let digest = CheckpointStore::rebuild_managed_selection(&path, &base).unwrap();
    let selection = fs::read(path.join("checkpoint.selection")).unwrap();
    let witness = witness_bytes(&path);
    let anchor = fs::read(path.join("commit.state")).unwrap();
    if require_v3 {
        assert!(
            fs::read(path.join("records.log"))
                .unwrap()
                .starts_with(b"EMDALOG3"),
            "V3 metadata witness required"
        );
        let file = File::open(path.join("records.log")).unwrap();
        let mut bytes = [0_u8; 128];
        let count = rustix::fs::fgetxattr(&file, "user.db.commit.v3", &mut bytes).unwrap();
        assert_eq!(count, 36);
        assert_eq!(&bytes[..count], witness);
        eprintln!("verified EMDALOG3 and user.db.commit.v3 ({count} bytes)");
    }

    let store = Store::open(&path).unwrap();
    store.put(b"complete", b"first").unwrap();
    store.put(b"torn", b"second").unwrap();
    drop(store);
    let log_path = path.join("records.log");
    let complete_log = fs::read(&log_path).unwrap();
    restore_witness(&path, &witness);
    let mut torn_log = complete_log.clone();
    torn_log.pop().unwrap();
    fs::write(&log_path, &torn_log).unwrap();

    let destination = root.path().join("rebuilt");
    let error = bounded_detailed(&path, &destination, &base, &digest, 1, 1 << 20, 100).unwrap_err();
    match error {
        ManagedRebuildFailure::NotPublished(error) => assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
            ),
            "v2={v2}: {error}"
        ),
        error => panic!("v2={v2}: {error}"),
    }
    assert!(!destination.exists());
    assert_eq!(fs::read(&log_path).unwrap(), torn_log);
    assert_eq!(witness_bytes(&path), witness);
    assert_eq!(fs::read(path.join("commit.state")).unwrap(), anchor);
    assert_eq!(
        fs::read(path.join("checkpoint.selection")).unwrap(),
        selection
    );
    assert_no_stages(&path);
    assert_no_stages(root.path());

    fs::write(&log_path, &complete_log).unwrap();
    bounded_detailed(&path, &destination, &base, &digest, 1, 1 << 20, 100).unwrap();
    let reopened = CheckpointStore::open_managed(&path).unwrap();
    assert_eq!(
        reopened.get(b"base").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    assert_eq!(
        reopened.get(b"complete").unwrap().as_deref(),
        Some(&b"first"[..])
    );
    assert_eq!(
        reopened.get(b"torn").unwrap().as_deref(),
        Some(&b"second"[..])
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
