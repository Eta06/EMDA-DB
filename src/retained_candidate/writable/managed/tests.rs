use super::*;
use crate::retained_base_v2::Limits;
use crate::retained_candidate::{IndexLimits, OverlayLimits, SuffixLimits};
use crate::{RetainedExportLimits, Store};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 8192,
        entries: 32,
        key_bytes: 256,
        work_bytes: 8193,
    },
    index: IndexLimits {
        blocks: 4,
        anchor_bytes: 64,
    },
    suffix: SuffixLimits {
        bytes: 8192,
        operations: 64,
        work: 4096,
    },
    overlay: OverlayLimits {
        entries: 32,
        key_bytes: 256,
    },
};

const EXPORT_LIMITS: RetainedExportLimits = RetainedExportLimits {
    max_log_bytes: 8192,
    max_live_keys: 32,
    max_live_key_bytes: 256,
    max_output_bytes: 8192,
};

struct Fixture {
    tmp: tempfile::TempDir,
    root: PathBuf,
    expected: Expected,
}

impl Fixture {
    fn new(limits: RetainedStoreLimits) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let source = Store::open(tmp.path().join("source")).unwrap();
        source.put(b"a", b"old").unwrap();
        source.put(b"z", b"kept").unwrap();
        let candidate = tmp.path().join("candidate");
        let expected = source
            .build_retained_candidate(&candidate, [3; 16], [4; 16], EXPORT_LIMITS)
            .unwrap();
        let root = tmp.path().join("catalog");
        ManagedRetainedStore::create(&candidate, &root, limits, &expected).unwrap();
        Self {
            tmp,
            root,
            expected,
        }
    }

    fn open(&self) -> ManagedRetainedStore {
        ManagedRetainedStore::open(&self.root, LIMITS).unwrap()
    }
}

#[test]
fn managed_backup_restores_the_captured_generation_after_source_reclamation() {
    let f = Fixture::new(LIMITS);
    let backup = f.tmp.path().join("backup");
    let restored = f.tmp.path().join("restored");
    let mut store = f.open();
    store.compact().unwrap();
    assert!(store.reclaim_inactive().unwrap());
    store
        .write_batch(&[
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put {
                key: b"b",
                value: b"captured",
            },
        ])
        .unwrap();
    let captured = store.scan(None, None, 4).unwrap();
    let source_id = store.generation_id().unwrap();
    let expected = store.backup_to(&backup).unwrap();
    assert_eq!(expected.end, store.store.suffix.len() as u64);
    store.put(b"b", b"changed").unwrap();
    store.compact().unwrap();
    assert!(store.reclaim_inactive().unwrap());
    let restored_id =
        ManagedRetainedStore::restore_backup(&backup, &restored, LIMITS, &expected).unwrap();
    assert_ne!(restored_id, source_id);
    let mut recovered = ManagedRetainedStore::open(&restored, LIMITS).unwrap();
    assert_eq!(recovered.scan(None, None, 4).unwrap(), captured);
    assert!(Store::open(backup.join("candidate")).is_err());
    recovered.put(b"new", b"recovery").unwrap();
    recovered.compact().unwrap();
    assert!(recovered.reclaim_inactive().unwrap());
    drop(recovered);
    assert_eq!(
        ManagedRetainedStore::open(&restored, LIMITS)
            .unwrap()
            .get(b"new", 8)
            .unwrap()
            .unwrap(),
        b"recovery"
    );
    assert_eq!(store.get(b"b", 7).unwrap().unwrap(), b"changed");
}

#[test]
fn managed_backup_rollback_binding_and_limits_reject_before_restore_publication() {
    let f = Fixture::new(LIMITS);
    let backup = f.tmp.path().join("backup");
    let destination = f.tmp.path().join("restored");
    let mut store = f.open();
    let earlier = store.backup_to(f.tmp.path().join("earlier")).unwrap();
    store.put(b"b", b"later").unwrap();
    let expected = store.backup_to(&backup).unwrap();
    let suffix = backup.join("suffix");
    let suffix_before = fs::read(&suffix).unwrap();
    let base = backup.join("candidate/base");
    let base_before = fs::read(&base).unwrap();

    assert!(matches!(
        ManagedRetainedStore::restore_backup(&backup, &destination, LIMITS, &earlier),
        Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    let limit = RetainedStoreLimits {
        suffix: SuffixLimits {
            bytes: expected.end - 1,
            ..LIMITS.suffix
        },
        ..LIMITS
    };
    assert!(matches!(
        ManagedRetainedStore::restore_backup(&backup, &destination, limit, &expected),
        Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    for (path, original) in [(&suffix, &suffix_before), (&base, &base_before)] {
        let mut corrupt = original.clone();
        corrupt[8] ^= 0x10;
        fs::write(path, corrupt).unwrap();
        assert!(matches!(
            ManagedRetainedStore::restore_backup(&backup, &destination, LIMITS, &expected),
            Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
        fs::write(path, original).unwrap();
    }
    assert!(!destination.exists());
    assert!(matches!(
        store.backup_to(f.root.join("inside")),
        Err(crate::SuffixExportFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        ManagedRetainedStore::restore_backup(&backup, backup.join("inside"), LIMITS, &expected),
        Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    ManagedRetainedStore::restore_backup(&backup, &destination, LIMITS, &expected).unwrap();
}

#[test]
fn managed_backup_uncertainty_returns_the_verified_binding() {
    let f = Fixture::new(LIMITS);
    let backup = f.tmp.path().join("backup");
    let restored = f.tmp.path().join("restored");
    let mut store = f.open();
    store.put(b"b", b"captured").unwrap();
    let error = store
        .backup_using(&backup, |_| Err(io::Error::other("sync interrupted")))
        .unwrap_err();
    let crate::SuffixExportFailure::OutcomeUnknown { error, expected } = error else {
        panic!("unexpected backup failure: {error:?}");
    };
    assert_eq!(error.to_string(), "sync interrupted");
    ManagedRetainedStore::restore_backup(&backup, &restored, LIMITS, &expected).unwrap();
    assert_eq!(
        ManagedRetainedStore::open(&restored, LIMITS)
            .unwrap()
            .get(b"b", 8)
            .unwrap()
            .unwrap(),
        b"captured"
    );
}

#[test]
fn managed_backup_refuses_changed_witnessed_suffix() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.put(b"b", b"captured").unwrap();
    let source = Slot::A.path(&f.root).join("records.log");
    let file = OpenOptions::new().write(true).open(source).unwrap();
    file.write_all_at(&[0], store.store.header.len as u64)
        .unwrap();
    let destination = f.tmp.path().join("backup");
    assert!(matches!(
        store.backup_to(&destination),
        Err(crate::SuffixExportFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    assert!(!destination.exists());
}

#[test]
fn ordinary_import_preserves_source_and_carries_ordered_batch_peaks() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let destination = tmp.path().join("managed");
    let store = Store::open(&source).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"temp",
                value: b"first",
            },
            BatchOperation::Put {
                key: b"a",
                value: b"live",
            },
            BatchOperation::Delete { key: b"temp" },
        ])
        .unwrap();
    let original_log = fs::read(source.join("records.log")).unwrap();
    let original_witness = fs::read(source.join("commit.state")).unwrap();
    let id =
        ManagedRetainedStore::copy_from_store(store, &source, &destination, EXPORT_LIMITS, LIMITS)
            .unwrap();
    let mut copy = ManagedRetainedStore::open(&destination, LIMITS).unwrap();
    assert_eq!(copy.generation_id().unwrap(), id);
    let base = copy.store.base.metadata();
    assert_eq!(
        (
            base.entries,
            base.key_bytes,
            base.peak_entries,
            base.peak_key_bytes
        ),
        (1, 1, 2, 5)
    );
    assert_eq!(base.boundary, original_log.len() as u64);
    assert_eq!(copy.get(b"a", 4).unwrap().unwrap(), b"live");
    assert_eq!(copy.get(b"temp", 8).unwrap(), None);
    assert_eq!(fs::read(source.join("records.log")).unwrap(), original_log);
    assert_eq!(
        fs::read(source.join("commit.state")).unwrap(),
        original_witness
    );
    copy.put(b"a", b"replacement").unwrap();
    drop(copy);
    let copy = ManagedRetainedStore::open(&destination, LIMITS).unwrap();
    assert_eq!(copy.get(b"a", 16).unwrap().unwrap(), b"replacement");
    assert_eq!(
        Store::open(&source).unwrap().get(b"a").unwrap().unwrap(),
        b"live"
    );
}

#[test]
fn ordinary_import_holds_source_lock_even_when_final_publication_is_uncertain() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let destination = tmp.path().join("managed");
    let store = Store::open(&source).unwrap();
    store.put(b"a", b"safe").unwrap();
    let original = fs::read(source.join("records.log")).unwrap();
    let result = ManagedRetainedStore::copy_from_store_using(
        store,
        &source,
        &destination,
        EXPORT_LIMITS,
        LIMITS,
        |_| {
            assert_eq!(
                Store::open(&source).err().unwrap().kind(),
                io::ErrorKind::WouldBlock
            );
            assert!(destination.join(ACTIVE_FILE).is_file());
            Err(io::Error::other("parent sync interrupted"))
        },
    );
    assert!(matches!(
        result,
        Err(PublicationFailure::OutcomeUnknown(error)) if error.to_string() == "parent sync interrupted"
    ));
    assert_eq!(fs::read(source.join("records.log")).unwrap(), original);
    let recovered = ManagedRetainedStore::open(&destination, LIMITS).unwrap();
    assert_eq!(recovered.get(b"a", 4).unwrap().unwrap(), b"safe");
    assert!(Store::open(&source).is_ok());
}

#[test]
fn ordinary_import_rejects_unsafe_paths_budget_exhaustion_and_corrupt_history() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let different = tmp.path().join("different");
    let destination = tmp.path().join("managed");
    let store = Store::open(&source).unwrap();
    store.put(b"a", b"superseded-value").unwrap();
    store.put(b"a", b"new").unwrap();
    drop(store);
    drop(Store::open(&different).unwrap());
    let original = fs::read(source.join("records.log")).unwrap();
    for (name, target, limits) in [
        (&different, destination.clone(), EXPORT_LIMITS),
        (&source, source.join("inside"), EXPORT_LIMITS),
        (
            &source,
            destination.clone(),
            RetainedExportLimits {
                max_log_bytes: 1,
                ..EXPORT_LIMITS
            },
        ),
    ] {
        let result = ManagedRetainedStore::copy_from_store(
            Store::open(&source).unwrap(),
            name,
            &target,
            limits,
            LIMITS,
        );
        assert!(matches!(result, Err(PublicationFailure::NotPublished(_))));
        assert!(!target.exists());
    }
    assert_eq!(fs::read(source.join("records.log")).unwrap(), original);
    let store = Store::open(&source).unwrap();
    let value = b"superseded-value";
    let offset = original
        .windows(value.len())
        .position(|bytes| bytes == value)
        .unwrap();
    let file = OpenOptions::new()
        .write(true)
        .open(source.join("records.log"))
        .unwrap();
    file.write_all_at(b"!", offset as u64).unwrap();
    let result =
        ManagedRetainedStore::copy_from_store(store, &source, &destination, EXPORT_LIMITS, LIMITS);
    assert!(matches!(
        result,
        Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    assert!(!destination.exists());
    assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 2);
}

#[test]
fn managed_cutover_reclaims_only_the_inactive_generation_and_preserves_batch_peaks() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let original_id = store.generation_id().unwrap();
    let original = Slot::A.path(&f.root);
    assert_eq!(
        ManagedRetainedStore::open(&f.root, LIMITS)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(Store::open(&f.root).is_err());
    assert!(!store.reclaim_inactive().unwrap());
    let long = b"intermediate-key";
    store
        .write_batch(&[
            BatchOperation::Put {
                key: long,
                value: b"transient",
            },
            BatchOperation::Put {
                key: b"b",
                value: b"bound",
            },
            BatchOperation::Delete { key: long },
            BatchOperation::Delete { key: b"a" },
        ])
        .unwrap();
    let state = store.scan(None, None, 32).unwrap();
    let old_size = fs::metadata(original.join("records.log")).unwrap().len();
    assert!(old_size > super::super::MANAGED_HEADER_BYTES as u64);
    let next_id = store.compact().unwrap();
    assert_ne!(next_id, original_id);
    assert_eq!(store.scan(None, None, 32).unwrap(), state);
    let meta = store.store.base.metadata();
    assert_eq!((meta.source_id, meta.boundary), (original_id, old_size));
    assert_eq!(
        (
            meta.entries,
            meta.key_bytes,
            meta.peak_entries,
            meta.peak_key_bytes
        ),
        (2, 2, 4, 3 + long.len() as u64)
    );
    assert_eq!(
        fs::metadata(Slot::B.path(&f.root).join("records.log"))
            .unwrap()
            .len(),
        super::super::MANAGED_HEADER_BYTES as u64,
    );
    assert!(
        matches!(store.compact(), Err(PublicationFailure::NotPublished(ref e)) if e.kind() == io::ErrorKind::AlreadyExists)
    );
    assert_eq!(
        RetainedStore::open(&original, LIMITS, &f.expected)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert!(store.reclaim_inactive().unwrap());
    assert!(!original.exists());
    store.put(b"c", b"new").unwrap();
    drop(store);
    let mut reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), next_id);
    assert_eq!(reopened.get(b"a", 0).unwrap(), None);
    assert_eq!(reopened.get(b"b", 5).unwrap(), Some(b"bound".to_vec()));
    assert_eq!(reopened.get(b"c", 3).unwrap(), Some(b"new".to_vec()));
    reopened.compact().unwrap();
    assert!(reopened.reclaim_inactive().unwrap());
    let source = Store::open(f.tmp.path().join("source")).unwrap();
    assert_eq!(source.get(b"a").unwrap(), Some(b"old".to_vec()));
    assert_eq!(source.get(b"c").unwrap(), None);
}

#[test]
fn rebase_limit_rejection_retains_the_active_generation_and_writes() {
    let limits = RetainedStoreLimits {
        base: Limits {
            entries: 2,
            ..LIMITS.base
        },
        suffix: SuffixLimits {
            operations: 1,
            ..LIMITS.suffix
        },
        ..LIMITS
    };
    let f = Fixture::new(limits);
    let mut store = ManagedRetainedStore::open(&f.root, limits).unwrap();
    store.put(b"b", b"outside base quota").unwrap();
    assert!(matches!(
        store.delete(b"b"),
        Err(MutationFailure::Rejected(ref error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    let active = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    assert!(
        matches!(store.compact(), Err(PublicationFailure::NotPublished(ref e)) if e.kind() == io::ErrorKind::InvalidInput)
    );
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), active);
    assert!(!Slot::B.path(&f.root).exists());
    drop(store);
    let mut reopened = ManagedRetainedStore::open(&f.root, LIMITS).unwrap();
    assert_eq!(
        reopened.get(b"b", 32).unwrap(),
        Some(b"outside base quota".to_vec())
    );
    assert_eq!(reopened.limits().unwrap().fields(), limits.fields());
    assert!(matches!(
        reopened.delete(b"b"),
        Err(MutationFailure::Rejected(ref error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    let generation = reopened.compact_with_limits(LIMITS).unwrap();
    assert!(reopened.reclaim_inactive().unwrap());
    reopened.delete(b"z").unwrap();
    drop(reopened);
    let error = ManagedRetainedStore::open(&f.root, limits).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("recovery ceiling"));
    let reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), generation);
    assert_eq!(reopened.limits().unwrap().fields(), LIMITS.fields());
    assert_eq!(
        reopened.get(b"b", 32).unwrap().unwrap(),
        b"outside base quota"
    );
    assert_eq!(reopened.get(b"z", 32).unwrap(), None);
}

#[test]
fn ceiling_recovers_the_selected_limits_after_an_uncertain_resize() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let original = store.generation_id().unwrap();
    let prior_selection = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let smaller = RetainedStoreLimits {
        suffix: SuffixLimits {
            operations: 1,
            ..LIMITS.suffix
        },
        ..LIMITS
    };
    assert!(matches!(
        store.compact_using_limits(
            smaller,
            || Ok(()),
            |_| Err(io::Error::other("selection sync"))
        ),
        Err(PublicationFailure::OutcomeUnknown(_))
    ));
    assert!(store.limits().is_err());
    drop(store);
    let next_selection = fs::read(f.root.join(ACTIVE_FILE)).unwrap();

    // Model either complete selector being visible on restart.
    fs::write(f.root.join(ACTIVE_FILE), &prior_selection).unwrap();
    let old = f.open();
    assert_eq!(old.generation_id().unwrap(), original);
    assert_eq!(old.limits().unwrap().fields(), LIMITS.fields());
    drop(old);
    fs::write(f.root.join(ACTIVE_FILE), &next_selection).unwrap();
    let mut next = f.open();
    assert_ne!(next.generation_id().unwrap(), original);
    assert_eq!(next.limits().unwrap().fields(), smaller.fields());
    next.put(b"b", b"first").unwrap();
    assert!(matches!(
        next.put(b"c", b"second"),
        Err(MutationFailure::Rejected(ref error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
}

#[test]
fn selection_failure_before_rename_leaves_only_old_slot_authorized() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let old = store.active.clone();
    store.put(b"b", b"safe").unwrap();
    let result = store.compact_using(|| Err(io::Error::other("before selection")), sync_existing);
    assert!(matches!(result, Err(PublicationFailure::NotPublished(_))));
    assert!(Slot::B.path(&f.root).exists());
    assert_eq!(Active::load(&f.root, old.catalog).unwrap(), old);
    store.put(b"c", b"still active").unwrap();
    assert!(store.reclaim_inactive().unwrap());
    drop(store);
    let reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), old.generation);
    assert_eq!(
        reopened.get(b"c", 16).unwrap(),
        Some(b"still active".to_vec())
    );
}

#[test]
fn selection_sync_failure_blocks_writes_and_reclaim_until_reopen() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.put(b"b", b"safe").unwrap();
    let old = store.active.clone();
    let result = store.compact_using(|| Ok(()), |_| Err(io::Error::other("selection sync")));
    assert!(matches!(result, Err(PublicationFailure::OutcomeUnknown(_))));
    assert!(store.get(b"b", 4).is_err());
    assert!(matches!(
        store.put(b"lost", b"value"),
        Err(MutationFailure::Rejected(_))
    ));
    assert!(store.reclaim_inactive().is_err());
    assert!(old.slot.path(&f.root).exists());
    let visible = Active::load(&f.root, old.catalog).unwrap();
    assert_ne!(old.generation, visible.generation);
    drop(store);
    let mut reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), visible.generation);
    assert_eq!(reopened.get(b"b", 4).unwrap(), Some(b"safe".to_vec()));
    assert_eq!(reopened.get(b"lost", 5).unwrap(), None);
    assert!(reopened.reclaim_inactive().unwrap());
}

#[test]
fn invalid_selection_never_falls_back_to_an_existing_generation() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.compact().unwrap();
    store.put(b"new", b"only in selected").unwrap();
    let inactive = store.active.slot.other().path(&f.root);
    let selected = store.active.slot.path(&f.root);
    drop(store);
    let path = f.root.join(ACTIVE_FILE);
    let before = fs::read(&path).unwrap();
    let mut forged = before.clone();
    forged[32] ^= 1;
    let checksum = Sha256::digest(&forged[..224]);
    forged[224..].copy_from_slice(&checksum);
    fs::write(&path, forged).unwrap();
    assert_eq!(
        ManagedRetainedStore::open(&f.root, LIMITS)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );
    fs::write(&path, b"damaged").unwrap();
    assert!(ManagedRetainedStore::open(&f.root, LIMITS).is_err());
    fs::write(&path, before).unwrap();
    fs::rename(&selected, f.tmp.path().join("missing")).unwrap();
    assert!(ManagedRetainedStore::open(&f.root, LIMITS).is_err());
    assert!(inactive.exists());
}

#[test]
fn reclamation_refuses_unknown_files_and_an_invalid_active_base() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.compact().unwrap();
    let inactive = store.active.slot.other().path(&f.root);
    let unknown = inactive.join("unknown");
    fs::write(&unknown, b"not owned").unwrap();
    assert!(store.reclaim_inactive().is_err());
    assert_eq!(fs::read(&unknown).unwrap(), b"not owned");
    fs::remove_file(unknown).unwrap();
    let base = store.active.slot.path(&f.root).join("base");
    let data = fs::read(&base).unwrap();
    let file = OpenOptions::new().write(true).open(&base).unwrap();
    file.write_all_at(&[0], 0).unwrap();
    assert!(store.reclaim_inactive().is_err());
    assert!(inactive.exists());
    file.write_all_at(&data, 0).unwrap();
    assert!(store.reclaim_inactive().unwrap());
}

#[test]
fn retired_move_error_leaves_owned_stage_for_later_cleanup() {
    for fail_catalog_sync in [false, true] {
        let f = Fixture::new(LIMITS);
        let mut store = f.open();
        store.compact().unwrap();
        let before = store.scan(None, None, 4).unwrap();
        let root = f.root.canonicalize().unwrap();
        let mut retired = None;
        let error = store
            .reclaim_using(|path| {
                if path != root {
                    retired = Some(path.join("retired"));
                }
                let retired = retired.as_ref().unwrap();
                assert!(retired.join("records.log").is_file());
                assert!(retired.join("base").is_file());
                if (path == root) == fail_catalog_sync {
                    return Err(io::Error::other("after move"));
                }
                sync_existing(path)
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "after move");
        assert!(retired.unwrap().join("commit.state").is_file());
        assert_eq!(store.scan(None, None, 4).unwrap(), before);
        assert!(!store.reclaim_inactive().unwrap());
        assert!(fs::read_dir(&f.root).unwrap().count() > 4);
        drop(store);
        let recovered =
            ManagedRetainedStore::open_with_auto_reclaim(&f.root, LIMITS, NonZeroU64::MIN).unwrap();
        assert_eq!(fs::read_dir(&f.root).unwrap().count(), 4);
        assert_eq!(recovered.scan(None, None, 4).unwrap(), before);
    }
}

#[test]
fn admission_is_rechecked_after_automatic_reclamation_before_appending() {
    let f = Fixture::new(LIMITS);
    let mut store =
        ManagedRetainedStore::open_with_auto_reclaim(&f.root, LIMITS, NonZeroU64::MIN).unwrap();
    store.put(b"a", b"acknowledged").unwrap();
    let original = store.generation_id().unwrap();
    let checks = std::cell::Cell::new(0);
    let result = store.write_batch_checking(
        &[BatchOperation::Put {
            key: b"b",
            value: b"rejected",
        }],
        || {
            checks.set(checks.get() + 1);
            if checks.get() < 3 {
                Ok(())
            } else {
                Err(io::Error::other("admission closed after reclamation"))
            }
        },
    );
    assert!(matches!(
        result,
        Err(MutationFailure::Rejected(error)) if error.to_string() == "admission closed after reclamation"
    ));
    assert_ne!(store.generation_id().unwrap(), original);
    assert_eq!(store.get(b"b", 8).unwrap(), None);
    drop(store);
    let reopened = f.open();
    assert_eq!(reopened.get(b"a", 12).unwrap().unwrap(), b"acknowledged");
    assert_eq!(reopened.get(b"b", 8).unwrap(), None);
}

#[test]
fn automatic_reclaim_recycles_both_slots_and_preserves_acknowledged_mutations() {
    let f = Fixture::new(LIMITS);
    let mut store =
        ManagedRetainedStore::open_with_auto_reclaim(&f.root, LIMITS, NonZeroU64::MIN).unwrap();
    let original = store.generation_id().unwrap();
    store.put(b"a", b"updated").unwrap();
    assert_eq!(store.generation_id().unwrap(), original);
    store
        .write_batch(&[
            BatchOperation::Delete { key: b"z" },
            BatchOperation::Put {
                key: b"b",
                value: b"added",
            },
        ])
        .unwrap();
    let second = store.generation_id().unwrap();
    assert_ne!(second, original);
    assert!(!Slot::A.path(&f.root).exists());
    store.delete(b"a").unwrap();
    assert_ne!(store.generation_id().unwrap(), second);
    assert!(!Slot::B.path(&f.root).exists());
    drop(store);
    assert_eq!(
        f.open().scan(None, None, 4).unwrap(),
        [(b"b".to_vec(), b"added".to_vec())]
    );
}

#[test]
fn automatic_reclaim_relaxes_accumulated_suffix_pressure_but_rejects_oversize_mutations() {
    for limits in [
        RetainedStoreLimits {
            suffix: SuffixLimits {
                bytes: 32,
                ..LIMITS.suffix
            },
            ..LIMITS
        },
        RetainedStoreLimits {
            suffix: SuffixLimits {
                operations: 1,
                ..LIMITS.suffix
            },
            ..LIMITS
        },
        RetainedStoreLimits {
            suffix: SuffixLimits {
                work: 1,
                ..LIMITS.suffix
            },
            ..LIMITS
        },
        RetainedStoreLimits {
            overlay: OverlayLimits {
                entries: 1,
                ..LIMITS.overlay
            },
            ..LIMITS
        },
        RetainedStoreLimits {
            overlay: OverlayLimits {
                key_bytes: 1,
                ..LIMITS.overlay
            },
            ..LIMITS
        },
    ] {
        let f = Fixture::new(limits);
        let mut store =
            ManagedRetainedStore::open_with_auto_reclaim(&f.root, limits, NonZeroU64::MAX).unwrap();
        store.put(b"a", b"one").unwrap();
        let generation = store.generation_id().unwrap();
        let impossible = store.write_batch(&[
            BatchOperation::Delete { key: b"z" },
            BatchOperation::Put {
                key: b"b",
                value: b"two",
            },
        ]);
        assert!(
            matches!(impossible, Err(MutationFailure::Rejected(error)) if error.kind() == io::ErrorKind::InvalidInput)
        );
        assert_eq!(store.generation_id().unwrap(), generation);
        assert!(!Slot::B.path(&f.root).exists());
        store.put(b"b", b"two").unwrap();
        assert_ne!(store.generation_id().unwrap(), generation);
        assert!(!Slot::A.path(&f.root).exists());
        drop(store);
        let reopened = ManagedRetainedStore::open(&f.root, limits).unwrap();
        assert_eq!(reopened.get(b"a", 3).unwrap().unwrap(), b"one");
        assert_eq!(reopened.get(b"b", 3).unwrap().unwrap(), b"two");
        assert_eq!(reopened.get(b"z", 4).unwrap().unwrap(), b"kept");
    }
}

#[test]
fn failed_automatic_rebase_rejects_the_triggering_append() {
    let limits = RetainedStoreLimits {
        base: Limits {
            entries: 2,
            ..LIMITS.base
        },
        ..LIMITS
    };
    let f = Fixture::new(limits);
    let mut store =
        ManagedRetainedStore::open_with_auto_reclaim(&f.root, limits, NonZeroU64::MIN).unwrap();
    store.put(b"b", b"acknowledged").unwrap();
    let active = Slot::A.path(&f.root);
    let log = fs::read(active.join("records.log")).unwrap();
    let witness = fs::read(active.join("commit.state")).unwrap();
    let selection = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    assert!(matches!(
        store.delete(b"z"),
        Err(MutationFailure::Rejected(error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert_eq!(fs::read(active.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(active.join("commit.state")).unwrap(), witness);
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selection);
    assert_eq!(store.get(b"z", 4).unwrap().unwrap(), b"kept");
    store.compact_with_limits(LIMITS).unwrap();
    store.delete(b"z").unwrap();
    drop(store);
    let reopened =
        ManagedRetainedStore::open_with_auto_reclaim(&f.root, LIMITS, NonZeroU64::MIN).unwrap();
    assert_eq!(reopened.get(b"z", 4).unwrap(), None);
    assert_eq!(reopened.get(b"b", 12).unwrap().unwrap(), b"acknowledged");
    assert_eq!(fs::read_dir(&f.root).unwrap().count(), 4);
}

#[test]
fn reclamation_refuses_a_complete_generation_owned_by_another_catalog() {
    let f = Fixture::new(LIMITS);
    let foreign = Fixture::new(LIMITS);
    let mut store = f.open();
    let inactive = Slot::B.path(&f.root);
    let foreign_path = Slot::A.path(&foreign.root);
    let files = ["base", "records.log", "commit.state", "store.lock"];
    fs::create_dir(&inactive).unwrap();
    for name in files {
        fs::copy(foreign_path.join(name), inactive.join(name)).unwrap();
    }
    let error = store.reclaim_inactive().unwrap_err();
    assert!(error.to_string().contains("ownership mismatch"));
    for name in files {
        assert_eq!(
            fs::read(foreign_path.join(name)).unwrap(),
            fs::read(inactive.join(name)).unwrap()
        );
    }
    assert_eq!(store.get(b"a", 3).unwrap(), Some(b"old".to_vec()));
}

#[test]
fn managed_cutover_crash_child() {
    let Some(root) = std::env::var_os("EMDA_MANAGED_RETAINED_CRASH_ROOT") else {
        return;
    };
    let point = std::env::var("EMDA_MANAGED_RETAINED_CRASH_POINT").unwrap();
    let pause = || -> io::Result<()> {
        println!("managed selection boundary");
        io::stdout().flush()?;
        loop {
            std::thread::park();
        }
    };
    if point == "import" {
        let root = Path::new(&root);
        let source = root.join("source");
        let result = ManagedRetainedStore::copy_from_store_using(
            Store::open(&source).unwrap(),
            &source,
            &root.join("managed"),
            EXPORT_LIMITS,
            LIMITS,
            |_| pause(),
        );
        panic!("managed import did not stop at the boundary: {result:?}");
    }
    if point == "backup" {
        let root = Path::new(&root);
        let store = ManagedRetainedStore::open(root.join("catalog"), LIMITS).unwrap();
        let result = store.backup_using(&root.join("interrupted-backup"), |_| pause());
        panic!("managed backup did not stop at the boundary: {result:?}");
    }
    let mut store = ManagedRetainedStore::open(root, LIMITS).unwrap();
    if point == "retire" {
        store.compact().unwrap();
        let result = store.reclaim_using(|_| pause());
        panic!("managed reclamation did not stop at the boundary: {result:?}");
    }
    let result = store.compact_using(
        || if point == "before" { pause() } else { Ok(()) },
        |_| if point == "after" { pause() } else { Ok(()) },
    );
    panic!("managed cutover did not stop at the boundary: {result:?}");
}

#[test]
fn killed_backup_can_restore_the_witnessed_snapshot_with_a_retained_binding() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.put(b"b", b"captured").unwrap();
    let expected = store
        .backup_to(f.tmp.path().join("original-backup"))
        .unwrap();
    drop(store);
    kill_at_boundary(f.tmp.path(), "backup");
    let restored = f.tmp.path().join("restored");
    ManagedRetainedStore::restore_backup(
        f.tmp.path().join("interrupted-backup"),
        &restored,
        LIMITS,
        &expected,
    )
    .unwrap();
    assert_eq!(
        ManagedRetainedStore::open(&restored, LIMITS)
            .unwrap()
            .scan(None, None, 8)
            .unwrap(),
        f.open().scan(None, None, 8).unwrap()
    );
}

#[test]
fn killed_import_recovers_the_published_copy_and_preserves_source() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source");
    let store = Store::open(&source).unwrap();
    store.put(b"a", b"acknowledged").unwrap();
    drop(store);
    let original = fs::read(source.join("records.log")).unwrap();
    kill_at_boundary(tmp.path(), "import");
    let copy = ManagedRetainedStore::open(tmp.path().join("managed"), LIMITS).unwrap();
    assert_eq!(copy.get(b"a", 12).unwrap().unwrap(), b"acknowledged");
    assert_eq!(fs::read(source.join("records.log")).unwrap(), original);
    assert_eq!(
        Store::open(&source).unwrap().get(b"a").unwrap().unwrap(),
        b"acknowledged"
    );
}

#[test]
fn killed_catalog_owner_recovers_the_visible_selection() {
    for point in ["before", "after", "retire"] {
        let f = Fixture::new(LIMITS);
        let mut store = f.open();
        store.put(b"safe", b"acknowledged").unwrap();
        let old = store.generation_id().unwrap();
        drop(store);
        kill_at_boundary(&f.root, point);

        let mut recovered =
            ManagedRetainedStore::open_with_auto_reclaim(&f.root, LIMITS, NonZeroU64::MAX).unwrap();
        assert_eq!(recovered.generation_id().unwrap() == old, point == "before");
        assert_eq!(
            recovered.get(b"safe", 16).unwrap(),
            Some(b"acknowledged".to_vec())
        );
        assert_eq!(fs::read_dir(&f.root).unwrap().count(), 4);
        recovered.put(b"new", point.as_bytes()).unwrap();
        assert!(!recovered.reclaim_inactive().unwrap());
        drop(recovered);
        assert_eq!(
            f.open().get(b"new", 8).unwrap(),
            Some(point.as_bytes().to_vec())
        );
    }
}

fn kill_at_boundary(root: &Path, point: &str) {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
        time::Duration,
    };

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "retained_candidate::writable::managed::tests::managed_cutover_crash_child",
            "--nocapture",
        ])
        .env("EMDA_MANAGED_RETAINED_CRASH_ROOT", root)
        .env("EMDA_MANAGED_RETAINED_CRASH_POINT", point)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let input = BufReader::new(child.stdout.take().unwrap());
    let (notify, wait) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in input.lines().map_while(Result::ok) {
            if line == "managed selection boundary" {
                let _ = notify.send(());
                break;
            }
        }
    });
    let reached = wait.recv_timeout(Duration::from_secs(15));
    let kill = child.kill();
    let status = child.wait().unwrap();
    reader.join().unwrap();
    reached.expect("child did not reach managed selection boundary");
    kill.unwrap();
    assert!(!status.success());
}

#[test]
fn unexpected_selection_change_through_a_live_handle_rejects_append() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let path = f.root.join(ACTIVE_FILE);
    let active = fs::read(&path).unwrap();
    let log = store.active.slot.path(&f.root).join("records.log");
    let before = fs::read(&log).unwrap();
    fs::write(&path, b"corrupted selection").unwrap();
    assert!(matches!(
        store.put(b"new", b"lost"),
        Err(MutationFailure::Rejected(_))
    ));
    assert_eq!(fs::read(&log).unwrap(), before);
    assert!(store.get(b"a", 3).is_err());
    drop(store);
    fs::write(&path, active).unwrap();
    assert_eq!(f.open().get(b"new", 8).unwrap(), None);
}

#[test]
fn managed_creation_imports_a_detached_suffix() {
    let f = Fixture::new(LIMITS);
    let source = Store::open(f.tmp.path().join("source")).unwrap();
    let candidate = f.tmp.path().join("candidate");
    let suffix = f.tmp.path().join("suffix");
    source.delete(b"a").unwrap();
    source.put(b"b", b"new").unwrap();
    let expected = source
        .export_retained_suffix(
            &candidate,
            &f.expected,
            &suffix,
            LIMITS.base,
            crate::retained_base_v2::StagingLimits {
                value_bytes: 64,
                staging_bytes: 1024,
            },
            LIMITS.suffix,
            RetainedExportLimits {
                max_log_bytes: 8192,
                max_live_keys: 32,
                max_live_key_bytes: 256,
                max_output_bytes: 8192,
            },
        )
        .unwrap();
    let destination = f.tmp.path().join("imported");
    let id = ManagedRetainedStore::create_from_suffix(
        &candidate,
        &suffix,
        &destination,
        LIMITS,
        &expected,
    )
    .unwrap();
    let imported = ManagedRetainedStore::open(&destination, LIMITS).unwrap();
    assert_eq!(imported.generation_id().unwrap(), id);
    assert_eq!(
        imported.scan(None, None, 8).unwrap(),
        source.scan(None, None, 8).unwrap()
    );
}
