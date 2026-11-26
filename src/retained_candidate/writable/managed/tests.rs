use super::*;
use crate::record::Operation;
use crate::retained_base_v2::Limits;
use crate::retained_candidate::writable::encode_batch;
use crate::retained_candidate::{IndexLimits, OverlayLimits, RetainedSuffixUsage, SuffixLimits};
use crate::{RetainedExportLimits, Store};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

pub(super) fn catalog_usage(root: &Path) -> CatalogStorageLimits {
    let mut total = CatalogStorageLimits {
        bytes: 0,
        entries: 0,
    };
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        total.entries += 1;
        if entry.file_type().unwrap().is_dir() {
            let child = catalog_usage(&entry.path());
            total.bytes += child.bytes;
            total.entries += child.entries;
        } else {
            total.bytes += entry.metadata().unwrap().len();
        }
    }
    total
}

#[test]
fn initial_catalog_quota_covers_candidate_and_bound_backup() {
    let f = Fixture::new(LIMITS);
    let candidate = f.tmp.path().join("candidate");
    let destination = f.tmp.path().join("quota");
    let mut exact = catalog_usage(&f.root);
    exact.bytes += (super::super::QUOTA_HEADER_BYTES - super::super::MANAGED_HEADER_BYTES) as u64;
    let create = |storage| {
        ManagedRetainedStore::create_with_storage_limits(
            &candidate,
            &destination,
            LIMITS,
            storage,
            &f.expected,
        )
    };
    for (limit, message) in [
        (
            CatalogStorageLimits {
                bytes: exact.bytes - 1,
                ..exact
            },
            "catalog byte limit exceeded",
        ),
        (
            CatalogStorageLimits {
                entries: exact.entries - 1,
                ..exact
            },
            "catalog entry limit exceeded",
        ),
    ] {
        assert!(
            matches!(create(limit), Err(PublicationFailure::NotPublished(error)) if error.to_string() == message)
        );
        assert!(!destination.exists());
    }
    create(exact).unwrap();
    assert_eq!(catalog_usage(&destination), exact);
    let mut store = ManagedRetainedStore::open(&destination, LIMITS).unwrap();
    assert_eq!(store.store.header.storage_limits(), Some(exact));
    assert!(matches!(
        store.put(b"b", b"new"),
        Err(MutationFailure::Rejected(_))
    ));
    assert_eq!(
        store.get(b"a", 8).unwrap().as_deref(),
        Some(b"old".as_slice())
    );

    let backup = f.tmp.path().join("backup");
    let mut source = f.open();
    source.put(b"a", b"new").unwrap();
    let binding = source.backup_to(&backup).unwrap();
    assert!(binding.end > 0);
    let backup_before = catalog_usage(&backup);
    let restored = f.tmp.path().join("restored");
    exact.bytes += binding.end;
    let restore = |storage| {
        ManagedRetainedStore::restore_backup_with_storage_limits(
            &backup, &restored, LIMITS, storage, &binding,
        )
    };
    assert!(
        matches!(restore(CatalogStorageLimits { bytes: exact.bytes - 1, ..exact }),
        Err(PublicationFailure::NotPublished(error)) if error.to_string() == "catalog byte limit exceeded")
    );
    assert!(!restored.exists());
    restore(exact).unwrap();
    assert_eq!(catalog_usage(&restored), exact);
    let store = ManagedRetainedStore::open(&restored, LIMITS).unwrap();
    assert_eq!(store.store.header.storage_limits(), Some(exact));
    assert_eq!(
        store.get(b"a", 8).unwrap().as_deref(),
        Some(b"new".as_slice())
    );
    assert_eq!(catalog_usage(&backup), backup_before);
}

#[test]
fn initial_catalog_quota_copy_is_effective_after_uncertain_publication() {
    let f = Fixture::new(LIMITS);
    let source = f.tmp.path().join("source");
    let destination = f.tmp.path().join("copy");
    let policy = CatalogStorageLimits {
        bytes: 1024,
        entries: 8,
    };
    let before = fs::read(source.join("records.log")).unwrap();
    let witness = fs::read(source.join("commit.state")).unwrap();
    let result = ManagedRetainedStore::copy_from_store_role(
        Store::open(&source).unwrap(),
        &source,
        &destination,
        EXPORT_LIMITS,
        InitialLimits::with_storage(LIMITS, policy),
        CATALOG_MAGIC,
        |_| Err(io::Error::other("injected parent sync failure")),
    );
    assert!(
        matches!(result, Err(PublicationFailure::OutcomeUnknown(error)) if error.to_string() == "injected parent sync failure")
    );
    let store = ManagedRetainedStore::open(&destination, LIMITS).unwrap();
    assert_eq!(store.store.header.storage_limits(), Some(policy));
    assert_eq!(
        store.get(b"z", 8).unwrap().as_deref(),
        Some(b"kept".as_slice())
    );
    assert_eq!(fs::read(source.join("records.log")).unwrap(), before);
    assert_eq!(fs::read(source.join("commit.state")).unwrap(), witness);
}

#[test]
fn persisted_catalog_quota_survives_selection_uncertainty_and_later_rotation() {
    let f = Fixture::new(LIMITS);
    let old = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let used = catalog_usage(&f.root);
    let policy = CatalogStorageLimits {
        bytes: used.bytes + 1024,
        entries: used.entries + 8,
    };
    let mut store = f.open();
    assert!(matches!(
        store.compact_with_persisted_storage_limits(used),
        Err(PublicationFailure::NotPublished(_))
    ));
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), old);
    assert!(!Slot::B.path(&f.root).exists());
    assert!(matches!(
        store.compact_using_limits(
            LIMITS,
            Some(policy),
            || Ok(()),
            |_| Err(io::Error::other("injected selection sync failure"))
        ),
        Err(PublicationFailure::OutcomeUnknown(_))
    ));
    assert!(store.get(b"a", 4).is_err());
    drop(store);

    let mut store = f.open();
    assert_eq!(store.store.header.storage_limits(), Some(policy));
    assert_eq!(store.get(b"a", 4).unwrap().unwrap(), b"old");
    assert!(store.reclaim_inactive().unwrap());
    let generation = store.compact().unwrap();
    assert_eq!(store.store.header.storage_limits(), Some(policy));
    assert!(store.reclaim_inactive().unwrap());
    let log = store.active.slot.path(&f.root).join("records.log");
    let witness = log.with_file_name("commit.state");
    let before = fs::read(&log).unwrap();
    let committed = fs::read(&witness).unwrap();
    drop(store);
    let mut store = ManagedRetainedStore::open_with_storage_limits(
        &f.root,
        LIMITS,
        CatalogStorageLimits {
            bytes: 64 * 1024,
            entries: 100,
        },
        None,
    )
    .unwrap();
    assert_eq!(store.generation_id().unwrap(), generation);
    let value = [7; 1024];
    assert!(
        matches!(store.put(b"too-large", &value), Err(MutationFailure::Rejected(ref cause))
        if cause.to_string() == "catalog byte limit exceeded")
    );
    assert_eq!(fs::read(&log).unwrap(), before);
    assert_eq!(fs::read(&witness).unwrap(), committed);
    drop(store);
    let tail = record::encode(Operation::Put, b"too-large", &value).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(&tail)
        .unwrap();
    let error = ManagedRetainedStore::open(&f.root, LIMITS).err().unwrap();
    assert_eq!(error.to_string(), "catalog byte limit exceeded");
    assert_eq!(fs::read(&witness).unwrap(), committed);
}

#[test]
fn catalog_storage_caps_appends_and_rejects_oversize_recovery_before_adoption() {
    let f = Fixture::new(LIMITS);
    let used = catalog_usage(&f.root);
    let record = record::encode(Operation::Put, b"b", b"exact").unwrap();
    let limit = CatalogStorageLimits {
        bytes: used.bytes + record.len() as u64,
        ..used
    };
    let mut store =
        ManagedRetainedStore::open_with_storage_limits(&f.root, LIMITS, limit, None).unwrap();
    store.put(b"b", b"exact").unwrap();
    let slot = Slot::A.path(&f.root);
    let log = slot.join("records.log");
    let witness = slot.join("commit.state");
    let accepted = fs::read(&log).unwrap();
    let committed = fs::read(&witness).unwrap();
    assert_eq!(catalog_usage(&f.root), limit);
    assert!(
        matches!(store.delete(b"a"), Err(MutationFailure::Rejected(ref e))
        if e.to_string() == "catalog byte limit exceeded")
    );
    assert_eq!(fs::read(&log).unwrap(), accepted);
    assert_eq!(fs::read(&witness).unwrap(), committed);
    drop(store);
    let tail = record::encode(Operation::Put, b"a", b"recover").unwrap();
    OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(&tail)
        .unwrap();
    let error = ManagedRetainedStore::open_with_storage_limits(&f.root, LIMITS, limit, None)
        .err()
        .unwrap();
    assert_eq!(error.to_string(), "catalog byte limit exceeded");
    assert_eq!(fs::read(&witness).unwrap(), committed);
    let suffix_bytes = (record.len() + tail.len()) as u64;
    let pending_log = [accepted, tail].concat();
    assert_eq!(fs::read(&log).unwrap(), pending_log);
    let selection = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let open = |cap, storage| {
        ManagedRetainedStore::open_with_suffix_scan_limit(&f.root, LIMITS, cap, Some(storage))
    };
    let exact_storage = catalog_usage(&f.root);
    let error = open(2 * suffix_bytes - 1, exact_storage).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "retained recovery suffix scan byte limit exceeded"
    );
    assert_eq!(
        open(2 * suffix_bytes, limit).err().unwrap().to_string(),
        "catalog byte limit exceeded"
    );
    assert_eq!(fs::read(&witness).unwrap(), committed);
    assert_eq!(fs::read(&log).unwrap(), pending_log);
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selection);
    let base_bytes = fs::metadata(slot.join("base")).unwrap().len();
    let open_combined = |cap| {
        ManagedRetainedStore::open_with_recovery_scan_limit(
            &f.root,
            LIMITS,
            cap,
            Some(exact_storage),
        )
    };
    let cap = base_bytes + 2 * suffix_bytes;
    assert_eq!(
        open_combined(cap - 1).err().unwrap().to_string(),
        "retained recovery scan byte limit exceeded"
    );
    assert_eq!(fs::read(&witness).unwrap(), committed);
    assert_eq!(fs::read(&log).unwrap(), pending_log);
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selection);
    let reopened = open_combined(cap).unwrap();
    assert_eq!(reopened.get(b"a", 7).unwrap().unwrap(), b"recover");
    assert_ne!(fs::read(&witness).unwrap(), committed);
    drop(reopened);
    assert!(open(suffix_bytes, exact_storage).is_ok());
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selection);
}

#[test]
fn catalog_quota_accepts_deep_tree_under_descriptor_limit() {
    use std::process::Command;

    let f = Fixture::new(LIMITS);
    let used = catalog_usage(&f.root);
    let mut deep = f.root.join("quota-fixture");
    const DEPTH: u64 = 64;
    for _ in 0..DEPTH {
        fs::create_dir(&deep).unwrap();
        deep.push("d");
    }
    let payload = deep.parent().unwrap().join("payload");
    fs::write(&payload, b"data").unwrap();
    fs::hard_link(&payload, f.root.join("quota-hard-link")).unwrap();
    let growth = record::encode(Operation::Put, b"b", b"exact")
        .unwrap()
        .len() as u64;
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "retained_candidate::writable::managed::tests::catalog_quota_descriptor_limit_child",
            "--nocapture",
        ])
        .env("EMDA_CATALOG_DESCRIPTOR_ROOT", &f.root)
        .env(
            "EMDA_CATALOG_DESCRIPTOR_BYTES",
            (used.bytes + 8 + growth).to_string(),
        )
        .env(
            "EMDA_CATALOG_DESCRIPTOR_ENTRIES",
            (used.entries + DEPTH + 2).to_string(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("catalog quota reopen succeeded"));
    assert_eq!(fs::read(payload).unwrap(), b"data");
}

#[test]
fn catalog_quota_descriptor_limit_child() {
    use rustix::process::{Resource, getrlimit, setrlimit};

    let Some(root) = std::env::var_os("EMDA_CATALOG_DESCRIPTOR_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let limits = CatalogStorageLimits {
        bytes: std::env::var("EMDA_CATALOG_DESCRIPTOR_BYTES")
            .unwrap()
            .parse()
            .unwrap(),
        entries: std::env::var("EMDA_CATALOG_DESCRIPTOR_ENTRIES")
            .unwrap()
            .parse()
            .unwrap(),
    };
    let mut descriptors = getrlimit(Resource::Nofile);
    descriptors.current = Some(descriptors.current.unwrap_or(32).min(32));
    setrlimit(Resource::Nofile, descriptors).unwrap();

    let mut store =
        ManagedRetainedStore::open_with_storage_limits(&root, LIMITS, limits, None).unwrap();
    assert_eq!(
        store.get(b"a", 8).unwrap().as_deref(),
        Some(b"old".as_slice())
    );
    store.put(b"b", b"exact").unwrap();
    assert!(matches!(
        store.put(b"c", b"over quota"),
        Err(MutationFailure::Rejected(error)) if error.to_string() == "catalog byte limit exceeded"
    ));
    drop(store);
    let error = ManagedRetainedStore::open_with_storage_limits(
        &root,
        LIMITS,
        CatalogStorageLimits {
            entries: limits.entries - 1,
            ..limits
        },
        None,
    )
    .err()
    .unwrap();
    assert_eq!(error.to_string(), "catalog entry limit exceeded");
    let reopened =
        ManagedRetainedStore::open_with_storage_limits(&root, LIMITS, limits, None).unwrap();
    assert_eq!(
        reopened.get(b"b", 8).unwrap().as_deref(),
        Some(b"exact".as_slice())
    );
    println!("catalog quota reopen succeeded");
}

#[test]
fn catalog_storage_charges_abandoned_stages_until_cleanup() {
    use crate::staging::Stage;
    let f = Fixture::new(LIMITS);
    let used = catalog_usage(&f.root);
    let value = [1; 32];
    let growth = record::encode(Operation::Put, b"b", &value).unwrap().len() as u64;
    let (stage, mut file) = Stage::file(&f.root).unwrap();
    file.write_all(&vec![0; (growth - Stage::FILE_MARKER_BYTES) as usize])
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    stage.retain_payload().unwrap();
    let limit = CatalogStorageLimits {
        bytes: used.bytes + growth,
        entries: used.entries + 2,
    };
    assert_eq!(catalog_usage(&f.root), limit);
    let mut store =
        ManagedRetainedStore::open_with_storage_limits(&f.root, LIMITS, limit, None).unwrap();
    assert!(
        matches!(store.put(b"b", &value), Err(MutationFailure::Rejected(ref e))
        if e.to_string() == "catalog byte limit exceeded")
    );
    drop(store);
    let mut store = ManagedRetainedStore::open_with_storage_limits(
        &f.root,
        LIMITS,
        limit,
        Some(NonZeroU64::MAX),
    )
    .unwrap();
    assert_eq!(catalog_usage(&f.root), used);
    store.put(b"b", &value).unwrap();
    assert_eq!(store.get(b"b", value.len()).unwrap(), Some(value.to_vec()));
}

#[test]
fn catalog_storage_reserves_successor_and_selection_stages() {
    use crate::staging::Stage;
    let f = Fixture::new(LIMITS);
    let used = catalog_usage(&f.root);
    let original = Slot::A.path(&f.root);
    let base_bytes = fs::metadata(original.join("base")).unwrap().len();
    let overhead = (super::super::MANAGED_HEADER_BYTES + super::super::WITNESS_BYTES + ACTIVE_BYTES)
        as u64
        + Stage::DIRECTORY_MARKER_BYTES
        + Stage::FILE_MARKER_BYTES;
    let exact = CatalogStorageLimits {
        bytes: used.bytes + base_bytes + overhead,
        entries: used.entries + 8,
    };
    let active = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    for (limit, message) in [
        (
            CatalogStorageLimits {
                bytes: exact.bytes - 1,
                ..exact
            },
            "compacted base byte limit exceeded",
        ),
        (
            CatalogStorageLimits {
                entries: exact.entries - 1,
                ..exact
            },
            "catalog entry limit exceeded",
        ),
    ] {
        let mut store =
            ManagedRetainedStore::open_with_storage_limits(&f.root, LIMITS, limit, None).unwrap();
        assert!(
            matches!(store.compact(), Err(PublicationFailure::NotPublished(ref e)) if e.to_string() == message)
        );
        assert_eq!(catalog_usage(&f.root), used);
        assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), active);
        assert!(!Slot::B.path(&f.root).exists());
    }
    let mut store =
        ManagedRetainedStore::open_with_storage_limits(&f.root, LIMITS, exact, None).unwrap();
    let new = store.compact().unwrap();
    assert!(original.exists());
    assert!(store.reclaim_inactive().unwrap());
    assert!(!original.exists());
    drop(store);
    let reopened =
        ManagedRetainedStore::open_with_storage_limits(&f.root, LIMITS, exact, None).unwrap();
    assert_eq!(reopened.generation_id().unwrap(), new);
    assert_eq!(reopened.get(b"a", 4).unwrap().unwrap(), b"old");
}

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
    assert_eq!(expected.end, store.store.suffix_len as u64);
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
fn managed_backup_and_restore_charge_copy_and_verification_before_staging() {
    let f = Fixture::new(LIMITS);
    let backup = f.tmp.path().join("backup");
    let restored = f.tmp.path().join("restored");
    let mut store = f.open();
    store.put(b"b", b"captured").unwrap();
    let active_path = store.active.slot.path(&f.root);
    let base_path = active_path.join("base");
    let base = fs::read(&base_path).unwrap();
    let log = fs::read(active_path.join("records.log")).unwrap();
    let witness = fs::read(active_path.join("commit.state")).unwrap();
    let cap = 2 * (base.len() as u64 + store.store.suffix_len as u64);

    let mut corrupt = base.clone();
    corrupt[base.len() - 33] ^= 1;
    fs::write(&base_path, corrupt).unwrap();
    assert!(matches!(
        store.backup_to_with_scan_limit(&backup, cap - 1),
        Err(crate::SuffixExportFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::InvalidInput
                && error.to_string() == "retained backup scan byte limit exceeded"
    ));
    assert!(matches!(
        store.backup_to_with_scan_limit(&backup, cap),
        Err(crate::SuffixExportFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::InvalidData
    ));
    assert!(!backup.exists());
    fs::write(&base_path, &base).unwrap();
    let binding = store.backup_to_with_scan_limit(&backup, cap).unwrap();

    let suffix_path = backup.join("suffix");
    let suffix = fs::read(&suffix_path).unwrap();
    let mut corrupt = suffix.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&suffix_path, corrupt).unwrap();
    let restore = |limit| {
        ManagedRetainedStore::restore_backup_with_scan_limit(
            &backup, &restored, LIMITS, limit, None, &binding,
        )
    };
    assert!(matches!(
        restore(cap - 1),
        Err(PublicationFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::InvalidInput
                && error.to_string() == "retained restore scan byte limit exceeded"
    ));
    assert!(matches!(
        restore(cap),
        Err(PublicationFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::InvalidData
    ));
    assert!(!restored.exists());
    fs::write(&suffix_path, &suffix).unwrap();
    let mut oversized = binding.clone();
    oversized.end = u64::MAX;
    assert!(matches!(
        ManagedRetainedStore::restore_backup_with_scan_limit(
            &backup, &restored, LIMITS, u64::MAX, None, &oversized,
        ),
        Err(PublicationFailure::NotPublished(error))
            if error.to_string() == "retained restore scan byte limit exceeded"
    ));
    let quota = CatalogStorageLimits {
        bytes: 2048,
        entries: 8,
    };
    let id = ManagedRetainedStore::restore_backup_with_scan_limit(
        &backup,
        &restored,
        LIMITS,
        cap,
        Some(quota),
        &binding,
    )
    .unwrap();
    let recovered = ManagedRetainedStore::open(&restored, LIMITS).unwrap();
    assert_eq!(recovered.generation_id().unwrap(), id);
    assert_eq!(recovered.store.header.storage_limits(), Some(quota));
    assert_eq!(
        recovered.scan(None, None, 8).unwrap(),
        store.scan(None, None, 8).unwrap()
    );
    assert_eq!(fs::read(&suffix_path).unwrap(), suffix);
    assert_eq!(fs::read(backup.join("candidate/base")).unwrap(), base);
    assert_eq!(fs::read(active_path.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(active_path.join("commit.state")).unwrap(), witness);
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
fn managed_restore_streams_large_suffix_and_replays_before_publication() {
    let limits = RetainedStoreLimits {
        suffix: SuffixLimits {
            bytes: 2 * 1024 * 1024,
            ..LIMITS.suffix
        },
        ..LIMITS
    };
    let f = Fixture::new(limits);
    let mut store = ManagedRetainedStore::open(&f.root, limits).unwrap();
    let old = vec![0x41; 640 * 1024];
    let live = vec![0x42; 640 * 1024 + 3];
    store.put(b"b", &old).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"b",
                value: &live,
            },
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put {
                key: b"d",
                value: b"",
            },
        ])
        .unwrap();
    let backup = f.tmp.path().join("large-backup");
    let expected = store.backup_to(&backup).unwrap();
    let suffix = backup.join("suffix");
    let original = fs::read(&suffix).unwrap();
    let destination = f.tmp.path().join("exact-restore");
    let exact = RetainedStoreLimits {
        suffix: SuffixLimits {
            bytes: expected.end,
            operations: 4,
            ..limits.suffix
        },
        ..limits
    };
    let too_few_operations = RetainedStoreLimits {
        suffix: SuffixLimits {
            operations: 3,
            ..exact.suffix
        },
        ..exact
    };
    assert!(matches!(
        ManagedRetainedStore::restore_backup(&backup, &destination, too_few_operations, &expected),
        Err(PublicationFailure::NotPublished(ref cause))
            if cause.kind() == io::ErrorKind::InvalidInput && cause.to_string().contains("suffix operation limit")
    ));
    let mut malformed = original.clone();
    *malformed.last_mut().unwrap() ^= 1;
    let malformed_binding = SuffixExpected {
        digest: Sha256::digest(&malformed).into(),
        ..expected.clone()
    };
    fs::write(&suffix, &malformed).unwrap();
    assert!(matches!(
        ManagedRetainedStore::restore_backup(&backup, &destination, exact, &malformed_binding),
        Err(PublicationFailure::NotPublished(ref cause))
            if cause.kind() == io::ErrorKind::InvalidData && cause.to_string() == "record checksum mismatch"
    ));
    assert_eq!(fs::read(&suffix).unwrap(), malformed);
    assert!(!destination.exists());
    fs::write(&suffix, &original).unwrap();
    let id = ManagedRetainedStore::restore_backup(&backup, &destination, exact, &expected).unwrap();
    let installed = ManagedRetainedStore::open(&destination, exact).unwrap();
    assert_eq!(installed.generation_id().unwrap(), id);
    assert_ne!(id, store.generation_id().unwrap());
    assert_eq!(
        installed.scan(None, None, 4).unwrap(),
        store.scan(None, None, 4).unwrap()
    );
    assert_eq!(installed.get(b"b", live.len()).unwrap(), Some(live));
    assert_eq!(fs::read(&suffix).unwrap(), original);
}

#[test]
fn managed_backup_uncertainty_returns_the_verified_binding() {
    let f = Fixture::new(LIMITS);
    let backup = f.tmp.path().join("backup");
    let restored = f.tmp.path().join("restored");
    let mut store = f.open();
    store.put(b"b", b"captured").unwrap();
    let error = store
        .backup_using(&backup, None, |_| Err(io::Error::other("sync interrupted")))
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
fn concurrent_managed_backups_use_independent_source_read_positions() {
    let tmp = tempfile::tempdir().unwrap();
    let source_path = tmp.path().join("source");
    let root = tmp.path().join("catalog");
    let limits = RetainedStoreLimits {
        base: Limits {
            file_bytes: 2 * 1024 * 1024,
            work_bytes: 2 * 1024 * 1024 + 1,
            ..LIMITS.base
        },
        ..LIMITS
    };
    let source = Store::open(&source_path).unwrap();
    let value = vec![0x78; 1024 * 1024];
    source.put(b"a", &value).unwrap();
    ManagedRetainedStore::copy_from_store(
        source,
        &source_path,
        &root,
        RetainedExportLimits {
            max_log_bytes: limits.base.file_bytes,
            max_output_bytes: limits.base.file_bytes,
            ..EXPORT_LIMITS
        },
        limits,
    )
    .unwrap();
    let mut store = ManagedRetainedStore::open(&root, limits).unwrap();
    store.put(b"b", b"captured").unwrap();
    let before = store.scan(None, None, 8).unwrap();

    let start = std::sync::Barrier::new(6);
    let backups = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..6)
            .map(|index| {
                let path = tmp.path().join(format!("backup-{index}"));
                let store = &store;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    let binding = store.backup_to(&path).unwrap();
                    (path, binding)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    for (index, (backup, binding)) in backups.iter().enumerate() {
        let destination = tmp.path().join(format!("restored-{index}"));
        ManagedRetainedStore::restore_backup(backup, &destination, limits, binding).unwrap();
        let recovered = ManagedRetainedStore::open(&destination, limits).unwrap();
        assert_eq!(recovered.scan(None, None, 8).unwrap(), before);
    }
    assert_eq!(store.scan(None, None, 8).unwrap(), before);
}

#[test]
fn managed_backup_rejects_corrupt_bytes_and_a_substituted_base_path() {
    let f = Fixture::new(LIMITS);
    let store = f.open();
    let base = Slot::A.path(&f.root).join("base");
    let original = fs::read(&base).unwrap();
    let backup = f.tmp.path().join("backup");
    let mut altered = original.clone();
    let trailer = altered.len() - 32;
    altered[trailer - 1] ^= 1;
    let mut hash = Sha256::new();
    hash.update(b"EMDA retained base v2\0");
    hash.update(&altered[..trailer]);
    altered[trailer..].copy_from_slice(&hash.finalize());
    fs::write(&base, &altered).unwrap();
    assert!(matches!(
        store.backup_to(&backup),
        Err(crate::SuffixExportFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    assert!(!backup.exists());
    assert_eq!(fs::read(&base).unwrap(), altered);
    fs::write(&base, &original).unwrap();

    let hidden = f.tmp.path().join("original-base");
    fs::rename(&base, &hidden).unwrap();
    fs::write(&base, &original).unwrap();
    assert!(matches!(
        store.backup_to(&backup),
        Err(crate::SuffixExportFailure::NotPublished(error)) if error.to_string() == "retained base path or length changed"
    ));
    assert!(!backup.exists());
    fs::rename(&hidden, &base).unwrap();
    store.backup_to(&backup).unwrap();
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
fn sparse_rebase_sizes_touched_blocks_and_authenticates_skipped_blocks_before_publication() {
    let tmp = tempfile::tempdir().unwrap();
    let source = Store::open(tmp.path().join("source")).unwrap();
    let keys = [b"m0".as_slice(), b"m1", b"n", b"p", b"z"];
    let sizes = [32, 32, 96, 96, 160].map(|kib| kib * 1024);
    let data: Vec<_> = sizes
        .into_iter()
        .enumerate()
        .map(|(i, size)| vec![1 + i as u8; size])
        .collect();
    for (&key, value) in keys.iter().zip(&data) {
        source.put(key, value).unwrap();
    }
    let candidate = tmp.path().join("candidate");
    let exported = source
        .build_retained_candidate(
            &candidate,
            [3; 16],
            [4; 16],
            RetainedExportLimits {
                max_log_bytes: 1024 * 1024,
                max_output_bytes: 1024 * 1024,
                ..EXPORT_LIMITS
            },
        )
        .unwrap();
    let limits = RetainedStoreLimits {
        base: Limits {
            file_bytes: 1024 * 1024,
            work_bytes: 1024 * 1024 + 1,
            ..LIMITS.base
        },
        ..LIMITS
    };
    let root = tmp.path().join("catalog");
    ManagedRetainedStore::create(&candidate, &root, limits, &exported).unwrap();
    let mut store = ManagedRetainedStore::open(&root, limits).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"earlier",
            },
            BatchOperation::Put {
                key: b"n0",
                value: b"between",
            },
            BatchOperation::Put {
                key: b"zz",
                value: b"later",
            },
            BatchOperation::Put {
                key: b"m0",
                value: b"replaced",
            },
            BatchOperation::Delete { key: b"m1" },
        ])
        .unwrap();
    let state = store.scan(None, None, 32).unwrap();
    let expected = [
        (b"a".as_slice(), b"earlier".as_slice()),
        (b"m0", b"replaced"),
        (b"n", &data[2]),
        (b"n0", b"between"),
        (b"p", &data[3]),
        (b"z", &data[4]),
        (b"zz", b"later"),
    ];
    assert!(
        state
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .eq(expected)
    );
    let encoded: u64 = state
        .iter()
        .map(|(key, value)| crate::retained_base_v2::encoded_entry_bytes(key, value.len()))
        .sum();
    let original_id = store.generation_id().unwrap();
    let base = Slot::A.path(&root).join("base");
    let file = OpenOptions::new().write(true).open(&base).unwrap();
    let header_bytes = crate::retained_base_v2::FRAME_BYTES - 32;
    let offset = header_bytes
        + keys[..3]
            .iter()
            .zip(sizes)
            .map(|(key, size)| crate::retained_base_v2::encoded_entry_bytes(key, size))
            .sum::<u64>()
        + 8
        + keys[3].len() as u64;
    file.write_all_at(&[0], offset).unwrap();
    let inner = &store.store;
    let peaks = inner
        .overlay
        .live_peaks(
            &inner.base,
            inner.suffix_file(),
            inner.limits.suffix,
            inner.hash.clone().finalize().into(),
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(peaks.encoded_bytes, encoded);
    assert_eq!((peaks.entries, peaks.key_bytes), (7, 10));
    assert_eq!((peaks.peak_entries, peaks.peak_key_bytes), (8, 12));
    assert!(
        matches!(store.compact(), Err(PublicationFailure::NotPublished(error))
            if error.to_string() == "retained base block digest mismatch")
    );
    assert!(!Slot::B.path(&root).exists());
    assert_eq!(store.generation_id().unwrap(), original_id);
    file.write_all_at(&data[3][..1], offset).unwrap();
    let generation = store.compact().unwrap();
    drop(store);
    let reopened = ManagedRetainedStore::open(&root, limits).unwrap();
    let metadata = reopened.store.base.metadata();
    assert_eq!(
        metadata.file_bytes,
        encoded + crate::retained_base_v2::FRAME_BYTES
    );
    assert_eq!((metadata.peak_entries, metadata.peak_key_bytes), (8, 12));
    assert_eq!(reopened.generation_id().unwrap(), generation);
    assert_eq!(reopened.scan(None, None, 32).unwrap(), state);
}

#[test]
fn streamed_rebase_peak_scan_rejects_rechecksummed_superseded_values() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let batch = |value| {
        [
            BatchOperation::Put { key: b"b", value },
            BatchOperation::Delete { key: b"a" },
        ]
    };
    store.write_batch(&batch(b"initial")).unwrap();
    store.put(b"b", b"current").unwrap();
    let inner = &store.store;
    let expected = inner.hash.clone().finalize().into();
    let peaks = || {
        inner.overlay.live_peaks(
            &inner.base,
            inner.suffix_file(),
            inner.limits.suffix,
            expected,
            |_| Ok(()),
        )
    };
    assert_eq!(peaks().unwrap().entries, 2);
    let generation = Slot::A.path(&f.root);
    let file = OpenOptions::new()
        .write(true)
        .open(generation.join("records.log"))
        .unwrap();
    let witness = fs::read(generation.join("commit.state")).unwrap();
    let altered = encode_batch(&batch(b"changed")).unwrap();
    file.write_all_at(&altered, inner.header.len as u64)
        .unwrap();
    assert_eq!(
        store.get(b"b", 7).unwrap().as_deref(),
        Some(b"current".as_slice())
    );
    assert_eq!(
        peaks().err().unwrap().to_string(),
        "retained suffix changed during rebase"
    );
    assert_eq!(fs::read(generation.join("commit.state")).unwrap(), witness);
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
fn rebase_streams_large_values_and_overlay_at_exact_byte_limits() {
    let tmp = tempfile::tempdir().unwrap();
    let large = vec![0x3c; 512 * 1024 + 3];
    let updated = vec![0x75; 256 * 1024 + 1];
    let source = Store::open(tmp.path().join("source")).unwrap();
    source.put(b"b", &large).unwrap();
    source.put(b"d", &large).unwrap();
    source.put(b"f", b"old").unwrap();
    let candidate = tmp.path().join("candidate");
    let expected = source
        .build_retained_candidate(
            &candidate,
            [3; 16],
            [4; 16],
            RetainedExportLimits {
                max_log_bytes: 2 * 1024 * 1024,
                max_output_bytes: 2 * 1024 * 1024,
                ..EXPORT_LIMITS
            },
        )
        .unwrap();
    let limits = RetainedStoreLimits {
        base: Limits {
            file_bytes: 2 * 1024 * 1024,
            work_bytes: 2 * 1024 * 1024 + 1,
            ..LIMITS.base
        },
        index: IndexLimits {
            blocks: 8,
            ..LIMITS.index
        },
        suffix: SuffixLimits {
            bytes: 512 * 1024,
            ..LIMITS.suffix
        },
        ..LIMITS
    };
    let root = tmp.path().join("catalog");
    let old = ManagedRetainedStore::create(&candidate, &root, limits, &expected).unwrap();
    let mut store = ManagedRetainedStore::open(&root, limits).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"first",
            },
            BatchOperation::Delete { key: b"d" },
            BatchOperation::Put {
                key: b"e",
                value: b"",
            },
            BatchOperation::Put {
                key: b"f",
                value: &updated,
            },
            BatchOperation::Put {
                key: b"z",
                value: b"last",
            },
        ])
        .unwrap();
    let model = vec![
        (b"a".to_vec(), b"first".to_vec()),
        (b"b".to_vec(), large),
        (b"e".to_vec(), vec![]),
        (b"f".to_vec(), updated),
        (b"z".to_vec(), b"last".to_vec()),
    ];
    let bytes = 144
        + model
            .iter()
            .map(|(k, v)| (8 + k.len() + v.len()) as u64)
            .sum::<u64>();
    let exact = RetainedStoreLimits {
        base: Limits {
            file_bytes: bytes,
            work_bytes: bytes + 1,
            ..limits.base
        },
        ..limits
    };
    for base in [
        Limits {
            file_bytes: bytes - 1,
            ..exact.base
        },
        Limits {
            work_bytes: bytes,
            ..exact.base
        },
    ] {
        let err = store
            .compact_with_limits(RetainedStoreLimits { base, ..exact })
            .unwrap_err();
        assert!(matches!(err, PublicationFailure::NotPublished(ref cause)
            if cause.kind() == io::ErrorKind::InvalidInput
                && cause.to_string() == "compacted base byte limit exceeded"));
        assert_eq!(store.generation_id().unwrap(), old);
        assert!(!Slot::B.path(&root).exists());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 4);
        assert_eq!(store.scan(None, None, 8).unwrap(), model);
    }
    let next = store.compact_with_limits(exact).unwrap();
    assert_ne!(next, old);
    assert_eq!(
        fs::metadata(Slot::B.path(&root).join("base"))
            .unwrap()
            .len(),
        bytes
    );
    assert!(store.reclaim_inactive().unwrap());
    drop(store);
    let reopened = ManagedRetainedStore::open(&root, exact).unwrap();
    assert_eq!(reopened.generation_id().unwrap(), next);
    assert_eq!(reopened.scan(None, None, 8).unwrap(), model);
}

#[test]
fn rebase_stream_authenticates_base_values_hidden_by_tombstones() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.delete(b"a").unwrap();
    let base = OpenOptions::new()
        .write(true)
        .open(Slot::A.path(&f.root).join("base"))
        .unwrap();
    // Change the first value byte of the deleted key without changing length.
    base.write_all_at(b"X", 112 + 8 + 1).unwrap();
    let stream = store.store.overlay.visit_merged(
        &store.store.base,
        OverlaySource::Disk(store.store.suffix_file()),
        |_| Ok(()),
    );
    let error = stream.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "retained base block digest mismatch");
    assert!(matches!(
        store.compact(),
        Err(PublicationFailure::NotPublished(ref error))
            if error.kind() == io::ErrorKind::InvalidData
    ));
    assert!(!Slot::B.path(&f.root).exists());
}

#[test]
fn rebase_rejects_a_changed_base_header_or_trailer_before_selection() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.put(b"b", b"suffix").unwrap();
    let generation = Slot::A.path(&f.root);
    let path = generation.join("base");
    let bytes = fs::read(&path).unwrap();
    let saved = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let witness = fs::read(generation.join("commit.state")).unwrap();
    let file = OpenOptions::new().write(true).open(path).unwrap();
    for (offset, message) in [
        (48, "retained base header changed"),
        (bytes.len() - 1, "retained base trailer changed"),
    ] {
        file.write_all_at(&[bytes[offset] ^ 1], offset as u64)
            .unwrap();
        assert!(
            matches!(store.compact(), Err(PublicationFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::InvalidData && error.to_string() == message)
        );
        assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), saved);
        assert_eq!(fs::read(generation.join("commit.state")).unwrap(), witness);
        assert!(!Slot::B.path(&f.root).exists());
        assert_eq!(fs::read_dir(&f.root).unwrap().count(), 4);
        file.write_all_at(&[bytes[offset]], offset as u64).unwrap();
    }
    let successor = store.compact().unwrap();
    drop(store);
    let reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), successor);
    assert_eq!(
        reopened.get(b"b", 6).unwrap().as_deref(),
        Some(b"suffix".as_slice())
    );
}

#[test]
fn successor_open_rejects_a_rechecksummed_staged_base() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.put(b"b", b"pending").unwrap();
    let source_state = store.scan(None, None, 32).unwrap();
    let selected = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let stage = crate::staging::Stage::directory(&f.root).unwrap();
    let path = stage.path().join("base");
    let expected = store
        .store
        .write_rebased_base(&path, LIMITS, u64::MAX, &mut None)
        .unwrap();
    let mut bytes = fs::read(&path).unwrap();
    let trailer = bytes.len() - 32;
    bytes[trailer - 1] ^= 1;
    let mut hash = Sha256::new();
    hash.update(b"EMDA retained base v2\0");
    hash.update(&bytes[..trailer]);
    bytes[trailer..].copy_from_slice(&hash.finalize());
    fs::write(&path, &bytes).unwrap();
    let valid = crate::retained_base_v2::validate(bytes.as_slice(), LIMITS.base).unwrap();
    assert_ne!(valid.digest, expected.base_digest);

    let error = RetainedStore::initialize(
        stage.path(),
        LIMITS,
        &expected,
        None,
        generation_role(store.catalog_magic, store.active.catalog),
        None,
        None,
    )
    .err()
    .expect("the staged base must match the writer's digest");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    drop(stage);
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selected);
    assert!(!Slot::B.path(&f.root).exists());
    assert_eq!(store.scan(None, None, 32).unwrap(), source_state);

    let next = store.compact().unwrap();
    drop(store);
    let reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), next);
    assert_eq!(reopened.scan(None, None, 32).unwrap(), source_state);
}

#[test]
fn rebase_admits_the_successor_index_before_writing_disallowed_blocks() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let path = f.tmp.path().join("rebased");
    let selection = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let with_index = |blocks, anchor_bytes| RetainedStoreLimits {
        index: IndexLimits {
            blocks,
            anchor_bytes,
        },
        ..LIMITS
    };

    let error = store
        .store
        .write_rebased_base(&path, with_index(0, 64), u64::MAX, &mut None)
        .unwrap_err();
    assert_eq!(error.to_string(), "retained base block limit exceeded");
    assert!(!path.exists());

    // A single anchor is possible within the lower bound of one byte,
    // but the first key of the merged state is longer than that.
    let anchor = b"0-new";
    store.put(anchor, &[b'n'; 512]).unwrap();
    let state = store.scan(None, None, 32).unwrap();
    let error = store
        .store
        .write_rebased_base(&path, with_index(1, anchor.len() - 1), u64::MAX, &mut None)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "retained base anchor byte limit exceeded"
    );
    assert!(fs::metadata(&path).unwrap().len() < crate::retained_base_v2::FRAME_BYTES);
    assert!(matches!(
        store.compact_with_limits(with_index(1, anchor.len() - 1)),
        Err(PublicationFailure::NotPublished(error))
            if error.to_string() == "retained base anchor byte limit exceeded"
    ));
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selection);
    assert_eq!(fs::read_dir(&f.root).unwrap().count(), 4);
    assert_eq!(store.scan(None, None, 32).unwrap(), state);

    let successor = store
        .compact_with_limits(with_index(1, anchor.len()))
        .unwrap();
    drop(store);
    let reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), successor);
    assert_eq!(reopened.scan(None, None, 32).unwrap(), state);
}

#[test]
fn rebase_sizes_an_empty_successor_at_the_frame_limit() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.delete(b"a").unwrap();
    store.delete(b"z").unwrap();
    let frame_bytes = crate::retained_base_v2::FRAME_BYTES;
    let limits = RetainedStoreLimits {
        base: Limits {
            file_bytes: frame_bytes,
            work_bytes: frame_bytes + 1,
            ..LIMITS.base
        },
        index: IndexLimits {
            blocks: 0,
            anchor_bytes: 0,
        },
        ..LIMITS
    };
    let successor = store.compact_with_limits(limits).unwrap();
    let path = Slot::B.path(&f.root).join("base");
    assert_eq!(fs::metadata(&path).unwrap().len(), frame_bytes);
    let base =
        crate::retained_base_v2::validate(fs::File::open(path).unwrap(), limits.base).unwrap();
    assert_eq!(
        (
            base.entries,
            base.key_bytes,
            base.peak_entries,
            base.peak_key_bytes
        ),
        (0, 0, 2, 2)
    );
    drop(store);
    let reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), successor);
    assert!(reopened.scan(None, None, 8).unwrap().is_empty());
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
            None,
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
fn bounded_startup_admits_tail_adoption_and_retirement_together() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.compact().unwrap();
    let base = store.store.base.metadata().file_bytes;
    let slot = store.active.slot.path(&f.root);
    let retired = store.active.slot.other().path(&f.root);
    drop(store);

    let witness = slot.join("commit.state");
    let before = fs::read(&witness).unwrap();
    let log = slot.join("records.log");
    let record = record::encode(Operation::Put, b"pending", b"value").unwrap();
    let mut file = OpenOptions::new().append(true).open(&log).unwrap();
    file.write_all(&record).unwrap();
    file.sync_all().unwrap();
    let tail = fs::read(&log).unwrap();
    let selection = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let exact = 2 * base + 3 * record.len() as u64;
    let open = |bytes| {
        ManagedRetainedStore::open_with_bounded_startup(
            &f.root,
            LIMITS,
            RetainedStartupLimits {
                max_startup_scan_bytes: bytes,
                max_maintenance_scan_bytes: 0,
                suffix_threshold: NonZeroU64::MIN,
                storage_limits: None,
            },
        )
    };
    assert!(matches!(
        open(exact - 1),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
            && error.to_string() == "retained startup scan byte limit exceeded"
    ));
    assert_eq!(fs::read(&witness).unwrap(), before);
    assert_eq!(fs::read(&log).unwrap(), tail);
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selection);
    assert!(retired.exists());

    let mut reopened = open(exact).unwrap();
    assert_ne!(fs::read(&witness).unwrap(), before);
    assert!(!retired.exists());
    assert_eq!(reopened.get(b"pending", 8).unwrap().unwrap(), b"value");
    assert!(matches!(
        reopened.put(b"other", b"absent"),
        Err(MutationFailure::Rejected(error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert_eq!(fs::read(&log).unwrap(), tail);
    assert_eq!(reopened.get(b"other", 8).unwrap(), None);
}

#[test]
fn automatic_maintenance_scan_limit_is_shared_and_resets_per_mutation() {
    use crate::retained_base_v2::FRAME_BYTES;

    for short in [false, true] {
        let f = Fixture::new(LIMITS);
        let mut store = f.open();
        store.put(b"a", b"new").unwrap();
        let original = store.generation_id().unwrap();
        let base = store.store.base.metadata().file_bytes;
        let suffix = store.store.suffix_len as u64;
        // Equal-sized replacement at an existing key in a one-block base.
        let rotation = 2 * base + 2 * (base - FRAME_BYTES) + 2 * suffix + 3;
        store
            .enable_auto_reclaim_with_scan_limit(
                NonZeroU64::MIN,
                rotation + base - u64::from(short),
            )
            .unwrap();
        let result = store.put(b"a", b"yes");
        let successor = store.generation_id().unwrap();
        assert_ne!(successor, original);
        let retired = Slot::A.path(&f.root);
        if short {
            assert!(matches!(
                result,
                Err(MutationFailure::Rejected(error))
                    if error.kind() == io::ErrorKind::InvalidInput
                        && error.to_string() == "retained reclamation scan byte limit exceeded"
            ));
            assert_eq!(store.get(b"a", 4).unwrap().as_deref(), Some(&b"new"[..]));
            assert_eq!(store.store.suffix_len, 0);
            assert!(retired.exists());
            // Reopen after the rejected write and verify that enabling the
            // policy also checks its cleanup budget before touching the slot.
            drop(store);
            store = f.open();
            assert_eq!(store.generation_id().unwrap(), successor);
            let error = store
                .enable_auto_reclaim_with_scan_limit(NonZeroU64::MIN, base - 1)
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(retired.exists());
            store
                .enable_auto_reclaim_with_scan_limit(NonZeroU64::MIN, base)
                .unwrap();
            store.put(b"a", b"yes").unwrap();
        } else {
            result.unwrap();
            // This triggers another equal-cost rotation with a fresh budget.
            store.put(b"a", b"yes").unwrap();
            assert_ne!(store.generation_id().unwrap(), successor);
        }
        assert!(!store.active.slot.other().path(&f.root).exists());
        assert!(!crate::staging::has_retained_stage(&f.root).unwrap());
        drop(store);
        assert_eq!(f.open().get(b"a", 4).unwrap().as_deref(), Some(&b"yes"[..]));
    }
}

#[test]
fn automatic_maintenance_scan_limit_charges_cleanup_before_rotation() {
    use crate::retained_base_v2::FRAME_BYTES;

    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let base = store.store.base.metadata().file_bytes;
    let suffix = store
        .store
        .encode_append(Append::Put(b"a", b"new"), false)
        .unwrap()
        .len() as u64;
    let rotation = 2 * base + 2 * (base - FRAME_BYTES) + 2 * suffix + 3;
    store
        .enable_auto_reclaim_with_scan_limit(NonZeroU64::MIN, rotation + base)
        .unwrap();
    // A manual rotation leaves the prior slot for the next maintenance pass.
    let selected = store.compact().unwrap();
    store.put(b"a", b"new").unwrap();
    assert_eq!(store.store.suffix_len as u64, suffix);
    let retired = Slot::A.path(&f.root);
    assert!(retired.exists());
    let current = Slot::B.path(&f.root);
    let log = fs::read(current.join("records.log")).unwrap();
    let witness = fs::read(current.join("commit.state")).unwrap();
    assert!(matches!(
        store.put(b"a", b"yes"),
        Err(MutationFailure::Rejected(error))
            if error.kind() == io::ErrorKind::InvalidInput
                && error.to_string().contains("retained rotation scan byte limit exceeded")
    ));
    assert!(!retired.exists());
    assert_eq!(store.generation_id().unwrap(), selected);
    assert_eq!(fs::read(current.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(current.join("commit.state")).unwrap(), witness);
    store.put(b"a", b"yes").unwrap();
    assert_ne!(store.generation_id().unwrap(), selected);
    assert!(!current.exists());
    drop(store);
    assert_eq!(f.open().get(b"a", 4).unwrap().as_deref(), Some(&b"yes"[..]));
}

#[test]
fn rotation_scan_limit_counts_touched_blocks_suffix_values_and_successor() {
    use crate::retained_base_v2::{FRAME_BYTES, encoded_entry_bytes};

    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"fresh",
            },
            BatchOperation::Delete { key: b"z" },
            BatchOperation::Put {
                key: b"b",
                value: b"more",
            },
        ])
        .unwrap();
    let original = Slot::A.path(&f.root);
    let successor = Slot::B.path(&f.root);
    let base = fs::read(original.join("base")).unwrap();
    let log = fs::read(original.join("records.log")).unwrap();
    let witness = fs::read(original.join("commit.state")).unwrap();
    let selection = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let files = catalog_usage(&f.root);
    let entries = store.scan(None, None, 4).unwrap();
    let base_bytes = base.len() as u64;
    assert!(base_bytes - FRAME_BYTES < 1024); // Includes just one touched block.
    let successor_bytes = FRAME_BYTES
        + encoded_entry_bytes(b"a", b"fresh".len())
        + encoded_entry_bytes(b"b", b"more".len());
    let suffix = store.store.suffix_len as u64;
    let cap = base_bytes
        + 2 * (base_bytes - FRAME_BYTES)
        + 2 * suffix
        + (b"fresh".len() + b"more".len()) as u64
        + successor_bytes;
    for short in [0, cap - 1] {
        assert!(matches!(
            store.compact_with_scan_limit(short),
            Err(RetainedRotationFailure::NotPublished(error))
                if error.kind() == io::ErrorKind::InvalidInput
                    && error.to_string() == "retained rotation scan byte limit exceeded"
        ));
        assert!(!successor.exists());
        assert_eq!(catalog_usage(&f.root), files);
        assert_eq!(fs::read(original.join("base")).unwrap(), base);
        assert_eq!(fs::read(original.join("records.log")).unwrap(), log);
        assert_eq!(fs::read(original.join("commit.state")).unwrap(), witness);
        assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selection);
        assert_eq!(store.scan(None, None, 4).unwrap(), entries);
    }
    let mut damaged = base.clone();
    // The final payload byte sits before the unchanged SHA-256 trailer.
    damaged[base.len() - 33] ^= 1;
    fs::write(original.join("base"), damaged).unwrap();
    let reserved = base_bytes + (base_bytes - FRAME_BYTES) + 2 * suffix;
    assert!(matches!(
        store.compact_with_scan_limit(reserved),
        Err(RetainedRotationFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        store.compact_with_scan_limit(cap),
        Err(RetainedRotationFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::InvalidData
                && error.to_string() == "retained base block digest mismatch"
    ));
    assert!(!successor.exists());
    fs::write(original.join("base"), base).unwrap();
    let generation = store.compact_with_scan_limit(cap).unwrap();
    assert_eq!(
        fs::metadata(successor.join("base")).unwrap().len(),
        successor_bytes
    );
    assert_eq!(fs::read(original.join("records.log")).unwrap(), log);
    drop(store);
    let reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), generation);
    assert_eq!(reopened.scan(None, None, 4).unwrap(), entries);
}

#[test]
fn detailed_rotation_reports_successor_sync_failure_before_selection() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.put(b"ack", b"kept").unwrap();
    let old = store.active.clone();
    let selection = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    let result = store.compact_detailed_using(
        LIMITS,
        None,
        &mut None,
        || panic!("selection must not start"),
        |root| {
            assert!(Slot::B.path(root).is_dir());
            assert_eq!(fs::read(root.join(ACTIVE_FILE))?, selection);
            Err(io::Error::other("successor sync"))
        },
        |_| panic!("selection must not be synchronized"),
    );
    assert!(matches!(
        result,
        Err(RetainedRotationFailure::SuccessorOutcomeUnknown(error))
            if error.to_string() == "successor sync"
    ));
    assert!(matches!(
        store.compact_detailed(),
        Err(RetainedRotationFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::AlreadyExists
    ));
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), selection);
    store.put(b"later", b"still active").unwrap();
    drop(store);
    let mut reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), old.generation);
    assert_eq!(reopened.get(b"ack", 4).unwrap().unwrap(), b"kept");
    assert_eq!(
        reopened.get(b"later", 12).unwrap().unwrap(),
        b"still active"
    );
    assert!(reopened.reclaim_inactive().unwrap());
    assert_ne!(reopened.compact_detailed().unwrap(), old.generation);
}

#[test]
fn selection_failure_before_rename_leaves_only_old_slot_authorized() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let old = store.active.clone();
    store.put(b"b", b"safe").unwrap();
    let result = store.compact_detailed_using(
        LIMITS,
        None,
        &mut None,
        || Err(io::Error::other("before selection")),
        sync_existing,
        sync_existing,
    );
    assert!(
        matches!(result, Err(RetainedRotationFailure::SuccessorOutcomeUnknown(error))
        if error.to_string() == "before selection")
    );
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
    let result = store.compact_detailed_using(
        LIMITS,
        None,
        &mut None,
        || Ok(()),
        sync_existing,
        |_| Err(io::Error::other("selection sync")),
    );
    assert!(
        matches!(result, Err(RetainedRotationFailure::SelectionOutcomeUnknown(error))
        if error.to_string() == "selection sync")
    );
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
    let mut changed = data.clone();
    let trailer = changed.len() - 32;
    changed[trailer - 1] ^= 1;
    let mut hash = Sha256::new();
    hash.update(b"EMDA retained base v2\0");
    hash.update(&changed[..trailer]);
    changed[trailer..].copy_from_slice(&hash.finalize());
    let mut bad_trailer = data.clone();
    bad_trailer[trailer] ^= 1;
    let mut extra = data.clone();
    extra.push(0);
    for bytes in [&changed, &bad_trailer, &data[..trailer], &extra] {
        fs::write(&base, bytes).unwrap();
        assert!(store.reclaim_inactive().is_err());
        assert!(inactive.exists());
    }
    fs::write(&base, data).unwrap();
    assert!(store.reclaim_inactive().unwrap());
}

#[test]
fn automatic_reclamation_resumes_after_interrupted_marker_preparation() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.compact().unwrap();
    let inactive = store.active.slot.other().path(&f.root);
    let expected = store.scan(None, None, 4).unwrap();
    drop(store);

    let pending = f.root.join(".emda-stage-v1-g-0123456789abcdef.pending");
    // Older writers created the owner name before writing its contents.
    // Recovery preserves a malformed owner because it cannot trust it.
    let incomplete_owner = pending.with_extension("owner");
    fs::write(&incomplete_owner, []).unwrap();
    let error = ManagedRetainedStore::open_with_auto_reclaim(&f.root, LIMITS, NonZeroU64::MIN)
        .err()
        .expect("malformed published owner must block reclamation");
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(error.to_string(), "retained retirement needs inspection");
    assert_eq!(fs::read(&incomplete_owner).unwrap(), b"");
    fs::remove_file(incomplete_owner).unwrap();

    // In the new protocol an interruption before sync leaves an empty
    // preparation file. The inactive generation is still intact.
    fs::write(&pending, []).unwrap();
    assert!(inactive.join("records.log").is_file());
    let mut recovered =
        ManagedRetainedStore::open_with_auto_reclaim(&f.root, LIMITS, NonZeroU64::MIN).unwrap();
    assert!(!pending.exists());
    assert!(!inactive.exists());
    assert_eq!(recovered.scan(None, None, 4).unwrap(), expected);
    recovered.put(b"z", b"after recovery").unwrap();
    assert_eq!(
        recovered.get(b"z", 32).unwrap().as_deref(),
        Some(b"after recovery".as_slice())
    );
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
            .reclaim_using(&mut None, |path| {
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
        assert!(matches!(error, StagingCleanupFailure::OutcomeUnknown(cause)
            if cause.to_string() == "after move"));
        let retired = retired.unwrap();
        assert!(retired.join("commit.state").is_file());
        assert_eq!(store.scan(None, None, 4).unwrap(), before);
        let summary = crate::cleanup_abandoned_staging(&f.root).unwrap();
        assert_eq!(summary.removed_payloads, 0);
        assert!(retired.join("base").is_file());
        assert!(matches!(
            store.compact(),
            Err(PublicationFailure::NotPublished(ref error))
                if error.kind() == io::ErrorKind::WouldBlock
        ));
        let selected = store.active.slot.path(&f.root).join("base");
        let saved = f.root.join("saved-base");
        fs::rename(&selected, &saved).unwrap();
        fs::copy(&saved, &selected).unwrap();
        assert_eq!(
            store.reclaim_inactive().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(retired.join("records.log").is_file());
        fs::rename(&saved, selected).unwrap();
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
fn retired_removal_sync_errors_are_reported_and_recoverable() {
    for fail_sync in [3, 4] {
        let f = Fixture::new(LIMITS);
        let mut store = f.open();
        let inactive = store.active.slot.path(&store.root);
        store.compact().unwrap();
        let rows = store.scan(None, None, 4).unwrap();
        let mut syncs = 0;
        let mut stage = None;
        let error = store
            .reclaim_using(&mut None, |path| {
                syncs += 1;
                if syncs == 1 {
                    stage = Some(path.to_path_buf());
                }
                if syncs == fail_sync {
                    assert_eq!(path, f.root.canonicalize().unwrap());
                    assert!(!inactive.exists());
                    assert!(!stage.as_ref().unwrap().exists());
                    return Err(io::Error::other("injected final retirement sync failure"));
                }
                sync_existing(path)
            })
            .unwrap_err();
        assert_eq!(syncs, fail_sync);
        assert!(matches!(error, StagingCleanupFailure::OutcomeUnknown(cause)
            if cause.to_string() == "injected final retirement sync failure"));
        assert_eq!(
            crate::staging::has_retained_stage(&store.root).unwrap(),
            fail_sync == 3
        );
        assert_eq!(store.scan(None, None, 4).unwrap(), rows);
        drop(store);

        let mut reopened = f.open();
        assert_eq!(reopened.reclaim_inactive().unwrap(), fail_sync == 3);
        assert!(!crate::staging::has_retained_stage(&reopened.root).unwrap());
        assert_eq!(reopened.scan(None, None, 4).unwrap(), rows);
        assert!(!inactive.exists());
    }
}

#[test]
fn detailed_reclamation_includes_removal_during_a_resumed_cleanup() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.compact().unwrap();
    let rows = store.scan(None, None, 4).unwrap();
    let mut retired = None;
    assert!(matches!(
        store.reclaim_using(&mut None, |path| {
            retired = Some(path.join("retired"));
            Err(io::Error::other("retirement move interrupted"))
        }),
        Err(StagingCleanupFailure::OutcomeUnknown(_))
    ));
    let retired = retired.unwrap();
    let foreign = crate::staging::Stage::retained_directory(&store.root, &[0; 16]).unwrap();
    let foreign_path = foreign.path().to_path_buf();
    foreign.retain_payload().unwrap();

    assert!(matches!(
        store.reclaim_inactive_detailed(),
        Err(StagingCleanupFailure::OutcomeUnknown(error))
            if error.kind() == io::ErrorKind::WouldBlock
    ));
    assert!(!retired.exists());
    assert!(foreign_path.is_dir());
    assert!(matches!(
        store.reclaim_inactive_detailed(),
        Err(StagingCleanupFailure::NotStarted(error))
            if error.kind() == io::ErrorKind::WouldBlock
    ));
    assert_eq!(store.scan(None, None, 4).unwrap(), rows);
}

#[test]
fn reclamation_budget_counts_resumed_cleanup_and_new_retirement() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.compact().unwrap();
    store.put(b"later", b"acknowledged").unwrap();
    let inactive = store.active.slot.other().path(&store.root);
    let previous_log = fs::read(inactive.join("records.log")).unwrap();
    let previous_witness = fs::read(inactive.join("commit.state")).unwrap();
    let active = store.active.slot.path(&store.root);
    let active_log = fs::read(active.join("records.log")).unwrap();
    let selection = fs::read(store.root.join(ACTIVE_FILE)).unwrap();
    let cost = store.store.base.metadata().file_bytes + store.store.suffix_len as u64;

    // A kill before moving the old slot can leave an empty owned stage.
    let mut stage =
        crate::staging::Stage::retained_directory(&store.root, &store.active.generation).unwrap();
    stage.preserve_payload_on_drop();
    drop(stage);

    assert!(matches!(
        store.reclaim_inactive_with_scan_limit(cost - 1),
        Err(StagingCleanupFailure::NotStarted(error))
            if error.kind() == io::ErrorKind::InvalidInput
                && error.to_string() == "retained reclamation scan byte limit exceeded"
    ));
    assert!(crate::staging::has_retained_stage(&store.root).unwrap());
    assert!(matches!(
        store.reclaim_inactive_with_scan_limit(cost),
        Err(StagingCleanupFailure::OutcomeUnknown(error))
            if error.kind() == io::ErrorKind::InvalidInput
                && error.to_string() == "retained reclamation scan byte limit exceeded"
    ));
    assert!(!crate::staging::has_retained_stage(&store.root).unwrap());
    assert_eq!(
        fs::read(inactive.join("records.log")).unwrap(),
        previous_log
    );
    assert_eq!(
        fs::read(inactive.join("commit.state")).unwrap(),
        previous_witness
    );

    assert!(store.reclaim_inactive_with_scan_limit(cost).unwrap());
    assert!(!inactive.exists());
    assert!(!store.reclaim_inactive_with_scan_limit(0).unwrap());
    assert_eq!(fs::read(active.join("records.log")).unwrap(), active_log);
    assert_eq!(fs::read(store.root.join(ACTIVE_FILE)).unwrap(), selection);
    drop(store);
    assert_eq!(
        f.open().get(b"later", 12).unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
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
        let result = store.backup_using(&root.join("interrupted-backup"), None, |_| pause());
        panic!("managed backup did not stop at the boundary: {result:?}");
    }
    let mut store = ManagedRetainedStore::open(root, LIMITS).unwrap();
    if point == "retire" {
        store.compact().unwrap();
        let result = store.reclaim_using(&mut None, |_| pause());
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

// Writes made while a rotation is prepared: singles, a delete and a batch.
fn write_during_preparation(store: &mut ManagedRetainedStore) {
    store.put(b"b", b"during").unwrap();
    store.delete(b"a").unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"c",
                value: b"batched",
            },
            BatchOperation::Put {
                key: b"b",
                value: b"overwritten",
            },
        ])
        .unwrap();
}

#[test]
fn a_rotation_prepared_off_the_writer_path_keeps_concurrent_writes() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.put(b"m", b"before").unwrap();
    let source = store.generation_id().unwrap();
    let mut job = store.begin_rotation().unwrap();
    // Preparation runs on another thread while the store keeps writing.
    let preparing = std::thread::spawn(move || {
        job.prepare().unwrap();
        job
    });
    write_during_preparation(&mut store);
    let job = preparing.join().unwrap();
    assert!(job.is_prepared());
    let expected = store.scan(None, None, 16).unwrap();
    let generation = store.complete_rotation(job).unwrap();
    assert_ne!(generation, source);
    assert_eq!(store.generation_id().unwrap(), generation);
    assert_eq!(store.scan(None, None, 16).unwrap(), expected);
    // The previous generation is reclaimable and the successor writable.
    assert!(store.reclaim_inactive().unwrap());
    store.put(b"after", b"rotation").unwrap();
    let expected = store.scan(None, None, 16).unwrap();
    drop(store);
    let reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), generation);
    assert_eq!(reopened.scan(None, None, 16).unwrap(), expected);
}

#[test]
fn an_abandoned_or_interrupted_job_leaves_the_catalog_usable() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let source = store.generation_id().unwrap();
    // Dropping a prepared job removes its stage.
    let mut job = store.begin_rotation().unwrap();
    job.prepare().unwrap();
    drop(job);
    assert!(!crate::staging::has_unowned_stage(&f.root).unwrap());
    store.put(b"b", b"kept").unwrap();
    let expected = store.scan(None, None, 16).unwrap();
    drop(store);
    // A child process prepares a job and exits without cleanup, as after a
    // process kill, leaving its stage behind.
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "retained_candidate::writable::managed::tests::rotation_job_exit_child",
            "--nocapture",
        ])
        .env("EMDA_ROTATION_JOB_CATALOG", &f.root)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(crate::staging::has_unowned_stage(&f.root).unwrap());
    crate::cleanup_abandoned_staging(&f.root).unwrap();
    let mut reopened = f.open();
    assert_eq!(reopened.generation_id().unwrap(), source);
    assert_eq!(reopened.scan(None, None, 16).unwrap(), expected);
    let mut job = reopened.begin_rotation().unwrap();
    job.prepare().unwrap();
    assert_ne!(reopened.complete_rotation(job).unwrap(), source);
    assert_eq!(reopened.scan(None, None, 16).unwrap(), expected);
}

#[test]
fn a_job_is_refused_after_another_rotation_or_without_preparation() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    let job = store.begin_rotation().unwrap();
    // Only one job at a time.
    assert_eq!(
        store.begin_rotation().err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    let failure = store.complete_rotation(job).unwrap_err();
    assert!(
        failure.to_string().contains("prepare the rotation first"),
        "{failure}"
    );
    let mut job = store.begin_rotation().unwrap();
    job.prepare().unwrap();
    store.compact().unwrap();
    let rotated = store.generation_id().unwrap();
    let failure = store.complete_rotation(job).unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("rotated since this job was captured"),
        "{failure}"
    );
    assert_eq!(store.generation_id().unwrap(), rotated);
    store.put(b"b", b"still writable").unwrap();
}

#[test]
fn a_catalog_quota_reserves_the_successor_during_preparation() {
    let f = Fixture::new(LIMITS);
    let used = catalog_usage(&f.root);
    let probe = f.open();
    // Reservation: base bound, stage marker and metadata allowance.
    let reserved =
        probe.store.rebase_bound() + crate::staging::Stage::DIRECTORY_MARKER_BYTES + 64 * 1024;
    drop(probe);
    let record = record::encode(Operation::Put, b"b", b"fits").unwrap().len() as u64;
    let limit = |bytes| CatalogStorageLimits {
        bytes,
        entries: used.entries + 64,
    };
    // One byte short of the reservation refuses the job before staging.
    let mut store = ManagedRetainedStore::open_with_storage_limits(
        &f.root,
        LIMITS,
        limit(used.bytes + reserved - 1),
        None,
    )
    .unwrap();
    let error = store.begin_rotation().err().unwrap();
    assert_eq!(error.to_string(), "catalog byte limit exceeded");
    assert!(!crate::staging::has_unowned_stage(&f.root).unwrap());
    drop(store);
    // With room for the reservation and one append, a second append is
    // refused while the job holds its room.
    let mut store = ManagedRetainedStore::open_with_storage_limits(
        &f.root,
        LIMITS,
        limit(used.bytes + reserved + record + 4096),
        None,
    )
    .unwrap();
    let mut job = store.begin_rotation().unwrap();
    store.put(b"b", b"fits").unwrap();
    assert!(matches!(
        store.put(b"c", &[b'x'; 4096]),
        Err(MutationFailure::Rejected(ref error)) if error.to_string() == "catalog byte limit exceeded"
    ));
    job.prepare().unwrap();
    let expected = store.scan(None, None, 16).unwrap();
    store.complete_rotation(job).unwrap();
    assert_eq!(store.scan(None, None, 16).unwrap(), expected);
    // Dropping a job releases its reservation.
    assert!(store.reclaim_inactive().unwrap());
    let job = store.begin_rotation().unwrap();
    drop(job);
    store.put(b"c", b"after release").unwrap();
}

#[test]
fn rotation_job_exit_child() {
    let Ok(root) = std::env::var("EMDA_ROTATION_JOB_CATALOG") else {
        return;
    };
    let mut store = ManagedRetainedStore::open(&root, LIMITS).unwrap();
    let mut job = store.begin_rotation().unwrap();
    job.prepare().unwrap();
    // Exit without running destructors, as a killed process would.
    std::process::exit(0);
}

#[test]
fn suffix_usage_reports_occupancy_until_rejection_and_resets_on_rotation() {
    let fixture = Fixture::new(LIMITS);
    let mut store = fixture.open();
    let empty = store.suffix_usage().unwrap();
    assert_eq!(
        empty,
        RetainedSuffixUsage {
            bytes: 0,
            operations: 0,
            work: 0,
            overlay_entries: 0,
            overlay_key_bytes: 0,
        }
    );
    assert_eq!(empty.max_fraction_used(&LIMITS), 0.0);

    let mut previous = empty;
    for index in 0..30 {
        store.put(format!("k{index:02}").as_bytes(), b"v").unwrap();
        let usage = store.suffix_usage().unwrap();
        assert_eq!(usage.operations, index + 1);
        assert_eq!(usage.overlay_entries, index + 1);
        assert_eq!(usage.overlay_key_bytes, 3 * (index + 1));
        assert!(usage.bytes > previous.bytes);
        assert!(usage.work >= previous.work);
        previous = usage;
    }
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"new",
            },
            BatchOperation::Delete { key: b"z" },
        ])
        .unwrap();
    let full = store.suffix_usage().unwrap();
    assert_eq!((full.operations, full.overlay_entries), (32, 32));
    assert_eq!(full.max_fraction_used(&LIMITS), 1.0);
    drop(store);

    // Usage is recomputed from the committed suffix on reopen.
    let mut store = fixture.open();
    assert_eq!(store.suffix_usage().unwrap(), full);
    // Rewriting a touched key fits; a new key would exceed the overlay.
    store.put(b"k00", b"w").unwrap();
    assert!(matches!(
        store.put(b"new", b"v"),
        Err(MutationFailure::Rejected(_))
    ));
    assert_eq!(store.suffix_usage().unwrap().overlay_entries, 32);

    // A prepared rotation starts the successor with only the catch-up.
    let mut job = store.begin_rotation().unwrap();
    job.prepare().unwrap();
    store.put(b"k01", b"caught up").unwrap();
    store.complete_rotation(job).unwrap();
    let rotated = store.suffix_usage().unwrap();
    assert_eq!((rotated.operations, rotated.overlay_entries), (1, 1));
    assert!(rotated.max_fraction_used(&LIMITS) < 0.1);
    store.put(b"new", b"v").unwrap();
    assert_eq!(store.get(b"k01", 16).unwrap().unwrap(), b"caught up");
    assert_eq!(store.get(b"z", 16).unwrap(), None);

    assert!(store.reclaim_inactive().unwrap());
    store.compact().unwrap();
    assert_eq!(store.suffix_usage().unwrap(), empty);
}
