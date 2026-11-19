use super::*;

const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: crate::retained_base_v2::Limits {
        file_bytes: 8192,
        entries: 32,
        key_bytes: 256,
        work_bytes: 8193,
    },
    index: crate::retained_candidate::IndexLimits {
        blocks: 4,
        anchor_bytes: 64,
    },
    suffix: crate::retained_candidate::SuffixLimits {
        bytes: 8192,
        operations: 64,
        work: 4096,
    },
    overlay: crate::retained_candidate::OverlayLimits {
        entries: 32,
        key_bytes: 256,
    },
};
const EXPORT: RetainedExportLimits = RetainedExportLimits {
    max_log_bytes: 8192,
    max_live_keys: 32,
    max_live_key_bytes: 256,
    max_output_bytes: 8192,
};

type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;

fn seed(path: &Path) -> Store {
    let store = Store::open(path).unwrap();
    store.put(b"legacy", b"acknowledged").unwrap();
    store
}

#[test]
fn initial_catalog_quota_is_bound_to_the_separate_authority() {
    use crate::retained_candidate::writable::managed::tests::catalog_usage;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let destination = tmp.path().join("authorized");
    let policy = CatalogStorageLimits {
        bytes: 4096,
        entries: 8,
    };
    let prepare = |storage| {
        Catalog::prepare_from_store_with_storage_limits(
            Store::open(&root).unwrap(),
            &root,
            &destination,
            EXPORT,
            LIMITS,
            storage,
        )
    };
    drop(seed(&root));
    assert!(
        matches!(prepare(CatalogStorageLimits { entries: policy.entries - 1, ..policy }),
        Err(PublicationFailure::NotPublished(error)) if error.to_string() == "catalog entry limit exceeded")
    );
    assert!(!destination.exists());
    let initial = prepare(policy).unwrap();
    let mut provider = FileRetainedAuthority::create(tmp.path().join("authority")).unwrap();
    assert!(provider.compare_and_swap(None, initial).unwrap());
    let mut store = Catalog::open(&destination, LIMITS, provider).unwrap();
    assert_eq!(store.inner.store.header.storage_limits(), Some(policy));
    assert_eq!(catalog_usage(&destination).entries, policy.entries);
    assert_eq!(
        store.get(b"legacy", 12).unwrap().as_deref(),
        Some(b"acknowledged".as_slice())
    );
    assert!(
        matches!(store.compact(), Err(PublicationFailure::NotPublished(error)) if error.to_string() == "catalog entry limit exceeded")
    );
    assert_eq!(store.current, initial);
}

#[test]
fn initial_catalog_quota_counts_cutover_staging_before_fencing() {
    use crate::retained_candidate::writable::managed::tests::catalog_usage;
    use crate::staging::Stage;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    drop(seed(&root));
    let original = catalog_usage(&root);
    let log = fs::read(root.join("records.log")).unwrap();
    let witness = fs::read(root.join("commit.state")).unwrap();
    let base_bytes = (144 + 8 + b"legacy".len() + b"acknowledged".len()) as u64;
    let fixed = (CATALOG_BYTES
        + ACTIVE_BYTES
        + super::super::super::super::QUOTA_HEADER_BYTES
        + super::super::super::super::WITNESS_BYTES) as u64;
    let exact = CatalogStorageLimits {
        bytes: original.bytes
            + log.len() as u64
            + witness.len() as u64
            + fixed
            + base_bytes
            + MARKER_LEN as u64
            + Stage::FILE_MARKER_BYTES,
        entries: original.entries + 10 + 3,
    };
    let cutover = |storage| {
        Catalog::cutover_from_store_with_storage_limits(
            Store::open(&root).unwrap(),
            &root,
            EXPORT,
            LIMITS,
            storage,
        )
    };
    for below in [
        CatalogStorageLimits {
            bytes: exact.bytes - 1,
            ..exact
        },
        CatalogStorageLimits {
            entries: exact.entries - 1,
            ..exact
        },
    ] {
        assert!(
            matches!(cutover(below), Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidInput)
        );
        assert_eq!(fs::read(root.join("records.log")).unwrap(), log);
        assert_eq!(fs::read(root.join("commit.state")).unwrap(), witness);
        assert_eq!(catalog_usage(&root), original);
    }

    let initial = cutover(exact).unwrap();
    assert_eq!(Catalog::recover_cutover(&root, LIMITS).unwrap(), initial);
    let mut provider = FileRetainedAuthority::create(tmp.path().join("authority")).unwrap();
    assert!(provider.compare_and_swap(None, initial).unwrap());
    let mut store = Catalog::open(&root, LIMITS, provider).unwrap();
    assert_eq!(store.inner.store.header.storage_limits(), Some(exact));
    assert_eq!(
        store.get(b"legacy", 12).unwrap().as_deref(),
        Some(b"acknowledged".as_slice())
    );
    let nested = store.fence.as_ref().unwrap().catalog_path();
    assert_eq!(fs::read(nested.join(ARCHIVE_LOG)).unwrap(), log);
    assert_eq!(fs::read(nested.join(ARCHIVE_WITNESS)).unwrap(), witness);
}

#[test]
fn abandoned_initial_catalog_quota_does_not_block_unfenced_cleanup() {
    use crate::retained_candidate::writable::managed::tests::catalog_usage;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let policy = CatalogStorageLimits {
        bytes: 4096,
        entries: 64,
    };
    let result = Catalog::cutover_using(
        seed(&root),
        &root,
        EXPORT,
        InitialLimits::with_storage(LIMITS, policy),
        || Err(io::Error::other("injected pre-fence failure")),
        sync_existing,
    );
    assert!(
        matches!(result, Err(PublicationFailure::NotPublished(error)) if error.to_string() == "injected pre-fence failure")
    );
    let preparation = only_preparation(&root);
    let source = Store::open(&root).unwrap();
    source.put(b"later", &[7; 4096]).unwrap();
    assert!(catalog_usage(&root).bytes > policy.bytes);
    let mut authority = FileRetainedAuthority::create(tmp.path().join("authority")).unwrap();
    assert!(
        Catalog::discard_unfenced_preparation(source, &root, preparation, LIMITS, &mut authority)
            .unwrap()
    );
    assert_eq!(
        Store::open(&root).unwrap().get(b"later").unwrap(),
        Some(vec![7; 4096])
    );
    assert!(authority.read().unwrap().is_none());
}

#[test]
fn persisted_catalog_quota_follows_authority_and_covers_original_root() {
    use crate::retained_candidate::writable::managed::tests::catalog_usage;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let authority_path = tmp.path().join("authority");
    let initial = Catalog::cutover_from_store(seed(&root), &root, EXPORT, LIMITS).unwrap();
    let mut provider = FileRetainedAuthority::create(&authority_path).unwrap();
    assert!(provider.compare_and_swap(None, initial).unwrap());
    let original = catalog_usage(&root);
    let policy = CatalogStorageLimits {
        bytes: original.bytes + 850,
        entries: original.entries + 8,
    };
    let mut store = Catalog::open(&root, LIMITS, provider).unwrap();
    let next = store.compact_with_persisted_storage_limits(policy).unwrap();
    assert_ne!(next, initial.committed.generation);
    let selected = store.current;
    let digest: [u8; 32] = Sha256::digest(store.inner.store.header.bytes()).into();
    assert_eq!(selected.committed.header_digest, digest);
    assert!(store.reclaim_inactive().unwrap());
    let authority_after_reclamation = store.current;
    drop(store);
    let mut store = Catalog::open(
        &root,
        LIMITS,
        FileRetainedAuthority::open(&authority_path).unwrap(),
    )
    .unwrap();
    assert_eq!(store.current, authority_after_reclamation);
    assert_eq!(store.inner.store.header.storage_limits(), Some(policy));
    let used = catalog_usage(&root);
    let extra = root.join("reserved-input");
    // This independently preserved file is charged even though it is
    // outside the selected catalog subdirectory.
    fs::write(&extra, vec![0; (policy.bytes - used.bytes) as usize]).unwrap();
    assert!(matches!(
        store.put(b"small", b"x"),
        Err(MutationFailure::Rejected(ref cause)) if cause.to_string() == "catalog byte limit exceeded"
    ));
    assert_eq!(
        store.authority.read().unwrap(),
        Some(authority_after_reclamation)
    );
    fs::remove_file(extra).unwrap();
    store.put(b"small", b"x").unwrap();
    assert_eq!(
        store.get(b"small", 1).unwrap().as_deref(),
        Some(b"x".as_slice())
    );
}

#[test]
fn catalog_storage_counts_cutover_archive_and_checks_before_authority_update() {
    use crate::retained_candidate::writable::managed::tests::catalog_usage;

    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let location = tmp.path().join("authority");
    let store = seed(&root);
    let old_log = fs::read(root.join("records.log")).unwrap();
    let old_witness = fs::read(root.join("commit.state")).unwrap();
    let archive = CutoverArchiveBinding {
        log_bytes: old_log.len() as u64,
        log_digest: Sha256::digest(&old_log).into(),
        witness_digest: Sha256::digest(&old_witness).into(),
    };
    let initial = Catalog::cutover_from_store(store, &root, EXPORT, LIMITS).unwrap();
    let mut authority = FileRetainedAuthority::create(&location).unwrap();
    assert!(authority.compare_and_swap(None, initial).unwrap());
    let limit = catalog_usage(&root);
    let mut store =
        Catalog::open_with_storage_limits(&root, LIMITS, authority, limit, None).unwrap();
    let log = Slot::A.path(&store.inner.root).join("records.log");
    let before = fs::read(&log).unwrap();
    assert!(matches!(
        store.put(b"new", b"ok"),
        Err(MutationFailure::Rejected(ref e)) if e.to_string() == "catalog byte limit exceeded"
    ));
    assert_eq!(store.authority.read().unwrap(), Some(initial));
    assert_eq!(fs::read(&log).unwrap(), before);
    assert_eq!(catalog_usage(&root), limit);
    let backup = tmp.path().join("backup");
    let expected = store.backup_to(&backup).unwrap();
    assert!(
        store
            .retire_cutover_archive(&archive, &backup, &expected)
            .unwrap()
    );
    store.put(b"new", b"ok").unwrap();
    drop(store);

    let outside = tmp.path().join("outside");
    fs::write(&outside, b"preserve").unwrap();
    let alias = root.join("untrusted-link");
    std::os::unix::fs::symlink(&outside, &alias).unwrap();
    let error = Catalog::open_with_storage_limits(
        &root,
        LIMITS,
        FileRetainedAuthority::open(&location).unwrap(),
        CatalogStorageLimits {
            entries: limit.entries + 1,
            ..limit
        },
        None,
    )
    .err()
    .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "unsupported file type in catalog storage"
    );
    assert_eq!(fs::read(&outside).unwrap(), b"preserve");
    fs::remove_file(&alias).unwrap();
    let mut reopened = Catalog::open_with_storage_limits(
        &root,
        LIMITS,
        FileRetainedAuthority::open(&location).unwrap(),
        limit,
        None,
    )
    .unwrap();
    assert_eq!(reopened.get(b"new", 2).unwrap().unwrap(), b"ok");
}

fn only_preparation(root: &Path) -> [u8; 16] {
    let mut paths = fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter_map(|name| name.strip_prefix("retained-cutover-").map(str::to_owned));
    let name = paths.next().unwrap();
    assert!(paths.next().is_none());
    assert_eq!(name.len(), 32);
    std::array::from_fn(|index| u8::from_str_radix(&name[index * 2..index * 2 + 2], 16).unwrap())
}

#[test]
fn original_path_requires_fence_and_independent_registration() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let authority = tmp.path().join("authority");
    let store = seed(&root);
    let old_log = fs::read(root.join("records.log")).unwrap();
    let old_witness = fs::read(root.join("commit.state")).unwrap();
    let initial = Catalog::cutover_from_store(store, &root, EXPORT, LIMITS).unwrap();
    let marker = Marker::load(&root).unwrap();
    let nested = marker.catalog_path(&root);
    assert_eq!(marker.initial, initial);
    assert_eq!(fs::read(nested.join("legacy.records")).unwrap(), old_log);
    assert_eq!(
        fs::read(nested.join("legacy.witness")).unwrap(),
        old_witness
    );
    assert_eq!(Catalog::recover_cutover(&root, LIMITS).unwrap(), initial);
    assert!(Store::open(&root).is_err());
    assert!(Store::open(&nested).is_err());
    let mut provider = FileRetainedAuthority::create(&authority).unwrap();
    assert!(
        Catalog::open(
            &root,
            LIMITS,
            FileRetainedAuthority::open(&authority).unwrap()
        )
        .is_err()
    );
    assert!(provider.compare_and_swap(None, initial).unwrap());
    assert!(
        Catalog::open(
            &nested,
            LIMITS,
            FileRetainedAuthority::open(&authority).unwrap()
        )
        .is_err()
    );
    let mut current = Catalog::open(&root, LIMITS, provider).unwrap();
    assert_eq!(
        current.get(b"legacy", 12).unwrap().unwrap(),
        b"acknowledged"
    );
    current.put(b"new", b"written").unwrap();
    current.compact().unwrap();
    assert!(current.reclaim_inactive().unwrap());
    drop(current);
    let mut current = Catalog::open(
        &root,
        LIMITS,
        FileRetainedAuthority::open(&authority).unwrap(),
    )
    .unwrap();
    assert_eq!(current.get(b"new", 7).unwrap().unwrap(), b"written");
    assert_eq!(fs::read(nested.join("legacy.records")).unwrap(), old_log);
}

#[test]
fn archive_retirement_checks_backup_and_identity_and_resumes_partial_unlink() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let authority_path = tmp.path().join("authority");
    let store = seed(&root);
    let old_log = fs::read(root.join("records.log")).unwrap();
    let old_witness = fs::read(root.join("commit.state")).unwrap();
    let binding = CutoverArchiveBinding {
        log_bytes: old_log.len() as u64,
        log_digest: Sha256::digest(&old_log).into(),
        witness_digest: Sha256::digest(&old_witness).into(),
    };
    let initial = Catalog::cutover_from_store(store, &root, EXPORT, LIMITS).unwrap();
    let fence = fs::read(root.join("records.log")).unwrap();
    let catalog = Marker::load(&root).unwrap().catalog_path(&root);
    let log = catalog.join(ARCHIVE_LOG);
    let witness = catalog.join(ARCHIVE_WITNESS);
    let mut provider = FileRetainedAuthority::create(&authority_path).unwrap();
    assert!(provider.compare_and_swap(None, initial).unwrap());
    let mut store = Catalog::open(&root, LIMITS, provider).unwrap();
    let earlier = tmp.path().join("earlier");
    let stale = store.backup_to(&earlier).unwrap();
    store.put(b"new", b"durable").unwrap();
    store.compact().unwrap();
    assert!(store.reclaim_inactive().unwrap());
    assert!(!Slot::A.path(&catalog).exists());
    store.put(b"tail", b"checked").unwrap();
    let backup = tmp.path().join("backup");
    let expected = store.backup_to(&backup).unwrap();

    let suffix = backup.join("suffix");
    let saved = fs::read(&suffix).unwrap();
    assert!(matches!(
        store.retire_cutover_archive(&binding, &earlier, &stale),
        Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    fs::write(&suffix, vec![0; saved.len()]).unwrap();
    assert!(matches!(
        store.retire_cutover_archive(&binding, &backup, &expected),
        Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    fs::write(&suffix, &saved).unwrap();
    let bad = CutoverArchiveBinding {
        log_digest: [0; 32],
        ..binding
    };
    assert!(matches!(
        store.retire_cutover_archive(&bad, &backup, &expected),
        Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    assert_eq!(fs::read(&log).unwrap(), old_log);
    assert_eq!(fs::read(&witness).unwrap(), old_witness);

    let parked = tmp.path().join("parked-legacy-log");
    fs::rename(&log, &parked).unwrap();
    std::os::unix::fs::symlink(&parked, &log).unwrap();
    assert!(matches!(
        store.retire_cutover_archive(&binding, &backup, &expected),
        Err(PublicationFailure::NotPublished(_))
    ));
    assert!(log.symlink_metadata().unwrap().file_type().is_symlink());
    assert_eq!(fs::read(&parked).unwrap(), old_log);
    fs::remove_file(&log).unwrap();
    fs::rename(&parked, &log).unwrap();

    let revision = store.current;
    assert!(matches!(
        store.retire_archive_using(&binding, &backup, &expected, |path| {
            if path.file_name() == Some(ARCHIVE_LOG.as_ref()) {
                fs::remove_file(path)
            } else {
                Err(io::Error::other("injected unlink failure"))
            }
        }),
        Err(PublicationFailure::OutcomeUnknown(error)) if error.to_string() == "injected unlink failure"
    ));
    assert!(!log.exists());
    assert_eq!(fs::read(&witness).unwrap(), old_witness);
    drop(store);
    let mut store = Catalog::open(
        &root,
        LIMITS,
        FileRetainedAuthority::open(&authority_path).unwrap(),
    )
    .unwrap();
    assert_eq!(store.current, revision);
    assert!(
        store
            .retire_cutover_archive(&binding, &backup, &expected)
            .unwrap()
    );
    assert!(
        !store
            .retire_cutover_archive(&binding, &backup, &expected)
            .unwrap()
    );
    assert!(!log.exists() && !witness.exists());
    let page = store.scan(None, None, 8).unwrap();
    store.put(b"later", b"accepted").unwrap();
    drop(store);
    assert!(Store::open(&root).is_err());
    assert_eq!(fs::read(root.join("records.log")).unwrap(), fence);
    assert_eq!(fs::read(root.join("commit.state")).unwrap(), old_witness);
    assert_eq!(fs::read(&suffix).unwrap(), saved);
    let restored = tmp.path().join("restored");
    ManagedRetainedStore::restore_backup(&backup, &restored, LIMITS, &expected).unwrap();
    assert_eq!(
        ManagedRetainedStore::open(&restored, LIMITS)
            .unwrap()
            .scan(None, None, 8)
            .unwrap(),
        page
    );
}

#[test]
fn failure_before_fencing_leaves_a_writable_ordinary_source_for_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let store = seed(&root);
    let result = Catalog::cutover_using(
        store,
        &root,
        EXPORT,
        LIMITS.into(),
        || Err(io::Error::other("before fence")),
        sync_existing,
    );
    assert!(matches!(
        result, Err(PublicationFailure::NotPublished(error)) if error.to_string() == "before fence"
    ));
    assert!(Catalog::recover_cutover(&root, LIMITS).is_err());
    let store = Store::open(&root).unwrap();
    store.put(b"written-after", b"kept").unwrap();
    let prep = only_preparation(&root);
    let path = catalog_path(&root, prep);
    let log = fs::read(root.join("records.log")).unwrap();
    let witness = fs::read(root.join("commit.state")).unwrap();
    let mut authority = FileRetainedAuthority::create(tmp.path().join("authority")).unwrap();
    let catalog = read_catalog(&path, CUTOVER_MAGIC).unwrap();
    let prepared = open_regular(&Slot::A.path(&path).join("records.log")).unwrap();
    let header = Header::read(&prepared, GenerationRole::Authorized(catalog)).unwrap();
    let mut occupied = FileRetainedAuthority::create(tmp.path().join("occupied")).unwrap();
    assert!(
        occupied
            .compare_and_swap(
                None,
                AuthorityRecord {
                    revision: 0,
                    committed: AuthorityPoint::new(catalog, &header, 0, Sha256::digest([]).into()),
                    pending: None,
                },
            )
            .unwrap()
    );
    assert!(matches!(
        Catalog::discard_unfenced_preparation(store, &root, prep, LIMITS, &mut occupied),
        Err(PublicationFailure::NotPublished(ref cause))
            if cause.to_string() == "cutover is no longer unregistered"
    ));
    assert!(path.is_dir());
    let linked = path.join(ARCHIVE_LOG);
    // A byte-for-byte copy is insufficient evidence of archive ownership.
    fs::remove_file(&linked).unwrap();
    fs::write(&linked, &log).unwrap();
    assert!(matches!(
        Catalog::discard_unfenced_preparation(Store::open(&root).unwrap(), &root, prep, LIMITS, &mut authority),
        Err(PublicationFailure::NotPublished(ref cause))
            if cause.to_string() == "cutover archive is not linked to the source"
    ));
    fs::remove_file(&linked).unwrap();
    fs::hard_link(root.join("records.log"), &linked).unwrap();
    assert!(
        Catalog::discard_unfenced_preparation(
            Store::open(&root).unwrap(),
            &root,
            prep,
            LIMITS,
            &mut authority,
        )
        .unwrap()
    );
    assert!(
        !Catalog::discard_unfenced_preparation(
            Store::open(&root).unwrap(),
            &root,
            prep,
            LIMITS,
            &mut authority,
        )
        .unwrap()
    );
    assert!(!path.exists());
    assert_eq!(fs::read(root.join("records.log")).unwrap(), log);
    assert_eq!(fs::read(root.join("commit.state")).unwrap(), witness);
    let store = Store::open(&root).unwrap();
    let initial = Catalog::cutover_from_store(store, &root, EXPORT, LIMITS).unwrap();
    assert!(authority.compare_and_swap(None, initial).unwrap());
    let mut current = Catalog::open(&root, LIMITS, authority).unwrap();
    assert_eq!(current.get(b"written-after", 4).unwrap().unwrap(), b"kept");
}

#[test]
fn selected_catalog_is_protected_and_preparation_can_be_removed_after_archive_retirement() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let store = seed(&root);
    let old_log = fs::read(root.join("records.log")).unwrap();
    let old_witness = fs::read(root.join("commit.state")).unwrap();
    let archive = CutoverArchiveBinding {
        log_bytes: old_log.len() as u64,
        log_digest: Sha256::digest(&old_log).into(),
        witness_digest: Sha256::digest(&old_witness).into(),
    };
    assert!(matches!(
        Catalog::cutover_using(
            store,
            &root,
            EXPORT,
            LIMITS.into(),
            || Err(io::Error::other("stop before fence")),
            sync_existing
        ),
        Err(PublicationFailure::NotPublished(_))
    ));
    let prep = only_preparation(&root);
    let path = catalog_path(&root, prep);
    let initial =
        Catalog::cutover_from_store(Store::open(&root).unwrap(), &root, EXPORT, LIMITS).unwrap();
    let fence = Marker::load(&root).unwrap();
    let location = tmp.path().join("authority");
    let mut authority = FileRetainedAuthority::create(&location).unwrap();
    assert!(authority.compare_and_swap(None, initial).unwrap());
    let mut store = Catalog::open(&root, LIMITS, authority).unwrap();
    assert!(matches!(
        store.discard_other_cutover_preparation(fence.directory, LIMITS, &archive),
        Err(PublicationFailure::NotPublished(ref cause))
            if cause.to_string() == "cannot discard the fenced cutover catalog"
    ));
    store.put(b"new", b"ack").unwrap();
    let backup = tmp.path().join("backup");
    let binding = store.backup_to(&backup).unwrap();
    assert!(
        store
            .retire_cutover_archive(&archive, &backup, &binding)
            .unwrap()
    );
    fs::write(path.join("unowned"), b"keep").unwrap();
    assert!(matches!(
        store.discard_other_cutover_preparation(prep, LIMITS, &archive),
        Err(PublicationFailure::NotPublished(ref cause))
            if cause.to_string() == "unknown entry in cutover preparation"
    ));
    assert_eq!(fs::read(path.join("unowned")).unwrap(), b"keep");
    fs::remove_file(path.join("unowned")).unwrap();
    assert!(
        store
            .discard_other_cutover_preparation(prep, LIMITS, &archive)
            .unwrap()
    );
    assert!(!path.exists());
    assert!(
        !store
            .discard_other_cutover_preparation(prep, LIMITS, &archive)
            .unwrap()
    );
    drop(store);
    let mut reopened = Catalog::open(
        &root,
        LIMITS,
        FileRetainedAuthority::open(location).unwrap(),
    )
    .unwrap();
    assert_eq!(
        reopened.get(b"legacy", 12).unwrap().unwrap(),
        b"acknowledged"
    );
    assert_eq!(reopened.get(b"new", 3).unwrap().unwrap(), b"ack");
}

#[test]
fn uncertain_fence_recovers_the_registration_record_without_reopening_the_legacy_writer() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("source");
    let store = seed(&root);
    let result = Catalog::cutover_using(
        store,
        &root,
        EXPORT,
        LIMITS.into(),
        || Ok(()),
        |path| {
            assert_eq!(
                Store::open(path).err().unwrap().kind(),
                io::ErrorKind::WouldBlock
            );
            Err(io::Error::other("fence sync interrupted"))
        },
    );
    assert!(matches!(result, Err(PublicationFailure::OutcomeUnknown(_))));
    assert!(Store::open(&root).is_err());
    let initial = Catalog::recover_cutover(&root, LIMITS).unwrap();
    let mut authority = FileRetainedAuthority::create(tmp.path().join("authority")).unwrap();
    assert!(authority.compare_and_swap(None, initial).unwrap());
    let mut current = Catalog::open(&root, LIMITS, authority).unwrap();
    assert_eq!(
        current.get(b"legacy", 12).unwrap().unwrap(),
        b"acknowledged"
    );

    let mut bytes = fs::read(root.join("records.log")).unwrap();
    bytes[9] ^= 1;
    fs::write(root.join("records.log"), bytes).unwrap();
    assert!(current.get(b"legacy", 12).is_err());
    assert!(matches!(
        current.put(b"no", b"write"),
        Err(MutationFailure::Rejected(_))
    ));
}

#[test]
fn cutover_child_at_fence() {
    let Some(root) = std::env::var_os("EMDA_CUTOVER_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let boundary = std::env::var("EMDA_CUTOVER_BOUNDARY").unwrap();
    let pause = |where_| -> io::Result<()> {
        if where_ == boundary {
            println!("cutover fence boundary");
            io::stdout().flush()?;
            loop {
                std::thread::park();
            }
        }
        Ok(())
    };
    let store = Store::open(&root).unwrap();
    Catalog::cutover_using(
        store,
        &root,
        EXPORT,
        LIMITS.into(),
        || pause("before"),
        |_| pause("after"),
    )
    .unwrap();
    panic!("child missed the cutover boundary");
}

#[test]
fn process_kills_leave_either_ordinary_source_or_recoverable_fence() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
        time::Duration,
    };

    for boundary in ["before", "after"] {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("source");
        drop(seed(&root));
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "retained_candidate::writable::managed::authority::cutover::tests::cutover_child_at_fence", "--nocapture"])
            .env("EMDA_CUTOVER_ROOT", &root)
            .env("EMDA_CUTOVER_BOUNDARY", boundary)
            .stdout(Stdio::piped())
            .spawn().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let (notify, seen) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in stdout.lines().map_while(Result::ok) {
                if line == "cutover fence boundary" {
                    notify.send(()).unwrap();
                    break;
                }
            }
        });
        let reached = seen.recv_timeout(Duration::from_secs(15));
        let killed = child.kill();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        reached.expect("cutover child did not reach fence boundary");
        killed.unwrap();
        assert!(!status.success());
        if boundary == "before" {
            let store = Store::open(&root).unwrap();
            let prep = only_preparation(&root);
            let mut provider = FileRetainedAuthority::create(tmp.path().join("authority")).unwrap();
            assert!(
                Catalog::discard_unfenced_preparation(store, &root, prep, LIMITS, &mut provider)
                    .unwrap()
            );
            assert!(!catalog_path(&root, prep).exists());
            assert_eq!(
                Store::open(&root).unwrap().get(b"legacy").unwrap().unwrap(),
                b"acknowledged"
            );
            assert!(Catalog::recover_cutover(&root, LIMITS).is_err());
        } else {
            assert!(Store::open(&root).is_err());
            let initial = Catalog::recover_cutover(&root, LIMITS).unwrap();
            let mut provider = FileRetainedAuthority::create(tmp.path().join("authority")).unwrap();
            assert!(provider.compare_and_swap(None, initial).unwrap());
            let mut store = Catalog::open(&root, LIMITS, provider).unwrap();
            assert_eq!(store.get(b"legacy", 12).unwrap().unwrap(), b"acknowledged");
        }
    }
}
