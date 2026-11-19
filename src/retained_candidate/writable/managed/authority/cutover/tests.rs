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
        LIMITS,
        || Err(io::Error::other("before fence")),
        sync_existing,
    );
    assert!(matches!(
        result, Err(PublicationFailure::NotPublished(error)) if error.to_string() == "before fence"
    ));
    assert!(Catalog::recover_cutover(&root, LIMITS).is_err());
    let store = Store::open(&root).unwrap();
    store.put(b"written-after", b"kept").unwrap();
    let initial = Catalog::cutover_from_store(store, &root, EXPORT, LIMITS).unwrap();
    let mut authority = FileRetainedAuthority::create(tmp.path().join("authority")).unwrap();
    assert!(authority.compare_and_swap(None, initial).unwrap());
    let mut current = Catalog::open(&root, LIMITS, authority).unwrap();
    assert_eq!(current.get(b"written-after", 4).unwrap().unwrap(), b"kept");
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
        LIMITS,
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
        LIMITS,
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
