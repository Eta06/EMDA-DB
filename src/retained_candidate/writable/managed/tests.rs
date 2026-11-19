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
            .build_retained_candidate(
                &candidate,
                [3; 16],
                [4; 16],
                RetainedExportLimits {
                    max_log_bytes: 8192,
                    max_live_keys: 32,
                    max_live_key_bytes: 256,
                    max_output_bytes: 8192,
                },
            )
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
        ..LIMITS
    };
    let f = Fixture::new(limits);
    let mut store = ManagedRetainedStore::open(&f.root, limits).unwrap();
    store.put(b"b", b"outside base quota").unwrap();
    let active = fs::read(f.root.join(ACTIVE_FILE)).unwrap();
    assert!(
        matches!(store.compact(), Err(PublicationFailure::NotPublished(ref e)) if e.kind() == io::ErrorKind::InvalidInput)
    );
    assert_eq!(fs::read(f.root.join(ACTIVE_FILE)).unwrap(), active);
    assert!(!Slot::B.path(&f.root).exists());
    drop(store);
    let reopened = ManagedRetainedStore::open(&f.root, limits).unwrap();
    assert_eq!(
        reopened.get(b"b", 32).unwrap(),
        Some(b"outside base quota".to_vec())
    );
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
    let f = Fixture::new(LIMITS);
    let mut store = f.open();
    store.compact().unwrap();
    let before = store.scan(None, None, 4).unwrap();
    let error = store
        .reclaim_using(|_| Err(io::Error::other("after move")))
        .unwrap_err();
    assert_eq!(error.to_string(), "after move");
    assert_eq!(store.scan(None, None, 4).unwrap(), before);
    assert!(!store.reclaim_inactive().unwrap());
    let cleaned = crate::cleanup_abandoned_staging(&f.root).unwrap();
    assert_eq!(cleaned.removed_payloads, 1);
    drop(store);
    assert_eq!(f.open().scan(None, None, 4).unwrap(), before);
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
    let mut store = ManagedRetainedStore::open(root, LIMITS).unwrap();
    let result = store.compact_using(
        || if point == "before" { pause() } else { Ok(()) },
        |_| if point == "after" { pause() } else { Ok(()) },
    );
    panic!("managed cutover did not stop at the boundary: {result:?}");
}

#[test]
fn killed_catalog_owner_recovers_the_visible_selection() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
        time::Duration,
    };

    for point in ["before", "after"] {
        let f = Fixture::new(LIMITS);
        let mut store = f.open();
        store.put(b"safe", b"acknowledged").unwrap();
        let old = store.generation_id().unwrap();
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "retained_candidate::writable::managed::tests::managed_cutover_crash_child",
                "--nocapture",
            ])
            .env("EMDA_MANAGED_RETAINED_CRASH_ROOT", &f.root)
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

        let mut recovered = f.open();
        assert_eq!(recovered.generation_id().unwrap() == old, point == "before");
        assert_eq!(
            recovered.get(b"safe", 16).unwrap(),
            Some(b"acknowledged".to_vec())
        );
        recovered.put(b"new", point.as_bytes()).unwrap();
        assert!(recovered.reclaim_inactive().unwrap());
        drop(recovered);
        assert_eq!(
            f.open().get(b"new", 8).unwrap(),
            Some(point.as_bytes().to_vec())
        );
    }
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
