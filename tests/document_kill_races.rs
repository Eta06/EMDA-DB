//! Process-kill races across acknowledged and in-flight document transactions.
//! These tests do not simulate loss of OS/device caches or physical power.
use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    AuthorityRecord, AuthorizedRetainedStore, FileRetainedAuthority, IndexLimits,
    ManagedRetainedStore, OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery, IndexValue,
    RetainedExportLimits, Store,
};
use serde_json::json;
use std::{
    env, fs,
    io::{self, ErrorKind, Read, Write},
    num::NonZeroU64,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

mod support;
use support::retry_lock;

const RETAINED_LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 65_536,
        entries: 256,
        key_bytes: 8_192,
        work_bytes: 65_537,
    },
    index: IndexLimits {
        blocks: 4,
        anchor_bytes: 1_024,
    },
    suffix: SuffixLimits {
        bytes: 65_536,
        operations: 256,
        work: 65_536,
    },
    overlay: OverlayLimits {
        entries: 256,
        key_bytes: 8_192,
    },
};

fn initialize(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "number".into(),
            fields: vec![vec!["n".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
}

fn acknowledge_documents(db: &DocumentDatabase<'_>) {
    for number in 0..3 {
        let id = format!("acked-{number}");
        let receipt = format!("receipt-{number}");
        db.transaction(|tx| {
            tx.insert("c", &id, &json!({"n":number}))?;
            tx.put_raw(receipt.as_bytes(), id.as_bytes())
        })
        .unwrap();
    }
}

fn insert_pending(db: &DocumentDatabase<'_>) {
    db.transaction(|tx| {
        tx.insert("c", "pending", &json!({"n":99}))?;
        tx.put_raw(b"pending-receipt", b"pending")
    })
    .unwrap();
}

fn child_work(db: &DocumentDatabase<'_>, signal: &Path) -> ! {
    acknowledge_documents(db);
    // All three transactions returned successfully before this signal. The
    // parent captures their log prefix while this child is paused on stdin.
    fs::write(signal, b"acknowledged").unwrap();
    let mut go = [0_u8; 1];
    io::stdin().read_exact(&mut go).unwrap();
    insert_pending(db);
    if env::var_os("EMDA_DOCUMENT_KILL_WAIT_FOR_PENDING").is_some() {
        fs::write(signal.with_extension("complete"), b"acknowledged").unwrap();
    }
    loop {
        thread::park();
    }
}

#[test]
fn document_kill_race_child() {
    let Ok(mode) = env::var("EMDA_DOCUMENT_KILL_CHILD") else {
        return;
    };
    let path = env::var_os("EMDA_DOCUMENT_KILL_PATH").unwrap();
    let signal = env::var_os("EMDA_DOCUMENT_KILL_SIGNAL").unwrap();
    if mode == "ordinary" {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        child_work(&DocumentDatabase::new(&store), Path::new(&signal));
    } else if mode == "retained" {
        let mut store = retry_lock(|| {
            ManagedRetainedStore::open_with_auto_reclaim(&path, RETAINED_LIMITS, NonZeroU64::MIN)
        })
        .unwrap();
        child_work(
            &DocumentDatabase::with_managed_retained_store(&mut store),
            Path::new(&signal),
        );
    } else if mode == "authorized" {
        let root = Path::new(&path).parent().unwrap();
        let (revision, when) = env::var("EMDA_DOCUMENT_KILL_AUTHORITY_BOUNDARY")
            .unwrap()
            .split_once('-')
            .map(|(revision, when)| (revision.parse().unwrap(), when == "after"))
            .unwrap();
        let provider = PausedAuthority {
            inner: FileRetainedAuthority::open(root.join("authority")).unwrap(),
            boundary: (revision, when),
            signal: signal.into(),
        };
        let mut store = AuthorizedRetainedStore::open_with_auto_reclaim(
            &path,
            RETAINED_LIMITS,
            provider,
            NonZeroU64::MIN,
        )
        .unwrap();
        insert_pending(&DocumentDatabase::with_authorized_retained_store(
            &mut store,
        ));
        panic!("child missed its authority boundary");
    } else {
        let checkpoint = env::var_os("EMDA_DOCUMENT_KILL_CHECKPOINT").unwrap();
        let digest: [u8; 32] = fs::read(env::var_os("EMDA_DOCUMENT_KILL_DIGEST").unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        child_work(
            &DocumentDatabase::with_checkpoint_store(&store),
            Path::new(&signal),
        );
    }
}

struct PausedAuthority {
    inner: FileRetainedAuthority,
    boundary: (u64, bool),
    signal: std::path::PathBuf,
}

impl PausedAuthority {
    fn pause(&self, revision: u64, after: bool) -> io::Result<()> {
        if self.boundary == (revision, after) {
            fs::write(&self.signal, b"authority boundary")?;
            loop {
                thread::park();
            }
        }
        Ok(())
    }
}

impl RetainedAuthority for PausedAuthority {
    fn read(&mut self) -> io::Result<Option<AuthorityRecord>> {
        self.inner.read()
    }

    fn compare_and_swap(
        &mut self,
        previous: Option<AuthorityRecord>,
        next: AuthorityRecord,
    ) -> io::Result<bool> {
        self.pause(next.revision, false)?;
        let replaced = self.inner.compare_and_swap(previous, next)?;
        if replaced {
            self.pause(next.revision, true)?;
        }
        Ok(replaced)
    }
}

#[test]
fn killed_authorized_document_writer_recovers_at_each_rotation_and_append_update() {
    type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;
    for revision in 3..=6 {
        for when in ["before", "after"] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            let authority = root.path().join("authority");
            let signal = root.path().join("signal");
            let original = Store::open(&path).unwrap();
            let db = DocumentDatabase::new(&original);
            initialize(&db);
            acknowledge_documents(&db);
            let initial = Catalog::cutover_from_store(
                original,
                &path,
                RetainedExportLimits {
                    max_log_bytes: 65_536,
                    max_live_keys: 256,
                    max_live_key_bytes: 8_192,
                    max_output_bytes: 65_536,
                },
                RETAINED_LIMITS,
            )
            .unwrap();
            let mut provider = FileRetainedAuthority::create(&authority).unwrap();
            assert!(provider.compare_and_swap(None, initial).unwrap());
            let mut store = Catalog::open(&path, RETAINED_LIMITS, provider).unwrap();
            store.put(b"rotation-trigger", b"acknowledged").unwrap();
            drop(store);

            let boundary = format!("{revision}-{when}");
            let mut child = Command::new(env::current_exe().unwrap())
                .args(["--exact", "document_kill_race_child"])
                .env("EMDA_DOCUMENT_KILL_CHILD", "authorized")
                .env("EMDA_DOCUMENT_KILL_PATH", &path)
                .env("EMDA_DOCUMENT_KILL_SIGNAL", &signal)
                .env("EMDA_DOCUMENT_KILL_AUTHORITY_BOUNDARY", &boundary)
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(20);
            let reached = loop {
                if signal.exists() {
                    break true;
                }
                if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                    break false;
                }
                thread::sleep(Duration::from_millis(1));
            };
            let killed = child.kill();
            let status = child.wait().unwrap();
            assert!(reached, "child did not reach authority boundary {boundary}");
            killed.unwrap();
            assert!(!status.success());

            let mut provider = FileRetainedAuthority::open(&authority).unwrap();
            let interrupted = provider.read().unwrap().unwrap();
            assert_eq!(
                interrupted.revision,
                if when == "before" {
                    revision - 1
                } else {
                    revision
                }
            );
            let mut recovered =
                Catalog::open_with_auto_reclaim(&path, RETAINED_LIMITS, provider, NonZeroU64::MIN)
                    .unwrap();
            assert_eq!(
                recovered.get(b"rotation-trigger", 12).unwrap().unwrap(),
                b"acknowledged"
            );
            let db = DocumentDatabase::with_authorized_retained_store(&mut recovered);
            assert_eq!(verify_recovered(&db), revision == 6, "boundary {boundary}");
        }
    }
}

#[test]
fn killed_retained_document_writers_preserve_acknowledgements_during_reclamation() {
    let repetitions = env::var("EMDA_RETAINED_DOCUMENT_KILL_REPETITIONS")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(2);
    assert!((1..=100).contains(&repetitions));
    let (mut recovered_count, mut fail_closed_count, mut pending_present_count) = (0, 0, 0);
    for _ in 0..repetitions {
        for delay in [0, 50, 2_000, 200_000] {
            let root = tempfile::tempdir().unwrap();
            let ordinary = root.path().join("ordinary");
            let catalog = root.path().join("catalog");
            let signal = root.path().join("signal");
            let initial = Store::open(&ordinary).unwrap();
            initialize(&DocumentDatabase::new(&initial));
            let original_id = ManagedRetainedStore::copy_from_store(
                initial,
                &ordinary,
                &catalog,
                RetainedExportLimits {
                    max_log_bytes: 65_536,
                    max_live_keys: 256,
                    max_live_key_bytes: 8_192,
                    max_output_bytes: 65_536,
                },
                RETAINED_LIMITS,
            )
            .unwrap();
            interrupt_writer(
                "retained",
                &catalog,
                &signal,
                Path::new(""),
                Path::new(""),
                Some(delay),
                || (),
            );
            let files = [
                "active.retained",
                "generation-a/records.log",
                "generation-a/commit.state",
                "generation-b/records.log",
                "generation-b/commit.state",
            ];
            let interrupted = files.map(|file| fs::read(catalog.join(file)).ok());
            match retry_lock(|| {
                ManagedRetainedStore::open_with_auto_reclaim(
                    &catalog,
                    RETAINED_LIMITS,
                    NonZeroU64::MIN,
                )
            }) {
                Ok(mut recovered) => {
                    assert_ne!(recovered.generation_id().unwrap(), original_id);
                    let db = DocumentDatabase::with_managed_retained_store(&mut recovered);
                    pending_present_count += usize::from(verify_recovered(&db));
                    recovered_count += 1;
                }
                Err(error) => {
                    assert!(
                        matches!(
                            error.kind(),
                            ErrorKind::InvalidData | ErrorKind::UnexpectedEof
                        ),
                        "unexpected retained recovery error: {error}"
                    );
                    assert_eq!(
                        files.map(|file| fs::read(catalog.join(file)).ok()),
                        interrupted
                    );
                    fail_closed_count += 1;
                }
            }
        }
    }
    assert!(
        recovered_count >= repetitions,
        "retained recovery never reaches the acknowledged state"
    );
    println!(
        "mode=retained repetitions={repetitions} recovered={recovered_count} pending_present={pending_present_count} failed_closed={fail_closed_count}"
    );
}

fn verify_recovered(db: &DocumentDatabase<'_>) -> bool {
    for number in 0..3 {
        let id = format!("acked-{number}");
        let receipt = format!("receipt-{number}");
        assert_eq!(db.get("c", &id).unwrap(), Some(json!({"n":number})));
        assert_eq!(
            db.transaction(|tx| tx.get_raw(receipt.as_bytes())).unwrap(),
            Some(id.as_bytes().to_vec())
        );
        let entries = db
            .query_index(
                "c",
                "number",
                &IndexQuery {
                    equal: vec![IndexValue::Integer(number)],
                    ..Default::default()
                },
            )
            .unwrap()
            .documents;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, id);
    }
    let pending = db.get("c", "pending").unwrap();
    let receipt = db.transaction(|tx| tx.get_raw(b"pending-receipt")).unwrap();
    let indexed = db
        .query_index(
            "c",
            "number",
            &IndexQuery {
                equal: vec![IndexValue::Integer(99)],
                ..Default::default()
            },
        )
        .unwrap()
        .documents;
    match pending {
        None => {
            assert_eq!(receipt, None);
            assert!(indexed.is_empty());
        }
        Some(value) => {
            assert_eq!(value, json!({"n":99}));
            assert_eq!(receipt.as_deref(), Some(&b"pending"[..]));
            assert_eq!(indexed.len(), 1);
            assert_eq!(indexed[0].id, "pending");
        }
    }
    let integrity = db.verify_collection("c").unwrap();
    assert_eq!(integrity.documents, 3 + usize::from(receipt.is_some()));
    receipt.is_some()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InterruptedImage {
    log: Vec<u8>,
    witness_file: Vec<u8>,
    metadata_witness: Option<[u8; 36]>,
}

impl InterruptedImage {
    fn capture(path: &Path) -> Self {
        let log_path = path.join("records.log");
        let log = fs::read(&log_path).unwrap();
        let metadata_witness = if log.starts_with(b"EMDALOG3") {
            #[cfg(target_os = "linux")]
            {
                let mut bytes = [0; 36];
                assert_eq!(
                    rustix::fs::getxattr(&log_path, "user.db.commit.v3", &mut bytes[..]).unwrap(),
                    bytes.len()
                );
                Some(bytes)
            }
            #[cfg(not(target_os = "linux"))]
            panic!("unexpected V3 fixture")
        } else {
            assert!(log.starts_with(b"EMDALOG2"));
            None
        };
        Self {
            log,
            witness_file: fs::read(path.join("commit.state")).unwrap(),
            metadata_witness,
        }
    }

    fn write_to(&self, path: &Path) {
        fs::create_dir(path).unwrap();
        let log = path.join("records.log");
        fs::write(&log, &self.log).unwrap();
        fs::write(path.join("commit.state"), &self.witness_file).unwrap();
        fs::File::create(path.join("store.lock")).unwrap();
        #[cfg(target_os = "linux")]
        if let Some(witness) = &self.metadata_witness {
            rustix::fs::setxattr(
                &log,
                "user.db.commit.v3",
                witness,
                rustix::fs::XattrFlags::empty(),
            )
            .unwrap();
        }
        assert_eq!(Self::capture(path), *self, "copied crash image differs");
    }
}

fn recover_killed_document_image(
    image: &InterruptedImage,
    checkpoint: &Path,
    digest: &[u8; 32],
) -> (Option<bool>, InterruptedImage) {
    fn check<T>(opened: io::Result<T>, verify: impl FnOnce(T) -> bool) -> Option<bool> {
        match opened {
            Ok(store) => Some(verify(store)),
            Err(error) => {
                assert!(
                    matches!(
                        error.kind(),
                        ErrorKind::InvalidData | ErrorKind::UnexpectedEof
                    ),
                    "unexpected recovery error: {error}"
                );
                None
            }
        }
    }

    let root = tempfile::tempdir_in(checkpoint.parent().unwrap()).unwrap();
    let ordinary = root.path().join("ordinary");
    let checkpoint_store = root.path().join("checkpoint");
    image.write_to(&ordinary);
    image.write_to(&checkpoint_store);
    let first = check(retry_lock(|| Store::open(&ordinary)), |store| {
        verify_recovered(&DocumentDatabase::new(&store))
    });
    let second = check(
        retry_lock(|| CheckpointStore::open(&checkpoint_store, checkpoint, digest)),
        |store| verify_recovered(&DocumentDatabase::with_checkpoint_store(&store)),
    );
    assert_eq!(first, second, "independent recovery paths disagree");
    let first_bytes = InterruptedImage::capture(&ordinary);
    let second_bytes = InterruptedImage::capture(&checkpoint_store);
    assert_eq!(
        first_bytes, second_bytes,
        "recovery changed the copies differently"
    );
    assert_eq!(first_bytes.log, image.log, "recovery modified the log");
    if first.is_none() {
        assert_eq!(first_bytes, *image, "failed recovery changed the witness");
    }
    if image.metadata_witness.is_some() {
        assert_eq!(
            first_bytes.witness_file, image.witness_file,
            "V3 anchor changed"
        );
    }
    (first, first_bytes)
}

#[test]
fn killed_document_writers_recover_acknowledged_transactions_or_fail_closed() {
    let repetitions = env::var("EMDA_DOCUMENT_KILL_REPETITIONS")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(2);
    assert!((1..=100).contains(&repetitions));
    for force_v2 in [false, true] {
        if force_v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for mode in ["ordinary", "checkpoint"] {
            let (mut recovered, mut fail_closed, mut present, mut adopted) = (0, 0, 0, 0);
            let mut format = "";
            for delay in [None]
                .into_iter()
                .chain((0..repetitions).flat_map(|_| [0, 50, 500, 2_000].map(Some)))
            {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("store");
                let checkpoint = root.path().join("checkpoint");
                let digest_file = root.path().join("digest");
                let signal = root.path().join("signal");
                if force_v2 {
                    fs::create_dir(&path).unwrap();
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
                let store = Store::open(&path).unwrap();
                initialize(&DocumentDatabase::new(&store));
                let digest = store.checkpoint_to(&checkpoint).unwrap();
                fs::write(&digest_file, digest).unwrap();
                drop(store);
                let acknowledged = interrupt_writer(
                    mode,
                    &path,
                    &signal,
                    &checkpoint,
                    &digest_file,
                    delay,
                    || InterruptedImage::capture(&path),
                );
                let interrupted = InterruptedImage::capture(&path);
                assert!(interrupted.log.starts_with(&acknowledged.log));
                format = if interrupted.metadata_witness.is_some() {
                    "V3"
                } else {
                    "V2"
                };
                let (result, after) =
                    recover_killed_document_image(&interrupted, &checkpoint, &digest);
                if delay.is_none() {
                    assert_eq!(result, Some(true));
                    // Model persistence states using the actual pending
                    // document batch and the last acknowledged witness.
                    let mut without_new_witness = acknowledged.clone();
                    without_new_witness.log = interrupted.log.clone();
                    let (result, after) =
                        recover_killed_document_image(&without_new_witness, &checkpoint, &digest);
                    assert_eq!(result, Some(true), "complete tail must be adopted");
                    assert_eq!(
                        after, interrupted,
                        "adoption advanced to the wrong boundary"
                    );

                    without_new_witness.log.pop().unwrap();
                    assert!(without_new_witness.log.len() > acknowledged.log.len());
                    assert_eq!(
                        recover_killed_document_image(&without_new_witness, &checkpoint, &digest).0,
                        None,
                        "torn tail was accepted"
                    );
                    assert_eq!(
                        recover_killed_document_image(&acknowledged, &checkpoint, &digest).0,
                        Some(false),
                        "old witness and complete prefix must reopen"
                    );
                } else {
                    match result {
                        Some(pending) => {
                            recovered += 1;
                            present += usize::from(pending);
                            adopted += usize::from(after != interrupted);
                        }
                        None => fail_closed += 1,
                    }
                }
                assert_eq!(InterruptedImage::capture(&path), interrupted);
            }
            println!(
                "format={format} mode={mode} repetitions={repetitions} recovered={recovered} pending_present={present} fail_closed={fail_closed} adopted={adopted} deterministic_images=4"
            );
        }
    }
}

fn interrupt_writer<T>(
    mode: &str,
    path: &Path,
    signal: &Path,
    checkpoint: &Path,
    digest_file: &Path,
    delay: Option<u64>,
    capture: impl FnOnce() -> T,
) -> T {
    let mut command = Command::new(env::current_exe().unwrap());
    if delay.is_none() {
        command.env("EMDA_DOCUMENT_KILL_WAIT_FOR_PENDING", "1");
    }
    let mut child = command
        .arg("--exact")
        .arg("document_kill_race_child")
        .env("EMDA_DOCUMENT_KILL_CHILD", mode)
        .env("EMDA_DOCUMENT_KILL_PATH", path)
        .env("EMDA_DOCUMENT_KILL_SIGNAL", signal)
        .env("EMDA_DOCUMENT_KILL_CHECKPOINT", checkpoint)
        .env("EMDA_DOCUMENT_KILL_DIGEST", digest_file)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !signal.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("child exited before acknowledgements: {status}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not acknowledge");
        }
        thread::sleep(Duration::from_millis(1));
    }
    let before = capture();
    child.stdin.take().unwrap().write_all(b"x").unwrap();
    if let Some(delay) = delay {
        if delay > 0 {
            thread::sleep(Duration::from_micros(delay));
        }
    } else {
        let complete = signal.with_extension("complete");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !complete.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before the final acknowledgement: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not acknowledge the final transaction");
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
    if let Some(status) = child.try_wait().unwrap() {
        panic!("child exited before interruption: {status}");
    }
    child.kill().unwrap();
    child.wait().unwrap();
    before
}
