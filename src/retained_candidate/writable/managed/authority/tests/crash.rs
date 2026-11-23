use super::*;
use crate::retained_candidate::writable::CommitPoint;
use crate::{CollectionSchema, DocumentDatabase, IndexDefinition, IndexQuery};
use serde_json::json;
use std::{
    cell::Cell,
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};

thread_local! {
    static COMMIT_POINT: Cell<Option<CommitPoint>> = const { Cell::new(None) };
}

const BOUNDARY: &str = "retained document commit boundary";
const CRASH_LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    overlay: OverlayLimits {
        key_bytes: 4096,
        ..LIMITS.overlay
    },
    ..LIMITS
};

fn pause() -> io::Result<()> {
    println!("{BOUNDARY}");
    io::stdout().flush()?;
    loop {
        std::thread::park();
    }
}

pub(in super::super) fn pause_at_commit_point(point: CommitPoint) -> io::Result<()> {
    if COMMIT_POINT.get() == Some(point) {
        pause()?;
    }
    Ok(())
}

struct PausingAuthority {
    file: FileRetainedAuthority,
    boundary: String,
}

impl RetainedAuthority for PausingAuthority {
    fn read(&mut self) -> io::Result<Option<AuthorityRecord>> {
        self.file.read()
    }

    fn compare_and_swap(
        &mut self,
        previous: Option<AuthorityRecord>,
        next: AuthorityRecord,
    ) -> io::Result<bool> {
        let intent = next.pending.is_some();
        if intent && self.boundary == "before-intent" {
            pause()?;
        }
        let changed = self.file.compare_and_swap(previous, next)?;
        let after = if intent {
            "after-intent"
        } else {
            "after-final"
        };
        if changed && self.boundary == after {
            pause()?;
        }
        Ok(changed)
    }
}

#[test]
fn document_commit_crash_child() {
    let Some(root) = std::env::var_os("EMDA_DOCUMENT_CRASH_CATALOG") else {
        return;
    };
    let authority = std::env::var_os("EMDA_DOCUMENT_CRASH_AUTHORITY").unwrap();
    let boundary = std::env::var("EMDA_DOCUMENT_CRASH_BOUNDARY").unwrap();
    COMMIT_POINT.set(match boundary.as_str() {
        "after-append" | "torn-tail" => Some(CommitPoint::Appended),
        "after-log-sync" => Some(CommitPoint::LogSynced),
        "after-witness-write" => Some(CommitPoint::WitnessWritten),
        "after-witness" => Some(CommitPoint::WitnessSynced),
        _ => None,
    });
    let provider = PausingAuthority {
        file: FileRetainedAuthority::open(authority).unwrap(),
        boundary,
    };
    let mut store = AuthorizedRetainedStore::open(root, CRASH_LIMITS, provider).unwrap();
    let db = DocumentDatabase::with_authorized_retained_store(&mut store);
    db.transaction(|tx| {
        tx.replace("items", "a", &json!({"rank": 2, "tag": "new"}), false)?;
        tx.insert("items", "b", &json!({"rank": 3, "tag": "new"}))?;
        tx.put_raw(b"receipt", b"committed")
    })
    .unwrap();
    panic!("the child should have paused before returning from the commit");
}

fn kill_at(root: &Path, authority: &Path, boundary: &str) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "retained_candidate::writable::managed::authority::tests::crash::document_commit_crash_child",
            "--nocapture",
        ])
        .env("EMDA_DOCUMENT_CRASH_CATALOG", root)
        .env("EMDA_DOCUMENT_CRASH_AUTHORITY", authority)
        .env("EMDA_DOCUMENT_CRASH_BOUNDARY", boundary);
    kill_paused(&mut command, boundary);
}

fn kill_paused(command: &mut Command, boundary: &str) {
    let mut child = command.stdout(Stdio::piped()).spawn().unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let (seen, notification) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in stdout.lines().map_while(Result::ok) {
            if line == BOUNDARY {
                let _ = seen.send(());
                break;
            }
        }
    });
    let reached = notification.recv_timeout(Duration::from_secs(15));
    let killed = child.kill();
    let status = child.wait().unwrap();
    reader.join().unwrap();
    reached.unwrap_or_else(|error| panic!("child missed {boundary}: {error}, {status}"));
    killed.unwrap();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(9)
    );
}

#[test]
fn recorded_restore_crash_child() {
    type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;
    let Some(root) = std::env::var_os("EMDA_RESTORE_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let boundary = std::env::var("EMDA_RESTORE_CRASH_BOUNDARY").unwrap();
    let provider = FileRetainedAuthority::open(root.join("source-authority")).unwrap();
    let mut source = Catalog::open(root.join("authorized"), LIMITS, provider).unwrap();
    let backup = root.join("backup");
    let binding = source.backup_to(&backup).unwrap();
    drop(source);
    let mut receipt = FileRetainedAuthority::create(root.join("restore-receipt")).unwrap();
    Catalog::prepare_from_backup_using(
        &backup,
        &root.join("restored"),
        LIMITS.into(),
        &binding,
        |parent| {
            if boundary == "after-rename" {
                pause()?;
            }
            sync_existing(parent)
        },
        |prepared| {
            assert!(receipt.compare_and_swap(None, prepared)?);
            if boundary == "before-rename" {
                pause()?;
            }
            Ok(())
        },
    )
    .unwrap();
    panic!("restore should pause before replying");
}

#[test]
fn recorded_backup_restore_survives_process_kills_around_publication() {
    type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;
    for boundary in ["before-rename", "after-rename"] {
        let fixture = Fixture::prepare();
        let root = fixture._tmp.path();
        let source_authority = root.join("source-authority");
        let mut provider = FileRetainedAuthority::create(&source_authority).unwrap();
        assert!(provider.compare_and_swap(None, fixture.initial).unwrap());
        let mut source = Catalog::open(
            &fixture.root,
            LIMITS,
            FileRetainedAuthority::open(&source_authority).unwrap(),
        )
        .unwrap();
        source.put(b"receipt", b"committed").unwrap();
        let committed = provider.read().unwrap().unwrap();
        let catalog_log = Slot::A.path(&fixture.root).join("records.log");
        let original_log = fs::read(&catalog_log).unwrap();
        drop(source);

        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "retained_candidate::writable::managed::authority::tests::crash::recorded_restore_crash_child",
                "--nocapture",
            ])
            .env("EMDA_RESTORE_CRASH_ROOT", root)
            .env("EMDA_RESTORE_CRASH_BOUNDARY", boundary);
        kill_paused(&mut command, boundary);

        let prepared = FileRetainedAuthority::open(root.join("restore-receipt"))
            .unwrap()
            .read()
            .unwrap()
            .unwrap();
        assert_eq!(
            prepared.committed.suffix_digest,
            committed.committed.suffix_digest
        );
        assert_ne!(prepared.committed.catalog, committed.committed.catalog);
        let destination = root.join("restored");
        let fresh = root.join("fresh-authority");
        let authority = FileRetainedAuthority::create(&fresh).unwrap();
        let registered = Catalog::register_prepared(&destination, LIMITS, authority, prepared);
        if boundary == "before-rename" {
            assert!(matches!(registered, Err(error) if error.kind() == io::ErrorKind::NotFound));
            assert_eq!(
                FileRetainedAuthority::open(&fresh).unwrap().read().unwrap(),
                None
            );
            assert!(!destination.exists());
        } else {
            let mut restored = registered.unwrap();
            assert_eq!(restored.get(b"base", 8).unwrap().unwrap(), b"imported");
            assert_eq!(restored.get(b"receipt", 9).unwrap().unwrap(), b"committed");
            assert_eq!(
                FileRetainedAuthority::open(&fresh).unwrap().read().unwrap(),
                Some(prepared)
            );
        }
        crate::cleanup_abandoned_staging(root).unwrap();
        assert_eq!(destination.exists(), boundary == "after-rename");
        assert_eq!(provider.read().unwrap(), Some(committed));
        assert_eq!(fs::read(&catalog_log).unwrap(), original_log);
    }
}

#[test]
fn process_kills_recover_document_indexes_and_raw_receipt_together() {
    for boundary in [
        "before-intent",
        "after-intent",
        "after-append",
        "after-log-sync",
        "after-witness-write",
        "after-witness",
        "after-final",
        "torn-tail",
    ] {
        let f = Fixture::prepare_with_limits(CRASH_LIMITS);
        let authority_path = f._tmp.path().join("authority");
        let mut authority = FileRetainedAuthority::create(&authority_path).unwrap();
        assert!(authority.compare_and_swap(None, f.initial).unwrap());
        {
            let file = FileRetainedAuthority::open(&authority_path).unwrap();
            let mut store = AuthorizedRetainedStore::open(&f.root, CRASH_LIMITS, file).unwrap();
            let db = DocumentDatabase::with_authorized_retained_store(&mut store);
            db.create_collection("items", CollectionSchema::default())
                .unwrap();
            for (field, unique) in [("rank", true), ("tag", false)] {
                db.create_index(
                    "items",
                    IndexDefinition {
                        name: field.into(),
                        fields: vec![vec![field.into()]],
                        unique,
                        partial: None,
                    },
                )
                .unwrap();
            }
            db.insert("items", "a", &json!({"rank": 1, "tag": "old"}))
                .unwrap();
        }
        let before = authority.read().unwrap().unwrap();
        let generation = Slot::A.path(&f.root);
        let log_path = generation.join("records.log");
        let witness_path = generation.join("commit.state");
        let log_before = fs::read(&log_path).unwrap();
        let witness_before = fs::read(&witness_path).unwrap();

        kill_at(&f.root, &authority_path, boundary);
        let visible = authority.read().unwrap().unwrap();
        let complete_log = fs::read(&log_path).unwrap();
        let witness = fs::read(&witness_path).unwrap();
        let appended = !matches!(boundary, "before-intent" | "after-intent");
        assert!(complete_log.starts_with(&log_before), "{boundary}");
        assert_eq!(
            complete_log.len() > log_before.len(),
            appended,
            "{boundary}"
        );
        assert_eq!(
            witness != witness_before,
            matches!(
                boundary,
                "after-witness-write" | "after-witness" | "after-final"
            ),
            "{boundary}"
        );
        match boundary {
            "before-intent" => assert_eq!(visible, before),
            "after-final" => {
                assert_eq!(visible.revision, before.revision + 2, "{boundary}");
                assert!(visible.pending.is_none(), "{boundary}");
                assert!(
                    visible.committed.suffix_end > before.committed.suffix_end,
                    "{boundary}"
                );
            }
            _ => {
                assert_eq!(visible.revision, before.revision + 1, "{boundary}");
                assert_eq!(visible.committed, before.committed, "{boundary}");
                assert!(visible.pending.is_some(), "{boundary}");
            }
        }

        let open = || {
            AuthorizedRetainedStore::open(
                &f.root,
                CRASH_LIMITS,
                FileRetainedAuthority::open(&authority_path).unwrap(),
            )
        };
        if boundary == "after-witness-write" {
            let proposed = visible.pending.unwrap();
            assert_eq!(
                u64::from_le_bytes(witness[40..48].try_into().unwrap()),
                proposed.suffix_end
            );
            let result = AuthorizedRetainedStore::open_using(
                &f.root,
                CRASH_LIMITS,
                FileRetainedAuthority::open(&authority_path).unwrap(),
                None,
                |store| {
                    assert_eq!(
                        AuthorityPoint::from_store(proposed.catalog, store),
                        proposed
                    );
                    store.log.sync_data()?;
                    Err(io::Error::other("injected recovery witness sync failure"))
                },
            );
            assert_eq!(
                result
                    .err()
                    .expect("sync failure before recovery CAS")
                    .to_string(),
                "injected recovery witness sync failure"
            );
            assert_eq!(authority.read().unwrap(), Some(visible));
            assert_eq!(fs::read(&log_path).unwrap(), complete_log);
            assert_eq!(fs::read(&witness_path).unwrap(), witness);
        }
        if boundary == "torn-tail" {
            let torn = &complete_log[..complete_log.len() - 1];
            fs::write(&log_path, torn).unwrap();
            assert!(matches!(open(), Err(error)
                    if error.kind() == io::ErrorKind::InvalidData
                        && error.to_string() == "retained suffix length does not match independent authority"));
            assert_eq!(fs::read(&log_path).unwrap(), torn);
            assert_eq!(fs::read(&witness_path).unwrap(), witness_before);
            assert_eq!(authority.read().unwrap(), Some(visible));
            fs::write(&log_path, &complete_log).unwrap();
        }
        let mut recovered = open().unwrap_or_else(|error| panic!("{boundary}: {error}"));
        let committed = if appended {
            visible.pending.unwrap_or(visible.committed)
        } else {
            before.committed
        };
        let resolved = if visible.pending.is_some() {
            visible.finalized(committed).unwrap()
        } else {
            visible
        };
        assert_eq!(authority.read().unwrap(), Some(resolved), "{boundary}");
        assert_eq!(
            AuthorityPoint::from_store(recovered.inner.active.catalog, &recovered.inner.store),
            resolved.committed,
            "{boundary}"
        );
        let db = DocumentDatabase::with_authorized_retained_store(&mut recovered);
        let a = if appended {
            json!({"rank": 2, "tag": "new"})
        } else {
            json!({"rank": 1, "tag": "old"})
        };
        assert_eq!(db.get("items", "a").unwrap(), Some(a), "{boundary}");
        assert_eq!(
            db.get("items", "b").unwrap(),
            appended.then(|| json!({"rank": 3, "tag": "new"})),
            "{boundary}",
        );
        for field in ["rank", "tag"] {
            let page = db
                .query_index("items", field, &IndexQuery::default())
                .unwrap();
            assert_eq!(
                page.documents.len(),
                if appended { 2 } else { 1 },
                "{boundary}"
            );
        }
        db.verify_collection("items").unwrap();
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
            appended.then(|| b"committed".to_vec()),
            "{boundary}"
        );
        recovered.put(b"subsequent", b"acknowledged").unwrap();
        drop(recovered);
        assert_eq!(
            open().unwrap().get(b"subsequent", 12).unwrap().as_deref(),
            Some(b"acknowledged".as_slice())
        );
    }
}
