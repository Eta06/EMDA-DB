//! Deadline races must not fracture document, index, and raw-key commits.
use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentTransactionMode, IndexDefinition,
    IndexQuery, IndexValue, Store, TimedTransactionError,
};
use serde_json::json;
use std::cell::Cell;
use std::io::ErrorKind;
use std::sync::{Barrier, Condvar, Mutex};
use std::time::{Duration, Instant};

mod support;
use support::retry_lock;

const WRITERS: usize = 6;

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Committed,
    Expired,
    // WouldBlock can mean a conflict, admission rejection, or I/O error.
    OperationWouldBlock,
    // An operation timeout is not an engine-confirmed abort; a commit I/O
    // timeout can have an unknown outcome even if the deadline also expired.
    OperationTimedOut,
}

#[derive(Debug, PartialEq, Eq)]
struct Observed {
    counter: u8,
    present: [bool; WRITERS],
}

#[derive(Default)]
struct StartGate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl StartGate {
    fn wait(&self) {
        let open = self.open.lock().unwrap();
        drop(self.changed.wait_while(open, |open| !*open).unwrap());
    }

    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

struct ReleaseOnDrop<'a>(&'a StartGate);

impl Drop for ReleaseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl Backend {
    fn database(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }
}

fn counter_keys() -> (&'static str, &'static str) {
    ("left", "right")
}

fn document_id(writer: usize) -> String {
    format!("writer-{writer}")
}

fn marker_key(id: &str) -> String {
    format!("marker/{id}")
}

fn read_counter(value: Option<Vec<u8>>) -> u8 {
    let value = value.expect("counter exists");
    assert_eq!(value.len(), 1);
    value[0]
}

fn inspect(db: &DocumentDatabase<'_>) -> Observed {
    db.transaction(|tx| {
        let (left, right) = counter_keys();
        let left = read_counter(tx.get_raw(left.as_bytes())?);
        let right = read_counter(tx.get_raw(right.as_bytes())?);
        assert_eq!(left, right, "counter pair was not atomic");
        let mut present = [false; WRITERS];
        for (writer, slot) in present.iter_mut().enumerate() {
            let id = document_id(writer);
            let marker = tx.get_raw(marker_key(&id).as_bytes())?;
            let document = tx.get("records", &id)?;
            let indexed = tx.query_index(
                "records",
                "tag",
                &IndexQuery {
                    equal: vec![IndexValue::String(id.clone())],
                    ..Default::default()
                },
            )?;
            if let Some(document) = document {
                assert_eq!(document, json!({"tag": id}));
                assert_eq!(marker.as_deref(), Some(&b"committed"[..]));
                assert_eq!(indexed.documents.len(), 1);
                assert_eq!(indexed.documents[0].id, id);
                *slot = true;
            } else {
                assert!(marker.is_none(), "raw marker without its document");
                assert!(
                    indexed.documents.is_empty(),
                    "index entry without its document"
                );
            }
        }
        assert_eq!(
            usize::from(left),
            present.iter().filter(|value| **value).count()
        );
        Ok(Observed {
            counter: left,
            present,
        })
    })
    .unwrap()
}

fn run_writer(db: &DocumentDatabase<'_>, writer: usize, gate: &StartGate) -> (Outcome, bool) {
    // Begin competing writes only after writer 1 has staged its changes. If
    // it fails or panics before reaching that point, always release waiters.
    let _release = (writer == 1).then(|| ReleaseOnDrop(gate));
    if writer != 1 {
        gate.wait();
    }
    let mode = match writer % 3 {
        0 => DocumentTransactionMode::Conservative,
        1 => DocumentTransactionMode::ReadSet,
        _ => DocumentTransactionMode::ReadSetFifo,
    };
    let budget = match writer {
        0 => Duration::ZERO,
        // This writer attempts to stage its entire transaction before expiry.
        1 => Duration::from_millis(50),
        2 | 3 => Duration::from_millis(2),
        _ => Duration::from_secs(10),
    };
    let deadline = Instant::now() + budget;
    let staged = Cell::new(false);
    let result = db.transaction_until(deadline, mode, |tx| {
        let (left, right) = counter_keys();
        let before_left = read_counter(tx.get_raw(left.as_bytes())?);
        let before_right = read_counter(tx.get_raw(right.as_bytes())?);
        assert_eq!(
            before_left, before_right,
            "transaction saw a fractured pair"
        );
        let next = before_left.checked_add(1).unwrap();
        tx.put_raw(left.as_bytes(), &[next])?;
        std::thread::yield_now();
        tx.put_raw(right.as_bytes(), &[next])?;
        let id = document_id(writer);
        tx.insert("records", &id, &json!({"tag": id}))?;
        tx.put_raw(marker_key(&id).as_bytes(), b"committed")?;
        staged.set(true);
        if writer == 1 {
            gate.release();
            std::thread::sleep(
                deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
            );
        }
        Ok(())
    });
    let outcome = match result {
        Ok(()) => Outcome::Committed,
        Err(TimedTransactionError::DeadlineExceeded) => Outcome::Expired,
        Err(TimedTransactionError::Operation(error)) if error.kind() == ErrorKind::WouldBlock => {
            Outcome::OperationWouldBlock
        }
        Err(TimedTransactionError::Operation(error)) if error.kind() == ErrorKind::TimedOut => {
            Outcome::OperationTimedOut
        }
        Err(error) => panic!("unexpected writer {writer} error: {error}"),
    };
    (outcome, staged.get())
}

#[test]
fn concurrent_deadlines_preserve_atomic_documents_indexes_and_recovery() {
    let histories = std::env::var("EMDA_DEADLINE_RACE_HISTORIES")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(24);
    assert!((1..=1024).contains(&histories));
    for checkpoint in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint_path = root.path().join("checkpoint");
        let initial = retry_lock(|| Store::open(&path)).unwrap();
        let db = DocumentDatabase::new(&initial);
        db.create_collection("records", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "records",
            IndexDefinition {
                name: "tag".into(),
                fields: vec![vec!["tag".into()]],
                unique: true,
                partial: None,
            },
        )
        .unwrap();
        let digest = initial.checkpoint_to(&checkpoint_path).unwrap();
        drop(initial);

        let mut expired = 0;
        let mut committed = 0;
        let mut staged_before_expiry = 0;
        let mut would_block = 0;
        let mut would_block_present = 0;
        let mut operation_timeouts = 0;
        let mut timeout_present = 0;
        for round in 0..histories {
            let backend = if checkpoint {
                Backend::Checkpoint(
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint_path, &digest)).unwrap(),
                )
            } else {
                Backend::Ordinary(retry_lock(|| Store::open(&path)).unwrap())
            };
            let db = backend.database();
            let (left, right) = counter_keys();
            db.transaction(|tx| {
                tx.put_raw(left.as_bytes(), &[0])?;
                tx.put_raw(right.as_bytes(), &[0])
            })
            .unwrap();
            let barrier = Barrier::new(WRITERS + 2);
            let gate = StartGate::default();
            let results = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for writer in 0..WRITERS {
                    let (db, barrier, gate) = (&db, &barrier, &gate);
                    handles.push(scope.spawn(move || {
                        barrier.wait();
                        run_writer(db, writer, gate)
                    }));
                }
                let observer = scope.spawn(|| {
                    barrier.wait();
                    for _ in 0..8 {
                        let result = db.transaction_with_read_set(|tx| {
                            let first = read_counter(tx.get_raw(left.as_bytes())?);
                            let second = read_counter(tx.get_raw(right.as_bytes())?);
                            assert_eq!(first, second, "reader saw a fractured pair");
                            Ok(())
                        });
                        match result {
                            Ok(()) => {}
                            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                            Err(error) => panic!("unexpected observer error: {error}"),
                        }
                        std::thread::yield_now();
                    }
                });
                barrier.wait();
                let results = handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect::<Vec<_>>();
                observer.join().unwrap();
                results
            });
            let observed = inspect(&db);
            for (writer, (outcome, staged)) in results.into_iter().enumerate() {
                match outcome {
                    Outcome::Committed => {
                        committed += 1;
                        assert!(observed.present[writer], "acknowledged write is missing");
                    }
                    Outcome::Expired => {
                        expired += 1;
                        assert!(!observed.present[writer], "expired write was submitted");
                        if writer == 1 && staged {
                            staged_before_expiry += 1;
                        }
                    }
                    Outcome::OperationWouldBlock => {
                        would_block += 1;
                        would_block_present += usize::from(observed.present[writer]);
                    }
                    Outcome::OperationTimedOut => {
                        operation_timeouts += 1;
                        timeout_present += usize::from(observed.present[writer]);
                    }
                }
            }
            let total_documents = db.verify_collection("records").unwrap().documents;
            drop(backend);
            let recovered = retry_lock(|| Store::open(&path)).unwrap();
            let recovered_db = DocumentDatabase::new(&recovered);
            assert_eq!(
                inspect(&recovered_db),
                observed,
                "checkpoint={checkpoint} round={round} recovery"
            );
            assert_eq!(
                recovered_db.verify_collection("records").unwrap().documents,
                total_documents
            );
            // Reuse the same IDs so long runs remain within collection and
            // checkpoint-overlay limits. Cleanup happens after recovery checks.
            recovered_db
                .transaction(|tx| {
                    for writer in 0..WRITERS {
                        if observed.present[writer] {
                            let id = document_id(writer);
                            assert!(tx.delete("records", &id)?);
                            tx.delete_raw(marker_key(&id).as_bytes())?;
                        }
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                recovered_db.verify_collection("records").unwrap().documents,
                0
            );
        }
        assert!(expired >= histories, "pre-expired transactions must abort");
        assert!(committed > 0, "at least one competing writer must commit");
        assert!(
            staged_before_expiry > 0,
            "exercise expiry after staging on each backend"
        );
        println!(
            "checkpoint={checkpoint} histories={histories} committed={committed} expired={expired} staged_expired={staged_before_expiry} would_block={would_block} would_block_present={would_block_present} operation_timeouts={operation_timeouts} timeout_present={timeout_present}"
        );
    }
}
