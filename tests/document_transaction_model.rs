//! Real-time serial-history checks for document, index and raw-key transactions.
use database_engine::{
    CancellationToken, CheckpointStore, CollectionSchema, ControlledTransactionError,
    DocumentDatabase, DocumentTransaction, DocumentTransactionMode, IndexDefinition, IndexQuery,
    IndexValue, Store, TimedTransactionError,
};
use serde_json::json;
use std::io::{self, ErrorKind};
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

mod support;
use support::retry_lock;

#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    amounts: [i64; 2],
    claim: Option<&'static str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Observation {
    amounts: [i64; 2],
    mirror: Vec<u8>,
    balance_index: Vec<(String, i64)>,
    claim_index: Vec<String>,
}

impl State {
    fn observation(&self) -> Observation {
        let mut balance_index: Vec<(String, i64)> =
            vec![("a".into(), self.amounts[0]), ("b".into(), self.amounts[1])];
        balance_index.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        Observation {
            amounts: self.amounts,
            mirror: mirror(self.amounts),
            balance_index,
            claim_index: self.claim.into_iter().map(str::to_owned).collect(),
        }
    }
}

fn mirror(amounts: [i64; 2]) -> Vec<u8> {
    format!("{},{}", amounts[0], amounts[1]).into_bytes()
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Transfer(usize),
    Reset,
    Observe,
    Claim(&'static str),
}

#[derive(Debug)]
enum Outcome {
    Conflict,
    Duplicate,
    Cancelled,
    Write(Observation),
    Read(Observation),
    Claimed,
}

#[derive(Debug)]
struct Event {
    operation: Operation,
    outcome: Outcome,
    start: usize,
    end: usize,
}

fn serializes(events: &[Event], final_state: &State) -> bool {
    fn search(events: &[Event], final_state: &State, used: u64, state: State) -> bool {
        if used.count_ones() as usize == events.len() {
            return &state == final_state;
        }
        for (index, event) in events.iter().enumerate() {
            let bit = 1_u64 << index;
            if used & bit != 0
                || events.iter().enumerate().any(|(other, predecessor)| {
                    other != index && used & (1 << other) == 0 && predecessor.end < event.start
                })
            {
                continue;
            }
            let mut next = state.clone();
            match (&event.operation, &event.outcome) {
                (_, Outcome::Conflict | Outcome::Cancelled) => {}
                (Operation::Claim(_), Outcome::Duplicate) if state.claim.is_some() => {}
                (Operation::Claim(id), Outcome::Claimed) if state.claim.is_none() => {
                    next.claim = Some(id);
                }
                (Operation::Observe, Outcome::Read(observed))
                    if *observed == state.observation() => {}
                (Operation::Transfer(from), Outcome::Write(observed))
                    if *observed == state.observation() =>
                {
                    if next.amounts[*from] > 0 {
                        next.amounts[*from] -= 1;
                        next.amounts[1 - *from] += 1;
                    }
                }
                (Operation::Reset, Outcome::Write(observed))
                    if *observed == state.observation() =>
                {
                    next.amounts = [4, 6];
                }
                _ => continue,
            }
            if search(events, final_state, used | bit, next) {
                return true;
            }
        }
        false
    }
    search(
        events,
        final_state,
        0,
        State {
            amounts: [3, 2],
            claim: None,
        },
    )
}

fn observe(tx: &DocumentTransaction<'_, '_>) -> io::Result<Observation> {
    let a = tx.get("accounts", "a")?.expect("account a");
    let b = tx.get("accounts", "b")?.expect("account b");
    let amounts = [
        a["balance"].as_i64().unwrap(),
        b["balance"].as_i64().unwrap(),
    ];
    let raw = tx.get_raw(b"mirror")?.expect("raw mirror");
    let balance_index = tx
        .query_index("accounts", "balance", &IndexQuery::default())?
        .documents
        .into_iter()
        .map(|entry| (entry.id, entry.document["balance"].as_i64().unwrap()))
        .collect();
    let claim_index = tx
        .query_index(
            "accounts",
            "handle",
            &IndexQuery {
                equal: vec![IndexValue::String("claim".into())],
                ..Default::default()
            },
        )?
        .documents
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    Ok(Observation {
        amounts,
        mirror: raw,
        balance_index,
        claim_index,
    })
}

fn write_amounts(tx: &mut DocumentTransaction<'_, '_>, amounts: [i64; 2]) -> io::Result<()> {
    tx.replace(
        "accounts",
        "a",
        &json!({"handle":"a", "balance":amounts[0]}),
        false,
    )?;
    std::thread::yield_now();
    tx.replace(
        "accounts",
        "b",
        &json!({"handle":"b", "balance":amounts[1]}),
        false,
    )?;
    tx.put_raw(b"mirror", &mirror(amounts))
}

fn execute(db: &DocumentDatabase<'_>, operation: Operation, mode: usize) -> Outcome {
    let action = |tx: &mut DocumentTransaction<'_, '_>| match operation {
        Operation::Observe => observe(tx).map(Outcome::Read),
        Operation::Claim(id) => {
            std::thread::yield_now();
            tx.insert("accounts", id, &json!({"handle":"claim"}))?;
            Ok(Outcome::Claimed)
        }
        Operation::Transfer(from) => {
            let before = observe(tx)?;
            let mut amounts = before.amounts;
            if amounts[from] > 0 {
                amounts[from] -= 1;
                amounts[1 - from] += 1;
            }
            write_amounts(tx, amounts)?;
            Ok(Outcome::Write(before))
        }
        Operation::Reset => {
            let before = observe(tx)?;
            write_amounts(tx, [4, 6])?;
            Ok(Outcome::Write(before))
        }
    };
    let result = match mode % 6 {
        0 => db
            .transaction(action)
            .map_err(ControlledTransactionError::Operation),
        1 => db
            .transaction_with_read_set(action)
            .map_err(ControlledTransactionError::Operation),
        2 => db
            .transaction_until(
                Instant::now() + Duration::from_secs(30),
                DocumentTransactionMode::ReadSetFifo,
                action,
            )
            .map_err(|error| match error {
                TimedTransactionError::Operation(error) => {
                    ControlledTransactionError::Operation(error)
                }
                TimedTransactionError::DeadlineExceeded => panic!("model deadline expired"),
            }),
        3 => {
            let token = CancellationToken::new();
            token.cancel();
            db.transaction_controlled(&token, None, DocumentTransactionMode::Conservative, action)
        }
        4 => {
            let token = CancellationToken::new();
            db.transaction_controlled(&token, None, DocumentTransactionMode::ReadSetFifo, |tx| {
                let outcome = action(tx)?;
                token.cancel();
                Ok(outcome)
            })
        }
        _ => std::thread::scope(|scope| {
            let token = CancellationToken::new();
            let cancelling = token.clone();
            let worker = scope.spawn(move || {
                std::thread::yield_now();
                cancelling.cancel();
            });
            let result =
                db.transaction_controlled(&token, None, DocumentTransactionMode::ReadSet, action);
            worker.join().unwrap();
            result
        }),
    };
    match result {
        Ok(outcome) => outcome,
        Err(ControlledTransactionError::Cancelled) => Outcome::Cancelled,
        Err(ControlledTransactionError::Operation(error))
            if error.kind() == ErrorKind::WouldBlock =>
        {
            Outcome::Conflict
        }
        Err(ControlledTransactionError::Operation(error))
            if error.kind() == ErrorKind::AlreadyExists =>
        {
            Outcome::Duplicate
        }
        Err(error) => panic!("unexpected {operation:?} error: {error}"),
    }
}

fn initialize(db: &DocumentDatabase<'_>) {
    db.create_collection("accounts", CollectionSchema::default())
        .unwrap();
    for (name, unique) in [("balance", false), ("handle", true)] {
        db.create_index(
            "accounts",
            IndexDefinition {
                name: name.into(),
                fields: vec![vec![name.into()]],
                unique,
                partial: None,
            },
        )
        .unwrap();
    }
    db.insert("accounts", "a", &json!({"handle":"a", "balance":3}))
        .unwrap();
    db.insert("accounts", "b", &json!({"handle":"b", "balance":2}))
        .unwrap();
    db.transaction(|tx| tx.put_raw(b"mirror", &mirror([3, 2])))
        .unwrap();
}

fn reset(db: &DocumentDatabase<'_>) {
    db.transaction(|tx| {
        tx.delete("accounts", "c")?;
        tx.delete("accounts", "d")?;
        write_amounts(tx, [3, 2])
    })
    .unwrap();
}

fn read_state(db: &DocumentDatabase<'_>) -> State {
    let observed = db.transaction(|tx| observe(tx)).unwrap();
    let claim = match observed.claim_index.as_slice() {
        [] => None,
        [id] if id == "c" => Some("c"),
        [id] if id == "d" => Some("d"),
        other => panic!("invalid unique index: {other:?}"),
    };
    let state = State {
        amounts: observed.amounts,
        claim,
    };
    assert_eq!(observed, state.observation());
    let integrity = db.verify_collection("accounts").unwrap();
    assert_eq!(integrity.documents, 2 + usize::from(claim.is_some()));
    state
}

#[test]
fn history_checker_rejects_fractured_raw_or_index_data() {
    let state = State {
        amounts: [3, 2],
        claim: None,
    };
    let mut invalid = state.observation();
    invalid.mirror = b"2,3".to_vec();
    let events = [Event {
        operation: Operation::Observe,
        outcome: Outcome::Read(invalid),
        start: 0,
        end: 1,
    }];
    assert!(!serializes(&events, &state));
    let mut invalid = state.observation();
    invalid.balance_index.swap(0, 1);
    let events = [Event {
        operation: Operation::Observe,
        outcome: Outcome::Read(invalid),
        start: 0,
        end: 1,
    }];
    assert!(!serializes(&events, &state));
}

#[test]
fn history_checker_rejects_duplicate_unique_claims_and_lost_writes() {
    let state = State {
        amounts: [3, 2],
        claim: Some("c"),
    };
    let claims = [
        Event {
            operation: Operation::Claim("c"),
            outcome: Outcome::Claimed,
            start: 0,
            end: 3,
        },
        Event {
            operation: Operation::Claim("d"),
            outcome: Outcome::Claimed,
            start: 1,
            end: 2,
        },
    ];
    assert!(!serializes(&claims, &state));
    let start = State {
        amounts: [3, 2],
        claim: None,
    };
    let transfer = [Event {
        operation: Operation::Transfer(0),
        outcome: Outcome::Write(start.observation()),
        start: 0,
        end: 1,
    }];
    assert!(!serializes(&transfer, &start));
}

#[test]
fn history_checker_rejects_changes_attributed_only_to_cancelled_work() {
    let cancelled = [Event {
        operation: Operation::Reset,
        outcome: Outcome::Cancelled,
        start: 0,
        end: 1,
    }];
    assert!(serializes(
        &cancelled,
        &State {
            amounts: [3, 2],
            claim: None
        }
    ));
    assert!(!serializes(
        &cancelled,
        &State {
            amounts: [4, 6],
            claim: None
        }
    ));
}

enum TestStore {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}

impl TestStore {
    fn database(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }
}

#[test]
fn concurrent_document_histories_serialize_and_recover_on_both_backends() {
    let histories = std::env::var("EMDA_DOCUMENT_MODEL_HISTORIES")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(128);
    assert!((1..=8192).contains(&histories));
    let operations = [
        Operation::Transfer(0),
        Operation::Transfer(1),
        Operation::Reset,
        Operation::Observe,
        Operation::Claim("c"),
        Operation::Claim("d"),
    ];
    for checkpoint_mode in [false, true] {
        let mut cancelled = 0_usize;
        let mut completed = 0_usize;
        let mut rejected = 0_usize;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = retry_lock(|| Store::open(&path)).unwrap();
        initialize(&DocumentDatabase::new(&initial));
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        drop(initial);
        for iteration in 0..histories {
            let backend = if checkpoint_mode {
                TestStore::Checkpoint(
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap(),
                )
            } else {
                TestStore::Ordinary(retry_lock(|| Store::open(&path)).unwrap())
            };
            let db = backend.database();
            reset(&db);
            let barrier = Barrier::new(operations.len() + 1);
            let clock = AtomicUsize::new(0);
            let events = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for offset in 0..operations.len() {
                    let operation = operations[(offset + iteration) % operations.len()];
                    let (db, barrier, clock) = (&db, &barrier, &clock);
                    handles.push(scope.spawn(move || {
                        barrier.wait();
                        if (iteration + offset) % 3 == 0 {
                            std::thread::yield_now();
                        }
                        let start = clock.fetch_add(1, Ordering::SeqCst);
                        // Vary mode independently of operation across iterations.
                        let outcome = execute(db, operation, iteration * 2 + offset);
                        let end = clock.fetch_add(1, Ordering::SeqCst);
                        Event {
                            operation,
                            outcome,
                            start,
                            end,
                        }
                    }));
                }
                barrier.wait();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect::<Vec<_>>()
            });
            let expected = read_state(&db);
            for event in &events {
                match &event.outcome {
                    Outcome::Cancelled => cancelled += 1,
                    Outcome::Conflict | Outcome::Duplicate => rejected += 1,
                    _ => completed += 1,
                }
            }
            assert!(
                serializes(&events, &expected),
                "backend={checkpoint_mode} iteration={iteration} history={events:?} final={expected:?}"
            );
            drop(backend);
            let recovered = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                read_state(&DocumentDatabase::new(&recovered)),
                expected,
                "backend={checkpoint_mode} iteration={iteration} recovery"
            );
        }
        assert!(
            cancelled >= histories,
            "pre-cancelled operations must abort"
        );
        println!(
            "checkpoint={checkpoint_mode} histories={histories} cancelled={cancelled} completed={completed} rejected={rejected}"
        );
    }
}
