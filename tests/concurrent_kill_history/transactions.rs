//! Concurrent committed and staged transactions during actual process kills.
use super::*;

#[path = "transactions/backend.rs"]
mod backend;
#[path = "transactions/model.rs"]
mod model;
use backend::TransactionStore;
use model::check_recovery;

const WORKERS: usize = 4;
const PER_WORKER: usize = 256;
const COUNTER: &[u8] = b"transaction-counter";
const BLOCKED: &[u8] = b"blocked-transaction";

fn number(bytes: Option<Vec<u8>>) -> io::Result<u64> {
    let bytes: [u8; 8] = bytes
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing transaction counter"))?
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid transaction counter"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn marker_key(worker: usize, sequence: usize) -> Vec<u8> {
    format!("transaction-{worker}-{sequence:04}").into_bytes()
}

#[derive(Clone, Copy, Debug)]
struct Receipt {
    worker: usize,
    sequence: usize,
    counter: u64,
}

#[derive(Clone, Copy, Debug)]
enum Event {
    Staged,
    Acknowledged(Receipt),
    Failed(usize),
}

fn parse_event(line: &str) -> Option<Event> {
    let (_, message) = line.split_once("EMDA_TX ")?;
    let mut parts = message.split_whitespace();
    let event = match parts.next()? {
        "staged" => Event::Staged,
        "ack" => Event::Acknowledged(Receipt {
            worker: parts.next()?.parse().ok()?,
            sequence: parts.next()?.parse().ok()?,
            counter: parts.next()?.parse().ok()?,
        }),
        "failed" => Event::Failed(parts.next()?.parse().ok()?),
        _ => return None,
    };
    if parts.next()? != "END" || parts.next().is_some() {
        return None;
    }
    if matches!(event, Event::Acknowledged(receipt)
        if receipt.worker >= WORKERS || receipt.sequence >= PER_WORKER || receipt.counter == 0)
        || matches!(event, Event::Failed(worker) if worker >= WORKERS)
    {
        return None;
    }
    Some(event)
}

#[test]
fn transaction_receipt_parser_rejects_incomplete_or_invalid_events() {
    assert!(matches!(
        parse_event("EMDA_TX staged END"),
        Some(Event::Staged)
    ));
    assert!(matches!(
        parse_event("EMDA_TX ack 1 2 3 END"),
        Some(Event::Acknowledged(_))
    ));
    assert!(matches!(
        parse_event("EMDA_TX failed 2 END"),
        Some(Event::Failed(2))
    ));
    for line in [
        "EMDA_TX ack 1 2 3",
        "EMDA_TX ack 4 2 3 END",
        "EMDA_TX ack 1 256 3 END",
        "EMDA_TX ack 1 2 0 END",
        "EMDA_TX failed 4 END",
        "EMDA_TX staged END extra",
    ] {
        assert!(
            parse_event(line).is_none(),
            "accepted malformed event: {line}"
        );
    }
}

fn run_child(store: &impl TransactionStore) -> ! {
    let (sender, receiver) = mpsc::channel();
    let (ready, staged) = mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let result = store.block_after_staging(ready);
            panic!("held transaction returned unexpectedly: {result:?}");
        });
        staged.recv_timeout(Duration::from_secs(20)).unwrap();
        println!("EMDA_TX staged END");
        io::stdout().flush().unwrap();
        for worker in 0..WORKERS {
            let sender = sender.clone();
            scope.spawn(move || {
                let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    for sequence in 0..PER_WORKER {
                        loop {
                            match store.increment(worker, sequence) {
                                Ok(counter) => {
                                    sender
                                        .send(Event::Acknowledged(Receipt {
                                            worker,
                                            sequence,
                                            counter,
                                        }))
                                        .unwrap();
                                    break;
                                }
                                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                    std::thread::yield_now();
                                }
                                Err(error) => panic!("transaction writer failed: {error}"),
                            }
                        }
                    }
                }));
                if run.is_err() {
                    let _ = sender.send(Event::Failed(worker));
                }
            });
        }
        drop(sender);
        for event in receiver {
            match event {
                Event::Acknowledged(receipt) => println!(
                    "EMDA_TX ack {} {} {} END",
                    receipt.worker, receipt.sequence, receipt.counter
                ),
                Event::Failed(worker) => println!("EMDA_TX failed {worker} END"),
                Event::Staged => unreachable!(),
            }
            io::stdout().flush().unwrap();
        }
        loop {
            std::thread::park();
        }
    });
    unreachable!()
}

#[test]
fn concurrent_transaction_kill_child() {
    let Some(parent) = std::env::var_os("EMDA_TX_KILL_PARENT") else {
        return;
    };
    let parent = Path::new(&parent);
    let path = parent.join("store");
    if std::env::var("EMDA_TX_KILL_BACKEND").unwrap() == "checkpoint" {
        let digest = fs::read(parent.join("digest")).unwrap().try_into().unwrap();
        let store = retry_lock(|| CheckpointStore::open(&path, parent.join("checkpoint"), &digest))
            .unwrap();
        run_child(&store);
    } else {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        run_child(&store);
    }
}

fn run_case(format: Format, checkpoint_writer: bool, target: usize) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    fs::create_dir(&path).unwrap();
    if matches!(format, Format::V2) {
        fs::write(
            path.join("records.log"),
            include_bytes!("../fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            path.join("commit.state"),
            include_bytes!("../fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"guard").unwrap();
    store.put(COUNTER, &0_u64.to_le_bytes()).unwrap();
    let checkpoint = root.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    fs::write(root.path().join("digest"), digest).unwrap();
    let initial_log = fs::read(path.join("records.log")).unwrap();
    assert_eq!(
        &initial_log[..8],
        if matches!(format, Format::V2) {
            b"EMDALOG2"
        } else {
            b"EMDALOG3"
        }
    );
    let anchor = fs::read(path.join("commit.state")).unwrap();
    drop(store);

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "transactions::concurrent_transaction_kill_child",
            "--nocapture",
        ])
        .env("EMDA_TX_KILL_PARENT", root.path())
        .env(
            "EMDA_TX_KILL_BACKEND",
            if checkpoint_writer {
                "checkpoint"
            } else {
                "ordinary"
            },
        )
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in io::BufReader::new(output).lines().map_while(Result::ok) {
            let event = parse_event(&line);
            assert!(
                !(line.contains("EMDA_TX ")
                    && line.trim_end().ends_with(" END")
                    && event.is_none()),
                "malformed complete event: {line}"
            );
            if let Some(event) = event {
                if sender.send(event).is_err() {
                    break;
                }
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut events = Vec::new();
    let mut staged = false;
    let mut acknowledgements = 0;
    let mut writer_failed = false;
    while (!staged || acknowledgements < target) && !writer_failed {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match receiver.recv_timeout(remaining) {
            Ok(event) => {
                staged |= matches!(event, Event::Staged);
                acknowledgements += usize::from(matches!(event, Event::Acknowledged(_)));
                writer_failed |= matches!(event, Event::Failed(_));
                events.push(event);
            }
            Err(_) => break,
        }
    }
    let busy = staged
        && acknowledgements >= target
        && matches!(Store::open(&path), Err(ref error) if error.kind() == io::ErrorKind::WouldBlock);
    let observed_prefix = fs::read(path.join("records.log")).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    reader.join().unwrap();
    events.extend(receiver.try_iter());
    assert!(
        !events.iter().any(|event| matches!(event, Event::Failed(_))),
        "a transaction writer failed before termination"
    );
    assert!(
        busy,
        "child must hold its store lock and reach the staged/acknowledgement target"
    );
    let mut next = [0_usize; WORKERS];
    let mut receipts = Vec::new();
    let mut staged_count = 0;
    for event in events {
        match event {
            Event::Staged => staged_count += 1,
            Event::Acknowledged(receipt) => {
                assert_eq!(
                    receipt.sequence, next[receipt.worker],
                    "noncontiguous receipt: {receipt:?}"
                );
                next[receipt.worker] += 1;
                receipts.push(receipt);
            }
            Event::Failed(_) => unreachable!(),
        }
    }
    assert_eq!(staged_count, 1);
    let log = fs::read(path.join("records.log")).unwrap();
    let witness = marker(&path, format);
    assert!(log.starts_with(&initial_log) && log.starts_with(&observed_prefix));
    let first = if checkpoint_writer {
        check_recovery(
            retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)),
            &receipts,
        )
    } else {
        check_recovery(retry_lock(|| Store::open(&path)), &receipts)
    };
    if first.is_none() {
        assert_eq!(marker(&path, format), witness);
    }
    assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
    let second = if checkpoint_writer {
        check_recovery(retry_lock(|| Store::open(&path)), &receipts)
    } else {
        check_recovery(
            retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)),
            &receipts,
        )
    };
    assert_eq!(first, second, "recovery paths disagreed");
    if second.is_none() {
        assert_eq!(marker(&path, format), witness);
    }
    assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
    if matches!(format, Format::V3) {
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), anchor);
    }
    println!(
        "format={format:?} checkpoint_writer={checkpoint_writer} target={target} acknowledgements={} recovered={} committed={}",
        receipts.len(),
        first.is_some(),
        first.as_ref().map_or(0, |state| state.count)
    );
}

#[test]
fn committed_transactions_survive_kill_without_publishing_the_staged_one() {
    for format in [Format::V2, Format::V3] {
        for checkpoint_writer in [false, true] {
            for target in [1, 8, 32, 128] {
                run_case(format, checkpoint_writer, target);
            }
        }
    }
}
