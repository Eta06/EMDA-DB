//! Backups racing independent atomic writers must represent a consistent cut.
use database_engine::{BatchOperation, CheckpointStore, Store};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

const WRITERS: usize = 4;
const ROUNDS: u64 = 12;

trait BackupWriter: Send + Sync {
    fn update(
        &self,
        first: &[u8],
        second: &[u8],
        value: &[u8],
        transaction: bool,
    ) -> io::Result<()>;
    fn backup(&self, destination: &Path) -> io::Result<()>;
}

impl BackupWriter for Store {
    fn update(
        &self,
        first: &[u8],
        second: &[u8],
        value: &[u8],
        transaction: bool,
    ) -> io::Result<()> {
        if transaction {
            self.transaction(|tx| {
                tx.put(first, value)?;
                tx.put(second, value)?;
                Ok(())
            })
        } else {
            self.write_batch(&[
                BatchOperation::Put { key: first, value },
                BatchOperation::Put { key: second, value },
            ])
        }
    }
    fn backup(&self, destination: &Path) -> io::Result<()> {
        Store::backup(self, destination)
    }
}

impl BackupWriter for CheckpointStore {
    fn update(
        &self,
        first: &[u8],
        second: &[u8],
        value: &[u8],
        transaction: bool,
    ) -> io::Result<()> {
        if transaction {
            self.transaction(|tx| {
                tx.put(first, value)?;
                tx.put(second, value)?;
                Ok(())
            })
        } else {
            self.write_batch(&[
                BatchOperation::Put { key: first, value },
                BatchOperation::Put { key: second, value },
            ])
        }
    }
    fn backup(&self, destination: &Path) -> io::Result<()> {
        CheckpointStore::backup(self, destination)
    }
}

fn keys(writer: usize) -> ([u8; 2], [u8; 2]) {
    ([writer as u8, b'a'], [writer as u8, b'b'])
}

fn read_value(store: &Store, key: &[u8]) -> u64 {
    let bytes = store.get(key).unwrap().expect("missing model key");
    u64::from_le_bytes(bytes.try_into().expect("unexpected model value length"))
}

fn check_case(v2: bool, checkpoint_writer: bool) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    if v2 {
        std::fs::create_dir(&source).unwrap();
        std::fs::write(
            source.join("records.log"),
            include_bytes!("fixtures/v2-empty-records.log"),
        )
        .unwrap();
        std::fs::write(
            source.join("commit.state"),
            include_bytes!("fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let ordinary = Store::open(&source).unwrap();
    let header = std::fs::read(source.join("records.log")).unwrap();
    assert_eq!(&header[..8], if v2 { b"EMDALOG2" } else { b"EMDALOG3" });
    let checkpoint = root.path().join("checkpoint");
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    let store: Arc<dyn BackupWriter> = if checkpoint_writer {
        drop(ordinary);
        Arc::new(CheckpointStore::open(&source, &checkpoint, &digest).unwrap())
    } else {
        Arc::new(ordinary)
    };
    for writer in 0..WRITERS {
        let (first, second) = keys(writer);
        store
            .update(&first, &second, &0_u64.to_le_bytes(), false)
            .unwrap();
    }

    let acknowledged = Arc::new((0..WRITERS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let (completed_tx, completed_rx) = mpsc::channel();
    let mut starts = Vec::new();
    let mut workers = Vec::new();
    for writer in 0..WRITERS {
        let (start_tx, start_rx) = mpsc::channel::<u64>();
        starts.push(start_tx);
        let store = Arc::clone(&store);
        let acknowledged = Arc::clone(&acknowledged);
        let completed = completed_tx.clone();
        workers.push(thread::spawn(move || {
            while let Ok(round) = start_rx.recv() {
                let (first, second) = keys(writer);
                let mut conflicts = 0;
                let outcome = loop {
                    let result =
                        store.update(&first, &second, &round.to_le_bytes(), round % 2 != 0);
                    match &result {
                        Err(error)
                            if error.kind() == io::ErrorKind::WouldBlock
                                && error.to_string().contains("transaction conflict")
                                && conflicts < 64 =>
                        {
                            conflicts += 1;
                            thread::yield_now();
                        }
                        _ => break result,
                    }
                };
                if outcome.is_ok() {
                    acknowledged[writer].store(round, Ordering::Release);
                }
                let failed = outcome.is_err();
                if completed.send((writer, round, outcome)).is_err() || failed {
                    break;
                }
            }
        }));
    }
    drop(completed_tx);

    let mut rounds_with_outstanding_writers = 0;
    let mut rounds_with_completed_writers = 0;
    for round in 1..=ROUNDS {
        let lower = acknowledged
            .iter()
            .map(|count| count.load(Ordering::Acquire))
            .collect::<Vec<_>>();
        for start in &starts {
            start.send(round).unwrap();
        }
        let backup = root.path().join(format!("backup-{round}"));
        store.backup(&backup).unwrap();
        let upper = acknowledged
            .iter()
            .map(|count| count.load(Ordering::Acquire))
            .collect::<Vec<_>>();
        rounds_with_outstanding_writers += usize::from(upper.iter().any(|count| *count < round));
        rounds_with_completed_writers += usize::from(upper.contains(&round));
        for _ in 0..WRITERS {
            let (_, completed_round, outcome) =
                completed_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            assert_eq!(completed_round, round);
            outcome.unwrap();
        }

        let restored = root.path().join(format!("restored-{round}"));
        Store::restore(&backup, &restored).unwrap();
        let snapshot = Store::open(&restored).unwrap();
        for writer in 0..WRITERS {
            let (first, second) = keys(writer);
            let value = read_value(&snapshot, &first);
            assert_eq!(
                value,
                read_value(&snapshot, &second),
                "fractured atomic update"
            );
            assert!(
                value >= lower[writer],
                "snapshot lost an acknowledged update"
            );
            // A writer can finish its commit before publishing its counter.
            // Each writer has at most one such operation in flight.
            assert!(
                value <= upper[writer] + 1,
                "snapshot includes a future update"
            );
        }
    }
    eprintln!(
        "format={} writer={} rounds={} outstanding_at_return={} completed_at_return={}",
        if v2 { "v2" } else { "v3" },
        if checkpoint_writer {
            "checkpoint"
        } else {
            "ordinary"
        },
        ROUNDS,
        rounds_with_outstanding_writers,
        rounds_with_completed_writers,
    );
    drop(starts);
    for worker in workers {
        worker.join().unwrap();
    }
    drop(store);
    let recovered = CheckpointStore::open(&source, &checkpoint, &digest).unwrap();
    for writer in 0..WRITERS {
        let (first, second) = keys(writer);
        for key in [&first, &second] {
            assert_eq!(
                recovered.get(key).unwrap(),
                Some(ROUNDS.to_le_bytes().to_vec())
            );
        }
    }
}

#[test]
fn concurrent_backup_is_a_consistent_acknowledged_cut() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for checkpoint_writer in [false, true] {
            check_case(v2, checkpoint_writer);
        }
    }
}
