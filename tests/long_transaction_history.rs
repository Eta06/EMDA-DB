//! Longer concurrent transfer histories with independently checked live snapshots.
use database_engine::{BatchOperation, CheckpointStore, CheckpointTransaction, Store, Transaction};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

const WRITERS: usize = 4;
const READERS: usize = 2;
const TRANSFERS: i64 = 64;
const START_BALANCE: i64 = 10_000;
const KEYS: [&[u8]; 6] = [b"left", b"right", b"p0", b"p1", b"p2", b"p3"];

trait TransactionAccess {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()>;
}

impl TransactionAccess for Transaction<'_> {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        Transaction::get(self, key)
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        Transaction::put(self, key, value)
    }
}

impl TransactionAccess for CheckpointTransaction<'_> {
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        CheckpointTransaction::get(self, key)
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        CheckpointTransaction::put(self, key, value)
    }
}

trait Bank: Send + Sync {
    fn transfer(&self, writer: usize, sequence: i64) -> io::Result<()>;
    fn read_many(&self) -> io::Result<Vec<Option<Vec<u8>>>>;
}

fn decode(value: Option<Vec<u8>>) -> i64 {
    i64::from_le_bytes(
        value
            .expect("model key missing")
            .try_into()
            .expect("invalid model value"),
    )
}

fn transfer(tx: &mut impl TransactionAccess, writer: usize, sequence: i64) -> io::Result<()> {
    let left = decode(tx.get(KEYS[0])?);
    let right = decode(tx.get(KEYS[1])?);
    let progress = decode(tx.get(KEYS[writer + 2])?);
    assert_eq!(
        progress,
        sequence - 1,
        "writer progress changed unexpectedly"
    );
    assert_eq!(
        left + right,
        START_BALANCE * 2,
        "transaction read a fractured balance"
    );
    let delta = if writer == 3 { 1 } else { -1 };
    tx.put(KEYS[0], &(left + delta).to_le_bytes())?;
    tx.put(KEYS[1], &(right - delta).to_le_bytes())?;
    tx.put(KEYS[writer + 2], &sequence.to_le_bytes())?;
    Ok(())
}

impl Bank for Store {
    fn transfer(&self, writer: usize, sequence: i64) -> io::Result<()> {
        if writer % 2 == 0 {
            self.transaction(|tx| transfer(tx, writer, sequence))
        } else {
            self.transaction_with_read_set(|tx| transfer(tx, writer, sequence))
        }
    }
    fn read_many(&self) -> io::Result<Vec<Option<Vec<u8>>>> {
        self.get_many(&KEYS)
    }
}

impl Bank for CheckpointStore {
    fn transfer(&self, writer: usize, sequence: i64) -> io::Result<()> {
        if writer % 2 == 0 {
            self.transaction(|tx| transfer(tx, writer, sequence))
        } else {
            self.transaction_with_read_set(|tx| transfer(tx, writer, sequence))
        }
    }
    fn read_many(&self) -> io::Result<Vec<Option<Vec<u8>>>> {
        self.get_many(&KEYS)
    }
}

fn valid_snapshot(values: &[i64; 6]) -> bool {
    let [left, right, first, second, third, fourth] = *values;
    [first, second, third, fourth]
        .iter()
        .all(|value| (0..=TRANSFERS).contains(value))
        && left == START_BALANCE - first - second - third + fourth
        && right == START_BALANCE + first + second + third - fourth
}

fn snapshot(bank: &dyn Bank) -> [i64; 6] {
    let values = bank.read_many().unwrap();
    values
        .into_iter()
        .map(decode)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}

struct Remaining(Arc<AtomicUsize>);
impl Drop for Remaining {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn check_case(v2: bool, checkpoint_writer: bool) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source");
    if v2 {
        std::fs::create_dir(&path).unwrap();
        std::fs::write(
            path.join("records.log"),
            include_bytes!("fixtures/v2-empty-records.log"),
        )
        .unwrap();
        std::fs::write(
            path.join("commit.state"),
            include_bytes!("fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let ordinary = Store::open(&path).unwrap();
    let header = std::fs::read(path.join("records.log")).unwrap();
    assert_eq!(&header[..8], if v2 { b"EMDALOG2" } else { b"EMDALOG3" });
    let initial = START_BALANCE.to_le_bytes();
    let zero = 0_i64.to_le_bytes();
    let operations = KEYS
        .iter()
        .enumerate()
        .map(|(index, key)| BatchOperation::Put {
            key,
            value: if index < 2 { &initial } else { &zero },
        })
        .collect::<Vec<_>>();
    ordinary.write_batch(&operations).unwrap();
    let checkpoint = root.path().join("checkpoint");
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    let bank: Arc<dyn Bank> = if checkpoint_writer {
        drop(ordinary);
        Arc::new(CheckpointStore::open(&path, &checkpoint, &digest).unwrap())
    } else {
        Arc::new(ordinary)
    };

    let start = Arc::new(Barrier::new(WRITERS + READERS + 1));
    let remaining = Arc::new(AtomicUsize::new(WRITERS));
    let conflicts = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let bank = Arc::clone(&bank);
            let start = Arc::clone(&start);
            let remaining = Arc::clone(&remaining);
            let conflicts = Arc::clone(&conflicts);
            scope.spawn(move || {
                let _remaining = Remaining(remaining);
                start.wait();
                for sequence in 1..=TRANSFERS {
                    let mut committed = false;
                    for _ in 0..4096 {
                        match bank.transfer(writer, sequence) {
                            Ok(()) => {
                                committed = true;
                                break;
                            }
                            Err(error)
                                if error.kind() == io::ErrorKind::WouldBlock
                                    && error.to_string().contains("transaction conflict") =>
                            {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                                thread::yield_now();
                            }
                            Err(error) => panic!("unexpected transaction error: {error}"),
                        }
                    }
                    assert!(committed, "transaction retry limit exceeded");
                }
            });
        }
        for _ in 0..READERS {
            let bank = Arc::clone(&bank);
            let start = Arc::clone(&start);
            let remaining = Arc::clone(&remaining);
            let reads = Arc::clone(&reads);
            scope.spawn(move || {
                let mut last_progress = [0; WRITERS];
                start.wait();
                loop {
                    let observed = snapshot(&*bank);
                    assert!(
                        valid_snapshot(&observed),
                        "invalid transaction snapshot: {observed:?}"
                    );
                    for index in 0..WRITERS {
                        assert!(
                            observed[index + 2] >= last_progress[index],
                            "reader moved backwards"
                        );
                        last_progress[index] = observed[index + 2];
                    }
                    reads.fetch_add(1, Ordering::Relaxed);
                    if remaining.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    thread::yield_now();
                }
            });
        }
        start.wait();
    });
    let expected = [
        START_BALANCE - 2 * TRANSFERS,
        START_BALANCE + 2 * TRANSFERS,
        TRANSFERS,
        TRANSFERS,
        TRANSFERS,
        TRANSFERS,
    ];
    assert_eq!(snapshot(&*bank), expected);
    eprintln!(
        "format={} writer={} commits={} reader_snapshots={} conflicts={}",
        if v2 { "v2" } else { "v3" },
        if checkpoint_writer {
            "checkpoint"
        } else {
            "ordinary"
        },
        WRITERS as i64 * TRANSFERS,
        reads.load(Ordering::Relaxed),
        conflicts.load(Ordering::Relaxed)
    );
    drop(bank);
    let recovered = Store::open(&path).unwrap();
    assert_eq!(snapshot(&recovered), expected);
    drop(recovered);
    let recovered = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    assert_eq!(snapshot(&recovered), expected);
}

#[test]
fn longer_concurrent_histories_preserve_snapshot_invariants_and_recover() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        for checkpoint_writer in [false, true] {
            check_case(v2, checkpoint_writer);
        }
    }
}

#[test]
fn snapshot_model_rejects_fractured_balances_and_progress() {
    assert!(valid_snapshot(&[10_000, 10_000, 0, 0, 0, 0]));
    assert!(!valid_snapshot(&[9_999, 10_000, 1, 0, 0, 0]));
    assert!(!valid_snapshot(&[10_000, 10_000, 1, 0, 0, 0]));
    assert!(!valid_snapshot(&[10_000, 10_000, -1, 0, 0, -1]));
}
