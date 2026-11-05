//! Read-only transaction admission under an optional durable writer.
use database_engine::{
    CancellationToken, CheckpointStore, CheckpointTransactionMode, ControlledTransactionError,
    Store, TransactionMode,
};
use serde_json::json;
use std::{
    error::Error,
    io,
    path::Path,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
enum Engine {
    Ordinary(Arc<Store>),
    Checkpoint(Arc<CheckpointStore>),
}

fn controlled_error(error: ControlledTransactionError) -> io::Error {
    match error {
        ControlledTransactionError::Operation(error) => error,
        other => io::Error::other(other.to_string()),
    }
}

impl Engine {
    fn observe(&self, controlled: bool, token: &CancellationToken) -> io::Result<()> {
        match self {
            Self::Ordinary(store) if controlled => store
                .transaction_controlled(token, None, TransactionMode::ReadSet, |_| Ok(()))
                .map_err(controlled_error),
            Self::Ordinary(store) => store.transaction_with_read_set(|_| Ok(())),
            Self::Checkpoint(store) if controlled => store
                .transaction_controlled(token, None, CheckpointTransactionMode::ReadSet, |_| Ok(()))
                .map_err(controlled_error),
            Self::Checkpoint(store) => store.transaction_with_read_set(|_| Ok(())),
        }
    }

    fn put(&self, value: &[u8]) -> io::Result<()> {
        match self {
            Self::Ordinary(store) => store.put(b"hot", value),
            Self::Checkpoint(store) => store.put(b"hot", value),
        }
    }

    fn get(&self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ordinary(store) => store.get(b"hot"),
            Self::Checkpoint(store) => store.get(b"hot"),
        }
    }
}

fn main() -> BenchResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [backend, mode, workload, clients, operations, parent] = args.as_slice() else {
        return Err("usage: controlled_lock_bench <ordinary|checkpoint> <plain|controlled> <idle|contended> <clients> <operations-per-client> <directory>".into());
    };
    if !matches!(backend.as_str(), "ordinary" | "checkpoint")
        || !matches!(mode.as_str(), "plain" | "controlled")
        || !matches!(workload.as_str(), "idle" | "contended")
    {
        return Err("invalid benchmark mode".into());
    }
    let clients: usize = clients.parse()?;
    let operations: usize = operations.parse()?;
    if !(1..=16).contains(&clients) || !(1..=100_000).contains(&operations) {
        return Err("invalid client or operation count".into());
    }
    let root = tempfile::tempdir_in(Path::new(parent))?;
    let path = root.path().join("store");
    let checkpoint = root.path().join("index");
    let store = Store::open(&path)?;
    store.put(b"hot", &0_u64.to_le_bytes())?;
    let engine = if backend == "checkpoint" {
        let digest = store.checkpoint_to(&checkpoint)?;
        drop(store);
        Engine::Checkpoint(Arc::new(CheckpointStore::open(
            &path,
            &checkpoint,
            &digest,
        )?))
    } else {
        Engine::Ordinary(Arc::new(store))
    };
    let stop = Arc::new(AtomicBool::new(false));
    let writer = if workload == "contended" {
        let writing = engine.clone();
        let stop = Arc::clone(&stop);
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = thread::spawn(move || -> io::Result<u64> {
            let mut count = 1_u64;
            let first = writing.put(&count.to_le_bytes());
            ready_tx.send(first.is_ok()).map_err(io::Error::other)?;
            first?;
            while !stop.load(Ordering::Acquire) {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("writer count overflow"))?;
                writing.put(&count.to_le_bytes())?;
            }
            Ok(count)
        });
        if !ready_rx.recv()? {
            return Err("writer failed to start".into());
        }
        Some(handle)
    } else {
        None
    };
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut workers = Vec::new();
    for _ in 0..clients {
        let engine = engine.clone();
        let barrier = Arc::clone(&barrier);
        let controlled = mode == "controlled";
        workers.push(thread::spawn(move || -> io::Result<Vec<u64>> {
            let token = CancellationToken::new();
            let mut samples = Vec::with_capacity(operations);
            barrier.wait();
            for iteration in 0..operations + 10 {
                let start = Instant::now();
                engine.observe(controlled, &token)?;
                let elapsed =
                    u64::try_from(start.elapsed().as_nanos()).map_err(io::Error::other)?;
                if iteration >= 10 {
                    samples.push(elapsed);
                }
            }
            Ok(samples)
        }));
    }
    barrier.wait();
    let joined = workers
        .into_iter()
        .map(|worker| worker.join())
        .collect::<Vec<_>>();
    stop.store(true, Ordering::Release);
    let writer_commits = match writer {
        Some(writer) => writer.join().map_err(|_| "writer panicked")??,
        None => 0,
    };
    let mut samples = Vec::with_capacity(clients * operations);
    for result in joined {
        samples.extend(result.map_err(|_| "client panicked")??);
    }
    if samples.len() != clients * operations {
        return Err("incomplete transaction samples".into());
    }
    let expected = writer_commits.to_le_bytes().to_vec();
    if engine.get()?.as_deref() != Some(expected.as_slice()) {
        return Err("live writer value mismatch".into());
    }
    drop(engine);
    let reopened = Store::open(&path)?;
    if reopened.get(b"hot")?.as_deref() != Some(expected.as_slice()) {
        return Err("recovered writer value mismatch".into());
    }
    println!(
        "{}",
        json!({
            "backend":backend, "mode":mode, "workload":workload,
        "clients":clients, "operations_per_client":operations, "warmup_per_client":10,
            "writer_commits":writer_commits, "samples_ns":samples, "reopen_verified":true,
        })
    );
    Ok(())
}
