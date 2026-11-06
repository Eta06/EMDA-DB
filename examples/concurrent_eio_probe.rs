//! Concurrent acknowledged writes and recovery for the Linux EIO driver.
use database_engine::{CheckpointStore, Store};
use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;
use std::sync::{Arc, Barrier};

const WORKERS: usize = 4;
const OPERATIONS: usize = 64;

trait Backend: Send + Sync {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn put_transaction(&self, key: &[u8], peer: &[u8], value: &[u8]) -> io::Result<()>;
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
}
impl Backend for Store {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        Store::put(self, key, value)
    }
    fn put_transaction(&self, key: &[u8], peer: &[u8], value: &[u8]) -> io::Result<()> {
        retry_idempotent_transaction(|| {
            self.transaction_with_read_set(|tx| {
                tx.put(key, value)?;
                tx.put(peer, value)
            })
        })
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        Store::get(self, key)
    }
}
impl Backend for CheckpointStore {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        CheckpointStore::put(self, key, value)
    }
    fn put_transaction(&self, key: &[u8], peer: &[u8], value: &[u8]) -> io::Result<()> {
        retry_idempotent_transaction(|| {
            self.transaction_with_read_set(|tx| {
                tx.put(key, value)?;
                tx.put(peer, value)
            })
        })
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        CheckpointStore::get(self, key)
    }
}

// WouldBlock can include an uncertain I/O result, not only a conflict. This
// workload retries only because both writes use the same unique keys and
// values on every attempt; callers cannot assume arbitrary transactions safe
// to retry after an operation error.
fn retry_idempotent_transaction(mut action: impl FnMut() -> io::Result<()>) -> io::Result<()> {
    for _ in 0..100_000 {
        match action() {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
            result => return result,
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "transaction conflict retry limit",
    ))
}

#[derive(Clone, Copy)]
enum Mode {
    Single,
    Transaction,
}
impl Mode {
    fn parse(value: &str) -> io::Result<Self> {
        match value {
            "single" => Ok(Self::Single),
            "transaction" => Ok(Self::Transaction),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown write mode",
            )),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Transaction => "transaction",
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Worker {
    id: usize,
    acknowledged: Vec<usize>,
    failed: Option<usize>,
    error: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Report {
    workers: Vec<Worker>,
}

fn key(worker: usize, sequence: usize) -> Vec<u8> {
    format!("worker-{worker}-{sequence:04}").into_bytes()
}
fn value(worker: usize, sequence: usize) -> Vec<u8> {
    format!("value-{worker}-{sequence:04}").into_bytes()
}
fn peer_key(worker: usize, sequence: usize) -> Vec<u8> {
    format!("peer-{worker}-{sequence:04}").into_bytes()
}
fn open(root: &Path, backend: &str) -> io::Result<Arc<dyn Backend>> {
    match backend {
        "ordinary" => Ok(Arc::new(Store::open(root.join("source"))?)),
        "checkpoint" => {
            let digest: [u8; 32] = std::fs::read(root.join("checkpoint.digest"))?
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid digest"))?;
            Ok(Arc::new(CheckpointStore::open(
                root.join("source"),
                root.join("checkpoint"),
                &digest,
            )?))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unknown backend",
        )),
    }
}

fn attempt(root: &Path, backend: &str, mode: Mode) -> io::Result<()> {
    let store = open(root, backend)?;
    let barrier = Arc::new(Barrier::new(WORKERS + 1));
    let workers = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for id in 0..WORKERS {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                let mut worker = Worker {
                    id,
                    acknowledged: Vec::new(),
                    failed: None,
                    error: None,
                };
                barrier.wait();
                for sequence in 0..OPERATIONS {
                    let key = key(id, sequence);
                    let value = value(id, sequence);
                    let result = match mode {
                        Mode::Single => store.put(&key, &value),
                        Mode::Transaction => {
                            store.put_transaction(&key, &peer_key(id, sequence), &value)
                        }
                    };
                    match result {
                        Ok(()) => worker.acknowledged.push(sequence),
                        Err(error) => {
                            worker.failed = Some(sequence);
                            worker.error = Some(format!("{:?}: {error}", error.kind()));
                            break;
                        }
                    }
                }
                worker
            }));
        }
        barrier.wait();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("writer panicked"))
            .collect()
    });
    drop(store);
    println!("{}", serde_json::to_string(&Report { workers })?);
    Ok(())
}

fn verify(root: &Path, backend: &str, receipts: &Path, mode: Mode) -> io::Result<()> {
    let report: Report = serde_json::from_slice(&std::fs::read(receipts)?)?;
    if report.workers.len() != WORKERS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "wrong worker count",
        ));
    }
    let store = open(root, backend)?;
    if store.get(b"safe")?.as_deref() != Some(&b"acknowledged"[..])
        || store.get(b"old")?.as_deref() != Some(&b"original"[..])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "lost earlier acknowledged value",
        ));
    }
    let mut unknown_present = Vec::new();
    for (id, worker) in report.workers.iter().enumerate() {
        if worker.id != id
            || worker
                .acknowledged
                .iter()
                .copied()
                .ne(0..worker.acknowledged.len())
            || worker
                .failed
                .is_some_and(|seq| seq != worker.acknowledged.len())
            || worker.failed.is_some_and(|seq| seq >= OPERATIONS)
            || worker.failed.is_some() != worker.error.is_some()
            || (worker.failed.is_none() && worker.acknowledged.len() != OPERATIONS)
            || worker.acknowledged.len() > OPERATIONS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid writer receipt",
            ));
        }
        for sequence in 0..OPERATIONS {
            let actual = store.get(&key(id, sequence))?;
            if matches!(mode, Mode::Transaction) && store.get(&peer_key(id, sequence))? != actual {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fractured transaction",
                ));
            }
            let expected = value(id, sequence);
            if sequence < worker.acknowledged.len() {
                if actual.as_ref() != Some(&expected) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "lost acknowledged concurrent write",
                    ));
                }
            } else if worker.failed == Some(sequence) {
                if actual.as_ref().is_some_and(|found| found != &expected) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid uncertain write",
                    ));
                }
                if actual.is_some() {
                    unknown_present.push([id, sequence]);
                }
            } else if actual.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected unattempted write",
                ));
            }
        }
    }
    println!(
        "{}",
        serde_json::json!({"status":"verified","backend":backend,"mode":mode.name(),
             "acknowledged":report.workers.iter().map(|worker| worker.acknowledged.len()).sum::<usize>(),
             "uncertain_present":unknown_present})
    );
    Ok(())
}

fn main() -> io::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, root, backend] if command == "attempt" => {
            attempt(Path::new(root), backend, Mode::Single)
        }
        [command, root, backend, mode] if command == "attempt" => {
            attempt(Path::new(root), backend, Mode::parse(mode)?)
        }
        [command, root, backend, receipts] if command == "verify" => {
            verify(Path::new(root), backend, Path::new(receipts), Mode::Single)
        }
        [command, root, backend, receipts, mode] if command == "verify" => verify(
            Path::new(root),
            backend,
            Path::new(receipts),
            Mode::parse(mode)?,
        ),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: concurrent_eio_probe attempt <root> <backend> [single|transaction] | verify <root> <backend> <receipts> [single|transaction]",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifier_rejects_a_missing_acknowledged_write() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        store.put(b"safe", b"acknowledged").unwrap();
        store.put(b"old", b"original").unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        std::fs::write(root.path().join("checkpoint.digest"), digest).unwrap();
        drop(store);
        let mut report = Report {
            workers: (0..WORKERS)
                .map(|id| Worker {
                    id,
                    acknowledged: if id == 0 { vec![0] } else { Vec::new() },
                    failed: Some(usize::from(id == 0)),
                    error: Some("synthetic error".into()),
                })
                .collect(),
        };
        let receipts = root.path().join("receipts.json");
        std::fs::write(&receipts, serde_json::to_vec(&report).unwrap()).unwrap();
        assert!(verify(root.path(), "ordinary", &receipts, Mode::Single).is_err());
        let store = Store::open(&source).unwrap();
        store.put(&key(0, 0), &value(0, 0)).unwrap();
        drop(store);
        verify(root.path(), "ordinary", &receipts, Mode::Single).unwrap();
        verify(root.path(), "checkpoint", &receipts, Mode::Single).unwrap();
        assert!(verify(root.path(), "ordinary", &receipts, Mode::Transaction).is_err());
        let store = Store::open(&source).unwrap();
        store.put(&peer_key(0, 0), &value(0, 0)).unwrap();
        drop(store);
        verify(root.path(), "ordinary", &receipts, Mode::Transaction).unwrap();
        verify(root.path(), "checkpoint", &receipts, Mode::Transaction).unwrap();
        report.workers[0].acknowledged = (0..OPERATIONS).collect();
        report.workers[0].failed = Some(OPERATIONS);
        std::fs::write(&receipts, serde_json::to_vec(&report).unwrap()).unwrap();
        assert_eq!(
            verify(root.path(), "ordinary", &receipts, Mode::Transaction)
                .unwrap_err()
                .to_string(),
            "invalid writer receipt"
        );
    }

    #[test]
    fn verifier_accepts_only_complete_or_absent_uncertain_transactions() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        store.put(b"safe", b"acknowledged").unwrap();
        store.put(b"old", b"original").unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        std::fs::write(root.path().join("checkpoint.digest"), digest).unwrap();
        let report = Report {
            workers: (0..WORKERS)
                .map(|id| Worker {
                    id,
                    acknowledged: Vec::new(),
                    failed: Some(0),
                    error: Some("synthetic error".into()),
                })
                .collect(),
        };
        let receipts = root.path().join("receipts.json");
        std::fs::write(&receipts, serde_json::to_vec(&report).unwrap()).unwrap();
        store.put(&key(0, 0), &value(0, 0)).unwrap();
        drop(store);
        assert!(verify(root.path(), "ordinary", &receipts, Mode::Transaction).is_err());
        let store = Store::open(&source).unwrap();
        store.put(&peer_key(0, 0), &value(0, 0)).unwrap();
        drop(store);
        verify(root.path(), "ordinary", &receipts, Mode::Transaction).unwrap();
        verify(root.path(), "checkpoint", &receipts, Mode::Transaction).unwrap();
        let store = Store::open(&source).unwrap();
        store.delete(&key(0, 0)).unwrap();
        store.delete(&peer_key(0, 0)).unwrap();
        drop(store);
        verify(root.path(), "ordinary", &receipts, Mode::Transaction).unwrap();
        verify(root.path(), "checkpoint", &receipts, Mode::Transaction).unwrap();
    }
}
