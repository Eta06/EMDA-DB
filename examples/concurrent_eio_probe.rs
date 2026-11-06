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
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
}
impl Backend for Store {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        Store::put(self, key, value)
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        Store::get(self, key)
    }
}
impl Backend for CheckpointStore {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        CheckpointStore::put(self, key, value)
    }
    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        CheckpointStore::get(self, key)
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

fn attempt(root: &Path, backend: &str) -> io::Result<()> {
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
                    match store.put(&key(id, sequence), &value(id, sequence)) {
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

fn verify(root: &Path, backend: &str, receipts: &Path) -> io::Result<()> {
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
        serde_json::json!({"status":"verified","backend":backend,
             "acknowledged":report.workers.iter().map(|worker| worker.acknowledged.len()).sum::<usize>(),
             "uncertain_present":unknown_present})
    );
    Ok(())
}

fn main() -> io::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, root, backend] if command == "attempt" => attempt(Path::new(root), backend),
        [command, root, backend, receipts] if command == "verify" => {
            verify(Path::new(root), backend, Path::new(receipts))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: concurrent_eio_probe attempt <root> <backend> | verify <root> <backend> <receipts>",
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
        let report = Report {
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
        assert!(verify(root.path(), "ordinary", &receipts).is_err());
        let store = Store::open(&source).unwrap();
        store.put(&key(0, 0), &value(0, 0)).unwrap();
        drop(store);
        verify(root.path(), "ordinary", &receipts).unwrap();
        verify(root.path(), "checkpoint", &receipts).unwrap();
    }
}
