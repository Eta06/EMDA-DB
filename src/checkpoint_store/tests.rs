//! Fault injection against the checkpoint writer's real append and witness paths.
use super::*;
use crate::Store;
use crate::log_io::LogIo;
use crate::test_support::retry_lock;
use crate::witness::{self, Witness, WitnessIo};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::FileExt;

fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    [u8; 32],
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join("records.log"),
        include_bytes!("../../tests/fixtures/v2-empty-records.log"),
    )
    .unwrap();
    fs::write(
        path.join("commit.state"),
        include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
    )
    .unwrap();
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"acknowledged").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    (dir, path, checkpoint, digest)
}

#[derive(Clone, Copy)]
enum LogFault {
    Partial(usize),
    Panic(usize),
    SyncBefore,
    SyncAfter,
}

struct FaultLog {
    file: File,
    fault: LogFault,
}

impl LogIo for FaultLog {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, bytes, offset)
    }
    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        match self.fault {
            LogFault::Partial(count) | LogFault::Panic(count) => {
                (&self.file).write_all(&bytes[..count.min(bytes.len())])?;
                if matches!(self.fault, LogFault::Panic(_)) {
                    panic!("injected append panic");
                }
                Err(io::Error::other("injected partial append"))
            }
            _ => (&self.file).write_all(bytes),
        }
    }
    fn sync_data(&self) -> io::Result<()> {
        match self.fault {
            LogFault::SyncBefore => Err(io::Error::other("injected log sync failure")),
            LogFault::SyncAfter => {
                self.file.sync_data()?;
                Err(io::Error::other("injected post-sync failure"))
            }
            _ => self.file.sync_data(),
        }
    }
}

fn failed_instance_rejects_all_operations(store: &CheckpointStore, path: &Path) {
    let before = fs::metadata(path.join("records.log")).unwrap().len();
    assert!(store.get(b"base").is_err());
    assert!(store.scan(None, None, 10).is_err());
    assert!(store.put(b"other", b"must-not-write").is_err());
    let backup = path.parent().unwrap().join("must-not-backup");
    assert!(store.backup(&backup).is_err());
    assert!(!backup.exists());
    assert_eq!(
        fs::metadata(path.join("records.log")).unwrap().len(),
        before
    );
}

#[test]
fn every_partial_append_and_sync_failure_poison_until_reopen() {
    let length = record::encode(Operation::Put, b"k", b"new").unwrap().len();
    let faults = (0..=length).map(LogFault::Partial).chain([
        LogFault::Panic(3),
        LogFault::Panic(length),
        LogFault::SyncBefore,
        LogFault::SyncAfter,
    ]);
    for fault in faults {
        let (_dir, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store
            .log
            .replace_io_for_test(Box::new(FaultLog { file, fault }));
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.put(b"k", b"new")));
        assert!(outcome.is_err() || outcome.unwrap().is_err());
        failed_instance_rejects_all_operations(&store, &path);
        drop(store);
        let complete = match fault {
            LogFault::Partial(n) | LogFault::Panic(n) => n == 0 || n >= length,
            LogFault::SyncBefore | LogFault::SyncAfter => true,
        };
        let reopened = retry_lock(|| Store::open(&path));
        if complete {
            let reopened = reopened.unwrap();
            assert_eq!(
                reopened.get(b"base").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            let expected = match fault {
                LogFault::Partial(0) => None,
                _ => Some(&b"new"[..]),
            };
            assert_eq!(reopened.get(b"k").unwrap().as_deref(), expected);
        } else {
            assert!(matches!(
                reopened.err().unwrap().kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
        }
    }
}

#[derive(Clone, Copy)]
enum WitnessFault {
    Partial(usize),
    Panic(usize),
    SyncBefore,
    SyncAfter,
}

struct FaultWitness {
    file: File,
    fault: WitnessFault,
}

impl WitnessIo for FaultWitness {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, bytes, offset)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        match self.fault {
            WitnessFault::Partial(count) | WitnessFault::Panic(count) => {
                FileExt::write_all_at(&self.file, &bytes[..count.min(bytes.len())], offset)?;
                if matches!(self.fault, WitnessFault::Panic(_)) {
                    panic!("injected witness panic");
                }
                Err(io::Error::other("injected partial witness"))
            }
            _ => FileExt::write_all_at(&self.file, bytes, offset),
        }
    }
    fn sync_data(&self) -> io::Result<()> {
        match self.fault {
            WitnessFault::SyncBefore => Err(io::Error::other("injected witness sync failure")),
            WitnessFault::SyncAfter => {
                self.file.sync_data()?;
                Err(io::Error::other("injected post-sync witness failure"))
            }
            _ => self.file.sync_data(),
        }
    }
}

#[test]
fn every_partial_witness_write_and_sync_failure_poison_until_reopen() {
    let faults = (0..=witness::WITNESS_SIZE)
        .map(WitnessFault::Partial)
        .chain([
            WitnessFault::Panic(3),
            WitnessFault::Panic(witness::WITNESS_SIZE),
            WitnessFault::SyncBefore,
            WitnessFault::SyncAfter,
        ]);
    for fault in faults {
        let (_dir, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let header = fs::read(path.join("records.log")).unwrap();
        let id =
            witness::parse_header(header[..witness::LOG_HEADER_SIZE].try_into().unwrap()).unwrap();
        let boundary = header.len() as u64;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("commit.state"))
            .unwrap();
        store.log.attach_test_witness(Witness::for_test(
            Box::new(FaultWitness { file, fault }),
            id,
            boundary,
        ));
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.put(b"k", b"new")));
        assert!(outcome.is_err() || outcome.unwrap().is_err());
        failed_instance_rejects_all_operations(&store, &path);
        drop(store);
        match retry_lock(|| Store::open(&path)) {
            Ok(reopened) => {
                assert_eq!(
                    reopened.get(b"base").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                assert_eq!(reopened.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
            }
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )),
        }
    }
}

#[test]
fn interrupted_batch_is_never_partially_recovered() {
    let operations = [
        BatchOperation::Put {
            key: b"left",
            value: b"one",
        },
        BatchOperation::Put {
            key: b"right",
            value: b"two",
        },
    ];
    let payload = encode_payload(&operations).unwrap();
    let length = record::encode(Operation::Batch, &[0], &payload)
        .unwrap()
        .len();
    for offset in 0..=length {
        let (_dir, path, checkpoint, digest) = fixture();
        let mut store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        store.log.replace_io_for_test(Box::new(FaultLog {
            file,
            fault: LogFault::Partial(offset),
        }));
        assert!(store.write_batch(&operations).is_err());
        failed_instance_rejects_all_operations(&store, &path);
        drop(store);
        let reopened = retry_lock(|| Store::open(&path));
        if offset == 0 || offset == length {
            let reopened = reopened.unwrap();
            let values = reopened.get_many(&[b"left", b"right"]).unwrap();
            if offset == 0 {
                assert_eq!(values, vec![None, None]);
            } else {
                assert_eq!(values, vec![Some(b"one".to_vec()), Some(b"two".to_vec())]);
            }
            assert_eq!(
                reopened.get(b"base").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
        } else {
            assert!(matches!(
                reopened.err().unwrap().kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
        }
    }
}

#[test]
fn complete_unacknowledged_tail_is_validated_synchronized_and_adopted() {
    let (dir, path, checkpoint, digest) = fixture();
    let witness_before = fs::read(path.join("commit.state")).unwrap();
    let record = record::encode(Operation::Put, b"tail", b"unacknowledged").unwrap();
    OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(&record)
        .unwrap();
    assert_eq!(
        retry_lock(|| crate::RecoveredCheckpointView::open(&path, &checkpoint, &digest))
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        store.get(b"tail").unwrap().as_deref(),
        Some(&b"unacknowledged"[..])
    );
    assert_ne!(fs::read(path.join("commit.state")).unwrap(), witness_before);
    let next = dir.path().join("after-recovery");
    let next_digest = store.rollover_to(&next).unwrap();
    assert_eq!(store.tail_bytes().unwrap(), 0);
    store.put(b"tail", b"acknowledged").unwrap();
    drop(store);
    let reopened = retry_lock(|| CheckpointStore::open(&path, &next, &next_digest)).unwrap();
    assert_eq!(
        reopened.get(b"tail").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    drop(reopened);
    assert_eq!(
        retry_lock(|| Store::open(&path))
            .unwrap()
            .get(b"tail")
            .unwrap()
            .as_deref(),
        Some(&b"acknowledged"[..])
    );
}

#[test]
fn complete_unacknowledged_batch_replays_after_witnessed_suffix() {
    let (_dir, path, checkpoint, digest) = fixture();
    let ordinary = retry_lock(|| Store::open(&path)).unwrap();
    ordinary.put(b"witnessed", b"old").unwrap();
    drop(ordinary);
    let payload = encode_payload(&[
        BatchOperation::Put {
            key: b"witnessed",
            value: b"new",
        },
        BatchOperation::Delete { key: b"base" },
        BatchOperation::Put {
            key: b"inserted",
            value: b"value",
        },
    ])
    .unwrap();
    let record = record::encode(Operation::Batch, &[0], &payload).unwrap();
    OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(&record)
        .unwrap();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        store.get(b"witnessed").unwrap().as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(store.get(b"base").unwrap(), None);
    assert_eq!(
        store.get(b"inserted").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    drop(store);
    let ordinary = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        ordinary.get(b"witnessed").unwrap().as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(ordinary.get(b"base").unwrap(), None);
    assert_eq!(
        ordinary.get(b"inserted").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn incomplete_single_and_batch_tails_fail_without_modifying_log_or_witness() {
    let payload = encode_payload(&[
        BatchOperation::Put {
            key: b"left",
            value: b"one",
        },
        BatchOperation::Delete { key: b"base" },
    ])
    .unwrap();
    let records = [
        record::encode(Operation::Put, b"tail", b"value").unwrap(),
        record::encode(Operation::Batch, &[0], &payload).unwrap(),
    ];
    for bytes in records {
        for offset in 1..bytes.len() {
            let (_dir, path, checkpoint, digest) = fixture();
            let witness = fs::read(path.join("commit.state")).unwrap();
            let log = path.join("records.log");
            OpenOptions::new()
                .append(true)
                .open(&log)
                .unwrap()
                .write_all(&bytes[..offset])
                .unwrap();
            let before = fs::read(&log).unwrap();
            let error = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest))
                .err()
                .unwrap();
            assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
            assert_eq!(fs::read(&log).unwrap(), before);
            assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness);
        }
    }
}

#[test]
fn unacknowledged_tail_limits_and_non_boundary_witness_fail_without_adoption() {
    for cap in ["bytes", "keys", "key-bytes", "boundary"] {
        let (_dir, path, checkpoint, digest) = fixture();
        let bytes = record::encode(Operation::Put, b"tail", b"value").unwrap();
        let log = path.join("records.log");
        let before_len = fs::metadata(&log).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        if cap == "boundary" {
            let header = fs::read(&log).unwrap();
            let id = witness::parse_header(header[..witness::LOG_HEADER_SIZE].try_into().unwrap())
                .unwrap();
            fs::write(
                path.join("commit.state"),
                witness::encode_for_test(&id, before_len + 1),
            )
            .unwrap();
        }
        let before_log = fs::read(&log).unwrap();
        let before_witness = fs::read(path.join("commit.state")).unwrap();
        let (max_tail, max_keys, max_key_bytes) = match cap {
            "bytes" => (bytes.len() as u64 - 1, 5, 100),
            "keys" => (1000, 0, 100),
            "key-bytes" => (1000, 5, 3),
            _ => (1000, 5, 100),
        };
        assert!(
            retry_lock(|| CheckpointStore::open_with_limits(
                &path,
                &checkpoint,
                &digest,
                max_tail,
                max_keys,
                max_key_bytes
            ))
            .is_err()
        );
        assert_eq!(fs::read(&log).unwrap(), before_log);
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), before_witness);
    }
}

#[test]
fn unacknowledged_tail_log_sync_failures_return_error_and_retry_safely() {
    for fault in [LogFault::SyncBefore, LogFault::SyncAfter] {
        let (_dir, path, checkpoint, digest) = fixture();
        let record = record::encode(Operation::Put, b"tail", b"value").unwrap();
        OpenOptions::new()
            .append(true)
            .open(path.join("records.log"))
            .unwrap()
            .write_all(&record)
            .unwrap();
        let log_before = fs::read(path.join("records.log")).unwrap();
        let witness_before = fs::read(path.join("commit.state")).unwrap();
        let result = retry_lock(|| {
            CheckpointStore::open_using(
                &path,
                &checkpoint,
                &digest,
                MAX_RECOVERED_VIEW_TAIL_BYTES,
                MAX_RECOVERED_VIEW_TAIL_KEYS,
                MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                |path, boundary, length| {
                    let mut log = Log::open_checkpoint_writer(path, boundary, length)?;
                    let file = OpenOptions::new()
                        .read(true)
                        .append(true)
                        .open(path.join("records.log"))?;
                    log.replace_io_for_test(Box::new(FaultLog { file, fault }));
                    Ok(log)
                },
            )
        });
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("outcome is uncertain")
        );
        assert_eq!(fs::read(path.join("records.log")).unwrap(), log_before);
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), witness_before);
        let recovered = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        assert_eq!(
            recovered.get(b"tail").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}

#[test]
fn unacknowledged_tail_witness_failures_never_return_a_store() {
    let faults = (0..=witness::WITNESS_SIZE)
        .map(WitnessFault::Partial)
        .chain([
            WitnessFault::Panic(3),
            WitnessFault::Panic(witness::WITNESS_SIZE),
            WitnessFault::SyncBefore,
            WitnessFault::SyncAfter,
        ]);
    for fault in faults {
        let (_dir, path, checkpoint, digest) = fixture();
        let log_path = path.join("records.log");
        let header = fs::read(&log_path).unwrap();
        let id =
            witness::parse_header(header[..witness::LOG_HEADER_SIZE].try_into().unwrap()).unwrap();
        let record = record::encode(Operation::Put, b"tail", b"value").unwrap();
        OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(&record)
            .unwrap();
        let log_before = fs::read(&log_path).unwrap();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            retry_lock(|| {
                CheckpointStore::open_using(
                    &path,
                    &checkpoint,
                    &digest,
                    MAX_RECOVERED_VIEW_TAIL_BYTES,
                    MAX_RECOVERED_VIEW_TAIL_KEYS,
                    MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                    |path, boundary, length| {
                        let mut log = Log::open_checkpoint_writer(path, boundary, length)?;
                        let file = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(path.join("commit.state"))?;
                        log.attach_test_witness(Witness::for_test(
                            Box::new(FaultWitness { file, fault }),
                            id,
                            boundary,
                        ));
                        Ok(log)
                    },
                )
            })
        }));
        assert!(outcome.is_err() || outcome.unwrap().is_err());
        assert_eq!(fs::read(&log_path).unwrap(), log_before);
        match retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)) {
            Ok(recovered) => assert_eq!(
                recovered.get(b"tail").unwrap().as_deref(),
                Some(&b"value"[..])
            ),
            Err(error) => assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )),
        }
    }
}

#[test]
fn streamed_checkpoint_parent_sync_failure_reports_uncertain_publication() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"new", b"value").unwrap();
    let destination = dir.path().join("published");
    let error = store
        .checkpoint_to_with_sync(&destination, |_| {
            Err(io::Error::other("injected parent sync failure"))
        })
        .unwrap_err();
    assert!(error.to_string().contains("checkpoint may exist"));
    assert!(destination.exists());
    assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
    drop(store);
    // The checksum is read from the fixture only to inspect this uncertain
    // publication; real callers must protect the digest separately.
    let bytes = fs::read(&destination).unwrap();
    let published_digest = bytes[bytes.len() - 32..].try_into().unwrap();
    let reopened =
        retry_lock(|| CheckpointStore::open(&path, &destination, &published_digest)).unwrap();
    assert_eq!(
        reopened.get(b"base").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(
        reopened.get(b"new").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn streamed_checkpoint_prepublication_failure_leaves_source_usable() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"new", b"value").unwrap();
    let destination = dir.path().join("not-published");
    let state = store.write().unwrap();
    let (count, peak) = store.verify_for_checkpoint(&state).unwrap();
    let result = crate::checkpoint::create_streaming_with_hooks(
        &store.log,
        count,
        peak,
        &destination,
        |emit| {
            state.view.visit_entries(None, None, |key, location| {
                emit(key, location)?;
                Ok(std::ops::ControlFlow::Continue(()))
            })
        },
        |parent| File::open(parent)?.sync_all(),
        || Err(io::Error::other("injected before publication")),
    );
    assert!(result.is_err());
    assert!(!destination.exists());
    drop(state);
    assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"value"[..]));
    store.put(b"later", b"still-writable").unwrap();
    drop(store);
    let reopened = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        reopened.get(b"later").unwrap().as_deref(),
        Some(&b"still-writable"[..])
    );
}

#[test]
fn streamed_checkpoint_blocks_writes_until_verified_publication() {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    let (dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    store.put(b"before", b"value").unwrap();
    let destination = dir.path().join("next");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let new_digest = std::thread::scope(|scope| {
        let checkpoint_store = Arc::clone(&store);
        let destination = &destination;
        let checkpoint_thread = scope.spawn(move || {
            let state = checkpoint_store.write().unwrap();
            let (count, peak) = checkpoint_store.verify_for_checkpoint(&state).unwrap();
            crate::checkpoint::create_streaming_with_hooks(
                &checkpoint_store.log,
                count,
                peak,
                destination,
                |emit| {
                    state.view.visit_entries(None, None, |key, location| {
                        emit(key, location)?;
                        Ok(std::ops::ControlFlow::Continue(()))
                    })
                },
                |parent| File::open(parent)?.sync_all(),
                || {
                    entered_tx.send(()).unwrap();
                    resume_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|_| io::Error::other("checkpoint test release timed out"))?;
                    Ok(())
                },
            )
            .unwrap()
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let writer = Arc::clone(&store);
        let writer_thread = scope.spawn(move || {
            started_tx.send(()).unwrap();
            let result = writer.put(b"after", b"value");
            done_tx.send(result).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(50));
        resume_tx.send(()).unwrap();
        let digest = checkpoint_thread.join().unwrap();
        writer_thread.join().unwrap();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        digest
    });
    drop(store);
    let historical =
        retry_lock(|| crate::CheckpointView::open(&path, &destination, &new_digest)).unwrap();
    assert_eq!(
        historical.get(b"before").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    assert_eq!(historical.get(b"after").unwrap(), None);
    drop(historical);
    let reopened = retry_lock(|| CheckpointStore::open(&path, &destination, &new_digest)).unwrap();
    assert_eq!(
        reopened.get(b"after").unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[test]
fn checkpoint_backup_blocks_writes_but_allows_readers() {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    let (dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let destination = dir.path().join("backup");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let backing = Arc::clone(&store);
        let destination = &destination;
        let backup = scope.spawn(move || {
            backing
                .backup_using(destination, |log, destination| {
                    crate::backup::create_with_hooks(
                        log,
                        destination,
                        |parent| File::open(parent)?.sync_all(),
                        || {
                            entered_tx.send(()).unwrap();
                            resume_rx
                                .recv_timeout(Duration::from_secs(10))
                                .map_err(|_| io::Error::other("backup test release timed out"))?;
                            Ok(())
                        },
                    )
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(
            store.get(b"base").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        let writer = Arc::clone(&store);
        let writer = scope.spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(writer.put(b"base", b"later")).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(50));
        resume_tx.send(()).unwrap();
        backup.join().unwrap();
        writer.join().unwrap();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
    });
    assert_eq!(store.get(b"base").unwrap().as_deref(), Some(&b"later"[..]));
    drop(store);
    let restored = dir.path().join("restored");
    Store::restore(&destination, &restored).unwrap();
    let copy = retry_lock(|| Store::open(&restored)).unwrap();
    assert_eq!(
        copy.get(b"base").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}

#[test]
fn live_rollover_sync_and_switch_failures_leave_the_old_view_usable() {
    for failure in ["sync", "switch"] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| {
            CheckpointStore::open_with_limits(&path, &checkpoint, &digest, 1000, 1, 3)
        })
        .unwrap();
        store.put(b"new", b"first").unwrap();
        let original_tail = store.tail_bytes().unwrap();
        let destination = dir.path().join("next");
        let result = store.rollover_to_with_sync(&destination, |parent| {
            if failure == "sync" {
                Err(io::Error::other("injected directory sync error"))
            } else {
                fs::remove_file(&destination)?;
                File::open(parent)?.sync_all()
            }
        });
        let error = result.unwrap_err().to_string();
        if failure == "sync" {
            assert!(error.contains("checkpoint may exist"));
            assert!(destination.exists());
        } else {
            assert!(error.contains("checkpoint was published but switching failed"));
            assert!(!destination.exists());
        }
        assert_eq!(store.tail_bytes().unwrap(), original_tail);
        assert_eq!(store.overlay_keys().unwrap(), 1);
        assert_eq!(store.get(b"new").unwrap().as_deref(), Some(&b"first"[..]));
        store.put(b"new", b"second").unwrap();
        assert!(store.put(b"other", b"full").is_err());
        drop(store);
        let reopened = retry_lock(|| Store::open(&path)).unwrap();
        assert_eq!(
            reopened.get(b"new").unwrap().as_deref(),
            Some(&b"second"[..])
        );
    }
}

#[cfg(target_os = "linux")]
mod metadata_faults {
    use super::*;
    use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};

    #[derive(Clone, Copy)]
    enum Fault {
        WriteBefore,
        WriteAfter,
        WritePanic,
        SyncBefore,
        SyncAfter,
        SyncPanic,
    }

    struct Metadata {
        file: File,
        fault: Fault,
    }

    impl WitnessIo for Metadata {
        fn len(&self) -> io::Result<u64> {
            Ok(witness::WITNESS_SIZE as u64)
        }
        fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            if offset != 0 || bytes.len() != witness::WITNESS_SIZE {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            bytes.copy_from_slice(&self.read_marker()?);
            Ok(())
        }
        fn read_marker(&self) -> io::Result<[u8; witness::WITNESS_SIZE]> {
            let mut bytes = [0; witness::WITNESS_SIZE];
            let count = fgetxattr(&self.file, "user.db.commit.v3", &mut bytes[..])?;
            if count != bytes.len() {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(bytes)
        }
        fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
            if offset != 0 {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            if matches!(self.fault, Fault::WriteBefore) {
                return Err(io::Error::other("injected metadata write failure"));
            }
            fsetxattr(&self.file, "user.db.commit.v3", bytes, XattrFlags::REPLACE)?;
            match self.fault {
                Fault::WriteAfter => Err(io::Error::other("injected post-write failure")),
                Fault::WritePanic => panic!("injected metadata write panic"),
                _ => Ok(()),
            }
        }
        fn sync_data(&self) -> io::Result<()> {
            if matches!(self.fault, Fault::SyncBefore) {
                return Err(io::Error::other("injected metadata sync failure"));
            }
            self.file.sync_all()?;
            match self.fault {
                Fault::SyncAfter => Err(io::Error::other("injected post-sync failure")),
                Fault::SyncPanic => panic!("injected metadata sync panic"),
                _ => Ok(()),
            }
        }
    }

    #[test]
    fn metadata_write_sync_failures_and_panics_fail_closed_for_single_and_batch_writes() {
        for fault in [
            Fault::WriteBefore,
            Fault::WriteAfter,
            Fault::WritePanic,
            Fault::SyncBefore,
            Fault::SyncAfter,
            Fault::SyncPanic,
        ] {
            for batch in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("store");
                let checkpoint = dir.path().join("checkpoint");
                let ordinary = Store::open(&path).unwrap();
                ordinary.put(b"base", b"acknowledged").unwrap();
                let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
                drop(ordinary);
                let header = fs::read(path.join("records.log")).unwrap();
                assert_eq!(
                    &header[..8],
                    b"EMDALOG3",
                    "test requires a filesystem supporting V3 xattrs"
                );
                let id = crate::metadata_witness::parse_header(
                    header[..witness::LOG_HEADER_SIZE].try_into().unwrap(),
                )
                .unwrap();
                let mut store =
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path.join("records.log"))
                    .unwrap();
                store.log.attach_test_witness(Witness::for_test(
                    Box::new(Metadata { file, fault }),
                    id,
                    header.len() as u64,
                ));
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if batch {
                        store.write_batch(&[
                            BatchOperation::Put {
                                key: b"left",
                                value: b"one",
                            },
                            BatchOperation::Put {
                                key: b"right",
                                value: b"two",
                            },
                        ])
                    } else {
                        store.put(b"left", b"one")
                    }
                }));
                assert!(outcome.is_err() || outcome.unwrap().is_err());
                failed_instance_rejects_all_operations(&store, &path);
                drop(store);
                let reopened = retry_lock(|| Store::open(&path)).unwrap();
                assert_eq!(
                    reopened.get(b"base").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                assert_eq!(reopened.get(b"left").unwrap().as_deref(), Some(&b"one"[..]));
                assert_eq!(
                    reopened.get(b"right").unwrap().as_deref(),
                    if batch { Some(&b"two"[..]) } else { None }
                );
            }
        }
    }

    #[test]
    fn unacknowledged_tail_metadata_failures_do_not_return_a_store() {
        for fault in [
            Fault::WriteBefore,
            Fault::WriteAfter,
            Fault::WritePanic,
            Fault::SyncBefore,
            Fault::SyncAfter,
            Fault::SyncPanic,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store");
            let checkpoint = dir.path().join("checkpoint");
            let ordinary = Store::open(&path).unwrap();
            ordinary.put(b"base", b"acknowledged").unwrap();
            let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
            drop(ordinary);
            let log_path = path.join("records.log");
            let header = fs::read(&log_path).unwrap();
            assert_eq!(&header[..8], b"EMDALOG3");
            let id = crate::metadata_witness::parse_header(
                header[..witness::LOG_HEADER_SIZE].try_into().unwrap(),
            )
            .unwrap();
            let record = record::encode(Operation::Put, b"tail", b"value").unwrap();
            OpenOptions::new()
                .append(true)
                .open(&log_path)
                .unwrap()
                .write_all(&record)
                .unwrap();
            let log_before = fs::read(&log_path).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                retry_lock(|| {
                    CheckpointStore::open_using(
                        &path,
                        &checkpoint,
                        &digest,
                        MAX_RECOVERED_VIEW_TAIL_BYTES,
                        MAX_RECOVERED_VIEW_TAIL_KEYS,
                        MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                        |path, boundary, length| {
                            let mut log = Log::open_checkpoint_writer(path, boundary, length)?;
                            let file = OpenOptions::new()
                                .read(true)
                                .write(true)
                                .open(path.join("records.log"))?;
                            log.attach_test_witness(Witness::for_test(
                                Box::new(Metadata { file, fault }),
                                id,
                                boundary,
                            ));
                            Ok(log)
                        },
                    )
                })
            }));
            assert!(result.is_err() || result.unwrap().is_err());
            assert_eq!(fs::read(&log_path).unwrap(), log_before);
            let recovered =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            assert_eq!(
                recovered.get(b"base").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            assert_eq!(
                recovered.get(b"tail").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        }
    }

    #[test]
    fn every_partial_v3_tail_preserves_log_and_metadata_witness() {
        let payload = encode_payload(&[
            BatchOperation::Put {
                key: b"left",
                value: b"one",
            },
            BatchOperation::Delete { key: b"base" },
        ])
        .unwrap();
        let records = [
            record::encode(Operation::Put, b"tail", b"value").unwrap(),
            record::encode(Operation::Batch, &[0], &payload).unwrap(),
        ];
        for record in records {
            for offset in 1..record.len() {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("store");
                let checkpoint = dir.path().join("checkpoint");
                let ordinary = Store::open(&path).unwrap();
                ordinary.put(b"base", b"acknowledged").unwrap();
                let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
                drop(ordinary);
                let log_path = path.join("records.log");
                let file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&log_path)
                    .unwrap();
                assert!(fs::read(&log_path).unwrap().starts_with(b"EMDALOG3"));
                let mut before_marker = [0; witness::WITNESS_SIZE];
                assert_eq!(
                    fgetxattr(&file, "user.db.commit.v3", &mut before_marker[..]).unwrap(),
                    before_marker.len()
                );
                (&file).write_all(&record[..offset]).unwrap();
                let before_log = fs::read(&log_path).unwrap();
                let error = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest))
                    .err()
                    .unwrap();
                assert!(matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ));
                assert_eq!(fs::read(&log_path).unwrap(), before_log);
                let mut after_marker = [0; witness::WITNESS_SIZE];
                assert_eq!(
                    fgetxattr(&file, "user.db.commit.v3", &mut after_marker[..]).unwrap(),
                    after_marker.len()
                );
                assert_eq!(after_marker, before_marker);
            }
        }
    }
}
