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
fn complete_unacknowledged_tail_requires_ordinary_recovery_first() {
    let (_dir, path, checkpoint, digest) = fixture();
    let record = record::encode(Operation::Put, b"tail", b"unacknowledged").unwrap();
    OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(&record)
        .unwrap();
    assert_eq!(
        retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest))
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let recovered = retry_lock(|| Store::open(&path)).unwrap();
    assert_eq!(
        recovered.get(b"tail").unwrap().as_deref(),
        Some(&b"unacknowledged"[..])
    );
    drop(recovered);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    assert_eq!(
        store.get(b"tail").unwrap().as_deref(),
        Some(&b"unacknowledged"[..])
    );
    store.put(b"tail", b"acknowledged").unwrap();
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&path))
            .unwrap()
            .get(b"tail")
            .unwrap()
            .as_deref(),
        Some(&b"acknowledged"[..])
    );
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
}
