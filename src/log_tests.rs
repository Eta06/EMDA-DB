//! Fault injection at the log-sync and witness-update boundaries.
use super::*;
use crate::witness::{self, WitnessIo};
use std::sync::{Arc, Mutex};

#[test]
fn verification_key_byte_overflow_distinguishes_admission_from_corruption() {
    assert_eq!(verified_key_bytes(usize::MAX, 0, true).unwrap(), usize::MAX);
    assert_eq!(
        verified_key_bytes(usize::MAX, 1, true).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        verified_key_bytes(usize::MAX, 1, false).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn replayed_tail_adoption_rejects_changes_after_validation() {
    for (same_length, use_checkpoint) in [(false, false), (true, false), (true, true)] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path();
        std::fs::write(
            path.join("records.log"),
            include_bytes!("../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap();
        std::fs::write(
            path.join("commit.state"),
            include_bytes!("../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap();
        let store = crate::Store::open(path).unwrap();
        store.put(b"base", b"committed").unwrap();
        let checkpoint_path = path.join("checkpoint");
        let digest = store.checkpoint_to(&checkpoint_path).unwrap();
        let checkpoint = use_checkpoint.then_some((checkpoint_path.as_path(), &digest));
        let original_witness = std::fs::read(path.join("commit.state")).unwrap();
        store.put(b"tail", b"complete").unwrap();
        drop(store);
        std::fs::write(path.join("commit.state"), &original_witness).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        let (id, metadata) = log_identity(&file).unwrap();
        let (witness, boundary) = Witness::open(path, id.unwrap()).unwrap();
        let (log, index, end) = Log::from_parts(
            Box::new(file),
            id,
            Some(witness),
            Some(boundary),
            metadata,
            (StoreRecoveryLimits::default(), None),
            checkpoint,
        )
        .unwrap();
        assert!(index.contains_key(&b"tail"[..]));
        let original = std::fs::read(path.join("records.log")).unwrap();
        let mut expected = original.clone();
        if same_length {
            let replacement = record::encode(Operation::Put, b"swap", b"complete").unwrap();
            let start = end.length - replacement.len() as u64;
            let file = OpenOptions::new()
                .write(true)
                .open(path.join("records.log"))
                .unwrap();
            std::os::unix::fs::FileExt::write_all_at(&file, &replacement, start).unwrap();
            expected[start as usize..].copy_from_slice(&replacement);
        } else {
            OpenOptions::new()
                .append(true)
                .open(path.join("records.log"))
                .unwrap()
                .write_all(b"x")
                .unwrap();
            expected.push(b'x');
        }
        let error = log
            .adopt_replayed_tail(Some(boundary), end, || Ok(()), None)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        if same_length {
            assert_eq!(error.to_string(), "log bytes changed after verified replay");
        }
        assert_eq!(
            std::fs::read(path.join("commit.state")).unwrap(),
            original_witness
        );
        assert_eq!(std::fs::read(path.join("records.log")).unwrap(), expected);

        drop(log);
        std::fs::write(path.join("records.log"), original).unwrap();
        let recovered = match checkpoint {
            Some((path_to_checkpoint, digest)) => {
                crate::Store::open_with_checkpoint(path, path_to_checkpoint, digest).unwrap()
            }
            None => crate::Store::open(path).unwrap(),
        };
        assert_eq!(
            recovered.get(b"base").unwrap().as_deref(),
            Some(b"committed".as_slice())
        );
        assert_eq!(
            recovered.get(b"tail").unwrap().as_deref(),
            Some(b"complete".as_slice())
        );
        assert_eq!(recovered.get(b"swap").unwrap(), None);
    }
}

#[test]
#[cfg(target_os = "linux")]
fn replayed_v3_tail_adoption_rejects_growth_after_validation() {
    use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let store = crate::Store::open(&path).unwrap();
    let marker_file = File::open(path.join("records.log")).unwrap();
    let mut witness = [0_u8; 36];
    assert_eq!(
        fgetxattr(&marker_file, "user.db.commit.v3", &mut witness).unwrap(),
        36
    );
    let anchor = std::fs::read(path.join("commit.state")).unwrap();
    store.put(b"tail", b"complete").unwrap();
    drop(store);
    fsetxattr(
        &marker_file,
        "user.db.commit.v3",
        &witness,
        XattrFlags::REPLACE,
    )
    .unwrap();
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    let (id, metadata) = log_identity(&file).unwrap();
    assert!(metadata);
    let (attached, boundary) =
        metadata_witness::open(file.try_clone().unwrap(), id.unwrap()).unwrap();
    let (log, index, end) = Log::from_parts(
        Box::new(file),
        id,
        Some(attached),
        Some(boundary),
        metadata,
        (StoreRecoveryLimits::default(), None),
        None,
    )
    .unwrap();
    assert!(index.contains_key(&b"tail"[..]));
    let mut expected = std::fs::read(path.join("records.log")).unwrap();
    OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap()
        .write_all(b"x")
        .unwrap();
    expected.push(b'x');
    assert_eq!(
        log.adopt_replayed_tail(Some(boundary), end, || Ok(()), None)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let mut after = [0_u8; 36];
    assert_eq!(
        fgetxattr(&marker_file, "user.db.commit.v3", &mut after).unwrap(),
        36
    );
    assert_eq!(after, witness);
    assert_eq!(std::fs::read(path.join("commit.state")).unwrap(), anchor);
    assert_eq!(std::fs::read(path.join("records.log")).unwrap(), expected);
}

#[test]
fn detached_checkpoint_view_cannot_acknowledge_writes_or_verify_a_missing_witness() {
    for v2 in [true, false] {
        if !v2 && !cfg!(target_os = "linux") {
            continue;
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        if v2 {
            std::fs::create_dir(&path).unwrap();
            std::fs::write(
                path.join("records.log"),
                include_bytes!("../tests/fixtures/v2-empty-records.log"),
            )
            .unwrap();
            std::fs::write(
                path.join("commit.state"),
                include_bytes!("../tests/fixtures/v2-empty-commit.state"),
            )
            .unwrap();
        }
        let store = crate::Store::open(&path).unwrap();
        store.put(b"safe", b"acknowledged").unwrap();
        drop(store);
        let original = std::fs::read(path.join("records.log")).unwrap();
        assert_eq!(&original[..8], if v2 { b"EMDALOG2" } else { b"EMDALOG3" });
        let (mut log, boundary) = Log::open_checkpoint_view(&path).unwrap();
        assert_eq!(boundary, original.len() as u64);
        log.release_view_witness().unwrap();
        assert!(log.verify_witness().is_err());
        assert!(log.sync_data().is_err());
        assert!(log.append_batch_unsynced(std::iter::empty()).is_err());
        assert_eq!(log.len().unwrap(), original.len() as u64);
        assert_eq!(std::fs::read(path.join("records.log")).unwrap(), original);
        drop(log);
        let reopened = crate::test_support::retry_lock(|| crate::Store::open(&path)).unwrap();
        assert_eq!(
            reopened.get(b"safe").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    LogSync,
    WitnessRead,
    WitnessWriteBefore,
    WitnessWriteAfter,
    WitnessSync,
}

struct Shared {
    events: Mutex<Vec<&'static str>>,
    witness: Mutex<Vec<u8>>,
    fault: Fault,
}
struct LogHandle(Arc<Shared>);
struct WitnessHandle(Arc<Shared>);

impl LogIo for LogHandle {
    fn len(&self) -> io::Result<u64> {
        Ok(128)
    }
    fn read_exact_at(&self, _buffer: &mut [u8], _offset: u64) -> io::Result<()> {
        unreachable!()
    }
    fn append_all(&self, _bytes: &[u8]) -> io::Result<()> {
        unreachable!()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.events.lock().unwrap().push("log_sync");
        if self.0.fault == Fault::LogSync {
            return Err(io::Error::other("log sync failure"));
        }
        Ok(())
    }
}
impl WitnessIo for WitnessHandle {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.witness.lock().unwrap().len() as u64)
    }
    fn read_exact_at(&self, bytes: &mut [u8], _offset: u64) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_read");
        if self.0.fault == Fault::WitnessRead {
            return Err(io::Error::other("witness read failure"));
        }
        bytes.copy_from_slice(&self.0.witness.lock().unwrap());
        Ok(())
    }
    fn write_all_at(&self, bytes: &[u8], _offset: u64) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_write");
        if self.0.fault == Fault::WitnessWriteBefore {
            return Err(io::Error::other("witness write failure"));
        }
        self.0.witness.lock().unwrap().copy_from_slice(bytes);
        if self.0.fault == Fault::WitnessWriteAfter {
            return Err(io::Error::other("witness write outcome unknown"));
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_sync");
        if self.0.fault == Fault::WitnessSync {
            return Err(io::Error::other("witness sync failure"));
        }
        Ok(())
    }
}

#[test]
fn log_sync_precedes_witness_and_all_boundary_failures_propagate() {
    for (fault, expected) in [
        (Fault::LogSync, &["log_sync"][..]),
        (Fault::WitnessRead, &["log_sync", "witness_read"][..]),
        (
            Fault::WitnessWriteBefore,
            &["log_sync", "witness_read", "witness_write"][..],
        ),
        (
            Fault::WitnessWriteAfter,
            &["log_sync", "witness_read", "witness_write"][..],
        ),
        (
            Fault::WitnessSync,
            &["log_sync", "witness_read", "witness_write", "witness_sync"][..],
        ),
        (
            Fault::None,
            &["log_sync", "witness_read", "witness_write", "witness_sync"][..],
        ),
    ] {
        let id = [3; 16];
        let state = Arc::new(Shared {
            events: Mutex::new(Vec::new()),
            witness: Mutex::new(witness::encode_for_test(&id, 28)),
            fault,
        });
        let log = Log {
            metadata: false,
            anchor: None,
            file: Box::new(LogHandle(Arc::clone(&state))),
            log_id: Some(id),
            witness: Some(Witness::for_test(
                Box::new(WitnessHandle(Arc::clone(&state))),
                id,
                28,
            )),
            ordered_barrier_sync: false,
        };
        let outcome = log.sync_data();
        assert_eq!(outcome.is_ok(), fault == Fault::None);
        assert_eq!(&*state.events.lock().unwrap(), expected);
    }
}

#[test]
fn metadata_path_requires_witness_update_and_inode_sync_without_a_second_log_sync() {
    for (fault, expected) in [
        (Fault::WitnessRead, &["witness_read"][..]),
        (
            Fault::WitnessWriteBefore,
            &["witness_read", "witness_write"][..],
        ),
        (
            Fault::WitnessWriteAfter,
            &["witness_read", "witness_write"][..],
        ),
        (
            Fault::WitnessSync,
            &["witness_read", "witness_write", "witness_sync"][..],
        ),
        (
            Fault::None,
            &["witness_read", "witness_write", "witness_sync"][..],
        ),
    ] {
        let id = [4; 16];
        let state = Arc::new(Shared {
            events: Mutex::new(Vec::new()),
            witness: Mutex::new(witness::encode_for_test(&id, 28)),
            fault,
        });
        let log = Log {
            metadata: true,
            anchor: None,
            file: Box::new(LogHandle(Arc::clone(&state))),
            log_id: Some(id),
            witness: Some(Witness::for_test(
                Box::new(WitnessHandle(Arc::clone(&state))),
                id,
                28,
            )),
            ordered_barrier_sync: false,
        };
        assert_eq!(log.sync_data().is_ok(), fault == Fault::None);
        assert_eq!(&*state.events.lock().unwrap(), expected);
    }
}

struct ReplayFile {
    bytes: Vec<u8>,
    reads: Arc<Mutex<Vec<usize>>>,
    fail_read: Option<usize>,
}

#[test]
fn upgrade_verification_reads_records_once_with_the_bound_header() {
    use sha2::Digest;

    let header = witness::new_header().unwrap();
    let records = record::encode(Operation::Put, b"large", &vec![7; 256 * 1024]).unwrap();
    let digest = Sha256::digest(&records).into();
    let mut bytes = header.to_vec();
    bytes.extend_from_slice(&records);
    let length = bytes.len() as u64;
    let reads = Arc::new(Mutex::new(Vec::new()));
    Log::verify_upgraded_io(
        Box::new(ReplayFile {
            bytes,
            reads: Arc::clone(&reads),
            fail_read: None,
        }),
        length,
        &header,
        &digest,
    )
    .unwrap();
    let sizes = reads.lock().unwrap();
    assert_eq!(sizes.first(), Some(&LOG_HEADER_SIZE));
    assert_eq!(sizes.last(), Some(&LOG_HEADER_SIZE));
    assert_eq!(
        sizes[1..sizes.len() - 1].iter().sum::<usize>(),
        records.len()
    );
}

impl LogIo for ReplayFile {
    fn len(&self) -> io::Result<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        let mut reads = self.reads.lock().unwrap();
        reads.push(buffer.len());
        if self.fail_read == Some(reads.len()) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "injected short read",
            ));
        }
        drop(reads);
        let start = usize::try_from(offset).unwrap();
        let end = start.checked_add(buffer.len()).unwrap();
        buffer.copy_from_slice(
            self.bytes.get(start..end).ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "replay read past end")
            })?,
        );
        Ok(())
    }

    fn append_all(&self, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::other("replay file is read-only"))
    }

    fn sync_data(&self) -> io::Result<()> {
        Err(io::Error::other("replay file is read-only"))
    }
}

fn replay_log(bytes: Vec<u8>, id: Option<[u8; 16]>) -> (Arc<Mutex<Vec<usize>>>, Log) {
    replay_log_with_failure(bytes, id, None)
}

fn replay_log_with_failure(
    bytes: Vec<u8>,
    id: Option<[u8; 16]>,
    fail_read: Option<usize>,
) -> (Arc<Mutex<Vec<usize>>>, Log) {
    let reads = Arc::new(Mutex::new(Vec::new()));
    let log = Log {
        metadata: false,
        anchor: None,
        file: Box::new(ReplayFile {
            bytes,
            reads: Arc::clone(&reads),
            fail_read,
        }),
        log_id: id,
        witness: None,
        ordered_barrier_sync: false,
    };
    (reads, log)
}

#[test]
fn streamed_value_short_read_does_not_publish_the_record() {
    let id = [10_u8; 16];
    let mut bytes = v2_header(id).to_vec();
    let value = vec![0x5a; crate::recovery::RECOVERY_READ_BUFFER_BYTES * 2];
    bytes.extend(record::encode(record::Operation::Put, b"wide", &value).unwrap());
    let (reads, log) = replay_log_with_failure(bytes, Some(id), Some(2));
    let mut visits = 0;
    let error = log
        .visit_records(0, None, |_, _| {
            visits += 1;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(visits, 0);
    assert_eq!(reads.lock().unwrap().len(), 2);
}

#[test]
fn streamed_record_requires_a_complete_witness_boundary() {
    let id = [11_u8; 16];
    let mut bytes = v2_header(id).to_vec();
    let value = vec![0x33; crate::recovery::RECOVERY_READ_BUFFER_BYTES + 1];
    bytes.extend(record::encode(record::Operation::Put, b"wide", &value).unwrap());
    let end = bytes.len() as u64;
    let (_, log) = replay_log(bytes, Some(id));
    let error = log
        .visit_records(0, Some(end - 1), |_, _| Ok(()))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("acknowledged commit boundary"));
    log.visit_records(0, Some(end), |_, _| Ok(())).unwrap();
}

#[test]
fn streamed_record_rejects_truncated_value_and_checksum_and_bad_trailer() {
    let id = [12_u8; 16];
    let value = vec![0x55; crate::recovery::RECOVERY_READ_BUFFER_BYTES + 64];
    let mut bytes = v2_header(id).to_vec();
    bytes.extend(record::encode(record::Operation::Put, b"wide", &value).unwrap());
    for length in [bytes.len() - 20, bytes.len() - 3, bytes.len() - 1] {
        let (_, log) = replay_log(bytes[..length].to_vec(), Some(id));
        let mut visits = 0;
        assert!(
            log.visit_records(0, None, |_, _| {
                visits += 1;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(visits, 0);
    }
    *bytes.last_mut().unwrap() ^= 1;
    let (_, log) = replay_log(bytes, Some(id));
    let mut visits = 0;
    let error = log
        .visit_records(0, None, |_, _| {
            visits += 1;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("record checksum mismatch"));
    assert_eq!(visits, 0);
}

fn v2_header(id: [u8; 16]) -> [u8; 28] {
    let mut header = [0_u8; 28];
    header[..8].copy_from_slice(witness::LOG_MAGIC);
    header[8..24].copy_from_slice(&id);
    let checksum = crc32fast::hash(&header[..24]);
    header[24..].copy_from_slice(&checksum.to_le_bytes());
    header
}

fn assert_chunked(reads: &Arc<Mutex<Vec<usize>>>) {
    let reads = reads.lock().unwrap();
    assert!(!reads.is_empty());
    assert!(
        reads
            .iter()
            .all(|len| *len <= crate::recovery::RECOVERY_READ_BUFFER_BYTES),
        "put/delete replay read {reads:?}"
    );
}

#[test]
fn detached_log_checks_digest_and_records_in_one_bounded_pass() {
    use sha2::Digest;

    let id = [7_u8; 16];
    let value = vec![0x5a; crate::recovery::RECOVERY_READ_BUFFER_BYTES * 2 + 11];
    let mut bytes = v2_header(id).to_vec();
    bytes.extend(record::encode(record::Operation::Put, b"wide", &value).unwrap());
    let length = bytes.len();
    let digest = Sha256::digest(&bytes).into();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let file = ReplayFile {
        bytes,
        reads: Arc::clone(&reads),
        fail_read: None,
    };
    Log::verify_io_digest(Box::new(file), length as u64, &digest).unwrap();
    assert_chunked(&reads);
    let reads = reads.lock().unwrap();
    assert_eq!(&reads[..2], &[8, LOG_HEADER_SIZE]);
    assert_eq!(reads[2..].iter().sum::<usize>(), length);
}

#[test]
fn put_and_delete_replay_checks_a_value_larger_than_the_read_buffer() {
    let id = [7_u8; 16];
    let value = vec![0x5a_u8; crate::recovery::RECOVERY_READ_BUFFER_BYTES + 100];
    let wide_key = vec![b'w'; record::MAX_KEY];
    let mut bytes = v2_header(id).to_vec();
    bytes.extend(record::encode(record::Operation::Put, &wide_key, &value).unwrap());
    bytes.extend(record::encode(record::Operation::Put, b"next", b"ok").unwrap());
    bytes.extend(record::encode(record::Operation::Delete, &wide_key, b"").unwrap());
    let (reads, log) = replay_log(bytes, Some(id));
    let mut seen = Vec::new();
    let mut scratch = None;
    log.visit_records(0, None, |key, location| {
        let Cow::Borrowed(key) = key else {
            panic!("single-record replay should borrow its scratch buffer");
        };
        assert_eq!(key.as_ptr(), *scratch.get_or_insert(key.as_ptr()));
        seen.push((key.to_vec(), location.map(|location| location.checksum)));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![
            (wide_key.clone(), Some(crc32fast::hash(&value))),
            (b"next".to_vec(), Some(crc32fast::hash(b"ok"))),
            (wide_key, None),
        ]
    );
    assert_chunked(&reads);
}

#[test]
fn legacy_replay_also_chunks_a_value_past_the_read_buffer() {
    let value = vec![0x11_u8; crate::recovery::RECOVERY_READ_BUFFER_BYTES + 1];
    let bytes = record::encode(record::Operation::Put, b"k", &value).unwrap();
    let (reads, log) = replay_log(bytes, None);
    let mut checksum = None;
    log.visit_records(0, None, |_key, location| {
        checksum = location.map(|location| location.checksum);
        Ok(())
    })
    .unwrap();
    assert_eq!(checksum, Some(crc32fast::hash(&value)));
    assert_chunked(&reads);
}

#[test]
fn corruption_past_the_recovery_buffer_fails_before_the_record_is_published() {
    let id = [8_u8; 16];
    let value = vec![0x5a_u8; crate::recovery::RECOVERY_READ_BUFFER_BYTES + 80];
    let encoded = record::encode(record::Operation::Put, b"wide", &value).unwrap();
    let mut bytes = v2_header(id).to_vec();
    let value_at = bytes.len() + record::HEADER_SIZE + b"wide".len();
    bytes.extend(encoded);
    bytes.extend(record::encode(record::Operation::Put, b"later", b"kept").unwrap());
    bytes[value_at + crate::recovery::RECOVERY_READ_BUFFER_BYTES] ^= 1;
    let (_reads, log) = replay_log(bytes, Some(id));
    let mut visits = 0_usize;
    let error = log
        .visit_records(0, None, |_, _| {
            visits += 1;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("record checksum mismatch"),
        "{error}"
    );
    assert_eq!(visits, 0);
}

#[test]
fn an_incomplete_large_record_is_rejected_without_reading_its_value() {
    let id = [9_u8; 16];
    let mut header = [0_u8; record::HEADER_SIZE];
    header[..4].copy_from_slice(b"DB01");
    header[4] = 1;
    header[5..9].copy_from_slice(&1_u32.to_le_bytes());
    header[9..13].copy_from_slice(&(crate::record::MAX_VALUE as u32).to_le_bytes());
    let checksum = crc32fast::hash(&header[..13]);
    header[13..].copy_from_slice(&checksum.to_le_bytes());
    let mut bytes = v2_header(id).to_vec();
    bytes.extend(header);
    bytes.extend_from_slice(b"k");
    let (reads, log) = replay_log(bytes, Some(id));
    let error = log.visit_records(0, None, |_, _| Ok(())).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("incomplete log record"),
        "{error}"
    );
    assert_chunked(&reads);
}

#[test]
fn streamed_batch_matches_prepare_batch_for_valid_and_malformed_payloads() {
    let small = sample_batch(&[
        BatchOp::Put {
            key: b"child",
            value: b"value",
        },
        BatchOp::Delete { key: b"gone" },
        BatchOp::Put {
            key: b"child",
            value: b"",
        },
    ]);
    let wide_key = vec![0x11_u8; 1024];
    let crossed = sample_batch(&[
        BatchOp::Put {
            key: &wide_key,
            value: &vec![0x5a_u8; crate::recovery::RECOVERY_READ_BUFFER_BYTES],
        },
        BatchOp::Put {
            key: b"tail",
            value: &vec![0x6b_u8; crate::recovery::RECOVERY_READ_BUFFER_BYTES + 1],
        },
        BatchOp::Delete { key: b"tail" },
    ]);
    let mut patterned = Vec::with_capacity(70_000);
    patterned.extend((0..70_000).map(|index| (index % 251) as u8));
    let patterned = sample_batch(&[
        BatchOp::Put {
            key: b"pattern",
            value: &patterned,
        },
        BatchOp::Delete { key: b"other" },
    ]);
    let max_ops = indexed_deletes(record::batch::MAX_ATOMIC_BATCH_OPERATIONS);
    let mut flipped_value = small.clone();
    let value_at = 4 + record::HEADER_SIZE + b"child".len();
    flipped_value[value_at] ^= 1;
    let mut flipped_header = small.clone();
    flipped_header[4] ^= 1;
    let mut truncated = small.clone();
    truncated.truncate(small.len() / 2);
    let mut short_checksum = small.clone();
    short_checksum.truncate(small.len() - 2);
    let mut trailing = small.clone();
    trailing.push(0);
    let mut payloads = vec![
        small,
        sample_batch(&[BatchOp::Delete { key: b"only" }]),
        sample_batch(&[BatchOp::Put {
            key: b"empty",
            value: b"",
        }]),
        crossed,
        patterned,
        max_ops,
        Vec::new(),
        vec![1, 2, 3],
        0_u32.to_le_bytes().to_vec(),
        count_payload(1, 0),
        count_payload(1, 10),
        count_payload(1025, 64),
        count_payload(1025, 70_000),
        count_payload(u32::MAX, 32),
        truncated,
        short_checksum,
        trailing,
        flipped_value,
        flipped_header,
        nested_batch_payload(),
        lone_nested_header(1, 0, 0),
        lone_nested_header(2, 1, 1),
    ];
    payloads.extend((1..6).map(|cut| {
        let mut partial = sample_batch(&[BatchOp::Put {
            key: b"cut",
            value: b"abcdef",
        }]);
        partial.truncate(partial.len() - cut);
        partial
    }));

    for payload in &payloads {
        assert_scanner_matches_prepare_batch(payload);
        assert_legacy_replay_matches_prepare_batch(payload);
    }
}

#[test]
fn batch_replay_streams_a_split_child_header_and_the_next_record() {
    let id = [6_u8; 16];
    let buffer = crate::recovery::RECOVERY_READ_BUFFER_BYTES;
    let mut bytes = v2_header(id).to_vec();
    bytes.extend(record::encode(record::Operation::Put, b"early", b"1").unwrap());
    let batch_at = bytes.len();
    let payload_at = batch_at + record::HEADER_SIZE + 1;
    let first_payload_budget = buffer - payload_at;
    let second_header_at = first_payload_budget - 3;
    let value_len = second_header_at - (4 + record::HEADER_SIZE + 1 + record::CHECKSUM_SIZE);
    assert!(second_header_at + record::HEADER_SIZE > first_payload_budget);
    let wide: Vec<u8> = (0..value_len).map(|index| (index % 251) as u8).collect();
    let payload = record::batch::encode_payload(&[
        record::batch::BatchOperation::Put {
            key: b"wide",
            value: &wide,
        },
        record::batch::BatchOperation::Put {
            key: b"next",
            value: b"child",
        },
        record::batch::BatchOperation::Delete { key: b"early" },
    ])
    .unwrap();
    let batch = record::encode(record::Operation::Batch, &[0], &payload).unwrap();
    bytes.extend(batch);
    bytes.extend(record::encode(record::Operation::Put, b"tail", b"z").unwrap());
    let payload_base = payload_at as u64;
    let expected = projected_batch(&payload, payload_base).unwrap();

    let (reads, log) = replay_log(bytes, Some(id));
    let seen = visit_projected(&log).unwrap();
    assert_eq!(seen[0].0, b"early");
    assert_eq!(&seen[1..seen.len() - 1], expected.as_slice());
    let (tail_offset, tail_len, tail_checksum) = seen.last().unwrap().1.unwrap();
    assert_eq!(seen.last().unwrap().0, b"tail");
    assert_eq!((tail_len, tail_checksum), (1, crc32fast::hash(b"z")));
    let batch_len = record::HEADER_SIZE + 1 + payload.len() + record::CHECKSUM_SIZE;
    assert_eq!(
        tail_offset,
        batch_at as u64 + batch_len as u64 + record::HEADER_SIZE as u64 + b"tail".len() as u64
    );
    assert_eq!(seen[1].1.unwrap().2, crc32fast::hash(&wide));
    assert_eq!(seen[2].1.unwrap().2, crc32fast::hash(b"child"));
    assert_eq!(seen[3].1, None);
    assert_chunked(&reads);
    assert!(reads.lock().unwrap().contains(&buffer));
}

#[test]
fn batch_children_are_published_in_order_only_after_validation() {
    let payload = record::batch::encode_payload(&[
        record::batch::BatchOperation::Put {
            key: b"a",
            value: b"first",
        },
        record::batch::BatchOperation::Put {
            key: b"b",
            value: b"second",
        },
        record::batch::BatchOperation::Delete { key: b"a" },
    ])
    .unwrap();
    let batch = record::encode(record::Operation::Batch, &[0], &payload).unwrap();
    let mut bytes = batch.clone();
    bytes.extend(record::encode(record::Operation::Put, b"tail", b"z").unwrap());
    let (_, log) = replay_log(bytes.clone(), None);
    let mut seen = Vec::new();
    log.visit_records(0, None, |key, location| {
        seen.push((key.into_owned(), location.map(|location| location.checksum)));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![
            (b"a".to_vec(), Some(crc32fast::hash(b"first"))),
            (b"b".to_vec(), Some(crc32fast::hash(b"second"))),
            (b"a".to_vec(), None),
            (b"tail".to_vec(), Some(crc32fast::hash(b"z"))),
        ]
    );

    let (_, log) = replay_log(bytes, None);
    let mut invoked = Vec::new();
    let mut applied = Vec::new();
    let error = log
        .visit_records(0, None, |key, _| {
            invoked.push(key.to_vec());
            if key.as_ref() == b"b" {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "quota"));
            }
            applied.push(key.into_owned());
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(invoked, vec![b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(applied, vec![b"a".to_vec()]);

    let mut corrupt = batch;
    let value_at = corrupt
        .windows(b"second".len())
        .position(|window| window == b"second")
        .unwrap();
    corrupt[value_at] ^= 1;
    resign(&mut corrupt, true);
    corrupt.extend(record::encode(record::Operation::Put, b"tail", b"z").unwrap());
    let (_, log) = replay_log(corrupt, None);
    let mut seen = Vec::new();
    let error = log
        .visit_records(0, None, |key, _| {
            seen.push(key.into_owned());
            Ok(())
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("record checksum mismatch"),
        "{error}"
    );
    assert!(seen.is_empty(), "{seen:?}");
}

#[test]
fn batch_replay_reports_outer_checksum_before_key_and_nested_errors() {
    let id = [4_u8; 16];
    let batch = encoded_batch(&[
        BatchOp::Put {
            key: b"child",
            value: &vec![0x3c_u8; crate::recovery::RECOVERY_READ_BUFFER_BYTES + 32],
        },
        BatchOp::Delete { key: b"gone" },
    ]);
    let mut checksum_wins = batch.clone();
    checksum_wins[record::HEADER_SIZE] ^= 1;
    checksum_wins[record::HEADER_SIZE + 1 + 4] ^= 1;
    resign(&mut checksum_wins, false);
    assert_batch_stops_after_early(
        &log_around(id, &checksum_wins),
        id,
        "record checksum mismatch",
    );

    let mut key_wins = batch.clone();
    key_wins[record::HEADER_SIZE] ^= 1;
    key_wins[record::HEADER_SIZE + 1 + 4] ^= 1;
    resign(&mut key_wins, true);
    assert_batch_stops_after_early(&log_around(id, &key_wins), id, "invalid batch record key");

    let mut nested_wins = batch;
    nested_wins[record::HEADER_SIZE + 1 + 4] ^= 1;
    resign(&mut nested_wins, true);
    assert_batch_stops_after_early(&log_around(id, &nested_wins), id, "invalid record header");

    let huge = count_payload(1025, 70_000);
    let mut count_checksum = record::encode(record::Operation::Batch, &[0], &huge).unwrap();
    resign(&mut count_checksum, false);
    assert_batch_stops_after_early(
        &log_around(id, &count_checksum),
        id,
        "record checksum mismatch",
    );

    let mut count_key = record::encode(record::Operation::Batch, &[0], &huge).unwrap();
    count_key[record::HEADER_SIZE] ^= 1;
    resign(&mut count_key, true);
    assert_batch_stops_after_early(&log_around(id, &count_key), id, "invalid batch record key");

    let count = record::encode(record::Operation::Batch, &[0], &huge).unwrap();
    let (reads, log) = replay_log(log_around(id, &count), Some(id));
    let error = assert_batch_stops_after_early_on(&log, "invalid batch operation count");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_chunked(&reads);

    let empty = record::encode(record::Operation::Batch, &[0], &[]).unwrap();
    assert_batch_stops_after_early(&log_around(id, &empty), id, "incomplete batch count");
    let mut empty_checksum = empty;
    resign(&mut empty_checksum, false);
    assert_batch_stops_after_early(
        &log_around(id, &empty_checksum),
        id,
        "record checksum mismatch",
    );
}

#[test]
fn batch_short_read_reports_io_before_key_or_nested_validation() {
    let id = [5_u8; 16];
    let mut batch = encoded_batch(&[BatchOp::Put {
        key: b"child",
        value: &vec![0x11_u8; crate::recovery::RECOVERY_READ_BUFFER_BYTES * 2 + 25],
    }]);
    batch[record::HEADER_SIZE] ^= 1;
    batch[record::HEADER_SIZE + 1 + crate::recovery::RECOVERY_READ_BUFFER_BYTES] ^= 1;
    let (reads, log) = replay_log_with_failure(log_around(id, &batch), Some(id), Some(2));
    let mut seen = Vec::new();
    let error = log
        .visit_records(0, None, |key, _| {
            seen.push(key.into_owned());
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof, "{error}");
    assert_eq!(seen, vec![b"early".to_vec()]);
    assert_eq!(reads.lock().unwrap().len(), 2);

    let mut truncated = v2_header(id).to_vec();
    truncated.extend(record::encode(record::Operation::Put, b"early", b"1").unwrap());
    truncated.extend(&batch[..batch.len() - 1]);
    let (_, log) = replay_log(truncated, Some(id));
    let mut seen = Vec::new();
    let error = log
        .visit_records(0, None, |key, _| {
            seen.push(key.into_owned());
            Ok(())
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("incomplete log record"),
        "{error}"
    );
    assert_eq!(seen, vec![b"early".to_vec()]);
}

#[test]
fn batch_suffix_boundary_rejects_a_partial_record_without_publishing_children() {
    let id = [13_u8; 16];
    let payload = record::batch::encode_payload(&[
        record::batch::BatchOperation::Put {
            key: b"child",
            value: &vec![0x44_u8; crate::recovery::RECOVERY_READ_BUFFER_BYTES + 9],
        },
        record::batch::BatchOperation::Delete { key: b"gone" },
    ])
    .unwrap();
    let batch = record::encode(record::Operation::Batch, &[0], &payload).unwrap();
    let mut bytes = v2_header(id).to_vec();
    bytes.extend(record::encode(record::Operation::Put, b"early", b"1").unwrap());
    let batch_at = bytes.len() as u64;
    bytes.extend(batch);
    let end = bytes.len() as u64;

    let (_, log) = replay_log(bytes.clone(), Some(id));
    let suffix = visit_projected_from(&log, batch_at, Some(end)).unwrap();
    assert_eq!(
        suffix.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
        vec![b"child".to_vec(), b"gone".to_vec()]
    );
    let error = log
        .visit_records(batch_at, Some(end - 1), |_, _| Ok(()))
        .unwrap_err();
    assert!(
        error.to_string().contains("acknowledged commit boundary"),
        "{error}"
    );

    let mut truncated = bytes.clone();
    truncated.truncate(end as usize - 8);
    let (_, log) = replay_log(truncated, Some(id));
    let mut seen = Vec::new();
    let error = log
        .visit_records(0, None, |key, _| {
            seen.push(key.into_owned());
            Ok(())
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("incomplete log record"),
        "{error}"
    );
    assert_eq!(seen, vec![b"early".to_vec()]);

    let mut header = [0_u8; record::HEADER_SIZE];
    header[..4].copy_from_slice(b"DB02");
    header[4] = 3;
    header[5..9].copy_from_slice(&1_u32.to_le_bytes());
    header[9..13].copy_from_slice(&(crate::record::MAX_VALUE as u32).to_le_bytes());
    let checksum = crc32fast::hash(&header[..13]);
    header[13..].copy_from_slice(&checksum.to_le_bytes());
    let mut huge = v2_header(id).to_vec();
    huge.extend(header);
    huge.push(0);
    let (reads, log) = replay_log(huge, Some(id));
    let error = log.visit_records(0, None, |_, _| Ok(())).unwrap_err();
    assert!(
        error.to_string().contains("incomplete log record"),
        "{error}"
    );
    assert_chunked(&reads);
    assert!(
        reads
            .lock()
            .unwrap()
            .iter()
            .all(|len| *len < crate::record::MAX_VALUE)
    );
}

fn resign(record: &mut [u8], valid: bool) {
    let end = record.len() - 4;
    let mut sum = crc32fast::hash(&record[..end]);
    if !valid {
        sum ^= u32::MAX;
    }
    record[end..].copy_from_slice(&sum.to_le_bytes());
}

fn log_around(id: [u8; 16], batch: &[u8]) -> Vec<u8> {
    let mut bytes = v2_header(id).to_vec();
    bytes.extend(record::encode(record::Operation::Put, b"early", b"1").unwrap());
    bytes.extend_from_slice(batch);
    bytes.extend(record::encode(record::Operation::Put, b"tail", b"z").unwrap());
    bytes
}

fn assert_batch_stops_after_early(bytes: &[u8], id: [u8; 16], message: &str) {
    let (_, log) = replay_log(bytes.to_vec(), Some(id));
    assert_batch_stops_after_early_on(&log, message);
}

fn assert_batch_stops_after_early_on(log: &Log, message: &str) -> io::Error {
    let mut seen = Vec::new();
    let error = log
        .visit_records(0, None, |key, _| {
            seen.push(key.into_owned());
            Ok(())
        })
        .unwrap_err();
    assert_eq!(seen, vec![b"early".to_vec()], "{error}");
    assert!(error.to_string().contains(message), "{error}");
    error
}

enum BatchOp<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

fn sample_batch(operations: &[BatchOp<'_>]) -> Vec<u8> {
    let encoded: Vec<_> = operations
        .iter()
        .map(|operation| match operation {
            BatchOp::Put { key, value } => record::batch::BatchOperation::Put { key, value },
            BatchOp::Delete { key } => record::batch::BatchOperation::Delete { key },
        })
        .collect();
    record::batch::encode_payload(&encoded).unwrap()
}

fn encoded_batch(operations: &[BatchOp<'_>]) -> Vec<u8> {
    record::encode(record::Operation::Batch, &[0], &sample_batch(operations)).unwrap()
}

fn indexed_deletes(count: usize) -> Vec<u8> {
    let keys: Vec<[u8; 2]> = (0..count)
        .map(|index| (index as u16).to_le_bytes())
        .collect();
    let operations: Vec<_> = keys
        .iter()
        .map(|key| record::batch::BatchOperation::Delete { key: &key[..] })
        .collect();
    record::batch::encode_payload(&operations).unwrap()
}

fn count_payload(count: u32, tail: usize) -> Vec<u8> {
    let mut payload = count.to_le_bytes().to_vec();
    payload.resize(4 + tail, 0);
    payload
}

fn nested_batch_payload() -> Vec<u8> {
    let inner = record::encode(record::Operation::Batch, &[0], b"x").unwrap();
    let mut payload = 1_u32.to_le_bytes().to_vec();
    payload.extend(inner);
    payload
}

fn lone_nested_header(operation: u8, key_len: u32, value_len: u32) -> Vec<u8> {
    let mut header = [0_u8; record::HEADER_SIZE];
    header[..4].copy_from_slice(b"DB01");
    header[4] = operation;
    header[5..9].copy_from_slice(&key_len.to_le_bytes());
    header[9..13].copy_from_slice(&value_len.to_le_bytes());
    let checksum = crc32fast::hash(&header[..13]);
    header[13..].copy_from_slice(&checksum.to_le_bytes());
    let mut payload = 1_u32.to_le_bytes().to_vec();
    payload.extend_from_slice(&header);
    payload
}

type Projected = (Vec<u8>, Option<(u64, usize, u32)>);

fn project(updates: Vec<BatchUpdate>) -> Vec<Projected> {
    updates
        .into_iter()
        .map(|update| {
            (
                update.key,
                update
                    .location
                    .map(|location| (location.offset, location.length, location.checksum)),
            )
        })
        .collect()
}

fn projected_batch(payload: &[u8], base: u64) -> io::Result<Vec<Projected>> {
    prepare_batch(
        payload,
        ValueLocation {
            offset: base,
            length: payload.len(),
            checksum: 0,
        },
    )
    .map(project)
}

fn assert_scanner_matches_prepare_batch(payload: &[u8]) {
    let expected = projected_batch(payload, (record::HEADER_SIZE + 1) as u64);
    for chunk in [1_usize, 2, 3, 7, 16, 17, 31, 64, 1024, 65_536] {
        let mut scan = record::batch::stream::BatchScan::<crc32fast::Hasher, _>::new(
            (record::HEADER_SIZE + 1) as u64,
            |_| Ok(()),
        );
        if !payload.is_empty() {
            for piece in payload.chunks(chunk) {
                scan.push(piece);
            }
        }
        let actual = scan.finish().map(|updates| {
            updates
                .into_iter()
                .map(|update| {
                    (
                        update.key,
                        update
                            .location
                            .map(|value| (value.offset, value.length, value.digest)),
                    )
                })
                .collect()
        });
        assert_same_projection(&expected, actual, chunk);
    }
}

fn assert_legacy_replay_matches_prepare_batch(payload: &[u8]) {
    let record = record::encode(record::Operation::Batch, &[0], payload).unwrap();
    let expected = projected_batch(payload, (record::HEADER_SIZE + 1) as u64);
    let (reads, log) = replay_log(record, None);
    let got = visit_projected(&log);
    assert_same_projection(&expected, got, payload.len());
    if payload.len() > crate::recovery::RECOVERY_READ_BUFFER_BYTES {
        assert_chunked(&reads);
    }
}

fn visit_projected(log: &Log) -> io::Result<Vec<Projected>> {
    visit_projected_from(log, 0, None)
}

fn visit_projected_from(
    log: &Log,
    offset: u64,
    boundary: Option<u64>,
) -> io::Result<Vec<Projected>> {
    let mut seen = Vec::new();
    log.visit_records(offset, boundary, |key, location| {
        seen.push((
            key.into_owned(),
            location.map(|location| (location.offset, location.length, location.checksum)),
        ));
        Ok(())
    })
    .map(|()| seen)
}

fn assert_same_projection(
    expected: &io::Result<Vec<Projected>>,
    got: io::Result<Vec<Projected>>,
    label: usize,
) {
    match (expected, got) {
        (Ok(expected), Ok(got)) => assert_eq!(expected, &got, "chunk or length {label}"),
        (Err(expected), Err(got)) => {
            assert_eq!(expected.kind(), got.kind(), "{expected} vs {got}");
            assert_eq!(
                expected.to_string(),
                got.to_string(),
                "chunk or length {label}"
            );
        }
        (expected, got) => panic!("stream {got:?} != in-memory decoder {expected:?} ({label})"),
    }
}

// These adapters use the real fixture files; only the selected sync fails.
struct AdoptionLog(File, bool);
impl LogIo for AdoptionLog {
    fn len(&self) -> io::Result<u64> {
        LogIo::len(&self.0)
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        LogIo::read_exact_at(&self.0, bytes, offset)
    }
    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        LogIo::append_all(&self.0, bytes)
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.1 {
            Err(io::Error::other("injected adoption log sync"))
        } else {
            LogIo::sync_data(&self.0)
        }
    }
}
struct AdoptionWitness(File, bool);
impl WitnessIo for AdoptionWitness {
    fn len(&self) -> io::Result<u64> {
        WitnessIo::len(&self.0)
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        WitnessIo::read_exact_at(&self.0, bytes, offset)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        WitnessIo::write_all_at(&self.0, bytes, offset)
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.1 {
            Err(io::Error::other("injected adoption witness sync"))
        } else {
            WitnessIo::sync_data(&self.0)
        }
    }
}

#[test]
fn v2_exact_tail_adoption_sync_failures_do_not_report_open_success() {
    use crate::RecoveryOpenFailure;
    use crate::publication_outcome::track_recovery_open;

    for (fail_log, fail_witness) in [(true, false), (false, true), (false, false)] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path();
        std::fs::write(
            path.join("records.log"),
            include_bytes!("../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap();
        std::fs::write(
            path.join("commit.state"),
            include_bytes!("../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap();
        let store = crate::Store::open(path).unwrap();
        store.put(b"base", b"acknowledged").unwrap();
        let old_witness = std::fs::read(path.join("commit.state")).unwrap();
        store.put(b"tail", b"complete").unwrap();
        drop(store);
        std::fs::write(path.join("commit.state"), &old_witness).unwrap();
        let original_log = std::fs::read(path.join("records.log")).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.join("records.log"))
            .unwrap();
        let (id, metadata) = log_identity(&file).unwrap();
        let id = id.unwrap();
        let boundary = witness::decode(old_witness.as_slice().try_into().unwrap(), &id).unwrap();
        let witness_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("commit.state"))
            .unwrap();
        let witness = Witness::for_test(
            Box::new(AdoptionWitness(witness_file, fail_witness)),
            id,
            boundary,
        );
        // Mirror the open sequence: replay must reach the exact validated end,
        // and an adoption error must prevent the replayed index being returned.
        let result = track_recovery_open(|tracker| -> io::Result<()> {
            let (log, index, end) = Log::from_parts(
                Box::new(AdoptionLog(file, fail_log)),
                Some(id),
                Some(witness),
                Some(boundary),
                metadata,
                (StoreRecoveryLimits::default(), None),
                None,
            )?;
            assert_eq!(end.length, original_log.len() as u64);
            assert!(boundary < end.length);
            assert!(index.contains_key(&b"base"[..]));
            assert!(index.contains_key(&b"tail"[..]));
            log.adopt_replayed_tail(Some(boundary), end, || Ok(()), Some(tracker))?;
            Err(io::Error::other("injected open error after adoption"))
        });
        match (fail_log, fail_witness, result.unwrap_err()) {
            (true, false, RecoveryOpenFailure::NotAdopted(error)) => {
                assert_eq!(error.to_string(), "injected adoption log sync");
            }
            (false, true, RecoveryOpenFailure::TailAdoptionOutcomeUnknown(error)) => {
                assert_eq!(error.to_string(), "injected adoption witness sync");
            }
            (false, false, RecoveryOpenFailure::TailAdopted(error)) => {
                assert_eq!(error.to_string(), "injected open error after adoption");
            }
            (_, _, failure) => panic!("unexpected recovery failure: {failure}"),
        }
        assert_eq!(
            std::fs::read(path.join("records.log")).unwrap(),
            original_log
        );
        let after = std::fs::read(path.join("commit.state")).unwrap();
        if fail_log {
            assert_eq!(after, old_witness);
        } else {
            // The update is visible even in the injected witness-sync failure case.
            assert_eq!(
                witness::decode(after.as_slice().try_into().unwrap(), &id).unwrap(),
                original_log.len() as u64
            );
        }
        let reopened = crate::Store::open(path).unwrap();
        assert_eq!(
            reopened.get(b"base").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        assert_eq!(
            reopened.get(b"tail").unwrap().as_deref(),
            Some(&b"complete"[..])
        );
    }
}

#[test]
fn prefix_reconstruction_requires_attached_witness() {
    let id = [10_u8; 16];
    let (_, log) = replay_log(v2_header(id).to_vec(), Some(id));
    let limits = PeakVerificationLimits {
        max_log_bytes: u64::MAX,
        max_live_keys: usize::MAX,
        max_live_key_bytes: usize::MAX,
    };
    assert!(
        log.reconstruct_prefix_index_peaks(&BTreeMap::new(), LOG_HEADER_SIZE as u64, limits)
            .is_err()
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BarrierFault {
    None,
    LogBarrier,
    WitnessRead,
    WitnessWriteBefore,
    WitnessWriteAfter,
    WitnessFullSync,
}

struct BarrierShared {
    events: Mutex<Vec<&'static str>>,
    witness: Mutex<Vec<u8>>,
    supports_barrier: Result<bool, &'static str>,
    log_dev: Result<Option<u64>, &'static str>,
    witness_dev: Result<Option<u64>, &'static str>,
    fault: BarrierFault,
}

struct BarrierLogHandle(Arc<BarrierShared>);
struct BarrierWitnessHandle(Arc<BarrierShared>);

impl LogIo for BarrierLogHandle {
    fn len(&self) -> io::Result<u64> {
        Ok(128)
    }
    fn read_exact_at(&self, _buffer: &mut [u8], _offset: u64) -> io::Result<()> {
        unreachable!()
    }
    fn append_all(&self, _bytes: &[u8]) -> io::Result<()> {
        unreachable!()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.events.lock().unwrap().push("log_sync_full");
        Ok(())
    }
    fn supports_ordered_witness_barrier(&self) -> io::Result<bool> {
        self.0.supports_barrier.map_err(io::Error::other)
    }
    fn sync_ordered_before_witness(&self) -> io::Result<()> {
        self.0.events.lock().unwrap().push("log_sync_barrier");
        if self.0.fault == BarrierFault::LogBarrier {
            return Err(io::Error::other("injected barrier sync failure"));
        }
        Ok(())
    }
    fn file_identity(&self) -> io::Result<Option<(u64, u64)>> {
        self.0
            .log_dev
            .map(|dev| dev.map(|id| (id, 1)))
            .map_err(io::Error::other)
    }
}

impl WitnessIo for BarrierWitnessHandle {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.witness.lock().unwrap().len() as u64)
    }
    fn read_exact_at(&self, bytes: &mut [u8], _offset: u64) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_read");
        if self.0.fault == BarrierFault::WitnessRead {
            return Err(io::Error::other("injected witness read failure"));
        }
        bytes.copy_from_slice(&self.0.witness.lock().unwrap());
        Ok(())
    }
    fn write_all_at(&self, bytes: &[u8], _offset: u64) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_write");
        if self.0.fault == BarrierFault::WitnessWriteBefore {
            return Err(io::Error::other("injected witness write failure"));
        }
        self.0.witness.lock().unwrap().copy_from_slice(bytes);
        if self.0.fault == BarrierFault::WitnessWriteAfter {
            return Err(io::Error::other("injected witness write outcome unknown"));
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_sync_data");
        Ok(())
    }
    fn sync_full(&self) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_sync_full");
        if self.0.fault == BarrierFault::WitnessFullSync {
            return Err(io::Error::other("injected witness fullfsync failure"));
        }
        Ok(())
    }
    fn device_id(&self) -> io::Result<Option<u64>> {
        self.0.witness_dev.map_err(io::Error::other)
    }
}

#[test]
fn ordered_barrier_sync_uses_barrier_then_full_witness_sync_and_propagates_faults() {
    use crate::publication_outcome::{ManagedRebuildFailure, track_managed_rebuild};

    let id = [9_u8; 16];
    for (fault, expected_events, expected_offset, unknown_tail_adoption) in [
        (
            BarrierFault::None,
            &[
                "log_sync_barrier",
                "witness_read",
                "witness_write",
                "witness_sync_full",
            ][..],
            128_u64,
            false,
        ),
        (
            BarrierFault::LogBarrier,
            &["log_sync_barrier"][..],
            29_u64,
            false,
        ),
        (
            BarrierFault::WitnessRead,
            &["log_sync_barrier", "witness_read"][..],
            29_u64,
            false,
        ),
        (
            BarrierFault::WitnessWriteBefore,
            &["log_sync_barrier", "witness_read", "witness_write"][..],
            29_u64,
            true,
        ),
        (
            BarrierFault::WitnessWriteAfter,
            &["log_sync_barrier", "witness_read", "witness_write"][..],
            128_u64,
            true,
        ),
        (
            BarrierFault::WitnessFullSync,
            &[
                "log_sync_barrier",
                "witness_read",
                "witness_write",
                "witness_sync_full",
            ][..],
            128_u64,
            true,
        ),
    ] {
        for tracked in [false, true] {
            let shared = Arc::new(BarrierShared {
                events: Mutex::new(Vec::new()),
                witness: Mutex::new(witness::encode_for_test(&id, 29)),
                supports_barrier: Ok(true),
                log_dev: Ok(Some(42)),
                witness_dev: Ok(Some(42)),
                fault,
            });
            let witness =
                Witness::for_test(Box::new(BarrierWitnessHandle(Arc::clone(&shared))), id, 29);
            let file: Box<dyn LogIo> = Box::new(BarrierLogHandle(Arc::clone(&shared)));
            let ordered = compute_ordered_barrier_sync(file.as_ref(), false, Some(&witness));
            assert!(ordered);
            let log = Log {
                file,
                log_id: Some(id),
                witness: Some(witness),
                metadata: false,
                anchor: None,
                ordered_barrier_sync: ordered,
            };
            assert!(log.uses_ordered_barrier_sync());
            if tracked {
                let result = track_managed_rebuild(|tracker| {
                    log.sync_validated_boundary_tracked(128, Some(&tracker.tail_adoption))
                });
                match fault {
                    BarrierFault::None => assert!(result.is_ok()),
                    _ if unknown_tail_adoption => assert!(matches!(
                        result,
                        Err(ManagedRebuildFailure::TailAdoptionOutcomeUnknown(_))
                    )),
                    _ => assert!(matches!(
                        result,
                        Err(ManagedRebuildFailure::NotPublished(_))
                    )),
                }
            } else {
                let result = log.sync_data();
                assert_eq!(result.is_ok(), fault == BarrierFault::None);
            }
            assert_eq!(&*shared.events.lock().unwrap(), expected_events);
            let stored = witness::decode(
                shared
                    .witness
                    .lock()
                    .unwrap()
                    .as_slice()
                    .try_into()
                    .unwrap(),
                &id,
            )
            .unwrap();
            assert_eq!(stored, expected_offset);
        }
    }
}

#[test]
fn ordered_barrier_sync_is_disabled_for_cross_device_missing_dev_legacy_and_v3() {
    let id = [11_u8; 16];
    let fallback_v2 = &[
        "log_sync_full",
        "witness_read",
        "witness_write",
        "witness_sync_data",
    ][..];
    for (name, metadata, has_witness, supports_barrier, log_dev, witness_dev, expected_events) in [
        (
            "cross-device-v2",
            false,
            true,
            Ok(true),
            Ok(Some(1)),
            Ok(Some(2)),
            fallback_v2,
        ),
        (
            "unknown-log-dev-v2",
            false,
            true,
            Ok(true),
            Ok(None),
            Ok(Some(2)),
            fallback_v2,
        ),
        (
            "unknown-witness-dev-v2",
            false,
            true,
            Ok(true),
            Ok(Some(1)),
            Ok(None),
            fallback_v2,
        ),
        (
            "unsupported-volume-v2",
            false,
            true,
            Ok(false),
            Ok(Some(1)),
            Ok(Some(1)),
            fallback_v2,
        ),
        (
            "capability-probe-error-v2",
            false,
            true,
            Err("probe error"),
            Ok(Some(1)),
            Ok(Some(1)),
            fallback_v2,
        ),
        (
            "log-dev-error-v2",
            false,
            true,
            Ok(true),
            Err("log stat error"),
            Ok(Some(1)),
            fallback_v2,
        ),
        (
            "witness-dev-error-v2",
            false,
            true,
            Ok(true),
            Ok(Some(1)),
            Err("witness stat error"),
            fallback_v2,
        ),
        (
            "legacy-unwitnessed",
            false,
            false,
            Ok(true),
            Ok(Some(1)),
            Ok(Some(1)),
            &["log_sync_full"][..],
        ),
        (
            "v3-metadata",
            true,
            true,
            Ok(true),
            Ok(Some(1)),
            Ok(Some(1)),
            &["witness_read", "witness_write", "witness_sync_data"][..],
        ),
    ] {
        let shared = Arc::new(BarrierShared {
            events: Mutex::new(Vec::new()),
            witness: Mutex::new(witness::encode_for_test(&id, 29)),
            supports_barrier,
            log_dev,
            witness_dev,
            fault: BarrierFault::None,
        });
        let witness = has_witness.then(|| {
            Witness::for_test(Box::new(BarrierWitnessHandle(Arc::clone(&shared))), id, 29)
        });
        let file: Box<dyn LogIo> = Box::new(BarrierLogHandle(Arc::clone(&shared)));
        let ordered = compute_ordered_barrier_sync(file.as_ref(), metadata, witness.as_ref());
        assert!(!ordered, "{name} must not enable ordered_barrier_sync");
        let log = Log {
            file,
            log_id: has_witness.then_some(id),
            witness,
            metadata,
            anchor: None,
            ordered_barrier_sync: ordered,
        };
        assert!(!log.uses_ordered_barrier_sync(), "{name}");
        log.sync_data().unwrap();
        assert_eq!(&*shared.events.lock().unwrap(), expected_events, "{name}");
    }
}

/// Whether this volume reports barrier-ordered sync for files at `path`.
/// Hosted macOS runners can report local APFS without the capability, so
/// tests compare the selected path with the probe instead of assuming it.
#[cfg(target_os = "macos")]
fn volume_supports_barrier(path: &std::path::Path) -> bool {
    let file = std::fs::File::open(path.join("records.log")).unwrap();
    crate::log_io::macos_sync::supports_barrier_fsync(&file).unwrap()
}

#[cfg(target_os = "macos")]
#[test]
fn macos_new_store_creates_v2_with_ordered_barrier_sync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let store = crate::Store::open(&path).unwrap();
    assert_eq!(
        store.uses_ordered_barrier_sync_for_test(),
        volume_supports_barrier(&path)
    );
    store.put(b"k1", b"v1").unwrap();
    let len_after_k1 = std::fs::metadata(path.join("records.log")).unwrap().len();
    store.put(b"k2", b"v2").unwrap();
    let checkpoint_path = dir.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint_path).unwrap();
    drop(store);

    let raw_log = std::fs::read(path.join("records.log")).unwrap();
    assert!(raw_log.starts_with(b"EMDALOG2"));
    assert!(!raw_log.starts_with(b"EMDALOG3"));
    let witness_bytes = std::fs::read(path.join("commit.state")).unwrap();
    assert_eq!(witness_bytes.len(), witness::WITNESS_SIZE);

    let checkpoint_store = crate::CheckpointStore::open(&path, &checkpoint_path, &digest).unwrap();
    assert_eq!(
        checkpoint_store.uses_ordered_barrier_sync_for_test(),
        volume_supports_barrier(&path)
    );
    checkpoint_store.put(b"k3", b"v3").unwrap();
    drop(checkpoint_store);

    let reopened = crate::Store::open(&path).unwrap();
    assert_eq!(
        reopened.uses_ordered_barrier_sync_for_test(),
        volume_supports_barrier(&path)
    );
    assert_eq!(reopened.get(b"k1").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(reopened.get(b"k2").unwrap().as_deref(), Some(&b"v2"[..]));
    assert_eq!(reopened.get(b"k3").unwrap().as_deref(), Some(&b"v3"[..]));
    drop(reopened);

    // Truncating records.log back to an earlier record boundary must fail closed.
    let witness_before = std::fs::read(path.join("commit.state")).unwrap();
    let raw_after_k3 = std::fs::read(path.join("records.log")).unwrap();
    std::fs::write(
        path.join("records.log"),
        &raw_after_k3[..len_after_k1 as usize],
    )
    .unwrap();
    let err = match crate::Store::open(&path) {
        Ok(_) => panic!("truncated V2 log must be rejected"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        std::fs::read(path.join("commit.state")).unwrap(),
        witness_before
    );

    // V3 on macOS remains intentionally unsupported (never silently opened or downgraded).
    let v3_dir = dir.path().join("v3-unsupported");
    std::fs::create_dir(&v3_dir).unwrap();
    let v3_log = include_bytes!("../tests/fixtures/v3-mixed-records.log");
    let v3_anchor = include_bytes!("../tests/fixtures/v3-mixed-commit.state");
    std::fs::write(v3_dir.join("records.log"), v3_log).unwrap();
    std::fs::write(v3_dir.join("commit.state"), v3_anchor).unwrap();
    let err = match crate::Store::open(&v3_dir) {
        Ok(_) => panic!("V3 store must remain unsupported on macOS"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    assert_eq!(std::fs::read(v3_dir.join("records.log")).unwrap(), v3_log);
    assert_eq!(
        std::fs::read(v3_dir.join("commit.state")).unwrap(),
        v3_anchor
    );
}

#[cfg(target_os = "macos")]
#[test]
fn macos_v2_fixture_uses_ordered_barrier_preserves_format_and_detects_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    std::fs::write(
        path.join("records.log"),
        include_bytes!("../tests/fixtures/v2-empty-records.log"),
    )
    .unwrap();
    std::fs::write(
        path.join("commit.state"),
        include_bytes!("../tests/fixtures/v2-empty-commit.state"),
    )
    .unwrap();
    let (log, _) = Log::open(path, false).unwrap();
    assert!(!log.metadata);
    assert_eq!(
        log.uses_ordered_barrier_sync(),
        volume_supports_barrier(path)
    );
    drop(log);

    let store = crate::Store::open(path).unwrap();
    assert_eq!(
        store.uses_ordered_barrier_sync_for_test(),
        volume_supports_barrier(path)
    );
    store.put(b"k1", b"v1").unwrap();
    let len_after_k1 = std::fs::metadata(path.join("records.log")).unwrap().len();
    store.put(b"k2", b"v2").unwrap();
    drop(store);

    let raw_log = std::fs::read(path.join("records.log")).unwrap();
    assert!(raw_log.starts_with(b"EMDALOG2"));
    let reopened = crate::Store::open(path).unwrap();
    assert_eq!(reopened.get(b"k1").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(reopened.get(b"k2").unwrap().as_deref(), Some(&b"v2"[..]));
    drop(reopened);

    // Truncating records.log back to an exact prior record boundary must still
    // be detected and rejected without modifying either file.
    let witness_before = std::fs::read(path.join("commit.state")).unwrap();
    std::fs::write(path.join("records.log"), &raw_log[..len_after_k1 as usize]).unwrap();
    let err = match crate::Store::open(path) {
        Ok(_) => panic!("truncated V2 log must be rejected"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        std::fs::read(path.join("records.log")).unwrap(),
        &raw_log[..len_after_k1 as usize]
    );
    assert_eq!(
        std::fs::read(path.join("commit.state")).unwrap(),
        witness_before
    );
}
