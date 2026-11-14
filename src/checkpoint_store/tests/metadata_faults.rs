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

#[test]
fn detailed_document_mutations_report_unknown_for_v3_witness_errors() {
    use crate::document::detailed_fault_model::{
        OPERATIONS, RecoveryExpectation, apply, prepare, verify_recovery,
    };
    use crate::{DocumentDatabase, TransactionFailure};
    for operation in 0..OPERATIONS {
        for (index, fault) in [
            Fault::WriteBefore,
            Fault::WriteAfter,
            Fault::SyncBefore,
            Fault::SyncAfter,
        ]
        .into_iter()
        .enumerate()
        {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            let checkpoint = root.path().join("checkpoint");
            let ordinary = Store::open(&path).unwrap();
            let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
            drop(ordinary);
            let mut store =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            prepare(&DocumentDatabase::with_checkpoint_store(&store));
            let header = fs::read(path.join("records.log")).unwrap();
            let id = crate::metadata_witness::parse_header(
                header[..witness::LOG_HEADER_SIZE].try_into().unwrap(),
            )
            .unwrap();
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
            let db = DocumentDatabase::with_checkpoint_store(&store);
            let result = apply(&db, operation);
            assert!(
                matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
                "operation={operation}, fault={index}: {result:?}"
            );
            assert!(store.get(b"health-check").is_err());
            drop(store);
            verify_recovery(
                &path,
                &checkpoint,
                &digest,
                operation,
                RecoveryExpectation::Committed(true),
                index % 2 == 0,
            );
        }
    }
}

#[test]
fn detailed_transactions_treat_v3_witness_failures_as_unknown_outcomes() {
    for fault in [
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncAfter,
    ] {
        for document in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            let checkpoint = root.path().join("checkpoint");
            let ordinary = Store::open(&path).unwrap();
            ordinary.put(b"base", b"acknowledged").unwrap();
            let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
            drop(ordinary);
            let mut store =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let header = fs::read(path.join("records.log")).unwrap();
            let id = crate::metadata_witness::parse_header(
                header[..witness::LOG_HEADER_SIZE].try_into().unwrap(),
            )
            .unwrap();
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
            let error = if document {
                crate::DocumentDatabase::with_checkpoint_store(&store)
                    .transaction_detailed(
                        crate::DocumentTransactionMode::Conservative,
                        None,
                        None,
                        |tx| tx.put_raw(b"unknown", b"value"),
                    )
                    .unwrap_err()
            } else {
                store
                    .transaction_detailed(
                        crate::CheckpointTransactionMode::Conservative,
                        None,
                        None,
                        |tx| tx.put(b"unknown", b"value"),
                    )
                    .unwrap_err()
            };
            assert!(matches!(
                error,
                crate::TransactionFailure::OutcomeUnknown(_)
            ));
            assert!(store.get(b"base").is_err());
            drop(store);
            let reopened = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                reopened.get(b"base").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            assert_eq!(
                reopened.get(b"unknown").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        }
    }
}

#[test]
fn detailed_direct_mutations_treat_v3_witness_failures_as_unknown() {
    for operation in 0..6 {
        for fault in [
            Fault::WriteBefore,
            Fault::WriteAfter,
            Fault::SyncBefore,
            Fault::SyncAfter,
        ] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            let checkpoint = root.path().join("checkpoint");
            let ordinary = Store::open(&path).unwrap();
            ordinary.put(b"base", b"acknowledged").unwrap();
            ordinary.put(b"target", &0_i64.to_le_bytes()).unwrap();
            let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
            drop(ordinary);
            let mut store =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            let header = fs::read(path.join("records.log")).unwrap();
            let id = crate::metadata_witness::parse_header(
                header[..witness::LOG_HEADER_SIZE].try_into().unwrap(),
            )
            .unwrap();
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
            let result = match operation {
                0 => store.put_detailed(b"target", b"new"),
                1 => store.delete_detailed(b"target"),
                2 => store
                    .compare_exchange_detailed(b"target", Some(&0_i64.to_le_bytes()), Some(b"new"))
                    .map(|changed| assert!(changed)),
                3 => store
                    .increment_i64_detailed(b"target", 1)
                    .map(|value| assert_eq!(value, 1)),
                4 => store.write_batch_detailed(&[
                    crate::BatchOperation::Put {
                        key: b"target",
                        value: b"new",
                    },
                    crate::BatchOperation::Put {
                        key: b"peer",
                        value: b"new",
                    },
                ]),
                _ => store
                    .compare_exchange_batch_detailed(
                        &[(b"target", Some(&0_i64.to_le_bytes()))],
                        &[
                            crate::BatchOperation::Put {
                                key: b"target",
                                value: b"new",
                            },
                            crate::BatchOperation::Put {
                                key: b"peer",
                                value: b"new",
                            },
                        ],
                    )
                    .map(|changed| assert!(changed)),
            };
            assert!(matches!(
                result,
                Err(crate::MutationFailure::OutcomeUnknown(_))
            ));
            assert!(store.get(b"base").is_err());
            drop(store);
            let recovered = retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                recovered.get(b"base").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            let expected = match operation {
                1 => None,
                3 => Some(1_i64.to_le_bytes().to_vec()),
                _ => Some(b"new".to_vec()),
            };
            assert_eq!(recovered.get(b"target").unwrap(), expected);
            assert_eq!(
                recovered.get(b"peer").unwrap(),
                (operation >= 4).then(|| b"new".to_vec())
            );
        }
    }
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
fn checkpoint_drop_cursor_and_raw_receipt_survive_v3_metadata_faults_together() {
    use crate::DocumentDatabase;
    use crate::store::collection_drop_fault_tests::{
        prepare, step, step_final, step_second, verify_checkpoint_first, verify_final,
        verify_second,
    };
    for phase in 0..3 {
        for fault in [
            Fault::WriteBefore,
            Fault::WriteAfter,
            Fault::WritePanic,
            Fault::SyncBefore,
            Fault::SyncAfter,
            Fault::SyncPanic,
        ] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("store");
            let checkpoint = root.path().join("checkpoint");
            let store = Store::open(&path).unwrap();
            prepare(&DocumentDatabase::new(&store));
            let digest = store.checkpoint_to(&checkpoint).unwrap();
            drop(store);
            let mut store =
                retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
            if phase == 1 {
                step(&DocumentDatabase::with_checkpoint_store(&store)).unwrap();
            } else if phase == 2 {
                let db = DocumentDatabase::with_checkpoint_store(&store);
                for _ in 0..6 {
                    let progress = db.advance_collection_drop("c", 1).unwrap();
                    assert_eq!((progress.removed, progress.complete), (1, false));
                }
            }
            let header = fs::read(path.join("records.log")).unwrap();
            assert_eq!(&header[..8], b"EMDALOG3");
            let id = crate::metadata_witness::parse_header(
                header[..witness::LOG_HEADER_SIZE].try_into().unwrap(),
            )
            .unwrap();
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
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let db = DocumentDatabase::with_checkpoint_store(&store);
                match phase {
                    0 => step(&db),
                    1 => step_second(&db),
                    _ => step_final(&db),
                }
            }));
            assert!(!matches!(result, Ok(Ok(()))));
            drop(store);
            let reopened = retry_lock(|| Store::open(&path)).unwrap();
            match phase {
                0 => verify_checkpoint_first(&reopened),
                1 => verify_second(&reopened),
                _ => verify_final(&reopened),
            }
        }
    }
}

#[test]
fn metadata_write_sync_failures_and_panics_fail_closed_for_mutations() {
    for fault in [
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::WritePanic,
        Fault::SyncBefore,
        Fault::SyncAfter,
        Fault::SyncPanic,
    ] {
        for mode in 0..4 {
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
                if mode == 1 {
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
                } else if mode == 0 {
                    store.put(b"left", b"one")
                } else if mode == 2 {
                    store.transaction(|tx| {
                        tx.put(b"left", b"one")?;
                        tx.put(b"right", b"two")
                    })
                } else {
                    store.transaction_with_read_set(|tx| {
                        assert_eq!(tx.get(b"base")?.as_deref(), Some(&b"acknowledged"[..]));
                        tx.put(b"left", b"one")?;
                        tx.put(b"right", b"two")
                    })
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
                if mode != 0 { Some(&b"two"[..]) } else { None }
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
                    OpenLimits {
                        max_tail_bytes: MAX_RECOVERED_VIEW_TAIL_BYTES,
                        max_tail_keys: MAX_RECOVERED_VIEW_TAIL_KEYS,
                        max_tail_key_bytes: MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
                        max_log_bytes: None,
                        ..Default::default()
                    },
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
        let recovered = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
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
