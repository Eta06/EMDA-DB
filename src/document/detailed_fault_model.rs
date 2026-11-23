//! Shared document and unique-index checks for detailed mutation faults.
use crate::{
    BulkOperation, CollectionSchema, DocumentDatabase, DocumentUpdate, FieldRule, FieldType,
    IndexDefinition, IndexQuery, IndexValue, TransactionFailure,
};
use serde_json::json;
use std::path::Path;

pub(crate) const OPERATIONS: usize = 5;

pub(crate) fn prepare(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "x".into(),
            fields: vec![vec!["x".into()]],
            unique: true,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "a", &json!({"x":1})).unwrap();
}

pub(crate) fn apply(db: &DocumentDatabase<'_>, operation: usize) -> Result<(), TransactionFailure> {
    match operation {
        0 => db.insert_detailed("c", "b", &json!({"x":2})),
        1 => db.replace_detailed("c", "a", &json!({"x":2}), false),
        2 => db
            .update_if_detailed(
                "c",
                "a",
                &[],
                &[DocumentUpdate::Set {
                    path: vec!["x".into()],
                    value: json!(2),
                }],
                false,
            )
            .map(|changed| assert!(changed)),
        3 => db.delete_detailed("c", "a").map(|deleted| assert!(deleted)),
        4 => db
            .bulk_detailed(&[
                BulkOperation::Replace {
                    collection: "c",
                    id: "a",
                    document: &json!({"x":2}),
                    upsert: false,
                },
                BulkOperation::Insert {
                    collection: "c",
                    id: "b",
                    document: &json!({"x":3}),
                },
            ])
            .map(|results| assert_eq!(results.len(), 2)),
        _ => unreachable!(),
    }
}

pub(crate) fn verify(db: &DocumentDatabase<'_>, operation: usize, committed: bool) {
    let expected = if committed {
        match operation {
            0 => [Some(1), Some(2)],
            1 | 2 => [Some(2), None],
            3 => [None, None],
            4 => [Some(2), Some(3)],
            _ => unreachable!(),
        }
    } else {
        [Some(1), None]
    };
    for (id, value) in ["a", "b"].iter().zip(expected) {
        assert_eq!(db.get("c", id).unwrap(), value.map(|x| json!({"x":x})));
    }
    db.verify_collection("c").unwrap();
    for value in 1..=3 {
        let documents = db
            .query_index(
                "c",
                "x",
                &IndexQuery {
                    equal: vec![IndexValue::Integer(value)],
                    ..Default::default()
                },
            )
            .unwrap()
            .documents;
        let expected_id = expected.iter().position(|entry| *entry == Some(value));
        assert_eq!(documents.len(), usize::from(expected_id.is_some()));
        if let Some(index) = expected_id {
            assert_eq!(documents[0].id, ["a", "b"][index]);
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RecoveryExpectation {
    Committed(bool),
    #[cfg(target_os = "linux")]
    Either,
    Torn,
}

const RESTART_RECEIPT: &[u8] = b"restart-receipt";

fn restart_index() -> IndexDefinition {
    IndexDefinition {
        name: "guarded".into(),
        fields: vec![vec!["x".into()]],
        unique: true,
        partial: None,
    }
}

fn restart_schema() -> CollectionSchema {
    CollectionSchema {
        fields: vec![FieldRule {
            path: vec!["x".into()],
            field_type: FieldType::Integer,
            required: true,
            nullable: false,
        }],
    }
}

pub(crate) fn prepare_guarded_restart(db: &DocumentDatabase<'_>) {
    prepare(db);
    db.insert("c", "b", &json!({"x":2})).unwrap();
    db.start_index_build("c", restart_index()).unwrap();
    assert_eq!(
        db.advance_index_build("c", "guarded", 1).unwrap().processed,
        1
    );
    db.start_schema_migration("c", restart_schema()).unwrap();
    assert_eq!(db.advance_schema_migration("c", 1).unwrap().processed, 1);
}

pub(crate) fn guarded_restart(
    db: &DocumentDatabase<'_>,
    key: &crate::MaintenanceKey,
) -> Result<(), TransactionFailure> {
    db.transaction_detailed(crate::DocumentTransactionMode::ReadSet, None, None, |tx| {
        tx.restart_index_build_guarded("c", &restart_index(), key)?;
        tx.restart_schema_migration_guarded("c", &restart_schema(), key)?;
        tx.put_raw(RESTART_RECEIPT, b"guarded")
    })
}

pub(crate) fn guarded_restart_torn_cuts(
    db: &DocumentDatabase<'_>,
    path: &Path,
    key: &crate::MaintenanceKey,
) -> [usize; 3] {
    use crate::record::{CHECKSUM_SIZE, HEADER_SIZE, Header, Operation, batch};

    prepare_guarded_restart(db);
    let log = path.join("records.log");
    let before = std::fs::metadata(&log).unwrap().len() as usize;
    guarded_restart(db, key).unwrap();
    let log = std::fs::read(log).unwrap();
    let appended = &log[before..];
    let header = Header::parse(appended[..HEADER_SIZE].try_into().unwrap()).unwrap();
    assert!(header.operation == Operation::Batch);
    assert_eq!(header.record_len() as usize, appended.len());
    let payload_at = HEADER_SIZE + header.key_len;
    let entries =
        batch::decode_payload(&appended[payload_at..payload_at + header.value_len]).unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries[0].key.starts_with(b"\0emda-document"));
    assert_eq!(entries[1].key, RESTART_RECEIPT);
    let metadata_end = payload_at + entries[0].value_offset + entries[0].value_len + CHECKSUM_SIZE;
    [1, metadata_end, appended.len() - 1]
}

#[cfg(target_os = "linux")]
pub(crate) fn attach_v3_restart_fault(
    log: &mut crate::log::Log,
    path: &Path,
    fault: crate::test_support::v3_metadata::Fault,
) {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join("records.log"))
        .unwrap();
    let witness = crate::witness::Witness::for_test(
        Box::new(crate::test_support::v3_metadata::Metadata { file, fault }),
        log.id().unwrap(),
        log.len().unwrap(),
    );
    log.attach_test_witness(witness);
}

#[cfg(target_os = "linux")]
pub(crate) fn guarded_restart_v3_faults(
    checkpoint_first: bool,
    run: impl Fn(
        &Path,
        &Path,
        &[u8; 32],
        &crate::MaintenanceKey,
        crate::test_support::v3_metadata::Fault,
    ) -> Result<(), TransactionFailure>,
) {
    use crate::test_support::v3_metadata::Fault;
    use std::fs;

    let key = crate::MaintenanceKey::generate().unwrap();
    for fault in [
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::SyncBefore,
        Fault::SyncAfter,
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = crate::Store::open(&path).unwrap();
        let anchor_path = path.join("commit.state");
        let anchor = fs::read(&anchor_path).unwrap();
        assert!(
            fs::read(path.join("records.log"))
                .unwrap()
                .starts_with(b"EMDALOG3")
        );
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        drop(store);

        let result = run(&path, &checkpoint, &digest, &key, fault);
        assert!(
            matches!(result, Err(TransactionFailure::OutcomeUnknown(_))),
            "{fault:?}: {result:?}"
        );
        verify_guarded_restart_recovery(
            &path,
            &checkpoint,
            &digest,
            &key,
            RecoveryExpectation::Committed(true),
            checkpoint_first,
        );
        assert_eq!(fs::read(&anchor_path).unwrap(), anchor);
        println!("V3 {fault:?}: recovered both guarded jobs and receipt");
    }
}

pub(crate) fn verify_guarded_restart_recovery(
    path: &Path,
    checkpoint: &Path,
    digest: &[u8; 32],
    key: &crate::MaintenanceKey,
    expected: RecoveryExpectation,
    checkpoint_first: bool,
) {
    use crate::{CheckpointStore, IndexJobStatus, Store, test_support::retry_lock};
    use std::{fs, io};

    let log = path.join("records.log");
    let witness = path.join("commit.state");
    let before_log = fs::read(&log).unwrap();
    let before_witness = fs::read(&witness).unwrap();
    let check = |db: &DocumentDatabase<'_>| {
        let RecoveryExpectation::Committed(committed) = expected else {
            panic!("torn guarded restart reopened");
        };
        let receipt = db.transaction(|tx| tx.get_raw(RESTART_RECEIPT)).unwrap();
        assert_eq!(
            receipt.as_deref(),
            committed.then_some(b"guarded".as_slice())
        );
        assert_eq!(
            db.index_jobs("c").unwrap(),
            vec![IndexJobStatus::Building {
                definition: restart_index(),
                after: (!committed).then(|| "a".to_owned()),
            }]
        );
        let migration = db.schema_migration("c").unwrap().unwrap();
        assert_eq!(migration.schema, restart_schema());
        assert_eq!(migration.after, (!committed).then(|| "a".to_owned()));
        verify(db, 0, true);
        if committed {
            assert_eq!(
                db.advance_index_build("c", "guarded", 1)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::PermissionDenied
            );
            assert_eq!(
                db.advance_schema_migration("c", 1).unwrap_err().kind(),
                io::ErrorKind::PermissionDenied
            );
        }
        // Verify the recovered seal and progress without changing the fixture
        // before the other recovery path opens it.
        let result = db.transaction::<()>(|tx| {
            let advance_index = |tx: &mut crate::DocumentTransaction<'_, '_>| {
                if committed {
                    tx.advance_index_build_guarded("c", "guarded", 1, key)
                } else {
                    tx.advance_index_build("c", "guarded", 1)
                }
            };
            let advance_schema = |tx: &mut crate::DocumentTransaction<'_, '_>| {
                if committed {
                    tx.advance_schema_migration_guarded("c", 1, key)
                } else {
                    tx.advance_schema_migration("c", 1)
                }
            };
            for _ in 0..if committed { 2 } else { 1 } {
                let index_step = advance_index(tx)?;
                let schema_step = advance_schema(tx)?;
                assert_eq!((index_step.processed, index_step.complete), (1, false));
                assert_eq!((schema_step.processed, schema_step.complete), (1, false));
            }
            assert!(advance_index(tx)?.complete);
            assert!(advance_schema(tx)?.complete);
            assert!(tx.schema_migration("c")?.is_none());
            assert_eq!(
                tx.insert("c", "invalid", &json!({})).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            let integrity = tx.verify_collection("c")?;
            assert_eq!((integrity.documents, integrity.index_keys), (2, 8));
            Err(io::Error::other("discard recovery probe"))
        });
        assert_eq!(result.unwrap_err().to_string(), "discard recovery probe");
    };
    for checkpoint_mode in [checkpoint_first, !checkpoint_first] {
        let result = if checkpoint_mode {
            retry_lock(|| CheckpointStore::open(path, checkpoint, digest))
                .map(|store| check(&DocumentDatabase::with_checkpoint_store(&store)))
        } else {
            retry_lock(|| Store::open(path)).map(|store| check(&DocumentDatabase::new(&store)))
        };
        if matches!(expected, RecoveryExpectation::Torn) {
            assert!(matches!(
                result.unwrap_err().kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
            assert_eq!(fs::read(&witness).unwrap(), before_witness);
        } else {
            result.unwrap();
        }
        assert_eq!(fs::read(&log).unwrap(), before_log);
    }
}

pub(crate) fn verify_recovery(
    path: &Path,
    checkpoint: &Path,
    digest: &[u8; 32],
    operation: usize,
    expected: RecoveryExpectation,
    checkpoint_first: bool,
) -> Option<bool> {
    use crate::{CheckpointStore, Store, test_support::retry_lock};
    let log = path.join("records.log");
    let before = std::fs::read(&log).unwrap();
    let mut recovered = None;
    let mut verify_opened = |db: &DocumentDatabase<'_>| {
        assert!(
            !matches!(expected, RecoveryExpectation::Torn),
            "torn record reopened"
        );
        let committed = match operation {
            0 => db.get("c", "b").unwrap().is_some(),
            1 | 2 | 4 => db.get("c", "a").unwrap() == Some(json!({"x":2})),
            3 => db.get("c", "a").unwrap().is_none(),
            _ => unreachable!(),
        };
        if let RecoveryExpectation::Committed(value) = expected {
            assert_eq!(committed, value);
        }
        if let Some(previous) = recovered {
            assert_eq!(committed, previous, "recovery paths disagree");
        }
        verify(db, operation, committed);
        recovered = Some(committed);
    };
    for checkpoint_mode in [checkpoint_first, !checkpoint_first] {
        if checkpoint_mode {
            match retry_lock(|| CheckpointStore::open(path, checkpoint, digest)) {
                Ok(store) => verify_opened(&DocumentDatabase::with_checkpoint_store(&store)),
                Err(error) => {
                    assert!(
                        matches!(expected, RecoveryExpectation::Torn),
                        "unexpected checkpoint recovery failure: {error}"
                    );
                    assert!(matches!(
                        error.kind(),
                        std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
                    ));
                }
            }
        } else {
            match retry_lock(|| Store::open(path)) {
                Ok(store) => verify_opened(&DocumentDatabase::new(&store)),
                Err(error) => {
                    assert!(
                        matches!(expected, RecoveryExpectation::Torn),
                        "unexpected ordinary recovery failure: {error}"
                    );
                    assert!(matches!(
                        error.kind(),
                        std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
                    ));
                }
            }
        }
        assert_eq!(std::fs::read(&log).unwrap(), before);
    }
    recovered
}
