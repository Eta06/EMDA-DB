//! Shared document and unique-index checks for detailed mutation faults.
use crate::{
    BulkOperation, CollectionSchema, DocumentDatabase, DocumentUpdate, IndexDefinition, IndexQuery,
    IndexValue, TransactionFailure,
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
