//! Atomic metadata/document observations for the optimized standalone read.
use super::*;
use crate::{BatchOperation, Store};
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;

fn metadata(field_type: FieldType) -> Vec<u8> {
    serde_json::to_vec(&transaction::Metadata {
        version: 1,
        schema: CollectionSchema {
            fields: vec![FieldRule {
                path: vec!["n".into()],
                field_type,
                required: true,
                nullable: false,
            }],
        },
        indexes: Vec::new(),
        jobs: Vec::new(),
        schema_job: None,
        dropping: false,
    })
    .unwrap()
}

#[test]
fn dropping_metadata_is_rejected_for_normal_reads_and_invalid_versions_fail_closed() {
    let mut value: serde_json::Value =
        serde_json::from_slice(&metadata(FieldType::Integer)).unwrap();
    for version in [1, 2] {
        value["version"] = json!(version);
        assert!(
            transaction::DocumentTransaction::decode_metadata(
                "c",
                Some(serde_json::to_vec(&value).unwrap())
            )
            .is_ok()
        );
    }
    value["version"] = json!(3);
    value["dropping"] = json!(true);
    assert_eq!(
        transaction::DocumentTransaction::decode_metadata(
            "c",
            Some(serde_json::to_vec(&value).unwrap())
        )
        .err()
        .unwrap()
        .kind(),
        std::io::ErrorKind::WouldBlock
    );
    for (version, dropping) in [(3, false), (2, true), (4, true)] {
        value["version"] = json!(version);
        value["dropping"] = json!(dropping);
        assert_eq!(
            transaction::DocumentTransaction::decode_metadata(
                "c",
                Some(serde_json::to_vec(&value).unwrap())
            )
            .err()
            .unwrap()
            .kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}

#[test]
fn standalone_get_never_mixes_schema_and_document_from_atomic_writes() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(root.path()).unwrap());
    let metadata_key = key::collection(b'M', "c").unwrap();
    let document_key = key::document("c", "a").unwrap();
    let integer_metadata = metadata(FieldType::Integer);
    let string_metadata = metadata(FieldType::String);
    let integer_document = br#"{"n":1}"#;
    let string_document = br#"{"n":"one"}"#;
    store
        .write_batch(&[
            BatchOperation::Put {
                key: &metadata_key,
                value: &integer_metadata,
            },
            BatchOperation::Put {
                key: &document_key,
                value: integer_document,
            },
        ])
        .unwrap();
    let start = Arc::new(Barrier::new(2));
    let writer_store = Arc::clone(&store);
    let writer_start = Arc::clone(&start);
    let writer = thread::spawn(move || {
        writer_start.wait();
        for index in 0..1000 {
            let (schema, document): (&[u8], &[u8]) = if index % 2 == 0 {
                (&string_metadata, string_document)
            } else {
                (&integer_metadata, integer_document)
            };
            writer_store
                .write_batch(&[
                    BatchOperation::Put {
                        key: &metadata_key,
                        value: schema,
                    },
                    BatchOperation::Put {
                        key: &document_key,
                        value: document,
                    },
                ])
                .unwrap();
            thread::yield_now();
        }
    });
    start.wait();
    let db = DocumentDatabase::new(&store);
    let mut successful = 0;
    for _ in 0..2000 {
        match db.get("c", "a") {
            Ok(Some(value)) => {
                assert!(value == json!({"n":1}) || value == json!({"n":"one"}));
                successful += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            outcome => panic!("unexpected standalone read: {outcome:?}"),
        }
        thread::yield_now();
    }
    writer.join().unwrap();
    assert!(successful > 0);
    assert_eq!(db.get("c", "a").unwrap(), Some(json!({"n":1})));
}
