use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, DocumentUpdate, FieldCondition,
    IndexValue, MAX_DOCUMENT_UPDATE_INPUT_BYTES, Store,
};
use serde_json::{Value, json};
use std::io::ErrorKind;

mod support;
use support::retry_lock;

fn path(value: &str) -> Vec<String> {
    vec![value.to_owned()]
}
fn both_backends(action: impl Fn(&DocumentDatabase<'_>)) {
    for checkpoint_mode in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let store = Store::open(&source).unwrap();
        let digest = store.checkpoint_to(&checkpoint).unwrap();
        if checkpoint_mode {
            drop(store);
            let store =
                retry_lock(|| CheckpointStore::open(&source, &checkpoint, &digest)).unwrap();
            action(&DocumentDatabase::with_checkpoint_store(&store));
        } else {
            action(&DocumentDatabase::new(&store));
        }
    }
}

#[test]
fn mixed_condition_and_operator_inputs_share_one_exact_byte_budget() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"x":0})).unwrap();
        assert_eq!(MAX_DOCUMENT_UPDATE_INPUT_BYTES, 4 * 1024 * 1024);
        let boundary = Value::String("x".repeat(1024 * 1024 - 2));
        assert_eq!(serde_json::to_vec(&boundary).unwrap().len(), 1024 * 1024);
        let conditions = [FieldCondition {
            path: path("absent"),
            expected: Some(boundary.clone()),
        }];
        let updates = vec![
            DocumentUpdate::SetIfMissing {
                path: path("x"),
                value: boundary.clone(),
            },
            DocumentUpdate::Set {
                path: path("x"),
                value: boundary.clone(),
            },
            DocumentUpdate::ArrayRemove {
                path: path("missing"),
                value: boundary.clone(),
            },
        ];
        assert!(
            !db.update_if("c", "a", &conditions, &updates, false)
                .unwrap()
        );
        let mut exact_with_scalar = updates.clone();
        let DocumentUpdate::Set { value, .. } = &mut exact_with_scalar[1] else {
            unreachable!()
        };
        *value = Value::String("x".repeat(1024 * 1024 - 6));
        exact_with_scalar.push(DocumentUpdate::Min {
            path: path("x"),
            value: IndexValue::Null,
        });
        assert!(
            !db.update_if("c", "a", &conditions, &exact_with_scalar, false)
                .unwrap()
        );
        let mut over = updates.clone();
        over.push(DocumentUpdate::Min {
            path: path("x"),
            value: IndexValue::Null,
        });
        let error = db
            .transaction(|tx| {
                tx.put_raw(b"must-rollback", b"no")?;
                tx.update_if("c", "a", &conditions, &over, false)?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"must-rollback")).unwrap(),
            None
        );
        assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":0})));
    });
}

#[test]
fn set_if_missing_inputs_respect_the_limit_even_when_no_value_changes() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"x":null})).unwrap();
        let boundary = Value::String("x".repeat(1024 * 1024 - 2));
        let update = DocumentUpdate::SetIfMissing {
            path: path("x"),
            value: boundary,
        };
        assert!(
            db.update_if("c", "a", &[], &vec![update.clone(); 4], false)
                .unwrap()
        );
        let mut over = vec![update; 4];
        over.push(DocumentUpdate::SetIfMissing {
            path: path("x"),
            value: json!(0),
        });
        assert_eq!(
            db.update_if("c", "a", &[], &over, false)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":null})));
    });
}

#[test]
fn escaped_json_bytes_are_counted_instead_of_raw_string_length() {
    both_backends(|db| {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"x":0})).unwrap();
        let escaped = Value::String("\0".repeat(174_763));
        let encoded = serde_json::to_vec(&escaped).unwrap().len();
        assert!(encoded * 4 > MAX_DOCUMENT_UPDATE_INPUT_BYTES);
        let update = DocumentUpdate::SetIfMissing {
            path: path("x"),
            value: escaped,
        };
        assert_eq!(
            db.update_if("c", "a", &[], &vec![update; 4], false)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(db.get("c", "a").unwrap(), Some(json!({"x":0})));
    });
}
