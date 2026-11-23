use database_engine::{
    CheckpointStore, CollectionSchema, DocumentDatabase, IndexBound, IndexCursor, IndexDefinition,
    IndexQuery, IndexValue, Store,
};
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::io;

mod support;
use support::retry_lock;

fn compare(left: &IndexValue, right: &IndexValue) -> Ordering {
    fn rank(value: &IndexValue) -> u8 {
        match value {
            IndexValue::Null => 0,
            IndexValue::Boolean(_) => 1,
            IndexValue::Integer(_) => 2,
            IndexValue::String(_) => 3,
        }
    }
    rank(left)
        .cmp(&rank(right))
        .then_with(|| match (left, right) {
            (IndexValue::Boolean(a), IndexValue::Boolean(b)) => a.cmp(b),
            (IndexValue::Integer(a), IndexValue::Integer(b)) => a.cmp(b),
            (IndexValue::String(a), IndexValue::String(b)) => a.as_bytes().cmp(b.as_bytes()),
            _ => Ordering::Equal,
        })
}
fn json_value(value: &IndexValue) -> Value {
    match value {
        IndexValue::Null => Value::Null,
        IndexValue::Boolean(value) => json!(value),
        IndexValue::Integer(value) => json!(value),
        IndexValue::String(value) => json!(value),
    }
}

#[test]
fn named_index_cursor_round_trips_across_reopen_and_validates_its_position() {
    let definition = IndexDefinition {
        name: "order".into(),
        fields: vec![vec!["x".into()]],
        unique: false,
        partial: None,
    };
    for checkpoint_backed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        let checkpoint = root.path().join("checkpoint");
        let initial = Store::open(&path).unwrap();
        let digest = initial.checkpoint_to(&checkpoint).unwrap();
        drop(initial);
        let with_db = |action: &mut dyn FnMut(DocumentDatabase<'_>)| {
            if checkpoint_backed {
                let store =
                    retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
                action(DocumentDatabase::with_checkpoint_store(&store));
            } else {
                let store = retry_lock(|| Store::open(&path)).unwrap();
                action(DocumentDatabase::new(&store));
            }
        };
        let mut saved = None;
        with_db(&mut |db| {
            db.create_collection("c", CollectionSchema::default())
                .unwrap();
            db.create_index("c", definition.clone()).unwrap();
            db.insert("c", "anchor", &json!({"x": 2})).unwrap();
            db.insert("c", "second", &json!({"x": 2})).unwrap();
            db.insert("c", "third", &json!({"x": 3})).unwrap();
            let page = db
                .query_index(
                    "c",
                    "order",
                    &IndexQuery {
                        limit: 1,
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(page.documents[0].id, "anchor");
            let cursor = page.next.unwrap();
            let bytes = cursor.to_bytes();
            assert_eq!(IndexCursor::from_bytes(&bytes).unwrap(), cursor);
            saved = Some(bytes);
        });
        let bytes = saved.unwrap();
        for end in 0..bytes.len() {
            assert_eq!(
                IndexCursor::from_bytes(&bytes[..end]).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let mut damaged = bytes.clone();
        damaged[8] ^= 1;
        assert_eq!(
            IndexCursor::from_bytes(&damaged).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let mut boundary = bytes[..bytes.len() - 4].to_vec();
        boundary.resize(1064, b'x');
        let crc = crc32fast::hash(&boundary).to_le_bytes();
        boundary.extend_from_slice(&crc);
        assert_eq!(
            IndexCursor::from_bytes(&boundary).unwrap().to_bytes(),
            boundary
        );
        boundary.truncate(1064);
        boundary.push(b'x');
        let crc = crc32fast::hash(&boundary).to_le_bytes();
        boundary.extend_from_slice(&crc);
        assert_eq!(
            IndexCursor::from_bytes(&boundary).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let mut invalid_position = bytes.clone();
        let checksum = invalid_position.len() - 4;
        invalid_position[checksum - 1] = 0xff;
        let crc = crc32fast::hash(&invalid_position[..checksum]).to_le_bytes();
        invalid_position[checksum..].copy_from_slice(&crc);

        with_db(&mut |db| {
            assert!(db.delete("c", "anchor").unwrap());
            db.insert("c", "earlier", &json!({"x": 1})).unwrap();
            let query = IndexQuery {
                after: Some(IndexCursor::from_bytes(&bytes).unwrap()),
                ..Default::default()
            };
            let page = db.query_index("c", "order", &query).unwrap();
            assert_eq!(
                page.documents
                    .iter()
                    .map(|row| row.id.as_str())
                    .collect::<Vec<_>>(),
                ["second", "third"]
            );
            let malformed = IndexQuery {
                after: Some(IndexCursor::from_bytes(&invalid_position).unwrap()),
                ..query.clone()
            };
            assert_eq!(
                db.query_index("c", "order", &malformed).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            let outside = IndexQuery {
                lower: Some(IndexBound {
                    value: IndexValue::Integer(2),
                    inclusive: false,
                }),
                ..query.clone()
            };
            assert_eq!(
                db.query_index("c", "order", &outside).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            db.create_collection("other", CollectionSchema::default())
                .unwrap();
            db.create_index("other", definition.clone()).unwrap();
            assert_eq!(
                db.query_index("other", "order", &query).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            db.drop_index("c", "order").unwrap();
            db.create_index(
                "c",
                IndexDefinition {
                    fields: vec![vec!["y".into()]],
                    ..definition.clone()
                },
            )
            .unwrap();
            assert_eq!(
                db.query_index("c", "order", &query).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        });
    }
}

#[test]
fn index_order_and_range_boundaries_match_an_independent_scalar_model() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    let mut values = vec![
        IndexValue::Null,
        IndexValue::Boolean(false),
        IndexValue::Boolean(true),
        IndexValue::Integer(i64::MIN),
        IndexValue::Integer(-1),
        IndexValue::Integer(0),
        IndexValue::Integer(i64::MAX),
        IndexValue::String("".into()),
        IndexValue::String("\0".into()),
        IndexValue::String("a\0z".into()),
        IndexValue::String("a".into()),
        IndexValue::String("é".into()),
        IndexValue::String("中".into()),
    ];
    for number in 0..60 {
        values.push(IndexValue::Integer((number * 97 % 101) as i64 - 50));
        values.push(IndexValue::String(format!("a\0{:02}", number * 37 % 61)));
    }
    let mut expected = Vec::new();
    for (number, value) in values.into_iter().enumerate() {
        let id = format!("{number:03}");
        db.insert("c", &id, &json!({"x":json_value(&value)}))
            .unwrap();
        expected.push((value, id));
    }
    db.create_index(
        "c",
        IndexDefinition {
            name: "x".into(),
            fields: vec![vec!["x".into()]],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    expected.sort_by(|a, b| compare(&a.0, &b.0).then_with(|| a.1.cmp(&b.1)));
    let mut query = IndexQuery {
        limit: 7,
        ..Default::default()
    };
    let mut actual = Vec::new();
    loop {
        let page = db.query_index("c", "x", &query).unwrap();
        assert_eq!(page.plan.index, "x");
        assert_eq!(page.plan.returned, page.documents.len());
        if page.documents.is_empty() {
            break;
        }
        actual.extend(page.documents.iter().map(|row| row.id.clone()));
        query.after = page.next;
    }
    assert_eq!(
        actual,
        expected
            .iter()
            .map(|(_, id)| id.clone())
            .collect::<Vec<_>>()
    );
    for (lower, lower_inclusive, upper, upper_inclusive) in [
        (
            IndexValue::Integer(i64::MIN),
            true,
            IndexValue::Integer(i64::MAX),
            true,
        ),
        (IndexValue::Integer(-1), false, IndexValue::Integer(0), true),
        (
            IndexValue::String("a".into()),
            true,
            IndexValue::String("b".into()),
            false,
        ),
        (IndexValue::Null, false, IndexValue::Boolean(true), true),
    ] {
        let query = IndexQuery {
            lower: Some(IndexBound {
                value: lower.clone(),
                inclusive: lower_inclusive,
            }),
            upper: Some(IndexBound {
                value: upper.clone(),
                inclusive: upper_inclusive,
            }),
            limit: 256,
            ..Default::default()
        };
        let actual = db
            .query_index("c", "x", &query)
            .unwrap()
            .documents
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        let expected = expected
            .iter()
            .filter(|(value, _)| {
                let low = compare(value, &lower);
                let high = compare(value, &upper);
                (low == Ordering::Greater || (lower_inclusive && low == Ordering::Equal))
                    && (high == Ordering::Less || (upper_inclusive && high == Ordering::Equal))
            })
            .map(|(_, id)| id.clone())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}
