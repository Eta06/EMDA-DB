use database_engine::{
    CollectionSchema, DocumentDatabase, IndexBound, IndexDefinition, IndexQuery, IndexValue, Store,
};
use serde_json::{Value, json};
use std::cmp::Ordering;

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
