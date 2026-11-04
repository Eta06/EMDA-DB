#![no_main]
use database_engine::{
    AggregateStatsRequest, CheckpointStore, CollectionSchema, DocumentDatabase, DocumentFilter,
    IndexValue, Store,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

enum Backend {
    Ordinary(Store),
    Checkpoint(CheckpointStore),
}
impl Backend {
    fn db(&self) -> DocumentDatabase<'_> {
        match self {
            Self::Ordinary(store) => DocumentDatabase::new(store),
            Self::Checkpoint(store) => DocumentDatabase::with_checkpoint_store(store),
        }
    }
}
fn open(path: &Path, checkpoint: &Path, digest: &[u8; 32], checkpoint_mode: bool) -> Backend {
    if checkpoint_mode {
        Backend::Checkpoint(CheckpointStore::open(path, checkpoint, digest).unwrap())
    } else {
        Backend::Ordinary(Store::open(path).unwrap())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Group {
    Missing,
    Null,
    False,
    A,
    B,
}
fn group(value: &Value) -> Group {
    match value.get("g") {
        None => Group::Missing,
        Some(Value::Null) => Group::Null,
        Some(Value::Bool(false)) => Group::False,
        Some(Value::String(s)) if s == "a" => Group::A,
        Some(Value::String(s)) if s == "b" => Group::B,
        _ => unreachable!(),
    }
}
fn index_value(group: Group) -> Option<IndexValue> {
    match group {
        Group::Missing => None,
        Group::Null => Some(IndexValue::Null),
        Group::False => Some(IndexValue::Boolean(false)),
        Group::A => Some(IndexValue::String("a".into())),
        Group::B => Some(IndexValue::String("b".into())),
    }
}
fn field(byte: u8) -> Option<Value> {
    match byte % 6 {
        0 => None,
        1 => Some(Value::Null),
        2 => Some(json!(i64::MAX)),
        3 => Some(json!(i64::MIN)),
        _ => Some(json!(i64::from(byte as i8))),
    }
}
fn document(group_byte: u8, n: u8, m: u8) -> Value {
    let mut document = json!({});
    match group_byte % 5 {
        0 => {}
        1 => document["g"] = Value::Null,
        2 => document["g"] = json!(false),
        3 => document["g"] = json!("a"),
        _ => document["g"] = json!("b"),
    }
    if let Some(value) = field(n) {
        document["n"] = value;
    }
    if let Some(value) = field(m) {
        document["m"] = value;
    }
    document
}

#[derive(Default)]
struct Stats {
    count: u64,
    sum: i128,
    min: Option<i64>,
    max: Option<i64>,
}
#[derive(Default)]
struct GroupStats {
    count: u64,
    fields: [Stats; 2],
}
fn check(db: &DocumentDatabase<'_>, model: &BTreeMap<u8, Value>, flags: u8) {
    let grouped = flags & 1 != 0;
    let selected = match (flags >> 1) % 4 {
        0 => None,
        1 => Some(Group::Null),
        2 => Some(Group::A),
        _ => Some(Group::Missing),
    };
    let filter = match selected {
        None => DocumentFilter::All,
        Some(Group::Null) => DocumentFilter::Equal {
            path: vec!["g".into()],
            value: Value::Null,
        },
        Some(Group::A) => DocumentFilter::Equal {
            path: vec!["g".into()],
            value: json!("a"),
        },
        Some(Group::Missing) => DocumentFilter::Exists {
            path: vec!["g".into()],
            exists: false,
        },
        _ => unreachable!(),
    };
    let request = AggregateStatsRequest {
        filter,
        group_by: grouped.then(|| vec!["g".into()]),
        fields: vec![vec!["n".into()], vec!["m".into()]],
        max_scanned: 16,
    };
    let actual = db.aggregate_stats("c", &request).unwrap();
    assert_eq!(actual.fields, request.fields);
    let mut expected = BTreeMap::<Group, GroupStats>::new();
    let mut matched = 0;
    for document in model.values() {
        let g = group(document);
        if selected.is_some_and(|wanted| wanted != g) {
            continue;
        }
        matched += 1;
        let entry = expected
            .entry(if grouped { g } else { Group::Missing })
            .or_default();
        entry.count += 1;
        for (index, name) in ["n", "m"].iter().enumerate() {
            if let Some(value) = document.get(name).and_then(Value::as_i64) {
                let stat = &mut entry.fields[index];
                stat.count += 1;
                stat.sum += i128::from(value);
                stat.min = Some(stat.min.map_or(value, |prior| prior.min(value)));
                stat.max = Some(stat.max.map_or(value, |prior| prior.max(value)));
            }
        }
    }
    if !grouped && expected.is_empty() {
        expected.insert(Group::Missing, GroupStats::default());
    }
    assert_eq!(
        (
            actual.plan.scanned,
            actual.plan.matched,
            actual.plan.returned
        ),
        (model.len(), matched, matched)
    );
    assert_eq!(actual.groups.len(), expected.len());
    for (actual, (key, expected)) in actual.groups.iter().zip(expected) {
        assert_eq!(
            (actual.key.clone(), actual.count),
            (index_value(key), expected.count)
        );
        for (actual, expected) in actual.fields.iter().zip(expected.fields) {
            assert_eq!(
                (actual.count, actual.sum, actual.min, actual.max),
                (expected.count, expected.sum, expected.min, expected.max)
            );
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 96 {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let checkpoint_mode = data[0] & 1 != 0;
    let mut store = open(&path, &checkpoint, &digest, checkpoint_mode);
    store
        .db()
        .create_collection("c", CollectionSchema::default())
        .unwrap();
    let mut model = BTreeMap::<u8, Value>::new();
    check(&store.db(), &model, data[0]);
    for bytes in data[1..].chunks_exact(4) {
        let id = bytes[1] % 8;
        match bytes[0] % 5 {
            0..=2 => {
                let doc = document(bytes[0].wrapping_add(bytes[2]), bytes[2], bytes[3]);
                store
                    .db()
                    .replace("c", &id.to_string(), &doc, true)
                    .unwrap();
                model.insert(id, doc);
            }
            3 => {
                store.db().delete("c", &id.to_string()).unwrap();
                model.remove(&id);
            }
            _ => {
                drop(store);
                store = open(&path, &checkpoint, &digest, checkpoint_mode);
            }
        }
        check(&store.db(), &model, bytes[3]);
    }
});
