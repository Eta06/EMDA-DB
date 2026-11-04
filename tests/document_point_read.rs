use database_engine::{BatchOperation, CollectionSchema, DocumentDatabase, Store};
use serde_json::json;
use std::io;

#[test]
fn standalone_get_reads_large_store_without_a_full_transaction_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"value":7})).unwrap();
    for start in (0..17_000).step_by(512) {
        let keys = (start..(start + 512).min(17_000))
            .map(|number| format!("raw-{number:05}").into_bytes())
            .collect::<Vec<_>>();
        let operations = keys
            .iter()
            .map(|key| BatchOperation::Put { key, value: b"x" })
            .collect::<Vec<_>>();
        store.write_batch(&operations).unwrap();
    }
    assert!(
        db.transaction(|tx| tx.get("c", "a"))
            .unwrap_err()
            .to_string()
            .contains("snapshot")
    );
    assert_eq!(db.get("c", "a").unwrap(), Some(json!({"value":7})));
    assert_eq!(db.get("c", "absent").unwrap(), None);
    drop(store);
    let store = Store::open(root.path()).unwrap();
    assert_eq!(
        DocumentDatabase::new(&store).get("c", "a").unwrap(),
        Some(json!({"value":7}))
    );
}

#[test]
fn standalone_get_retains_missing_invalid_and_corrupt_data_errors() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = DocumentDatabase::new(&store);
    assert_eq!(
        db.get("missing", "").unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.insert("c", "a", &json!({"value":7})).unwrap();
    assert_eq!(
        db.get("c", "").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(db.get("c", "absent").unwrap(), None);
    let rows = store.scan(None, None, 100).unwrap();
    let (metadata_key, metadata_value) = rows
        .iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01M"))
        .unwrap();
    let document_key = rows
        .iter()
        .find(|(key, _)| key.starts_with(b"\0emda-document\x01D"))
        .unwrap()
        .0
        .clone();
    store.put(metadata_key, b"{broken").unwrap();
    assert_eq!(
        db.get("c", "a").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    store.put(metadata_key, metadata_value).unwrap();
    store.put(&document_key, b"{broken").unwrap();
    assert_eq!(
        db.get("c", "a").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn standalone_get_still_detects_corrupted_log_values() {
    use std::os::unix::fs::FileExt;
    for corrupt_metadata in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let db = DocumentDatabase::new(&store);
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.insert("c", "a", &json!({"value":7})).unwrap();
        let records = store.scan(None, None, 100).unwrap();
        let kind: &[u8] = if corrupt_metadata {
            b"\0emda-document\x01M"
        } else {
            b"\0emda-document\x01D"
        };
        let value = &records
            .iter()
            .find(|(key, _)| key.starts_with(kind))
            .unwrap()
            .1;
        let path = root.path().join("records.log");
        let log = std::fs::read(&path).unwrap();
        let offset = log
            .windows(value.len())
            .rposition(|window| window == value)
            .unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_all_at(&[value[0] ^ 1], offset as u64).unwrap();
        assert_eq!(
            db.get("c", "a").unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
