use database_engine::{MAX_SCAN_ENTRIES, Store};
use std::io::ErrorKind;

#[test]
fn bulk_read_preserves_order_duplicates_and_missing_values_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"a", b"one").unwrap();
    store.put(b"empty", b"").unwrap();
    let expected = vec![
        Some(b"one".to_vec()),
        None,
        Some(Vec::new()),
        Some(b"one".to_vec()),
    ];
    let keys = [b"a".as_slice(), b"missing", b"empty", b"a"];
    assert_eq!(store.get_many(&keys).unwrap(), expected);
    assert!(store.get_many(&[]).unwrap().is_empty());
    drop(store);
    assert_eq!(
        Store::open(dir.path()).unwrap().get_many(&keys).unwrap(),
        expected
    );
}

#[test]
fn bulk_read_rejects_invalid_keys_and_count_and_response_byte_limits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(
        store.get_many(&[b""]).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let keys = vec![b"k".as_slice(); MAX_SCAN_ENTRIES + 1];
    assert_eq!(
        store.get_many(&keys).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let value = vec![42; 12 * 1024 * 1024];
    store.put(b"large", &value).unwrap();
    assert_eq!(
        store
            .get_many(&[b"large", b"large", b"large"])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    let values = store.get_many(&[b"large", b"missing"]).unwrap();
    assert_eq!(values[0].as_ref(), Some(&value));
    assert_eq!(values[1], None);
}
