use database_engine::{MAX_SCAN_BYTES, MAX_SCAN_ENTRIES, Store};
use std::io::ErrorKind;

#[test]
fn ordered_pages_apply_exclusive_bounds_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    for (key, value) in [
        (b"b".as_slice(), b"2".as_slice()),
        (b"a", b"1"),
        (b"aa", b"3"),
        (b"\xff", b"4"),
    ] {
        store.put(key, value).unwrap();
    }
    store.delete(b"b").unwrap();
    let first = store.scan(None, None, 2).unwrap();
    assert_eq!(
        first,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"aa".to_vec(), b"3".to_vec())
        ]
    );
    let second = store.scan(Some(&first[1].0), None, 2).unwrap();
    assert_eq!(second, vec![(b"\xff".to_vec(), b"4".to_vec())]);
    assert!(store.scan(Some(&second[0].0), None, 2).unwrap().is_empty());
    assert_eq!(
        store.scan(Some(b"a"), Some(b"b"), 10).unwrap(),
        vec![(b"aa".to_vec(), b"3".to_vec())]
    );
    assert!(store.scan(Some(b"b"), Some(b"a"), 10).unwrap().is_empty());
    assert!(store.scan(Some(b"a"), Some(b"a"), 10).unwrap().is_empty());
    drop(store);
    assert_eq!(
        Store::open(dir.path())
            .unwrap()
            .scan(None, None, 2)
            .unwrap(),
        first
    );
}

#[test]
fn scan_rejects_invalid_bounds_and_limits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    for result in [
        store.scan(Some(b""), None, 1),
        store.scan(None, Some(b""), 1),
        store.scan(None, None, 0),
        store.scan(None, None, MAX_SCAN_ENTRIES + 1),
    ] {
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn scan_bounds_response_bytes_and_next_page_makes_progress() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let value = vec![42; 12 * 1024 * 1024];
    for key in [b"a", b"b", b"c"] {
        store.put(key, &value).unwrap();
    }
    let first = store.scan(None, None, 3).unwrap();
    assert_eq!(first.len(), 2);
    assert!(first.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() <= MAX_SCAN_BYTES);
    let second = store.scan(Some(&first.last().unwrap().0), None, 3).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, b"c");
    assert_eq!(second[0].1, value);
    assert!(store.scan(Some(b"c"), None, 3).unwrap().is_empty());
}
