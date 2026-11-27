//! `Store::discard_incomplete_tail`: a process that stops during an append
//! can leave an incomplete final record, which `Store::open` refuses. The
//! repair removes only such a record at or after the witnessed boundary.
use database_engine::Store;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

fn log_len(path: &Path) -> u64 {
    fs::metadata(path.join("records.log")).unwrap().len()
}

fn append(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path.join("records.log"))
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

/// A store with acknowledged keys `a0..a9`, and the bytes of one more
/// complete record. Record bytes depend only on the key and value, not on
/// the store's identity, so they are taken from a separate fresh store.
fn store_and_record(path: &Path) -> Vec<u8> {
    let store = Store::open(path).unwrap();
    for index in 0..10 {
        store
            .put(format!("a{index}").as_bytes(), &vec![index as u8; 300])
            .unwrap();
    }
    drop(store);
    let scratch = tempfile::tempdir().unwrap();
    let other = Store::open(scratch.path()).unwrap();
    let before = log_len(scratch.path());
    other.put(b"tail", &[7; 5_000]).unwrap();
    drop(other);
    let log = fs::read(scratch.path().join("records.log")).unwrap();
    log[before as usize..].to_vec()
}

fn assert_acknowledged(store: &Store) {
    for index in 0..10 {
        assert_eq!(
            store.get(format!("a{index}").as_bytes()).unwrap(),
            Some(vec![index as u8; 300])
        );
    }
}

#[test]
fn torn_final_records_are_removed_and_acknowledged_writes_survive() {
    for cut in [1usize, 16, 17, 40, 2_500, 5_000] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let record = store_and_record(&path);
        let acknowledged = log_len(&path);
        append(&path, &record[..cut.min(record.len() - 1)]);
        let error = Store::open(&path)
            .err()
            .expect("open refuses an incomplete record");
        assert!(
            error.to_string().contains("incomplete log record"),
            "{error}"
        );
        let removed = Store::discard_incomplete_tail(&path).unwrap();
        assert_eq!(removed, Some(cut.min(record.len() - 1) as u64), "cut {cut}");
        assert_eq!(log_len(&path), acknowledged);
        let store = Store::open(&path).unwrap();
        assert_acknowledged(&store);
        assert_eq!(store.get(b"tail").unwrap(), None);
        // The store accepts writes again.
        store.put(b"after", b"ok").unwrap();
        drop(store);
        assert_eq!(Store::discard_incomplete_tail(&path).unwrap(), None);
    }
}

#[test]
fn a_complete_unacknowledged_record_is_kept_and_a_torn_one_after_it_removed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let record = store_and_record(&path);
    let acknowledged = log_len(&path);
    // One complete record beyond the witness, then half of another.
    append(&path, &record);
    append(&path, &record[..record.len() / 2]);
    assert!(Store::open(&path).is_err());
    assert_eq!(
        Store::discard_incomplete_tail(&path).unwrap(),
        Some((record.len() / 2) as u64)
    );
    assert_eq!(log_len(&path), acknowledged + record.len() as u64);
    // Open verifies and adopts the complete tail record.
    let store = Store::open(&path).unwrap();
    assert_acknowledged(&store);
    assert_eq!(store.get(b"tail").unwrap(), Some(vec![7; 5_000]));
}

#[test]
fn anything_other_than_an_incomplete_record_is_refused_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let record = store_and_record(&path);
    // A clean log has nothing to remove.
    assert_eq!(Store::discard_incomplete_tail(&path).unwrap(), None);
    // A corrupt record header after the boundary is an error, not a tear.
    let mut corrupt = record.clone();
    corrupt[3] ^= 0xff;
    append(&path, &corrupt);
    let before = fs::read(path.join("records.log")).unwrap();
    assert!(Store::discard_incomplete_tail(&path).is_err());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    // While the store is open the repair cannot take its lock.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let _record = store_and_record(&path);
    let open = Store::open(&path).unwrap();
    assert!(Store::discard_incomplete_tail(&path).is_err());
    drop(open);
    // A path that is not a store is an error.
    assert!(Store::discard_incomplete_tail(dir.path().join("missing")).is_err());
}

/// The same repair applies to the log behind a `CheckpointStore`: after a
/// torn final record, open refuses, the repair removes it, and the store
/// reopens with every acknowledged write.
#[test]
fn the_repair_applies_to_a_checkpoint_store_log() {
    use database_engine::CheckpointStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let record = store_and_record(&path);
    let initial = Store::open(&path).unwrap();
    let digest = initial.checkpoint_to(dir.path().join("cp")).unwrap();
    drop(initial);
    {
        let checkpoint = CheckpointStore::open(&path, dir.path().join("cp"), &digest).unwrap();
        checkpoint.put(b"after", b"checkpoint").unwrap();
    }
    append(&path, &record[..40]);
    let error = CheckpointStore::open(&path, dir.path().join("cp"), &digest)
        .err()
        .expect("a torn tail is refused");
    assert!(
        error.to_string().contains("incomplete log record"),
        "{error}"
    );
    assert_eq!(Store::discard_incomplete_tail(&path).unwrap(), Some(40));
    let checkpoint = CheckpointStore::open(&path, dir.path().join("cp"), &digest).unwrap();
    for index in 0..10 {
        assert_eq!(
            checkpoint.get(format!("a{index}").as_bytes()).unwrap(),
            Some(vec![index as u8; 300])
        );
    }
    assert_eq!(
        checkpoint.get(b"after").unwrap(),
        Some(b"checkpoint".to_vec())
    );
}
