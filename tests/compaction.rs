use database_engine::Store;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

#[test]
fn compaction_preserves_live_values_and_omits_obsolete_records() {
    let dir = tempdir().unwrap();
    let live = dir.path().join("live");
    let store = Store::open(&live).unwrap();
    store.put(b"name", b"obsolete-secret-value").unwrap();
    store.put(b"name", b"current").unwrap();
    store.put(b"deleted", b"deleted-secret-value").unwrap();
    store.delete(b"deleted").unwrap();
    store.put(b"empty", b"").unwrap();
    let before = fs::read(live.join("records.log")).unwrap();
    let compact = dir.path().join("compacted");
    store.compact_to(&compact).unwrap();
    assert_eq!(fs::read(live.join("records.log")).unwrap(), before);
    let bytes = fs::read(compact.join("records.log")).unwrap();
    assert!(bytes.len() < before.len());
    assert!(
        !bytes
            .windows(b"obsolete-secret-value".len())
            .any(|w| w == b"obsolete-secret-value")
    );
    assert!(
        !bytes
            .windows(b"deleted-secret-value".len())
            .any(|w| w == b"deleted-secret-value")
    );
    let copy = Store::open(&compact).unwrap();
    assert_eq!(copy.get(b"name").unwrap().as_deref(), Some(&b"current"[..]));
    assert_eq!(copy.get(b"deleted").unwrap(), None);
    assert_eq!(copy.get(b"empty").unwrap().as_deref(), Some(&b""[..]));
    copy.put(b"fresh", b"working").unwrap();
    drop(copy);
    assert_eq!(
        Store::open(&compact)
            .unwrap()
            .get(b"fresh")
            .unwrap()
            .as_deref(),
        Some(&b"working"[..])
    );
    store.put(b"live-only", b"still writable").unwrap();
}

#[test]
fn compaction_rejects_corrupt_obsolete_records_without_publication() {
    let dir = tempdir().unwrap();
    let live = dir.path().join("live");
    let store = Store::open(&live).unwrap();
    store.put(b"key", b"old").unwrap();
    store.put(b"key", b"new").unwrap();
    let mut log = OpenOptions::new()
        .write(true)
        .open(live.join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start(28 + 17 + 3)).unwrap();
    log.write_all(b"X").unwrap();
    log.sync_all().unwrap();
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"new"[..]));
    let target = dir.path().join("compact");
    assert_eq!(
        store.compact_to(&target).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(!target.exists());
}

#[test]
fn compaction_never_overwrites_existing_destination() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path().join("live")).unwrap();
    store.put(b"key", b"value").unwrap();
    let destination = dir.path().join("existing");
    fs::create_dir(&destination).unwrap();
    fs::write(destination.join("sentinel"), b"preserve").unwrap();
    assert_eq!(
        store.compact_to(&destination).unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(destination.join("sentinel")).unwrap(), b"preserve");
    assert!(fs::read_dir(dir.path()).unwrap().all(|e| {
        !e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".db-compact-")
    }));
}
