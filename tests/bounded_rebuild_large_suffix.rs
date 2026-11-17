//! Bounded managed rebuild when the witnessed suffix exceeds the default
//! recovered-overlay key cap.
use database_engine::{
    BatchOperation, CheckpointStore, Entry, MAX_ATOMIC_BATCH_OPERATIONS,
    MAX_RECOVERED_VIEW_TAIL_KEYS, MAX_SCAN_ENTRIES, Store, cleanup_abandoned_staging,
};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::path::Path;
use std::thread;
use std::time::Duration;

/// One past the hard recovered-overlay cap. Distinct suffix keys also include
/// base keys rewritten or deleted after the checkpoint.
const SUFFIX_KEYS: u32 = MAX_RECOVERED_VIEW_TAIL_KEYS as u32 + 1;
// 4,096 keys per chunk forces several spill runs for this suffix. 64 KiB
// covers those 8-byte keys, and 2 MiB covers the runs while a merge output
// still exists. The operation cap is the exact suffix child count.
const CHUNK_KEYS: usize = 4_096;
const CHUNK_KEY_BYTES: usize = 64 * 1024;
const SPILL_BYTES: u64 = 2 * 1024 * 1024;
const MERGE_FAN_IN: usize = 4;
/// `b/replaced` put, `b/removed` delete, `b/cycled` delete + put, `b/absent` delete.
const BASE_SUFFIX_OPERATIONS: u64 = 5;

#[test]
fn bounded_rebuild_restores_a_suffix_beyond_the_recovered_overlay_limit() {
    assert_eq!(MAX_RECOVERED_VIEW_TAIL_KEYS, 16_384);

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("base");
    let mut model = Model::default();
    {
        let store = Store::open(&path).unwrap();
        model.put(&store, b"b/kept", b"kept");
        model.put(&store, b"b/replaced", b"old");
        model.put(&store, b"b/removed", b"gone");
        model.put(&store, b"b/cycled", b"first");
        model.flush(&store);
    }
    let digest = CheckpointStore::rebuild_managed_selection(&path, &base).unwrap();
    let base_bytes = fs::read(&base).unwrap();
    {
        let store = retry(|| Store::open(&path)).unwrap();
        model.begin_suffix();
        for id in 0..SUFFIX_KEYS {
            model.put(&store, &suffix_key(id), b"v1");
        }
        for id in 0..SUFFIX_KEYS {
            let key = suffix_key(id);
            match id % 32 {
                0 => model.put(&store, &key, b"v2"),
                1 => model.delete(&store, &key),
                2 => {
                    model.put(&store, &key, b"v2");
                    model.delete(&store, &key);
                }
                3 => {
                    model.delete(&store, &key);
                    model.put(&store, &key, b"v3");
                }
                4 => {
                    model.put(&store, &key, b"v2");
                    model.delete(&store, &key);
                    model.put(&store, &key, b"v4");
                }
                _ => {}
            }
        }
        model.put(&store, b"b/replaced", b"new");
        model.delete(&store, b"b/removed");
        model.delete(&store, b"b/cycled");
        model.put(&store, b"b/cycled", b"second");
        model.delete(&store, b"b/absent");
        model.flush(&store);
    }

    assert!(model.suffix_distinct > MAX_RECOVERED_VIEW_TAIL_KEYS);
    assert_eq!(model.suffix_distinct, MAX_RECOVERED_VIEW_TAIL_KEYS + 1 + 4);
    assert!(!model.seen.contains(b"b/kept".as_slice()));
    assert_eq!(model.peak, 4 + usize::try_from(SUFFIX_KEYS).unwrap());
    assert!(model.live < model.peak);
    assert_eq!(model.live, model.expected.len());
    let revisions = (0..SUFFIX_KEYS).map(revision_operations).sum::<u64>();
    assert_eq!(
        model.operations,
        u64::from(SUFFIX_KEYS) + revisions + BASE_SUFFIX_OPERATIONS
    );
    assert!(CHUNK_KEYS < model.suffix_distinct);
    assert!(model.operations > u64::from(SUFFIX_KEYS));
    assert!(model.operations < u64::from(SUFFIX_KEYS) * 2);

    let image = log_image(&path);
    let rejected = match retry(|| CheckpointStore::open_managed(&path)) {
        Ok(_) => panic!("managed open accepted a suffix past the overlay key limit"),
        Err(error) => error,
    };
    assert_eq!(rejected.kind(), ErrorKind::InvalidInput, "{rejected}");
    assert!(
        rejected
            .to_string()
            .contains("recovered view suffix key limit exceeded"),
        "{rejected}"
    );
    assert_log_unchanged(&path, &image);

    let reference_path = root.path().join("reference");
    let reference = retry(|| Store::open(&path)).unwrap();
    let reference_entries = collect_entries(|after| reference.scan(after, None, MAX_SCAN_ENTRIES));
    assert_same_entries(&reference_entries, &model.expected);
    let reference_digest = reference.checkpoint_to(&reference_path).unwrap();
    drop(reference);
    assert_log_unchanged(&path, &image);
    assert_eq!(fs::read(&base).unwrap(), base_bytes);

    let destination = root.path().join("rebuilt");
    let rebuilt_digest = CheckpointStore::rebuild_managed_selection_bounded(
        &path,
        &destination,
        &base,
        &digest,
        CHUNK_KEYS,
        CHUNK_KEY_BYTES,
        SPILL_BYTES,
        MERGE_FAN_IN,
        model.operations,
    )
    .unwrap();
    assert_log_unchanged(&path, &image);
    assert_eq!(fs::read(&base).unwrap(), base_bytes);
    assert_eq!(rebuilt_digest, reference_digest);
    assert_eq!(
        fs::read(&destination).unwrap(),
        fs::read(&reference_path).unwrap()
    );
    let (count, peak) = checkpoint_counts(&destination);
    assert_eq!(count, model.live as u64);
    assert_eq!(peak, model.peak as u64);
    assert!(peak > count);

    let rebuilt = retry(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(rebuilt.overlay_keys().unwrap(), 0);
    assert_eq!(rebuilt.tail_bytes().unwrap(), 0);
    assert_eq!(
        rebuilt.get(b"b/kept").unwrap().as_deref(),
        Some(&b"kept"[..])
    );
    assert_eq!(
        rebuilt.get(b"b/replaced").unwrap().as_deref(),
        Some(&b"new"[..])
    );
    assert!(rebuilt.get(b"b/removed").unwrap().is_none());
    assert_eq!(
        rebuilt.get(b"b/cycled").unwrap().as_deref(),
        Some(&b"second"[..])
    );
    assert!(rebuilt.get(b"b/absent").unwrap().is_none());
    assert_eq!(
        rebuilt.get(&suffix_key(0)).unwrap().as_deref(),
        Some(&b"v2"[..])
    );
    assert!(rebuilt.get(&suffix_key(1)).unwrap().is_none());
    assert!(rebuilt.get(&suffix_key(2)).unwrap().is_none());
    assert_eq!(
        rebuilt.get(&suffix_key(3)).unwrap().as_deref(),
        Some(&b"v3"[..])
    );
    assert_eq!(
        rebuilt.get(&suffix_key(4)).unwrap().as_deref(),
        Some(&b"v4"[..])
    );
    assert_eq!(
        rebuilt.get(&suffix_key(5)).unwrap().as_deref(),
        Some(&b"v1"[..])
    );
    assert_eq!(
        rebuilt
            .get(&suffix_key(SUFFIX_KEYS - 1))
            .unwrap()
            .as_deref(),
        Some(&b"v2"[..])
    );
    let restored = collect_entries(|after| rebuilt.scan(after, None, MAX_SCAN_ENTRIES));
    assert_same_entries(&restored, &model.expected);
    drop(rebuilt);
    assert_log_unchanged(&path, &image);

    let cleaned = cleanup_abandoned_staging(&path).unwrap();
    assert_eq!(cleaned.removed_markers, 0);
    assert_eq!(cleaned.removed_payloads, 0);
    assert_eq!(cleaned.skipped, 0);
}

fn suffix_key(id: u32) -> [u8; 8] {
    let mut key = [0; 8];
    key[0] = b's';
    key[4..].copy_from_slice(&id.to_be_bytes());
    key
}

fn revision_operations(id: u32) -> u64 {
    match id % 32 {
        0 | 1 => 1,
        2 | 3 => 2,
        4 => 3,
        _ => 0,
    }
}

struct Model {
    expected: BTreeMap<Vec<u8>, Vec<u8>>,
    seen: HashSet<Vec<u8>>,
    pending_keys: Vec<Vec<u8>>,
    pending_values: Vec<Option<&'static [u8]>>,
    live: usize,
    peak: usize,
    suffix_distinct: usize,
    operations: u64,
    in_suffix: bool,
}

impl Default for Model {
    fn default() -> Self {
        Self {
            expected: BTreeMap::new(),
            seen: HashSet::new(),
            pending_keys: Vec::with_capacity(MAX_ATOMIC_BATCH_OPERATIONS),
            pending_values: Vec::with_capacity(MAX_ATOMIC_BATCH_OPERATIONS),
            live: 0,
            peak: 0,
            suffix_distinct: 0,
            operations: 0,
            in_suffix: false,
        }
    }
}

impl Model {
    fn begin_suffix(&mut self) {
        assert!(self.pending_keys.is_empty());
        assert_eq!(self.operations, 0);
        self.in_suffix = true;
    }

    fn put(&mut self, store: &Store, key: &[u8], value: &'static [u8]) {
        self.note_suffix(key);
        if !self.expected.contains_key(key) {
            self.live += 1;
            self.peak = self.peak.max(self.live);
        }
        self.expected.insert(key.to_vec(), value.to_vec());
        self.enqueue(store, key, Some(value));
    }

    fn delete(&mut self, store: &Store, key: &[u8]) {
        self.note_suffix(key);
        if self.expected.remove(key).is_some() {
            self.live -= 1;
        }
        self.enqueue(store, key, None);
    }

    fn note_suffix(&mut self, key: &[u8]) {
        if self.in_suffix && self.seen.insert(key.to_vec()) {
            self.suffix_distinct += 1;
        }
    }

    fn enqueue(&mut self, store: &Store, key: &[u8], value: Option<&'static [u8]>) {
        if self.pending_keys.len() == MAX_ATOMIC_BATCH_OPERATIONS {
            self.flush(store);
        }
        self.pending_keys.push(key.to_vec());
        self.pending_values.push(value);
    }

    fn flush(&mut self, store: &Store) {
        if self.pending_keys.is_empty() {
            return;
        }
        let batch = self
            .pending_keys
            .iter()
            .zip(self.pending_values.iter())
            .map(|(key, value)| match value {
                Some(value) => BatchOperation::Put { key, value },
                None => BatchOperation::Delete { key },
            })
            .collect::<Vec<_>>();
        store.write_batch(&batch).unwrap();
        if self.in_suffix {
            self.operations += u64::try_from(batch.len()).unwrap();
        }
        self.pending_keys.clear();
        self.pending_values.clear();
    }
}

fn collect_entries(
    mut scan: impl FnMut(Option<&[u8]>) -> io::Result<Vec<Entry>>,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut entries = BTreeMap::new();
    let mut after = None;
    loop {
        let page = scan(after.as_deref()).unwrap();
        if page.is_empty() {
            break;
        }
        let last = page.last().expect("page is not empty").0.clone();
        let full = page.len() == MAX_SCAN_ENTRIES;
        for (key, value) in page {
            entries.insert(key, value);
        }
        if !full {
            break;
        }
        after = Some(last);
    }
    entries
}

fn assert_same_entries(actual: &BTreeMap<Vec<u8>, Vec<u8>>, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut missing = None;
    let mut mismatched = None;
    for (key, value) in expected {
        match actual.get(key) {
            Some(found) if found == value => {}
            Some(found) => {
                mismatched = Some((key.clone(), found.clone(), value.clone()));
                break;
            }
            None => {
                missing = Some(key.clone());
                break;
            }
        }
    }
    let extra = actual
        .keys()
        .find(|key| !expected.contains_key(*key))
        .cloned();
    assert!(
        missing.is_none()
            && mismatched.is_none()
            && extra.is_none()
            && actual.len() == expected.len(),
        "content mismatch: missing={missing:?} extra={extra:?} mismatched={mismatched:?} actual={} expected={}",
        actual.len(),
        expected.len()
    );
}

fn checkpoint_counts(path: &Path) -> (u64, u64) {
    let header = fs::read(path).unwrap();
    assert_eq!(&header[..8], b"EMDAIDX1");
    let count = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let peak = u64::from_le_bytes(header[24..32].try_into().unwrap());
    (count, peak)
}

struct LogImage {
    bytes: Vec<u8>,
    witness: Vec<u8>,
}

fn log_image(path: &Path) -> LogImage {
    LogImage {
        bytes: fs::read(path.join("records.log")).unwrap(),
        witness: witness_bytes(path),
    }
}

fn assert_log_unchanged(path: &Path, image: &LogImage) {
    assert_eq!(fs::read(path.join("records.log")).unwrap(), image.bytes);
    assert_eq!(witness_bytes(path), image.witness);
}

fn witness_bytes(path: &Path) -> Vec<u8> {
    let log = fs::read(path.join("records.log")).unwrap();
    if log.starts_with(b"EMDALOG3") {
        let file = File::open(path.join("records.log")).unwrap();
        let mut buffer = vec![0_u8; 128];
        let count = rustix::fs::fgetxattr(&file, "user.db.commit.v3", &mut buffer).unwrap();
        buffer[..count].to_vec()
    } else {
        fs::read(path.join("commit.state")).unwrap()
    }
}

fn retry<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let mut last = None;
    for _ in 0..100 {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                last = Some(error);
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(error),
        }
    }
    Err(last.expect("retry observed a lock conflict"))
}
