//! Deterministic histories for the bounded managed-checkpoint rebuild.
//!
//! Fixed seeds write a base checkpoint and a witnessed suffix. The suffix
//! overlaps those keys and adds deletes, repeated overwrites, and ordered
//! atomic-batch children. Each small chunk and fan-in limit must reproduce
//! the full-log checkpoint bytes and live values while leaving the log and
//! witness unchanged. An operation budget one child short of the suffix, and
//! a one-byte spill budget once a second key forces a run, both fail before
//! publication.

use database_engine::{
    BatchOperation, CheckpointStore, Entry, MAX_SCAN_ENTRIES, ManagedRebuildFailure, Store,
    cleanup_abandoned_staging,
};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::path::Path;
use std::thread;
use std::time::Duration;

const KEY_COUNT: usize = 8;
const BASE_KEYS: usize = 5;
const SPILL_BUDGET: u64 = 1 << 20;
const SEEDS: [u64; 6] = [
    0x0000_0000_0000_0011,
    0x0000_0000_0000_5eed,
    0x0ddb_a11c_0ddb_a11c,
    0xc0ff_ee01_c0ff_ee01,
    0xa5a5_a5a5_a5a5_a5a5,
    0x0123_4567_89ab_cdef,
];

#[derive(Clone, Copy)]
struct Limits {
    chunk_keys: usize,
    chunk_key_bytes: usize,
    fan_in: usize,
}

/// Count flush, byte flush, mixed flush, wider fan-in, and the in-memory path.
const SUCCESS_LIMITS: [Limits; 5] = [
    Limits {
        chunk_keys: 1,
        chunk_key_bytes: 64,
        fan_in: 2,
    },
    Limits {
        chunk_keys: 8,
        chunk_key_bytes: 2,
        fan_in: 2,
    },
    Limits {
        chunk_keys: 3,
        chunk_key_bytes: 5,
        fan_in: 3,
    },
    Limits {
        chunk_keys: 2,
        chunk_key_bytes: 16,
        fan_in: 4,
    },
    Limits {
        chunk_keys: 8,
        chunk_key_bytes: 64,
        fan_in: 2,
    },
];

const TIGHT_CHUNK: Limits = Limits {
    chunk_keys: 1,
    chunk_key_bytes: 64,
    fan_in: 2,
};

#[derive(Clone, Copy, Debug)]
struct Child {
    key: u8,
    value: Option<u16>,
}

#[derive(Clone, Debug)]
enum Step {
    One(Child),
    Batch(Vec<Child>),
}

impl Step {
    fn children(&self) -> &[Child] {
        match self {
            Self::One(child) => std::slice::from_ref(child),
            Self::Batch(children) => children,
        }
    }
}

struct History {
    base: Vec<Step>,
    suffix: Vec<Step>,
    expected: BTreeMap<Vec<u8>, Vec<u8>>,
    suffix_operations: u64,
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64. The seed is the whole sequence, so a rerun cannot drift.
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }

    fn below(&mut self, limit: usize) -> usize {
        assert!(limit > 0);
        (self.next_u64() as usize) % limit
    }
}

fn key_of(key: u8) -> [u8; 2] {
    [b'k', key]
}

fn fresh(next_value: &mut u16, key: u8) -> Child {
    let value = *next_value;
    *next_value = next_value.checked_add(1).expect("value ids fit in u16");
    Child {
        key,
        value: Some(value),
    }
}

fn deleted(key: u8) -> Child {
    Child { key, value: None }
}

fn random_child(rng: &mut Rng, keys: usize, next_value: &mut u16, key: Option<u8>) -> Child {
    let key = key.unwrap_or_else(|| u8::try_from(rng.below(keys)).unwrap());
    if rng.below(3) == 0 {
        deleted(key)
    } else {
        fresh(next_value, key)
    }
}

fn random_step(rng: &mut Rng, keys: usize, next_value: &mut u16) -> Step {
    if rng.below(5) < 2 {
        let count = 2 + rng.below(3);
        let focus = u8::try_from(rng.below(keys)).unwrap();
        let mut children = Vec::with_capacity(count);
        for index in 0..count {
            let key = (index > 0 && rng.below(2) == 0).then_some(focus);
            children.push(random_child(rng, keys, next_value, key));
        }
        Step::Batch(children)
    } else {
        Step::One(random_child(rng, keys, next_value, None))
    }
}

fn sample(rng: &mut Rng, count: usize) -> Vec<u8> {
    let mut keys: Vec<u8> = (0..u8::try_from(KEY_COUNT).unwrap()).collect();
    for index in 0..count {
        let swap = index + rng.below(keys.len() - index);
        keys.swap(index, swap);
    }
    keys.truncate(count);
    keys
}

fn observe(state: &mut BTreeMap<Vec<u8>, Vec<u8>>, child: &Child) {
    let key = key_of(child.key).to_vec();
    match child.value {
        Some(value) => {
            state.insert(key, value.to_le_bytes().to_vec());
        }
        None => {
            state.remove(&key);
        }
    }
}

fn generate(seed: u64) -> History {
    let mut rng = Rng::new(seed);
    let mut next_value = 1_u16;
    let mut base = Vec::new();
    for key in 0..BASE_KEYS {
        base.push(Step::One(fresh(
            &mut next_value,
            u8::try_from(key).unwrap(),
        )));
    }
    base.push(Step::Batch(vec![
        fresh(&mut next_value, 0),
        deleted(0),
        fresh(&mut next_value, 0),
        fresh(&mut next_value, 1),
        deleted(1),
        fresh(&mut next_value, 1),
    ]));
    for _ in 0..(2 + rng.below(3)) {
        base.push(random_step(&mut rng, BASE_KEYS, &mut next_value));
    }

    let roles = sample(&mut rng, 6);
    let ordered_put = roles[0];
    let ordered_delete = roles[1];
    let overlap_put = roles[2];
    let overlap_delete = roles[3];
    let overwrite = roles[4];
    let dropped = roles[5];

    let mut suffix = Vec::new();
    for key in 0..KEY_COUNT {
        suffix.push(Step::One(fresh(
            &mut next_value,
            u8::try_from(key).unwrap(),
        )));
    }
    for _ in 0..(6 + rng.below(5)) {
        suffix.push(random_step(&mut rng, KEY_COUNT, &mut next_value));
    }

    let earlier_put = fresh(&mut next_value, ordered_put);
    let middle_put = fresh(&mut next_value, ordered_put);
    let winning_put = fresh(&mut next_value, ordered_put);
    assert_ne!(earlier_put.value, middle_put.value, "seed {seed:#x}");
    assert_ne!(middle_put.value, winning_put.value, "seed {seed:#x}");
    suffix.push(Step::Batch(vec![
        earlier_put,
        middle_put,
        fresh(&mut next_value, ordered_delete),
        deleted(ordered_delete),
        winning_put,
    ]));
    let overlap_earlier = fresh(&mut next_value, overlap_put);
    let overlap_winner = fresh(&mut next_value, overlap_put);
    assert_ne!(
        overlap_earlier.value, overlap_winner.value,
        "seed {seed:#x}"
    );
    suffix.push(Step::Batch(vec![
        overlap_earlier,
        fresh(&mut next_value, overlap_delete),
        deleted(overlap_delete),
        overlap_winner,
    ]));
    suffix.push(Step::One(fresh(&mut next_value, overwrite)));
    suffix.push(Step::One(fresh(&mut next_value, overwrite)));
    let last_overwrite = fresh(&mut next_value, overwrite);
    suffix.push(Step::One(last_overwrite));
    suffix.push(Step::One(fresh(&mut next_value, dropped)));
    suffix.push(Step::One(deleted(dropped)));

    let mut expected = BTreeMap::new();
    for step in base.iter().chain(&suffix) {
        for child in step.children() {
            observe(&mut expected, child);
        }
    }
    audit(
        seed,
        &suffix,
        &expected,
        &roles,
        winning_put,
        overlap_winner,
        last_overwrite,
    );

    let suffix_operations = suffix
        .iter()
        .map(|step| u64::try_from(step.children().len()).unwrap())
        .sum();
    History {
        base,
        suffix,
        expected,
        suffix_operations,
    }
}

fn audit(
    seed: u64,
    suffix: &[Step],
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    roles: &[u8],
    winning_put: Child,
    overlap_winner: Child,
    last_overwrite: Child,
) {
    let mut seen = [false; KEY_COUNT];
    let mut puts = [0_u32; KEY_COUNT];
    let mut deletes = 0_u32;
    let mut repeated_batch = false;
    let mut base_overlap = false;
    for step in suffix {
        let mut batch_seen = [false; KEY_COUNT];
        for child in step.children() {
            let index = usize::from(child.key);
            seen[index] = true;
            if index < BASE_KEYS {
                base_overlap = true;
            }
            match child.value {
                Some(_) => puts[index] += 1,
                None => deletes += 1,
            }
            if matches!(step, Step::Batch(_)) {
                if batch_seen[index] {
                    repeated_batch = true;
                }
                batch_seen[index] = true;
            }
        }
    }
    let distinct = seen.iter().filter(|present| **present).count();
    assert!(
        distinct >= 5,
        "seed {seed:#x} distinct suffix keys {distinct}"
    );
    assert!(deletes >= 1, "seed {seed:#x} suffix has no delete");
    assert!(repeated_batch, "seed {seed:#x} batch never repeated a key");
    assert!(base_overlap, "seed {seed:#x} suffix missed every base key");
    assert!(
        puts.iter().any(|count| *count >= 3),
        "seed {seed:#x} suffix has no repeated overwrite"
    );
    assert!(
        expected.len() < KEY_COUNT,
        "seed {seed:#x} final state retained every key"
    );
    assert_value(seed, expected, roles[0], winning_put.value);
    assert_absent(seed, expected, roles[1]);
    assert_value(seed, expected, roles[2], overlap_winner.value);
    assert_absent(seed, expected, roles[3]);
    assert_value(seed, expected, roles[4], last_overwrite.value);
    assert_absent(seed, expected, roles[5]);
}

fn assert_value(seed: u64, expected: &BTreeMap<Vec<u8>, Vec<u8>>, key: u8, value: Option<u16>) {
    let wanted = value.expect("put").to_le_bytes();
    assert_eq!(
        expected.get(key_of(key).as_slice()).map(Vec::as_slice),
        Some(wanted.as_slice()),
        "seed {seed:#x} key {key} lost its last put"
    );
}

fn assert_absent(seed: u64, expected: &BTreeMap<Vec<u8>, Vec<u8>>, key: u8) {
    assert!(
        !expected.contains_key(key_of(key).as_slice()),
        "seed {seed:#x} key {key} survived its last delete"
    );
}

fn apply_steps(store: &Store, steps: &[Step]) {
    for step in steps {
        match step {
            Step::One(child) => apply_child(store, child),
            Step::Batch(children) => apply_batch(store, children),
        }
    }
}

fn apply_child(store: &Store, child: &Child) {
    let key = key_of(child.key);
    match child.value {
        Some(value) => store.put(&key, &value.to_le_bytes()).unwrap(),
        None => store.delete(&key).unwrap(),
    }
}

fn apply_batch(store: &Store, children: &[Child]) {
    let keys: Vec<[u8; 2]> = children.iter().map(|child| key_of(child.key)).collect();
    let values: Vec<[u8; 2]> = children
        .iter()
        .map(|child| child.value.map_or([0; 2], u16::to_le_bytes))
        .collect();
    let operations: Vec<BatchOperation<'_>> = children
        .iter()
        .enumerate()
        .map(|(index, child)| match child.value {
            Some(_) => BatchOperation::Put {
                key: &keys[index],
                value: &values[index],
            },
            None => BatchOperation::Delete { key: &keys[index] },
        })
        .collect();
    store.write_batch(&operations).unwrap();
}

struct Image {
    log: Vec<u8>,
    witness: Vec<u8>,
    base: Vec<u8>,
}

fn snapshot(path: &Path, base: &Path) -> Image {
    Image {
        log: fs::read(path.join("records.log")).unwrap(),
        witness: witness_bytes(path),
        base: fs::read(base).unwrap(),
    }
}

fn assert_preserved(path: &Path, base: &Path, image: &Image, context: &str) {
    assert_eq!(
        fs::read(path.join("records.log")).unwrap(),
        image.log,
        "{context}: log bytes changed"
    );
    assert_eq!(
        witness_bytes(path),
        image.witness,
        "{context}: witness changed"
    );
    assert_eq!(
        fs::read(base).unwrap(),
        image.base,
        "{context}: base changed"
    );
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

fn header_counts(bytes: &[u8]) -> (u64, u64) {
    assert!(
        bytes.len() >= 32,
        "checkpoint is shorter than its count header"
    );
    assert_eq!(&bytes[..8], b"EMDAIDX1");
    let count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let peak = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    (count, peak)
}

fn assert_same_bytes(actual: &[u8], expected: &[u8], context: &str) {
    if actual == expected {
        return;
    }
    let at = actual
        .iter()
        .zip(expected.iter())
        .position(|(left, right)| left != right);
    panic!(
        "{context}: checkpoint bytes differ at {at:?} (rebuilt {} bytes, reference {} bytes)",
        actual.len(),
        expected.len()
    );
}

fn live_entries(
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
            assert!(entries.insert(key, value).is_none(), "scan repeated a key");
        }
        if !full {
            break;
        }
        after = Some(last);
    }
    entries
}

fn assert_point_reads(
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    context: &str,
    mut get: impl FnMut(&[u8]) -> io::Result<Option<Vec<u8>>>,
) {
    for index in 0..KEY_COUNT {
        let key = key_of(u8::try_from(index).unwrap());
        let found = get(&key).unwrap();
        assert_eq!(
            found.as_deref(),
            expected.get(key.as_slice()).map(Vec::as_slice),
            "{context} key {index}"
        );
    }
}

fn rebuild(
    path: &Path,
    destination: &Path,
    base: &Path,
    digest: &[u8; 32],
    limits: Limits,
    spill_bytes: u64,
    operations: u64,
) -> Result<[u8; 32], ManagedRebuildFailure> {
    CheckpointStore::rebuild_managed_selection_bounded_detailed(
        path,
        destination,
        base,
        digest,
        limits.chunk_keys,
        limits.chunk_key_bytes,
        spill_bytes,
        limits.fan_in,
        operations,
    )
}

#[derive(Clone, Copy)]
enum BudgetFailure {
    Operations,
    Spill,
}

fn reject_before_publication(
    path: &Path,
    destination: &Path,
    base: &Path,
    digest: &[u8; 32],
    suffix_operations: u64,
    failure: BudgetFailure,
    context: &str,
) {
    let (operations, spill_bytes, needle) = match failure {
        BudgetFailure::Operations => (suffix_operations - 1, SPILL_BUDGET, "operation limit"),
        BudgetFailure::Spill => (suffix_operations, 1, "spill byte limit"),
    };
    let error = rebuild(
        path,
        destination,
        base,
        digest,
        TIGHT_CHUNK,
        spill_bytes,
        operations,
    )
    .unwrap_err();
    match error {
        ManagedRebuildFailure::NotPublished(inner) => {
            assert_eq!(inner.kind(), ErrorKind::InvalidInput, "{context}: {inner}");
            assert!(inner.to_string().contains(needle), "{context}: {inner}");
        }
        other => panic!("{context}: {other}"),
    }
    assert!(!destination.exists(), "{context}: destination was created");
    assert!(
        !path.join("checkpoint.selection").exists(),
        "{context}: selection was published"
    );
}

fn assert_no_stages(path: &Path, context: &str) {
    let cleaned = cleanup_abandoned_staging(path).unwrap();
    assert_eq!(
        (
            cleaned.removed_markers,
            cleaned.removed_payloads,
            cleaned.skipped
        ),
        (0, 0, 0),
        "{context}: {cleaned:?}"
    );
    for entry in fs::read_dir(path).unwrap() {
        let name = entry.unwrap().file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.starts_with(".emda-stage"),
            "{context}: leftover stage {name}"
        );
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

fn check_seed(seed: u64) {
    let history = generate(seed);
    assert!(
        history.suffix_operations >= 2,
        "seed {seed:#x} suffix is too small to reject an operation budget"
    );
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let base = root.path().join("base");
    let digest = {
        let store = Store::open(&path).unwrap();
        apply_steps(&store, &history.base);
        let digest = store.checkpoint_to(&base).unwrap();
        apply_steps(&store, &history.suffix);
        digest
    };
    let image = snapshot(&path, &base);

    let reference_path = root.path().join("reference");
    let (reference_digest, reference_bytes, reference_live) = {
        let store = retry(|| Store::open(&path)).unwrap();
        let live = live_entries(|after| store.scan(after, None, MAX_SCAN_ENTRIES));
        assert_eq!(
            live, history.expected,
            "seed {seed:#x} full-log scan differs from the model"
        );
        assert_point_reads(
            &history.expected,
            &format!("seed {seed:#x} full-log get"),
            |key| store.get(key),
        );
        let digest = store.checkpoint_to(&reference_path).unwrap();
        (digest, fs::read(&reference_path).unwrap(), live)
    };
    let (count, peak) = header_counts(&reference_bytes);
    assert_eq!(
        count,
        u64::try_from(history.expected.len()).unwrap(),
        "seed {seed:#x} reference live count"
    );
    assert!(
        peak > count,
        "seed {seed:#x} reference peak {peak} is not above live count {count}"
    );
    assert_preserved(
        &path,
        &base,
        &image,
        &format!("seed {seed:#x} after full-log checkpoint"),
    );

    let operation_destination = root.path().join("reject-operations");
    reject_before_publication(
        &path,
        &operation_destination,
        &base,
        &digest,
        history.suffix_operations,
        BudgetFailure::Operations,
        &format!("seed {seed:#x} operation budget"),
    );
    assert_preserved(
        &path,
        &base,
        &image,
        &format!("seed {seed:#x} after operation rejection"),
    );

    let spill_destination = root.path().join("reject-spill");
    reject_before_publication(
        &path,
        &spill_destination,
        &base,
        &digest,
        history.suffix_operations,
        BudgetFailure::Spill,
        &format!("seed {seed:#x} spill budget"),
    );
    assert_preserved(
        &path,
        &base,
        &image,
        &format!("seed {seed:#x} after spill rejection"),
    );
    assert_no_stages(&path, &format!("seed {seed:#x} store after rejection"));
    assert_no_stages(root.path(), &format!("seed {seed:#x} root after rejection"));

    for (index, limits) in SUCCESS_LIMITS.iter().enumerate() {
        let destination = root.path().join(format!("rebuilt-{index}"));
        let context = format!(
            "seed {seed:#x} chunk {} key-bytes {} fan-in {}",
            limits.chunk_keys, limits.chunk_key_bytes, limits.fan_in
        );
        let rebuilt = rebuild(
            &path,
            &destination,
            &base,
            &digest,
            *limits,
            SPILL_BUDGET,
            history.suffix_operations,
        )
        .unwrap_or_else(|error| panic!("{context}: {error}"));
        assert_eq!(rebuilt, reference_digest, "{context} digest");
        assert_same_bytes(&fs::read(&destination).unwrap(), &reference_bytes, &context);
        assert_preserved(&path, &base, &image, &context);
        let rebuilt_store = retry(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            rebuilt_store.overlay_keys().unwrap(),
            0,
            "{context} overlay"
        );
        assert_eq!(rebuilt_store.tail_bytes().unwrap(), 0, "{context} tail");
        let live = live_entries(|after| rebuilt_store.scan(after, None, MAX_SCAN_ENTRIES));
        assert_eq!(live, reference_live, "{context} scan");
        assert_point_reads(&reference_live, &context, |key| rebuilt_store.get(key));
        drop(rebuilt_store);
        assert_preserved(&path, &base, &image, &context);
    }
    assert_no_stages(&path, &format!("seed {seed:#x} store after rebuild"));
    assert_no_stages(root.path(), &format!("seed {seed:#x} root after rebuild"));
}

#[test]
fn seeded_histories_match_full_log_reference_and_reject_tight_budgets() {
    for seed in SEEDS {
        check_seed(seed);
    }
}
