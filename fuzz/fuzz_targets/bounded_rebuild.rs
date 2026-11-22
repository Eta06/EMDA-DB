//! Differential fuzz target for bounded managed-checkpoint rebuild.
//!
//! Each input is a short put/delete history. A prefix is sealed into a base
//! checkpoint and the remainder is a witnessed suffix. Batch roles become
//! ordered atomic-batch children. The target rebuilds that suffix with a tiny
//! chunk and with the input's chunk and fan-in, then compares the published
//! checkpoint bytes, digest, and reopened live state with a full-log
//! checkpoint of the same history.
//!
//! Checkpoint bytes are compared with full replay. A separate generator model
//! checks final values, historical key-count peak, and value locations from
//! the generated record sizes, independently of the shared replay parser.
//!
//! When the suffix has at least one child, an operation budget one below that
//! count must stop before publication. The probe keeps every one-byte key in
//! one chunk, so the rejection is the operation budget. When the suffix names
//! at least two distinct keys, a one-key chunk has to spill before the second
//! key and a one-byte spill budget must also stop before publication. A
//! history that never opens a run keeps that one-byte budget and must still
//! match the reference. Either failure leaves no destination and no selection.
//! The log and its applicable witness stay unchanged: the history is fully
//! witnessed before the first rebuild, so these calls do not adopt a tail.
//!
//! Inputs longer than 48 bytes are discarded. At most 12 children are applied.
//! The high bit of the second byte instead selects full-log rebuild, with
//! variable-length keys. Bit 6 rolls back the witness to the base boundary
//! before rebuilding. Bits 3–5 select a one-under admission test. Every
//! value is at most three bytes.

#![no_main]

use database_engine::{
    BatchOperation, BoundedFullRebuildLimits, CheckpointStore, CheckpointViewLimits, Entry,
    MAX_SCAN_ENTRIES, ManagedRebuildFailure, Store, cleanup_abandoned_staging,
};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::thread;
use std::time::Duration;

const MAX_INPUT: usize = 48;
const MAX_CHILDREN: usize = 12;
const KEY_SPACE: usize = 8;
const SPILL_BUDGET: u64 = 1 << 20;
const TIGHT_SPILL: u64 = 1;
const LOCK_ATTEMPTS: usize = 40;

/// One-key chunk. A second distinct suffix key has to open a spill run.
const TINY: Limits = Limits {
    chunk_keys: 1,
    chunk_key_bytes: 1,
    fan_in: 2,
};

/// Holds every key this generator can name without opening a spill run.
const FIT: Limits = Limits {
    chunk_keys: KEY_SPACE,
    chunk_key_bytes: KEY_SPACE,
    fan_in: 2,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Limits {
    chunk_keys: usize,
    chunk_key_bytes: usize,
    fan_in: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Lone,
    Start,
    Continue,
    End,
}

struct Child {
    key: u8,
    delete: bool,
    role: Role,
    value: Vec<u8>,
}

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

struct Image {
    log: Vec<u8>,
    witness: Vec<u8>,
    anchor: Vec<u8>,
    base: Vec<u8>,
}

struct FullLog {
    live: BTreeMap<Vec<u8>, Vec<u8>>,
    digest: [u8; 32],
    bytes: Vec<u8>,
}

struct Oracle<'a> {
    input: &'a str,
    path: &'a Path,
    root: &'a Path,
    base: &'a Path,
    digest: &'a [u8; 32],
    image: &'a Image,
    live: &'a BTreeMap<Vec<u8>, Vec<u8>>,
    reference_digest: [u8; 32],
    reference_bytes: &'a [u8],
    variable_keys: bool,
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > MAX_INPUT {
        return;
    }
    exercise(data);
});

fn exercise(data: &[u8]) {
    let input = hex(data);
    let limits = limits_from(data[0]);
    let v2 = data[0] & 1 == 1;
    let options = data.get(1).copied().unwrap_or(0);
    let full = options & 0x80 != 0;
    let mut children = parse(data.get(2..).unwrap_or_default());
    let base_len = usize::from(options & 0b111).min(children.len());
    let suffix_children = children.split_off(base_len);
    let base_steps = group(children);
    let suffix_steps = group(suffix_children);
    let mut present = [false; KEY_SPACE];
    let mut peak = 0_usize;
    let mut peak_bytes = 0_u64;
    for child in base_steps
        .iter()
        .chain(&suffix_steps)
        .flat_map(Step::children)
    {
        present[usize::from(child.key)] = !child.delete;
        peak = peak.max(present.iter().filter(|value| **value).count());
        let live_bytes = present
            .iter()
            .enumerate()
            .filter(|&(_, live)| *live)
            .map(|(key, _)| key_len(key as u8, full) as u64)
            .sum();
        peak_bytes = peak_bytes.max(live_bytes);
    }

    let root = tempfile::tempdir().expect("temporary directory");
    let path = root.path().join("store");
    let base = root.path().join("base");
    let mut model = BTreeMap::new();
    let (digest, old_witness) = {
        let store = open_writer(&path, v2, &input);
        apply(&store, &base_steps, &mut model, full);
        let digest = store
            .checkpoint_to(&base)
            .unwrap_or_else(|error| panic!("{input}: base checkpoint: {error}"));
        let old_witness = if full && options & 0x40 != 0 && !suffix_steps.is_empty() {
            let log = fs::read(path.join("records.log")).unwrap();
            Some(witness_bytes(&path, &log, &input))
        } else {
            None
        };
        apply(&store, &suffix_steps, &mut model, full);
        (digest, old_witness)
    };
    let image = snapshot(&path, &base, &input);
    let reference_path = root.path().join("reference");
    let reference = full_log_reference(&path, &reference_path, &model, full, &input);
    assert_eq!(&reference.bytes[..8], b"EMDAIDX1", "{input}");
    let count = u64::from_le_bytes(reference.bytes[16..24].try_into().unwrap());
    let stored_peak = u64::from_le_bytes(reference.bytes[24..32].try_into().unwrap());
    assert_eq!(count, model.len() as u64, "{input}: model live count");
    assert_eq!(stored_peak, peak as u64, "{input}: model historical peak");
    check_generated_locations(
        &base_steps,
        &suffix_steps,
        &reference.bytes,
        &image.log,
        full,
        &input,
    );
    assert_preserved(
        &path,
        &base,
        &image,
        &format!("{input}: after full-log checkpoint"),
    );

    let suffix_ops = operation_count(&suffix_steps);
    let distinct = distinct_keys(&suffix_steps);
    assert!(
        distinct <= KEY_SPACE,
        "{input}: suffix names {distinct} keys"
    );
    let oracle = Oracle {
        input: &input,
        path: &path,
        root: root.path(),
        base: &base,
        digest: &digest,
        image: &image,
        live: &reference.live,
        reference_digest: reference.digest,
        reference_bytes: &reference.bytes,
        variable_keys: full,
    };
    if full {
        let limits = BoundedFullRebuildLimits {
            max_log_bytes: image.log.len() as u64,
            max_operations: operation_count(&base_steps) + suffix_ops,
            max_live_keys: peak,
            max_live_key_bytes: peak_bytes,
            max_chunk_keys: limits.chunk_keys,
            max_chunk_key_bytes: limits.chunk_key_bytes * 1024,
            max_spill_bytes: SPILL_BUDGET,
            max_merge_fan_in: limits.fan_in,
        };
        let checkpoint_limits = CheckpointViewLimits {
            max_file_bytes: reference.bytes.len() as u64,
            // Eight possible keys of at most 1024 bytes fit one view block.
            max_blocks: usize::from(!model.is_empty()),
        };
        oracle.rebuild_full(limits, checkpoint_limits, old_witness, (options >> 3) & 7);
        return;
    }

    if suffix_ops >= 1 {
        oracle.reject(
            &root.path().join("reject-operations"),
            FIT,
            SPILL_BUDGET,
            suffix_ops - 1,
            "operation limit",
        );
    }
    if distinct >= 2 {
        oracle.reject(
            &root.path().join("reject-spill"),
            TINY,
            TIGHT_SPILL,
            suffix_ops,
            "spill byte limit",
        );
    }

    // Exactly two distinct one-byte keys produce two 92-byte runs. Their
    // merge holds both inputs while writing a 124-byte output and its 16-byte
    // offset sidecar: the peak is 324 bytes. Exercise both sides of that
    // boundary, including the temporary bytes that outlive an unlink.
    if suffix_ops == 2 && distinct == 2 {
        oracle.reject(
            &root.path().join("reject-merge-peak"),
            TINY,
            323,
            suffix_ops,
            "spill byte limit",
        );
        oracle.publish_detailed(
            &root.path().join("rebuilt-merge-peak"),
            TINY,
            324,
            suffix_ops,
        );
    }

    oracle.publish_detailed(
        &root.path().join("rebuilt-selected"),
        limits,
        SPILL_BUDGET,
        suffix_ops,
    );
    let tiny_spill = if distinct >= 2 {
        SPILL_BUDGET
    } else {
        TIGHT_SPILL
    };
    if limits != TINY || tiny_spill != SPILL_BUDGET {
        oracle.publish(
            &root.path().join("rebuilt-tiny"),
            TINY,
            tiny_spill,
            suffix_ops,
        );
    } else {
        oracle.publish(
            &root.path().join("rebuilt-plain"),
            limits,
            SPILL_BUDGET,
            suffix_ops,
        );
    }
    oracle.no_stages("after rebuild");
}

fn limits_from(flags: u8) -> Limits {
    const CHUNK_KEYS: [usize; 8] = [1, 2, 3, 4, 8, 1, 2, 3];
    const CHUNK_BYTES: [usize; 4] = [1, 2, 4, 8];
    const FAN_IN: [usize; 4] = [2, 3, 4, 8];
    Limits {
        chunk_keys: CHUNK_KEYS[usize::from((flags >> 3) & 0b111)],
        chunk_key_bytes: CHUNK_BYTES[usize::from((flags >> 6) & 0b11)],
        fan_in: FAN_IN[usize::from((flags >> 1) & 0b11)],
    }
}

fn parse(bytes: &[u8]) -> Vec<Child> {
    let mut children = Vec::new();
    let mut ordinal = 0_u16;
    for byte in bytes.iter().copied().take(MAX_CHILDREN) {
        let key = byte & 0b111;
        let delete = byte & 0b0000_1000 != 0;
        let role = match (byte >> 4) & 0b11 {
            0 => Role::Lone,
            1 => Role::Start,
            2 => Role::Continue,
            _ => Role::End,
        };
        let value = if delete {
            Vec::new()
        } else {
            ordinal += 1;
            value_of(byte >> 6, ordinal, key)
        };
        children.push(Child {
            key,
            delete,
            role,
            value,
        });
    }
    children
}

fn value_of(tag: u8, ordinal: u16, key: u8) -> Vec<u8> {
    let raw = ordinal.to_le_bytes();
    match tag {
        0 => Vec::new(),
        1 => vec![raw[0]],
        2 => vec![raw[0], raw[1]],
        _ => vec![raw[0], raw[1], key.wrapping_add(1)],
    }
}

fn group(children: Vec<Child>) -> Vec<Step> {
    let mut steps = Vec::new();
    let mut batch = Vec::new();
    for child in children {
        let continues = matches!(child.role, Role::Continue | Role::End) && !batch.is_empty();
        if continues {
            let end = child.role == Role::End;
            batch.push(child);
            if end {
                steps.push(Step::Batch(std::mem::take(&mut batch)));
            }
            continue;
        }
        if !batch.is_empty() {
            steps.push(Step::Batch(std::mem::take(&mut batch)));
        }
        if child.role == Role::Start {
            batch.push(child);
        } else {
            steps.push(Step::One(child));
        }
    }
    if !batch.is_empty() {
        steps.push(Step::Batch(batch));
    }
    steps
}

fn operation_count(steps: &[Step]) -> u64 {
    steps
        .iter()
        .map(|step| u64::try_from(step.children().len()).expect("child count"))
        .sum()
}

fn distinct_keys(steps: &[Step]) -> usize {
    let mut seen = [false; KEY_SPACE];
    for step in steps {
        for child in step.children() {
            seen[usize::from(child.key)] = true;
        }
    }
    seen.into_iter().filter(|present| *present).count()
}

fn key_len(key: u8, variable_keys: bool) -> usize {
    const LENGTHS: [usize; KEY_SPACE] = [1, 2, 3, 4, 7, 15, 31, 1024];
    if variable_keys {
        LENGTHS[usize::from(key)]
    } else {
        1
    }
}

fn key_bytes(key: u8, variable_keys: bool) -> Vec<u8> {
    assert!(usize::from(key) < KEY_SPACE, "key id {key}");
    vec![b'a' + key; key_len(key, variable_keys)]
}

// Model DB01/DB02 lengths from the generated operations, rather than using
// either engine's record decoder. Checkpoint entries carry physical offsets.
fn check_generated_locations(
    base: &[Step],
    suffix: &[Step],
    checkpoint: &[u8],
    log: &[u8],
    variable_keys: bool,
    input: &str,
) {
    let mut locations = BTreeMap::<Vec<u8>, (u64, u32, u32)>::new();
    let mut offset = 28_u64; // witnessed log header
    for step in base.iter().chain(suffix) {
        let mut child_offset = match step {
            Step::One(_) => offset,
            Step::Batch(_) => offset + 17 + 1 + 4, // outer header, key, count
        };
        for child in step.children() {
            let key = key_bytes(child.key, variable_keys);
            let key_len = key.len() as u64;
            let value_len = if child.delete { 0 } else { child.value.len() };
            if child.delete {
                locations.remove(&key);
            } else {
                locations.insert(
                    key,
                    (
                        child_offset + 17 + key_len,
                        value_len as u32,
                        crc32fast::hash(&child.value),
                    ),
                );
            }
            child_offset += 17 + key_len + value_len as u64 + 4;
        }
        offset = match step {
            Step::One(_) => child_offset,
            Step::Batch(_) => child_offset + 4, // outer checksum
        };
    }
    assert_eq!(offset, log.len() as u64, "{input}: generated log length");
    let mut cursor = 68_usize; // EMDAIDX1 fixed header
    for (key, (expected_offset, expected_len, expected_crc)) in locations {
        let key_len =
            u16::from_le_bytes(checkpoint[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;
        assert_eq!(
            &checkpoint[cursor..cursor + key_len],
            key,
            "{input}: checkpoint key"
        );
        cursor += key_len;
        let actual_offset = u64::from_le_bytes(checkpoint[cursor..cursor + 8].try_into().unwrap());
        let actual_len =
            u32::from_le_bytes(checkpoint[cursor + 8..cursor + 12].try_into().unwrap());
        let actual_crc =
            u32::from_le_bytes(checkpoint[cursor + 12..cursor + 16].try_into().unwrap());
        assert_eq!(
            (actual_offset, actual_len, actual_crc),
            (expected_offset, expected_len, expected_crc),
            "{input}: generated value location"
        );
        cursor += 16;
    }
    assert_eq!(
        cursor + 32,
        checkpoint.len(),
        "{input}: checkpoint entry extent"
    );
}

fn apply(
    store: &Store,
    steps: &[Step],
    model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    variable_keys: bool,
) {
    for step in steps {
        match step {
            Step::One(child) => {
                apply_child(store, child, variable_keys);
                observe(model, child, variable_keys);
            }
            Step::Batch(children) => {
                apply_batch(store, children, variable_keys);
                for child in children {
                    observe(model, child, variable_keys);
                }
            }
        }
    }
}

fn apply_child(store: &Store, child: &Child, variable_keys: bool) {
    let key = key_bytes(child.key, variable_keys);
    let result = if child.delete {
        store.delete(&key)
    } else {
        store.put(&key, &child.value)
    };
    result.unwrap_or_else(|error| panic!("key {} delete {}: {error}", child.key, child.delete));
}

fn apply_batch(store: &Store, children: &[Child], variable_keys: bool) {
    assert!(!children.is_empty(), "batch has no children");
    let keys: Vec<_> = children
        .iter()
        .map(|child| key_bytes(child.key, variable_keys))
        .collect();
    let operations: Vec<BatchOperation<'_>> = children
        .iter()
        .enumerate()
        .map(|(index, child)| {
            if child.delete {
                BatchOperation::Delete { key: &keys[index] }
            } else {
                BatchOperation::Put {
                    key: &keys[index],
                    value: &child.value,
                }
            }
        })
        .collect();
    store
        .write_batch(&operations)
        .unwrap_or_else(|error| panic!("batch of {}: {error}", children.len()));
}

fn observe(state: &mut BTreeMap<Vec<u8>, Vec<u8>>, child: &Child, variable_keys: bool) {
    let key = key_bytes(child.key, variable_keys);
    if child.delete {
        state.remove(&key);
    } else {
        state.insert(key, child.value.clone());
    }
}

fn open_writer(path: &Path, v2: bool, input: &str) -> Store {
    if v2 {
        fs::create_dir(path).unwrap_or_else(|error| panic!("{input}: create store: {error}"));
        fs::write(
            path.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .unwrap_or_else(|error| panic!("{input}: write v2 log: {error}"));
        fs::write(
            path.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .unwrap_or_else(|error| panic!("{input}: write v2 witness: {error}"));
    }
    Store::open(path).unwrap_or_else(|error| panic!("{input}: open store: {error}"))
}

fn full_log_reference(
    path: &Path,
    destination: &Path,
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    variable_keys: bool,
    input: &str,
) -> FullLog {
    let store = retry(|| Store::open(path))
        .unwrap_or_else(|error| panic!("{input}: reopen full log: {error}"));
    let live = scan_all(
        |after| store.scan(after, None, MAX_SCAN_ENTRIES),
        &format!("{input}: full log"),
    );
    assert_eq!(
        &live, model,
        "{input}: generator final state differs from the full-log scan"
    );
    assert_reads(
        &live,
        variable_keys,
        |key| store.get(key),
        &format!("{input}: full log"),
    );
    let digest = store
        .checkpoint_to(destination)
        .unwrap_or_else(|error| panic!("{input}: full-log checkpoint: {error}"));
    let bytes = fs::read(destination)
        .unwrap_or_else(|error| panic!("{input}: read full-log checkpoint: {error}"));
    drop(store);
    FullLog {
        live,
        digest,
        bytes,
    }
}

impl Oracle<'_> {
    fn rebuild_full(
        &self,
        limits: BoundedFullRebuildLimits,
        checkpoint_limits: CheckpointViewLimits,
        old_witness: Option<Vec<u8>>,
        cap: u8,
    ) {
        let previous = self.root.join("missing-selected");
        CheckpointStore::rebuild_managed_selection(self.path, &previous).unwrap();
        let selection = self.path.join("checkpoint.selection");
        let selection_before = fs::read(&selection).unwrap();
        fs::remove_file(&previous).unwrap();

        if let Some(witness) = old_witness {
            assert_ne!(
                witness, self.image.witness,
                "{}: witness did not advance",
                self.input
            );
            restore_witness(self.path, &self.image.log, &witness).unwrap();
        }
        let before = snapshot(self.path, self.base, self.input);
        let destination = self.root.join("full-rebuilt");
        let attempt = |limits, checkpoint_limits| {
            CheckpointStore::rebuild_managed_selection_full_bounded_with_checkpoint_limits(
                self.path,
                &destination,
                limits,
                checkpoint_limits,
            )
        };

        let mut tight = limits;
        let mut tight_output = checkpoint_limits;
        let expected = match cap {
            0 if tight.max_operations > 0 => {
                tight.max_operations -= 1;
                Some("operation limit")
            }
            1 => {
                tight.max_log_bytes -= 1;
                Some("log exceeds configured limit")
            }
            2 if tight.max_live_keys > 0 => {
                tight.max_live_keys -= 1;
                Some("historical live key limit")
            }
            3 if tight.max_live_key_bytes > 0 => {
                tight.max_live_key_bytes -= 1;
                Some("historical live key-byte limit")
            }
            4 => {
                tight_output.max_file_bytes -= 1;
                Some("file size limit")
            }
            5 if tight_output.max_blocks > 0 => {
                tight_output.max_blocks -= 1;
                Some("too many view blocks")
            }
            _ => None,
        };
        if let Some(expected) = expected {
            let result = attempt(tight, tight_output);
            let Err(ManagedRebuildFailure::NotPublished(error)) = result else {
                panic!("{}: expected {expected}: {result:?}", self.input);
            };
            assert_eq!(
                error.kind(),
                ErrorKind::InvalidInput,
                "{}: {error}",
                self.input
            );
            assert!(
                error.to_string().contains(expected),
                "{}: {error}",
                self.input
            );
            assert_eq!(
                fs::read(&selection).unwrap(),
                selection_before,
                "{}",
                self.input
            );
            assert!(!destination.exists(), "{}", self.input);
            assert_preserved(self.path, self.base, &before, self.input);
            self.no_stages(self.input);
        }
        let digest = attempt(limits, checkpoint_limits).unwrap_or_else(|error| {
            panic!("{}: full rebuild at exact limits: {error}", self.input)
        });
        self.check_published(&destination, digest, self.input);
    }

    fn reject(
        &self,
        destination: &Path,
        limits: Limits,
        spill_bytes: u64,
        operations: u64,
        needle: &str,
    ) {
        let context = self.label("reject", limits, spill_bytes, operations);
        match rebuild_detailed(
            self.path,
            destination,
            self.base,
            self.digest,
            limits,
            spill_bytes,
            operations,
        ) {
            Err(ManagedRebuildFailure::NotPublished(error)) => {
                assert_eq!(error.kind(), ErrorKind::InvalidInput, "{context}: {error}");
                assert!(error.to_string().contains(needle), "{context}: {error}");
            }
            Ok(digest) => panic!("{context}: published {}", hex(&digest)),
            Err(error) => panic!("{context}: {error}"),
        }
        assert!(!destination.exists(), "{context}: destination was created");
        assert!(
            !self.path.join("checkpoint.selection").exists(),
            "{context}: selection was published"
        );
        assert_preserved(self.path, self.base, self.image, &context);
        self.no_stages(&context);
    }

    fn publish_detailed(
        &self,
        destination: &Path,
        limits: Limits,
        spill_bytes: u64,
        operations: u64,
    ) {
        let context = self.label("detailed", limits, spill_bytes, operations);
        let rebuilt = rebuild_detailed(
            self.path,
            destination,
            self.base,
            self.digest,
            limits,
            spill_bytes,
            operations,
        )
        .unwrap_or_else(|error| panic!("{context}: {error}"));
        self.check_published(destination, rebuilt, &context);
    }

    fn publish(&self, destination: &Path, limits: Limits, spill_bytes: u64, operations: u64) {
        let context = self.label("rebuild", limits, spill_bytes, operations);
        let rebuilt = rebuild(
            self.path,
            destination,
            self.base,
            self.digest,
            limits,
            spill_bytes,
            operations,
        )
        .unwrap_or_else(|error| panic!("{context}: {error}"));
        self.check_published(destination, rebuilt, &context);
    }

    fn check_published(&self, destination: &Path, rebuilt: [u8; 32], context: &str) {
        assert_eq!(rebuilt, self.reference_digest, "{context} digest");
        let bytes = fs::read(destination)
            .unwrap_or_else(|error| panic!("{context}: read rebuilt checkpoint: {error}"));
        assert_same_bytes(&bytes, self.reference_bytes, context);
        let selection = fs::read(self.path.join("checkpoint.selection"))
            .unwrap_or_else(|error| panic!("{context}: read selection: {error}"));
        assert_eq!(&selection[..8], b"EMDASEL1", "{context}");
        let path_len = u32::from_le_bytes(selection[8..12].try_into().unwrap()) as usize;
        assert_eq!(selection.len(), 12 + path_len + 64, "{context}");
        assert_eq!(
            &selection[12..12 + path_len],
            fs::canonicalize(destination)
                .unwrap()
                .as_os_str()
                .as_bytes(),
            "{context}: selected checkpoint"
        );
        assert_eq!(
            &selection[12 + path_len..12 + path_len + 32],
            &rebuilt,
            "{context}: selected digest"
        );
        assert_preserved(self.path, self.base, self.image, context);
        let store = retry(|| CheckpointStore::open_managed(self.path))
            .unwrap_or_else(|error| panic!("{context}: reopen managed store: {error}"));
        assert_eq!(
            store
                .overlay_keys()
                .unwrap_or_else(|error| panic!("{context}: overlay: {error}")),
            0,
            "{context} overlay"
        );
        assert_eq!(
            store
                .tail_bytes()
                .unwrap_or_else(|error| panic!("{context}: tail: {error}")),
            0,
            "{context} tail"
        );
        let scanned = scan_all(|after| store.scan(after, None, MAX_SCAN_ENTRIES), context);
        assert_eq!(scanned, *self.live, "{context} scan");
        assert_reads(self.live, self.variable_keys, |key| store.get(key), context);
        drop(store);
        assert_preserved(self.path, self.base, self.image, context);
        self.no_stages(context);
    }

    fn no_stages(&self, context: &str) {
        assert_no_stages(self.path, &format!("{} {context}", self.input));
        assert_no_stages(self.root, &format!("{} {context}", self.input));
    }

    fn label(&self, kind: &str, limits: Limits, spill_bytes: u64, operations: u64) -> String {
        format!(
            "{} {kind} chunk {} key-bytes {} fan-in {} spill {spill_bytes} ops {operations}",
            self.input, limits.chunk_keys, limits.chunk_key_bytes, limits.fan_in
        )
    }
}

fn rebuild_detailed(
    path: &Path,
    destination: &Path,
    base: &Path,
    digest: &[u8; 32],
    limits: Limits,
    spill_bytes: u64,
    operations: u64,
) -> Result<[u8; 32], ManagedRebuildFailure> {
    let mut blocked = None;
    for _ in 0..LOCK_ATTEMPTS {
        match CheckpointStore::rebuild_managed_selection_bounded_detailed(
            path,
            destination,
            base,
            digest,
            limits.chunk_keys,
            limits.chunk_key_bytes,
            spill_bytes,
            limits.fan_in,
            operations,
        ) {
            Err(ManagedRebuildFailure::NotPublished(error))
                if error.kind() == ErrorKind::WouldBlock =>
            {
                blocked = Some(error);
                thread::sleep(Duration::from_millis(2));
            }
            other => return other,
        }
    }
    Err(ManagedRebuildFailure::NotPublished(
        blocked.expect("retry observed a lock conflict"),
    ))
}

fn rebuild(
    path: &Path,
    destination: &Path,
    base: &Path,
    digest: &[u8; 32],
    limits: Limits,
    spill_bytes: u64,
    operations: u64,
) -> io::Result<[u8; 32]> {
    let mut blocked = None;
    for _ in 0..LOCK_ATTEMPTS {
        match CheckpointStore::rebuild_managed_selection_bounded(
            path,
            destination,
            base,
            digest,
            limits.chunk_keys,
            limits.chunk_key_bytes,
            spill_bytes,
            limits.fan_in,
            operations,
        ) {
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                blocked = Some(error);
                thread::sleep(Duration::from_millis(2));
            }
            other => return other,
        }
    }
    Err(blocked.expect("retry observed a lock conflict"))
}

fn retry<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let mut last = None;
    for _ in 0..LOCK_ATTEMPTS {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                last = Some(error);
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error),
        }
    }
    Err(last.expect("retry observed a lock conflict"))
}

fn snapshot(path: &Path, base: &Path, input: &str) -> Image {
    let log = fs::read(path.join("records.log"))
        .unwrap_or_else(|error| panic!("{input}: read log: {error}"));
    Image {
        witness: witness_bytes(path, &log, input),
        anchor: fs::read(path.join("commit.state"))
            .unwrap_or_else(|error| panic!("{input}: read witness anchor: {error}")),
        log,
        base: fs::read(base).unwrap_or_else(|error| panic!("{input}: read base: {error}")),
    }
}

fn witness_bytes(path: &Path, log: &[u8], input: &str) -> Vec<u8> {
    // V3 stores the mutable boundary in the log xattr. Every other log keeps
    // it in commit.state. V3 metadata is initialized only on Linux.
    if log.starts_with(b"EMDALOG3") {
        let file = File::open(path.join("records.log"))
            .unwrap_or_else(|error| panic!("{input}: open log witness: {error}"));
        let mut buffer = vec![0_u8; 128];
        let count = rustix::fs::fgetxattr(&file, "user.db.commit.v3", &mut buffer)
            .unwrap_or_else(|error| panic!("{input}: read v3 witness: {error}"));
        buffer[..count].to_vec()
    } else {
        fs::read(path.join("commit.state"))
            .unwrap_or_else(|error| panic!("{input}: read witness: {error}"))
    }
}

fn restore_witness(path: &Path, log: &[u8], witness: &[u8]) -> io::Result<()> {
    if log.starts_with(b"EMDALOG3") {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path.join("records.log"))?;
        rustix::fs::fsetxattr(
            &file,
            "user.db.commit.v3",
            witness,
            rustix::fs::XattrFlags::REPLACE,
        )?;
        file.sync_all()
    } else {
        let file = path.join("commit.state");
        fs::write(&file, witness)?;
        File::open(file)?.sync_all()
    }
}

fn assert_preserved(path: &Path, base: &Path, image: &Image, context: &str) {
    let log = fs::read(path.join("records.log"))
        .unwrap_or_else(|error| panic!("{context}: read log: {error}"));
    assert_eq!(log, image.log, "{context}: log bytes changed");
    assert_eq!(
        witness_bytes(path, &log, context),
        image.witness,
        "{context}: witness changed"
    );
    assert_eq!(
        fs::read(path.join("commit.state")).unwrap(),
        image.anchor,
        "{context}: witness anchor changed"
    );
    assert_eq!(
        fs::read(base).unwrap_or_else(|error| panic!("{context}: read base: {error}")),
        image.base,
        "{context}: base changed"
    );
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

fn assert_reads(
    live: &BTreeMap<Vec<u8>, Vec<u8>>,
    variable_keys: bool,
    mut get: impl FnMut(&[u8]) -> io::Result<Option<Vec<u8>>>,
    context: &str,
) {
    for id in 0..u8::try_from(KEY_SPACE).expect("key space") {
        let key = key_bytes(id, variable_keys);
        let found = get(&key).unwrap_or_else(|error| panic!("{context}: get {id}: {error}"));
        assert_eq!(
            found.as_deref(),
            live.get(key.as_slice()).map(Vec::as_slice),
            "{context} key {id}"
        );
    }
}

fn scan_all(
    mut scan: impl FnMut(Option<&[u8]>) -> io::Result<Vec<Entry>>,
    context: &str,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut entries = BTreeMap::new();
    let mut after = None;
    loop {
        let page =
            scan(after.as_deref()).unwrap_or_else(|error| panic!("{context}: scan: {error}"));
        if page.is_empty() {
            break;
        }
        let last = page.last().expect("page is not empty").0.clone();
        let full = page.len() == MAX_SCAN_ENTRIES;
        for (key, value) in page {
            assert!(
                entries.insert(key, value).is_none(),
                "{context}: scan repeated a key"
            );
        }
        if !full {
            break;
        }
        after = Some(last);
    }
    entries
}

fn assert_no_stages(path: &Path, context: &str) {
    let cleaned = cleanup_abandoned_staging(path)
        .unwrap_or_else(|error| panic!("{context}: cleanup {}: {error}", path.display()));
    assert_eq!(
        (
            cleaned.removed_markers,
            cleaned.removed_payloads,
            cleaned.skipped
        ),
        (0, 0, 0),
        "{context}: {cleaned:?}"
    );
    for entry in fs::read_dir(path)
        .unwrap_or_else(|error| panic!("{context}: read {}: {error}", path.display()))
    {
        let name = entry
            .unwrap_or_else(|error| panic!("{context}: directory entry: {error}"))
            .file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.starts_with(".emda-stage"),
            "{context}: leftover stage {name}"
        );
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
