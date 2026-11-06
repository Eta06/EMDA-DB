//! Conservative synthetic persistence states around atomic and grouped commits.
#![cfg(target_os = "linux")]

use database_engine::{BatchOperation as Op, CheckpointStore, Store};
use rustix::fs::{XattrFlags, getxattr, setxattr};
use std::fs;
use std::io;
use std::path::Path;

const METADATA_NAME: &str = "user.db.commit.v3";
const KEYS: &[&[u8]] = &[b"a", b"b", b"c", b"safe"];

#[derive(Clone, Copy, Debug)]
enum Format {
    V2,
    V3,
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    AtomicBatch,
    IndependentRecords,
}

fn marker(path: &Path, format: Format) -> Vec<u8> {
    match format {
        Format::V2 => fs::read(path.join("commit.state")).unwrap(),
        Format::V3 => {
            let mut bytes = [0; 36];
            assert_eq!(
                getxattr(path.join("records.log"), METADATA_NAME, &mut bytes[..]).unwrap(),
                bytes.len()
            );
            bytes.to_vec()
        }
    }
}

fn install(path: &Path, format: Format, log: &[u8], anchor: &[u8], witness: &[u8]) {
    fs::write(path.join("records.log"), log).unwrap();
    match format {
        Format::V2 => fs::write(path.join("commit.state"), witness).unwrap(),
        Format::V3 => {
            fs::write(path.join("commit.state"), anchor).unwrap();
            setxattr(
                path.join("records.log"),
                METADATA_NAME,
                witness,
                XattrFlags::empty(),
            )
            .unwrap();
        }
    }
}

fn expected(step: usize) -> Vec<Option<Vec<u8>>> {
    vec![
        Some(if step == 0 { b"old-a" } else { b"new-a" }.to_vec()),
        (step < 2).then(|| b"old-b".to_vec()),
        (step == 3).then(|| b"new-c".to_vec()),
        Some(b"guard".to_vec()),
    ]
}

fn check_format(format: Format, scenario: Scenario) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    if matches!(format, Format::V2) {
        fs::write(
            source.join("records.log"),
            include_bytes!("fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            source.join("commit.state"),
            include_bytes!("fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let store = Store::open(&source).unwrap();
    store.put(b"a", b"old-a").unwrap();
    store.put(b"b", b"old-b").unwrap();
    store.put(b"safe", b"guard").unwrap();
    let checkpoint = root.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    let old_log = fs::read(source.join("records.log")).unwrap();
    let old_marker = marker(&source, format);
    let anchor = fs::read(source.join("commit.state")).unwrap();
    let mut states = vec![(old_log.len(), old_marker.clone(), expected(0))];
    let mut capture = |step| {
        states.push((
            fs::metadata(source.join("records.log")).unwrap().len() as usize,
            marker(&source, format),
            expected(step),
        ));
    };
    match scenario {
        Scenario::AtomicBatch => {
            store
                .write_batch(&[
                    Op::Put {
                        key: b"a",
                        value: b"new-a",
                    },
                    Op::Delete { key: b"b" },
                    Op::Put {
                        key: b"c",
                        value: b"new-c",
                    },
                ])
                .unwrap();
            capture(3);
        }
        Scenario::IndependentRecords => {
            // Independently submitted records can share one synchronization,
            // but a group is not an atomic multi-key transaction. Capture
            // each possible complete-prefix result for the synthetic model.
            store.put(b"a", b"new-a").unwrap();
            capture(1);
            store.delete(b"b").unwrap();
            capture(2);
            store.put(b"c", b"new-c").unwrap();
            capture(3);
        }
    }
    let new_log = fs::read(source.join("records.log")).unwrap();
    let new_marker = marker(&source, format);
    drop(store);
    assert!(new_log.starts_with(&old_log));
    assert_ne!(old_marker, new_marker);
    assert_eq!(old_marker.len(), 36);
    assert_eq!(new_marker.len(), old_marker.len());

    let candidate = root.path().join("candidate");
    fs::create_dir(&candidate).unwrap();
    for length in old_log.len()..=new_log.len() {
        for split in 0..=old_marker.len() {
            // Model a prefix of the new witness reaching storage with the
            // remaining bytes still equal to the old witness. This is a
            // conservative synthetic model, not a physical power-loss test.
            let mut torn = old_marker.clone();
            torn[..split].copy_from_slice(&new_marker[..split]);
            let state = states.iter().find(|(boundary, _, _)| *boundary == length);
            let should_open = state.is_some()
                && (torn == old_marker || (length == new_log.len() && torn == new_marker));
            for checkpoint_backed in [false, true] {
                install(&candidate, format, &new_log[..length], &anchor, &torn);
                let observed: io::Result<Vec<Option<Vec<u8>>>> = if checkpoint_backed {
                    CheckpointStore::open(&candidate, &checkpoint, &digest)
                        .and_then(|opened| opened.get_many(KEYS))
                } else {
                    Store::open(&candidate).and_then(|opened| opened.get_many(KEYS))
                };
                let context = format!(
                    "{format:?}, {scenario:?}, log length {length}, witness split {split}, checkpoint {checkpoint_backed}"
                );
                if should_open {
                    let (_, expected_marker, expected_values) = state.unwrap();
                    assert_eq!(
                        observed.unwrap_or_else(|e| panic!("{context}: {e}")),
                        *expected_values,
                        "{context}"
                    );
                    assert_eq!(marker(&candidate, format), *expected_marker, "{context}");
                } else {
                    assert!(
                        matches!(observed, Err(ref e) if e.kind() == io::ErrorKind::InvalidData),
                        "{context}: {observed:?}"
                    );
                    assert_eq!(marker(&candidate, format), torn, "{context}");
                }
                assert_eq!(
                    fs::read(candidate.join("records.log")).unwrap(),
                    new_log[..length],
                    "{context}"
                );
                if matches!(format, Format::V3) {
                    assert_eq!(
                        fs::read(candidate.join("commit.state")).unwrap(),
                        anchor,
                        "{context}"
                    );
                }
            }
        }
    }
    println!(
        "{format:?} {scenario:?}: {} synthetic reopen cases",
        (new_log.len() - old_log.len() + 1) * (old_marker.len() + 1) * 2
    );
}

#[test]
fn v2_atomic_batch_persisted_prefix_and_torn_witness_matrix() {
    check_format(Format::V2, Scenario::AtomicBatch);
}

#[test]
fn v3_atomic_batch_persisted_prefix_and_torn_witness_matrix() {
    check_format(Format::V3, Scenario::AtomicBatch);
}

#[test]
fn v2_independent_record_persisted_prefix_and_torn_witness_matrix() {
    check_format(Format::V2, Scenario::IndependentRecords);
}

#[test]
fn v3_independent_record_persisted_prefix_and_torn_witness_matrix() {
    check_format(Format::V3, Scenario::IndependentRecords);
}
