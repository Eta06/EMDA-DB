use super::*;

#[test]
fn persisted_limit_fields_round_trip_and_each_requires_its_own_ceiling() {
    let distinct = RetainedStoreLimits {
        base: Limits {
            file_bytes: 21,
            entries: 22,
            key_bytes: 23,
            work_bytes: 24,
        },
        index: IndexLimits {
            blocks: 25,
            anchor_bytes: 26,
        },
        suffix: SuffixLimits {
            bytes: 27,
            operations: 28,
            work: 29,
        },
        overlay: OverlayLimits {
            entries: 30,
            key_bytes: 31,
        },
    };
    let mut header = Header::for_role(GenerationRole::Managed([1; 16]));
    header.fill_limits(distinct);
    let opened = header
        .recovery_limits(RecoveryLimits::Ceiling(distinct))
        .unwrap();
    assert_eq!(opened.fields(), distinct.fields());

    for (index, value) in distinct.fields().into_iter().enumerate() {
        let mut changed = Header {
            bytes: header.bytes,
            len: header.len,
        };
        let offset = LIMITS_OFFSET + index * 8;
        changed.bytes[offset..offset + 8].copy_from_slice(&(value + 1).to_le_bytes());
        assert_eq!(
            changed
                .recovery_limits(RecoveryLimits::Ceiling(distinct))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput,
        );
    }
}
use crate::{RetainedExportLimits, Store};
use std::{fs, path::PathBuf};

const LIMITS: RetainedStoreLimits = RetainedStoreLimits {
    base: Limits {
        file_bytes: 4096,
        entries: 10,
        key_bytes: 1024,
        work_bytes: 4097,
    },
    index: IndexLimits {
        blocks: 1,
        anchor_bytes: 4,
    },
    suffix: SuffixLimits {
        bytes: 4096,
        operations: 16,
        work: 256,
    },
    overlay: OverlayLimits {
        entries: 8,
        key_bytes: 64,
    },
};

struct Fixture {
    root: tempfile::TempDir,
    path: PathBuf,
    expected: Expected,
}

impl Fixture {
    fn new(limits: RetainedStoreLimits) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("live");
        let candidate = root.path().join("candidate");
        let source = Store::open(root.path().join("source")).unwrap();
        source.put(b"a", b"base").unwrap();
        source.put(b"z", b"kept").unwrap();
        let expected = source
            .build_retained_candidate(
                &candidate,
                [6; 16],
                [7; 16],
                RetainedExportLimits {
                    max_log_bytes: 4096,
                    max_live_keys: 16,
                    max_live_key_bytes: 64,
                    max_output_bytes: 4096,
                },
            )
            .unwrap();
        RetainedStore::create(&candidate, &path, limits, &expected).unwrap();
        Self {
            root,
            path,
            expected,
        }
    }

    fn open(&self, limits: RetainedStoreLimits) -> io::Result<RetainedStore> {
        RetainedStore::open(&self.path, limits, &self.expected)
    }

    fn log(&self) -> PathBuf {
        self.path.join("records.log")
    }

    fn witness(&self) -> PathBuf {
        self.path.join("commit.state")
    }

    fn append_unacknowledged(&self, bytes: &[u8]) {
        let mut file = OpenOptions::new().append(true).open(self.log()).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_data().unwrap();
    }
}

#[test]
fn disk_suffix_reads_authenticate_selected_batch_values() {
    let limits = RetainedStoreLimits {
        suffix: SuffixLimits {
            bytes: 256 * 1024,
            ..LIMITS.suffix
        },
        ..LIMITS
    };
    let f = Fixture::new(limits);
    let value = vec![0x78; 70 * 1024];
    let altered = vec![0x79; value.len()];
    let mut store = f.open(limits).unwrap();
    store.put(b"b", b"prefix").unwrap();
    let offset = store.header.len as u64 + store.suffix_len as u64;
    let batch = |value| {
        [
            BatchOperation::Put {
                key: b"d",
                value: b"overwritten",
            },
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put { key: b"d", value },
            BatchOperation::Put {
                key: b"x",
                value: b"",
            },
        ]
    };
    store.write_batch(&batch(&value)).unwrap();
    assert_eq!(store.get(b"d", value.len()).unwrap(), Some(value.clone()));
    drop(store);
    let store = f.open(limits).unwrap();
    assert_eq!(
        store.scan(None, None, 2).unwrap(),
        vec![
            (b"b".to_vec(), b"prefix".to_vec()),
            (b"d".to_vec(), value.clone())
        ]
    );
    assert_eq!(store.get(b"x", 0).unwrap(), Some(vec![]));
    let witness = fs::read(f.witness()).unwrap();
    let forged = encode_batch(&batch(&altered)).unwrap();
    assert_eq!(
        offset + forged.len() as u64,
        fs::metadata(f.log()).unwrap().len()
    );
    OpenOptions::new()
        .write(true)
        .open(f.log())
        .unwrap()
        .write_all_at(&forged, offset)
        .unwrap();
    for error in [
        store.get(b"d", altered.len()).unwrap_err(),
        store.scan(None, None, 4).unwrap_err(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "retained suffix value digest mismatch");
    }
    assert_eq!(store.get(b"b", 6).unwrap(), Some(b"prefix".to_vec()));
    drop(store);
    let error = f.open(limits).err().unwrap();
    assert_eq!(error.to_string(), "committed suffix digest mismatch");
    assert_eq!(fs::read(f.witness()).unwrap(), witness);
}

#[test]
fn writable_generation_preserves_old_reader_gate_and_reopens_with_the_same_limits() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open(LIMITS).unwrap();
    let id = store.generation_id();
    assert_eq!(store.get(b"a", 4).unwrap(), Some(b"base".to_vec()));
    assert_eq!(
        f.open(LIMITS).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"x",
                value: b"discarded",
            },
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Delete { key: b"x" },
            BatchOperation::Put {
                key: b"x",
                value: b"",
            },
        ])
        .unwrap();
    store.put(b"b", b"new").unwrap();
    store.delete(b"z").unwrap();
    let page = vec![(b"b".to_vec(), b"new".to_vec()), (b"x".to_vec(), vec![])];
    assert_eq!(store.scan(None, None, 2).unwrap(), page);
    drop(store);
    let before = fs::read(f.log()).unwrap();
    assert!(Store::open(&f.path).is_err());
    assert_eq!(fs::read(f.log()).unwrap(), before);
    let changed = RetainedStoreLimits {
        suffix: SuffixLimits {
            bytes: LIMITS.suffix.bytes + 1,
            ..LIMITS.suffix
        },
        ..LIMITS
    };
    assert_eq!(
        f.open(changed).err().unwrap().kind(),
        io::ErrorKind::InvalidInput
    );
    let store = f.open(LIMITS).unwrap();
    assert_eq!(store.generation_id(), id);
    assert_eq!(store.scan(None, None, 2).unwrap(), page);
    assert_eq!(store.get(b"a", 4).unwrap(), None);
    assert_eq!(store.get(b"x", 0).unwrap(), Some(vec![]));
    assert_eq!(store.scan(Some(b"b"), Some(b"z"), 1).unwrap(), page[1..]);
    let source = Store::open(f.root.path().join("source")).unwrap();
    assert_eq!(source.get(b"a").unwrap(), Some(b"base".to_vec()));
    assert_eq!(source.get(b"z").unwrap(), Some(b"kept".to_vec()));
}

#[test]
fn admission_failure_preserves_log_witness_and_prepared_overlay() {
    let encoded = record::encode(Operation::Put, b"a", b"one").unwrap();
    for (limits, next_key, reason) in [
        (
            RetainedStoreLimits {
                suffix: SuffixLimits {
                    bytes: encoded.len() as u64,
                    ..LIMITS.suffix
                },
                ..LIMITS
            },
            b"a".as_slice(),
            "byte limit",
        ),
        (
            RetainedStoreLimits {
                suffix: SuffixLimits {
                    operations: 1,
                    ..LIMITS.suffix
                },
                ..LIMITS
            },
            b"a".as_slice(),
            "operation limit",
        ),
        (
            RetainedStoreLimits {
                suffix: SuffixLimits {
                    work: 1,
                    ..LIMITS.suffix
                },
                ..LIMITS
            },
            b"a".as_slice(),
            "work limit",
        ),
        (
            RetainedStoreLimits {
                overlay: OverlayLimits {
                    entries: 1,
                    key_bytes: 16,
                },
                ..LIMITS
            },
            b"b".as_slice(),
            "entry limit",
        ),
        (
            RetainedStoreLimits {
                overlay: OverlayLimits {
                    entries: 2,
                    key_bytes: 1,
                },
                ..LIMITS
            },
            b"b".as_slice(),
            "key byte limit",
        ),
    ] {
        let f = Fixture::new(limits);
        let mut store = f.open(limits).unwrap();
        store.put(b"a", b"one").unwrap();
        let before = (fs::read(f.log()).unwrap(), fs::read(f.witness()).unwrap());
        let result = store.put(next_key, b"two");
        assert!(
            matches!(result, Err(MutationFailure::Rejected(ref e)) if e.kind() == io::ErrorKind::InvalidInput && e.to_string().contains(reason)),
            "{reason}: {result:?}"
        );
        assert_eq!(
            (fs::read(f.log()).unwrap(), fs::read(f.witness()).unwrap()),
            before
        );
        assert_eq!(store.get(b"a", 3).unwrap(), Some(b"one".to_vec()));
        drop(store);
        let reopened = f.open(limits).unwrap();
        assert_eq!(reopened.get(b"b", 3).unwrap(), None);
        assert_eq!(reopened.get(b"a", 3).unwrap(), Some(b"one".to_vec()));
    }
}

#[test]
fn batch_preflight_and_replay_charge_the_same_ordered_work() {
    let ops = [
        BatchOperation::Put {
            key: b"b",
            value: b"first",
        },
        BatchOperation::Delete { key: b"b" },
        BatchOperation::Put {
            key: b"a",
            value: b"last",
        },
        BatchOperation::Delete { key: b"a" },
    ];
    // The cap charges 1 + 4 + 4 + 7 units. Deleted keys stay in the overlay.
    for work in [15, 16] {
        let limits = RetainedStoreLimits {
            suffix: SuffixLimits {
                work,
                ..LIMITS.suffix
            },
            overlay: OverlayLimits {
                entries: 2,
                key_bytes: 2,
            },
            ..LIMITS
        };
        let f = Fixture::new(limits);
        let mut store = f.open(limits).unwrap();
        let before = (fs::read(f.log()).unwrap(), fs::read(f.witness()).unwrap());
        let result = store.write_batch(&ops);
        if work == 15 {
            assert!(
                matches!(result, Err(MutationFailure::Rejected(ref e)) if e.to_string().contains("work limit"))
            );
            assert_eq!(
                (fs::read(f.log()).unwrap(), fs::read(f.witness()).unwrap()),
                before
            );
            assert_eq!(store.get(b"a", 4).unwrap(), Some(b"base".to_vec()));
        } else {
            result.unwrap();
            assert_eq!(store.get(b"a", 4).unwrap(), None);
        }
        let page = store.scan(None, None, 8).unwrap();
        drop(store);
        assert_eq!(f.open(limits).unwrap().scan(None, None, 8).unwrap(), page);
    }
}

#[test]
fn each_failed_commit_boundary_poisons_and_recovers_the_complete_batch() {
    for point in [
        CommitPoint::Appended,
        CommitPoint::LogSynced,
        CommitPoint::WitnessSynced,
    ] {
        let f = Fixture::new(LIMITS);
        let mut store = f.open(LIMITS).unwrap();
        store.put(b"safe", b"acknowledged").unwrap();
        let before = fs::read(f.witness()).unwrap();
        let operations = [
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put {
                key: b"new",
                value: b"batch",
            },
        ];
        let result = store.write(
            || {
                record::encode(
                    Operation::Batch,
                    &[0],
                    &record::batch::encode_payload(&operations)?,
                )
            },
            |observed| {
                if observed == point {
                    return Err(io::Error::other("injected write interruption"));
                }
                Ok(())
            },
        );
        assert!(matches!(result, Err(MutationFailure::OutcomeUnknown(_))));
        assert!(store.get(b"safe", 16).is_err());
        assert!(store.scan(None, None, 3).is_err());
        assert!(matches!(
            store.delete(b"z"),
            Err(MutationFailure::Rejected(_))
        ));
        assert_eq!(
            fs::read(f.witness()).unwrap() == before,
            point != CommitPoint::WitnessSynced
        );
        drop(store);
        let reopened = f.open(LIMITS).unwrap();
        assert_eq!(reopened.get(b"a", 4).unwrap(), None);
        assert_eq!(reopened.get(b"new", 5).unwrap(), Some(b"batch".to_vec()));
        assert_eq!(
            reopened.get(b"safe", 16).unwrap(),
            Some(b"acknowledged".to_vec())
        );
        assert_ne!(fs::read(f.witness()).unwrap(), before);
    }
}

#[test]
fn recovery_rejects_a_mismatched_base_length_before_decoding_the_suffix() {
    let f = Fixture::new(LIMITS);
    let path = f.path.join("base");
    let base = fs::read(&path).unwrap();
    let witness = fs::read(f.witness()).unwrap();
    let mut log = fs::read(f.log()).unwrap();
    // With the base intact, the short suffix would fail during decoding.
    f.append_unacknowledged(&[0]);
    let error = f.open(LIMITS).err().unwrap();
    assert_eq!(error.to_string(), "incomplete retained suffix header");
    log.push(0);
    for altered in [&base[..base.len() - 1], &[base.as_slice(), &[0]].concat()] {
        fs::write(&path, altered).unwrap();
        let error = f.open(LIMITS).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "retained base length does not match its binding"
        );
        assert_eq!(fs::read(f.log()).unwrap(), log);
        assert_eq!(fs::read(f.witness()).unwrap(), witness);
        assert_eq!(fs::read(&path).unwrap(), altered);
    }
    fs::write(&path, &base).unwrap();
    log.pop();
    log.extend_from_slice(&record::encode(Operation::Put, b"pending", b"proposed").unwrap());
    fs::write(f.log(), &log).unwrap();
    let store = f.open(LIMITS).unwrap();
    assert_eq!(
        store.get(b"a", 4).unwrap().as_deref(),
        Some(b"base".as_slice())
    );
    assert_eq!(
        store.get(b"pending", 8).unwrap().as_deref(),
        Some(b"proposed".as_slice())
    );
    assert_ne!(fs::read(f.witness()).unwrap(), witness);
}

#[test]
fn tampered_or_incomplete_suffix_leaves_witness_unchanged() {
    for mode in ["short header", "torn", "split", "missing", "substituted"] {
        let f = Fixture::new(LIMITS);
        f.open(LIMITS).unwrap().put(b"key", b"a").unwrap();
        let next = record::encode(Operation::Put, b"new", b"b").unwrap();
        match mode {
            "short header" => f.append_unacknowledged(&next[..record::HEADER_SIZE - 1]),
            "torn" => f.append_unacknowledged(&next[..next.len() - 1]),
            "split" => {
                f.append_unacknowledged(&next);
                let bytes = fs::read(f.log()).unwrap();
                let header = &bytes[..HEADER_BYTES];
                let suffix = &bytes[HEADER_BYTES..];
                let end = suffix.len() - 1;
                let witness = marker(header, end as u64, &Sha256::digest(&suffix[..end]).into());
                fs::write(f.witness(), witness).unwrap();
            }
            "missing" => {
                OpenOptions::new()
                    .write(true)
                    .open(f.log())
                    .unwrap()
                    .set_len(HEADER_BYTES as u64)
                    .unwrap();
            }
            "substituted" => {
                let alternate = record::encode(Operation::Put, b"key", b"b").unwrap();
                let file = OpenOptions::new().write(true).open(f.log()).unwrap();
                assert_eq!(
                    file.metadata().unwrap().len(),
                    (HEADER_BYTES + alternate.len()) as u64
                );
                file.write_all_at(&alternate, HEADER_BYTES as u64).unwrap();
            }
            _ => unreachable!(),
        }
        let before = fs::read(f.witness()).unwrap();
        assert_eq!(
            f.open(LIMITS).err().unwrap().kind(),
            io::ErrorKind::InvalidData,
            "{mode}"
        );
        assert_eq!(fs::read(f.witness()).unwrap(), before, "{mode}");
    }
}

#[test]
fn different_generation_witness_and_over_budget_tail_are_rejected() {
    let limits = RetainedStoreLimits {
        suffix: SuffixLimits {
            operations: 1,
            ..LIMITS.suffix
        },
        ..LIMITS
    };
    let f = Fixture::new(limits);
    let other = f.root.path().join("other");
    let id = RetainedStore::create(f.root.path().join("candidate"), &other, limits, &f.expected)
        .unwrap();
    let mut store = f.open(limits).unwrap();
    assert_ne!(store.generation_id(), id);
    store.put(b"one", b"one").unwrap();
    drop(store);
    let before = fs::read(f.witness()).unwrap();
    fs::copy(other.join("commit.state"), f.witness()).unwrap();
    assert_eq!(
        f.open(limits).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
    fs::write(f.witness(), &before).unwrap();
    f.append_unacknowledged(&record::encode(Operation::Put, b"two", b"two").unwrap());
    let error = f.open(limits).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("operation limit"));
    assert_eq!(fs::read(f.witness()).unwrap(), before);
}

#[test]
fn import_from_detached_suffix_preserves_committed_source_state() {
    use crate::retained_base_v2::StagingLimits;

    let f = Fixture::new(LIMITS);
    let candidate = f.root.path().join("candidate");
    let detached = f.root.path().join("detached");
    let importer = f.root.path().join("imported");
    let source = Store::open(f.root.path().join("source")).unwrap();
    source
        .write_batch(&[
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put {
                key: b"b",
                value: b"new",
            },
        ])
        .unwrap();
    let binding = source
        .export_retained_suffix(
            &candidate,
            &f.expected,
            &detached,
            LIMITS.base,
            StagingLimits {
                value_bytes: 64,
                staging_bytes: 1024,
            },
            LIMITS.suffix,
            RetainedExportLimits {
                max_log_bytes: 4096,
                max_live_keys: 16,
                max_live_key_bytes: 64,
                max_output_bytes: 4096,
            },
        )
        .unwrap();
    let id = RetainedStore::create_from_suffix(&candidate, &detached, &importer, LIMITS, &binding)
        .unwrap();
    let mut store = RetainedStore::open(&importer, LIMITS, &f.expected).unwrap();
    assert_eq!(store.generation_id(), id);
    assert_eq!(
        store.scan(None, None, 4).unwrap(),
        source.scan(None, None, 4).unwrap()
    );
    store.put(b"b", b"only in retained").unwrap();
    assert_eq!(source.get(b"b").unwrap(), Some(b"new".to_vec()));
    drop(store);
    let stable = fs::read(importer.join("records.log")).unwrap();
    assert!(
        RetainedStore::create_from_suffix(&candidate, &detached, &importer, LIMITS, &binding)
            .is_err()
    );
    assert_eq!(fs::read(importer.join("records.log")).unwrap(), stable);

    let bytes = fs::read(&detached).unwrap();
    let mut wrong = binding.clone();
    wrong.digest[0] ^= 1;
    let rejected = f.root.path().join("rejected");
    assert!(matches!(
        RetainedStore::create_from_suffix(&candidate, &detached, &rejected, LIMITS, &wrong),
        Err(PublicationFailure::NotPublished(_))
    ));
    assert!(!rejected.exists());
    assert_eq!(fs::read(&detached).unwrap(), bytes);
}

#[test]
fn torn_witness_prevents_adopting_a_complete_tail() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open(LIMITS).unwrap();
    store.put(b"key", b"old").unwrap();
    let result = store.write(
        || record::encode(Operation::Put, b"key", b"new"),
        |point| {
            if point == CommitPoint::LogSynced {
                let file = OpenOptions::new().write(true).open(f.witness())?;
                file.write_all_at(&[0; 16], 0)?;
                file.sync_data()?;
                return Err(io::Error::other("injected torn witness"));
            }
            Ok(())
        },
    );
    assert!(matches!(result, Err(MutationFailure::OutcomeUnknown(_))));
    assert!(store.get(b"key", 3).is_err());
    drop(store);
    let before = fs::read(f.witness()).unwrap();
    assert_eq!(
        f.open(LIMITS).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(fs::read(f.witness()).unwrap(), before);
}

#[test]
fn uncertain_creation_leaves_a_complete_candidate_for_inspection() {
    let f = Fixture::new(LIMITS);
    let candidate = f.root.path().join("candidate");
    let destination = f.root.path().join("uncertain");
    let result =
        RetainedStore::create_using(&candidate, &destination, LIMITS, &f.expected, None, |_| {
            Err(io::Error::other("injected parent sync failure"))
        });
    assert!(matches!(result, Err(PublicationFailure::OutcomeUnknown(_))));
    let log = destination.join("records.log");
    let mut file = OpenOptions::new().append(true).open(log).unwrap();
    file.write_all(&record::encode(Operation::Put, b"new", b"tail").unwrap())
        .unwrap();
    file.sync_data().unwrap();
    let witness = destination.join("commit.state");
    let before = fs::read(&witness).unwrap();
    let error = RetainedStore::open_using(&destination, LIMITS, &f.expected, |_| {
        Err(io::Error::other("injected open directory sync failure"))
    })
    .err()
    .unwrap();
    assert!(error.to_string().contains("directory sync failure"));
    assert_eq!(fs::read(&witness).unwrap(), before);
    let inspected = RetainedStore::open(&destination, LIMITS, &f.expected).unwrap();
    assert_eq!(inspected.get(b"a", 4).unwrap(), Some(b"base".to_vec()));
    assert_eq!(inspected.get(b"new", 4).unwrap(), Some(b"tail".to_vec()));
    assert_eq!(
        f.open(LIMITS).unwrap().get(b"z", 4).unwrap(),
        Some(b"kept".to_vec())
    );
}

#[test]
fn retained_store_crash_child() {
    let Some(root) = std::env::var_os("EMDA_WRITABLE_RETAINED_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let binding = fs::read(root.join("binding")).unwrap();
    assert_eq!(binding.len(), 176);
    let expected = Expected {
        header: binding[..112].try_into().unwrap(),
        witness: binding[112..144].try_into().unwrap(),
        base_digest: binding[144..].try_into().unwrap(),
    };
    let point = match std::env::var("EMDA_WRITABLE_RETAINED_CRASH_POINT")
        .unwrap()
        .as_str()
    {
        "log" => CommitPoint::LogSynced,
        "witness" => CommitPoint::WitnessSynced,
        _ => panic!("unexpected commit boundary"),
    };
    let mut store = RetainedStore::open(root.join("live"), LIMITS, &expected).unwrap();
    let operations = [
        BatchOperation::Delete { key: b"a" },
        BatchOperation::Put {
            key: b"new",
            value: b"batch",
        },
    ];
    let result = store.write(
        || {
            record::encode(
                Operation::Batch,
                &[0],
                &record::batch::encode_payload(&operations)?,
            )
        },
        |observed| {
            if observed == point {
                println!("retained commit boundary");
                io::stdout().flush()?;
                loop {
                    std::thread::park();
                }
            }
            Ok(())
        },
    );
    panic!("child did not stop at the selected boundary: {result:?}");
}

#[test]
fn killed_retained_writer_recovers_the_synced_batch() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
        time::Duration,
    };
    for point in ["log", "witness"] {
        let f = Fixture::new(LIMITS);
        f.open(LIMITS)
            .unwrap()
            .put(b"safe", b"acknowledged")
            .unwrap();
        let mut binding = f.expected.header.to_vec();
        binding.extend(f.expected.witness);
        binding.extend(f.expected.base_digest);
        fs::write(f.root.path().join("binding"), binding).unwrap();
        let before = fs::read(f.witness()).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "retained_candidate::writable::tests::retained_store_crash_child",
                "--nocapture",
            ])
            .env("EMDA_WRITABLE_RETAINED_CRASH_ROOT", f.root.path())
            .env("EMDA_WRITABLE_RETAINED_CRASH_POINT", point)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let input = BufReader::new(child.stdout.take().unwrap());
        let (tell, wait) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in input.lines().map_while(Result::ok) {
                if line == "retained commit boundary" {
                    let _ = tell.send(());
                    break;
                }
            }
        });
        let reached = wait.recv_timeout(Duration::from_secs(15));
        let kill = child.kill();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        reached.expect("child did not reach the retained commit boundary");
        kill.unwrap();
        assert!(!status.success());
        assert_eq!(fs::read(f.witness()).unwrap() == before, point == "log");
        let store = f.open(LIMITS).unwrap();
        assert_eq!(store.get(b"a", 4).unwrap(), None);
        assert_eq!(store.get(b"new", 5).unwrap(), Some(b"batch".to_vec()));
        assert_eq!(
            store.get(b"safe", 16).unwrap(),
            Some(b"acknowledged".to_vec())
        );
        assert_ne!(fs::read(f.witness()).unwrap(), before);
    }
}
