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
fn retained_import_checks_base_before_creating_copy() {
    let f = Fixture::new(LIMITS);
    let candidate = f.root.path().join("candidate");
    let source = candidate.join("base");
    let original = fs::read(&source).unwrap();
    let output = f.root.path().join("copy");
    fs::create_dir(&output).unwrap();
    let mut exact = LIMITS;
    exact.base.file_bytes = original.len() as u64;
    exact.base.work_bytes = exact.base.file_bytes + 1;
    exact.base.entries = 2;
    exact.base.key_bytes = 2;
    exact.index.anchor_bytes = 1;
    for (limits, message) in [
        (
            RetainedStoreLimits {
                base: Limits {
                    entries: 1,
                    ..exact.base
                },
                ..exact
            },
            "invalid totals or budget exceeded",
        ),
        (
            RetainedStoreLimits {
                base: Limits {
                    key_bytes: 1,
                    ..exact.base
                },
                ..exact
            },
            "invalid totals or budget exceeded",
        ),
        (
            RetainedStoreLimits {
                index: IndexLimits {
                    blocks: 0,
                    ..exact.index
                },
                ..exact
            },
            "retained base block limit exceeded",
        ),
        (
            RetainedStoreLimits {
                index: IndexLimits {
                    anchor_bytes: 0,
                    ..exact.index
                },
                ..exact
            },
            "retained base anchor byte limit exceeded",
        ),
    ] {
        let error = BaseImport::open(&candidate, limits, &f.expected, u64::MAX)
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), message);
        assert!(!output.join("base").exists());
    }
    let mut changed = original.clone();
    *changed.last_mut().unwrap() ^= 1;
    for (bytes, message) in [
        (
            &changed[..changed.len() - 1],
            "retained base length does not match its binding",
        ),
        (
            changed.as_slice(),
            "retained base trailer does not match its binding",
        ),
    ] {
        fs::write(&source, bytes).unwrap();
        let error = BaseImport::open(&candidate, exact, &f.expected, u64::MAX)
            .err()
            .unwrap();
        assert_eq!(error.to_string(), message);
        assert!(!output.join("base").exists());
        assert_eq!(fs::read(&source).unwrap(), bytes);
    }
    fs::write(&source, &original).unwrap();
    BaseImport::open(&candidate, exact, &f.expected, exact.base.file_bytes)
        .unwrap()
        .copy_to(&output.join("base"))
        .unwrap();
    assert_eq!(fs::read(output.join("base")).unwrap(), original);
}

#[test]
fn retained_import_replays_suffix_admission_before_copying_base() {
    let f = Fixture::new(LIMITS);
    let parent = f.root.path();
    let candidate = parent.join("candidate");
    let base = fs::read(candidate.join("base")).unwrap();
    let suffix_path = parent.join("suffix");
    let encoded = record::encode(Operation::Put, b"b", b"retained").unwrap();
    let exact = RetainedStoreLimits {
        suffix: SuffixLimits {
            bytes: encoded.len() as u64,
            operations: 1,
            work: 1,
        },
        overlay: OverlayLimits {
            entries: 1,
            key_bytes: 1,
        },
        ..LIMITS
    };
    let initialize = |stage: &Path, limits: RetainedStoreLimits, suffix: &[u8]| {
        fs::write(&suffix_path, suffix).unwrap();
        let binding = SuffixExpected {
            candidate: f.expected.clone(),
            end: suffix.len() as u64,
            digest: Sha256::digest(suffix).into(),
        };
        RetainedStore::initialize(
            stage,
            limits,
            &f.expected,
            Some(SuffixImport::open(&suffix_path, limits.suffix, &binding).unwrap()),
            GenerationRole::Standalone,
            None,
            Some(BaseImport::open(&candidate, limits, &f.expected, u64::MAX).unwrap()),
        )
    };
    for (limits, message) in [
        (
            RetainedStoreLimits {
                suffix: SuffixLimits {
                    operations: 0,
                    ..exact.suffix
                },
                ..exact
            },
            "suffix operation limit exceeded",
        ),
        (
            RetainedStoreLimits {
                suffix: SuffixLimits {
                    work: 0,
                    ..exact.suffix
                },
                ..exact
            },
            "suffix work limit exceeded",
        ),
        (
            RetainedStoreLimits {
                overlay: OverlayLimits {
                    entries: 0,
                    ..exact.overlay
                },
                ..exact
            },
            "overlay entry limit exceeded",
        ),
        (
            RetainedStoreLimits {
                overlay: OverlayLimits {
                    key_bytes: 0,
                    ..exact.overlay
                },
                ..exact
            },
            "overlay key byte limit exceeded",
        ),
    ] {
        let stage = crate::staging::Stage::directory(parent).unwrap();
        let error = initialize(stage.path(), limits, &encoded).err().unwrap();
        assert!(error.to_string().contains(message), "{error}");
        assert!(!stage.path().join("base").exists());
        assert_eq!(fs::read(&suffix_path).unwrap(), encoded);
    }
    let stage = crate::staging::Stage::directory(parent).unwrap();
    let mut corrupt = encoded.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    let error = initialize(stage.path(), exact, &corrupt).err().unwrap();
    assert_eq!(error.to_string(), "record checksum mismatch");
    assert!(!stage.path().join("base").exists());
    drop(stage);

    let stage = crate::staging::Stage::directory(parent).unwrap();
    let store = initialize(stage.path(), exact, &encoded).unwrap();
    assert_eq!(store.get(b"b", 8).unwrap().unwrap(), b"retained");
    assert_eq!(store.get(b"a", 4).unwrap().unwrap(), b"base");
    assert_eq!(fs::read(stage.path().join("base")).unwrap(), base);
    assert_eq!(fs::read(candidate.join("base")).unwrap(), base);
}

#[test]
fn writable_retained_import_still_checks_the_full_base_body() {
    let f = Fixture::new(LIMITS);
    let candidate = f.root.path().join("candidate");
    let base = candidate.join("base");
    let mut damaged = fs::read(&base).unwrap();
    let first_value = crate::retained_base_v2::FRAME_BYTES as usize - 32 + 8 + 1;
    damaged[first_value] ^= 1;
    fs::write(&base, &damaged).unwrap();
    let standalone = f.root.path().join("standalone");
    let managed = f.root.path().join("managed");
    for (destination, result) in [
        (
            &standalone,
            RetainedStore::create(&candidate, &standalone, LIMITS, &f.expected),
        ),
        (
            &managed,
            ManagedRetainedStore::create(&candidate, &managed, LIMITS, &f.expected),
        ),
    ] {
        assert!(
            matches!(result, Err(PublicationFailure::NotPublished(error)) if error.kind() == io::ErrorKind::InvalidData)
        );
        assert!(!destination.exists());
    }
    assert_eq!(fs::read(base).unwrap(), damaged);
    assert!(fs::read_dir(f.root.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".emda-stage-v1-")
    }));
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
        if work == 15 {
            let batch = record::encode(
                Operation::Batch,
                &[0],
                &record::batch::encode_payload(&ops).unwrap(),
            )
            .unwrap();
            f.append_unacknowledged(&batch);
            let log = fs::read(f.log()).unwrap();
            let error = f.open(limits).err().expect("over-budget recovery batch");
            assert!(error.to_string().contains("work limit"), "{error}");
            assert_eq!(fs::read(f.log()).unwrap(), log);
            assert_eq!(fs::read(f.witness()).unwrap(), before.1);
        }
    }
}

#[test]
fn mixed_overlay_batch_merges_new_and_existing_keys_before_reopen() {
    let f = Fixture::new(LIMITS);
    let mut store = f.open(LIMITS).unwrap();
    for key in [b"b", b"d", b"f", b"h"] {
        store.put(key, key).unwrap();
    }
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"z",
                value: b"end",
            },
            BatchOperation::Put {
                key: b"e",
                value: b"middle",
            },
            BatchOperation::Put {
                key: b"a",
                value: b"new",
            },
            BatchOperation::Delete { key: b"d" },
            BatchOperation::Put {
                key: b"f",
                value: b"changed",
            },
            BatchOperation::Delete { key: b"c" },
            BatchOperation::Put {
                key: b"e",
                value: b"last",
            },
        ])
        .unwrap();
    let expected = [
        (b'a', "new"),
        (b'b', "b"),
        (b'e', "last"),
        (b'f', "changed"),
        (b'h', "h"),
        (b'z', "end"),
    ]
    .into_iter()
    .map(|(key, value)| (vec![key], value.as_bytes().to_vec()))
    .collect::<Vec<_>>();
    assert_eq!(store.scan(None, None, 8).unwrap(), expected);
    drop(store);
    assert_eq!(
        f.open(LIMITS).unwrap().scan(None, None, 8).unwrap(),
        expected
    );
}

#[test]
fn sorted_tail_replay_checks_the_ordered_work_boundary_before_adoption() {
    let ops = [
        BatchOperation::Delete { key: b"a" },
        BatchOperation::Put {
            key: b"b",
            value: b"new",
        },
        BatchOperation::Delete { key: b"z" },
    ];
    let bytes = record::encode(
        Operation::Batch,
        &[0],
        &record::batch::encode_payload(&ops).unwrap(),
    )
    .unwrap();
    for work in [11, 12] {
        let limits = RetainedStoreLimits {
            suffix: SuffixLimits {
                work,
                ..LIMITS.suffix
            },
            ..LIMITS
        };
        let f = Fixture::new(limits);
        f.append_unacknowledged(&bytes);
        let log = fs::read(f.log()).unwrap();
        let witness = fs::read(f.witness()).unwrap();
        if work == 11 {
            let error = f
                .open(limits)
                .err()
                .expect("work limit must reject the batch");
            assert!(error.to_string().contains("work limit"), "{error}");
            assert_eq!(fs::read(f.witness()).unwrap(), witness);
        } else {
            let store = f.open(limits).unwrap();
            assert_eq!(
                store.scan(None, None, 8).unwrap(),
                [(b"b".to_vec(), b"new".to_vec())]
            );
            assert_ne!(fs::read(f.witness()).unwrap(), witness);
        }
        assert_eq!(fs::read(f.log()).unwrap(), log);
    }
}

#[test]
fn replay_work_preflight_accounts_for_the_committed_overlay() {
    let mut payload = 2_u32.to_le_bytes().to_vec();
    payload.extend_from_slice(&record::encode(Operation::Put, b"key", b"new").unwrap());
    payload.extend_from_slice(&record::encode(Operation::Put, b"key", b"last").unwrap());
    let batch = record::encode(Operation::Batch, &[0], &payload).unwrap();
    for work in [8, 9] {
        let limits = RetainedStoreLimits {
            suffix: SuffixLimits {
                work,
                ..LIMITS.suffix
            },
            ..LIMITS
        };
        let f = Fixture::new(limits);
        f.open(limits).unwrap().put(b"key", b"old").unwrap();
        let witness = fs::read(f.witness()).unwrap();
        f.append_unacknowledged(&batch);
        let log = fs::read(f.log()).unwrap();
        if work == 8 {
            let error = f.open(limits).err().unwrap();
            assert_eq!(error.to_string(), "suffix work limit exceeded");
            assert_eq!(fs::read(f.witness()).unwrap(), witness);
        } else {
            let store = f.open(limits).unwrap();
            assert_eq!(
                store.get(b"key", 4).unwrap().as_deref(),
                Some(b"last".as_slice())
            );
            assert_ne!(fs::read(f.witness()).unwrap(), witness);
        }
        assert_eq!(fs::read(f.log()).unwrap(), log);
    }
}

#[test]
fn each_failed_commit_boundary_poisons_and_recovers_the_complete_batch() {
    for point in [
        CommitPoint::Appended,
        CommitPoint::LogSynced,
        CommitPoint::WitnessWritten,
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
            matches!(point, CommitPoint::Appended | CommitPoint::LogSynced)
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
fn recovery_preflights_the_base_frame_without_trusting_its_contents() {
    let f = Fixture::new(LIMITS);
    let base_path = f.path.join("base");
    let base = fs::read(&base_path).unwrap();
    let witness = fs::read(f.witness()).unwrap();
    let acknowledged = fs::read(f.log()).unwrap();
    f.append_unacknowledged(&[0]);
    let partial = fs::read(f.log()).unwrap();

    let mut malformed_header = base.clone();
    malformed_header[0] ^= 1;
    fs::write(&base_path, malformed_header).unwrap();
    assert_eq!(f.open(LIMITS).err().unwrap().to_string(), "invalid header");

    // An intact trailer cannot make impossible key or value totals admissible.
    for (entries, keys) in [(2_u64, 1_u64), (2, 20), (0, 0)] {
        let mut impossible = base.clone();
        impossible[56..64].copy_from_slice(&entries.to_le_bytes());
        impossible[64..72].copy_from_slice(&keys.to_le_bytes());
        impossible[80..88].copy_from_slice(&keys.to_le_bytes());
        fs::write(&base_path, &impossible).unwrap();
        assert_eq!(
            f.open(LIMITS).err().unwrap().to_string(),
            "invalid totals or budget exceeded"
        );
        assert_eq!(fs::read(f.log()).unwrap(), partial);
        assert_eq!(fs::read(f.witness()).unwrap(), witness);
    }

    let trailer = base.len() - 32;
    let mut modified_body = base.clone();
    modified_body[trailer - 1] ^= 1;
    let mut alternate = modified_body.clone();
    let mut hash = Sha256::new();
    hash.update(b"EMDA retained base v2\0");
    hash.update(&alternate[..trailer]);
    alternate[trailer..].copy_from_slice(&hash.finalize());
    crate::retained_base_v2::validate(alternate.as_slice(), LIMITS.base).unwrap();
    fs::write(&base_path, &alternate).unwrap();
    assert_eq!(
        f.open(LIMITS).err().unwrap().to_string(),
        "retained base trailer does not match its binding"
    );
    assert_eq!(fs::read(f.log()).unwrap(), partial);
    assert_eq!(fs::read(f.witness()).unwrap(), witness);

    fs::write(&base_path, &modified_body).unwrap();
    fs::write(f.log(), &acknowledged).unwrap();
    f.append_unacknowledged(&record::encode(Operation::Put, b"tail", b"valid").unwrap());
    let complete = fs::read(f.log()).unwrap();
    assert_eq!(f.open(LIMITS).err().unwrap().to_string(), "digest mismatch");
    assert_eq!(fs::read(f.log()).unwrap(), complete);
    assert_eq!(fs::read(f.witness()).unwrap(), witness);
    fs::write(&base_path, &base).unwrap();
    let store = f.open(LIMITS).unwrap();
    assert_eq!(store.get(b"tail", 5).unwrap(), Some(b"valid".to_vec()));
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
            bytes: 256 * 1024,
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
    let prefix = fs::read(f.log()).unwrap();
    let tail = record::encode(Operation::Put, b"two", &vec![4; 128 * 1024]).unwrap();
    f.append_unacknowledged(&tail);
    let error = f.open(limits).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("operation limit"));
    assert_eq!(fs::read(f.witness()).unwrap(), before);
    assert_eq!(
        fs::read(f.log()).unwrap(),
        [prefix.as_slice(), &tail].concat()
    );
    fs::write(f.log(), prefix).unwrap();
    let recovered = f.open(limits).unwrap();
    assert_eq!(
        recovered.get(b"one", 3).unwrap().as_deref(),
        Some(b"one".as_slice())
    );
    assert_eq!(recovered.get(b"two", 3).unwrap(), None);
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
fn recovery_rechecks_suffix_bytes_before_advancing_witness() {
    for replace_committed in [false, true] {
        let f = Fixture::new(LIMITS);
        let mut store = f.open(LIMITS).unwrap();
        store.put(b"ack", b"old").unwrap();
        drop(store);
        let committed_end = fs::metadata(f.log()).unwrap().len();
        let tail = record::encode(Operation::Put, b"tail", b"old").unwrap();
        f.append_unacknowledged(&tail);
        let verified = fs::read(f.log()).unwrap();
        let witness = fs::read(f.witness()).unwrap();
        let (offset, key) = if replace_committed {
            (HEADER_BYTES as u64, b"ack".as_slice())
        } else {
            (committed_end, b"tail".as_slice())
        };
        let replacement = record::encode(Operation::Put, key, b"new").unwrap();

        let result = RetainedStore::open_using(&f.path, LIMITS, &f.expected, |path| {
            let file = OpenOptions::new().write(true).open(f.log())?;
            file.write_all_at(&replacement, offset)?;
            crate::directory::sync_existing(path)
        });
        let error = result.err().expect("changed suffix was adopted");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("suffix bytes changed"));
        assert_eq!(fs::read(f.witness()).unwrap(), witness);
        let mut changed = verified.clone();
        let offset = offset as usize;
        changed[offset..offset + replacement.len()].copy_from_slice(&replacement);
        assert_eq!(fs::read(f.log()).unwrap(), changed);

        fs::write(f.log(), verified).unwrap();
        let reopened = f.open(LIMITS).unwrap();
        assert_eq!(reopened.get(b"ack", 3).unwrap(), Some(b"old".to_vec()));
        assert_eq!(reopened.get(b"tail", 3).unwrap(), Some(b"old".to_vec()));
        assert_ne!(fs::read(f.witness()).unwrap(), witness);
    }
}

#[test]
fn recovery_verifies_the_base_after_directory_sync() {
    for pending in [true, false] {
        for replace_path in [false, true] {
            let f = Fixture::new(LIMITS);
            let base_path = f.path.join("base");
            let base = fs::read(&base_path).unwrap();
            let witness = fs::read(f.witness()).unwrap();
            let original_path = f.path.join("original-base");
            let mut changed = base.clone();
            let last_body_byte = changed.len() - 33;
            changed[last_body_byte] ^= 1;
            if pending {
                f.append_unacknowledged(&record::encode(Operation::Put, b"tail", b"new").unwrap());
            }
            let log = fs::read(f.log()).unwrap();

            let result = RetainedStore::open_using(&f.path, LIMITS, &f.expected, |path| {
                if replace_path {
                    fs::rename(&base_path, &original_path)?;
                    fs::write(&base_path, &base)?;
                } else {
                    fs::write(&base_path, &changed)?;
                }
                crate::directory::sync_existing(path)
            });
            let error = result.err().expect("changed base was accepted");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            let expected = if replace_path {
                "retained base path or length changed"
            } else {
                "digest mismatch"
            };
            assert_eq!(error.to_string(), expected);
            assert_eq!(fs::read(f.witness()).unwrap(), witness);
            assert_eq!(fs::read(f.log()).unwrap(), log);
            if replace_path {
                assert_eq!(fs::read(&base_path).unwrap(), base);
                fs::rename(original_path, &base_path).unwrap();
            } else {
                assert_eq!(fs::read(&base_path).unwrap(), changed);
                fs::write(&base_path, &base).unwrap();
            }

            let reopened = f.open(LIMITS).unwrap();
            assert_eq!(reopened.get(b"a", 4).unwrap(), Some(b"base".to_vec()));
            assert_eq!(
                reopened.get(b"tail", 3).unwrap(),
                pending.then(|| b"new".to_vec())
            );
            assert_eq!(fs::read(f.witness()).unwrap() != witness, pending);
        }
    }
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
        "witness-written" => CommitPoint::WitnessWritten,
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
    for point in ["log", "witness-written", "witness"] {
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
        let stderr_path = f.root.path().join("child.stderr");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "retained_candidate::writable::tests::retained_store_crash_child",
                "--nocapture",
            ])
            .env("EMDA_WRITABLE_RETAINED_CRASH_ROOT", f.root.path())
            .env("EMDA_WRITABLE_RETAINED_CRASH_POINT", point)
            .stdout(Stdio::piped())
            .stderr(File::create(&stderr_path).unwrap())
            .spawn()
            .unwrap();
        let input = BufReader::new(child.stdout.take().unwrap());
        let (tell, wait) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut output = String::new();
            for line in input.lines() {
                match line {
                    Ok(line) if line == "retained commit boundary" => {
                        let _ = tell.send(());
                        break;
                    }
                    Ok(line) => {
                        output.push_str(&line);
                        output.push('\n');
                    }
                    Err(error) => {
                        output.push_str(&format!("could not read child stdout: {error}"));
                        break;
                    }
                }
            }
            output
        });
        let reached = wait.recv_timeout(Duration::from_secs(15));
        let kill = child.kill();
        let status = child.wait().unwrap();
        let stdout = reader.join().unwrap();
        if let Err(error) = reached {
            panic!(
                "child missed retained commit boundary {point}: {error:?} ({status})\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&fs::read(&stderr_path).unwrap())
            );
        }
        kill.unwrap_or_else(|error| panic!("could not kill child at {point}: {error} ({status})"));
        assert!(!status.success(), "child exited successfully at {point}");
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
