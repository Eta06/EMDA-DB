//! Disk-backed access to a fully verified, non-authoritative retained base.
use database_engine::retained_base_v2::{Limits, StagingLimits};
use database_engine::retained_candidate::{
    Expected, IndexLimits, IndexedCandidate, IndexedSuffix, OverlayLimits, RestoreLimits,
    SuffixExpected, SuffixLimits, restore_exported_suffix,
};
use database_engine::{BatchOperation, PublicationFailure, RetainedExportLimits, Store};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

const MAX_VALUE: usize = 16 * 1024 * 1024;
const BASE_LIMITS: Limits = Limits {
    file_bytes: 40 * 1024 * 1024,
    entries: 16,
    key_bytes: 64,
    work_bytes: 40 * 1024 * 1024 + 1,
};
const EXPORT_LIMITS: RetainedExportLimits = RetainedExportLimits {
    max_log_bytes: BASE_LIMITS.file_bytes,
    max_live_keys: BASE_LIMITS.entries as usize,
    max_live_key_bytes: BASE_LIMITS.key_bytes as usize,
    max_output_bytes: BASE_LIMITS.file_bytes,
};

fn candidate(root: &Path, entries: &[(&[u8], &[u8])]) -> Expected {
    let store = Store::open(root.join("source")).unwrap();
    for (key, value) in entries {
        store.put(key, value).unwrap();
    }
    store
        .build_retained_candidate(root.join("candidate"), [3; 16], [4; 16], EXPORT_LIMITS)
        .unwrap()
}

#[test]
fn indexed_pages_and_points_match_the_candidate_under_exact_limits() {
    let root = tempfile::tempdir().unwrap();
    let chunk = vec![1; 64 * 1024];
    let max = vec![2; MAX_VALUE];
    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"a", b""),
        (b"b", &chunk),
        (b"c", &chunk),
        (b"d", b""),
        (b"e", &max),
        (b"f", &max),
        (b"z", b"tail"),
    ];
    let expected = candidate(root.path(), &entries);
    let path = root.path().join("candidate");
    let limits = IndexLimits {
        blocks: 5,
        anchor_bytes: 5,
    };
    for (smaller, message) in [
        (
            IndexLimits {
                blocks: 4,
                ..limits
            },
            "block limit",
        ),
        (
            IndexLimits {
                anchor_bytes: 4,
                ..limits
            },
            "anchor byte limit",
        ),
    ] {
        let error = IndexedCandidate::open(&path, BASE_LIMITS, smaller, &expected)
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains(message), "{error}");
    }
    let reader = IndexedCandidate::open(&path, BASE_LIMITS, limits, &expected).unwrap();
    assert_eq!(reader.metadata().entries, entries.len() as u64);
    assert_eq!(reader.metadata().digest, expected.base_digest);
    assert_eq!(reader.get(b"a", 0).unwrap(), Some(vec![]));
    assert_eq!(
        reader.get(b"b", chunk.len()).unwrap().as_deref(),
        Some(chunk.as_slice())
    );
    assert_eq!(
        reader.get(b"e", MAX_VALUE).unwrap().as_deref(),
        Some(max.as_slice())
    );
    assert_eq!(
        reader.get(b"c", 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    for absent in [b"0".as_slice(), b"bb", b"g", b"zz"] {
        assert_eq!(reader.get(absent, 0).unwrap(), None);
    }
    assert_eq!(
        reader.get(b"", 0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        reader.scan(None, None, 0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(reader.scan(Some(b"d"), Some(b"d"), 1).unwrap().is_empty());
    let middle = reader.scan(Some(b"b"), Some(b"e"), 10).unwrap();
    assert_eq!(
        middle,
        vec![(b"c".to_vec(), chunk), (b"d".to_vec(), vec![])]
    );
    let page = reader.scan(Some(b"d"), Some(b"z"), 10).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].0, b"e");
    assert_eq!(page[0].1, max);
    let next = reader.scan(Some(&page[0].0), None, 2).unwrap();
    assert_eq!(
        next,
        vec![(b"f".to_vec(), max), (b"z".to_vec(), b"tail".to_vec())]
    );
}

#[test]
fn indexed_open_rejects_infeasible_frame_budgets_before_reading_entries() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("source")).unwrap();
    let value = vec![7; 64 * 1024 - 1024];
    for key in [vec![b'a'; 1024], vec![b'b'; 1024]] {
        store.put(&key, &value).unwrap();
    }
    let path = root.path().join("candidate");
    let expected = store
        .build_retained_candidate(
            &path,
            [3; 16],
            [4; 16],
            RetainedExportLimits {
                max_live_key_bytes: 2048,
                ..EXPORT_LIMITS
            },
        )
        .unwrap();
    let base_limits = Limits {
        key_bytes: 2048,
        ..BASE_LIMITS
    };
    let exact = IndexLimits {
        blocks: 2,
        anchor_bytes: 2048,
    };
    let open = |limits| IndexedCandidate::open(&path, base_limits, limits, &expected);
    assert_eq!(open(exact).unwrap().metadata().entries, 2);

    // Lengths and keys alone fit in a block. The frame's value total proves
    // that neither entry can be oversized, but their encoded sum needs two
    // blocks. Both keys must therefore be anchors.
    let file = OpenOptions::new()
        .write(true)
        .open(path.join("base"))
        .unwrap();
    file.write_all_at(&u32::MAX.to_le_bytes(), 116).unwrap();
    for (limits, error) in [
        (
            IndexLimits { blocks: 1, ..exact },
            "retained base block limit exceeded",
        ),
        (
            IndexLimits {
                anchor_bytes: 2047,
                ..exact
            },
            "retained base anchor byte limit exceeded",
        ),
    ] {
        assert_eq!(open(limits).err().unwrap().to_string(), error);
    }
    assert!(open(exact).is_err());

    let suffix_expected = SuffixExpected {
        candidate: expected,
        end: 0,
        digest: Sha256::digest([]).into(),
    };
    let missing_suffix = root.path().join("missing-suffix");
    let too_few_blocks = IndexLimits { blocks: 1, ..exact };
    let suffix = SuffixLimits {
        bytes: 0,
        operations: 0,
        work: 0,
    };
    let overlay = OverlayLimits {
        entries: 0,
        key_bytes: 0,
    };
    for outcome in [
        IndexedSuffix::open(
            &path,
            &missing_suffix,
            base_limits,
            too_few_blocks,
            suffix,
            overlay,
            &suffix_expected,
        ),
        IndexedSuffix::open_file_backed(
            &path,
            &missing_suffix,
            base_limits,
            too_few_blocks,
            suffix,
            overlay,
            &suffix_expected,
        ),
    ] {
        assert_eq!(
            outcome.err().unwrap().to_string(),
            "retained base block limit exceeded"
        );
    }
}

#[test]
fn indexed_reads_authenticate_touched_blocks_from_the_captured_file() {
    let root = tempfile::tempdir().unwrap();
    let large = vec![42; 128 * 1024];
    let expected = candidate(
        root.path(),
        &[(b"a", b"first"), (b"b", &large), (b"c", b"end")],
    );
    let path = root.path().join("candidate");
    let limits = IndexLimits {
        blocks: 3,
        anchor_bytes: 3,
    };
    let wrong = Expected {
        base_digest: [0; 32],
        ..expected.clone()
    };
    assert!(IndexedCandidate::open(&path, BASE_LIMITS, limits, &wrong).is_err());
    let reader = IndexedCandidate::open(&path, BASE_LIMITS, limits, &expected).unwrap();
    let base = path.join("base");
    let original = root.path().join("captured-base");
    fs::rename(&base, &original).unwrap();
    fs::write(&base, b"different inode").unwrap();
    assert!(IndexedCandidate::open(&path, BASE_LIMITS, limits, &expected).is_err());
    assert_eq!(reader.get(b"b", large.len()).unwrap(), Some(large));

    let file = OpenOptions::new().write(true).open(&original).unwrap();
    // First entry starts at 112 and consumes 8 length bytes, one key byte,
    // and five value bytes. Change the last byte of the second value.
    let offset = 112 + 14 + 9 + 128 * 1024 - 1;
    file.write_all_at(&[0], offset).unwrap();
    assert_eq!(
        reader.get(b"b", MAX_VALUE).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        reader.scan(None, None, 2).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(reader.get(b"a", 5).unwrap(), Some(b"first".to_vec()));
    // The verified directory proves that this oversized block holds only b.
    assert_eq!(reader.get(b"bb", 0).unwrap(), None);
    assert_eq!(
        reader.scan(Some(b"b"), Some(b"c"), 1).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    file.set_len(112).unwrap();
    assert_eq!(
        reader.get(b"a", 5).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn empty_candidate_needs_no_anchors() {
    let root = tempfile::tempdir().unwrap();
    let expected = candidate(root.path(), &[]);
    let reader = IndexedCandidate::open(
        root.path().join("candidate"),
        BASE_LIMITS,
        IndexLimits {
            blocks: 0,
            anchor_bytes: 0,
        },
        &expected,
    )
    .unwrap();
    assert_eq!(reader.metadata().entries, 0);
    assert_eq!(reader.get(b"absent", 0).unwrap(), None);
    assert!(reader.scan(None, None, 1).unwrap().is_empty());

    let suffix = root.path().join("empty-suffix");
    fs::write(&suffix, []).unwrap();
    let open = |digest| {
        IndexedSuffix::open_file_backed(
            root.path().join("candidate"),
            &suffix,
            BASE_LIMITS,
            IndexLimits {
                blocks: 0,
                anchor_bytes: 0,
            },
            SuffixLimits {
                bytes: 0,
                operations: 0,
                work: 0,
            },
            OverlayLimits {
                entries: 0,
                key_bytes: 0,
            },
            &SuffixExpected {
                candidate: expected.clone(),
                end: 0,
                digest,
            },
        )
    };
    let view = open(Sha256::digest([]).into()).unwrap();
    assert!(view.scan(None, None, 1).unwrap().is_empty());
    assert_eq!(
        open([0; 32]).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn indexed_suffix_matches_full_replay_across_batch_edits_and_page_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let expected = candidate(
        root.path(),
        &[
            (b"b", b"old"),
            (b"d", b"delete"),
            (b"f", b"untouched"),
            (b"h", b"old"),
        ],
    );
    let store = Store::open(root.path().join("source")).unwrap();
    store
        .write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"first",
            },
            BatchOperation::Put {
                key: b"b",
                value: b"temporary",
            },
            BatchOperation::Delete { key: b"d" },
            BatchOperation::Put {
                key: b"c",
                value: b"middle",
            },
            BatchOperation::Put {
                key: b"b",
                value: b"",
            },
            BatchOperation::Delete { key: b"x" },
        ])
        .unwrap();
    store.delete(b"h").unwrap();
    store.put(b"z", b"last").unwrap();
    let path = root.path().join("candidate");
    let suffix = root.path().join("suffix");
    let suffix_limits = SuffixLimits {
        bytes: 8192,
        operations: 8,
        work: 1000,
    };
    let binding = store
        .export_retained_suffix(
            &path,
            &expected,
            &suffix,
            BASE_LIMITS,
            StagingLimits {
                value_bytes: 64,
                staging_bytes: 1024,
            },
            suffix_limits,
            EXPORT_LIMITS,
        )
        .unwrap();
    let index = IndexLimits {
        blocks: 1,
        anchor_bytes: 1,
    };
    let overlay = OverlayLimits {
        entries: 7,
        key_bytes: 7,
    };
    let open = |suffix_limits, overlay| {
        IndexedSuffix::open(
            &path,
            &suffix,
            BASE_LIMITS,
            index,
            suffix_limits,
            overlay,
            &binding,
        )
    };
    let open_file_backed = |suffix_limits, overlay| {
        IndexedSuffix::open_file_backed(
            &path,
            &suffix,
            BASE_LIMITS,
            index,
            suffix_limits,
            overlay,
            &binding,
        )
    };
    for (reduced_suffix, reduced_overlay, message) in [
        (
            suffix_limits,
            OverlayLimits {
                entries: 6,
                ..overlay
            },
            "overlay entry limit",
        ),
        (
            suffix_limits,
            OverlayLimits {
                key_bytes: 6,
                ..overlay
            },
            "overlay key byte limit",
        ),
        (
            SuffixLimits {
                bytes: binding.end - 1,
                ..suffix_limits
            },
            overlay,
            "suffix byte limit",
        ),
        (
            SuffixLimits {
                operations: 7,
                ..suffix_limits
            },
            overlay,
            "suffix operation limit",
        ),
        (
            SuffixLimits {
                work: 0,
                ..suffix_limits
            },
            overlay,
            "suffix work limit",
        ),
    ] {
        for result in [
            open(reduced_suffix, reduced_overlay),
            open_file_backed(reduced_suffix, reduced_overlay),
        ] {
            let error = result.err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains(message), "{error}");
        }
    }
    let view = open(
        SuffixLimits {
            bytes: binding.end,
            ..suffix_limits
        },
        overlay,
    )
    .unwrap();
    let disk = open_file_backed(
        SuffixLimits {
            bytes: binding.end,
            ..suffix_limits
        },
        overlay,
    )
    .unwrap();
    let expected_state = store.scan(None, None, 10).unwrap();
    assert_eq!(view.candidate_metadata().entries, 4);
    assert_eq!(view.scan(None, None, 10).unwrap(), expected_state);
    assert_eq!(disk.scan(None, None, 10).unwrap(), expected_state);
    assert_eq!(view.scan(None, Some(b"b"), 1).unwrap(), expected_state[..1]);
    assert_eq!(
        view.scan(Some(b"a"), Some(b"f"), 10).unwrap(),
        expected_state[1..3]
    );
    assert_eq!(view.scan(Some(b"z"), None, 10).unwrap(), vec![]);
    let mut after = None;
    for entry in expected_state {
        let page = view.scan(after.as_deref(), None, 1).unwrap();
        assert_eq!(disk.scan(after.as_deref(), None, 1).unwrap(), page);
        assert_eq!(page, [entry]);
        after = Some(page.into_iter().next().unwrap().0);
    }
    assert!(view.scan(after.as_deref(), None, 1).unwrap().is_empty());
    assert_eq!(view.get(b"b", 0).unwrap(), Some(vec![]));
    assert_eq!(disk.get(b"b", 0).unwrap(), Some(vec![]));
    assert_eq!(view.get(b"f", 9).unwrap(), Some(b"untouched".to_vec()));
    assert_eq!(
        view.get(b"a", 4).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    for absent in [b"d", b"h", b"x"] {
        assert_eq!(view.get(absent, 0).unwrap(), None);
        assert_eq!(disk.get(absent, 0).unwrap(), None);
    }
    let mut wrong_binding = binding.clone();
    wrong_binding.digest[0] ^= 1;
    assert_eq!(
        IndexedSuffix::open_file_backed(
            &path,
            &suffix,
            BASE_LIMITS,
            index,
            suffix_limits,
            overlay,
            &wrong_binding,
        )
        .err()
        .unwrap()
        .to_string(),
        "committed suffix digest mismatch"
    );

    // Both views retain the verified source across replacement of its name.
    let original = root.path().join("original-suffix");
    fs::rename(&suffix, &original).unwrap();
    fs::write(&suffix, vec![0; binding.end as usize]).unwrap();
    assert!(open(suffix_limits, overlay).is_err());
    assert!(open_file_backed(suffix_limits, overlay).is_err());
    assert_eq!(view.get(b"z", 4).unwrap(), Some(b"last".to_vec()));
    assert_eq!(disk.get(b"z", 4).unwrap(), Some(b"last".to_vec()));

    let file = OpenOptions::new().write(true).open(&original).unwrap();
    // The last record ends with the value followed by a four-byte CRC.
    file.write_all_at(b"X", binding.end - 4 - b"last".len() as u64)
        .unwrap();
    assert_eq!(
        disk.get(b"z", 4).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        disk.scan(Some(b"y"), None, 1).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(view.get(b"z", 4).unwrap(), Some(b"last".to_vec()));
    file.set_len(binding.end + 1).unwrap();
    assert_eq!(
        disk.get(b"f", 9).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn a_page_filled_by_earlier_suffix_keys_need_not_read_the_next_base_block() {
    let root = tempfile::tempdir().unwrap();
    let large = vec![42; 256 * 1024];
    let expected = candidate(root.path(), &[(b"b", &large), (b"z", b"base tail")]);
    let store = Store::open(root.path().join("source")).unwrap();
    store.put(b"a", b"suffix value").unwrap();
    let path = root.path().join("candidate");
    let suffix = root.path().join("suffix");
    let limits = SuffixLimits {
        bytes: 4096,
        operations: 1,
        work: 64,
    };
    let binding = store
        .export_retained_suffix(
            &path,
            &expected,
            &suffix,
            BASE_LIMITS,
            StagingLimits {
                value_bytes: large.len(),
                staging_bytes: 2 * large.len() as u64,
            },
            limits,
            EXPORT_LIMITS,
        )
        .unwrap();
    let view = IndexedSuffix::open_file_backed(
        &path,
        &suffix,
        BASE_LIMITS,
        IndexLimits {
            blocks: 2,
            anchor_bytes: 2,
        },
        limits,
        OverlayLimits {
            entries: 1,
            key_bytes: 1,
        },
        &binding,
    )
    .unwrap();
    let first = vec![(b"a".to_vec(), b"suffix value".to_vec())];
    assert_eq!(view.scan(None, None, 1).unwrap(), first);
    assert_eq!(
        view.scan(Some(b"a"), None, 1).unwrap(),
        [(b"b".to_vec(), large.clone())]
    );

    let file = OpenOptions::new()
        .write(true)
        .open(path.join("base"))
        .unwrap();
    file.write_all_at(&[0], 112 + 8 + 1 + large.len() as u64 - 1)
        .unwrap();
    assert_eq!(view.scan(None, None, 1).unwrap(), first);
    assert_eq!(
        view.scan(None, None, 2).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        view.scan(Some(b"a"), None, 1).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn suffix_values_share_the_page_byte_budget_with_base_values() {
    let root = tempfile::tempdir().unwrap();
    let max = vec![23; MAX_VALUE];
    let expected = candidate(root.path(), &[(b"a", &max), (b"z", b"base tail")]);
    let store = Store::open(root.path().join("source")).unwrap();
    store.put(b"b", &max).unwrap();
    store.put(b"c", b"suffix tail").unwrap();
    let path = root.path().join("candidate");
    let suffix = root.path().join("suffix");
    let limits = SuffixLimits {
        bytes: MAX_VALUE as u64 + 1024,
        operations: 2,
        work: 100,
    };
    let binding = store
        .export_retained_suffix(
            &path,
            &expected,
            &suffix,
            BASE_LIMITS,
            StagingLimits {
                value_bytes: MAX_VALUE,
                staging_bytes: 2 * MAX_VALUE as u64 + 1024,
            },
            limits,
            EXPORT_LIMITS,
        )
        .unwrap();
    let view = IndexedSuffix::open_file_backed(
        &path,
        &suffix,
        BASE_LIMITS,
        IndexLimits {
            blocks: 2,
            anchor_bytes: 2,
        },
        limits,
        OverlayLimits {
            entries: 2,
            key_bytes: 2,
        },
        &binding,
    )
    .unwrap();
    let first = view.scan(None, None, 4).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, b"a");
    assert_eq!(first[0].1, max);
    let next = view.scan(Some(b"a"), None, 4).unwrap();
    assert_eq!(
        next,
        [
            (b"b".to_vec(), max),
            (b"c".to_vec(), b"suffix tail".to_vec()),
            (b"z".to_vec(), b"base tail".to_vec()),
        ]
    );
    let destination = root.path().join("restored");
    restore_exported_suffix(
        &path,
        &suffix,
        &destination,
        BASE_LIMITS,
        StagingLimits {
            value_bytes: MAX_VALUE,
            staging_bytes: 2 * MAX_VALUE as u64 + 1024,
        },
        limits,
        &binding,
        RestoreLimits {
            log_bytes: EXPORT_LIMITS.max_log_bytes,
            live_keys: 4,
            live_key_bytes: 4,
        },
    )
    .unwrap();
    let restored = Store::open(&destination).unwrap();
    assert_eq!(restored.scan(None, None, 4).unwrap(), first);
    assert_eq!(restored.scan(Some(b"a"), None, 4).unwrap(), next);
}

#[test]
fn new_history_restore_enforces_intermediate_batch_budgets() {
    let root = tempfile::tempdir().unwrap();
    let expected = candidate(root.path(), &[(b"a", b"x"), (b"b", b"")]);
    let source = Store::open(root.path().join("source")).unwrap();
    source
        .write_batch(&[
            BatchOperation::Put {
                key: b"z",
                value: b"yy",
            },
            BatchOperation::Delete { key: b"a" },
            BatchOperation::Put {
                key: b"z",
                value: b"",
            },
        ])
        .unwrap();
    let candidate_path = root.path().join("candidate");
    let suffix_path = root.path().join("suffix");
    let mut suffix = SuffixLimits {
        bytes: 1024,
        operations: 3,
        work: 26,
    };
    let base = Limits {
        entries: 3,
        key_bytes: 3,
        ..BASE_LIMITS
    };
    let staging = StagingLimits {
        value_bytes: 2,
        staging_bytes: 6,
    };
    let binding = source
        .export_retained_suffix(
            &candidate_path,
            &expected,
            &suffix_path,
            base,
            staging,
            suffix,
            EXPORT_LIMITS,
        )
        .unwrap();
    suffix.bytes = binding.end;
    let destination = root.path().join("restored");
    let output = RestoreLimits {
        log_bytes: 1024,
        live_keys: 2,
        live_key_bytes: 2,
    };
    let restore = |base, staging, suffix| {
        restore_exported_suffix(
            &candidate_path,
            &suffix_path,
            &destination,
            base,
            staging,
            suffix,
            &binding,
            output,
        )
    };
    for (base, staging, suffix, message) in [
        (
            Limits { entries: 2, ..base },
            staging,
            suffix,
            "live set limit",
        ),
        (
            Limits {
                key_bytes: 2,
                ..base
            },
            staging,
            suffix,
            "live set limit",
        ),
        (
            base,
            StagingLimits {
                staging_bytes: 5,
                ..staging
            },
            suffix,
            "live set limit",
        ),
        (
            base,
            StagingLimits {
                value_bytes: 1,
                ..staging
            },
            suffix,
            "value limit",
        ),
        (
            base,
            staging,
            SuffixLimits { work: 25, ..suffix },
            "suffix work limit",
        ),
        (
            base,
            staging,
            SuffixLimits {
                operations: 2,
                ..suffix
            },
            "suffix operation limit",
        ),
        (
            base,
            staging,
            SuffixLimits {
                bytes: binding.end - 1,
                ..suffix
            },
            "suffix byte limit",
        ),
    ] {
        let Err(PublicationFailure::NotPublished(error)) = restore(base, staging, suffix) else {
            panic!("restore exceeded {message}");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(message), "{error}");
        assert!(!destination.exists());
    }
    restore(base, staging, suffix).unwrap();
    let restored = Store::open(&destination).unwrap();
    assert_eq!(
        restored.scan(None, None, 3).unwrap(),
        source.scan(None, None, 3).unwrap()
    );
}
