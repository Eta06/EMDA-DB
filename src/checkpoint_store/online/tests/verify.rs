//! Final source/candidate comparison: complete ordered live sets, bounded
//! windows and streamed values. These check detection, not elapsed time.
use super::*;

fn verify_unlimited(source: &CheckpointStore, candidate: &CheckpointStore) -> io::Result<()> {
    verify_candidate(source, candidate, &mut SourceReadBudget(None))
}

type Changes = Vec<(Vec<u8>, Option<Vec<u8>>)>;

// A checkpoint store whose live set is split between the checkpoint and the
// suffix overlay, so the merged traversal is exercised.
fn store(
    root: &Path,
    name: &str,
    base: &[(Vec<u8>, Vec<u8>)],
    suffix: &[(Vec<u8>, Option<Vec<u8>>)],
) -> CheckpointStore {
    let path = root.join(name);
    let checkpoint = root.join(format!("{name}.checkpoint"));
    let ordinary = Store::open(&path).unwrap();
    for chunk in base.chunks(256) {
        let operations = chunk
            .iter()
            .map(|(key, value)| crate::BatchOperation::Put { key, value })
            .collect::<Vec<_>>();
        ordinary.write_batch(&operations).unwrap();
    }
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    for (key, value) in suffix {
        match value {
            Some(value) => store.put(key, value).unwrap(),
            None => store.delete(key).unwrap(),
        }
    }
    store
}

fn entries(
    count: usize,
    key_bytes: usize,
    value: impl Fn(usize) -> Vec<u8>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|index| {
            let mut key = format!("{index:08}").into_bytes();
            key.resize(key_bytes.max(8), b'k');
            (key, value(index))
        })
        .collect()
}

fn is_mismatch(result: io::Result<()>) -> bool {
    result.is_err_and(|error| {
        error.kind() == io::ErrorKind::InvalidData && error.to_string().contains("does not match")
    })
}

fn is_corruption(result: io::Result<()>) -> bool {
    result.is_err_and(|error| error.to_string().contains("checksum mismatch"))
}

#[test]
fn identical_live_sets_match_across_count_and_byte_windows() {
    let root = tempfile::tempdir().unwrap();
    // 2,600 short keys span three count windows; 600 long keys span three
    // byte windows. Some live entries come from each side's overlay.
    for (count, key_bytes) in [(2_600, 8), (600, 1_000)] {
        let base = entries(count, key_bytes, |index| index.to_le_bytes().to_vec());
        let suffix = vec![
            (base[5].0.clone(), Some(b"overwritten".to_vec())),
            (base[count - 1].0.clone(), None),
            (b"zz-suffix-only".to_vec(), Some(b"new".to_vec())),
        ];
        let source = store(root.path(), &format!("source-{key_bytes}"), &base, &suffix);
        // The candidate holds the same live set entirely in its checkpoint.
        let mut flattened = base.clone();
        flattened[5].1 = b"overwritten".to_vec();
        flattened.pop();
        flattened.push((b"zz-suffix-only".to_vec(), b"new".to_vec()));
        let candidate = store(
            root.path(),
            &format!("candidate-{key_bytes}"),
            &flattened,
            &[],
        );
        verify_unlimited(&source, &candidate).unwrap();
        verify_unlimited(&candidate, &source).unwrap();
    }
}

#[test]
fn key_count_and_value_differences_are_mismatches_in_every_window() {
    let root = tempfile::tempdir().unwrap();
    let base = entries(2_100, 8, |index| index.to_le_bytes().to_vec());
    let source = store(root.path(), "source", &base, &[]);
    let last = base.len() - 1;
    let cases: [(&str, Changes); 6] = [
        (
            "missing in a later window",
            vec![(base[1_500].0.clone(), None)],
        ),
        ("missing last", vec![(base[last].0.clone(), None)]),
        (
            "extra inside a window",
            vec![(b"00001024a".to_vec(), Some(b"x".to_vec()))],
        ),
        (
            "extra after the end",
            vec![(b"zzzz".to_vec(), Some(b"x".to_vec()))],
        ),
        (
            "same-length value",
            vec![(base[1_030].0.clone(), Some(b"XXXXXXXX".to_vec()))],
        ),
        (
            "longer value",
            vec![(base[0].0.clone(), Some(b"0123456789".to_vec()))],
        ),
    ];
    for (name, change) in cases {
        let candidate = store(root.path(), name, &base, &change);
        assert!(is_mismatch(verify_unlimited(&source, &candidate)), "{name}");
        assert!(
            is_mismatch(verify_unlimited(&candidate, &source)),
            "{name} reversed"
        );
    }
    let empty = store(root.path(), "empty", &[], &[]);
    assert!(is_mismatch(verify_unlimited(&source, &empty)));
    assert!(is_mismatch(verify_unlimited(&empty, &source)));
    let also_empty = store(root.path(), "also-empty", &[], &[]);
    verify_unlimited(&empty, &also_empty).unwrap();
}

#[test]
fn large_values_stream_and_detect_late_differences_and_corruption() {
    let root = tempfile::tempdir().unwrap();
    // Values span several 64 KiB chunks and exceed one scan page together.
    let large = |seed: u8| {
        (0..300 * 1024)
            .map(|index| (index as u8).wrapping_add(seed))
            .collect::<Vec<_>>()
    };
    let base = vec![
        (b"a".to_vec(), large(1)),
        (b"b".to_vec(), large(2)),
        (b"c".to_vec(), b"small".to_vec()),
    ];
    let source = store(root.path(), "source", &base, &[]);
    let same = store(root.path(), "same", &base, &[]);
    verify_unlimited(&source, &same).unwrap();

    // A difference in the last chunk only.
    let mut late = large(2);
    *late.last_mut().unwrap() ^= 1;
    let changed = store(root.path(), "late", &base, &[(b"b".to_vec(), Some(late))]);
    assert!(is_mismatch(verify_unlimited(&source, &changed)));

    // A first-chunk difference followed by corruption later in the same value
    // is still reported as corruption, on either side.
    let mut early = large(2);
    early[0] ^= 1;
    for corrupt_source in [false, true] {
        let name = format!("early-{corrupt_source}");
        let differing = store(
            root.path(),
            &name,
            &base,
            &[(b"b".to_vec(), Some(early.clone()))],
        );
        let (victim, victim_log) = if corrupt_source {
            (&source, root.path().join("source/records.log"))
        } else {
            (&differing, root.path().join(format!("{name}/records.log")))
        };
        let mut location = None;
        victim
            .read()
            .unwrap()
            .view
            .visit_entries(Some(b"a"), Some(b"c"), |_, found| {
                location = Some(found);
                Ok(std::ops::ControlFlow::Break(()))
            })
            .unwrap();
        let location = location.unwrap();
        let offset = location.offset + 200 * 1024;
        let original = {
            use std::os::unix::fs::FileExt;
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&victim_log)
                .unwrap();
            let mut byte = [0];
            file.read_exact_at(&mut byte, offset).unwrap();
            file.write_all_at(&[byte[0] ^ 0x55], offset).unwrap();
            (file, byte)
        };
        assert!(
            is_corruption(verify_unlimited(&source, &differing)),
            "{name}"
        );
        assert!(
            is_corruption(verify_unlimited(&differing, &source)),
            "{name} reversed"
        );
        use std::os::unix::fs::FileExt;
        original.0.write_all_at(&original.1, offset).unwrap();
    }
    verify_unlimited(&source, &same).unwrap();
}
