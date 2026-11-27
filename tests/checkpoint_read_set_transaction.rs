use database_engine::{
    BatchOperation, CheckpointStore, CheckpointTransactionMode, MAX_TRANSACTION_READ_CONSTRAINTS,
    Store,
};
use std::io;
use std::time::{Duration, Instant};

fn fixture() -> (tempfile::TempDir, CheckpointStore) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    ordinary.put(b"a", b"old").unwrap();
    ordinary.put(b"c", b"base").unwrap();
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    (dir, store)
}

#[test]
fn cumulative_validation_limit_counts_points_and_complete_ranges_across_generations() {
    for cap in [5, 6] {
        let (dir, store) = fixture();
        let log = dir.path().join("store/records.log");
        let mut before_commit = Vec::new();
        let result = store.transaction_with_read_set_validation_limit(cap, |tx| {
            assert!(tx.get(b"missing")?.is_none());
            // The page returns one key, but both base keys are dependencies.
            assert_eq!(tx.scan(None, Some(b"d"), 1)?.len(), 1);
            tx.put(b"staged", b"value")?;
            store.put(b"z1", b"independent")?;
            assert!(tx.get(b"missing")?.is_none());
            assert!(tx.get(b"missing")?.is_none());
            store.put(b"z2", b"independent")?;
            before_commit = std::fs::read(&log)?;
            Ok(())
        });
        if cap == 5 {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert_eq!(
                error.to_string(),
                "checkpoint transaction cumulative validation limit exceeded"
            );
            assert!(!database_engine::is_transaction_conflict(&error));
            assert_eq!(store.get(b"staged").unwrap(), None);
            assert_eq!(std::fs::read(&log).unwrap(), before_commit);
        } else {
            result.unwrap();
            assert_eq!(
                store.get(b"staged").unwrap().as_deref(),
                Some(b"value".as_slice())
            );
        }
        let status = store.transaction_budget_status().unwrap();
        assert_eq!(status.reserved_bytes, 0);
        assert_eq!(status.active_transactions, 0);
        store.put(b"after", b"writer gate released").unwrap();
        drop(store);
        let recovered = Store::open(dir.path().join("store")).unwrap();
        assert_eq!(
            recovered.get(b"staged").unwrap(),
            (cap == 6).then(|| b"value".to_vec())
        );
        assert_eq!(
            recovered.get(b"z2").unwrap().as_deref(),
            Some(b"independent".as_slice())
        );
    }
}

#[test]
fn caught_validation_limit_errors_prevent_append_and_charged_phantoms_conflict() {
    for cap in [0, 1] {
        let (dir, store) = fixture();
        store
            .transaction_with_read_set_validation_limit(0, |tx| {
                assert!(tx.get(b"a")?.is_some());
                assert!(tx.scan(Some(b"p"), Some(b"r"), 1)?.is_empty());
                store.rollover_to(dir.path().join("rolled"))?;
                assert!(tx.get(b"a")?.is_some());
                tx.put(b"same-generation", b"allowed")
            })
            .unwrap();
        let log = dir.path().join("store/records.log");
        let mut before_commit = Vec::new();
        let outcome = store.transaction_with_read_set_validation_limit(cap, |tx| {
            assert!(tx.scan(Some(b"p"), Some(b"r"), 1)?.is_empty());
            tx.put(b"staged", b"value")?;
            store.put(b"q", b"phantom")?;
            before_commit = std::fs::read(&log)?;
            let observed = tx.get(b"a").unwrap_err();
            assert_eq!(
                database_engine::is_transaction_conflict(&observed),
                cap == 1
            );
            if cap == 0 {
                assert_eq!(
                    observed.to_string(),
                    "checkpoint transaction cumulative validation limit exceeded"
                );
            }
            Ok(())
        });
        let error = outcome.unwrap_err();
        assert_eq!(database_engine::is_transaction_conflict(&error), cap == 1);
        if cap == 0 {
            assert_eq!(
                error.to_string(),
                "checkpoint transaction cumulative validation limit exceeded"
            );
        }
        assert_eq!(std::fs::read(&log).unwrap(), before_commit);
        assert_eq!(store.get(b"staged").unwrap(), None);
        assert_eq!(store.transaction_budget_status().unwrap().reserved_bytes, 0);
    }
}

#[test]
fn unrelated_writes_can_commit_but_observed_present_versions_conflict() {
    let (_dir, store) = fixture();
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
            store.put(b"unrelated", b"changed")?;
            tx.put(b"a", b"new")
        })
        .unwrap();
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
    let result = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"new"[..]));
        store.put(b"a", b"other")?;
        store.put(b"a", b"new")?;
        tx.put(b"must-not-commit", b"x")
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(store.get(b"must-not-commit").unwrap(), None);
}

#[test]
fn a_concurrent_atomic_update_cannot_expose_a_fractured_pair() {
    let (_dir, store) = fixture();
    let result = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
        store.write_batch(&[
            BatchOperation::Put {
                key: b"a",
                value: b"new-a",
            },
            BatchOperation::Put {
                key: b"c",
                value: b"new-c",
            },
        ])?;
        assert_eq!(tx.get(b"c").unwrap_err().kind(), io::ErrorKind::WouldBlock);
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn observed_conflicts_stay_conflicts_even_if_callback_catches_them() {
    let (_dir, store) = fixture();
    let result = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.get(b"missing")?, None);
        store.put(b"missing", b"temporary")?;
        assert_eq!(
            tx.get(b"missing").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        store.delete(b"missing")?;
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    // If the temporary value was never observed, absence can match again.
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"missing")?, None);
            store.put(b"missing", b"temporary")?;
            store.delete(b"missing")?;
            tx.put(b"after-absence", b"ok")
        })
        .unwrap();
    assert_eq!(
        store.get(b"after-absence").unwrap().as_deref(),
        Some(&b"ok"[..])
    );
}

#[test]
fn scans_validate_the_entire_range_and_ignore_unrelated_writes() {
    let (_dir, store) = fixture();
    store
        .transaction_with_read_set(|tx| {
            tx.put(b"a", b"staged")?;
            store.put(b"a", b"independent")?;
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"staged"[..]));
            Ok(())
        })
        .unwrap();
    assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"staged"[..]));
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.scan(Some(b"a"), Some(b"d"), 10)?.len(), 1);
            store.put(b"outside-range", b"changed")?;
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"staged"[..]));
            Ok(())
        })
        .unwrap();
    let result = store.transaction_with_read_set(|tx| {
        assert_eq!(tx.scan(Some(b"a"), Some(b"d"), 1)?.len(), 1);
        store.put(b"b", b"phantom")?;
        assert_eq!(tx.get(b"a").unwrap_err().kind(), io::ErrorKind::WouldBlock);
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    // An unrelated write before the first scan is permitted when earlier points still match.
    store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"staged"[..]));
            store.put(b"other", b"changed")?;
            assert_eq!(tx.scan(Some(b"a"), Some(b"d"), 10)?.len(), 2);
            tx.put(b"done", b"ok")
        })
        .unwrap();
}

#[test]
fn scan_validation_covers_entries_beyond_the_returned_page() {
    let (_dir, store) = fixture();
    store.put(b"e", b"last").unwrap();
    let result = store.transaction_with_read_set(|tx| {
        let page = tx.scan(None, None, 1)?;
        assert_eq!(page[0].0, b"a");
        store.put(b"e", b"changed")?;
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    // An insertion and deletion never observed by the transaction can leave
    // an identical range at validation, allowing serialization afterwards.
    store
        .transaction_with_read_set(|tx| {
            tx.scan(Some(b"a"), Some(b"d"), 1)?;
            store.put(b"b", b"temporary")?;
            store.delete(b"b")?;
            tx.put(b"done", b"ok")
        })
        .unwrap();
    let result = store.transaction_with_read_set(|tx| {
        tx.scan(Some(b"a"), Some(b"d"), 1)?;
        store.put(b"b", b"temporary")?;
        assert_eq!(tx.get(b"a").unwrap_err().kind(), io::ErrorKind::WouldBlock);
        store.delete(b"b")?;
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn repeated_scan_rechecks_checkpoint_after_using_new_range_with_staged_entries() {
    use std::fs::OpenOptions;
    use std::os::unix::fs::FileExt;

    let (dir, store) = fixture();
    // Disable the default verified-block cache to verify that recorded read-set
    // ranges do not bypass checkpoint block verification on repeated scans.
    store.set_checkpoint_block_cache_limit(0).unwrap();
    let result = store.transaction_with_read_set(|tx| {
        tx.delete(b"a")?;
        tx.put(b"b", b"staged")?;
        assert_eq!(
            tx.scan(None, None, 2)?,
            vec![
                (b"b".to_vec(), b"staged".to_vec()),
                (b"c".to_vec(), b"base".to_vec()),
            ],
        );
        // The change does not touch the live log or advance its generation.
        // Both the original range and a covered page must recheck the block.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.path().join("checkpoint"))?;
        let entry_start = 68;
        let mut byte = [0_u8; 1];
        file.read_exact_at(&mut byte, entry_start)?;
        file.write_all_at(&[byte[0] ^ 1], entry_start)?;
        let repeated = tx.scan(None, None, 2);
        let narrower = tx.scan(Some(b"b"), None, 1);
        let point = tx.get(b"c");
        file.write_all_at(&byte, entry_start)?;
        assert_eq!(narrower.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert_eq!(point.unwrap_err().kind(), io::ErrorKind::InvalidData);
        repeated
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    assert_eq!(store.get(b"b").unwrap(), None);
    assert_eq!(store.transaction_budget_status().unwrap().reserved_bytes, 0);
}

#[test]
fn range_observations_survive_rollover_and_validate_later_writes() {
    let (dir, store) = fixture();
    let next = dir.path().join("next-checkpoint");
    let digest = store
        .transaction_with_read_set(|tx| {
            assert_eq!(tx.scan(Some(b"a"), Some(b"d"), 10)?.len(), 1);
            let digest = store.rollover_to(&next)?;
            store.put(b"outside", b"changed")?;
            tx.put(b"result", b"committed")?;
            Ok(digest)
        })
        .unwrap();
    drop(store);
    let reopened = CheckpointStore::open(dir.path().join("store"), &next, &digest).unwrap();
    assert_eq!(
        reopened.get(b"result").unwrap().as_deref(),
        Some(&b"committed"[..])
    );
}

#[test]
fn range_validation_preserves_variable_length_binary_key_boundaries() {
    let (dir, store) = fixture();
    for key in [b"a\0".as_slice(), b"aa", b"az", b"b", b"ba"] {
        store.put(key, b"value").unwrap();
    }
    store
        .rollover_to(dir.path().join("packed-key-base"))
        .unwrap();
    store
        .transaction_with_read_set(|tx| {
            let page = tx.scan(Some(b"a"), Some(b"c"), 2)?;
            assert_eq!(page[0].0, b"a\0");
            assert_eq!(page[1].0, b"aa");
            store.put(b"outside", b"changed")?;
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
            Ok(())
        })
        .unwrap();
    let result = store.transaction_with_read_set(|tx| {
        tx.scan(Some(b"a"), Some(b"c"), 1)?;
        store.put(b"ba", b"changed")?;
        Ok(())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn point_dependency_limit_rejects_before_returning_an_untracked_read() {
    let (_dir, store) = fixture();
    let result = store.transaction_with_read_set(|tx| {
        for index in 0..=MAX_TRANSACTION_READ_CONSTRAINTS {
            assert_eq!(tx.get(format!("missing-{index:05}").as_bytes())?, None);
        }
        Ok(())
    });
    assert!(result.is_err());
    store.put(b"later", b"ok").unwrap();
}

#[test]
fn read_set_modes_work_with_fifo_and_deadlines() {
    let (_dir, store) = fixture();
    store
        .transaction_with_read_set_fifo(|tx| {
            assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
            store.put(b"unrelated", b"changed")?;
            tx.put(b"result", b"fifo")
        })
        .unwrap();
    for mode in [
        CheckpointTransactionMode::ReadSet,
        CheckpointTransactionMode::ReadSetFifo,
    ] {
        store
            .transaction_until(Instant::now() + Duration::from_secs(30), mode, |tx| {
                assert_eq!(tx.get(b"a")?.as_deref(), Some(&b"old"[..]));
                store.put(b"unrelated", b"again")?;
                tx.put(b"result", b"timed")
            })
            .unwrap();
    }
    assert_eq!(
        store.get(b"result").unwrap().as_deref(),
        Some(&b"timed"[..])
    );
}

#[test]
fn full_range_item_limit_rejects_before_returning_a_partial_page() {
    use database_engine::MAX_TRANSACTION_VALIDATION_ITEMS;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large-store");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    let keys = (0..=MAX_TRANSACTION_VALIDATION_ITEMS)
        .map(|index| format!("k{index:05}").into_bytes())
        .collect::<Vec<_>>();
    for chunk in keys.chunks(1024) {
        let operations = chunk
            .iter()
            .map(|key| BatchOperation::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        ordinary.write_batch(&operations).unwrap();
    }
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    // Exactly the supported number of observed entries is admitted.
    store
        .transaction_with_read_set(|tx| {
            let page = tx.scan(None, Some(keys.last().unwrap()), 1)?;
            assert_eq!(page.len(), 1);
            Ok(())
        })
        .unwrap();
    // The requested page is small, but its complete range exceeds the cap.
    let result = store.transaction_with_read_set(|tx| tx.scan(None, None, 1));
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("range observation limit")
    );
    store.put(b"later", b"ok").unwrap();
}

#[test]
fn full_range_key_byte_limit_counts_observed_keys_and_bounds() {
    use database_engine::MAX_TRANSACTION_READ_CONSTRAINT_BYTES;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("long-keys");
    let checkpoint = dir.path().join("checkpoint");
    let ordinary = Store::open(&path).unwrap();
    let count = MAX_TRANSACTION_READ_CONSTRAINT_BYTES / 1024;
    let keys = (0..=count)
        .map(|index| {
            let mut key = vec![b'x'; 1024];
            key[..4].copy_from_slice(&(index as u32).to_be_bytes());
            key
        })
        .collect::<Vec<_>>();
    for chunk in keys.chunks(1024) {
        let operations = chunk
            .iter()
            .map(|key| BatchOperation::Put { key, value: b"v" })
            .collect::<Vec<_>>();
        ordinary.write_batch(&operations).unwrap();
    }
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    drop(ordinary);
    let store = CheckpointStore::open(&path, &checkpoint, &digest).unwrap();
    // One bound and 2,047 observed keys exactly fill the byte allowance.
    store
        .transaction_with_read_set(|tx| {
            let mut after = None;
            let mut seen = 0;
            loop {
                let page = tx.scan(after, Some(&keys[count - 1]), 256)?;
                if page.is_empty() {
                    break;
                }
                for (key, value) in &page {
                    assert_eq!(key, &keys[seen]);
                    assert_eq!(value, b"v");
                    seen += 1;
                }
                after = Some(keys[seen - 1].as_slice());
            }
            assert_eq!(seen, count - 1);
            let reserved = store.transaction_budget_status()?.reserved_bytes;
            for key in [&keys[0], &keys[count / 2], &keys[count - 2]] {
                assert_eq!(tx.get(key)?.as_deref(), Some(b"v".as_slice()));
            }
            assert_eq!(tx.get(b"\0\0\0\0")?, None);
            assert_eq!(store.transaction_budget_status()?.reserved_bytes, reserved);
            assert!(
                tx.get(&keys[count - 1])
                    .unwrap_err()
                    .to_string()
                    .contains("read-set limit exceeded")
            );
            Ok(())
        })
        .unwrap();
    let result = store.transaction_with_read_set(|tx| tx.scan(None, None, 1));
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("range observation limit")
    );
    store.put(b"later", b"ok").unwrap();
}

#[test]
fn covering_ranges_preserve_phantom_checks_and_never_cover_wider_bounds() {
    let (_dir, store) = fixture();
    let result = store.transaction_with_read_set(|tx| {
        let first = tx.scan(None, None, 1)?;
        assert_eq!(first[0].0, b"a");
        tx.delete(b"c")?;
        tx.put(b"b", b"staged")?;
        let second = tx.scan(Some(b"a"), Some(b"d"), 1)?;
        assert_eq!(second, [(b"b".to_vec(), b"staged".to_vec())]);
        store.put(b"z", b"phantom outside the second page")?;
        Ok(())
    });
    assert!(database_engine::is_transaction_conflict(
        &result.unwrap_err()
    ));
    assert_eq!(store.get(b"b").unwrap(), None);
    assert_eq!(
        store.get(b"c").unwrap().as_deref(),
        Some(b"base".as_slice())
    );

    for (after, before, outside) in [
        (None, Some(b"d".as_slice()), b"a".as_slice()),
        (Some(b"a".as_slice()), None, b"z".as_slice()),
    ] {
        let result = store.transaction_with_read_set(|tx| {
            tx.scan(Some(b"a"), Some(b"d"), 1)?;
            tx.scan(after, before, 1)?;
            store.put(outside, b"new version")?;
            Ok(())
        });
        assert!(database_engine::is_transaction_conflict(
            &result.unwrap_err()
        ));
    }
}

#[test]
fn covered_point_reads_keep_gaps_and_exclusive_range_boundaries_correct() {
    let (_dir, store) = fixture();
    for (key, original) in [(b"a", Some(b"old".as_slice())), (b"b", None), (b"d", None)] {
        let result = store.transaction_with_read_set(|tx| {
            tx.scan(Some(b"a"), Some(b"d"), 1)?;
            let before_point = store.transaction_budget_status()?.reserved_bytes;
            assert_eq!(tx.get(key)?.as_deref(), original);
            let after_point = store.transaction_budget_status()?.reserved_bytes;
            if key == b"b" {
                assert_eq!(before_point, after_point);
            } else {
                assert!(after_point > before_point);
            }
            tx.put(b"receipt", b"must not commit")?;
            store.put(key, b"changed")?;
            Ok(())
        });
        assert!(database_engine::is_transaction_conflict(
            &result.unwrap_err()
        ));
        assert_eq!(store.get(b"receipt").unwrap(), None);
    }
}
