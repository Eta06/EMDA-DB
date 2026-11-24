use super::*;
use crate::test_support::retry_lock;
use crate::{COMPACTED_CHECKPOINT_FILE, CheckpointStoreLimits, HandoffFailure, Store, log::Log};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const TAIL_BYTES: u64 = 4096;
const TAIL_KEYS: usize = 2;
const TAIL_KEY_BYTES: usize = 128;
const LOG_LIMIT: u64 = 1_000_000;

fn fixture(root: &Path, managed: bool, automatic: bool) -> CheckpointStore {
    let path = root.join("live");
    let checkpoint = root.join("base");
    let store = retry_lock(|| Store::open(&path)).unwrap();
    store.put(b"base", b"initial").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let cp = retry_lock(|| {
        CheckpointStore::open_with_resource_limits(
            &path,
            &checkpoint,
            &digest,
            TAIL_BYTES,
            TAIL_KEYS,
            TAIL_KEY_BYTES,
            Some(LOG_LIMIT),
        )
    })
    .unwrap();
    if managed {
        cp.rollover_managed(root.join("managed")).unwrap();
    }
    if automatic {
        cp.enable_automatic_checkpoints().unwrap();
    }
    cp.put(b"a", b"old").unwrap();
    cp.put(b"a", b"latest").unwrap();
    cp.put(b"b", b"value").unwrap();
    cp
}
fn verify_store(path: &Path) {
    let store = retry_lock(|| Store::open(path)).unwrap();
    assert_eq!(store.get(b"base").unwrap(), Some(b"initial".to_vec()));
    assert_eq!(store.get(b"a").unwrap(), Some(b"latest".to_vec()));
    assert_eq!(store.get(b"b").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn checkpoint_handoff_log_budgets_preserve_selection_on_rejection() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("live");
    let previous = root.path().join("previous");
    let store = fixture(root.path(), true, false);
    let log_original = fs::read(path.join("records.log")).unwrap();
    let witness_original = fs::read(path.join("commit.state")).unwrap();
    let selection_original = fs::read(selection::path(&path)).unwrap();
    let live_values = (b"initial".len() + b"latest".len() + b"value".len()) as u64;
    let source_limit = log_original.len() as u64 + live_values;
    let target_limit = crate::witness::LOG_HEADER_SIZE as u64
        + 3 * (crate::record::HEADER_SIZE + crate::record::CHECKSUM_SIZE) as u64
        + (b"base".len() + b"a".len() + b"b".len()) as u64
        + live_values;
    let result = store.compact_and_replace_with_limits_detailed(
        &previous,
        CheckpointCompactionLimits {
            max_source_log_read_bytes: Some(source_limit - 1),
            max_target_log_bytes: Some(target_limit),
        },
    );
    assert!(matches!(
        result,
        Err(HandoffFailure::NotPublished(error))
            if error.kind() == io::ErrorKind::InvalidInput
                && error.to_string().contains("source log read")
    ));
    assert!(!previous.exists());
    assert_eq!(fs::read(path.join("records.log")).unwrap(), log_original);
    assert_eq!(
        fs::read(path.join("commit.state")).unwrap(),
        witness_original
    );
    assert_eq!(
        fs::read(selection::path(&path)).unwrap(),
        selection_original
    );

    let store = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    let (active, _) = store
        .compact_and_replace_with_limits_detailed(
            &previous,
            CheckpointCompactionLimits {
                max_source_log_read_bytes: Some(source_limit),
                max_target_log_bytes: Some(target_limit),
            },
        )
        .ok()
        .expect("exact ceilings admit the exchange");
    assert_eq!(
        fs::metadata(path.join("records.log")).unwrap().len(),
        target_limit
    );
    assert_eq!(
        fs::read(previous.join("records.log")).unwrap(),
        log_original
    );
    assert_eq!(
        fs::read(previous.join("commit.state")).unwrap(),
        witness_original
    );
    assert_eq!(
        active.get(b"a").unwrap().as_deref(),
        Some(b"latest".as_slice())
    );
    active.put(b"a", b"after exchange").unwrap();
    active.log.verify_witness().unwrap();
    verify_store(&previous);
}

#[test]
fn checkpoint_limits_reject_handoff_before_sibling_publication() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let base = root.path().join("base");
    let previous = root.path().join("previous");
    let store = Store::open(&source).unwrap();
    let digest = store.checkpoint_to(&base).unwrap();
    store.put(b"key", b"acknowledged").unwrap();
    store.close_private_store().unwrap();
    let original_log = fs::read(source.join("records.log")).unwrap();
    let original_witness = fs::read(source.join("commit.state")).unwrap();
    let open = |checkpoint_view| {
        retry_lock(|| {
            CheckpointStore::open_using(
                &source,
                &base,
                &digest,
                CheckpointStoreLimits {
                    checkpoint_view,
                    ..Default::default()
                },
                None,
                Log::open_checkpoint_writer,
            )
        })
        .unwrap()
    };
    let store = open(Default::default());
    let copy = root.path().join("independent-copy");
    store.compact_to(&copy).unwrap();
    let bytes = fs::metadata(copy.join(COMPACTED_CHECKPOINT_FILE))
        .unwrap()
        .len();
    drop(store);
    for (max_file_bytes, max_blocks, message) in [
        (
            bytes - 1,
            1,
            "checkpoint exceeds configured file size limit",
        ),
        (bytes, 0, "checkpoint has too many view blocks"),
    ] {
        let store = open(crate::CheckpointViewLimits {
            max_file_bytes,
            max_blocks,
        });
        assert!(matches!(
            store.compact_and_replace_detailed(&previous),
            Err(HandoffFailure::NotPublished(error))
                if error.kind() == io::ErrorKind::InvalidInput && error.to_string() == message
        ));
        assert!(!previous.exists());
        assert_eq!(fs::read(source.join("records.log")).unwrap(), original_log);
        assert_eq!(
            fs::read(source.join("commit.state")).unwrap(),
            original_witness
        );
    }
    let limits = crate::CheckpointViewLimits {
        max_file_bytes: bytes,
        max_blocks: 1,
    };
    let (store, _) = open(limits)
        .compact_and_replace_detailed(&previous)
        .unwrap();
    assert_eq!(store.limits().checkpoint_view, limits);
    assert_eq!(
        store.get(b"key").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(
        fs::metadata(source.join(COMPACTED_CHECKPOINT_FILE))
            .unwrap()
            .len(),
        bytes
    );
    assert_eq!(
        fs::read(previous.join("records.log")).unwrap(),
        original_log
    );
}

#[test]
fn unmanaged_compaction_preserves_live_data_and_clears_overlay() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let previous = root.path().join("previous");
    let cp = fixture(root.path(), false, false);
    let old_length = fs::metadata(source.join("records.log")).unwrap().len();
    let (cp, digest) = cp.compact_and_replace(&previous).unwrap();
    assert_eq!(cp.overlay_keys().unwrap(), 0);
    assert!(!cp.automatic_checkpoints_enabled().unwrap());
    assert!(fs::metadata(source.join("records.log")).unwrap().len() < old_length);
    verify_store(&previous);
    cp.put(b"c", b"new").unwrap();
    drop(cp);
    let reopened = retry_lock(|| {
        CheckpointStore::open_with_resource_limits(
            &source,
            source.join(COMPACTED_CHECKPOINT_FILE),
            &digest,
            TAIL_BYTES,
            TAIL_KEYS,
            TAIL_KEY_BYTES,
            Some(LOG_LIMIT),
        )
    })
    .unwrap();
    assert_eq!(reopened.get(b"a").unwrap(), Some(b"latest".to_vec()));
    assert_eq!(reopened.get(b"c").unwrap(), Some(b"new".to_vec()));
    assert_eq!(
        Store::open(&source).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn managed_auto_setting_survives_handoff_reopen_and_new_rollover() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let previous = root.path().join("previous");
    let cp = fixture(root.path(), true, true);
    cp.set_transaction_staging_limit(0).unwrap();
    cp.set_automatic_checkpoint_slot_protection(true).unwrap();
    cp.set_automatic_checkpoint_log_scan_limit(Some(3 * LOG_LIMIT))
        .unwrap();
    let (cp, _digest) = cp.compact_and_replace_detailed(&previous).unwrap();
    assert_eq!(cp.transaction_budget_status().unwrap().limit_bytes, 0);
    assert!(cp.read().unwrap().protect_pinned_automatic_slots);
    assert_eq!(
        cp.read().unwrap().automatic_checkpoint_log_scan_limit(),
        Some(3 * LOG_LIMIT)
    );
    assert!(cp.automatic_checkpoints_enabled().unwrap());
    assert_eq!(cp.overlay_keys().unwrap(), 0);
    verify_store(&previous);
    cp.put(b"c", b"three").unwrap();
    cp.put(b"d", b"four").unwrap();
    cp.put(b"e", b"five").unwrap();
    drop(cp);
    let cp = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(
            &source,
            TAIL_BYTES,
            TAIL_KEYS,
            TAIL_KEY_BYTES,
            Some(LOG_LIMIT),
        )
    })
    .unwrap();
    assert!(cp.automatic_checkpoints_enabled().unwrap());
    assert!(!cp.read().unwrap().protect_pinned_automatic_slots);
    assert_eq!(
        cp.read().unwrap().automatic_checkpoint_log_scan_limit(),
        None
    );
    assert_eq!(
        cp.transaction_budget_status().unwrap().limit_bytes,
        crate::MAX_TRANSACTION_STAGED_TOTAL_BYTES
    );
    assert_eq!(cp.get(b"a").unwrap(), Some(b"latest".to_vec()));
    assert_eq!(cp.get(b"e").unwrap(), Some(b"five".to_vec()));
}

#[test]
fn disabled_automatic_configuration_stays_disabled() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let cp = fixture(root.path(), true, true);
    cp.disable_automatic_checkpoints().unwrap();
    let (cp, _) = cp
        .compact_and_replace(root.path().join("previous"))
        .unwrap();
    assert!(!cp.automatic_checkpoints_enabled().unwrap());
    drop(cp);
    let cp = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(
            &source,
            TAIL_BYTES,
            TAIL_KEYS,
            TAIL_KEY_BYTES,
            Some(LOG_LIMIT),
        )
    })
    .unwrap();
    assert!(!cp.automatic_checkpoints_enabled().unwrap());
    cp.enable_automatic_checkpoints().unwrap();
}

#[test]
fn checkpoint_exchange_rejects_unknown_source_files() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let cp = fixture(root.path(), true, true);
    fs::write(source.join("user-data"), b"keep").unwrap();
    assert_eq!(
        cp.compact_and_replace(root.path().join("previous"))
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(fs::read(source.join("user-data")).unwrap(), b"keep");
    assert!(!root.path().join("previous").exists());
    verify_store(&source);
}

#[test]
fn checkpoint_handoff_faults_leave_two_recoverable_logs_and_valid_source_selection() {
    for point in [
        ExchangePoint::CopyPublished,
        ExchangePoint::SelectionBeforeRename,
        ExchangePoint::SelectionBeforeSync,
        ExchangePoint::CandidateReady,
        ExchangePoint::AfterExchange,
    ] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("live");
        let previous = root.path().join("previous");
        let cp = fixture(root.path(), true, true);
        assert!(
            cp.compact_and_replace_with_hooks(
                &previous,
                |current| if current == point {
                    Err(io::Error::other("injected"))
                } else {
                    Ok(())
                },
                crate::directory::sync_existing
            )
            .is_err()
        );
        verify_store(&source);
        verify_store(&previous);
        crate::cleanup_abandoned_staging(&previous).unwrap();
        let cp = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(
                &source,
                TAIL_BYTES,
                TAIL_KEYS,
                TAIL_KEY_BYTES,
                Some(LOG_LIMIT),
            )
        })
        .unwrap();
        assert!(cp.automatic_checkpoints_enabled().unwrap());
        assert_eq!(cp.get(b"a").unwrap(), Some(b"latest".to_vec()));
    }
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let previous = root.path().join("previous");
    let cp = fixture(root.path(), true, true);
    let error = cp
        .compact_and_replace_with_hooks(
            &previous,
            |_| Ok(()),
            |_| Err(io::Error::other("sync failure")),
        )
        .err()
        .unwrap();
    assert!(error.to_string().contains("exchange may have occurred"));
    verify_store(&source);
    verify_store(&previous);
}

#[test]
fn detailed_checkpoint_handoff_classifies_candidate_and_exchange_failures() {
    for managed in [false, true] {
        for failure in [
            "preflight",
            "copy",
            "selection-before",
            "selection-sync",
            "ready",
            "exchange",
            "sync",
        ] {
            if !managed && failure.starts_with("selection-") {
                continue;
            }
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("live");
            let destination = if failure == "preflight" {
                root.path().join("missing/previous")
            } else {
                root.path().join("previous")
            };
            let cp = fixture(root.path(), managed, managed);
            let original = fs::read(source.join("records.log")).unwrap();
            let result = cp.compact_and_replace_detailed_with_hooks(
                &destination,
                CheckpointCompactionLimits::default(),
                |point| {
                    let reject = match failure {
                        "copy" => point == ExchangePoint::CopyPublished,
                        "selection-before" => point == ExchangePoint::SelectionBeforeRename,
                        "selection-sync" => point == ExchangePoint::SelectionBeforeSync,
                        "ready" => point == ExchangePoint::CandidateReady,
                        "exchange" => point == ExchangePoint::AfterExchange,
                        _ => false,
                    };
                    if reject {
                        Err(io::Error::other("injected handoff failure"))
                    } else {
                        Ok(())
                    }
                },
                |parent| {
                    if failure == "sync" {
                        Err(io::Error::other("injected parent sync failure"))
                    } else {
                        crate::directory::sync_existing(parent)
                    }
                },
            );
            match failure {
                "preflight" => assert!(matches!(result, Err(HandoffFailure::NotPublished(_)))),
                "exchange" | "sync" => assert!(matches!(
                    result,
                    Err(HandoffFailure::ExchangeOutcomeUnknown(_))
                )),
                _ => assert!(matches!(
                    result,
                    Err(HandoffFailure::CandidateOutcomeUnknown(_))
                )),
            }
            assert_eq!(
                fs::read(source.join("records.log")).unwrap() == original,
                !matches!(failure, "exchange" | "sync")
            );
            verify_store(&source);
            if failure == "preflight" {
                assert!(!destination.exists());
            } else {
                verify_store(&destination);
                assert_eq!(
                    fs::read(destination.join("records.log")).unwrap() == original,
                    matches!(failure, "exchange" | "sync")
                );
            }
            if managed {
                let reopened = retry_lock(|| CheckpointStore::open_managed(&source)).unwrap();
                assert_eq!(reopened.get(b"a").unwrap(), Some(b"latest".to_vec()));
            }
        }
    }
}

#[test]
fn checkpoint_exchange_kill_child() {
    let Ok(point) = std::env::var("EMDA_CP_EXCHANGE_CHILD") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("EMDA_CP_EXCHANGE_ROOT").unwrap());
    let source = root.join("live");
    let previous = root.join("previous");
    let cp = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(
            &source,
            TAIL_BYTES,
            TAIL_KEYS,
            TAIL_KEY_BYTES,
            Some(LOG_LIMIT),
        )
    })
    .unwrap();
    let selected = match point.as_str() {
        "selection_rename" => ExchangePoint::SelectionBeforeRename,
        "selection_sync" => ExchangePoint::SelectionBeforeSync,
        "ready" => ExchangePoint::CandidateReady,
        _ => ExchangePoint::AfterExchange,
    };
    let _ = cp.compact_and_replace_with_hooks(
        &previous,
        |point| {
            if point == selected {
                let mut signal = File::create(root.join("signal")).unwrap();
                signal.write_all(b"ready").unwrap();
                signal.sync_all().unwrap();
                loop {
                    thread::park();
                }
            }
            Ok(())
        },
        crate::directory::sync_existing,
    );
    panic!("kill hook not reached");
}

#[test]
fn process_kills_preserve_managed_source_and_both_logs() {
    for point in ["selection_rename", "selection_sync", "ready", "exchange"] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("live");
        let previous = root.path().join("previous");
        drop(fixture(root.path(), true, true));
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("checkpoint_store::compact_exchange::tests::checkpoint_exchange_kill_child")
            .env("EMDA_CP_EXCHANGE_CHILD", point)
            .env("EMDA_CP_EXCHANGE_ROOT", root.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !root.path().join("signal").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before hook: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not reach hook");
            }
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        verify_store(&source);
        verify_store(&previous);
        crate::cleanup_abandoned_staging(&previous).unwrap();
        let cp = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(
                &source,
                TAIL_BYTES,
                TAIL_KEYS,
                TAIL_KEY_BYTES,
                Some(LOG_LIMIT),
            )
        })
        .unwrap();
        assert!(cp.automatic_checkpoints_enabled().unwrap());
        assert_eq!(cp.get(b"a").unwrap(), Some(b"latest".to_vec()));
    }
}

#[test]
fn changed_selection_configuration_and_stale_lock_fail_before_copy() {
    for change in ["selection", "automatic", "lock"] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("live");
        let previous = root.path().join("previous");
        let cp = fixture(root.path(), true, true);
        match change {
            "selection" => {
                fs::write(source.join("checkpoint.selection"), b"invalid selection").unwrap()
            }
            "automatic" => fs::write(
                source.join(".emda-auto-checkpoints-v1").join("owner"),
                b"invalid marker",
            )
            .unwrap(),
            _ => {
                fs::remove_file(source.join("store.lock")).unwrap();
                File::create(source.join("store.lock")).unwrap();
            }
        }
        assert!(cp.compact_and_replace(&previous).is_err());
        assert!(!previous.exists());
        verify_store(&source);
    }
}
