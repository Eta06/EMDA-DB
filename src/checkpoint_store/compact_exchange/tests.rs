use super::*;
use crate::{COMPACTED_CHECKPOINT_FILE, Store};
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
    let store = Store::open(&path).unwrap();
    store.put(b"base", b"initial").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let cp = CheckpointStore::open_with_resource_limits(
        &path,
        &checkpoint,
        &digest,
        TAIL_BYTES,
        TAIL_KEYS,
        TAIL_KEY_BYTES,
        Some(LOG_LIMIT),
    )
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
    let store = Store::open(path).unwrap();
    assert_eq!(store.get(b"base").unwrap(), Some(b"initial".to_vec()));
    assert_eq!(store.get(b"a").unwrap(), Some(b"latest".to_vec()));
    assert_eq!(store.get(b"b").unwrap(), Some(b"value".to_vec()));
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
    let reopened = CheckpointStore::open_with_resource_limits(
        &source,
        source.join(COMPACTED_CHECKPOINT_FILE),
        &digest,
        TAIL_BYTES,
        TAIL_KEYS,
        TAIL_KEY_BYTES,
        Some(LOG_LIMIT),
    )
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
    let (cp, _digest) = cp.compact_and_replace(&previous).unwrap();
    assert!(cp.automatic_checkpoints_enabled().unwrap());
    assert_eq!(cp.overlay_keys().unwrap(), 0);
    verify_store(&previous);
    cp.put(b"c", b"three").unwrap();
    cp.put(b"d", b"four").unwrap();
    cp.put(b"e", b"five").unwrap();
    drop(cp);
    let cp = CheckpointStore::open_managed_with_resource_limits(
        &source,
        TAIL_BYTES,
        TAIL_KEYS,
        TAIL_KEY_BYTES,
        Some(LOG_LIMIT),
    )
    .unwrap();
    assert!(cp.automatic_checkpoints_enabled().unwrap());
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
    let cp = CheckpointStore::open_managed_with_resource_limits(
        &source,
        TAIL_BYTES,
        TAIL_KEYS,
        TAIL_KEY_BYTES,
        Some(LOG_LIMIT),
    )
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
        let cp = CheckpointStore::open_managed_with_resource_limits(
            &source,
            TAIL_BYTES,
            TAIL_KEYS,
            TAIL_KEY_BYTES,
            Some(LOG_LIMIT),
        )
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
fn checkpoint_exchange_kill_child() {
    let Ok(point) = std::env::var("EMDA_CP_EXCHANGE_CHILD") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("EMDA_CP_EXCHANGE_ROOT").unwrap());
    let source = root.join("live");
    let previous = root.join("previous");
    let cp = CheckpointStore::open_managed_with_resource_limits(
        &source,
        TAIL_BYTES,
        TAIL_KEYS,
        TAIL_KEY_BYTES,
        Some(LOG_LIMIT),
    )
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
        let cp = CheckpointStore::open_managed_with_resource_limits(
            &source,
            TAIL_BYTES,
            TAIL_KEYS,
            TAIL_KEY_BYTES,
            Some(LOG_LIMIT),
        )
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
