use super::*;

#[test]
fn automatic_rollover_failures_preserve_or_fail_closed_before_user_writes() {
    for phase in [
        "slot-staged",
        "slot-published",
        "selection-staged",
        "selection-published",
    ] {
        let (_dir, path, store) = managed_fixture(1024, 1, 64);
        store.enable_automatic_checkpoints().unwrap();
        store.put(b"a", b"acknowledged").unwrap();
        let old_selection = fs::read(crate::checkpoint_store::selection::path(&path)).unwrap();
        let old_log = fs::read(path.join("records.log")).unwrap();
        let mut state = store.write().unwrap();
        let result = store.automatic_rollover_with_hooks(
            &mut state,
            || {
                if phase == "slot-staged" {
                    Err(io::Error::other("injected before slot publication"))
                } else {
                    Ok(())
                }
            },
            |parent| {
                if phase == "slot-published" {
                    Err(io::Error::other("injected slot sync"))
                } else {
                    crate::directory::sync_existing(parent)
                }
            },
            || {
                if phase == "selection-staged" {
                    Err(io::Error::other("injected before selection rename"))
                } else {
                    Ok(())
                }
            },
            |parent| {
                if phase == "selection-published" {
                    Err(io::Error::other("injected selection sync"))
                } else {
                    crate::directory::sync_existing(parent)
                }
            },
        );
        assert!(result.is_err());
        let poisoned = state.failed;
        drop(state);
        assert_eq!(fs::read(path.join("records.log")).unwrap(), old_log);
        assert_eq!(poisoned, phase.starts_with("selection-"));
        if poisoned {
            assert!(store.get(b"a").is_err());
        } else {
            assert_eq!(
                store.get(b"a").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
        }
        let selection = fs::read(crate::checkpoint_store::selection::path(&path)).unwrap();
        assert_eq!(selection == old_selection, phase != "selection-published");
        drop(store);
        let reopened = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None)
        })
        .unwrap();
        assert!(reopened.automatic_checkpoints_enabled().unwrap());
        assert_eq!(
            reopened.get(b"a").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        reopened.put(b"b", b"recovered").unwrap();
        assert_eq!(
            reopened.get(b"b").unwrap().as_deref(),
            Some(&b"recovered"[..])
        );
    }
}

#[test]
fn automatic_rollover_refuses_unrecognized_inactive_slot_contents() {
    let (_dir, path, store) = managed_fixture(1024, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"first").unwrap();
    store.put(b"b", b"second").unwrap();
    store.put(b"c", b"third").unwrap();
    let root = crate::checkpoint_store::automatic::root(&path);
    let unknown = root.join("slot-0").join("do-not-delete");
    fs::write(&unknown, b"keep").unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    assert!(store.put(b"d", b"must-not-commit").is_err());
    assert_eq!(fs::read(&unknown).unwrap(), b"keep");
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(store.get(b"c").unwrap().as_deref(), Some(&b"third"[..]));
    assert_eq!(store.get(b"d").unwrap(), None);
}

#[test]
fn corrupted_automatic_configuration_disables_the_open_store_and_fails_closed_on_reopen() {
    let (_dir, path, store) = managed_fixture(1024, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"first").unwrap();
    fs::write(
        crate::checkpoint_store::automatic::root(&path).join("owner"),
        b"corrupt",
    )
    .unwrap();
    assert!(store.put(b"b", b"must-not-commit").is_err());
    assert!(store.get(b"a").is_err());
    drop(store);
    assert!(retry_lock(|| CheckpointStore::open_managed(&path)).is_err());
    assert_eq!(
        fs::read(crate::checkpoint_store::automatic::root(&path).join("owner")).unwrap(),
        b"corrupt"
    );
}

#[test]
fn automatic_configuration_publication_errors_poison_until_reopen() {
    for after_rename in [false, true] {
        let (_dir, path, store) = managed_fixture(1024, 1, 64);
        store.enable_automatic_checkpoints().unwrap();
        store.put(b"a", b"acknowledged").unwrap();
        let original = fs::read(path.join("records.log")).unwrap();
        let result = store.disable_automatic_checkpoints_with_hooks(
            || {
                if after_rename {
                    Ok(())
                } else {
                    Err(io::Error::other("injected before configuration rename"))
                }
            },
            |parent| {
                if after_rename {
                    Err(io::Error::other("injected configuration sync"))
                } else {
                    crate::directory::sync_existing(parent)
                }
            },
        );
        assert!(result.unwrap_err().to_string().contains("reopen required"));
        assert!(store.get(b"a").is_err());
        assert!(store.put(b"b", b"not committed").is_err());
        assert_eq!(fs::read(path.join("records.log")).unwrap(), original);
        drop(store);
        let reopened = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None)
        })
        .unwrap();
        assert_eq!(
            reopened.automatic_checkpoints_enabled().unwrap(),
            !after_rename
        );
        assert_eq!(
            reopened.get(b"a").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        assert_eq!(reopened.get(b"b").unwrap(), None);
    }
}

#[test]
fn automatic_enable_publication_errors_leave_recoverable_configuration() {
    for after_publish in [false, true] {
        let (_dir, path, store) = managed_fixture(1024, 1, 64);
        let result = crate::checkpoint_store::automatic::initialize_with_hooks(
            &path,
            || {
                if after_publish {
                    Ok(())
                } else {
                    Err(io::Error::other("injected before root publication"))
                }
            },
            |parent| {
                if after_publish {
                    Err(io::Error::other("injected root sync"))
                } else {
                    crate::directory::sync_existing(parent)
                }
            },
        );
        assert!(result.is_err());
        assert_eq!(
            store.get(b"base").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        drop(store);
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.automatic_checkpoints_enabled().unwrap(),
            after_publish
        );
        reopened.enable_automatic_checkpoints().unwrap();
        assert!(reopened.automatic_checkpoints_enabled().unwrap());
    }
}

#[test]
fn automatic_rollover_refuses_symlink_slots_without_touching_target() {
    use std::os::unix::fs::symlink;
    let (dir, path, store) = managed_fixture(1024, 1, 64);
    store.enable_automatic_checkpoints().unwrap();
    store.put(b"a", b"acknowledged").unwrap();
    let outside = dir.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("keep"), b"untouched").unwrap();
    let root = crate::checkpoint_store::automatic::root(&path);
    symlink(&outside, root.join("slot-0")).unwrap();
    let before = fs::read(path.join("records.log")).unwrap();
    assert!(store.put(b"b", b"must-not-commit").is_err());
    assert_eq!(fs::read(outside.join("keep")).unwrap(), b"untouched");
    assert_eq!(fs::read(path.join("records.log")).unwrap(), before);
    assert_eq!(
        store.get(b"a").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}

#[test]
fn automatic_configuration_changes_reject_a_changed_managed_selection() {
    for disabling in [false, true] {
        let (_dir, path, store) = managed_fixture(1024, 1, 64);
        if disabling {
            store.enable_automatic_checkpoints().unwrap();
        }
        let selection = crate::checkpoint_store::selection::path(&path);
        let previous = fs::read(&selection).unwrap();
        fs::write(&selection, b"corrupt").unwrap();
        let error = if disabling {
            store.disable_automatic_checkpoints().unwrap_err()
        } else {
            store.enable_automatic_checkpoints().unwrap_err()
        };
        assert!(error.to_string().contains("selection"));
        assert!(store.get(b"base").is_err());
        assert!(store.put(b"key", b"not committed").is_err());
        drop(store);
        fs::write(&selection, previous).unwrap();
        let reopened = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            reopened.get(b"base").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }
}

#[test]
fn automatic_inactive_slot_removal_errors_allow_safe_retry() {
    use crate::checkpoint_store::automatic::RemovalPoint;
    for failed in [
        RemovalPoint::Index,
        RemovalPoint::Owner,
        RemovalPoint::Directory,
    ] {
        let (_dir, path, store) = managed_fixture(1024, 1, 64);
        store.enable_automatic_checkpoints().unwrap();
        store.put(b"a", b"one").unwrap();
        store.put(b"b", b"two").unwrap();
        store.put(b"c", b"three").unwrap();
        let original = fs::read(path.join("records.log")).unwrap();
        let selection = fs::read(crate::checkpoint_store::selection::path(&path)).unwrap();
        let root = crate::checkpoint_store::automatic::root(&path);
        let slot = root.join("slot-0");
        let state = store.write().unwrap();
        let result = crate::checkpoint_store::automatic::remove_inactive_slot_with_hooks(
            &root,
            &slot,
            |point| {
                if point == failed {
                    Err(io::Error::other("injected removal error"))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        drop(state);
        assert_eq!(fs::read(path.join("records.log")).unwrap(), original);
        assert_eq!(
            fs::read(crate::checkpoint_store::selection::path(&path)).unwrap(),
            selection
        );
        store.put(b"d", b"four").unwrap();
        assert_eq!(store.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
        assert_eq!(store.get(b"d").unwrap().as_deref(), Some(&b"four"[..]));
    }
}
