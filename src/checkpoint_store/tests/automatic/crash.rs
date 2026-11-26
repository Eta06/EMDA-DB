use super::*;

#[test]
fn automatic_rollover_crash_child() {
    let Ok(parent) = std::env::var("EMDA_AUTO_CRASH_PATH") else {
        return;
    };
    let parent = Path::new(&parent);
    let phase = std::env::var("EMDA_AUTO_CRASH_PHASE").unwrap();
    let store = retry_lock(|| {
        CheckpointStore::open_managed_with_resource_limits(parent.join("store"), 1024, 1, 64, None)
    })
    .unwrap();
    store
        .automatic_rollover_with_hooks(
            store.write().unwrap(),
            || {
                if phase == "slot-staged" {
                    pause_automatic_rollover()
                } else {
                    Ok(())
                }
            },
            |root| {
                if phase == "slot-published" {
                    pause_automatic_rollover()
                } else {
                    crate::directory::sync_existing(root)
                }
            },
            || {
                if phase == "selection-staged" {
                    pause_automatic_rollover()
                } else {
                    Ok(())
                }
            },
            |parent| {
                if phase == "selection-published" {
                    pause_automatic_rollover()
                } else {
                    crate::directory::sync_existing(parent)
                }
            },
        )
        .unwrap();
}

#[test]
fn process_kill_at_automatic_rollover_boundaries_recovers_acknowledged_data() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    for phase in [
        "slot-staged",
        "slot-published",
        "selection-staged",
        "selection-published",
    ] {
        let (dir, path, store) = managed_fixture(1024, 1, 64);
        store.enable_automatic_checkpoints().unwrap();
        store.put(b"a", b"acknowledged").unwrap();
        let previous = fs::read(crate::checkpoint_store::selection::path(&path)).unwrap();
        let original_log = fs::read(path.join("records.log")).unwrap();
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "checkpoint_store::tests::automatic::crash::automatic_rollover_crash_child",
                "--nocapture",
            ])
            .env("EMDA_AUTO_CRASH_PATH", dir.path())
            .env("EMDA_AUTO_CRASH_PHASE", phase)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line.contains("AUTOMATIC_ROLLOVER_BOUNDARY_REACHED") {
                    let _ = send.send(());
                    break;
                }
            }
        });
        let reached = receive.recv_timeout(Duration::from_secs(20));
        let root = crate::checkpoint_store::automatic::root(&path);
        let busy = crate::cleanup_abandoned_staging(&root);
        let _ = child.kill();
        let _ = child.wait();
        reader.join().unwrap();
        reached.expect("child did not reach automatic rollover boundary");
        assert_eq!(busy.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        retry_lock(|| crate::cleanup_abandoned_staging(&root)).unwrap();
        retry_lock(|| crate::cleanup_abandoned_staging(&path)).unwrap();
        assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
        let selected = fs::read(crate::checkpoint_store::selection::path(&path)).unwrap();
        assert_eq!(selected == previous, phase != "selection-published");
        let reopened = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None)
        })
        .unwrap();
        assert!(reopened.automatic_checkpoints_enabled().unwrap());
        assert_eq!(
            reopened.get(b"a").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        reopened.put(b"b", b"after").unwrap();
        reopened.put(b"c", b"after-again").unwrap();
        assert_eq!(
            reopened.get(b"a").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        assert_eq!(
            reopened.get(b"c").unwrap().as_deref(),
            Some(&b"after-again"[..])
        );
    }
}

#[test]
fn automatic_configuration_crash_child() {
    let Ok(parent) = std::env::var("EMDA_AUTO_CONFIG_CRASH_PATH") else {
        return;
    };
    let parent = Path::new(&parent);
    let phase = std::env::var("EMDA_AUTO_CONFIG_CRASH_PHASE").unwrap();
    let store = retry_lock(|| CheckpointStore::open_managed(parent.join("store"))).unwrap();
    if phase.starts_with("enable-") {
        let _state = store.write().unwrap();
        crate::checkpoint_store::automatic::initialize_with_hooks(
            &parent.join("store"),
            || {
                if phase == "enable-staged" {
                    pause_automatic_rollover()
                } else {
                    Ok(())
                }
            },
            |directory| {
                if phase == "enable-published" {
                    pause_automatic_rollover()
                } else {
                    crate::directory::sync_existing(directory)
                }
            },
        )
        .unwrap();
    } else {
        store
            .disable_automatic_checkpoints_with_hooks(
                || {
                    if phase == "disable-staged" {
                        pause_automatic_rollover()
                    } else {
                        Ok(())
                    }
                },
                |directory| {
                    if phase == "disable-published" {
                        pause_automatic_rollover()
                    } else {
                        crate::directory::sync_existing(directory)
                    }
                },
            )
            .unwrap();
    }
}

#[test]
fn process_kill_at_automatic_configuration_boundaries_recovers_and_cleans_stages() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    for phase in [
        "enable-staged",
        "enable-published",
        "disable-staged",
        "disable-published",
    ] {
        let (dir, path, store) = managed_fixture(1024, 1, 64);
        if phase.starts_with("disable-") {
            store.enable_automatic_checkpoints().unwrap();
        }
        store.put(b"a", b"acknowledged").unwrap();
        let original_log = fs::read(path.join("records.log")).unwrap();
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "checkpoint_store::tests::automatic::crash::automatic_configuration_crash_child",
                "--nocapture",
            ])
            .env("EMDA_AUTO_CONFIG_CRASH_PATH", dir.path())
            .env("EMDA_AUTO_CONFIG_CRASH_PHASE", phase)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line.contains("AUTOMATIC_ROLLOVER_BOUNDARY_REACHED") {
                    let _ = send.send(());
                    break;
                }
            }
        });
        let reached = receive.recv_timeout(Duration::from_secs(20));
        let staging_parent = if phase.starts_with("enable-") {
            path.clone()
        } else {
            crate::checkpoint_store::automatic::root(&path)
        };
        let busy = crate::cleanup_abandoned_staging(&staging_parent);
        let _ = child.kill();
        let _ = child.wait();
        reader.join().unwrap();
        reached.expect("child did not reach configuration boundary");
        assert_eq!(busy.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
        let reopened = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None)
        })
        .unwrap();
        let expected_enabled = matches!(phase, "enable-published" | "disable-staged");
        assert_eq!(
            reopened.automatic_checkpoints_enabled().unwrap(),
            expected_enabled
        );
        assert_eq!(
            reopened.get(b"a").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        reopened.enable_automatic_checkpoints().unwrap();
        reopened.put(b"b", b"recovered").unwrap();
        assert_eq!(
            reopened.get(b"b").unwrap().as_deref(),
            Some(&b"recovered"[..])
        );
        let root = crate::checkpoint_store::automatic::root(&path);
        assert_eq!(
            crate::cleanup_abandoned_staging(&root)
                .unwrap()
                .removed_payloads,
            0
        );
        assert_eq!(
            crate::cleanup_abandoned_staging(&path)
                .unwrap()
                .removed_payloads,
            0
        );
    }
}

#[test]
fn automatic_slot_removal_crash_child() {
    let Ok(parent) = std::env::var("EMDA_AUTO_REMOVE_CRASH_PATH") else {
        return;
    };
    let parent = Path::new(&parent);
    let phase = std::env::var("EMDA_AUTO_REMOVE_CRASH_PHASE").unwrap();
    let store = retry_lock(|| CheckpointStore::open_managed(parent.join("store"))).unwrap();
    let _state = store.write().unwrap();
    let root = crate::checkpoint_store::automatic::root(&parent.join("store"));
    let slot = root.join("slot-0");
    crate::checkpoint_store::automatic::remove_inactive_slot_with_hooks(&root, &slot, |point| {
        let matches = matches!(
            (phase.as_str(), point),
            (
                "index",
                crate::checkpoint_store::automatic::RemovalPoint::Index
            ) | (
                "owner",
                crate::checkpoint_store::automatic::RemovalPoint::Owner
            ) | (
                "directory",
                crate::checkpoint_store::automatic::RemovalPoint::Directory
            )
        );
        if matches {
            pause_automatic_rollover()
        } else {
            Ok(())
        }
    })
    .unwrap();
}

#[test]
fn process_kill_during_inactive_slot_removal_is_recoverable() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    for phase in ["index", "owner", "directory"] {
        let (dir, path, store) = managed_fixture(1024, 1, 64);
        store.enable_automatic_checkpoints().unwrap();
        store.put(b"a", b"one").unwrap();
        store.put(b"b", b"two").unwrap();
        store.put(b"c", b"three").unwrap();
        let original_log = fs::read(path.join("records.log")).unwrap();
        let selection = fs::read(crate::checkpoint_store::selection::path(&path)).unwrap();
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "checkpoint_store::tests::automatic::crash::automatic_slot_removal_crash_child",
                "--nocapture",
            ])
            .env("EMDA_AUTO_REMOVE_CRASH_PATH", dir.path())
            .env("EMDA_AUTO_REMOVE_CRASH_PHASE", phase)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line.contains("AUTOMATIC_ROLLOVER_BOUNDARY_REACHED") {
                    let _ = send.send(());
                    break;
                }
            }
        });
        let reached = receive.recv_timeout(Duration::from_secs(20));
        let _ = child.kill();
        let _ = child.wait();
        reader.join().unwrap();
        reached.expect("child did not reach slot removal boundary");
        assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
        assert_eq!(
            fs::read(crate::checkpoint_store::selection::path(&path)).unwrap(),
            selection
        );
        let reopened = retry_lock(|| {
            CheckpointStore::open_managed_with_resource_limits(&path, 1024, 1, 64, None)
        })
        .unwrap();
        assert_eq!(reopened.get(b"a").unwrap().as_deref(), Some(&b"one"[..]));
        assert_eq!(reopened.get(b"b").unwrap().as_deref(), Some(&b"two"[..]));
        assert_eq!(reopened.get(b"c").unwrap().as_deref(), Some(&b"three"[..]));
        reopened.put(b"d", b"four").unwrap();
        assert_eq!(reopened.get(b"d").unwrap().as_deref(), Some(&b"four"[..]));
    }
}

// Logical bytes a budgeted reopen must charge: the store tree, an external
// selected checkpoint and one spare budget-record length.
fn measured_charge(path: &Path) -> u64 {
    let store = fs::canonicalize(path).unwrap();
    let selected = crate::checkpoint_store::selection::load(path)
        .unwrap()
        .unwrap()
        .checkpoint;
    let external = if selected.starts_with(&store) {
        0
    } else {
        crate::staging::tree_bytes(&selected).unwrap()
    };
    crate::staging::tree_bytes(&store).unwrap()
        + external
        + crate::checkpoint_store::disk_budget::RECORD_BYTES as u64
}

#[test]
fn process_kill_during_a_budgeted_rollover_reopens_with_every_file_charged() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    for phase in [
        "slot-staged",
        "slot-published",
        "selection-staged",
        "selection-published",
    ] {
        let (dir, path, store) = managed_fixture(1024, 1, 64);
        let online = crate::OnlineCheckpointStore::from_store(store).unwrap();
        online.set_disk_budget(Some(u64::MAX)).unwrap();
        online
            .with_store(CheckpointStore::enable_automatic_checkpoints)
            .unwrap();
        online
            .with_store(|store| store.put(b"a", b"acknowledged"))
            .unwrap();
        drop(online);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "checkpoint_store::tests::automatic::crash::automatic_rollover_crash_child",
                "--nocapture",
            ])
            .env("EMDA_AUTO_CRASH_PATH", dir.path())
            .env("EMDA_AUTO_CRASH_PHASE", phase)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line.contains("AUTOMATIC_ROLLOVER_BOUNDARY_REACHED") {
                    let _ = send.send(());
                    break;
                }
            }
        });
        let reached = receive.recv_timeout(Duration::from_secs(20));
        let _ = child.kill();
        let _ = child.wait();
        reader.join().unwrap();
        reached.expect("child did not reach automatic rollover boundary");
        // Reopen without manual cleanup: the open removes leftover stages
        // and measures every remaining file before admitting anything.
        let reopened = retry_lock(|| crate::OnlineCheckpointStore::open_managed(&path)).unwrap();
        let status = reopened.disk_budget_status().unwrap().unwrap();
        // No stage of the killed rollover survives to be charged.
        let root = crate::checkpoint_store::automatic::root(&path);
        for directory in [&path, &root] {
            let stages = fs::read_dir(directory)
                .unwrap()
                .filter(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".emda-stage-v1-")
                })
                .count();
            assert_eq!(stages, 0, "{phase}: {}", directory.display());
        }
        assert_eq!(status.pending, crate::DiskBudgetPending::None, "{phase}");
        assert_eq!(status.automatic_reserved, 0, "{phase}");
        assert_eq!(status.charged, measured_charge(&path), "{phase}");
        assert!(
            reopened
                .with_store(CheckpointStore::automatic_checkpoints_enabled)
                .unwrap()
        );
        reopened
            .with_store(|store| {
                assert_eq!(store.get(b"a")?.as_deref(), Some(&b"acknowledged"[..]));
                // Both writes roll over under the reopened charge.
                store.put(b"b", b"after")?;
                store.put(b"c", b"after-again")
            })
            .unwrap();
        assert_eq!(
            reopened.disk_budget_status().unwrap().unwrap().charged,
            measured_charge(&path),
            "{phase}"
        );
        drop(reopened);
        let store = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
        assert_eq!(
            store.get(b"c").unwrap().as_deref(),
            Some(&b"after-again"[..])
        );
    }
}
