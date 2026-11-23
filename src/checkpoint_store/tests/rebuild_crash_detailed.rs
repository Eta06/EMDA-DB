use super::*;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

#[cfg(target_os = "linux")]
fn metadata_witness(path: &Path) -> Option<Vec<u8>> {
    if !fs::read(path.join("records.log"))
        .unwrap()
        .starts_with(b"EMDALOG3")
    {
        return None;
    }
    let log = File::open(path.join("records.log")).unwrap();
    let mut marker = [0; witness::WITNESS_SIZE];
    assert_eq!(
        rustix::fs::fgetxattr(&log, "user.db.commit.v3", &mut marker[..]).unwrap(),
        marker.len()
    );
    Some(marker.to_vec())
}

#[cfg(not(target_os = "linux"))]
fn metadata_witness(_path: &Path) -> Option<Vec<u8>> {
    None
}

fn pause_at_boundary() -> io::Result<()> {
    println!("DETAILED_REBUILD_CRASH_BOUNDARY");
    io::stdout().flush()?;
    loop {
        std::thread::park();
    }
}

#[test]
fn detailed_rebuild_crash_child() {
    let Ok(root) = std::env::var("EMDA_DETAILED_REBUILD_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let stage = std::env::var("EMDA_DETAILED_REBUILD_STAGE").unwrap();
    crate::publication_outcome::track_managed_rebuild(|tracker| {
        CheckpointStore::rebuild_managed_using(
            &root.join("store"),
            &root.join("result"),
            |path, check| Store::open_with_recovery_checks(path, None, None, None, check),
            |parent| {
                if stage == "checkpoint-published" {
                    pause_at_boundary()
                } else {
                    super::super::selection::sync_ancestors(parent)
                }
            },
            || {
                if stage == "selection-staged" {
                    pause_at_boundary()
                } else {
                    Ok(())
                }
            },
            |parent| {
                if stage == "selection-published" {
                    pause_at_boundary()
                } else {
                    super::super::selection::sync_ancestors(parent)
                }
            },
            Some(tracker),
        )
    })
    .unwrap();
}

#[test]
fn detailed_rebuild_process_kills_preserve_tail_selection_and_backup() {
    for v2 in [true, false] {
        for managed in [false, true] {
            for stage in [
                "checkpoint-published",
                "selection-staged",
                "selection-published",
            ] {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("store");
                if v2 {
                    fs::create_dir(&path).unwrap();
                    fs::write(
                        path.join("records.log"),
                        include_bytes!("../../../tests/fixtures/v2-empty-records.log"),
                    )
                    .unwrap();
                    fs::write(
                        path.join("commit.state"),
                        include_bytes!("../../../tests/fixtures/v2-empty-commit.state"),
                    )
                    .unwrap();
                }
                let initial = Store::open(&path).unwrap();
                initial.put(b"safe", b"acknowledged").unwrap();
                let base = root.path().join("base");
                let base_digest = initial.checkpoint_to(&base).unwrap();
                drop(initial);
                if managed {
                    CheckpointStore::rebuild_managed_selection(&path, root.path().join("old"))
                        .unwrap();
                    let store = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
                    store.put(b"tail", b"acknowledged").unwrap();
                } else {
                    let store = retry_lock(|| Store::open(&path)).unwrap();
                    store.put(b"tail", b"acknowledged").unwrap();
                }
                let previous = fs::read(super::super::selection::path(&path)).ok();
                let reference = retry_lock(|| Store::open(&path)).unwrap();
                let expected_digest = reference
                    .checkpoint_to(root.path().join("expected"))
                    .unwrap();
                let backup = root.path().join("backup");
                reference.backup(&backup).unwrap();
                drop(reference);
                let original_log = fs::read(path.join("records.log")).unwrap();
                let original_witness = fs::read(path.join("commit.state")).unwrap();
                let original_metadata = metadata_witness(&path);
                let original_backup = fs::read(&backup).unwrap();

                let mut child = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "checkpoint_store::tests::rebuild_crash_detailed::detailed_rebuild_crash_child",
                        "--nocapture",
                    ])
                    .env("EMDA_DETAILED_REBUILD_ROOT", root.path())
                    .env("EMDA_DETAILED_REBUILD_STAGE", stage)
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
                let stdout = child.stdout.take().unwrap();
                let (send, receive) = mpsc::channel();
                let reader = std::thread::spawn(move || {
                    for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                        if line.contains("DETAILED_REBUILD_CRASH_BOUNDARY") {
                            let _ = send.send(());
                            break;
                        }
                    }
                });
                let reached = receive.recv_timeout(Duration::from_secs(20));
                let stage_parent = if stage == "checkpoint-published" {
                    root.path()
                } else {
                    &path
                };
                let busy = crate::cleanup_abandoned_staging(stage_parent);
                let killed = child.kill();
                let status = child.wait();
                reader.join().unwrap();
                reached.expect("child did not reach the requested rebuild boundary");
                killed.expect("failed to terminate the paused rebuild child");
                assert!(!status.expect("failed to wait for rebuild child").success());
                assert_eq!(busy.unwrap_err().kind(), io::ErrorKind::WouldBlock);
                let cleaned =
                    retry_lock(|| crate::cleanup_abandoned_staging(stage_parent)).unwrap();
                assert_eq!(cleaned.removed_markers, 1);
                let visible = fs::read(super::super::selection::path(&path)).ok();
                assert_eq!(visible == previous, stage != "selection-published");
                assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
                assert_eq!(
                    fs::read(path.join("commit.state")).unwrap(),
                    original_witness
                );
                assert_eq!(metadata_witness(&path), original_metadata);
                assert_eq!(fs::read(&backup).unwrap(), original_backup);

                let ordinary = retry_lock(|| Store::open(&path)).unwrap();
                assert_eq!(
                    ordinary.get(b"tail").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                drop(ordinary);
                let recovered = if visible.is_some() {
                    retry_lock(|| CheckpointStore::open_managed(&path)).unwrap()
                } else {
                    retry_lock(|| CheckpointStore::open(&path, &base, &base_digest)).unwrap()
                };
                assert_eq!(
                    recovered.get(b"safe").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                assert_eq!(
                    recovered.get(b"tail").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                drop(recovered);
                let view = retry_lock(|| {
                    CheckpointView::open(&path, root.path().join("result"), &expected_digest)
                })
                .unwrap();
                assert_eq!(
                    view.get(b"tail").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                drop(view);
                let restored_path = root.path().join("restored");
                Store::restore(&backup, &restored_path).unwrap();
                let restored = retry_lock(|| Store::open(&restored_path)).unwrap();
                assert_eq!(
                    restored.get(b"tail").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
                println!(
                    "format={} managed={} stage={} recovered",
                    if original_log.starts_with(b"EMDALOG3") {
                        "v3"
                    } else {
                        "v2"
                    },
                    managed,
                    stage
                );
            }
        }
    }
}
