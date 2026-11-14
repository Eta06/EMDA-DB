//! Real process descriptor exhaustion must not strand locks or acknowledged data.
#![cfg(target_os = "linux")]

use database_engine::{CheckpointStore, Store};
use rustix::process::{Resource, getrlimit, setrlimit};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const PROJECTS: usize = 12;

fn project(root: &Path, index: usize) -> PathBuf {
    root.join(format!("project-{index}"))
}

#[test]
fn checkpoint_fd_exhaustion_child() {
    let Some(root) = std::env::var_os("EMDA_FD_EXHAUSTION_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    // Read fixture metadata before lowering the limit, so exhaustion occurs
    // inside a store open rather than while reading a test input.
    let digests = (0..PROJECTS)
        .map(|index| {
            <[u8; 32]>::try_from(fs::read(project(&root, index).join("digest")).unwrap()).unwrap()
        })
        .collect::<Vec<_>>();
    let mut limit = getrlimit(Resource::Nofile);
    let soft = limit.current.unwrap_or(40).min(40);
    assert!(soft >= 32, "test requires at least 32 file descriptors");
    limit.current = Some(soft);
    setrlimit(Resource::Nofile, limit).unwrap();

    let mut open = Vec::new();
    let mut failed = None;
    for (index, digest) in digests.iter().enumerate() {
        let path = project(&root, index);
        match CheckpointStore::open(path.join("store"), path.join("checkpoint"), digest) {
            Ok(store) => open.push(store),
            Err(error) => {
                assert_eq!(
                    error.raw_os_error(),
                    Some(24),
                    "unexpected open error: {error}"
                );
                failed = Some(index);
                break;
            }
        }
    }
    let failed = failed.expect("fixture did not exhaust the child descriptor limit");
    assert!(open.len() >= 2, "too few open stores for the retry check");
    for store in &open {
        assert_eq!(
            store.get(b"safe").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
    }
    // Already-open stores retain the descriptors needed to commit and read.
    open[0].put(b"during-exhaustion", b"acknowledged").unwrap();
    assert_eq!(
        open[0].get(b"during-exhaustion").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    // A failed open must release any partially opened descriptors and its
    // store lock. Free two stores to allow the next open's temporary handles.
    drop(open.pop());
    drop(open.pop());
    let path = project(&root, failed);
    let retry = CheckpointStore::open(
        path.join("store"),
        path.join("checkpoint"),
        &digests[failed],
    )
    .expect("retry after descriptor exhaustion should release its lock");
    retry.put(b"after-retry", b"acknowledged").unwrap();
    drop(retry);
    drop(open);
    println!("EMDA_FD_RETRY_INDEX={failed}");
}

#[test]
fn exhausted_open_can_retry_without_losing_existing_stores() {
    for v2 in [true, false] {
        let root = tempfile::tempdir().unwrap();
        for index in 0..PROJECTS {
            let directory = project(root.path(), index);
            let path = directory.join("store");
            fs::create_dir(&directory).unwrap();
            if v2 {
                fs::create_dir(&path).unwrap();
                fs::write(
                    path.join("records.log"),
                    include_bytes!("fixtures/v2-empty-records.log"),
                )
                .unwrap();
                fs::write(
                    path.join("commit.state"),
                    include_bytes!("fixtures/v2-empty-commit.state"),
                )
                .unwrap();
            }
            let store = Store::open(&path).unwrap();
            store.put(b"safe", b"acknowledged").unwrap();
            let digest = store.checkpoint_to(directory.join("checkpoint")).unwrap();
            fs::write(directory.join("digest"), digest).unwrap();
            drop(store);
            let header = fs::read(path.join("records.log")).unwrap();
            assert_eq!(&header[..8], if v2 { b"EMDALOG2" } else { b"EMDALOG3" });
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "checkpoint_fd_exhaustion_child", "--nocapture"])
            .env("EMDA_FD_EXHAUSTION_ROOT", root.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let retried: usize = stdout
            .lines()
            .find_map(|line| {
                line.split_once("EMDA_FD_RETRY_INDEX=")
                    .map(|(_, value)| value)
            })
            .expect("child did not report retry index")
            .trim()
            .parse()
            .unwrap();
        for index in 0..PROJECTS {
            let store = Store::open(project(root.path(), index).join("store")).unwrap();
            assert_eq!(
                store.get(b"safe").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            if index == 0 {
                assert_eq!(
                    store.get(b"during-exhaustion").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
            }
            if index == retried {
                assert_eq!(
                    store.get(b"after-retry").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
            }
            drop(store);
            let directory = project(root.path(), index);
            let digest: [u8; 32] = fs::read(directory.join("digest"))
                .unwrap()
                .try_into()
                .unwrap();
            let recovered = CheckpointStore::open(
                directory.join("store"),
                directory.join("checkpoint"),
                &digest,
            )
            .unwrap();
            assert_eq!(
                recovered.get(b"safe").unwrap().as_deref(),
                Some(&b"acknowledged"[..])
            );
            if index == 0 {
                assert_eq!(
                    recovered.get(b"during-exhaustion").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
            }
            if index == retried {
                assert_eq!(
                    recovered.get(b"after-retry").unwrap().as_deref(),
                    Some(&b"acknowledged"[..])
                );
            }
        }
    }
}
