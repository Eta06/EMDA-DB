use super::*;

fn pause_compaction() -> io::Result<()> {
    println!("CHECKPOINT_COMPACTION_BOUNDARY_REACHED");
    io::stdout().flush()?;
    loop {
        std::thread::park();
    }
}

#[test]
fn checkpoint_compaction_crash_child() {
    let Ok(parent) = std::env::var("EMDA_CHECKPOINT_COMPACTION_PATH") else {
        return;
    };
    let parent = Path::new(&parent);
    let mode = std::env::var("EMDA_CHECKPOINT_COMPACTION_STAGE").unwrap();
    let digest = fs::read(parent.join("digest")).unwrap().try_into().unwrap();
    let store = retry_lock(|| {
        CheckpointStore::open(parent.join("store"), parent.join("checkpoint"), &digest)
    })
    .unwrap();
    let state = store.read().unwrap();
    let (count, _) = store.verify_for_checkpoint(&state).unwrap();
    crate::store::compact_checkpoint_with_hooks(
        &store.log,
        &state.view,
        count,
        &parent.join("copy"),
        |parent| {
            if mode == "published" {
                pause_compaction()
            } else {
                File::open(parent)?.sync_all()
            }
        },
        || {
            if mode == "staged" {
                pause_compaction()
            } else {
                Ok(())
            }
        },
        |_| Ok(()),
    )
    .unwrap();
}

#[test]
fn checkpoint_compaction_process_kill_preserves_source_and_cleanup() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    for mode in ["staged", "published"] {
        let (dir, path, checkpoint, digest) = fixture();
        let source = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        source.put(b"key", b"value").unwrap();
        let original = fs::read(path.join("records.log")).unwrap();
        drop(source);
        fs::write(dir.path().join("digest"), digest).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "checkpoint_store::tests::compaction::checkpoint_compaction_crash_child",
                "--nocapture",
            ])
            .env("EMDA_CHECKPOINT_COMPACTION_PATH", dir.path())
            .env("EMDA_CHECKPOINT_COMPACTION_STAGE", mode)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(output).lines().map_while(Result::ok) {
                if line.contains("CHECKPOINT_COMPACTION_BOUNDARY_REACHED") {
                    let _ = send.send(());
                    break;
                }
            }
        });
        let reached = receive.recv_timeout(Duration::from_secs(20));
        let busy = crate::cleanup_abandoned_staging(dir.path());
        let _ = child.kill();
        let _ = child.wait();
        reader.join().unwrap();
        reached.expect("compaction child did not reach requested boundary");
        assert_eq!(busy.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        let cleaned = retry_lock(|| crate::cleanup_abandoned_staging(dir.path())).unwrap();
        assert_eq!(cleaned.removed_markers, 1);
        assert_eq!(cleaned.removed_payloads, usize::from(mode == "staged"));
        assert_eq!(fs::read(path.join("records.log")).unwrap(), original);
        let source = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        assert_eq!(source.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        let destination = dir.path().join("copy");
        if mode == "staged" {
            assert!(!destination.exists());
        } else {
            let bytes = fs::read(destination.join(crate::COMPACTED_CHECKPOINT_FILE)).unwrap();
            let digest = bytes[bytes.len() - 32..].try_into().unwrap();
            let copy = retry_lock(|| {
                CheckpointStore::open(
                    &destination,
                    destination.join(crate::COMPACTED_CHECKPOINT_FILE),
                    &digest,
                )
            })
            .unwrap();
            assert_eq!(copy.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        }
    }
}

#[test]
fn streamed_compaction_rejects_corrupt_staged_data_before_publication() {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    store.put(b"key", b"value").unwrap();
    let destination = dir.path().join("must-not-exist");
    let state = store.read().unwrap();
    let (count, _) = store.verify_for_checkpoint(&state).unwrap();
    let result = crate::store::compact_checkpoint_with_hooks(
        &store.log,
        &state.view,
        count,
        &destination,
        |parent| File::open(parent)?.sync_all(),
        || Ok(()),
        |stage| {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(stage.join("records.log"))?;
            let mut byte = [0];
            FileExt::read_exact_at(&file, &mut byte, 28)?;
            byte[0] ^= 0x40;
            FileExt::write_all_at(&file, &byte, 28)
        },
    );
    assert!(result.is_err());
    assert!(!destination.exists());
    drop(state);
    assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
}

#[test]
fn streamed_compaction_reports_pre_and_post_publication_failures() {
    for post_publish in [false, true] {
        let (dir, path, checkpoint, digest) = fixture();
        let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
        store.put(b"key", b"value").unwrap();
        let destination = dir.path().join("copy");
        let state = store.read().unwrap();
        let (count, _) = store.verify_for_checkpoint(&state).unwrap();
        let result = crate::store::compact_checkpoint_with_hooks(
            &store.log,
            &state.view,
            count,
            &destination,
            |parent| {
                if post_publish {
                    Err(io::Error::other("injected parent sync failure"))
                } else {
                    File::open(parent)?.sync_all()
                }
            },
            || {
                if post_publish {
                    Ok(())
                } else {
                    Err(io::Error::other("injected prepublication failure"))
                }
            },
            |_| Ok(()),
        );
        let error = result.unwrap_err();
        assert_eq!(destination.exists(), post_publish);
        if post_publish {
            assert!(error.to_string().contains("compacted store may exist"));
            let bytes = fs::read(destination.join(crate::COMPACTED_CHECKPOINT_FILE)).unwrap();
            let digest = bytes[bytes.len() - 32..].try_into().unwrap();
            let copy = retry_lock(|| {
                CheckpointStore::open(
                    &destination,
                    destination.join(crate::COMPACTED_CHECKPOINT_FILE),
                    &digest,
                )
            })
            .unwrap();
            assert_eq!(copy.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        }
        drop(state);
        assert_eq!(store.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
        store.put(b"later", b"still-writable").unwrap();
    }
}

#[test]
fn streamed_compaction_blocks_mutations_while_allowing_reads() {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    let (dir, path, checkpoint, digest) = fixture();
    let store =
        Arc::new(retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap());
    let destination = dir.path().join("copy");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let digest = std::thread::scope(|scope| {
        let copying = Arc::clone(&store);
        let destination = &destination;
        let worker = scope.spawn(move || {
            let state = copying.read().unwrap();
            let (count, _) = copying.verify_for_checkpoint(&state).unwrap();
            crate::store::compact_checkpoint_with_hooks(
                &copying.log,
                &state.view,
                count,
                destination,
                |parent| File::open(parent)?.sync_all(),
                || {
                    entered_tx.send(()).unwrap();
                    resume_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|_| io::Error::other("compaction test release timed out"))?;
                    Ok(())
                },
                |_| Ok(()),
            )
            .unwrap()
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(
            store.get(b"base").unwrap().as_deref(),
            Some(&b"acknowledged"[..])
        );
        let writer = Arc::clone(&store);
        let writer = scope.spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(writer.put(b"later", b"source-only")).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(50));
        resume_tx.send(()).unwrap();
        let digest = worker.join().unwrap();
        writer.join().unwrap();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        digest
    });
    let copy = retry_lock(|| {
        CheckpointStore::open(
            &destination,
            destination.join(crate::COMPACTED_CHECKPOINT_FILE),
            &digest,
        )
    })
    .unwrap();
    assert_eq!(copy.get(b"later").unwrap(), None);
    assert_eq!(
        store.get(b"later").unwrap().as_deref(),
        Some(&b"source-only"[..])
    );
}
