//! A separate test binary avoids leaking unrelated test locks into the forked child.
use database_engine::{BatchOperation, CheckpointStore, Store};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

#[test]
fn acknowledged_checkpoint_writes_survive_process_termination() {
    if let Ok(parent) = std::env::var("EMDA_CHECKPOINT_KILL_CHILD") {
        let parent = std::path::PathBuf::from(parent);
        let digest = fs::read(parent.join("digest")).unwrap().try_into().unwrap();
        let store = CheckpointStore::open(parent.join("store"), parent.join("checkpoint"), &digest)
            .unwrap();
        store.put(b"single", b"acknowledged").unwrap();
        store
            .write_batch(&[
                BatchOperation::Put {
                    key: b"left",
                    value: b"one",
                },
                BatchOperation::Put {
                    key: b"right",
                    value: b"two",
                },
            ])
            .unwrap();
        let rotated = store.rollover_to(parent.join("rotated")).unwrap();
        fs::write(parent.join("rotated.digest"), rotated).unwrap();
        store.put(b"after-rollover", b"acknowledged").unwrap();
        println!("EMDA_CHECKPOINT_KILL_ACKNOWLEDGED");
        io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    initial.put(b"base", b"persisted").unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    fs::write(dir.path().join("digest"), digest).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "acknowledged_checkpoint_writes_survive_process_termination",
            "--nocapture",
        ])
        .env("EMDA_CHECKPOINT_KILL_CHILD", dir.path())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("EMDA_CHECKPOINT_KILL_ACKNOWLEDGED") {
                let _ = send.send(());
                break;
            }
        }
    });
    let acknowledged = receive.recv_timeout(Duration::from_secs(10));
    let _ = child.kill();
    let _ = child.wait();
    reader.join().unwrap();
    acknowledged.expect("child did not acknowledge writes before timeout");

    let rotated_digest = fs::read(dir.path().join("rotated.digest"))
        .unwrap()
        .try_into()
        .unwrap();
    let store = CheckpointStore::open(&path, dir.path().join("rotated"), &rotated_digest).unwrap();
    assert_eq!(
        store.get(b"single").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(store.get(b"left").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(store.get(b"right").unwrap().as_deref(), Some(&b"two"[..]));
    assert_eq!(
        store.get(b"after-rollover").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
}
