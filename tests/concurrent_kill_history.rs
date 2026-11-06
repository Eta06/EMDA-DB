//! Acknowledged independent writes during concurrent process-kill races.
//! SIGKILL leaves OS/device caches running and is not a power-loss test.
#![cfg(target_os = "linux")]

use database_engine::{CheckpointStore, Store};
use rustix::fs::getxattr;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Barrier, mpsc};
use std::time::{Duration, Instant};

mod support;
use support::retry_lock;

const XATTR: &str = "user.db.commit.v3";

#[derive(Clone, Copy, Debug)]
enum Format {
    V2,
    V3,
}

trait TestStore: Sync {
    fn write(&self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
}

impl TestStore for Store {
    fn write(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.put(key, value)
    }
    fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get(key)
    }
}

impl TestStore for CheckpointStore {
    fn write(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.put(key, value)
    }
    fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get(key)
    }
}

fn key(worker: usize, sequence: usize) -> Vec<u8> {
    format!("worker-{worker}-{sequence:04}").into_bytes()
}

fn value(worker: usize, sequence: usize) -> Vec<u8> {
    format!("value-{worker}-{sequence:04}").into_bytes()
}

fn write_until_killed(store: &impl TestStore) -> ! {
    let (sender, receiver) = mpsc::channel();
    let barrier = Barrier::new(5);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let sender = sender.clone();
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for sequence in 0..1000 {
                    store
                        .write(&key(worker, sequence), &value(worker, sequence))
                        .unwrap();
                    sender.send((worker, sequence)).unwrap();
                }
            });
        }
        drop(sender);
        barrier.wait();
        for (worker, sequence) in receiver {
            println!("EMDA_CONCURRENT_ACK {worker} {sequence}");
            io::stdout().flush().unwrap();
        }
        loop {
            std::thread::park();
        }
    });
    unreachable!()
}

#[test]
fn concurrent_kill_child() {
    let Some(parent) = std::env::var_os("EMDA_CONCURRENT_KILL_PARENT") else {
        return;
    };
    let parent = Path::new(&parent);
    let path = parent.join("store");
    if std::env::var("EMDA_CONCURRENT_KILL_BACKEND").unwrap() == "checkpoint" {
        let digest = fs::read(parent.join("digest")).unwrap().try_into().unwrap();
        let store = retry_lock(|| CheckpointStore::open(&path, parent.join("checkpoint"), &digest))
            .unwrap();
        write_until_killed(&store);
    } else {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        write_until_killed(&store);
    }
}

fn marker(path: &Path, format: Format) -> Vec<u8> {
    match format {
        Format::V2 => fs::read(path.join("commit.state")).unwrap(),
        Format::V3 => {
            let mut bytes = [0; 36];
            assert_eq!(
                getxattr(path.join("records.log"), XATTR, &mut bytes[..]).unwrap(),
                bytes.len()
            );
            bytes.to_vec()
        }
    }
}

fn check_open<S: TestStore>(opened: io::Result<S>, receipts: &[(usize, usize)]) -> bool {
    match opened {
        Ok(store) => {
            assert_eq!(store.read(b"safe").unwrap().as_deref(), Some(&b"guard"[..]));
            for &(worker, sequence) in receipts {
                assert_eq!(
                    store.read(&key(worker, sequence)).unwrap(),
                    Some(value(worker, sequence)),
                    "lost acknowledged write {worker}:{sequence}"
                );
            }
            true
        }
        Err(error) => {
            assert!(
                matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ),
                "unexpected recovery error: {error}"
            );
            false
        }
    }
}

fn run_case(format: Format, checkpoint_writer: bool, receipt_count: usize) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    fs::create_dir(&path).unwrap();
    if matches!(format, Format::V2) {
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
    store.put(b"safe", b"guard").unwrap();
    let checkpoint = root.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    fs::write(root.path().join("digest"), digest).unwrap();
    let initial_log = fs::read(path.join("records.log")).unwrap();
    assert_eq!(
        &initial_log[..8],
        if matches!(format, Format::V2) {
            b"EMDALOG2"
        } else {
            b"EMDALOG3"
        }
    );
    let anchor = fs::read(path.join("commit.state")).unwrap();
    drop(store);

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "concurrent_kill_child", "--nocapture"])
        .env("EMDA_CONCURRENT_KILL_PARENT", root.path())
        .env(
            "EMDA_CONCURRENT_KILL_BACKEND",
            if checkpoint_writer {
                "checkpoint"
            } else {
                "ordinary"
            },
        )
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in io::BufReader::new(output).lines().map_while(Result::ok) {
            if let Some((_, message)) = line.split_once("EMDA_CONCURRENT_ACK ") {
                let mut parts = message.split_whitespace();
                if let (Some(worker), Some(sequence)) = (parts.next(), parts.next()) {
                    let receipt = (
                        worker.parse::<usize>().unwrap(),
                        sequence.parse::<usize>().unwrap(),
                    );
                    if sender.send(receipt).is_err() {
                        break;
                    }
                }
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut receipts = Vec::new();
    while receipts.len() < receipt_count {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match receiver.recv_timeout(remaining) {
            Ok(receipt) => receipts.push(receipt),
            Err(_) => break,
        }
    }
    let busy = receipts.len() == receipt_count
        && matches!(Store::open(&path), Err(ref error) if error.kind() == io::ErrorKind::WouldBlock);
    let observed_prefix = fs::read(path.join("records.log")).unwrap();
    let killed = child.kill();
    child.wait().unwrap();
    drop(receiver);
    reader.join().unwrap();
    assert_eq!(
        receipts.len(),
        receipt_count,
        "{format:?} checkpoint={checkpoint_writer}"
    );
    killed.unwrap();
    assert!(busy, "active child must hold the store lock");
    let interrupted_log = fs::read(path.join("records.log")).unwrap();
    let interrupted_marker = marker(&path, format);
    assert!(interrupted_log.starts_with(&observed_prefix));
    assert!(interrupted_log.starts_with(&initial_log));
    for &(worker, sequence) in &receipts {
        let key = key(worker, sequence);
        let value = value(worker, sequence);
        assert!(
            observed_prefix
                .windows(key.len())
                .any(|part| part == key.as_slice())
        );
        assert!(
            observed_prefix
                .windows(value.len())
                .any(|part| part == value.as_slice())
        );
    }

    let first = if checkpoint_writer {
        check_open(
            retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)),
            &receipts,
        )
    } else {
        check_open(retry_lock(|| Store::open(&path)), &receipts)
    };
    if !first {
        assert_eq!(marker(&path, format), interrupted_marker);
    }
    assert_eq!(fs::read(path.join("records.log")).unwrap(), interrupted_log);
    let second = if checkpoint_writer {
        check_open(retry_lock(|| Store::open(&path)), &receipts)
    } else {
        check_open(
            retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)),
            &receipts,
        )
    };
    assert_eq!(first, second, "recovery paths disagreed");
    if !second {
        assert_eq!(marker(&path, format), interrupted_marker);
    }
    assert_eq!(fs::read(path.join("records.log")).unwrap(), interrupted_log);
    if matches!(format, Format::V3) {
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), anchor);
    }
    println!(
        "format={format:?} checkpoint_writer={checkpoint_writer} receipts={} recovered={first}",
        receipts.len()
    );
}

#[test]
fn acknowledged_concurrent_writes_survive_process_kill_or_fail_closed() {
    for format in [Format::V2, Format::V3] {
        for checkpoint_writer in [false, true] {
            for count in [1, 8, 32, 128] {
                run_case(format, checkpoint_writer, count);
            }
        }
    }
}
