use database_engine::Store;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::process::{Command, Stdio};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

const LOG_HEADER_BYTES: u64 = 28;

#[test]
fn updates_deletes_and_empty_values_survive_reopening() {
    let dir = tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"name", b"old").unwrap();
        store.put(b"name", b"new").unwrap();
        store.put(b"empty", b"").unwrap();
        store.put(b"removed", b"gone").unwrap();
        store.delete(b"removed").unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.get(b"name").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(store.get(b"empty").unwrap().as_deref(), Some(&b""[..]));
    assert_eq!(store.get(b"removed").unwrap(), None);
}

#[test]
fn incomplete_tail_refuses_to_open_without_modifying_the_log() {
    let dir = tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"saved", b"value").unwrap();
    }
    let log = dir.path().join("records.log");
    OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(b"DB")
        .unwrap();
    let bytes_before = std::fs::read(&log).unwrap();
    let result = Store::open(dir.path());
    assert!(matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::InvalidData));
    assert_eq!(std::fs::read(&log).unwrap(), bytes_before);
}

#[test]
fn fully_present_corrupt_record_is_not_silently_discarded() {
    let dir = tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"key", b"value").unwrap();
    }
    let path = dir.path().join("records.log");
    let mut log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    log.seek(SeekFrom::Start(LOG_HEADER_BYTES + 20)).unwrap();
    let mut original = [0];
    log.read_exact(&mut original).unwrap();
    log.seek(SeekFrom::Start(LOG_HEADER_BYTES + 20)).unwrap();
    log.write_all(&[original[0] ^ 1]).unwrap();
    log.sync_all().unwrap();
    let result = Store::open(dir.path());
    assert!(matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::InvalidData));
}

#[test]
fn corrupt_header_is_not_mistaken_for_an_incomplete_tail() {
    let dir = tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"key", b"value").unwrap();
    }
    let mut log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.path().join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start(LOG_HEADER_BYTES + 5)).unwrap();
    log.write_all(&[0xff]).unwrap();
    log.sync_all().unwrap();
    let result = Store::open(dir.path());
    assert!(matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::InvalidData));
}

#[test]
fn unsupported_record_version_is_rejected_without_changing_the_log() {
    let dir = tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"key", b"value").unwrap();
    }
    let path = dir.path().join("records.log");
    let mut log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut header = [0_u8; 17];
    log.seek(SeekFrom::Start(LOG_HEADER_BYTES)).unwrap();
    log.read_exact(&mut header).unwrap();
    header[3] = b'2';
    let checksum = crc32fast::hash(&header[..13]);
    header[13..17].copy_from_slice(&checksum.to_le_bytes());
    log.seek(SeekFrom::Start(LOG_HEADER_BYTES)).unwrap();
    log.write_all(&header).unwrap();
    log.sync_all().unwrap();
    let bytes_before = std::fs::read(&path).unwrap();
    let result = Store::open(dir.path());
    assert!(matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::InvalidData));
    assert_eq!(std::fs::read(path).unwrap(), bytes_before);
}

#[test]
fn value_corruption_after_open_is_detected_on_read() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"key", b"value").unwrap();
    let mut log = OpenOptions::new()
        .write(true)
        .open(dir.path().join("records.log"))
        .unwrap();
    log.seek(SeekFrom::Start(LOG_HEADER_BYTES + 20)).unwrap();
    log.write_all(b"X").unwrap();
    log.sync_all().unwrap();
    assert_eq!(
        store.get(b"key").unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[test]
fn all_partial_final_records_fail_closed_without_data_loss() {
    let source = tempdir().unwrap();
    let log_path = source.path().join("records.log");
    let baseline_len;
    {
        let store = Store::open(source.path()).unwrap();
        store.put(b"first", b"saved").unwrap();
        baseline_len = std::fs::metadata(&log_path).unwrap().len();
        store.put(b"last", b"incomplete").unwrap();
    }
    let log_bytes = std::fs::read(&log_path).unwrap();
    for end in baseline_len as usize + 1..log_bytes.len() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("records.log"), &log_bytes[..end]).unwrap();
        std::fs::copy(
            source.path().join("commit.state"),
            dir.path().join("commit.state"),
        )
        .unwrap();
        let result = Store::open(dir.path());
        assert!(
            matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::InvalidData),
            "partial log length {end}"
        );
        assert_eq!(
            std::fs::read(dir.path().join("records.log")).unwrap(),
            &log_bytes[..end]
        );
    }
}

#[test]
fn second_owner_cannot_open_a_locked_store() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    assert!(Store::open(dir.path()).is_err());
    drop(store);
    assert!(Store::open(dir.path()).is_ok());
}

#[test]
fn missing_log_of_existing_store_does_not_create_an_empty_database() {
    let dir = tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"important", b"value").unwrap();
    }
    let log = dir.path().join("records.log");
    std::fs::remove_file(&log).unwrap();
    let result = Store::open(dir.path());
    assert!(matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::InvalidData));
    assert!(!log.exists());
}

#[test]
fn concurrent_clients_write_and_read_independent_keys() {
    let dir = tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let mut workers = Vec::new();
    for id in 0..6 {
        let store = Arc::clone(&store);
        workers.push(thread::spawn(move || {
            for item in 0..25 {
                let key = format!("{id}-{item}");
                store.put(key.as_bytes(), key.as_bytes()).unwrap();
                assert_eq!(
                    store.get(key.as_bytes()).unwrap().as_deref(),
                    Some(key.as_bytes())
                );
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    for id in 0..6 {
        for item in 0..25 {
            let key = format!("{id}-{item}");
            assert_eq!(
                reopened.get(key.as_bytes()).unwrap().as_deref(),
                Some(key.as_bytes())
            );
        }
    }
}

#[test]
fn concurrent_readers_never_observe_partial_overwrites() {
    let dir = tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let a = vec![b'a'; 4096];
    let b = vec![b'b'; 4096];
    store.put(b"shared", &a).unwrap();
    let mut workers = Vec::new();
    for id in 0..4 {
        let store = Arc::clone(&store);
        workers.push(thread::spawn(move || {
            for _ in 0..40 {
                if id < 2 {
                    let value = if id == 0 { b'a' } else { b'b' };
                    store.put(b"shared", &vec![value; 4096]).unwrap();
                } else {
                    let value = store.get(b"shared").unwrap().unwrap();
                    assert!(value == vec![b'a'; 4096] || value == vec![b'b'; 4096]);
                }
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    let expected = store.get(b"shared").unwrap().unwrap();
    assert!(expected == a || expected == b);
    drop(store);
    assert_eq!(
        Store::open(dir.path()).unwrap().get(b"shared").unwrap(),
        Some(expected)
    );
}

#[test]
fn rejected_inputs_do_not_affect_existing_data() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.put(b"saved", b"ok").unwrap();
    assert_eq!(
        store.put(b"", b"value").unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        store.put(&[b'x'; 1025], b"value").unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        store
            .put(b"big", &vec![0; 16 * 1024 * 1024 + 1])
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(store.get(b"saved").unwrap().as_deref(), Some(&b"ok"[..]));
}

// The parent test starts this test in a separate process and kills it after
// acknowledged writes, simulating abrupt process termination (not power loss).
#[test]
fn child_writer_process() {
    let Ok(path) = std::env::var("ENGINE_CHILD_PATH") else {
        return;
    };
    let store = Arc::new(Store::open(path).unwrap());
    for item in 0..10 {
        let key = format!("child-{item}");
        store.put(key.as_bytes(), b"committed").unwrap();
    }
    let mut workers = Vec::new();
    for id in 0..6 {
        let store = Arc::clone(&store);
        workers.push(thread::spawn(move || {
            for item in 0..20 {
                let key = format!("group-child-{id}-{item}");
                store.put(key.as_bytes(), b"committed").unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    println!("ENGINE_WRITES_ACKNOWLEDGED");
    std::io::stdout().flush().unwrap();
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

#[test]
fn acknowledged_writes_survive_abrupt_process_termination() {
    let dir = tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_writer_process", "--nocapture"])
        .env("ENGINE_CHILD_PATH", dir.path())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(output)
            .lines()
            .map_while(Result::ok)
        {
            if line.contains("ENGINE_WRITES_ACKNOWLEDGED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let ready = rx.recv_timeout(Duration::from_secs(20));
    child.kill().unwrap();
    child.wait().unwrap();
    ready.expect("child did not acknowledge writes");
    let store = Store::open(dir.path()).unwrap();
    for item in 0..10 {
        let key = format!("child-{item}");
        assert_eq!(
            store.get(key.as_bytes()).unwrap().as_deref(),
            Some(&b"committed"[..])
        );
    }
    for id in 0..6 {
        for item in 0..20 {
            let key = format!("group-child-{id}-{item}");
            assert_eq!(
                store.get(key.as_bytes()).unwrap().as_deref(),
                Some(&b"committed"[..])
            );
        }
    }
}

#[test]
fn recovery_checks_records_across_buffer_boundaries() {
    let dir = tempdir().unwrap();
    let first = vec![0x5a; 65536 - 6 - LOG_HEADER_BYTES as usize - 17 - 1 - 4];
    let large = vec![0xc3; 131_079];
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"a", &first).unwrap();
        // The next header starts six bytes before a 64 KiB boundary, and its
        // payload spans multiple buffers and the large-read path.
        store.put(b"next", &large).unwrap();
        store.put(b"last", b"value").unwrap();
    }
    {
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(first));
        assert_eq!(store.get(b"next").unwrap(), Some(large));
        assert_eq!(store.get(b"last").unwrap().as_deref(), Some(&b"value"[..]));
    }
    let path = dir.path().join("records.log");
    let mut log = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    log.seek(SeekFrom::Start((65536 - 6 + 17 + 4 + 12345) as u64))
        .unwrap();
    let mut byte = [0_u8];
    log.read_exact(&mut byte).unwrap();
    log.seek(SeekFrom::Current(-1)).unwrap();
    log.write_all(&[byte[0] ^ 1]).unwrap();
    log.sync_all().unwrap();
    let before = std::fs::read(&path).unwrap();
    let error = Store::open(dir.path());
    assert!(matches!(error, Err(ref error) if error.kind() == std::io::ErrorKind::InvalidData));
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn partial_records_at_buffer_boundaries_fail_closed() {
    let source = tempdir().unwrap();
    {
        let store = Store::open(source.path()).unwrap();
        store
            .put(
                b"a",
                &vec![0x5a; 65536 - 6 - LOG_HEADER_BYTES as usize - 17 - 1 - 4],
            )
            .unwrap();
        store.put(b"next", &vec![0xc3; 131_079]).unwrap();
    }
    let original = std::fs::read(source.path().join("records.log")).unwrap();
    let cuts = [
        65_529,
        65_531,
        65_535,
        65_536,
        65_537,
        65_546,
        65_547,
        131_071,
        131_072,
        131_073,
        original.len() - 1,
    ];
    for cut in cuts {
        let dir = tempdir().unwrap();
        let path = dir.path().join("records.log");
        std::fs::write(&path, &original[..cut]).unwrap();
        std::fs::copy(
            source.path().join("commit.state"),
            dir.path().join("commit.state"),
        )
        .unwrap();
        let result = Store::open(dir.path());
        assert!(
            matches!(result, Err(ref error) if error.kind() == std::io::ErrorKind::InvalidData),
            "cut={cut}"
        );
        assert_eq!(std::fs::read(path).unwrap(), &original[..cut]);
    }
}

#[test]
fn recovery_preserves_maximum_sized_value() {
    let dir = tempdir().unwrap();
    let value: Vec<_> = (0..16 * 1024 * 1024).map(|i| (i * 17 + 3) as u8).collect();
    {
        let store = Store::open(dir.path()).unwrap();
        store.put(b"maximum", &value).unwrap();
    }
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"maximum").unwrap().as_deref(),
        Some(value.as_slice())
    );
}
