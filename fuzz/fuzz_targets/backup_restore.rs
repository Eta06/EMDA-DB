#![no_main]
use database_engine::{BatchOperation, Store};
use libfuzzer_sys::fuzz_target;
use sha2::{Digest, Sha256};
use std::fs;
use std::io;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 || data.len() > 256 {
        return;
    }
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("source");
    let backup = dir.path().join("backup");
    let output = dir.path().join("restored");
    let store = Store::open(&path).expect("create store");
    store.put(b"old", b"value").expect("write");
    store
        .write_batch(&[
            BatchOperation::Delete { key: b"old" },
            BatchOperation::Put {
                key: b"new",
                value: b"content",
            },
        ])
        .expect("batch");
    store.backup(&backup).expect("backup");
    drop(store);
    let mut bytes = fs::read(&backup).expect("read backup");
    let log_end = bytes.len() - 48;
    if data[0] & 1 != 0 {
        let offset = 8 + usize::from(data[1]) % (log_end - 8);
        bytes[offset] ^= data[2] | 1;
    }
    if data[0] & 2 != 0 {
        // Recompute the unauthenticated outer digest to exercise the record
        // parser rather than only the backup envelope checksum.
        let digest = Sha256::digest(&bytes[8..log_end]);
        bytes[log_end + 16..].copy_from_slice(&digest);
    }
    if data[0] & 4 != 0 {
        let length = usize::from(data[3]) % bytes.len();
        bytes.truncate(length);
    }
    if data[0] & 8 != 0 {
        bytes.extend_from_slice(&data[4..]);
    }
    fs::write(&backup, &bytes).expect("write candidate backup");
    match Store::restore(&backup, &output) {
        Ok(()) => {
            let restored = Store::open(&output).expect("open restored store");
            let expected = restored.scan(None, None, 20).expect("restored scan");
            drop(restored);
            let reopened = Store::open(&output).expect("reopen restored store");
            assert_eq!(
                reopened.scan(None, None, 20).expect("reopened scan"),
                expected
            );
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidData
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::InvalidInput
            ) =>
        {
            assert!(!output.exists());
        }
        Err(_) => {}
    }
});
