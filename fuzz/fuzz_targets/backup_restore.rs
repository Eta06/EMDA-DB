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
    if data[0] & 0x20 != 0 {
        // Explicit empty-legacy upgrade must preserve its source and never
        // replace a destination or accept nonempty/witnessed input.
        fs::create_dir(&path).expect("create legacy source");
        let mode = data[1] % 5;
        if mode != 3 {
            let contents = if mode == 1 { &data[2..] } else { &[] };
            fs::write(path.join("records.log"), contents).expect("write legacy source");
        }
        if mode == 2 {
            fs::write(path.join("commit.state"), b"existing").expect("write witness");
        }
        if mode == 4 {
            fs::create_dir(&output).expect("create existing destination");
            fs::write(output.join("sentinel"), b"keep").expect("write sentinel");
        }
        let result = Store::upgrade_empty_legacy(&path, &output);
        if mode == 0 {
            result.expect("upgrade verified empty legacy source");
            let restored = Store::open(&output).expect("open witnessed upgrade");
            restored
                .transaction(|tx| tx.put(b"probe", b"value"))
                .expect("atomic write");
            drop(restored);
            let reopened = Store::open(&output).expect("reopen witnessed upgrade");
            assert_eq!(
                reopened.get(b"probe").expect("read"),
                Some(b"value".to_vec())
            );
        } else {
            assert!(result.is_err());
            if mode == 4 {
                assert_eq!(
                    fs::read(output.join("sentinel")).expect("sentinel"),
                    b"keep"
                );
            } else {
                assert!(!output.exists());
            }
        }
        if mode == 3 {
            assert!(!path.join("records.log").exists());
        } else {
            let expected = if mode == 1 { &data[2..] } else { &[] };
            assert_eq!(
                fs::read(path.join("records.log")).expect("read source"),
                expected
            );
        }
        if mode == 2 {
            assert_eq!(
                fs::read(path.join("commit.state")).expect("read witness"),
                b"existing"
            );
        } else {
            assert!(!path.join("commit.state").exists());
        }
        return;
    }
    if data[0] & 0x10 != 0 {
        // A verified zero-length legacy backup must restore as a usable
        // witnessed store. Mutating its envelope must never publish it.
        let mut bytes = b"DBBKP001".to_vec();
        bytes.extend_from_slice(b"DBBEND01");
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&Sha256::digest([]));
        if data[0] & 1 != 0 {
            let index = usize::from(data[1]) % bytes.len();
            bytes[index] ^= data[2] | 1;
        }
        if data[0] & 2 != 0 {
            bytes.push(data[3]);
        }
        fs::write(&backup, &bytes).expect("write empty legacy backup");
        if data[0] & 3 != 0 {
            assert!(Store::restore(&backup, &output).is_err());
            assert!(!output.exists());
        } else {
            Store::restore(&backup, &output).expect("restore empty legacy backup");
            let restored = Store::open(&output).expect("open witnessed restore");
            assert_eq!(restored.get(b"probe").expect("read"), None);
            restored
                .transaction(|tx| tx.put(b"probe", b"value"))
                .expect("atomic write");
            drop(restored);
            let reopened = Store::open(&output).expect("reopen witnessed restore");
            assert_eq!(
                reopened.get(b"probe").expect("read"),
                Some(b"value".to_vec())
            );
        }
        return;
    }
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
