#![no_main]
use database_engine::Store;
use libfuzzer_sys::fuzz_target;
use std::{fs, io};

// First byte: bit 7 indicates an anchor/witness file; bit 6 indicates
// metadata witness; low six bits give the external witness length. If bit 7
// is clear, bit 5 instead requests a dangling external witness symlink.
// Metadata is prefixed by its one-byte length after the external witness.
// Remaining bytes are the witness followed by the log. Rejected malformed
// input must not change either file; an I/O failure during witness publication
// can have an uncertain outcome, so it is not treated as a no-write guarantee.
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 8192 {
        return;
    }
    let has_witness = data[0] & 0x80 != 0;
    let has_metadata = data[0] & 0x40 != 0;
    let dangling_witness = !has_witness && data[0] & 0x20 != 0;
    let witness_len = if has_witness {
        (data[0] & 0x3f) as usize
    } else {
        0
    };
    if data.len() < 1 + witness_len {
        return;
    }
    let witness = &data[1..1 + witness_len];
    let mut start = 1 + witness_len;
    let metadata = if has_metadata {
        if data.len() <= start {
            return;
        }
        let length = data[start] as usize;
        start += 1;
        if data.len() < start + length {
            return;
        }
        let bytes = &data[start..start + length];
        start += length;
        Some(bytes)
    } else {
        None
    };
    let log = &data[start..];
    let directory = tempfile::tempdir().expect("fuzz temporary directory");
    let log_path = directory.path().join("records.log");
    let witness_path = directory.path().join("commit.state");
    fs::write(&log_path, log).expect("write fuzz log");
    if has_witness {
        fs::write(&witness_path, witness).expect("write fuzz witness");
    } else if dangling_witness {
        std::os::unix::fs::symlink("missing-witness", &witness_path)
            .expect("write dangling witness");
    }
    if let Some(bytes) = metadata {
        rustix::fs::setxattr(
            &log_path,
            "user.db.commit.v3",
            bytes,
            rustix::fs::XattrFlags::empty(),
        )
        .expect("write fuzz metadata");
    }
    match Store::open(directory.path()) {
        Ok(store) => {
            assert!(
                !log.is_empty(),
                "an existing empty log must not open as legacy"
            );
            assert!(
                !dangling_witness,
                "a dangling witness must not permit fallback"
            );
            let _ = store.get(b"probe");
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ) =>
        {
            assert_eq!(fs::read(&log_path).expect("read rejected log"), log);
            if let Some(bytes) = metadata {
                let mut actual = [0; 255];
                let count = rustix::fs::getxattr(&log_path, "user.db.commit.v3", &mut actual[..])
                    .expect("read rejected metadata");
                assert_eq!(&actual[..count], bytes);
            }
            if has_witness {
                assert_eq!(
                    fs::read(witness_path).expect("read rejected witness"),
                    witness
                );
            } else if dangling_witness {
                assert!(
                    fs::symlink_metadata(witness_path)
                        .expect("read dangling witness")
                        .file_type()
                        .is_symlink()
                );
            } else {
                assert!(!witness_path.exists());
            }
        }
        Err(_) => {}
    }
});
