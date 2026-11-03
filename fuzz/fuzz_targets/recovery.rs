#![no_main]
use database_engine::Store;
use libfuzzer_sys::fuzz_target;
use std::{fs, io};

// First byte: high bit indicates a witness file; low bits give its length.
// Remaining bytes are the witness followed by the log. Rejected malformed
// input must not change either file; an I/O failure during witness publication
// can have an uncertain outcome, so it is not treated as a no-write guarantee.
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 8192 {
        return;
    }
    let has_witness = data[0] & 0x80 != 0;
    let witness_len = if has_witness {
        (data[0] & 0x7f) as usize
    } else {
        0
    };
    if data.len() < 1 + witness_len {
        return;
    }
    let witness = &data[1..1 + witness_len];
    let log = &data[1 + witness_len..];
    let directory = tempfile::tempdir().expect("fuzz temporary directory");
    let log_path = directory.path().join("records.log");
    let witness_path = directory.path().join("commit.state");
    fs::write(&log_path, log).expect("write fuzz log");
    if has_witness {
        fs::write(&witness_path, witness).expect("write fuzz witness");
    }
    match Store::open(directory.path()) {
        Ok(store) => {
            let _ = store.get(b"probe");
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ) =>
        {
            assert_eq!(fs::read(log_path).expect("read rejected log"), log);
            if has_witness {
                assert_eq!(
                    fs::read(witness_path).expect("read rejected witness"),
                    witness
                );
            } else {
                assert!(!witness_path.exists());
            }
        }
        Err(_) => {}
    }
});
