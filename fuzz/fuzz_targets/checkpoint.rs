#![no_main]
use database_engine::Store;
use libfuzzer_sys::fuzz_target;
use sha2::{Digest, Sha256};
use std::fs;

const LOG: &[u8] = include_bytes!("../../tests/fixtures/v2-batch-records.log");
const WITNESS: &[u8] = include_bytes!("../../tests/fixtures/v2-batch-commit.state");
const TOKEN: &[u8; 32] = include_bytes!("../../tests/fixtures/v2-batch-index.sha256");

// Bit 0 selects a recomputed, attacker-controlled digest to exercise deeper
// parsing. This mode does not assert semantic integrity because the caller has
// explicitly supplied an untrusted digest; only a separately trusted token binds
// a checkpoint to the index produced by the engine.
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 8192 {
        return;
    }
    let dir = tempfile::tempdir().expect("temporary directory");
    let log = dir.path().join("records.log");
    let witness = dir.path().join("commit.state");
    let checkpoint = dir.path().join("checkpoint");
    fs::write(&log, LOG).expect("write log fixture");
    fs::write(&witness, WITNESS).expect("write witness fixture");
    let mut bytes = data[1..].to_vec();
    let mut token = *TOKEN;
    let trusted = data[0] & 1 == 0;
    if !trusted && bytes.len() >= 32 {
        let end = bytes.len() - 32;
        token = Sha256::digest(&bytes[..end]).into();
        bytes[end..].copy_from_slice(&token);
    }
    fs::write(&checkpoint, &bytes).expect("write fuzz checkpoint");
    if let Ok(store) =
        Store::open_with_checkpoint_and_limits(dir.path(), &checkpoint, &token, Some(4096), Some(2))
    {
        if trusted {
            assert_eq!(store.get(b"alpha").unwrap().as_deref(), Some(&b"one"[..]));
            assert_eq!(store.get(b"beta").unwrap().as_deref(), Some(&b"two"[..]));
        }
        let _ = store.scan(None, None, 10);
    }
    assert_eq!(fs::read(log).expect("read log"), LOG);
    assert_eq!(fs::read(witness).expect("read witness"), WITNESS);
});
