#![no_main]

use database_engine::{BatchOperation, Store};
use libfuzzer_sys::fuzz_target;
use std::{fs, io, path::Path};

const LARGE_VALUE_MIN: usize = 65_537;
const LARGE_VALUE_EXTRA: usize = 63;
const MAX_CONTROL_BYTES: usize = 16;
const KEY: &[u8] = b"large-record";
const PEER: &[u8] = b"batch-peer";

// Controls are deliberately tiny. Byte 0 selects legacy, V2, or the platform's
// default witnessed storage, byte 1 selects a value length just above 64 KiB, byte 2
// selects no-op / one-byte corruption / short tail truncation, and bytes 3..6
// select the bounded mutation offset. Byte 8 selects an atomic batch for
// witnessed stores. Byte 9 optionally repairs the outer CRC after corrupting a
// nested batch byte, exercising nested validation. Other bytes do not enlarge
// the generated store.
fuzz_target!(|control: &[u8]| {
    if control.is_empty() || control.len() > MAX_CONTROL_BYTES {
        return;
    }

    let format = control[0] % 3;
    let batch = format != 0 && control.get(8).copied().unwrap_or(0) & 1 != 0;
    let value_len =
        LARGE_VALUE_MIN + control.get(1).copied().unwrap_or(0) as usize % (LARGE_VALUE_EXTRA + 1);
    let value: Vec<u8> = (0..value_len)
        .map(|index| ((index.wrapping_mul(31) + 17) % 251) as u8)
        .collect();
    let directory = tempfile::tempdir().expect("fuzz temporary directory");
    let log_path = directory.path().join("records.log");
    let witness_path = directory.path().join("commit.state");

    if format == 0 {
        fs::write(&log_path, encode_legacy_record(KEY, &value)).expect("write legacy record");
    } else {
        if format == 1 {
            fs::write(
                &log_path,
                include_bytes!("../../tests/fixtures/v2-empty-records.log"),
            )
            .expect("write V2 log");
            fs::write(
                &witness_path,
                include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
            )
            .expect("write V2 witness");
        }
        let store = Store::open(directory.path()).expect("create witnessed store");
        if batch {
            store
                .write_batch(&[
                    BatchOperation::Put {
                        key: KEY,
                        value: &value,
                    },
                    BatchOperation::Put {
                        key: PEER,
                        value: b"paired",
                    },
                ])
                .expect("write witnessed large batch");
        } else {
            store.put(KEY, &value).expect("write witnessed large value");
        }
        drop(store);
    }

    let original_log = fs::read(&log_path).expect("read generated log");
    let mutation = control.get(2).copied().unwrap_or(0) % 3;
    let mut candidate = original_log.clone();
    match mutation {
        0 => {}
        1 if !candidate.is_empty() => {
            let selector = control
                .get(3..7)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes)
                .unwrap_or(0) as usize;
            let repair_outer = batch && control.get(9).copied().unwrap_or(0) & 1 != 0;
            let offset = if repair_outer {
                // 28-byte witnessed header + 17-byte DB02 header + outer key
                // + four-byte count. Exclude the outer trailer.
                let first_nested = 28 + 17 + 1 + 4;
                first_nested + selector % (candidate.len() - 4 - first_nested)
            } else {
                selector % candidate.len()
            };
            candidate[offset] ^= 1 << (control.get(7).copied().unwrap_or(0) % 8);
            if repair_outer {
                let trailer = candidate.len() - 4;
                let checksum = crc32fast::hash(&candidate[28..trailer]);
                candidate[trailer..].copy_from_slice(&checksum.to_le_bytes());
            }
        }
        _ if candidate.len() > 1 => {
            let max_truncation = candidate.len().min(32);
            let amount = 1 + control.get(3).copied().unwrap_or(0) as usize % max_truncation;
            candidate.truncate(candidate.len() - amount);
        }
        _ => {}
    }
    fs::write(&log_path, &candidate).expect("apply bounded recovery mutation");
    let metadata_required = original_log.starts_with(b"EMDALOG3");
    let evidence = Evidence::capture(&log_path, &witness_path, metadata_required);

    match Store::open(directory.path()) {
        Ok(store) => {
            assert_eq!(mutation, 0, "corrupted or truncated record opened");
            // A complete unacknowledged tail may be adopted by the storage
            // contract. This generator has one acknowledged large record, so
            // any successful recovery must still expose that exact value.
            assert_eq!(
                store.get(KEY).expect("read recovered large value"),
                Some(value.clone()),
                "successful recovery lost or changed the committed large value"
            );
            assert_eq!(
                store.get(PEER).expect("read recovered peer").as_deref(),
                batch.then_some(&b"paired"[..])
            );
            drop(store);
            let reopened = Store::open(directory.path()).expect("reopen recovered large record");
            assert_eq!(
                reopened.get(KEY).expect("read reopened large value"),
                Some(value),
                "reopening changed the recovered large value"
            );
            assert_eq!(
                reopened.get(PEER).expect("read reopened peer").as_deref(),
                batch.then_some(&b"paired"[..])
            );
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ) =>
        {
            evidence.assert_unchanged(&log_path, &witness_path);
        }
        Err(error) => panic!("unclassified large-record recovery error: {error:?}"),
    }
});

struct Evidence {
    log: Vec<u8>,
    witness: Option<Vec<u8>>,
    metadata: Option<Vec<u8>>,
    metadata_required: bool,
}

impl Evidence {
    fn capture(log_path: &Path, witness_path: &Path, metadata_required: bool) -> Self {
        let mut metadata = [0u8; 255];
        let result = rustix::fs::getxattr(log_path, "user.db.commit.v3", &mut metadata);
        if metadata_required {
            assert!(result.is_ok(), "missing V3 witness metadata: {result:?}");
        }
        let metadata = result.ok().map(|length| metadata[..length].to_vec());
        Self {
            log: fs::read(log_path).expect("snapshot generated log"),
            witness: fs::read(witness_path).ok(),
            metadata,
            metadata_required,
        }
    }

    fn assert_unchanged(&self, log_path: &Path, witness_path: &Path) {
        assert_eq!(fs::read(log_path).expect("read rejected log"), self.log);
        assert_eq!(fs::read(witness_path).ok(), self.witness);
        let mut metadata = [0u8; 255];
        let result = rustix::fs::getxattr(log_path, "user.db.commit.v3", &mut metadata);
        if self.metadata_required {
            assert!(
                result.is_ok(),
                "V3 witness metadata became unreadable: {result:?}"
            );
        }
        let metadata = result.ok().map(|length| metadata[..length].to_vec());
        assert_eq!(
            metadata, self.metadata,
            "rejected recovery changed metadata"
        );
    }
}

fn encode_legacy_record(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut record = Vec::with_capacity(17 + key.len() + value.len() + 4);
    record.extend_from_slice(b"DB01");
    record.push(1); // Put
    record.extend_from_slice(&(key.len() as u32).to_le_bytes());
    record.extend_from_slice(&(value.len() as u32).to_le_bytes());
    let header_crc = crc32fast::hash(&record);
    record.extend_from_slice(&header_crc.to_le_bytes());
    record.extend_from_slice(key);
    record.extend_from_slice(value);
    let record_crc = crc32fast::hash(&record);
    record.extend_from_slice(&record_crc.to_le_bytes());
    record
}
