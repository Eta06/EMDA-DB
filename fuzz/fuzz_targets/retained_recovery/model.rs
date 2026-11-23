use crate::fixture::{self, BASE, LIMITS, LOG, WITNESS};
use database_engine::retained_candidate::RetainedStore;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, io::ErrorKind};

const HEADER: usize = 224;
const FIRST_PUT: usize = 25;
const FIXTURE_TAIL: usize = LOG.len() - HEADER;

fn witness(end: usize, log: &[u8]) -> Vec<u8> {
    let mut marker = b"EMDAWIT4".to_vec();
    marker.extend_from_slice(&Sha256::digest(&LOG[..HEADER]));
    marker.extend_from_slice(&(end as u64).to_le_bytes());
    marker.extend_from_slice(&Sha256::digest(&log[HEADER..HEADER + end]));
    marker.extend_from_slice(&crc32fast::hash(&marker).to_le_bytes());
    marker
}

fn append_record(log: &mut Vec<u8>, operation: u8, key: &[u8], value: &[u8]) {
    // Encode the generated operation independently of the writer.
    let start = log.len();
    log.extend_from_slice(b"DB01");
    log.push(operation);
    log.extend_from_slice(&(key.len() as u32).to_le_bytes());
    log.extend_from_slice(&(value.len() as u32).to_le_bytes());
    log.extend_from_slice(&crc32fast::hash(&log[start..]).to_le_bytes());
    log.extend_from_slice(key);
    log.extend_from_slice(value);
    log.extend_from_slice(&crc32fast::hash(&log[start..]).to_le_bytes());
}

fn flip(bytes: &mut [u8], position: usize, mask: u8) {
    if !bytes.is_empty() {
        let index = position % bytes.len();
        bytes[index] ^= mask | 1;
    }
}

fn expected_rows(end: usize, operation: u8, value: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = BTreeMap::from([
        (b"a".to_vec(), b"old".to_vec()),
        (b"retired".to_vec(), b"old".to_vec()),
    ]);
    if end >= FIRST_PUT {
        rows.insert(b"a".to_vec(), b"new".to_vec());
    }
    if end >= FIXTURE_TAIL {
        rows.remove(b"retired".as_slice());
        rows.insert(vec![0, 255], vec![0, 2, 255]);
        rows.insert(b"empty".to_vec(), vec![]);
    }
    if end > FIXTURE_TAIL {
        match operation {
            1 => {
                rows.insert(b"generated".to_vec(), value.to_vec());
            }
            2 => {
                rows.remove(b"a".as_slice());
            }
            _ => unreachable!("only generated records extend the fixture"),
        }
    }
    rows.into_iter().collect()
}

pub fn check(data: &[u8]) {
    if !(7..=128).contains(&data.len()) {
        return;
    }
    let operation = data[0] % 3;
    let value = &data[7..];
    let mut log = LOG.to_vec();
    assert_eq!(witness(FIXTURE_TAIL, &log), WITNESS);
    match operation {
        1 => append_record(&mut log, operation, b"generated", value),
        2 => append_record(&mut log, operation, b"a", &[]),
        _ => {}
    }
    let suffix_len = log.len() - HEADER;
    let boundaries = [0, FIRST_PUT, FIXTURE_TAIL, suffix_len];
    let length = match boundaries.get(usize::from(data[1])) {
        Some(end) => HEADER + end,
        None => usize::from(u16::from_le_bytes([data[2], data[1]])) % (log.len() + 1),
    };
    let committed = boundaries
        .get(usize::from(data[3]))
        .copied()
        .unwrap_or_else(|| usize::from(data[4]) % (suffix_len + 1));
    let mut marker = witness(committed, &log);
    log.truncate(length);
    let mut base = BASE.to_vec();
    let mut binding = fixture::binding();
    let damage = data[5] % 7;
    let position = usize::from(u16::from_le_bytes([data[2], data[4]]));
    match damage {
        1 => flip(&mut log, position, data[6]),
        2 => flip(&mut base, position, data[6]),
        3 => flip(&mut marker, position, data[6]),
        4 => flip(&mut binding.header, position, data[6]),
        5 => marker.truncate(position % marker.len()),
        6 => base.truncate(position % base.len()),
        _ => {}
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let log_path = root.join("records.log");
    let marker_path = root.join("commit.state");
    let base_path = root.join("base");
    fs::write(&log_path, &log).unwrap();
    fs::write(&marker_path, &marker).unwrap();
    fs::write(&base_path, &base).unwrap();
    fs::write(root.join("store.lock"), []).unwrap();

    let end = length.checked_sub(HEADER);
    let admissible = damage == 0
        && boundaries.contains(&committed)
        && end.is_some_and(|end| boundaries.contains(&end) && committed <= end);
    let result = RetainedStore::open(root, LIMITS, &binding);
    if admissible {
        let end = end.unwrap();
        let store = result.unwrap();
        assert_eq!(
            store.scan(None, None, 8).unwrap(),
            expected_rows(end, operation, value)
        );
        assert_eq!(fs::read(&marker_path).unwrap(), witness(end, &log));
    } else {
        let error = result.err().expect("invalid retained image opened");
        assert!(
            matches!(
                error.kind(),
                ErrorKind::InvalidData | ErrorKind::UnexpectedEof | ErrorKind::InvalidInput
            ),
            "unexpected recovery error: {error:?}"
        );
        assert_eq!(fs::read(&marker_path).unwrap(), marker);
    }
    assert_eq!(fs::read(&log_path).unwrap(), log);
    assert_eq!(fs::read(&base_path).unwrap(), base);
}
