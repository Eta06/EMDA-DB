#![no_main]
use database_engine::{BatchOperation, CheckpointStore, RecoveredCheckpointView, Store};
use libfuzzer_sys::fuzz_target;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

#[cfg(target_os = "linux")]
fn metadata_marker(path: &Path, metadata: bool) -> Option<Vec<u8>> {
    if !metadata {
        return None;
    }
    let mut bytes = [0; 36];
    let count = rustix::fs::getxattr(
        path.join("records.log"),
        "user.db.commit.v3",
        &mut bytes[..],
    )
    .expect("read metadata witness");
    assert_eq!(count, bytes.len());
    Some(bytes.to_vec())
}

#[cfg(not(target_os = "linux"))]
fn metadata_marker(_: &Path, _: bool) -> Option<Vec<u8>> {
    None
}

#[cfg(target_os = "linux")]
fn restore_metadata(path: &Path, bytes: &[u8]) {
    rustix::fs::setxattr(
        path.join("records.log"),
        "user.db.commit.v3",
        bytes,
        rustix::fs::XattrFlags::REPLACE,
    )
    .expect("restore metadata witness");
}

#[cfg(not(target_os = "linux"))]
fn restore_metadata(_: &Path, _: &[u8]) {
    unreachable!("V3 metadata is Linux-only");
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 || data.len() > 256 {
        return;
    }
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    fs::create_dir(&path).expect("create directory");
    if data[0] & 128 == 0 {
        fs::write(
            path.join("records.log"),
            include_bytes!("../../tests/fixtures/v2-empty-records.log"),
        )
        .expect("log fixture");
        fs::write(
            path.join("commit.state"),
            include_bytes!("../../tests/fixtures/v2-empty-commit.state"),
        )
        .expect("witness fixture");
    }
    let ordinary = Store::open(&path).expect("open fixture");
    ordinary.put(b"base", b"persisted").expect("base write");
    let digest = ordinary.checkpoint_to(&checkpoint).expect("checkpoint");
    let log_path = path.join("records.log");
    let witness_path = path.join("commit.state");
    let prefix = fs::metadata(&log_path).expect("log metadata").len();
    let old_witness = fs::read(&witness_path).expect("old witness");
    let metadata = fs::read(&log_path)
        .expect("read header")
        .starts_with(b"EMDALOG3");
    let old_metadata = metadata_marker(&path, metadata);
    ordinary.put(b"tail", b"first").expect("suffix put");
    ordinary
        .write_batch(&[
            BatchOperation::Delete { key: b"base" },
            BatchOperation::Put {
                key: b"tail",
                value: b"updated",
            },
            BatchOperation::Put {
                key: b"new",
                value: b"value",
            },
        ])
        .expect("suffix batch");
    drop(ordinary);
    if data[0] & 1 != 0 {
        if let Some(bytes) = &old_metadata {
            restore_metadata(&path, bytes);
        } else {
            fs::write(&witness_path, &old_witness).expect("roll back witness");
        }
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)
        .expect("open log");
    let original_len = file.metadata().expect("metadata").len();
    if data[0] & 2 != 0 {
        let remaining = u64::from(data[1]) % (original_len - prefix + 1);
        file.set_len(prefix + remaining).expect("truncate suffix");
    }
    let length = file.metadata().expect("metadata").len();
    if data[0] & 4 != 0 && length > prefix {
        let offset = prefix + u64::from(data[2]) % (length - prefix);
        let mut byte = [0];
        file.read_exact_at(&mut byte, offset)
            .expect("read suffix byte");
        byte[0] ^= data[3] | 1;
        file.write_all_at(&byte, offset)
            .expect("change suffix byte");
    }
    drop(file);
    if data[0] & 8 != 0 {
        OpenOptions::new()
            .append(true)
            .open(&log_path)
            .expect("open append")
            .write_all(&data[4..])
            .expect("append arbitrary bytes");
    }
    let log_before = fs::read(&log_path).expect("read candidate log");
    let witness_before = fs::read(&witness_path).expect("read candidate witness");
    let metadata_before = metadata_marker(&path, metadata);
    let max_tail = if data[0] & 16 != 0 {
        u64::from(data[1])
    } else {
        64 * 1024 * 1024
    };
    let max_keys = if data[0] & 32 != 0 {
        usize::from(data[2] % 4)
    } else {
        16_384
    };
    let max_key_bytes = if data[0] & 64 != 0 {
        usize::from(data[3] % 16)
    } else {
        4 * 1024 * 1024
    };
    match CheckpointStore::open_with_limits(
        &path,
        &checkpoint,
        &digest,
        max_tail,
        max_keys,
        max_key_bytes,
    ) {
        Ok(store) => {
            let actual = store.scan(None, None, 20).expect("checkpoint scan");
            drop(store);
            let ordinary =
                Store::open(&path).expect("ordinary replay after successful bounded recovery");
            assert_eq!(
                ordinary.scan(None, None, 20).expect("ordinary scan"),
                actual
            );
            drop(ordinary);
            let view = RecoveredCheckpointView::open(&path, &checkpoint, &digest)
                .expect("witnessed recovered view");
            assert_eq!(view.scan(None, None, 20).expect("view scan"), actual);
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidData
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::InvalidInput
            ) =>
        {
            assert_eq!(fs::read(&log_path).expect("read rejected log"), log_before);
            assert_eq!(
                fs::read(&witness_path).expect("read rejected witness"),
                witness_before
            );
            assert_eq!(metadata_marker(&path, metadata), metadata_before);
        }
        Err(_) => {}
    }
});
