use super::*;
use crate::BatchOperation;

fn managed_fixture(
    max_tail_bytes: u64,
    max_tail_keys: usize,
    max_key_bytes: usize,
) -> (tempfile::TempDir, std::path::PathBuf, CheckpointStore) {
    let (dir, path, checkpoint, digest) = fixture();
    let store = retry_lock(|| {
        CheckpointStore::open_with_limits(
            &path,
            &checkpoint,
            &digest,
            max_tail_bytes,
            max_tail_keys,
            max_key_bytes,
        )
    })
    .unwrap();
    store
        .rollover_managed(dir.path().join("initial-managed"))
        .unwrap();
    (dir, path, store)
}

fn managed_default_fixture(
    max_tail_bytes: u64,
    max_tail_keys: usize,
    max_key_bytes: usize,
) -> (tempfile::TempDir, std::path::PathBuf, CheckpointStore) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint = dir.path().join("checkpoint");
    let initial = Store::open(&path).unwrap();
    initial.put(b"base", b"acknowledged").unwrap();
    let digest = initial.checkpoint_to(&checkpoint).unwrap();
    drop(initial);
    let store = retry_lock(|| {
        CheckpointStore::open_with_limits(
            &path,
            &checkpoint,
            &digest,
            max_tail_bytes,
            max_tail_keys,
            max_key_bytes,
        )
    })
    .unwrap();
    store
        .rollover_managed(dir.path().join("initial-managed"))
        .unwrap();
    (dir, path, store)
}

fn pause_automatic_rollover() -> io::Result<()> {
    println!("AUTOMATIC_ROLLOVER_BOUNDARY_REACHED");
    io::stdout().flush()?;
    loop {
        std::thread::park();
    }
}

mod basic;
mod crash;
mod deadline;
mod faults;
