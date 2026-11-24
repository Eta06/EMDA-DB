use super::*;
use crate::BatchOperation;

#[test]
fn invalid_automatic_configuration_rejects_before_tail_adoption() {
    let (_dir, path, store) = managed_fixture(
        MAX_RECOVERED_VIEW_TAIL_BYTES,
        MAX_RECOVERED_VIEW_TAIL_KEYS,
        MAX_RECOVERED_VIEW_TAIL_KEY_BYTES,
    );
    store.enable_automatic_checkpoints().unwrap();
    let witness = path.join("commit.state");
    let original_witness = fs::read(&witness).unwrap();
    store.put(b"pending", b"complete").unwrap();
    drop(store);
    fs::write(&witness, &original_witness).unwrap();
    let original_log = fs::read(path.join("records.log")).unwrap();
    let selection = super::super::selection::path(&path);
    let original_selection = fs::read(&selection).unwrap();
    let marker = super::super::automatic::root(&path).join("owner");
    let original_marker = fs::read(&marker).unwrap();
    fs::write(&marker, b"invalid automatic configuration").unwrap();

    let error = retry_lock(|| CheckpointStore::open_managed(&path))
        .err()
        .expect("invalid configuration must fail");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "automatic checkpoint configuration is invalid"
    );
    assert_eq!(fs::read(&witness).unwrap(), original_witness);
    assert_eq!(fs::read(path.join("records.log")).unwrap(), original_log);
    assert_eq!(fs::read(selection).unwrap(), original_selection);
    assert_eq!(
        fs::read(&marker).unwrap(),
        b"invalid automatic configuration"
    );

    fs::write(marker, original_marker).unwrap();
    let recovered = retry_lock(|| CheckpointStore::open_managed(&path)).unwrap();
    assert_eq!(
        recovered.get(b"pending").unwrap().as_deref(),
        Some(&b"complete"[..])
    );
    assert_ne!(fs::read(witness).unwrap(), original_witness);
}

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
mod concurrency;
mod crash;
mod deadline;
mod faults;
