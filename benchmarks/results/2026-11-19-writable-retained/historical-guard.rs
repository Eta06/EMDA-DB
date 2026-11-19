use database_engine::{CheckpointStore, CheckpointView, RecoveredCheckpointView, Store};
use sha2::{Digest, Sha256};
use std::{fs, io, path::PathBuf};

#[test]
fn historical_reader_rejects_actual_writable_retained_generation() {
    let live = PathBuf::from(std::env::var_os("EMDA_WRITABLE_RETAINED_FIXTURE").expect("fixture"));
    let saved = ["base", "records.log", "commit.state", "store.lock"]
        .map(|name| fs::read(live.join(name)).unwrap());
    assert!(saved[1].starts_with(b"EMDALOG4"));
    assert!(saved[1].len() > 224);
    assert!(saved[2].starts_with(b"EMDAWIT4"));
    let error = Store::open(&live)
        .err()
        .expect("legacy Store admitted generation");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    let scratch = tempfile::tempdir().unwrap();
    let ordinary = Store::open(scratch.path().join("ordinary")).unwrap();
    ordinary.put(b"k", b"value").unwrap();
    let checkpoint = scratch.path().join("checkpoint");
    let digest = ordinary.checkpoint_to(&checkpoint).unwrap();
    assert!(CheckpointStore::open(&live, &checkpoint, &digest).is_err());
    assert!(CheckpointView::open(&live, &checkpoint, &digest).is_err());
    assert!(RecoveredCheckpointView::open(&live, &checkpoint, &digest).is_err());

    let backup = scratch.path().join("backup");
    ordinary.backup(&backup).unwrap();
    let old = fs::read(&backup).unwrap();
    let mut forged = old[..8].to_vec();
    forged.extend(&saved[1]);
    forged.extend(&old[old.len() - 48..old.len() - 40]);
    forged.extend((saved[1].len() as u64).to_le_bytes());
    forged.extend(Sha256::digest(&saved[1]));
    fs::write(&backup, &forged).unwrap();
    let destination = scratch.path().join("restore");
    assert!(Store::restore(&backup, &destination).is_err());
    assert!(!destination.exists());
    assert_eq!(fs::read(&backup).unwrap(), forged);
    assert_eq!(
        ["base", "records.log", "commit.state", "store.lock"]
            .map(|name| fs::read(live.join(name)).unwrap()),
        saved
    );
}
