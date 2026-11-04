use super::*;
use crate::test_support::retry_lock;
use std::time::Duration;

mod basic;
mod concurrency;
mod failure;
mod recovery;

const WAIT: Duration = Duration::from_secs(10);

fn paths(root: &Path) -> (PathBuf, PathBuf) {
    (root.join("live"), root.join("previous"))
}

fn insert_history(store: &OnlineStore) {
    store
        .with_store(|db| {
            for n in 0..40 {
                db.put(b"overwritten", n.to_string().as_bytes())?;
            }
            db.put(b"delete", b"old")?;
            Ok(())
        })
        .unwrap();
}
mod reclaim_tests;
