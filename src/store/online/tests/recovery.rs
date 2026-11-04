use super::*;
use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn online_compaction_kill_child() {
    let Ok(selected) = std::env::var("EMDA_ONLINE_COMPACTION_CHILD") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("EMDA_ONLINE_COMPACTION_ROOT").unwrap());
    let (source, previous) = paths(&root);
    let store = OnlineStore::open(&source).unwrap();
    let selected = match selected.as_str() {
        "copy" => copy::Point::InitialCopy,
        "published" => copy::Point::BeforeExchange,
        "exchange" => copy::Point::AfterExchange,
        _ => panic!("unknown hook"),
    };
    let _ = store.compact_with_hooks(
        &previous,
        &Default::default(),
        |point| {
            if point == selected {
                let mut signal = File::create(root.join("signal")).unwrap();
                signal.write_all(b"ready").unwrap();
                signal.sync_all().unwrap();
                loop {
                    thread::park();
                }
            }
            Ok(())
        },
        crate::directory::sync_existing,
    );
    panic!("kill hook did not run");
}

#[test]
fn process_kill_at_copy_publication_and_exchange_recovers_complete_data() {
    for point in ["copy", "published", "exchange"] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = OnlineStore::open(&source).unwrap();
        insert_history(&store);
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("store::online::tests::recovery::online_compaction_kill_child")
            .env("EMDA_ONLINE_COMPACTION_CHILD", point)
            .env("EMDA_ONLINE_COMPACTION_ROOT", root.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + WAIT;
        while !root.path().join("signal").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before hook: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not reach hook");
            }
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let live = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        if point == "copy" {
            assert!(!previous.exists());
        } else {
            let old = retry_lock(|| Store::open(&previous)).unwrap();
            assert_eq!(old.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        }
        drop(live);
        let result = crate::cleanup_abandoned_staging(root.path()).unwrap();
        if point == "copy" {
            assert!(result.removed_payloads >= 1);
        }
    }
}

#[test]
fn legacy_v2_and_v3_fixtures_compact_without_changing_live_data() {
    for version in ["legacy", "v2", "v3"] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        fs::create_dir(&source).unwrap();
        match version {
            "legacy" => fs::write(
                source.join("records.log"),
                include_bytes!("../../../../tests/fixtures/db01-legacy-record.bin"),
            )
            .unwrap(),
            "v2" => {
                fs::write(
                    source.join("records.log"),
                    include_bytes!("../../../../tests/fixtures/v2-batch-records.log"),
                )
                .unwrap();
                fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../../../tests/fixtures/v2-batch-commit.state"),
                )
                .unwrap();
            }
            "v3" => {
                fs::write(
                    source.join("records.log"),
                    include_bytes!("../../../../tests/fixtures/v3-mixed-records.log"),
                )
                .unwrap();
                fs::write(
                    source.join("commit.state"),
                    include_bytes!("../../../../tests/fixtures/v3-mixed-commit.state"),
                )
                .unwrap();
                rustix::fs::setxattr(
                    source.join("records.log"),
                    "user.db.commit.v3",
                    include_bytes!("../../../../tests/fixtures/v3-mixed-metadata.state"),
                    rustix::fs::XattrFlags::empty(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let store = OnlineStore::open(&source).unwrap();
        store.with_store(|s| s.put(b"later", b"write")).unwrap();
        let expected = store.with_store(|s| s.scan(None, None, 100)).unwrap();
        store
            .compact_and_replace(&previous, &Default::default())
            .unwrap();
        assert_eq!(
            store.with_store(|s| s.scan(None, None, 100)).unwrap(),
            expected,
            "{version}"
        );
        assert_eq!(
            retry_lock(|| Store::open(&previous))
                .unwrap()
                .scan(None, None, 100)
                .unwrap(),
            expected,
            "{version}"
        );
        drop(store);
        assert_eq!(
            retry_lock(|| Store::open(&source))
                .unwrap()
                .scan(None, None, 100)
                .unwrap(),
            expected,
            "{version}"
        );
    }
}
