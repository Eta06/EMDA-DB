use super::*;

#[test]
fn online_reclaim_kill_child() {
    use std::io::Write;
    let Ok(selected) = std::env::var("EMDA_ONLINE_RECLAIM_CHILD") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("EMDA_ONLINE_RECLAIM_ROOT").unwrap());
    let (source, previous) = paths(&root);
    let selected = match selected.as_str() {
        "before-move" => reclaim::Point::BeforeMove,
        "after-move" => reclaim::Point::AfterMove,
        "after-records" => reclaim::Point::AfterRecords,
        "after-lock" => reclaim::Point::AfterLock,
        "after-retired-sync" => reclaim::Point::AfterRetiredSync,
        "after-retired-removal" => reclaim::Point::AfterRetiredRemoval,
        "after-stage-sync" => reclaim::Point::AfterStageSync,
        "after-stage-removal" => reclaim::Point::AfterStageRemoval,
        "after-parent-sync" => reclaim::Point::AfterParentSync,
        _ => panic!("unknown hook"),
    };
    let store = retry_lock(|| OnlineStore::open(&source)).unwrap();
    let _ = store.compact_reclaim_with_hooks(
        &previous,
        &Default::default(),
        |_| Ok(()),
        crate::directory::sync_existing,
        |point| {
            if point == selected {
                let mut signal = fs::File::create(root.join("signal")).unwrap();
                signal.write_all(b"ready").unwrap();
                signal.sync_all().unwrap();
                loop {
                    thread::park();
                }
            }
            Ok(())
        },
    );
    panic!("kill hook did not run");
}

#[test]
fn process_kill_at_each_reclamation_boundary_preserves_acknowledged_data() {
    use std::process::{Command, Stdio};
    use std::time::Instant;
    for point in [
        "before-move",
        "after-move",
        "after-records",
        "after-lock",
        "after-retired-sync",
        "after-retired-removal",
        "after-stage-sync",
        "after-stage-removal",
        "after-parent-sync",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (source, previous) = paths(root.path());
        let store = OnlineStore::open(&source).unwrap();
        insert_history(&store);
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("store::online::tests::reclaim_tests::recovery::online_reclaim_kill_child")
            .env("EMDA_ONLINE_RECLAIM_CHILD", point)
            .env("EMDA_ONLINE_RECLAIM_ROOT", root.path())
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
                panic!("child did not reach hook {point}");
            }
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let live = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        assert_eq!(live.get(b"delete").unwrap(), Some(b"old".to_vec()));
        if point == "before-move" {
            let old = retry_lock(|| Store::open(&previous)).unwrap();
            assert_eq!(old.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        } else {
            assert!(!previous.exists());
        }
        crate::cleanup_abandoned_staging(root.path()).unwrap();
        assert_eq!(live.get(b"overwritten").unwrap(), Some(b"39".to_vec()));
        drop(live);
        assert_eq!(
            retry_lock(|| Store::open(&source))
                .unwrap()
                .get(b"overwritten")
                .unwrap(),
            Some(b"39".to_vec())
        );
    }
}
