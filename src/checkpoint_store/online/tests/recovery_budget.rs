//! Bounded recovery of the selected generation after online checkpoint
//! handoffs. The combined budget counts logical checkpoint and log bytes
//! scanned during open, not physical I/O, elapsed time or memory. These run
//! on a live OS with process kills; they are not physical power-loss tests.
use super::*;
use crate::{
    CheckpointRecoveryLimits, CheckpointStoreLimits, DiskBudgetPending, RecoveryOpenFailure,
};
use sha2::{Digest, Sha256};

// Every file under the parent directory with its content digest, so a
// refused open can be shown to leave all state, including leftovers, intact.
fn tree(root: &Path) -> BTreeMap<PathBuf, [u8; 32]> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            let metadata = fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                pending.push(path);
            } else {
                let digest = Sha256::digest(fs::read(&path).unwrap()).into();
                files.insert(path.strip_prefix(root).unwrap().to_path_buf(), digest);
            }
        }
    }
    files
}

// The logical scan a clean open performs: the selected checkpoint file and
// the complete witnessed log. Read without opening, so nothing is mutated.
fn exact_budget(source: &Path) -> u64 {
    let selected = selection::load(source).unwrap().expect("managed selection");
    fs::metadata(&selected.checkpoint).unwrap().len()
        + fs::metadata(source.join("records.log")).unwrap().len()
}

fn open_bounded(source: &Path, total: u64) -> Result<CheckpointStore, RecoveryOpenFailure> {
    CheckpointStore::open_managed_with_total_recovery_scan_limit(
        source,
        CheckpointStoreLimits::default(),
        CheckpointRecoveryLimits::default(),
        total,
    )
}

// One byte short is refused before any durable step and changes no file;
// the exact budget recovers every acknowledged value.
fn check_bounded_reopen(fixture: &Fixture, model: &Model, label: &str) -> OnlineCheckpointStore {
    let exact = exact_budget(&fixture.source);
    let before = tree(fixture.root.path());
    let refused = retry_lock(|| match open_bounded(&fixture.source, exact - 1) {
        Err(RecoveryOpenFailure::NotAdopted(error))
            if error.kind() == io::ErrorKind::WouldBlock =>
        {
            Err(error)
        }
        other => Ok(other),
    })
    .unwrap();
    let Err(RecoveryOpenFailure::NotAdopted(error)) = refused else {
        panic!("{label}: one byte short was not refused before adoption");
    };
    assert_eq!(
        error.kind(),
        io::ErrorKind::InvalidInput,
        "{label}: {error}"
    );
    assert_eq!(
        error.to_string(),
        "checkpoint total recovery scan byte limit exceeded",
        "{label}"
    );
    assert_eq!(
        tree(fixture.root.path()),
        before,
        "{label}: refused open changed state"
    );
    let store = retry_lock(|| {
        open_bounded(&fixture.source, exact).map_err(|failure| match failure {
            RecoveryOpenFailure::NotAdopted(error) => error,
            other => panic!("{label}: {other:?}"),
        })
    })
    .unwrap();
    let online = OnlineCheckpointStore::from_store(store).unwrap();
    assert_eq!(live(&online), *model, "{label}");
    online
}

#[test]
fn bounded_reopen_after_a_clean_handoff_with_concurrent_writes() {
    for budgeted in [false, true] {
        let (fixture, online, mut model) = fixture(false);
        if budgeted {
            online.set_disk_budget(Some(u64::MAX)).unwrap();
        }
        let before = model.clone();
        let snapshot = online.snapshot().unwrap();
        let mut written = Model::new();
        online
            .compact_reclaim_detailed_with_hooks(
                &fixture.previous,
                &Default::default(),
                (
                    |point| {
                        if matches!(point, Point::InitialCopy | Point::BeforeFinalBarrier) {
                            let key = format!("during-{point:?}").into_bytes();
                            online.with_store(|store| store.put(&key, b"acknowledged"))?;
                            written.insert(key, b"acknowledged".to_vec());
                        }
                        Ok(())
                    },
                    crate::directory::sync_existing,
                ),
                |_| Ok(()),
            )
            .unwrap();
        model.extend(written);
        assert_eq!(captured(&snapshot), before);
        drop(snapshot);
        let charged = online.disk_budget_status().unwrap();
        drop(online);
        // The selected generation is now the compacted replacement.
        let selected = selection::load(&fixture.source).unwrap().unwrap();
        assert_eq!(
            selected.checkpoint,
            fs::canonicalize(&fixture.source)
                .unwrap()
                .join(crate::COMPACTED_CHECKPOINT_FILE)
        );
        let online = check_bounded_reopen(&fixture, &model, &format!("clean budgeted={budgeted}"));
        // Budget accounting is unchanged by the bounded reopen.
        assert_eq!(online.disk_budget_status().unwrap(), charged);
        write(&online, &mut model, b"after", b"reopen");
        drop(online);
        assert_eq!(reopened(&fixture.source), model);
    }
}

#[test]
fn bounded_reopen_after_interrupted_publication_and_retirement() {
    for (point, leftover) in [
        ("InitialCopy", true),
        ("BeforeExchange", true),
        ("AfterExchange", true),
        ("AfterMove", false),
        ("AfterRecords", false),
    ] {
        let (fixture, online, model) = fixture(false);
        online.set_disk_budget(Some(u64::MAX)).unwrap();
        drop(online);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("checkpoint_store::online::tests::online_checkpoint_kill_child")
            .env(CHILD_POINT, point)
            .env(CHILD_ROOT, fixture.root.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        while !fixture.root.path().join("signal").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before {point}: {status}");
            }
            assert!(Instant::now() < deadline, "child did not reach {point}");
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        fs::remove_file(fixture.root.path().join("signal")).unwrap();
        assert_eq!(fixture.previous.exists(), leftover, "{point}");
        let online = check_bounded_reopen(&fixture, &model, point);
        // The bounded reopen keeps the interrupted maintenance charged.
        let status = online.disk_budget_status().unwrap().unwrap();
        let expected = if matches!(point, "InitialCopy" | "BeforeExchange") {
            DiskBudgetPending::Compaction
        } else {
            DiskBudgetPending::Retirement
        };
        assert_eq!(status.pending, expected, "{point}");
        assert!(status.charged > status.live_bytes, "{point}: {status:?}");
        // Reconcile the leftovers, then compact again. A staged retirement
        // is finished by scoped cleanup under the same exact scan ceiling.
        if fixture.previous.exists() {
            fs::remove_dir_all(&fixture.previous).unwrap();
        } else {
            let exact = exact_budget(&fixture.source);
            let charged = online.disk_budget_status().unwrap();
            // Substitute a larger file at the selected checkpoint's path, as a
            // new inode. Admission measures the open view's pinned descriptor,
            // so the exact ceiling is unchanged.
            let selected = selection::load(&fixture.source)
                .unwrap()
                .unwrap()
                .checkpoint;
            let original = fs::read(&selected).unwrap();
            let substitute = fixture.root.path().join("substitute");
            fs::write(&substitute, vec![0_u8; original.len() * 4]).unwrap();
            fs::rename(&substitute, &selected).unwrap();
            let before = tree(fixture.root.path());
            let refused = online.cleanup_retired_staging_with_scan_limit(exact - 1);
            assert!(
                matches!(
                    &refused,
                    Err(crate::StagingCleanupFailure::NotStarted(error))
                        if error.kind() == io::ErrorKind::InvalidInput
                            && error.to_string()
                                == "checkpoint total recovery scan byte limit exceeded"
                ),
                "{point}: {refused:?}"
            );
            assert_eq!(
                tree(fixture.root.path()),
                before,
                "{point}: refused cleanup changed state"
            );
            assert_eq!(online.disk_budget_status().unwrap(), charged, "{point}");
            let cleaned = online
                .cleanup_retired_staging_with_scan_limit(exact)
                .unwrap();
            // Restore the original file, as a new inode, for the next reopen.
            fs::write(&substitute, &original).unwrap();
            fs::rename(&substitute, &selected).unwrap();
            assert_eq!(cleaned.removed_payloads, 1, "{point}");
        }
        online.cleanup_retired_staging_detailed().unwrap();
        crate::cleanup_abandoned_staging(fixture.root.path()).unwrap();
        let released = online.reconcile_disk_budget().unwrap().unwrap();
        assert_eq!(released.pending, DiskBudgetPending::None, "{point}");
        online
            .compact_and_reclaim(&fixture.previous, &Default::default())
            .unwrap();
        drop(online);
        // The next generation also reopens at its exact budget.
        let online = check_bounded_reopen(&fixture, &model, &format!("{point} after retry"));
        drop(online);
    }
}

#[test]
fn bounded_cleanup_without_matching_stages_reads_nothing() {
    let (fixture, online, model) = fixture(false);
    // No stage to remove, so no verification and no scan, even at zero.
    let cleaned = online.cleanup_retired_staging_with_scan_limit(0).unwrap();
    assert_eq!((cleaned.removed_payloads, cleaned.removed_markers), (0, 0));
    assert_eq!(live(&online), model);
    drop(online);
    assert_eq!(reopened(&fixture.source), model);
}

#[test]
fn bounded_cleanup_admits_one_pass_over_writes_appended_after_reopen() {
    let (fixture, online, mut model) = fixture(false);
    drop(online);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("checkpoint_store::online::tests::online_checkpoint_kill_child")
        .env(CHILD_POINT, "AfterMove")
        .env(CHILD_ROOT, fixture.root.path())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !fixture.root.path().join("signal").exists() {
        assert!(child.try_wait().unwrap().is_none(), "child exited early");
        assert!(Instant::now() < deadline, "child did not reach AfterMove");
        thread::sleep(Duration::from_millis(5));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let online = check_bounded_reopen(&fixture, &model, "AfterMove before appends");
    // Acknowledged appends after open extend the witnessed log beyond the
    // boundary captured when the replacement was opened.
    for index in 0..8 {
        write(
            &online,
            &mut model,
            format!("appended-{index}").as_bytes(),
            b"value",
        );
    }
    let exact = exact_budget(&fixture.source);
    let before = tree(fixture.root.path());
    let refused = online.cleanup_retired_staging_with_scan_limit(exact - 1);
    assert!(
        matches!(&refused, Err(crate::StagingCleanupFailure::NotStarted(error))
            if error.kind() == io::ErrorKind::InvalidInput),
        "{refused:?}"
    );
    assert_eq!(tree(fixture.root.path()), before);
    let cleaned = online
        .cleanup_retired_staging_with_scan_limit(exact)
        .unwrap();
    assert_eq!(cleaned.removed_payloads, 1);
    assert_eq!(live(&online), model);
    drop(online);
    let online = check_bounded_reopen(&fixture, &model, "after appended cleanup");
    drop(online);
}
