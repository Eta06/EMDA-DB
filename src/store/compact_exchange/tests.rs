use super::*;
use crate::log::Log;
use crate::test_support::retry_lock;
use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(target_os = "macos")]
#[test]
fn macos_exchange_refuses_non_apfs_before_staging() {
    let error = paths(Path::new("/dev/null"), Path::new("/dev/emda-unused")).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert_eq!(
        error.to_string(),
        "atomic directory exchange on macOS requires APFS"
    );
}

fn populated(root: &Path) -> (Store, PathBuf, PathBuf) {
    let source = root.join("live");
    let previous = root.join("previous");
    let store = retry_lock(|| Store::open(&source)).unwrap();
    store.put(b"key", b"obsolete").unwrap();
    store.put(b"key", b"latest").unwrap();
    store.put(b"removed", b"old").unwrap();
    store.delete(b"removed").unwrap();
    (store, source, previous)
}

#[test]
fn exchange_preserves_live_data_old_store_and_open_limits() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("live");
    let previous = root.path().join("previous");
    let store =
        Store::open_with_resource_limits(&source, Some(1_000_000), Some(2), Some(2)).unwrap();
    for number in 0..20 {
        store.put(b"a", number.to_string().as_bytes()).unwrap();
    }
    store.put(b"b", b"two").unwrap();
    let old_length = fs::metadata(source.join("records.log")).unwrap().len();
    let store = store
        .compact_and_replace_detailed(&source, &previous)
        .unwrap();
    assert!(fs::metadata(source.join("records.log")).unwrap().len() < old_length);
    assert_eq!(store.get(b"a").unwrap(), Some(b"19".to_vec()));
    assert!(store.put(b"c", b"three").is_err());
    assert!(Store::open(&source).is_err());
    let old = retry_lock(|| Store::open(&previous)).unwrap();
    assert_eq!(old.get(b"a").unwrap(), Some(b"19".to_vec()));
    store.put(b"a", b"new").unwrap();
    assert_eq!(old.get(b"a").unwrap(), Some(b"19".to_vec()));
    drop(old);
    drop(store);
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"a")
            .unwrap(),
        Some(b"new".to_vec())
    );
}

#[test]
fn unknown_files_and_non_sibling_or_existing_destinations_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let (store, source, previous) = populated(root.path());
    fs::write(source.join("user-data"), b"keep").unwrap();
    assert_eq!(
        store
            .compact_and_replace(&source, &previous)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(fs::read(source.join("user-data")).unwrap(), b"keep");
    assert!(!previous.exists());
    fs::remove_file(source.join("user-data")).unwrap();
    let store = retry_lock(|| Store::open(&source)).unwrap();
    fs::create_dir(root.path().join("other")).unwrap();
    assert_eq!(
        store
            .compact_and_replace(&source, root.path().join("other").join("dest"))
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let store = retry_lock(|| Store::open(&source)).unwrap();
    fs::create_dir(&previous).unwrap();
    assert_eq!(
        store
            .compact_and_replace(&source, &previous)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"latest".to_vec())
    );
}

#[test]
fn injected_exchange_failures_preserve_both_complete_copies() {
    for point in [
        ExchangePoint::CopyPublished,
        ExchangePoint::BeforeExchange,
        ExchangePoint::AfterExchange,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (store, source, previous) = populated(root.path());
        let result = store.compact_and_replace_with_hooks(
            &source,
            &previous,
            |current| {
                if current == point {
                    Err(io::Error::other("injected"))
                } else {
                    Ok(())
                }
            },
            crate::directory::sync_existing,
        );
        assert!(result.is_err());
        let live = retry_lock(|| Store::open(&source)).unwrap();
        let old = retry_lock(|| Store::open(&previous)).unwrap();
        for store in [&live, &old] {
            assert_eq!(store.get(b"key").unwrap(), Some(b"latest".to_vec()));
            assert_eq!(store.get(b"removed").unwrap(), None);
        }
    }
    let root = tempfile::tempdir().unwrap();
    let (store, source, previous) = populated(root.path());
    let error = store
        .compact_and_replace_with_hooks(
            &source,
            &previous,
            |_| Ok(()),
            |_| Err(io::Error::other("injected sync failure")),
        )
        .err()
        .unwrap();
    assert!(error.to_string().contains("exchange may have occurred"));
    assert_eq!(
        retry_lock(|| Store::open(&source))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"latest".to_vec())
    );
    assert_eq!(
        retry_lock(|| Store::open(&previous))
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(b"latest".to_vec())
    );
}

#[test]
fn detailed_handoff_classifies_copy_and_exchange_boundaries() {
    use crate::HandoffFailure;
    for failure in [
        "preflight",
        "copy",
        "before-exchange",
        "after-exchange",
        "sync",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (store, source, previous) = populated(root.path());
        let original = fs::read(source.join("records.log")).unwrap();
        let destination = if failure == "preflight" {
            root.path().join("missing/previous")
        } else {
            previous.clone()
        };
        let result = store.compact_and_replace_detailed_with_hooks(
            &source,
            &destination,
            |point| {
                if (failure == "copy" && point == ExchangePoint::CopyPublished)
                    || (failure == "before-exchange" && point == ExchangePoint::BeforeExchange)
                    || (failure == "after-exchange" && point == ExchangePoint::AfterExchange)
                {
                    Err(io::Error::other("injected handoff failure"))
                } else {
                    Ok(())
                }
            },
            |parent| {
                if failure == "sync" {
                    Err(io::Error::other("injected parent sync failure"))
                } else {
                    crate::directory::sync_existing(parent)
                }
            },
        );
        match failure {
            "preflight" => assert!(matches!(result, Err(HandoffFailure::NotPublished(_)))),
            "copy" | "before-exchange" => assert!(matches!(
                result,
                Err(HandoffFailure::CandidateOutcomeUnknown(_))
            )),
            _ => assert!(matches!(
                result,
                Err(HandoffFailure::ExchangeOutcomeUnknown(_))
            )),
        }
        assert_eq!(
            fs::read(source.join("records.log")).unwrap() == original,
            !matches!(failure, "after-exchange" | "sync")
        );
        let live = retry_lock(|| Store::open(&source)).unwrap();
        assert_eq!(live.get(b"key").unwrap(), Some(b"latest".to_vec()));
        assert_eq!(live.get(b"removed").unwrap(), None);
        if failure != "preflight" {
            let old = retry_lock(|| Store::open(&destination)).unwrap();
            assert_eq!(old.get(b"key").unwrap(), Some(b"latest".to_vec()));
            assert_eq!(old.get(b"removed").unwrap(), None);
            assert_eq!(
                fs::read(destination.join("records.log")).unwrap() == original,
                matches!(failure, "after-exchange" | "sync")
            );
        } else {
            assert!(!destination.exists());
        }
    }
}

#[test]
fn exchange_holds_both_mapping_locks_through_failed_parent_sync() {
    let root = tempfile::tempdir().unwrap();
    let (store, source, previous) = populated(root.path());
    let original = fs::read(source.join("records.log")).unwrap();
    let result = store.compact_and_replace_detailed_with_hooks(
        &source,
        &previous,
        |point| {
            if matches!(
                point,
                ExchangePoint::BeforeExchange | ExchangePoint::AfterExchange
            ) {
                for path in [&source, &previous] {
                    assert_eq!(
                        Store::open(path).err().unwrap().kind(),
                        io::ErrorKind::WouldBlock
                    );
                }
                assert_eq!(
                    fs::read(source.join("records.log")).unwrap() == original,
                    point == ExchangePoint::BeforeExchange
                );
            }
            Ok(())
        },
        |parent| {
            assert_eq!(parent, fs::canonicalize(root.path()).unwrap());
            for path in [&source, &previous] {
                assert_eq!(
                    Store::open(path).err().unwrap().kind(),
                    io::ErrorKind::WouldBlock
                );
            }
            assert_eq!(fs::read(previous.join("records.log")).unwrap(), original);
            Err(io::Error::other("injected parent sync failure"))
        },
    );
    assert!(matches!(
        result,
        Err(crate::HandoffFailure::ExchangeOutcomeUnknown(_))
    ));

    // The current protocol releases both locks after an unknown result. Once
    // writes resume, equal pre-exchange contents cannot identify the current
    // mapping: the retained copy is a stale but otherwise valid store.
    let live = Store::open(&source).unwrap();
    let retained = Store::open(&previous).unwrap();
    live.put(b"key", b"after").unwrap();
    assert_eq!(live.get(b"key").unwrap(), Some(b"after".to_vec()));
    assert_eq!(retained.get(b"key").unwrap(), Some(b"latest".to_vec()));
}

#[test]
fn a_contender_with_a_stale_lock_descriptor_cannot_open_the_new_log() {
    let root = tempfile::tempdir().unwrap();
    let (store, source, previous) = populated(root.path());
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let path = source.clone();
    let contender = thread::spawn(move || {
        Store::open_using_with_hook(&path, Log::open, || {
            ready_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            Ok(())
        })
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let compacted = store.compact_and_replace(&source, &previous).unwrap();
    resume_tx.send(()).unwrap();
    let error = contender
        .join()
        .unwrap()
        .err()
        .expect("stale lock must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    compacted.put(b"key", b"after").unwrap();
    assert_eq!(compacted.get(b"key").unwrap(), Some(b"after".to_vec()));
}

#[test]
fn exchange_kill_child() {
    let Ok(point) = std::env::var("EMDA_EXCHANGE_CHILD") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("EMDA_EXCHANGE_ROOT").unwrap());
    let source = root.join("live");
    let previous = root.join("previous");
    let store = retry_lock(|| Store::open(&source)).unwrap();
    let selected = if point == "copy" {
        ExchangePoint::CopyPublished
    } else {
        ExchangePoint::AfterExchange
    };
    let _ = store.compact_and_replace_with_hooks(
        &source,
        &previous,
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
fn process_kill_before_and_after_exchange_preserves_live_data() {
    for point in ["copy", "exchange"] {
        let root = tempfile::tempdir().unwrap();
        let (store, source, previous) = populated(root.path());
        drop(store);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("store::compact_exchange::tests::exchange_kill_child")
            .env("EMDA_EXCHANGE_CHILD", point)
            .env("EMDA_EXCHANGE_ROOT", root.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
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
        let old = retry_lock(|| Store::open(&previous)).unwrap();
        for store in [&live, &old] {
            assert_eq!(store.get(b"key").unwrap(), Some(b"latest".to_vec()));
            assert_eq!(store.get(b"removed").unwrap(), None);
        }
    }
}
