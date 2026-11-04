use super::*;
use crate::log::InitializationPoint;
use std::io::{BufRead, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn point(name: &str) -> InitializationPoint {
    match name {
        "log-created" => InitializationPoint::LogCreated,
        "metadata-decided" => InitializationPoint::MetadataDecided,
        "header-written" => InitializationPoint::HeaderWritten,
        "log-synced" => InitializationPoint::LogSynced,
        "witness-created" => InitializationPoint::WitnessCreated,
        _ => panic!("unknown initialization boundary"),
    }
}

#[test]
fn initialization_crash_child() {
    let Ok(path) = std::env::var("EMDA_INITIALIZATION_CRASH_PATH") else {
        return;
    };
    let requested = point(&std::env::var("EMDA_INITIALIZATION_CRASH_POINT").unwrap());
    Store::open_using(Path::new(&path), |path, initialize| {
        Log::open_with_initialization_hook(path, initialize, |current| {
            if current == requested {
                println!("INITIALIZATION_CRASH_BOUNDARY_REACHED");
                std::io::stdout().flush()?;
                loop {
                    std::thread::park();
                }
            }
            Ok(())
        })
    })
    .unwrap();
}

fn kill_at_initialization_boundary(path: &Path, name: &str) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "store::initialization_tests::initialization_crash_child",
            "--nocapture",
        ])
        .env("EMDA_INITIALIZATION_CRASH_PATH", path)
        .env("EMDA_INITIALIZATION_CRASH_POINT", name)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(output)
            .lines()
            .map_while(Result::ok)
        {
            if line.contains("INITIALIZATION_CRASH_BOUNDARY_REACHED") {
                let _ = sender.send(());
                break;
            }
        }
    });
    let reached = receiver.recv_timeout(Duration::from_secs(20));
    let busy = reached.is_ok().then(|| Store::open(path));
    let killed = child.kill();
    child.wait().unwrap();
    reached.expect("child did not reach initialization boundary");
    killed.expect("could not terminate initialization child");
    assert!(matches!(busy, Some(Err(ref error)) if error.kind() == io::ErrorKind::WouldBlock));
}

#[cfg(target_os = "linux")]
fn metadata(path: &Path) -> Option<Vec<u8>> {
    use rustix::fs::getxattr;
    use rustix::io::Errno;
    let mut bytes = [0; 255];
    match getxattr(path, "user.db.commit.v3", &mut bytes[..]) {
        Ok(length) => Some(bytes[..length].to_vec()),
        Err(Errno::NODATA | Errno::NOTSUP | Errno::NOSYS) => None,
        Err(error) => panic!("read metadata witness: {error}"),
    }
}

#[cfg(not(target_os = "linux"))]
fn metadata(_: &Path) -> Option<Vec<u8>> {
    None
}

#[test]
fn killed_initialization_never_silently_reopens_as_legacy() {
    for name in [
        "log-created",
        "metadata-decided",
        "header-written",
        "log-synced",
        "witness-created",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        kill_at_initialization_boundary(&path, name);
        let log_path = path.join("records.log");
        let before = std::fs::read(&log_path).unwrap();
        let before_metadata = metadata(&log_path);
        let witness_path = path.join("commit.state");
        let before_witness = match std::fs::read(&witness_path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => panic!("read commit witness: {error}"),
        };
        let opened = crate::test_support::retry_lock(|| Store::open(&path));
        if name == "witness-created" {
            let store = opened.unwrap();
            assert_eq!(store.get(b"key").unwrap(), None);
            store.transaction(|tx| tx.put(b"key", b"value")).unwrap();
            drop(store);
            let recovered = crate::test_support::retry_lock(|| Store::open(&path)).unwrap();
            assert_eq!(
                recovered.get(b"key").unwrap().as_deref(),
                Some(&b"value"[..])
            );
        } else {
            assert!(opened.is_err(), "{name}");
            assert_eq!(std::fs::read(&log_path).unwrap(), before, "{name}");
            assert_eq!(metadata(&log_path), before_metadata, "{name}");
            assert_eq!(std::fs::read(&witness_path).ok(), before_witness, "{name}");
        }
    }
}
