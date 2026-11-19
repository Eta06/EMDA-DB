//! Process-level checks for store and historical-view lock ownership.
use database_engine::{CheckpointView, Store};
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::Duration;

// Keep the deliberate pre-exec pauses independent under the parallel harness.
static PROCESS_TEST: Mutex<()> = Mutex::new(());

#[test]
fn lock_child() {
    let Ok(mode) = std::env::var("EMDA_LOCK_LIFECYCLE_CHILD") else {
        return;
    };
    match mode.as_str() {
        "probe" => {
            let path = std::env::var_os("EMDA_LOCK_LIFECYCLE_PATH").unwrap();
            let result = Store::open(path);
            assert!(matches!(result, Err(ref error) if error.kind() == io::ErrorKind::WouldBlock));
        }
        "hold" => loop {
            thread::park();
        },
        _ => panic!("unknown child mode"),
    }
}

fn verify_lock_lifecycle(path: &Path, release_owner: impl FnOnce(), reopen_before_exec: bool) {
    let executable = std::env::current_exe().unwrap();
    let probe = Command::new(&executable)
        .args(["--exact", "lock_child", "--nocapture"])
        .env("EMDA_LOCK_LIFECYCLE_CHILD", "probe")
        .env("EMDA_LOCK_LIFECYCLE_PATH", path)
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(
        probe.success(),
        "a separate process acquired the active lock"
    );

    let (mut parent, child_socket) = UnixStream::pair().unwrap();
    parent
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let (sender, receiver) = mpsc::channel();
    let spawning = thread::spawn(move || {
        let mut command = Command::new(executable);
        command
            .args(["--exact", "lock_child", "--nocapture"])
            .env("EMDA_LOCK_LIFECYCLE_CHILD", "hold")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // SAFETY: after fork and before exec, the closure performs only
        // async-signal-safe one-byte read/write syscalls on an inherited socket.
        // It does not use the inherited Store or acquire process-local locks.
        unsafe {
            command.pre_exec(move || {
                if rustix::io::write(&child_socket, b"r")? != 1 {
                    return Err(io::Error::from_raw_os_error(5));
                }
                let mut proceed = [0_u8; 1];
                if rustix::io::read(&child_socket, &mut proceed)? != 1 || proceed != *b"e" {
                    return Err(io::Error::from_raw_os_error(5));
                }
                Ok(())
            });
        }
        let _ = sender.send(command.spawn());
    });
    let mut ready = [0_u8; 1];
    let read_ready = parent.read_exact(&mut ready);
    let active_error = Store::open(path).err().map(|error| error.kind());
    release_owner();
    let before_exec = Store::open(path).and_then(|store| store.get(b"key"));
    // Release the child before asserting either outcome.
    let released = parent.write_all(b"e");
    drop(parent);
    let spawned = receiver.recv_timeout(Duration::from_secs(10));
    let mut child = match spawned {
        Ok(Ok(child)) => child,
        Ok(Err(error)) => {
            let _ = spawning.join();
            panic!("failed to exec child: {error}");
        }
        Err(error) => panic!("child did not complete exec: {error}"),
    };
    spawning.join().unwrap();
    let reopened = Store::open(path);
    let reopened_value = reopened
        .as_ref()
        .ok()
        .and_then(|store| store.get(b"key").ok().flatten());
    drop(reopened);
    let _ = child.kill();
    let _ = child.wait();
    read_ready.expect("child did not reach the pre-exec boundary");
    released.expect("could not release child to exec");
    assert_eq!(ready, *b"r");
    assert_eq!(active_error, Some(io::ErrorKind::WouldBlock));
    if reopen_before_exec {
        assert_eq!(before_exec.unwrap().as_deref(), Some(&b"value"[..]));
    } else {
        assert_eq!(before_exec.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }
    assert_eq!(reopened_value.as_deref(), Some(&b"value"[..]));
}

#[test]
fn store_reopens_after_owner_closes_before_child_exec() {
    let _serial = PROCESS_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    verify_lock_lifecycle(&path, || drop(store), true);
}

#[test]
fn checkpoint_view_releases_lock_before_child_exec() {
    let _serial = PROCESS_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
    verify_lock_lifecycle(&path, || drop(view), true);
}

#[test]
fn bare_close_on_exec_descriptor_keeps_lock_until_exec() {
    let _serial = PROCESS_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    drop(store);
    let lock = std::fs::File::open(path.join("store.lock")).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    verify_lock_lifecycle(&path, || drop(lock), false);
}
