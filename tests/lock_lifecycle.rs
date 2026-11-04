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

// A fork inherits descriptors belonging to every thread in the process. Keep
// these deliberately blocked pre-exec children from retaining each other's
// store locks while the two lifecycle cases run under the parallel harness.
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

fn verify_lock_until_exec(path: &Path, release_owner: impl FnOnce()) {
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
    release_owner();
    // The child still has the inherited descriptor until exec, even though the
    // original owner was dropped. Never leave the blocked child behind on error.
    let blocked_kind = match Store::open(path) {
        Err(error) => Some(error.kind()),
        Ok(store) => {
            drop(store);
            None
        }
    };
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
    assert_eq!(blocked_kind, Some(io::ErrorKind::WouldBlock));
    assert_eq!(reopened_value.as_deref(), Some(&b"value"[..]));
}

#[test]
fn store_lock_survives_fork_until_exec_but_not_the_executed_child() {
    let _serial = PROCESS_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    verify_lock_until_exec(&path, || drop(store));
}

#[test]
fn checkpoint_view_lock_survives_fork_until_exec_but_not_the_executed_child() {
    let _serial = PROCESS_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let checkpoint = directory.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    store.put(b"key", b"value").unwrap();
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    drop(store);
    let view = CheckpointView::open(&path, &checkpoint, &digest).unwrap();
    verify_lock_until_exec(&path, || drop(view));
}
