//! Bounded child-process execution for Linux file-limit tests.
use std::io;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

pub fn child_command(test: &str, ignore_signal: bool) -> io::Result<Command> {
    let executable = std::env::current_exe()?;
    let mut command = if ignore_signal {
        let mut shell = Command::new("sh");
        shell.args(["-c", "trap '' XFSZ; exec \"$@\"", "file-limit-child"]);
        shell.arg(executable);
        shell
    } else {
        Command::new(executable)
    };
    command.args(["--exact", test, "--nocapture"]);
    Ok(command)
}

pub fn output_with_timeout(command: &mut Command) -> io::Result<Output> {
    output_with_deadline(command, Duration::from_secs(30))
}

fn output_with_deadline(command: &mut Command, timeout: Duration) -> io::Result<Output> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output(),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                let _ = child.kill();
                let output = child.wait_with_output()?;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "file-limit child timed out; stdout={} stderr={}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr),
                    ),
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deliberately_blocked_child() {
        if std::env::var_os("EMDA_FILE_LIMIT_TIMEOUT_PROBE").is_some() {
            std::thread::sleep(Duration::from_secs(60));
        }
    }

    #[test]
    fn timeout_terminates_a_blocked_child() {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "kernel_file_limit_common::tests::deliberately_blocked_child",
            ])
            .env("EMDA_FILE_LIMIT_TIMEOUT_PROBE", "1");
        let error = output_with_deadline(&mut command, Duration::from_millis(100)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}
