"""Record one benchmark command and its unmodified combined output."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import subprocess
import time


def git(*args):
    result = subprocess.run(
        ("git", *args), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        check=False, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def record_case(directory, case, command):
    directory.mkdir()
    metadata_path = directory / "attempt.json"
    record = {
        "schema_version": 1,
        "case": case,
        "command": command,
        "working_directory": str(Path.cwd()),
        "revision": git("rev-parse", "HEAD"),
        "tracked_changes": git("status", "--porcelain", "--untracked-files=no"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "started_at": utc_now(),
        "status": "running",
        "output": "command.log",
    }
    metadata_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(f"Recording {case} in {directory}", flush=True)
    start = time.monotonic()
    with (directory / record["output"]).open("xb") as log:
        try:
            child = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
            record["returncode"] = child.returncode
            record["status"] = "exited" if child.returncode >= 0 else "signalled"
            exit_code = child.returncode if child.returncode >= 0 else 128 - child.returncode
        except KeyboardInterrupt:
            record["status"] = "interrupted"
            exit_code = 130
        except OSError as error:
            record["status"] = "launch_failed"
            record["error"] = str(error)
            exit_code = 1
    record["finished_at"] = utc_now()
    record["elapsed_seconds"] = time.monotonic() - start
    record["wrapper_exit_code"] = exit_code
    pending = directory / "attempt.tmp"
    pending.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    os.replace(pending, metadata_path)
    return exit_code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path, help="new attempt directory")
    parser.add_argument("--case", required=True, help="stable workload name")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("supply a benchmark command after --")
    try:
        return record_case(args.directory, args.case, command)
    except OSError as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
