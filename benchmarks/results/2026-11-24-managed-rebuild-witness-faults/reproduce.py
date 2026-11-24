import collections
import json
import os
import pathlib
import re
import subprocess
import tempfile

binary = pathlib.Path("target/debug/examples/sync_failure_probe").resolve()
operation = "checkpoint-managed-rebuild-tail-existing"
attribute = "user.db.commit.v3"
syscalls = "fsync,fdatasync,pwrite64,fsetxattr"

def probe(*args):
    return json.loads(subprocess.check_output([binary, *args], text=True))

def run(format_name, injection=None):
    with tempfile.TemporaryDirectory(dir=".") as directory:
        root = pathlib.Path(directory).resolve()
        log = root / "source/records.log"
        witness = root / "source/commit.state"
        selection = root / "source/checkpoint.selection"
        probe("setup", root, format_name)
        probe("setup-managed", root, operation)
        previous = selection.read_bytes()
        boundary = witness.read_bytes() if format_name == "v2" else os.getxattr(log, attribute)
        probe("setup-managed-tail", root, operation)
        if format_name == "v2":
            witness.write_bytes(boundary)
            marker = witness
        else:
            os.setxattr(log, attribute, boundary)
            marker = log
        current_log = log.read_bytes()
        trace = root / "trace.log"
        command = ["strace", "-y", "-qq", "-o", trace, "-e", "trace=" + syscalls]
        if injection is not None:
            syscall, index = injection
            command += ["-e", f"inject={syscall}:error=EIO:when={index}"]
        outcome = json.loads(subprocess.check_output([*command, binary, "attempt-publication",
                                                      root, operation], text=True))
        lines = trace.read_text().splitlines()
        assert current_log == log.read_bytes()
        if injection is None:
            assert outcome["status"] == "success", outcome
            assert (root / "result").is_file()
            counts = collections.Counter()
            points = {}
            for line in lines:
                match = re.match(r"(\w+)\(", line)
                if not match:
                    continue
                syscall = match[1]
                counts[syscall] += 1
                if str(marker) not in line:
                    continue
                if syscall in ("pwrite64", "fsetxattr"):
                    points["witness write"] = (syscall, counts[syscall])
                elif "witness write" in points and syscall in ("fsync", "fdatasync"):
                    points["witness sync"] = (syscall, counts[syscall])
                    break
            assert len(points) == 2, lines
            return points
        assert outcome["classification"] == "tail_adoption_unknown", outcome
        assert not (root / "result").exists()
        assert selection.read_bytes() == previous
        errors = [line for line in lines if "(INJECTED)" in line]
        assert len(errors) == 1 and str(marker) in errors[0], errors
        assert probe("verify", root, operation, "error", "ordinary-first")["status"] == "verified"
        return outcome, errors[0]

for format_name in ("v2", "v3"):
    for point, injection in run(format_name).items():
        outcome, trace = run(format_name, injection)
        print(json.dumps(dict(format=format_name, point=point, injection=injection,
                              outcome=outcome, trace=trace)))
