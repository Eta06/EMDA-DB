"""Check pending-tail witness write and sync errors with Linux strace."""
import argparse
import collections
import json
import os
import pathlib
import re
import subprocess
import tempfile

operation = "checkpoint-managed-rebuild-tail-existing"
attribute = "user.db.commit.v3"
syscalls = "fsync,fdatasync,pwrite64,fgetxattr,fsetxattr"

def probe(binary, *args):
    return json.loads(subprocess.check_output([binary, *args], text=True, timeout=30))

def run(binary, parent, format_name, rebuild, fault=None):
    with tempfile.TemporaryDirectory(prefix="rebuild-tail-", dir=parent) as directory:
        root = pathlib.Path(directory).resolve()
        log = root / "source/records.log"
        witness = root / "source/commit.state"
        selection = root / "source/checkpoint.selection"
        probe(binary, "setup", root, format_name)
        probe(binary, "setup-managed", root, operation)
        previous = selection.read_bytes()
        boundary = witness.read_bytes() if format_name == "v2" else os.getxattr(log, attribute)
        probe(binary, "setup-managed-tail", root, operation)
        new_boundary = witness.read_bytes() if format_name == "v2" else os.getxattr(log, attribute)
        assert boundary != new_boundary
        if format_name == "v2":
            witness.write_bytes(boundary)
            marker = witness
        else:
            os.setxattr(log, attribute, boundary)
            marker = log
        current_log = log.read_bytes()
        trace = root / "trace.log"
        command = ["strace", "-y", "-qq", "-o", trace, "-e", "trace=" + syscalls]
        if fault is not None:
            point, (syscall, index) = fault
            command += ["-e", f"inject={syscall}:error=EIO:when={index}"]
        outcome = json.loads(subprocess.check_output([*command, binary, "attempt-rebuild-publication",
                                                      root, operation, rebuild], text=True, timeout=30))
        lines = trace.read_text().splitlines()
        assert current_log == log.read_bytes()
        if fault is None:
            assert outcome["status"] == "success", outcome
            assert (root / "result").is_file()
            assert selection.read_bytes() != previous
            assert probe(binary, "verify", root, operation, "success", "checkpoint-first")["status"] == "verified"
            counts = collections.Counter()
            points = {}
            for line in lines:
                match = re.match(r"(\w+)\(", line)
                if not match:
                    continue
                syscall = match[1]
                counts[syscall] += 1
                if ("witness write" not in points and str(log) in line
                        and syscall == ("fdatasync" if format_name == "v2" else "fgetxattr")):
                    points["before witness write"] = (syscall, counts[syscall])
                if str(marker) not in line:
                    continue
                if syscall in ("pwrite64", "fsetxattr"):
                    points["witness write"] = (syscall, counts[syscall])
                elif "witness write" in points and syscall in ("fsync", "fdatasync"):
                    points["witness sync"] = (syscall, counts[syscall])
                    break
            assert len(points) == 3, lines
            return points
        classification = "not_published" if point == "before witness write" else "tail_adoption_unknown"
        assert outcome["classification"] == classification, outcome
        assert not (root / "result").exists()
        assert selection.read_bytes() == previous
        observed = witness.read_bytes() if format_name == "v2" else os.getxattr(log, attribute)
        expected = new_boundary if point == "witness sync" else boundary
        assert observed == expected
        errors = [line for line in lines if "(INJECTED)" in line]
        error_file = log if point == "before witness write" else marker
        assert len(errors) == 1 and str(error_file) in errors[0], errors
        assert probe(binary, "verify", root, operation, "error", "ordinary-first")["status"] == "verified"
        retry = probe(binary, "attempt-rebuild-publication", root, operation, rebuild)
        assert retry["status"] == "success", retry
        assert probe(binary, "verify", root, operation, "success", "checkpoint-first")["status"] == "verified"
        return outcome, errors[0]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--directory", type=pathlib.Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    for rebuild in ("ordinary", "full-bounded", "suffix-bounded"):
        for format_name in ("v2", "v3"):
            points = run(binary, args.directory, format_name, rebuild)
            for point, injection in points.items():
                outcome, trace = run(binary, args.directory, format_name, rebuild, (point, injection))
                print(json.dumps(dict(rebuild=rebuild, format=format_name, point=point,
                                      injection=injection, outcome=outcome, trace=trace)), flush=True)


if __name__ == "__main__":
    main()
