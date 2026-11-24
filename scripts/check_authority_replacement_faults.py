"""Inject Linux errors into a disposable file-backed authority replacement."""
import argparse
from collections import Counter
import errno
import json
from pathlib import Path
import re
import subprocess
import tempfile


def probe(binary, root, operation, command="authority"):
    return json.loads(subprocess.check_output(
        [binary, command, root, operation], text=True, timeout=30))


def attempted(binary, root, inject=None, operation="authority"):
    trace = root / "trace.log"
    command = ["strace", "-qq", "-y", "-o", trace,
               "-e", "trace=write,fsync,fdatasync,rename,renameat,renameat2"]
    if inject is not None:
        syscall, ordinal = inject
        command += ["-e", f"inject={syscall}:error=EIO:when={ordinal}"]
    command += [binary, operation, root, "attempt"]
    outcome = json.loads(subprocess.check_output(command, text=True, timeout=30))
    return outcome, trace.read_text().splitlines()


def cas_points(authority, lines):
    stage = authority / ".emda-authority-next-v1"
    counts = Counter()
    points = {}
    seen_write = False
    for line in lines:
        syscall = re.match(r"(\w+)\(", line)
        if syscall is None:
            continue
        syscall = syscall[1]
        counts[syscall] += 1
        if syscall == "write" and f"<{stage}>" in line:
            point = "replacement write"
            seen_write = True
        elif syscall in ("fsync", "fdatasync") and f"<{stage}>" in line:
            point = "replacement sync"
        elif syscall.startswith("rename") and str(stage) in line:
            point = "rename"
        elif syscall in ("fsync", "fdatasync") and seen_write:
            assert f"<{authority}>" in line, line
            point = "directory sync"
        else:
            continue
        assert point not in points, lines
        points[point] = (syscall, counts[syscall])
    assert list(points) == ["replacement write", "replacement sync", "rename", "directory sync"], lines
    return points


def reclamation_points(root, trace):
    authority = root / "authority"
    inactive = root / "catalog/generation-a"
    write_seen = moved = False
    counts = Counter()
    points = {}
    for line in trace:
        syscall = re.match(r"(\w+)\(", line)
        if syscall is None:
            continue
        syscall = syscall[1]
        counts[syscall] += 1
        if syscall == "write" and str(authority / ".emda-authority-next-v1") in line:
            write_seen = True
        elif syscall.startswith("rename") and str(inactive) in line and "/retired" in line:
            moved = True
        elif syscall in ("fsync", "fdatasync"):
            if write_seen and f"<{authority}>" in line:
                points["authority fence sync"] = (syscall, counts[syscall])
                write_seen = False
            if moved:
                assert ".emda-stage-v1-" in line, line
                points["retirement move sync"] = (syscall, counts[syscall])
                break
    assert list(points) == ["authority fence sync", "retirement move sync"], trace
    return points


def check_reclamation(binary, parent):
    command = "authorized-reclamation"
    with tempfile.TemporaryDirectory(prefix="authority-reclaim-success-", dir=parent) as directory:
        root = Path(directory).resolve()
        assert probe(binary, root, "setup", command) == {"status": "ready"}
        outcome, trace = attempted(binary, root, operation=command)
        assert outcome == {"status": "success", "removed": True}
        points = reclamation_points(root, trace)
        assert probe(binary, root, "inspect", command) == {"revision": 2}
        print(json.dumps(dict(point="reclamation success", trace=trace)), flush=True)
    for point, inject in points.items():
        with tempfile.TemporaryDirectory(prefix="authority-reclaim-fault-", dir=parent) as directory:
            root = Path(directory).resolve()
            assert probe(binary, root, "setup", command) == {"status": "ready"}
            outcome, trace = attempted(binary, root, inject, command)
            assert len([line for line in trace if "(INJECTED)" in line]) == 1, trace
            unconfirmed = point == "authority fence sync"
            assert outcome == {"status": "error", "errno": errno.EIO,
                               "fence": "unconfirmed" if unconfirmed else "confirmed",
                               "cleanup": "not_started" if unconfirmed else "unknown",
                               "blocked": unconfirmed}, outcome
            assert (root / "catalog/generation-a").is_dir() == unconfirmed
            assert probe(binary, root, "inspect", command) == {"revision": 2}
            assert probe(binary, root, "attempt", command) == {"status": "success", "removed": True}
            revision = 3 if unconfirmed else 2
            assert probe(binary, root, "inspect", command) == {"revision": revision}
            assert probe(binary, root, "attempt", command) == {"status": "success", "removed": False}
            print(json.dumps(dict(point=point, injection=inject, outcome=outcome,
                                  recovered_revision=revision, trace=trace)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="authority-success-", dir=args.directory) as directory:
        root = Path(directory).resolve()
        assert probe(binary, root, "setup") == {"status": "ready"}
        outcome, trace = attempted(binary, root)
        assert outcome == {"status": "success", "replaced": True}
        points = cas_points(root / "authority", trace)
        assert probe(binary, root, "inspect") == {"revision": 1}
        complete = (root / "authority/authority.record").read_bytes()
        print(json.dumps(dict(point="success", syncs=2, trace=trace)), flush=True)
    for point, inject in points.items():
        with tempfile.TemporaryDirectory(prefix="authority-fault-", dir=args.directory) as directory:
            root = Path(directory).resolve()
            assert probe(binary, root, "setup") == {"status": "ready"}
            record = root / "authority/authority.record"
            stage = root / "authority/.emda-authority-next-v1"
            initial = record.read_bytes()
            outcome, trace = attempted(binary, root, inject)
            injected = [line for line in trace if "(INJECTED)" in line]
            assert len(injected) == 1, trace
            assert outcome == {"status": "error", "errno": errno.EIO}, outcome
            published = point == "directory sync"
            assert record.read_bytes() == (complete if published else initial)
            assert not stage.exists()
            assert probe(binary, root, "inspect") == {"revision": int(published)}
            assert probe(binary, root, "attempt") == {"status": "success", "replaced": not published}
            assert probe(binary, root, "inspect") == {"revision": 1}
            print(json.dumps(dict(point=point, injection=inject, outcome=outcome,
                                  published=published, trace=trace)), flush=True)
    check_reclamation(binary, args.directory)


if __name__ == "__main__":
    main()
