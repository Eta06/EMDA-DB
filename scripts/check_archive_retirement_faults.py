"""Check cutover archive retirement on disposable V2 and V3 Linux fixtures."""
import argparse
from collections import Counter
import errno
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

SYSCALLS = "fsync,fdatasync,unlink,unlinkat"
SUCCESS = {"status": "success", "removed": True}
ALREADY_ABSENT = {"status": "success", "removed": False}


def probe(binary, *args):
    return json.loads(subprocess.check_output([binary, *args], text=True, timeout=30))


def prepare(binary, root, fmt):
    root.mkdir()
    assert probe(binary, "setup", root, fmt) == {"status": "ready", "format": fmt}
    assert probe(binary, "archive-retirement", root, "prepare") == {"status": "ready"}
    catalog, = (root / "source").glob("retained-cutover-*")
    if fmt == "v3":
        assert len(os.getxattr(catalog / "legacy.records", "user.db.commit.v3")) == 36
    return catalog


def manifest(root):
    state = {}
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if relative.parts[0] == "archive-restore":
            continue
        xattrs = {name: os.getxattr(path, name) for name in os.listxattr(path)}
        state[relative] = (path.lstat().st_mode, path.read_bytes() if path.is_file() else None, xattrs)
    return state


def trace_attempt(binary, root, trace_path, injection=None):
    command = ["strace", "-qq", "-y", "-o", trace_path, "-e", "trace=" + SYSCALLS]
    if injection is not None:
        syscall, ordinal = injection
        command += ["-e", f"inject={syscall}:error=EIO:when={ordinal}"]
    result = probe(*command, binary, "archive-retirement", root, "attempt")
    return result, trace_path.read_text().splitlines()


def fault_points(root, catalog, trace, already_absent=False):
    counts = Counter()
    points = {}
    for line in trace:
        match = re.match(r"(\w+)\(", line)
        assert match, line
        syscall = match[1]
        counts[syscall] += 1
        if syscall in ("fsync", "fdatasync") and f"<{root / 'archive-backup/suffix'}>" in line:
            point = "Verify"
        elif syscall.startswith("unlink") and str(catalog / "legacy.records") in line:
            point = "UnlinkLog"
        elif syscall.startswith("unlink") and str(catalog / "legacy.witness") in line:
            point = "UnlinkWitness"
        elif ((already_absent and "Verify" in points) or "UnlinkWitness" in points) and (
            syscall in ("fsync", "fdatasync") and f"<{catalog}>" in line
        ):
            points["SyncCatalog"] = syscall, counts[syscall]
            break
        else:
            continue
        assert point not in points, trace
        points[point] = syscall, counts[syscall]
    expected = ["Verify", "SyncCatalog"] if already_absent else [
        "Verify", "UnlinkLog", "UnlinkWitness", "SyncCatalog"
    ]
    assert list(points) == expected, trace
    return points


def run_format(binary, directory, fmt):
    with tempfile.TemporaryDirectory(prefix=f"archive-{fmt}-", dir=directory) as temporary:
        parent = Path(temporary).resolve()
        root = parent / "baseline"
        catalog = prepare(binary, root, fmt)
        trace_path = parent / "trace.log"
        result, trace = trace_attempt(binary, root, trace_path)
        assert result == SUCCESS, result
        points = fault_points(root, catalog, trace)
        print(json.dumps(dict(format=fmt, case="baseline", trace=trace)), flush=True)
        result, trace = trace_attempt(binary, root, trace_path)
        assert result == ALREADY_ABSENT, result
        empty_sync = fault_points(root, catalog, trace, already_absent=True)["SyncCatalog"]
        print(json.dumps(dict(format=fmt, case="already absent baseline", trace=trace)), flush=True)

        for case, injection in [*points.items(), ("already absent sync", empty_sync)]:
            root = parent / ("fault-" + case.replace(" ", "-"))
            catalog = prepare(binary, root, fmt)
            absent = case == "already absent sync"
            if absent:
                assert probe(binary, "archive-retirement", root, "attempt") == SUCCESS
            before = manifest(root)
            log_path = catalog / "legacy.records"
            witness_path = catalog / "legacy.witness"
            log_unlinked = case in ("UnlinkWitness", "SyncCatalog")
            witness_unlinked = case == "SyncCatalog"
            expected_remaining = before.copy()
            if log_unlinked:
                del expected_remaining[log_path.relative_to(root)]
            if witness_unlinked:
                del expected_remaining[witness_path.relative_to(root)]
            outcome, trace = trace_attempt(binary, root, trace_path, injection)
            step = "SyncCatalog" if absent else case
            assert outcome == dict(status="error", step=step, errno=errno.EIO,
                                   log_unlinked=log_unlinked, witness_unlinked=witness_unlinked), outcome
            injected, = [line for line in trace if "(INJECTED)" in line]
            failed_path = {"Verify": root / "archive-backup/suffix", "UnlinkLog": log_path,
                           "UnlinkWitness": witness_path, "SyncCatalog": catalog}[step]
            assert str(failed_path) in injected and " EIO " in injected, injected
            assert manifest(root) == expected_remaining
            assert probe(binary, "archive-retirement", root, "verify") == {"status": "verified", "entries": 3}
            assert manifest(root) == expected_remaining
            retry = probe(binary, "archive-retirement", root, "attempt")
            assert retry == (ALREADY_ABSENT if absent or witness_unlinked else SUCCESS), retry
            for path in (log_path, witness_path):
                expected_remaining.pop(path.relative_to(root), None)
            assert manifest(root) == expected_remaining
            print(json.dumps(dict(format=fmt, case=case, injection=injection, outcome=outcome,
                                  retry=retry, verified_entries=3, trace=trace)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    for fmt in ("v2", "v3"):
        run_format(args.binary.resolve(strict=True), args.directory, fmt)


if __name__ == "__main__":
    main()
