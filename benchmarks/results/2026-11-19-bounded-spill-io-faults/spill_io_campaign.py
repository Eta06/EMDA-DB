"""Inject one EIO into each selected spill syscall in a Linux control trace."""
import argparse
from collections import Counter
import errno
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

SPILL_CALLS = (
    "openat", "pread64", "read", "write", "lseek", "statx", "fstat",
    "newfstatat", "unlink", "unlinkat",
)
PUBLICATION_CALLS = ("rename", "renameat", "renameat2", "link", "linkat")
RUN = re.compile(r"\.emda-stage-v1-[^/]+\.data/(run-[0-9]+(?:\.off)?)")
CALL = re.compile(r"^(\w+)\(")


def invoke(argv):
    return subprocess.run(argv, text=True, capture_output=True, check=True, timeout=30).stdout


def marker(root, format_name):
    source = root / "store"
    if format_name == "v3":
        return os.getxattr(source / "records.log", "user.db.commit.v3")
    return (source / "commit.state").read_bytes()


def snapshot(root, format_name):
    paths = ("store/records.log", "store/checkpoint.selection", "store/commit.state", "base")
    blobs = {
        p: (root / p).read_bytes() for p in paths
        if p != "store/commit.state" or format_name == "v3"
    }
    return blobs, marker(root, format_name)


def candidates(lines, root):
    counts = Counter()
    targets = []
    published = False
    result_name = str(root / "result")
    for line in lines:
        call = CALL.match(line)
        if not call:
            continue
        name = call[1]
        counts[name] += 1
        if name in PUBLICATION_CALLS and f'"{result_name}"' in line and re.search(r"= 0\s*$", line):
            published = True
            break
        spill = RUN.search(line)
        if name in SPILL_CALLS and spill and re.search(r"= \d+(?:<[^>]+>)?\s*$", line):
            targets.append({"syscall": name, "ordinal": counts[name], "run": spill[1]})
    assert published, "checkpoint publication missing from successful trace"
    assert {t["syscall"] for t in targets} >= {"openat", "read", "pread64", "write", "lseek"}
    assert any(t["run"].endswith(".off") for t in targets)
    assert any(not t["run"].endswith(".off") for t in targets)
    return targets


def assert_clean(root):
    for directory in (root, root / "store"):
        assert not any(p.name.startswith(".emda-stage-") for p in directory.iterdir()), directory


def campaign(binary, workspace, output, format_name, tail, fault, syscalls, retry_required):
    label = f"{format_name}-{tail}"
    def command(root, action):
        return [str(binary), action, str(root), format_name, tail]

    def traced(root, trace_path, target=None):
        argv = ["strace", "-qq", "-yy", "-o", str(trace_path),
                "-e", "trace=" + ",".join(SPILL_CALLS + PUBLICATION_CALLS)]
        if target:
            argv += ["-e", f"inject={target['syscall']}:error={fault}:when={target['ordinal']}"]
        return invoke(argv + command(root, "attempt"))

    with tempfile.TemporaryDirectory(prefix=label + "-", dir=workspace) as owned:
        root = Path(owned)
        invoke(command(root, "prepare"))
        original, initial = snapshot(root, format_name)
        final = (root / "expected-witness").read_bytes()
        assert (initial != final) == (tail == "tail")
        baseline = output / f"{label}-control.strace"
        assert traced(root, baseline).strip() == "published"
        targets = candidates(baseline.read_text().splitlines(), root)
        assert invoke(command(root, "verify")).startswith("reference matches")
        assert_clean(root)

    targets = [target for target in targets if target['syscall'] in syscalls]
    assert targets
    print(label, len(targets), dict(Counter(t["syscall"] for t in targets)), flush=True)

    with (output / f"{label}-injections.jsonl").open("w") as records:
        for target in targets:
            with tempfile.TemporaryDirectory(prefix=label + "-", dir=workspace) as owned:
                root = Path(owned)
                invoke(command(root, "prepare"))
                before, witness = snapshot(root, format_name)
                expected = (root / "expected-witness").read_bytes()
                trace_path = root / "trace"
                result = traced(root, trace_path, target)
                trace = trace_path.read_text().splitlines()
                injected = [line for line in trace if "(INJECTED)" in line]
                assert len(injected) == 1, (target, injected, result)
                line = injected[0]
                assert CALL.match(line)[1] == target["syscall"], (target, line)
                actual_run = RUN.search(line)
                assert actual_run and actual_run[1] == target["run"], (target, line)
                assert f" {fault} " in line, (target, line)
                published = result.strip() == "published"
                if fault != "EINTR":
                    assert not published, (target, line)
                if target["syscall"] in retry_required:
                    assert published, (target, line, result)
                after, marker_after = snapshot(root, format_name)
                if published:
                    assert (root / "result").exists(), (target, line)
                    previous = before.pop("store/checkpoint.selection")
                    assert after.pop("store/checkpoint.selection") != previous, (target, line)
                    assert previous != b"", (target, line)
                    assert after == before and marker_after == expected, (target, line)
                else:
                    assert result.startswith(f"not_published_errno=Some({getattr(errno, fault)})\n"), (
                        target, line, result
                    )
                    assert not (root / "result").exists(), (target, line)
                    assert after == before, (target, line)
                    assert marker_after in (witness, expected), (target, line)
                assert_clean(root)
                if not published:
                    assert invoke(command(root, "attempt")).strip() == "published", (target, line)
                verification = invoke(command(root, "verify"))
                assert verification.startswith("reference matches"), (target, line)
                assert_clean(root)
                records.write(json.dumps({
                    **target, "injection": line, "outcome": result.strip(),
                    "published": published, "witness_advanced": marker_after != witness,
                    "verified": verification.strip(),
                }) + "\n")
                records.flush()
    print(label, "all fault checks passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("probe", type=Path)
    parser.add_argument("workspace", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--error", choices=("EIO", "EINTR"), default="EIO")
    parser.add_argument("--syscall", action="append", choices=SPILL_CALLS)
    parser.add_argument("--require-retry", action="append", choices=SPILL_CALLS, default=[])
    parser.add_argument("--format", action="append", choices=("v2", "v3"))
    args = parser.parse_args()
    args.output.mkdir()
    for format_name in args.format or ("v2", "v3"):
        for tail in ("acked", "tail"):
            campaign(args.probe.resolve(), args.workspace.resolve(), args.output.resolve(),
                     format_name, tail, args.error, args.syscall or SPILL_CALLS, args.require_retry)


if __name__ == "__main__":
    main()
