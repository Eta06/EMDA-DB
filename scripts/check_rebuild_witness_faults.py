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
syscalls = "fsync,fdatasync,read,pread64,pwrite64,fgetxattr,fsetxattr"

def probe(binary, *args):
    return json.loads(subprocess.check_output([binary, *args], text=True, timeout=30))

def log_read_requests(lines, log):
    requests = []
    for line in lines:
        if not line.startswith(("read(", "pread64(")) or f"<{log}>" not in line:
            continue
        read = re.search(r", (\d+)(?:, \d+)?\)\s+= (\d+)$", line)
        assert read is not None, line
        requested, returned = map(int, read.groups())
        assert requested == returned, line
        requests.append(requested)
    return requests

def run(binary, parent, format_name, rebuild, fault=None):
    with tempfile.TemporaryDirectory(prefix="rebuild-tail-", dir=parent) as directory:
        root = pathlib.Path(directory).resolve()
        log = root / "source/records.log"
        witness = root / "source/commit.state"
        selection = root / "source/checkpoint.selection"
        def read_boundary():
            return witness.read_bytes() if format_name == "v2" else os.getxattr(log, attribute)

        def read_selection():
            return selection.read_bytes() if selection.exists() else None

        probe(binary, "setup", root, format_name)
        explicit = rebuild == "explicit-checkpoint-open"
        if not explicit:
            probe(binary, "setup-managed", root, operation)
        previous = read_selection()
        boundary = read_boundary()
        prefix = log.stat().st_size
        probe(binary, "setup-managed-tail", root,
              "checkpoint-managed-rebuild-tail" if explicit else operation)
        new_boundary = read_boundary()
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
        recovery_open = rebuild in ("full-log-open", "managed-checkpoint-open", "explicit-checkpoint-open")
        attempt = ("attempt-recovery-open", root, rebuild) if recovery_open else (
            "attempt-rebuild-publication", root, operation, rebuild)
        scan_cap = None
        full_log = rebuild == "full-log-open"
        scan_kind = "full" if full_log else "checkpoint"
        if fault is None and recovery_open:
            scan_cap = 2 * len(current_log) - (0 if full_log else prefix)
            rejected = probe(*command, binary, *attempt, str(scan_cap - 1))
            assert rejected == {"status": "error", "classification": "not_adopted",
                                "kind": "InvalidInput",
                                "message": f"{scan_kind} recovery log scan byte limit exceeded"}, rejected
            rejected_reads = log_read_requests(trace.read_text().splitlines(), log)
            assert sum(rejected_reads) == 8 + 28, rejected_reads
            assert log.read_bytes() == current_log and read_boundary() == boundary
            assert read_selection() == previous
            attempt += (str(scan_cap),)
        outcome = probe(command[0], *command[1:], binary, *attempt)
        lines = trace.read_text().splitlines()
        assert current_log == log.read_bytes()
        if fault is None:
            assert outcome["status"] == "success", outcome
            if scan_cap is not None:
                requests = log_read_requests(lines, log)
                identity_opens = 1 if full_log else 2
                # Identity opens and the probe's final point read are outside the scan ceiling.
                assert sum(requests) == scan_cap + identity_opens * (8 + 28) + len(b"acknowledged-tail"), requests
                print(json.dumps(dict(rebuild=rebuild, format=format_name,
                                      point=f"{scan_kind} recovery log scan limit", scan_bytes=scan_cap,
                                      rejected_request_bytes=rejected_reads, request_bytes=requests)), flush=True)
            if rebuild in ("full-bounded", "suffix-bounded"):
                requests = log_read_requests(lines, log)
                expected_reads = 3 * len(current_log) + 7 * (8 + 28)
                assert sum(requests) == expected_reads, requests
                print(json.dumps(dict(rebuild=rebuild, format=format_name,
                                      point="source log read reservation", expected_bytes=expected_reads,
                                      request_bytes=requests)), flush=True)
            if recovery_open:
                assert not (root / "result").exists()
                assert read_selection() == previous
                assert read_boundary() == new_boundary
            else:
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
                if rebuild == "managed-checkpoint-open" and syscall == "fsync" and str(selection) in line:
                    points["selection sync"] = (syscall, counts[syscall])
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
            assert len(points) == (4 if rebuild == "managed-checkpoint-open" else 3), lines
            return points
        before_write = "not_adopted" if recovery_open else "not_published"
        classification = before_write if point in ("before witness write", "selection sync") else "tail_adoption_unknown"
        assert outcome["classification"] == classification, outcome
        assert not (root / "result").exists()
        assert read_selection() == previous
        expected = new_boundary if point == "witness sync" else boundary
        assert read_boundary() == expected
        errors = [line for line in lines if "(INJECTED)" in line]
        error_file = {"before witness write": log, "selection sync": selection}.get(point, marker)
        assert len(errors) == 1 and str(error_file) in errors[0], errors
        if not recovery_open:
            assert probe(binary, "verify", root, operation, "error", "ordinary-first")["status"] == "verified"
        retry = probe(binary, *attempt)
        assert retry["status"] == "success", retry
        if recovery_open:
            assert read_boundary() == new_boundary
            assert current_log == log.read_bytes()
            assert read_selection() == previous
            assert not (root / "result").exists()
        else:
            assert probe(binary, "verify", root, operation, "success", "checkpoint-first")["status"] == "verified"
        return outcome, errors[0]

def check_retained_scan(binary, directory, fmt):
    with tempfile.TemporaryDirectory(prefix="retained-scan-", dir=directory) as temporary:
        root = pathlib.Path(temporary).resolve()
        assert probe(binary, "setup", root, fmt)["status"] == "ready"
        ready = probe(binary, "prepare-retained-scan", root)
        assert ready["status"] == "ready"
        header = ready["header_bytes"]
        catalog = root / "scan-catalog"
        log = catalog / "generation-a/records.log"
        base = catalog / "generation-a/base"
        witness = catalog / "generation-a/commit.state"
        selection = catalog / "active.retained"
        old = (root / "scan-witness-before").read_bytes()
        complete = witness.read_bytes()
        log_bytes, selection_bytes = log.read_bytes(), selection.read_bytes()
        assert old != complete
        base_bytes = base.stat().st_size
        suffix_bytes = len(log_bytes) - header
        assert suffix_bytes > 0
        trace = root / "scan.trace"
        command = ["strace", "-qq", "-y", "-o", trace, "-e", "trace=read,pread64"]
        for combined in (False, True):
            witness.write_bytes(old)
            base_cost = base_bytes if combined else 0
            operation = "attempt-retained-combined-scan" if combined else "attempt-retained-scan"
            name = "retained recovery scan" if combined else "retained recovery suffix scan"
            checks = []
            for scans, accepted in ((2, False), (2, True), (1, False), (1, True)):
                cap = base_cost + scans * suffix_bytes - int(not accepted)
                previous = witness.read_bytes()
                outcome = probe(*command, binary, operation, root, str(cap))
                lines = trace.read_text().splitlines()
                suffix_reads = []
                base_reads = []
                for line in lines:
                    if line.startswith("pread64(") and f"<{log}>" in line:
                        chunks = re.search(r", (\d+), (\d+)\)\s+= (\d+)$", line)
                        assert chunks, line
                        requested, offset, returned = map(int, chunks.groups())
                        assert requested == returned, line
                        if offset >= header:
                            suffix_reads.append(returned)
                    if line.startswith("read(") and f"<{base}>" in line:
                        size = re.search(r", \d+\)\s+= (\d+)$", line)
                        assert size, line
                        base_reads.append(int(size[1]))
                assert sum(suffix_reads) == (scans * suffix_bytes if accepted else 0), suffix_reads
                assert sum(base_reads) == (base_bytes if accepted else 0), base_reads
                if accepted:
                    assert outcome == {"status": "success"}, outcome
                    assert witness.read_bytes() == complete
                    if combined:
                        assert sum(suffix_reads) + sum(base_reads) == cap
                else:
                    assert outcome == {"status": "error", "kind": "InvalidInput",
                                       "message": f"{name} byte limit exceeded"}, outcome
                    assert witness.read_bytes() == previous
                    assert not any(f"<{base}>" in line for line in lines), lines
                assert log.read_bytes() == log_bytes and selection.read_bytes() == selection_bytes
                checks.append(dict(cap=cap, outcome=outcome, suffix_reads=suffix_reads,
                                   base_reads=base_reads, trace=lines))
            assert probe(binary, "verify-retained-scan", root) == {"status": "verified"}
            print(json.dumps(dict(rebuild="retained-managed", format=fmt, point=f"{name} limit",
                                  base_bytes=base_bytes, suffix_bytes=suffix_bytes,
                                  checks=checks)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--directory", type=pathlib.Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    for rebuild in ("ordinary", "full-bounded", "suffix-bounded", "full-log-open",
                    "managed-checkpoint-open", "explicit-checkpoint-open"):
        for format_name in ("v2", "v3"):
            points = run(binary, args.directory, format_name, rebuild)
            for point, injection in points.items():
                outcome, trace = run(binary, args.directory, format_name, rebuild, (point, injection))
                print(json.dumps(dict(rebuild=rebuild, format=format_name, point=point,
                                      injection=injection, outcome=outcome, trace=trace)), flush=True)
    for format_name in ("v2", "v3"):
        check_retained_scan(binary, args.directory, format_name)


if __name__ == "__main__":
    main()
