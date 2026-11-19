"""Run on macOS from the scratch directory holding the fixture and executables."""
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess
import tempfile

output = Path("measurement")
output.mkdir()


def file_hash(path):
    with path.open("rb") as stream:
        result = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
        return result.hexdigest()


def manifest():
    directories = ["fixture", "fixture-reopen"]
    return {str(path): file_hash(path)
            for directory in directories
            for path in sorted(Path(directory).rglob("*")) if path.is_file()}


def record(name, command):
    with (output / (name + ".log")).open("w") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)


inputs = manifest()
(output / "input-before.json").write_text(json.dumps(inputs, indent=2) + "\n")
restores = ["materialized", "portable", "arm"]
reopens = ["reopen-portable", "reopen-arm"]
for run in range(5):
    for offset in range(3):
        engine = restores[(run + offset) % 3]
        with tempfile.TemporaryDirectory(prefix="restore-", dir=output) as stage:
            destination = str(Path(stage) / "restored")
            record(f"restore-{run}-{engine}",
                   ["/usr/bin/time", "-l", f"./{engine}", "restore", "fixture", destination])
            record(f"verified-{run}-{engine}",
                   ["./portable", "verify", "fixture", destination])
    for offset in range(2):
        engine = reopens[(run + offset) % 2]
        record(f"open-{run}-{engine}",
               [f"./{engine}", "reopen", "fixture-reopen/catalog"])
final = manifest()
(output / "input-after.json").write_text(json.dumps(final, indent=2) + "\n")
assert inputs == final
summary = {}
for engine in restores + reopens:
    restore = engine in restores
    phase = "restore" if restore else "open"
    rows = [(output / f"{phase}-{run}-{engine}.log").read_text() for run in range(5)]
    timings = [int(re.search(rf"{phase}_ns=(\d+)", row)[1]) for row in rows]
    entry = {"elapsed_ns": timings, "median_elapsed_ns": statistics.median(timings)}
    if restore:
        memory = [int(re.search(r"(\d+)\s+maximum resident set size", row)[1]) for row in rows]
        entry.update(peak_rss_bytes=memory, median_peak_rss_bytes=statistics.median(memory))
    summary[engine] = entry
report = json.dumps(summary, indent=2)
(output / "summary.json").write_text(report + "\n")
print(report)
