"""Reuse the spill fault runner for source-log positional reads."""
import importlib.util
from pathlib import Path
import re

runner_path = (
    Path(__file__).resolve().parent.parent
    / "2026-11-19-bounded-spill-io-faults"
    / "spill_io_campaign.py"
)
spec = importlib.util.spec_from_file_location("spill_io_campaign", runner_path)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
runner.SPILL_CALLS = ("pread64",)
runner.RUN = re.compile(r"/store/(records\.log)>")


def prepublication_log_reads(lines, root):
    ordinal = 0
    targets = []
    destination = str(root / "result")
    for line in lines:
        call = runner.CALL.match(line)
        if not call:
            continue
        name = call[1]
        if (
            name in runner.PUBLICATION_CALLS
            and f'"{destination}"' in line
            and line.endswith("= 0")
        ):
            assert targets, "no source-log reads before publication"
            return targets
        if name == "pread64":
            ordinal += 1
            match = runner.RUN.search(line)
            if match and re.search(r"= [0-9]+$", line):
                targets.append({"syscall": name, "ordinal": ordinal, "run": match[1]})
    raise AssertionError("checkpoint publication missing from control trace")


runner.candidates = prepublication_log_reads
if __name__ == "__main__":
    runner.main()
