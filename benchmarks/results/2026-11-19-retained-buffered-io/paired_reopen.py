"""Measure fresh-process reopens of the same fixture with alternating binaries."""
import json
import os
from pathlib import Path
import subprocess
import sys

fixture, baseline, buffered = [str(Path(arg).resolve(strict=True)) for arg in sys.argv[1:]]
environment = dict(os.environ, EMDA_BENCH_SUFFIX_WORK="60000000")
binaries = {"baseline": baseline, "buffered": buffered}
selected = None
for pair in range(9):
    order = ["baseline", "buffered"] if pair % 2 == 0 else ["buffered", "baseline"]
    for build in order:
        result = subprocess.check_output(
            [binaries[build], "reopen", fixture, "retained", "131072", "8"],
            text=True, env=environment,
        ).splitlines()
        state, = (line for line in result if line.startswith("revision="))
        elapsed, = (int(line.split("=", 1)[1]) for line in result if line.startswith("open_ns="))
        if selected is None:
            selected = state
        assert state == selected, state
        print(json.dumps({"build": build, "pair": pair, "open_ns": elapsed,
                          "authority": state}), flush=True)
