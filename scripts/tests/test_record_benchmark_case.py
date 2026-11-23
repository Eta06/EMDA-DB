from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import signal
import sys
import tempfile
import unittest

from scripts.benchmarks.record_case import record_case


class RecordBenchmarkCaseTests(unittest.TestCase):
    def test_preserves_output_and_nonzero_exit_without_overwriting(self):
        with tempfile.TemporaryDirectory() as scratch, redirect_stdout(io.StringIO()):
            directory = Path(scratch) / "comparison"
            command = [sys.executable, "-c",
                       "import sys; print('partial timing', flush=True); "
                       "print('verification failure', file=sys.stderr); sys.exit(7)"]
            self.assertEqual(record_case(directory, "test comparison", command), 7)
            metadata = json.loads((directory / "attempt.json").read_text())
            self.assertEqual(metadata["command"], command)
            self.assertEqual(metadata["status"], "exited")
            self.assertEqual(metadata["returncode"], 7)
            self.assertIn("started_at", metadata)
            self.assertIn("finished_at", metadata)
            output = (directory / "command.log").read_text()
            self.assertEqual(output, "partial timing\nverification failure\n")
            with self.assertRaises(FileExistsError):
                record_case(directory, "reused directory", command)
            self.assertEqual((directory / "command.log").read_text(), output)

    def test_preserves_signal_failure_and_launch_error(self):
        with tempfile.TemporaryDirectory() as scratch, redirect_stdout(io.StringIO()):
            root = Path(scratch)
            command = [sys.executable, "-c",
                       "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"]
            self.assertEqual(record_case(root / "signal", "signal", command), 128 + signal.SIGTERM)
            metadata = json.loads((root / "signal" / "attempt.json").read_text())
            self.assertEqual(metadata["status"], "signalled")
            self.assertEqual(metadata["returncode"], -signal.SIGTERM)

            self.assertEqual(record_case(root / "missing", "missing", [str(root / "no-tool")]), 1)
            metadata = json.loads((root / "missing" / "attempt.json").read_text())
            self.assertEqual(metadata["status"], "launch_failed")
            self.assertIn("no-tool", metadata["error"])


if __name__ == "__main__":
    unittest.main()
