import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import compare_crashes


class CrashComparisonTests(unittest.TestCase):
    def test_retains_failed_round_and_continues_with_fresh_order(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            server = parent / "server"
            server.write_bytes(b"fixture")
            output = parent / "results.json"
            calls = []

            def check(backend, fixture_dir, binary):
                state = json.loads(output.read_text())
                calls.append(backend)
                self.assertEqual(state["current"], {"round": (len(calls) - 1) // 2 + 1, "backend": backend})
                self.assertFalse(state["completed"])
                if len(calls) == 2:
                    raise RuntimeError("injected test failure")
                return {"backend": backend, "result": "passed"}

            args = ["compare_crashes.py", "--directory", directory, "--output", str(output),
                    "--server", str(server), "--rounds", "2", "--backends", "prototype", "sqlite"]
            with patch.object(sys, "argv", args), patch.object(compare_crashes, "check", side_effect=check):
                with self.assertRaisesRegex(SystemExit, "One or more crash checks failed"):
                    compare_crashes.main()
            result = json.loads(output.read_text())
            self.assertTrue(result["completed"])
            self.assertIsNone(result["current"])
            self.assertEqual(calls, ["prototype", "sqlite", "sqlite", "prototype"])
            self.assertEqual([run["result"] for run in result["runs"]],
                             ["passed", "failed", "passed", "passed"])
            self.assertEqual(result["runs"][1]["error"], "injected test failure")

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            server = parent / "server"
            server.write_bytes(b"fixture")
            output = parent / "results.json"
            output.write_text("prior result")
            args = ["compare_crashes.py", "--directory", directory, "--output", str(output),
                    "--server", str(server), "--backends", "prototype"]
            with patch.object(sys, "argv", args), patch.object(compare_crashes, "check") as check:
                with self.assertRaises(SystemExit):
                    compare_crashes.main()
                check.assert_not_called()
            self.assertEqual(output.read_text(), "prior result")


if __name__ == "__main__":
    unittest.main()
