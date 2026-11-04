import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from snapshot_contention_compare import validate


class ContentionComparisonTests(unittest.TestCase):
    def sample(self):
        return {"base_keys": 10, "writers": 2, "operations_per_writer": 2,
                "reopen_verified": True, "log_format": "V3", "elapsed_ns": 10,
                "writer_samples_ns": [1, 2, 3, 4], "reader_samples_ns": [1, 4],
                "reader_max_ns": 5, "reader_reads": 20, "reader_sample_cap": 20,
                "reader_sampling": "bounded reservoir"}

    def test_accepts_complete_recovered_workload(self):
        validate(self.sample(), 10, 2, 2)

    def test_rejects_incomplete_samples_and_failed_recovery(self):
        for invalid in ("missing", "maximum", "recovery", "format"):
            sample = self.sample()
            if invalid == "missing":
                sample["writer_samples_ns"].pop()
            elif invalid == "maximum":
                sample["reader_max_ns"] = 0
            elif invalid == "recovery":
                sample["reopen_verified"] = False
            else:
                sample["log_format"] = "unknown"
            with self.assertRaises(ValueError):
                validate(sample, 10, 2, 2)
