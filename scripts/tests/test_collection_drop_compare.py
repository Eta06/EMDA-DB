import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collection_drop_compare import validate


class CollectionDropComparisonTests(unittest.TestCase):
    def sample(self):
        return {"mode": "checkpoint", "documents": 2, "removed": 6,
                "log_format": "EMDALOG3", "recovery_verified": True,
                "log_bytes_before": 10, "log_bytes_after": 20,
                "samples": [{"removed": 6, "complete": False, "ns": 10},
                            {"removed": 0, "complete": True, "ns": 8}]}

    def test_accepts_verified_complete_history(self):
        validate(self.sample(), "checkpoint", 2)

    def test_rejects_invalid_results_and_timings(self):
        for invalid in ("recovery", "format", "count", "completion", "time", "log"):
            sample = self.sample()
            if invalid == "recovery":
                sample["recovery_verified"] = False
            elif invalid == "format":
                sample["log_format"] = "unwitnessed"
            elif invalid == "count":
                sample["samples"][0]["removed"] = 5
            elif invalid == "completion":
                sample["samples"][0]["complete"] = True
            elif invalid == "time":
                sample["samples"][0]["ns"] = -1
            else:
                sample["log_bytes_after"] = sample["log_bytes_before"]
            with self.assertRaises(ValueError):
                validate(sample, "checkpoint", 2)


if __name__ == "__main__":
    unittest.main()
