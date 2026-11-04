import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from online_reclaim_compare import validate


class ReclaimComparisonTests(unittest.TestCase):
    def result(self, mode):
        previous = 100 if mode == "retain" else 0
        return {"mode": mode, "log_format": "V3", "keys": 16, "live_keys": 14, "reopen_verified": True, "staging_clean": True,
                "source_log_bytes": 100, "compacted_log_bytes": 50,
                "previous_log_bytes": previous, "total_log_bytes_after": previous + 50,
                "elapsed_us": 1}

    def test_accepts_verified_retained_and_reclaimed_stores(self):
        for mode in ("retain", "reclaim"):
            validate(self.result(mode), mode, 16)

    def test_rejects_unverified_or_wrong_log_accounting(self):
        for field in ("reopen_verified", "staging_clean", "previous_log_bytes", "total_log_bytes_after", "elapsed_us"):
            result = self.result("reclaim")
            result[field] = False
            with self.assertRaises(ValueError):
                validate(result, "reclaim", 16)
