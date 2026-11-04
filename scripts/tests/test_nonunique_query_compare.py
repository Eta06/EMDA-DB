import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from nonunique_query_compare import VARIANTS, parse_cases, validate


class NonuniqueQueryComparisonTests(unittest.TestCase):
    def result(self):
        return {"documents": 100, "matches": 10, "cycles": 2, "reopen_verified": True,
                "sqlite_journal": "wal", "sqlite_synchronous": 2,
                "sqlite_query_plan": "SEARCH docs USING INDEX docs_group (<expr>=?)",
                "sqlite_scan_plan": "SCAN docs", "sqlite_version": "3.0",
                "samples_ns": {variant: [10, 20] for variant in VARIANTS}}

    def test_accepts_only_bounded_distinct_cases(self):
        self.assertEqual(parse_cases("100:10,1000:100"), [(100, 10), (1000, 100)])
        for text in ("", "10", "10:0", "10:11", "1025:1", "10:1,10:1"):
            with self.assertRaises(ValueError):
                parse_cases(text)

    def test_accepts_complete_verified_result(self):
        validate(self.result(), 100, 10, 2)
        result = self.result()
        result["sqlite_query_plan"] = "SEARCH docs USING INDEX docs_number (<expr>>?)"
        validate(result, 100, 10, 2, "docs_number")
        with self.assertRaises(ValueError):
            validate(result, 100, 10, 2)

    def test_rejects_failed_verification_or_invalid_samples(self):
        for change in ("reopen", "journal", "synchronous", "plan", "scan_plan", "version", "missing", "boolean", "negative"):
            result = self.result()
            if change == "reopen":
                result["reopen_verified"] = False
            elif change == "journal":
                result["sqlite_journal"] = "delete"
            elif change == "synchronous":
                result["sqlite_synchronous"] = 1
            elif change == "plan":
                result["sqlite_query_plan"] = "SCAN docs"
            elif change == "scan_plan":
                result["sqlite_scan_plan"] = "SEARCH docs USING INDEX docs_group"
            elif change == "version":
                result["sqlite_version"] = ""
            elif change == "missing":
                result["samples_ns"]["prototype_planned"].pop()
            elif change == "boolean":
                result["samples_ns"]["sqlite_index"][0] = True
            else:
                result["samples_ns"]["sqlite_scan"][0] = -1
            with self.assertRaises(ValueError):
                validate(result, 100, 10, 2)
