import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from document_scale_compare import OPERATIONS, targets_for_size, validate_result


class ScaleComparisonTests(unittest.TestCase):
    def result(self):
        return {"engine": "document-prototype", "preloaded_documents": 100, "cycles": 2,
                "reopen_verified": True, "wall_ops_per_sec": 1.0, "log_format": "V3",
                "latency": {name: {"count": 2, "samples_ns": [1, 2]}
                                                        for name in OPERATIONS}}

    def test_accepts_complete_validated_samples(self):
        validate_result(self.result(), "prototype", 100, 2)

    def test_requires_requested_query_mode(self):
        result = self.result()
        result["query_mode"] = "planned"
        validate_result(result, "prototype-planned", 100, 2)
        with self.assertRaises(ValueError):
            validate_result(result, "prototype-scan", 100, 2)
        result["query_mode"] = "index"
        validate_result(result, "prototype-index", 100, 2)

    def test_plan_comparison_uses_old_planned_binary_and_omits_oversized_scan(self):
        small = targets_for_size(100, "new", "old", True)
        large = targets_for_size(3000, "new", "old", True)
        self.assertEqual(small[0], ("scan", "prototype-scan", "new"))
        self.assertIn(("before-planned", "prototype-planned", "old"), small)
        self.assertIn(("before-planned", "prototype-planned", "old"), large)
        self.assertFalse(any(entry[0] == "scan" for entry in large))

    def test_rejects_failed_reopen_or_missing_samples(self):
        for change in ("reopen", "samples", "size", "invalid_sample", "rate", "format"):
            result = self.result()
            if change == "reopen":
                result["reopen_verified"] = False
            elif change == "samples":
                result["latency"]["read"]["samples_ns"].pop()
            elif change == "size":
                result["preloaded_documents"] = 0
            elif change == "invalid_sample":
                result["latency"]["read"]["samples_ns"][0] = True
            elif change == "rate":
                result["wall_ops_per_sec"] = float("nan")
            else:
                result["log_format"] = "unknown"
            with self.assertRaises(ValueError):
                validate_result(result, "prototype", 100, 2)
