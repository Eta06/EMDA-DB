import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from aggregate_stats_compare import validate


class AggregateStatsCompareTests(unittest.TestCase):
    def test_accepts_matching_verified_full_scan(self):
        result = {"documents": 16, "cycles": 2, "prototype_reopen_verified": True,
                  "groups": 8, "sqlite_journal": "wal", "sqlite_synchronous": 2,
                  "sqlite_query_plan": "SCAN docs; USE TEMP B-TREE FOR GROUP BY",
                  "sqlite_version": "3.0", "samples_ns": {"prototype": [1, 2], "sqlite": [3, 4]}}
        validate(result, 16, 2)
        for change in ({"groups": 7}, {"sqlite_journal": "delete"},
                       {"prototype_reopen_verified": False}, {"sqlite_query_plan": "SEARCH docs"},
                       {"samples_ns": {"prototype": [1, -2], "sqlite": [3, 4]}},
                       {"samples_ns": {"prototype": [1], "sqlite": [3, 4]}}):
            bad = copy.deepcopy(result)
            bad.update(change)
            with self.assertRaises(ValueError):
                validate(bad, 16, 2)


if __name__ == "__main__":
    unittest.main()
