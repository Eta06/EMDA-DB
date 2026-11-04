import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compound_range_external_compare import documents, uses_index


class CompoundRangeExternalComparisonTests(unittest.TestCase):
    def test_documents_match_the_expected_compound_predicate(self):
        for total, count in ((16, 4), (1000, 10), (1000, 100)):
            rows = documents(total, count)
            selected = [(identifier, body) for identifier, body in rows
                        if body["tenant"] == "target" and body["number"] >= total - count]
            self.assertEqual(selected, rows[:count])
            self.assertEqual(len({identifier for identifier, _ in rows}), total)

    def test_index_plan_search_handles_nested_postgres_and_mongo_plans(self):
        postgres = [{"Plan": {"Node Type": "Sort", "Plans": [{"Node Type": "Index Scan", "Index Name": "wanted"}]}}]
        mongo = {"queryPlan": {"stage": "FETCH", "inputStage": {"stage": "IXSCAN", "indexName": "wanted"}}}
        for plan in (postgres, mongo):
            self.assertTrue(uses_index(plan, "wanted"))
            self.assertFalse(uses_index(plan, "other"))
        self.assertFalse(uses_index({"Node Type": "Seq Scan"}, "wanted"))
        self.assertFalse(uses_index({"Node Type": "Seq Scan", "Index Name": "wanted"}, "wanted"))
        self.assertFalse(uses_index({"stage": "COLLSCAN", "indexName": "wanted"}, "wanted"))
