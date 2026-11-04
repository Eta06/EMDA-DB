import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from document_compare import execute_cycles, latency


class DocumentComparisonTests(unittest.TestCase):
    def test_percentiles_use_nearest_rank(self):
        self.assertEqual(latency([9000, 1000, 3000, 5000])["p50_us"], 3.0)
        self.assertEqual(latency([9000, 1000, 3000, 5000])["p99_us"], 9.0)

    def test_workload_verifies_each_result_and_leaves_no_documents(self):
        values = {}
        def create(identifier, body):
            values[identifier] = body.copy()
        def get(identifier):
            return values.get(identifier)
        def update(identifier, body):
            values[identifier] = body.copy()
            return 1
        def lookup(email):
            return next(((key, value) for key, value in values.items() if value["email"] == email), None)
        def delete(identifier):
            values.pop(identifier)
            return 1
        result = execute_cycles(3, create, get, update, lookup, delete)
        self.assertEqual(result["cycles"], 3)
        self.assertEqual(result["warmup_cycles"], 20)
        self.assertTrue(all(item["count"] == 3 for item in result["latency"].values()))
        self.assertEqual(values, {})

    def test_invalid_read_is_a_failed_run(self):
        with self.assertRaisesRegex(RuntimeError, "read validation"):
            execute_cycles(1, lambda *_: None, lambda *_: None, lambda *_: 1,
                           lambda *_: None, lambda *_: 1)


if __name__ == "__main__":
    unittest.main()
