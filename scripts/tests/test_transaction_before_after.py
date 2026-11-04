import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from transaction_before_after import parse_result, percentile


class TransactionBeforeAfterTests(unittest.TestCase):
    def test_parses_verified_workload_and_nearest_rank_samples(self):
        output = "backend=readset version=0.1.0 clients=2 operations=2 workload=shared elapsed_ns=20 conflicts=1,2 samples_ns=2,4,6,8"
        result = parse_result(output, "readset", 2, 2, "shared")
        self.assertEqual(result["conflicts_per_client"], [1, 2])
        self.assertEqual((result["p50_ns"], result["p95_ns"], result["p99_ns"]), (4, 8, 8))
        self.assertEqual(result["operations_per_second"], 200_000_000)
        self.assertEqual(percentile([9, 1, 5], .50), 5)

    def test_rejects_wrong_identity_and_incomplete_or_negative_samples(self):
        output = "backend=readset version=0.1.0 clients=2 operations=2 workload=shared elapsed_ns=20 conflicts=1,2 samples_ns=2,4,6,8"
        for changed in (output.replace("backend=readset", "backend=checkpoint"),
                        output.replace("workload=shared", "workload=disjoint"),
                        output.replace("samples_ns=2,4,6,8", "samples_ns=2,4,6"),
                        output.replace("conflicts=1,2", "conflicts=1,-1"),
                        output.replace("elapsed_ns=20", "elapsed_ns=0")):
            with self.assertRaises(ValueError):
                parse_result(changed, "readset", 2, 2, "shared")


if __name__ == "__main__":
    unittest.main()
