import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from checkpoint_lookup_compare import percentile


class CheckpointLookupComparisonTests(unittest.TestCase):
    def test_nearest_rank_percentiles_use_sorted_samples(self):
        values = [9, 1, 7, 3, 5]
        self.assertEqual(percentile(values, 50), 5)
        self.assertEqual(percentile(values, 95), 9)
        self.assertEqual(percentile(values, 100), 9)


if __name__ == '__main__':
    unittest.main()
