import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from recovery_before_after import parse


class RecoveryComparisonTests(unittest.TestCase):
    def output(self):
        return (
            'expected_records=3 repetitions=4 value_bytes=64 log_bytes=512 '
            'open_p50_us=2 open_p95_us=4 open_p99_us=4 open_max_us=4\n'
            'open_samples_us=[1,2,3,4]\n'
        )

    def test_accepts_sorted_samples_and_matching_percentiles(self):
        self.assertEqual(parse(self.output(), 3, 4, 512), {
            'samples_us': [1, 2, 3, 4], 'p50_us': 2,
            'p95_us': 4, 'p99_us': 4, 'max_us': 4,
        })

    def test_rejects_wrong_workload_unsorted_samples_and_bad_statistics(self):
        variants = [
            self.output().replace('expected_records=3', 'expected_records=2'),
            self.output().replace('repetitions=4', 'repetitions=3'),
            self.output().replace('[1,2,3,4]', '[2,1,3,4]'),
            self.output().replace('[1,2,3,4]', '[1,2,3,true]'),
            self.output().replace('open_p50_us=2', 'open_p50_us=3'),
            self.output().replace('open_p95_us=4', 'open_p95_us=3'),
            self.output().replace('open_p99_us=4', 'open_p99_us=3'),
            self.output().replace('open_max_us=4', 'open_max_us=3'),
        ]
        for output in variants:
            with self.subTest(output=output):
                with self.assertRaises(ValueError):
                    parse(output, 3, 4, 512)


if __name__ == '__main__':
    unittest.main()
