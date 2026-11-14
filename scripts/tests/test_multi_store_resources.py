import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from multi_store_resources import validate


def sample():
    return {'status': 'verified', 'mode': 'ordinary', 'projects': 2, 'keys_per_project': 5,
            'verified_reads': 6, 'rss_before_kib': 100, 'rss_open_kib': 120,
            'rss_after_reads_kib': 130, 'rss_after_drop_kib': 110,
            'fds_before': 4, 'fds_open': 8, 'fds_after_drop': 4,
            'open_total_ns': 100, 'open_samples_ns': [40, 60],
            'lookup_total_ns': 50, 'read_bytes': 0, 'rchar': 100}


class MultiStoreResultsTests(unittest.TestCase):
    def test_requires_each_store_to_have_expected_reads_and_resource_samples(self):
        good = sample()
        self.assertEqual(validate(good, 'ordinary', 2, 5), good)
        for key, value in [('verified_reads', 5), ('open_samples_ns', [40]),
                           ('fds_open', 3), ('fds_after_drop', 5),
                           ('rss_open_kib', -1), ('open_total_ns', True)]:
            broken = copy.deepcopy(good)
            broken[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(broken, 'ordinary', 2, 5)


if __name__ == '__main__':
    unittest.main()
