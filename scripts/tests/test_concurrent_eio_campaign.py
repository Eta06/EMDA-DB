import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from concurrent_eio_campaign import validate_receipts


class ConcurrentEioCampaignTests(unittest.TestCase):
    def test_rejects_missing_error_and_noncontiguous_receipts(self):
        report = {'workers': [{'id': i, 'acknowledged': [0, 1],
                               'failed': 2, 'error': 'synthetic EIO'} for i in range(4)]}
        validate_receipts(report)
        missing = copy.deepcopy(report)
        missing['workers'][0]['acknowledged'] = [0, 2]
        with self.assertRaises(ValueError):
            validate_receipts(missing)
        invalid = copy.deepcopy(report)
        invalid['workers'][0]['failed'] = 3
        with self.assertRaises(ValueError):
            validate_receipts(invalid)
        no_errors = {'workers': [{'id': i, 'acknowledged': list(range(64)),
                                  'failed': None, 'error': None} for i in range(4)]}
        with self.assertRaises(ValueError):
            validate_receipts(no_errors)


if __name__ == '__main__':
    unittest.main()
