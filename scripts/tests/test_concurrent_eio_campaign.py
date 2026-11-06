import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from concurrent_eio_campaign import check_recovery, validate_receipts


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

    def test_recovery_must_agree_on_which_uncertain_write_was_recovered(self):
        verified = [{'status': 'verified', 'backend': 'ordinary', 'acknowledged': 3,
                     'uncertain_present': [[0, 3]]},
                    {'status': 'verified', 'backend': 'checkpoint', 'acknowledged': 3,
                     'uncertain_present': [[1, 3]]}]
        with self.assertRaises(ValueError):
            check_recovery(verified, ('ordinary', 'checkpoint'))
        verified[1]['uncertain_present'] = [[0, 3]]
        check_recovery(verified, ('ordinary', 'checkpoint'))


if __name__ == '__main__':
    unittest.main()
