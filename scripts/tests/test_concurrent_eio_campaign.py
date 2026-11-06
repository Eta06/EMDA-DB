import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from concurrent_eio_campaign import check_recovery, targets_log_sync, targets_witness, validate_receipts


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

    def test_interruption_can_be_retried_without_losing_receipt_validation(self):
        report = {'workers': [{'id': i, 'acknowledged': list(range(64)),
                               'failed': None, 'error': None} for i in range(4)]}
        validate_receipts(report, require_failed=False)
        report['workers'][1]['acknowledged'].pop()
        with self.assertRaises(ValueError):
            validate_receipts(report, require_failed=False)

    def test_recovery_must_agree_on_which_uncertain_write_was_recovered(self):
        verified = [{'status': 'verified', 'backend': 'ordinary', 'acknowledged': 3,
                     'uncertain_present': [[0, 3]]},
                    {'status': 'verified', 'backend': 'checkpoint', 'acknowledged': 3,
                     'uncertain_present': [[1, 3]]}]
        with self.assertRaises(ValueError):
            check_recovery(verified, ('ordinary', 'checkpoint'))
        verified[1]['uncertain_present'] = [[0, 3]]
        check_recovery(verified, ('ordinary', 'checkpoint'))
        for item in verified:
            item['mode'] = 'transaction'
        check_recovery(verified, ('ordinary', 'checkpoint'), 'transaction')
        verified[1]['mode'] = 'single'
        with self.assertRaises(ValueError):
            check_recovery(verified, ('ordinary', 'checkpoint'), 'transaction')

    def test_injection_must_target_the_expected_witness(self):
        root = Path('/work/case')
        v2 = '1 pwrite64(3</work/case/source/commit.state>, "x", 1, 0) = -1 EIO (INJECTED)'
        v3 = '1 fsetxattr(3</work/case/source/records.log>, "user.db.commit.v3", "x", 1, 0) = -1 EIO (INJECTED)'
        self.assertTrue(targets_witness(v2, root, 'v2'))
        self.assertTrue(targets_witness(v3, root, 'v3'))
        self.assertFalse(targets_witness(v2.replace('commit.state', 'other'), root, 'v2'))
        self.assertFalse(targets_witness(v3.replace('user.db.commit.v3', 'other'), root, 'v3'))

    def test_log_sync_must_match_format_and_source_log(self):
        root = Path('/work/case')
        v2 = '1 fdatasync(3</work/case/source/records.log>) = -1 EIO (INJECTED)'
        v3 = '1 fsync(3</work/case/source/records.log>) = -1 EIO (INJECTED)'
        self.assertTrue(targets_log_sync(v2, root, 'v2'))
        self.assertTrue(targets_log_sync(v3, root, 'v3'))
        self.assertFalse(targets_log_sync(v2, root, 'v3'))
        self.assertFalse(targets_log_sync(v3.replace('records.log', 'commit.state'), root, 'v3'))


if __name__ == '__main__':
    unittest.main()
