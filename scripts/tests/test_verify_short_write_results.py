import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_short_write_results import expected_cases, receipts_count, verify, verify_case


class VerifyShortWriteTests(unittest.TestCase):
    def good_case(self):
        fixture = {'backup_sha256': 'a', 'checkpoint_sha256': 'b',
                   'checkpoint_digest_sha256': 'c', 'backup_bytes': 100,
                   'witness_sha256': 'd', 'metadata_sha256': None, 'log_bytes': 10}
        return {'format': 'v2', 'operation': 'write', 'target': 'records.log',
                'cap': 1, 'mode': 'success', 'order': 'ordinary-first',
                'events': [{'call': 'write', 'requested': 4, 'result': 1, 'errno': 0},
                           {'call': 'write', 'requested': 3, 'result': 3, 'errno': 0}],
                'outcome': {'status': 'success', 'operation': 'write'}, 'recovery': 'reopened',
                'verification': {'status': 'verified', 'new_present': True,
                                 'recovery_order': ['ordinary', 'checkpoint']},
                'before': fixture, 'after': copy.deepcopy(fixture),
                'after_verification': copy.deepcopy(fixture)}

    def test_accepts_valid_case_and_expected_matrix(self):
        self.assertEqual(verify_case(self.good_case()), 0)
        self.assertEqual(len(expected_cases()), 224)

    def test_rejects_unchecked_or_damaged_evidence(self):
        for change in ('missing_short', 'lost_backup', 'missing_value', 'wrong_order'):
            case = self.good_case()
            if change == 'missing_short':
                case['events'] = [case['events'][1]]
            elif change == 'lost_backup':
                case['after']['backup_sha256'] = 'changed'
            elif change == 'missing_value':
                case['verification']['new_present'] = False
            else:
                case['verification']['recovery_order'].reverse()
            with self.subTest(change=change), self.assertRaises(ValueError):
                verify_case(case)

    def test_rejects_incomplete_report_and_invalid_receipts(self):
        with self.assertRaises(ValueError):
            verify({'completed': True, 'failure': None, 'current': None, 'cases': []})
        workers = [{'id': i, 'acknowledged': list(range(64)), 'failed': None, 'error': None}
                   for i in range(4)]
        self.assertEqual(receipts_count({'workers': workers}, 'success'), 256)
        workers[0]['acknowledged'].pop()
        with self.assertRaises(ValueError):
            receipts_count({'workers': workers}, 'success')


if __name__ == '__main__':
    unittest.main()
