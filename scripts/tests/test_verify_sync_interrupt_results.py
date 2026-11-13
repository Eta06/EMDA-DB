import copy
import errno
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_sync_interrupt_results import expected_cases, verify, verify_case


class VerifyInterruptedSyncTests(unittest.TestCase):
    def good_case(self):
        fixture = {'backup_sha256': 'a', 'checkpoint_sha256': 'b',
                   'checkpoint_digest_sha256': 'c', 'backup_bytes': 10,
                   'witness_sha256': 'w', 'log_bytes': 100}
        after = copy.deepcopy(fixture)
        after['log_bytes'] = 200
        return {'format': 'v3', 'target': 'records.log', 'syscall': 'fsync',
                'operation': 'write', 'mode': 'error', 'order': 'ordinary-first',
                'events': [{'call': 'fsync', 'number': 1, 'actual': 0,
                            'reported': -1, 'errno': errno.EINTR},
                           {'call': 'fsync', 'number': 2, 'actual': -2,
                            'reported': -1, 'errno': errno.EIO}],
                'outcome': {'status': 'error', 'operation': 'write',
                            'read_blocked': True, 'write_blocked': True},
                'verification': {'status': 'verified', 'operation': 'write', 'new_present': True,
                                 'recovery_order': ['ordinary', 'checkpoint']},
                'before': fixture, 'after': after, 'after_verification': copy.deepcopy(after)}

    def test_accepts_case_and_expected_matrix(self):
        verify_case(self.good_case())
        self.assertEqual(len(expected_cases()), 48)

    def test_rejects_unchecked_retry_or_handle_and_changed_evidence(self):
        for change in ('first_not_synced', 'ignored_error', 'unblocked_handle', 'lost_backup'):
            case = self.good_case()
            if change == 'first_not_synced':
                case['events'][0]['actual'] = -2
            elif change == 'ignored_error':
                case['outcome']['status'] = 'success'
            elif change == 'unblocked_handle':
                case['outcome']['write_blocked'] = False
            else:
                case['after']['backup_sha256'] = 'changed'
            with self.subTest(change=change), self.assertRaises(ValueError):
                verify_case(case)

    def test_rejects_incomplete_matrix(self):
        with self.assertRaises(ValueError):
            verify({'completed': True, 'failure': None, 'current': None, 'cases': []})


if __name__ == '__main__':
    unittest.main()
