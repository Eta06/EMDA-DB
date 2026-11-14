import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_staging_cleanup_eio_results import FILE, expected_fixture, removable, verify


def fixture():
    before = expected_fixture()
    final = {path: value for path, value in before.items() if not removable(path)}
    control = {
        'before': before, 'after_attempt': final, 'after_cleanup': final,
        'outcome': {'status': 'success', 'operation': 'cleanup', 'classification': None,
                    'removed_payloads': 2, 'removed_markers': 2, 'skipped': 3},
        'resumed': {'status': 'success', 'operation': 'cleanup', 'classification': None,
                    'removed_payloads': 0, 'removed_markers': 0, 'skipped': 3},
        'injected_traces': [], 'counts': {'fsync': 1, 'fdatasync': 0},
        'eligible_ordinals': {'fsync': [1], 'fdatasync': []},
    }
    injected = copy.deepcopy(control)
    injected.update(syscall='fsync', ordinal=1, fixture='/tmp/fixture', after_attempt=copy.deepcopy(before))
    injected['outcome'] = {'status': 'error', 'operation': 'cleanup', 'classification': 'not_started'}
    injected['resumed'].update(removed_payloads=2, removed_markers=2)
    injected['injected_traces'] = ['123 fsync(3</tmp/fixture>) = -1 EIO (Input/output error) (INJECTED)']
    return {'completed': True, 'failure': None, 'current': None, 'syscall_set': 'sync',
            'injected_error': 'EIO', 'max_ordinals': 0, 'control': control, 'cases': [injected]}


class StagingCleanupVerifierTests(unittest.TestCase):
    def test_checks_not_started_and_unknown_partial_removal(self):
        result = fixture()
        self.assertEqual(verify(result)['outcomes'], {'not_started': 1})
        case = result['cases'][0]
        case['outcome']['classification'] = 'unknown'
        del case['after_attempt'][FILE]
        del case['after_attempt'][FILE + '.data']
        case['resumed'].update(removed_payloads=1, removed_markers=1)
        self.assertEqual(verify(result)['outcomes'], {'unknown': 1})
        case['outcome']['classification'] = 'not_started'
        with self.assertRaisesRegex(ValueError, 'not-started cleanup changed'):
            verify(result)

    def test_rejects_protected_loss_missing_injection_and_false_resume_counts(self):
        result = fixture()
        broken = copy.deepcopy(result)
        del broken['cases'][0]['after_attempt']['sentinel']
        with self.assertRaisesRegex(ValueError, 'protected fixture'):
            verify(broken)
        broken = copy.deepcopy(result)
        broken['cases'] = []
        with self.assertRaisesRegex(ValueError, 'missing or duplicate'):
            verify(broken)
        broken = copy.deepcopy(result)
        broken['cases'][0]['resumed']['removed_markers'] = 0
        with self.assertRaisesRegex(ValueError, 'resume removal counts'):
            verify(broken)


if __name__ == '__main__':
    unittest.main()
