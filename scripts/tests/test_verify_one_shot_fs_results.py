import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_one_shot_fs_results import verify


def probe(order, injected):
    return {'outcome': {'operation': 'write', 'status': 'success'},
            'verification': {'operation': 'write', 'status': 'verified', 'recovery_order': order},
            'injected_traces': (['1 fsync(3</work>) = -1 EINTR (Interrupted system call) (INJECTED)']
                                if injected else [])}


def fixture():
    control = probe(['ordinary', 'checkpoint'], False)
    control.update(counts={'fsync': 1, 'fdatasync': 0}, eligible_ordinals={'fsync': [1], 'fdatasync': []})
    injections = []
    for name, order in [('ordinary-first', ['ordinary', 'checkpoint']),
                        ('checkpoint-first', ['checkpoint', 'ordinary'])]:
        item = probe(order, True)
        item.update(syscall='fsync', ordinal=1, order=name)
        injections.append(item)
    return {'completed': True, 'failure': None, 'current': None, 'injection_mode': 'one-shot',
            'syscall_set': 'sync', 'fixture_paths_only': False, 'injected_error': 'EINTR',
            'formats': ['v3'], 'operations': ['write'], 'max_ordinals': 0,
            'cases': [{'format': 'v3', 'operation': 'write', 'control': control, 'injections': injections}]}


class OneShotCoverageTests(unittest.TestCase):
    def test_complete_report_checks_both_orders(self):
        self.assertEqual(verify(fixture()),
                         {'controls': 1, 'injections': 2, 'outcomes': {'success': 2}, 'errno': 'EINTR'})

    def test_missing_duplicate_and_unobserved_controls_are_rejected(self):
        original = fixture()
        broken = copy.deepcopy(original)
        broken['cases'][0]['injections'].pop()
        with self.assertRaisesRegex(ValueError, 'selected injection'):
            verify(broken)
        broken = copy.deepcopy(original)
        broken['cases'][0]['injections'][1]['order'] = 'ordinary-first'
        with self.assertRaisesRegex(ValueError, 'selected injection'):
            verify(broken)
        broken = copy.deepcopy(original)
        broken['cases'][0]['control']['eligible_ordinals']['fsync'] = []
        with self.assertRaisesRegex(ValueError, 'omitted an observed call'):
            verify(broken)

    def test_wrong_errno_and_incomplete_run_are_rejected(self):
        broken = fixture()
        broken['completed'] = False
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            verify(broken)
        broken = fixture()
        broken['cases'][0]['injections'][0]['injected_traces'][0] = '1 fsync(3</work>) = -1 EIO (INJECTED)'
        with self.assertRaisesRegex(ValueError, 'errno differs'):
            verify(broken)


if __name__ == '__main__':
    unittest.main()
