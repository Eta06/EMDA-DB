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

    def test_eagain_requires_the_same_errno_in_every_injected_trace(self):
        report = fixture()
        report['injected_error'] = 'EAGAIN'
        for injection in report['cases'][0]['injections']:
            injection['injected_traces'][0] = '1 fsync(3</work>) = -1 EAGAIN (INJECTED)'
        self.assertEqual(verify(report)['errno'], 'EAGAIN')
        report['cases'][0]['injections'][1]['injected_traces'][0] = '1 fsync(3</work>) = -1 EIO (INJECTED)'
        with self.assertRaisesRegex(ValueError, 'errno differs'):
            verify(report)

    def test_wrong_errno_and_incomplete_run_are_rejected(self):
        broken = fixture()
        broken['completed'] = False
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            verify(broken)
        broken = fixture()
        broken['cases'][0]['injections'][0]['injected_traces'][0] = '1 fsync(3</work>) = -1 EIO (INJECTED)'
        with self.assertRaisesRegex(ValueError, 'errno differs'):
            verify(broken)

    def test_publication_classification_must_match_visible_destination(self):
        report = fixture()
        report['publication_outcomes'] = True
        report['operations'] = ['backup']
        case = report['cases'][0]
        case['operation'] = 'backup'
        for probe_value in [case['control'], *case['injections']]:
            probe_value['outcome']['operation'] = 'backup'
            probe_value['verification']['operation'] = 'backup'
        case['control']['outcome']['classification'] = None
        case['control']['verification']['target_visible'] = True
        for injection in case['injections']:
            injection['outcome'].update(status='error', classification='not_published')
            injection['verification']['target_visible'] = False
        self.assertEqual(verify(report)['injections'], 2)
        for operation in ('checkpoint', 'checkpoint-copy', 'compaction', 'checkpoint-compaction', 'upgrade'):
            checked = copy.deepcopy(report)
            checked['operations'] = [operation]
            checked['cases'][0]['operation'] = operation
            for value in [checked['cases'][0]['control'], *checked['cases'][0]['injections']]:
                value['outcome']['operation'] = operation
                value['verification']['operation'] = operation
            self.assertEqual(verify(checked)['injections'], 2)
        broken = copy.deepcopy(report)
        broken['cases'][0]['injections'][0]['verification']['target_visible'] = True
        with self.assertRaisesRegex(ValueError, 'not-published call left a visible destination'):
            verify(broken)
        broken = copy.deepcopy(report)
        del broken['cases'][0]['injections'][0]['outcome']['classification']
        with self.assertRaisesRegex(ValueError, 'missing publication classification'):
            verify(broken)


if __name__ == '__main__':
    unittest.main()
