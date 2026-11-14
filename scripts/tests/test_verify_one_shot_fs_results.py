import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_one_shot_fs_results import verify


def probe(order, injected):
    state = {name + '_sha256': 'a' * 64 for name in
             ('log', 'witness', 'backup', 'checkpoint', 'checkpoint_digest')}
    state.update(metadata_sha256=None, log_bytes=12, backup_bytes=20)
    return {'outcome': {'operation': 'write', 'status': 'success'},
            'verification': {'operation': 'write', 'status': 'verified', 'recovery_order': order},
            'before': state.copy(), 'after_attempt': state.copy(), 'after_verification': state.copy(),
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
        for operation in ('checkpoint', 'checkpoint-copy', 'compaction', 'checkpoint-compaction', 'upgrade', 'checkpoint-rollover'):
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
        broken = copy.deepcopy(report)
        broken['cases'][0]['injections'][0]['after_attempt']['backup_sha256'] = 'b' * 64
        with self.assertRaisesRegex(ValueError, 'copy changed source'):
            verify(broken)

    def test_legacy_reports_require_unchanged_source_digest(self):
        for operation, size in [('upgrade-legacy', 32), ('upgrade-empty-legacy', 0)]:
            report = fixture()
            report['publication_outcomes'] = True
            report['operations'] = [operation]
            case = report['cases'][0]
            case['operation'] = operation
            for value in [case['control'], *case['injections']]:
                value['outcome'].update(operation=operation, classification=None)
                value['verification'].update(operation=operation, target_visible=True)
                state = {'bytes': size, 'log_sha256': 'a' * 64,
                         'witness_sha256': None, 'metadata_sha256': None}
                value['legacy_source'] = {'before': state.copy(), 'after_attempt': state.copy(),
                                          'after_verification': state.copy()}
            self.assertEqual(verify(report)['injections'], 2)
            report['cases'][0]['injections'][0]['legacy_source']['after_attempt']['log_sha256'] = 'b' * 64
            with self.assertRaisesRegex(ValueError, 'legacy source log or witness changed'):
                verify(report)
            report['cases'][0]['injections'][0]['legacy_source']['after_attempt'] = state.copy()
            report['cases'][0]['injections'][0]['legacy_source']['after_verification']['metadata_sha256'] = 'c' * 64
            with self.assertRaisesRegex(ValueError, 'legacy source log or witness changed'):
                verify(report)


if __name__ == '__main__':
    unittest.main()
