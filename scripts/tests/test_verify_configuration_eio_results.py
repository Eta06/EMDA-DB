import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configuration_eio_campaign import FILES
from verify_configuration_eio_results import verify


def fixture():
    original = {name: 'a' * 64 for name in FILES}
    original['v3_metadata'] = None

    def probe(order, success):
        return {'outcome': {'operation': 'initial-enable', 'status': 'success' if success else 'error',
                            'classification': None if success else 'unknown', 'handle': 'opened' if success else 'blocked'},
                'verification': {'status': 'verified', 'operation': 'initial-enable', 'order': order,
                                 'enabled': True, 'backup_restored': True},
                'marker_before': 'absent', 'marker_after': 'enabled',
                'before': original.copy(), 'after_attempt': original.copy(), 'after_verification': original.copy(),
                'stages_after_cleanup': {'root': [], 'source': [], 'configuration': []},
                'injected_traces': [] if success else ['1 fsync(3</tmp/a>) = -1 EIO (INJECTED)']}

    control = probe('ordinary-first', True)
    control.update(counts={'fsync': 1, 'fdatasync': 0}, eligible_ordinals={'fsync': [1], 'fdatasync': []})
    injections = []
    for order in ('ordinary-first', 'managed-first'):
        case = probe(order, False)
        case.update(syscall='fsync', ordinal=1, order=order)
        injections.append(case)
    return {'completed': True, 'failure': None, 'current': None, 'syscall_set': 'sync',
            'fixture_paths_only': False, 'injected_error': 'EIO', 'formats': ['v2'],
            'operations': ['initial-enable'], 'max_ordinals': 0,
            'cases': [{'format': 'v2', 'operation': 'initial-enable', 'control': control, 'injections': injections}]}


class ConfigurationCoverageTests(unittest.TestCase):
    def test_requires_both_recovery_orders(self):
        report = fixture()
        self.assertEqual(verify(report)['injections'], 2)
        report['cases'][0]['injections'].pop()
        with self.assertRaisesRegex(ValueError, 'missing or duplicate'):
            verify(report)

    def test_rejects_unsafe_classifications(self):
        report = fixture()
        case = report['cases'][0]['injections'][0]
        case['outcome']['handle'] = 'readable'
        with self.assertRaisesRegex(ValueError, 'blocked handle'):
            verify(report)
        case['outcome'].update(classification='not_published', handle='blocked')
        with self.assertRaisesRegex(ValueError, 'changed state'):
            verify(report)

    def test_rejects_changed_backup_wrong_errno_and_leftover_stage(self):
        for field, message in [('backup', 'retained evidence changed'), ('errno', 'syscall or errno differs'),
                               ('staging', 'staging cleanup incomplete')]:
            broken = copy.deepcopy(fixture())
            case = broken['cases'][0]['injections'][0]
            if field == 'backup':
                case['after_attempt']['reference-backup'] = 'b' * 64
            elif field == 'errno':
                case['injected_traces'][0] = '1 fsync(3</tmp/a>) = -1 EAGAIN (INJECTED)'
            else:
                case['stages_after_cleanup']['configuration'] = ['leftover']
            with self.assertRaisesRegex(ValueError, message):
                verify(broken)


if __name__ == '__main__':
    unittest.main()
