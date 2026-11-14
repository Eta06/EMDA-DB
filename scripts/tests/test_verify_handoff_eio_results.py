import copy
import hashlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_handoff_eio_results import verify


def fixture():
    original = {'log_sha256': 'a' * 64, 'witness_sha256': 'b' * 64,
                'metadata_sha256': None, 'selection_sha256': None, 'automatic_owner_sha256': None}
    candidate = {**original, 'log_sha256': 'c' * 64}
    external = {name + '_sha256': 'd' * 64 for name in
                ('reference-backup', 'checkpoint', 'checkpoint.digest', 'original-log.sha256')}
    external['original-log.sha256_sha256'] = hashlib.sha256(bytes.fromhex(original['log_sha256'])).hexdigest()
    external['managed-base_sha256'] = None

    def probe(order, success):
        return {'outcome': {'operation': 'ordinary', 'status': 'success' if success else 'error',
                            'classification': None if success else 'candidate_unknown'},
                'verification': {'status': 'verified', 'operation': 'ordinary', 'order': order,
                                 'target_visible': True, 'source_original': not success, 'target_original': success},
                'original': original.copy(), 'source_after': candidate.copy() if success else original.copy(),
                'target_after': original.copy() if success else candidate.copy(),
                'source_after_verification': candidate.copy() if success else original.copy(),
                'target_after_verification': original.copy() if success else candidate.copy(),
                'external_before': external.copy(), 'external_after': external.copy(),
                'external_after_verification': external.copy(),
                'stages_after_cleanup': {'root': [], 'source': [], 'target': []},
                'injected_traces': [] if success else ['1 fsync(3</tmp/a>) = -1 EIO (INJECTED)']}

    control = probe('source-first', True)
    control.update(counts={'fsync': 1, 'fdatasync': 0}, eligible_ordinals={'fsync': [1], 'fdatasync': []})
    injections = []
    for order in ('source-first', 'target-first'):
        item = probe(order, False)
        item.update(syscall='fsync', ordinal=1, order=order)
        injections.append(item)
    return {'completed': True, 'failure': None, 'current': None, 'syscall_set': 'sync',
            'fixture_paths_only': False, 'injected_error': 'EIO', 'formats': ['v2'],
            'backends': ['ordinary'], 'max_ordinals': 0,
            'cases': [{'format': 'v2', 'backend': 'ordinary', 'control': control, 'injections': injections}]}


class HandoffCoverageTests(unittest.TestCase):
    def test_checks_both_recovery_orders_and_classifications(self):
        report = fixture()
        self.assertEqual(verify(report)['injections'], 2)
        broken = copy.deepcopy(report)
        broken['cases'][0]['injections'].pop()
        with self.assertRaisesRegex(ValueError, 'missing or duplicate'):
            verify(broken)
        broken = copy.deepcopy(report)
        broken['cases'][0]['injections'][0]['outcome']['classification'] = 'not_published'
        with self.assertRaisesRegex(ValueError, 'before publication'):
            verify(broken)

    def test_rejects_changed_original_reference_or_injected_errno(self):
        for field in ('source', 'backup', 'errno'):
            broken = fixture()
            value = broken['cases'][0]['injections'][0]
            if field == 'source':
                value['source_after']['log_sha256'] = 'e' * 64
                message = 'original handoff evidence differs'
            elif field == 'backup':
                value['external_after']['reference-backup_sha256'] = 'e' * 64
                message = 'reference backup or checkpoint changed'
            else:
                value['injected_traces'][0] = '1 fsync(3</tmp/a>) = -1 EAGAIN (INJECTED)'
                message = 'syscall or errno differs'
            with self.assertRaisesRegex(ValueError, message):
                verify(broken)

    def test_exchange_uncertainty_allows_both_orders_but_requires_cleanup(self):
        report = fixture()
        first, second = report['cases'][0]['injections']
        first['outcome']['classification'] = 'exchange_unknown'
        second['outcome']['classification'] = 'exchange_unknown'
        second['verification'].update(source_original=False, target_original=True)
        second['source_after'], second['target_after'] = second['target_after'], second['source_after']
        second['source_after_verification'], second['target_after_verification'] = (
            second['target_after_verification'], second['source_after_verification'])
        self.assertEqual(verify(report)['injections'], 2)
        report['cases'][0]['injections'][1]['stages_after_cleanup']['source'] = ['leftover']
        with self.assertRaisesRegex(ValueError, 'staging cleanup incomplete'):
            verify(report)

    def test_online_exchange_uncertainty_requires_a_blocked_handle(self):
        report = fixture()
        report['backends'] = ['online']
        case = report['cases'][0]
        case['backend'] = 'online'
        for value in [case['control'], *case['injections']]:
            value['outcome']['operation'] = 'online'
            value['verification']['operation'] = 'online'
        injection = case['injections'][0]
        injection['outcome'].update(classification='exchange_unknown', handle_after_exchange_failure='blocked')
        self.assertEqual(verify(report)['injections'], 2)
        del injection['outcome']['handle_after_exchange_failure']
        with self.assertRaisesRegex(ValueError, 'did not block online handle access'):
            verify(report)


if __name__ == '__main__':
    unittest.main()
