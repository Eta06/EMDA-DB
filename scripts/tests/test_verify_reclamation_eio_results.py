import copy
import hashlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reclamation_eio_campaign import EXTERNAL
from verify_reclamation_eio_results import verify


def fixture():
    original = {'records.log': 'a' * 64, 'commit.state': 'b' * 64, 'store.lock': 'c' * 64,
                'v3_metadata': None, 'format': 'v2'}
    compacted = {**original, 'records.log': 'd' * 64}
    external = {name: 'e' * 64 for name in EXTERNAL}
    external['original-log.sha256'] = hashlib.sha256(bytes.fromhex(original['records.log'])).hexdigest()

    def probe(order, success):
        return {'outcome': {'operation': 'reclaim', 'status': 'success' if success else 'error',
                            'classification': None if success else 'reclamation', 'handle': 'opened' if success else 'readable'},
                'verification': {'status': 'verified', 'classification': 'success' if success else 'reclamation',
                                 'order': order, 'backup_restored': True},
                'original': original.copy(), 'source_after': compacted.copy(), 'previous_after': None,
                'source_after_verification': compacted.copy(), 'previous_after_verification': None,
                'external': external.copy(), 'external_after_verification': external.copy(),
                'stages_after': {}, 'stages_after_verification': {},
                'injected_traces': [] if success else ['1 fsync(3</tmp/a>) = -1 EIO (INJECTED)']}

    control = probe('source-first', True)
    control.update(counts={'fsync': 1, 'fdatasync': 0}, eligible_ordinals={'fsync': [1], 'fdatasync': []})
    injections = []
    for order in ('source-first', 'backup-first'):
        item = probe(order, False)
        item.update(syscall='fsync', ordinal=1, order=order)
        injections.append(item)
    return {'completed': True, 'failure': None, 'current': None, 'syscall_set': 'sync',
            'fixture_paths_only': False, 'injected_error': 'EIO', 'formats': ['v2'], 'max_ordinals': 0,
            'cases': [{'format': 'v2', 'control': control, 'injections': injections}]}


class ReclamationCoverageTests(unittest.TestCase):
    def test_checks_both_recovery_orders(self):
        report = fixture()
        self.assertEqual(verify(report)['injections'], 2)
        report['cases'][0]['injections'].pop()
        with self.assertRaisesRegex(ValueError, 'missing or duplicate'):
            verify(report)

    def test_rejects_invalid_handoff_classifications(self):
        report = fixture()
        case = report['cases'][0]['injections'][0]
        case['outcome']['handle'] = 'blocked'
        with self.assertRaisesRegex(ValueError, 'usable handoff'):
            verify(report)
        case['outcome'].update(classification='not_published', handle='opened')
        case['verification']['classification'] = 'not_published'
        with self.assertRaisesRegex(ValueError, 'changed paths'):
            verify(report)

    def test_checks_exchange_and_retained_evidence(self):
        report = fixture()
        case = report['cases'][0]['injections'][0]
        case['outcome'].update(classification='exchange_unknown', handle='blocked')
        case['verification']['classification'] = 'exchange_unknown'
        case['previous_after'] = case['original'].copy()
        case['previous_after_verification'] = case['original'].copy()
        self.assertEqual(verify(report)['injections'], 2)
        case['outcome']['handle'] = 'readable'
        with self.assertRaisesRegex(ValueError, 'block the handle'):
            verify(report)
        for field, message in [('backup', 'retained evidence'), ('errno', 'syscall or errno differs'),
                               ('stage', 'invalid staged path')]:
            broken = copy.deepcopy(fixture())
            case = broken['cases'][0]['injections'][0]
            if field == 'backup':
                case['external_after_verification']['reference-backup'] = 'f' * 64
            elif field == 'errno':
                case['injected_traces'][0] = '1 fsync(3</tmp/a>) = -1 EAGAIN (INJECTED)'
            else:
                case['stages_after'] = {'../escape': {'type': 'directory'}}
                case['stages_after_verification'] = copy.deepcopy(case['stages_after'])
            with self.assertRaisesRegex(ValueError, message):
                verify(broken)

    def test_partial_retired_copy_is_recorded_without_claiming_it_is_recoverable(self):
        report = fixture()
        case = report['cases'][0]['injections'][0]
        partial = {'records.log': None, 'commit.state': None, 'store.lock': 'c' * 64,
                   'v3_metadata': None, 'format': None}
        case['previous_after'] = partial
        case['previous_after_verification'] = partial.copy()
        self.assertEqual(verify(report)['injections'], 2)
        case['previous_after_verification']['store.lock'] = None
        with self.assertRaisesRegex(ValueError, 'retained evidence'):
            verify(report)

    def test_v3_evidence_requires_metadata_witness(self):
        report = fixture()
        case = report['cases'][0]['injections'][0]
        case['source_after']['format'] = 'v3'
        with self.assertRaisesRegex(ValueError, 'metadata digest'):
            verify(report)


if __name__ == '__main__':
    unittest.main()
