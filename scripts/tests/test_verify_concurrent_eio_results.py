import copy
import itertools
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_concurrent_eio_results import verify


def fixture():
    report = {'completed': True, 'failure': None, 'current': None, 'error': 'EIO',
              'mode': 'transaction', 'repetitions': 1, 'cases': []}
    for fmt, backend, fault, ordinal, order in itertools.product(
            ('v2', 'v3'), ('ordinary', 'checkpoint'), ('witness', 'log-sync'),
            (1, 2), ('ordinary-first', 'checkpoint-first')):
        workers = [{'id': i, 'acknowledged': [0, 1], 'failed': 2, 'error': 'synthetic error'} for i in range(4)]
        backends = ('ordinary', 'checkpoint') if order == 'ordinary-first' else ('checkpoint', 'ordinary')
        checked = [{'status': 'verified', 'backend': item, 'mode': 'transaction', 'acknowledged': 8,
                    'uncertain_present': [[0, 2]]} for item in backends]
        if fault == 'witness' and fmt == 'v2':
            call = 'pwrite64(3</work/source/commit.state>, "x", 1, 0)'
        elif fault == 'witness':
            call = 'fsetxattr(3</work/source/records.log>, "user.db.commit.v3", "x", 1, 0)'
        else:
            call = ('fdatasync' if fmt == 'v2' else 'fsync') + '(3</work/source/records.log>)'
        state = {'backup_sha256': 'backup', 'backup_bytes': 1, 'checkpoint_sha256': 'checkpoint',
                 'checkpoint_digest_sha256': 'digest', 'witness_sha256': 'anchor', 'log_bytes': 10}
        report['cases'].append({'repetition': 1, 'format': fmt, 'backend': backend, 'fault': fault,
                                'ordinal_per_tracee': ordinal, 'recovery_order': order,
                                'receipts': {'workers': workers}, 'verified': checked,
                                'injected_traces': ['1 ' + call + ' = -1 EIO (INJECTED)'],
                                'before': copy.deepcopy(state), 'after_attempt': copy.deepcopy(state),
                                'after_verification': copy.deepcopy(state)})
    return report


class ConcurrentReportVerificationTests(unittest.TestCase):
    def test_complete_report_and_interrupted_successes(self):
        self.assertEqual(verify(fixture()), {'errno': 'EIO', 'mode': 'transaction', 'cases': 32,
                                             'acknowledgements': 256, 'failed_calls': 128,
                                             'uncertain_present': 32, 'injections': 32})
        interrupted = fixture()
        interrupted['error'] = 'EINTR'
        for case in interrupted['cases']:
            case['injected_traces'] = [line.replace('EIO', 'EINTR') for line in case['injected_traces']]
            for worker in case['receipts']['workers']:
                worker.update(acknowledged=list(range(64)), failed=None, error=None)
            for checked in case['verified']:
                checked.update(acknowledged=256, uncertain_present=[])
        self.assertEqual(verify(interrupted)['acknowledgements'], 8192)

    def test_missing_duplicate_and_malformed_cases_are_rejected(self):
        for mutate in (
            lambda report: report['cases'].pop(),
            lambda report: report['cases'].append(copy.deepcopy(report['cases'][0])),
            lambda report: report['cases'][0]['receipts']['workers'][0].update(acknowledged=[0, 2]),
            lambda report: report['cases'][0]['verified'][0].update(acknowledged=0),
            lambda report: report['cases'][0]['verified'][0].update(uncertain_present=[[0, 3]]),
            lambda report: report['cases'][0]['verified'][1].update(mode='single'),
            lambda report: report['cases'][0]['after_verification'].update(backup_sha256='changed'),
            lambda report: report['cases'][0]['injected_traces'].__setitem__(0, '1 fsync(3</wrong>) = -1 EIO (INJECTED)'),
            lambda report: report['cases'][0].update(repetition=True),
        ):
            broken = fixture()
            mutate(broken)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                verify(broken)


if __name__ == '__main__':
    unittest.main()
