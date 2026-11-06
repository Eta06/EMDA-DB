import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_double_eio_results import verify


def probe(order, injections):
    return {
        'outcome': {'operation': 'backup', 'status': 'success'},
        'verification': {'operation': 'backup', 'status': 'verified', 'recovery_order': order},
        'injected_traces': [f'1 {call}(3</work>) = -1 EIO (INJECTED)' for call in injections],
    }


def result_fixture():
    control = probe(['ordinary', 'checkpoint'], [])
    control.update(counts={'fsync': 1}, eligible_ordinals={'fsync': [1]})
    first = probe(['ordinary', 'checkpoint'], ['fsync'])
    first['calls_after_primary'] = {'fsync': [2], 'fdatasync': [], 'unlink': [], 'unlinkat': [], 'rmdir': []}
    first['counts'] = {'fsync': 2, 'fdatasync': 0, 'unlink': 0, 'unlinkat': 0, 'rmdir': 0}
    second = []
    for name, order in [('ordinary-first', ['ordinary', 'checkpoint']),
                        ('checkpoint-first', ['checkpoint', 'ordinary'])]:
        item = probe(order, ['fsync', 'fsync'])
        item.update(syscall='fsync', ordinal=2, order=name)
        second.append(item)
    return {
        'completed': True, 'failure': None, 'current': None, 'formats': ['v3'],
        'operations': ['backup'], 'primary_syscalls': ['fsync'], 'max_primary': 0,
        'all_sync_paths': True,
        'cases': [{'format': 'v3', 'operation': 'backup', 'control': control,
                   'primary_checks': [{'syscall': 'fsync', 'ordinal': 1, 'first': first, 'second': second}]}],
    }


class PairedEioResultTests(unittest.TestCase):
    def test_complete_report_counts_both_recovery_orders(self):
        self.assertEqual(verify(result_fixture()),
                         {'controls': 1, 'primary': 1, 'paired': 2, 'same_syscall_pairs': 2})

    def test_missing_or_duplicate_fault_or_recovery_order_is_rejected(self):
        original = result_fixture()
        broken = copy.deepcopy(original)
        broken['cases'][0]['primary_checks'].clear()
        with self.assertRaisesRegex(ValueError, 'primary injection'):
            verify(broken)
        broken = copy.deepcopy(original)
        broken['cases'][0]['primary_checks'][0]['second'].pop()
        with self.assertRaisesRegex(ValueError, 'secondary injection'):
            verify(broken)
        broken = copy.deepcopy(original)
        broken['cases'][0]['primary_checks'][0]['second'][1]['order'] = 'ordinary-first'
        with self.assertRaisesRegex(ValueError, 'secondary injection'):
            verify(broken)
        broken = copy.deepcopy(original)
        check = broken['cases'][0]['primary_checks'][0]
        check['first']['calls_after_primary']['fsync'].append(2)
        check['second'].extend(copy.deepcopy(check['second']))
        with self.assertRaisesRegex(ValueError, 'duplicate selected secondary ordinal'):
            verify(broken)

    def test_incomplete_or_forged_injection_is_rejected(self):
        broken = result_fixture()
        broken['completed'] = False
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            verify(broken)
        broken = result_fixture()
        broken['cases'][0]['primary_checks'][0]['second'][0]['injected_traces'][1] = '1 unlink("/work") = 0'
        with self.assertRaisesRegex(ValueError, 'injection syscall'):
            verify(broken)


if __name__ == '__main__':
    unittest.main()
