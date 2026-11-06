import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from double_eio_campaign import (ReportWriter, calls_after_primary, injection_rules,
                                 primary_ordinals, targets_fixture_or_ancestor_sync)


class DoubleEioCampaignTests(unittest.TestCase):
    def test_partial_snapshot_can_lag_current_case_but_failure_forces_full_report(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'result.json'
            writer = ReportWriter(output, 1.0)
            result = {'revision': 'test', 'current': {'case': 1}, 'completed': False,
                      'failure': None, 'cases': []}
            clock = [10.0]
            with patch('double_eio_campaign.time.monotonic', side_effect=lambda: clock[0]):
                writer.save(result, force=True)
                result['current'] = {'case': 2}
                result['cases'].append({'passed': 1})
                clock[0] = 10.5
                writer.save(result)
                self.assertEqual(json.loads(output.read_text())['current'], {'case': 1})
                self.assertEqual(json.loads(writer.progress.read_text())['current'], {'case': 2})
                result['failure'] = 'injected error'
                writer.save(result, force=True)
            final = json.loads(output.read_text())
            self.assertEqual(final['current'], {'case': 2})
            self.assertEqual(final['cases'], [{'passed': 1}])
            self.assertEqual(final['failure'], 'injected error')
            self.assertEqual(final['full_report_snapshots'], 2)
            self.assertEqual(json.loads(writer.progress.read_text())['failure'], 'injected error')

    def test_failed_serialization_preserves_previous_report(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'result.json'
            output.write_text('previous report')
            with self.assertRaises(TypeError):
                ReportWriter._atomic_json(output, {'invalid': object()})
            self.assertEqual(output.read_text(), 'previous report')
            self.assertEqual(list(Path(directory).glob('.double-eio-report-*')), [])

    def test_cleanup_ordinals_count_synchronizations_before_the_first_fault(self):
        lines = [
            '1 fsync(3</work/case/file>) = 0',
            '1 fdatasync(3</work/case/file>) = 0',
            '1 unlink("/other/file") = 0',
            '1 write(3</work/case/file>, "x", 1) = -1 EIO (INJECTED)',
            '1 fsync(4</work/case>) = 0',
            '1 fsync(5</work>) = 0',
            '1 fdatasync(3</work/case/file>) = 0',
            '1 unlink("/other/file") = 0',
            '1 unlink("/work/case/partial") = 0',
            '1 rmdir("/work/case/partial-dir") = 0',
        ]
        self.assertEqual(calls_after_primary(lines, 'write', Path('/work/case')),
                         {'fsync': [2, 3], 'fdatasync': [2], 'unlink': [3],
                          'unlinkat': [], 'rmdir': [1]})

    def test_missing_or_duplicate_primary_fault_is_rejected(self):
        with self.assertRaises(ValueError):
            calls_after_primary(['1 fsync(3</work>) = 0'], 'write', Path('/work'))
        with self.assertRaises(ValueError):
            calls_after_primary(['1 write(3</work/f>, "x", 1) = -1 EIO (INJECTED)',
                                 '1 write(3</work/f>, "x", 1) = -1 EIO (INJECTED)'], 'write', Path('/work'))

    def test_same_syscall_pair_is_selected_and_bounded(self):
        lines = ['1 unlink("/work/case/a") = -1 EIO (INJECTED)',
                 '1 unlink("/work/case/b") = 0', '1 rmdir("/work/case/c") = 0']
        result = calls_after_primary(lines, 'unlink', Path('/work/case'))
        self.assertEqual(result['unlink'], [2])
        self.assertEqual(result['rmdir'], [1])
        self.assertEqual(injection_rules('unlink', 2, ('unlink', 5)),
                         ['-e', 'inject=unlink:error=EIO:when=2..5+3'])
        self.assertEqual(injection_rules('fsync', 4, ('fsync', 5)),
                         ['-e', 'inject=fsync:error=EIO:when=4..5+1'])
        with self.assertRaises(ValueError):
            injection_rules('unlink', 2, ('unlink', 2))
        injected_twice = [lines[0], lines[1].replace('= 0', '= -1 EIO (INJECTED)')]
        self.assertEqual(calls_after_primary(injected_twice, 'unlink', Path('/work/case'),
                                             allow_same_second=True)['unlink'], [2])

    def test_sync_ancestor_mode_selects_only_additional_sync_calls(self):
        control = {'counts': {'fsync': 5, 'unlink': 5},
                   'eligible_ordinals': {'fsync': [2, 4], 'unlink': [3]}}
        self.assertEqual(primary_ordinals(control, 'fsync', False), [2, 4])
        self.assertEqual(primary_ordinals(control, 'fsync', True), [1, 2, 3, 4, 5])
        self.assertEqual(primary_ordinals(control, 'unlink', True), [3])
        root = Path('/work/case')
        self.assertTrue(targets_fixture_or_ancestor_sync('1 fsync(3</work>) = -1 EIO', 'fsync', root))
        self.assertTrue(targets_fixture_or_ancestor_sync('1 fsync(3</work/case/a>) = -1 EIO', 'fsync', root))
        self.assertFalse(targets_fixture_or_ancestor_sync('1 fsync(3</work/other>) = -1 EIO', 'fsync', root))
        self.assertFalse(targets_fixture_or_ancestor_sync('1 unlink("/work/case/a") = -1 EIO', 'unlink', root))


if __name__ == '__main__':
    unittest.main()
