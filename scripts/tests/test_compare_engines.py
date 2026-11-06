import contextlib
import io
import json
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import compare_engines
from compare_engines import CASES, configure_cases, run_case_command, validate_result


class EngineComparisonConfigurationTests(unittest.TestCase):
    def test_selected_large_workloads_do_not_mutate_default_cases(self):
        baseline = [case.copy() for case in CASES]
        selected = configure_cases(['write_only', 'concurrent_mixed'], 100000)
        self.assertEqual([case['name'] for case in selected], ['concurrent_mixed', 'write_only'])
        self.assertTrue(all(case['keys'] == 100000 for case in selected))
        self.assertEqual(CASES, baseline)
        selected[0]['clients'] = 17
        self.assertEqual(next(case for case in CASES if case['name'] == 'concurrent_mixed')['clients'], 4)

    def test_noise_calibration_selects_only_its_configured_workload(self):
        cases = configure_cases(key_count=1000, noise_calibration=True)
        self.assertEqual([case['name'] for case in cases], ['concurrent_mixed'])
        self.assertEqual(cases[0]['keys'], 1000)

    def test_rejects_ambiguous_or_invalid_workloads(self):
        for kwargs in ({'names': ['write_only', 'write_only']}, {'names': ['unknown']},
                       {'key_count': 0}, {'key_count': 100001},
                       {'names': ['write_only'], 'noise_calibration': True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                configure_cases(**kwargs)

    def test_rejects_mismatched_or_incomplete_measured_results(self):
        case = configure_cases(['write_only'], 100000)[0]
        latency = {'count': case['clients'] * case['operations'], 'p50_us': 1, 'p95_us': 2,
                   'p99_us': 3, 'p99_9_us': 4, 'max_us': 5}
        result = {'backend': 'prototype', 'workload': 'mixed', 'clients': case['clients'],
                  'operations_per_client': case['operations'], 'keys': case['keys'],
                  'write_percent': 100, 'value_bytes': case['value_bytes'],
                  'completed': case['clients'] * case['operations'], 'ops_per_sec': 12,
                  'write': latency, 'read': {'count': 0}, 'create': {'count': 0},
                  'update': {'count': 0}, 'delete': {'count': 0}}
        self.assertIs(validate_result(result, 'prototype', case), result)
        wrong = {**result, 'keys': 250}
        with self.assertRaisesRegex(ValueError, 'requested workload'):
            validate_result(wrong, 'prototype', case)
        wrong = {**result, 'write': {**latency, 'count': latency['count'] - 1}}
        with self.assertRaisesRegex(ValueError, 'incomplete operations'):
            validate_result(wrong, 'prototype', case)

    def test_timeout_terminates_the_owned_process_group(self):
        process = MagicMock(pid=12345)
        process.communicate.side_effect = [subprocess.TimeoutExpired('fixture', 1), ('', 'diagnostic')]
        with patch.object(compare_engines.subprocess, 'Popen', return_value=process) as popen, \
                patch.object(compare_engines.os, 'killpg') as killpg:
            with self.assertRaisesRegex(RuntimeError, 'timed out'):
                run_case_command(['fixture'], {}, 1)
        self.assertTrue(popen.call_args.kwargs['start_new_session'])
        killpg.assert_called_once_with(12345, signal.SIGKILL)
        self.assertEqual(process.communicate.call_count, 2)

    def test_startup_failure_is_recorded_and_all_services_are_cleaned(self):
        closed = []

        class Service:
            def __init__(self, backend, parent, cpus):
                self.backend = backend

            def start(self):
                raise RuntimeError('injected startup failure')

            def close(self):
                closed.append(self.backend)
                if self.backend == 'postgres':
                    raise RuntimeError('injected cleanup failure')

            def redact(self, message):
                return message

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = root / 'server'
            server.write_bytes(b'fixture')
            output = root / 'report.json'
            args = ['compare_engines.py', '--directory', directory, '--server', str(server),
                    '--output', str(output), '--backends', 'postgres', 'mongo', '--rounds', '2']
            with (patch.object(sys, 'argv', args), patch.object(compare_engines.os, 'sched_getaffinity', return_value={0, 1, 2, 3, 4}),
                  patch.object(compare_engines.os, 'sched_setaffinity'),
                  patch.object(compare_engines.importlib.metadata, 'version', return_value='test'),
                  patch.object(compare_engines, 'snapshot', return_value={}),
                  patch.object(compare_engines, 'LocalService', Service)):
                with self.assertRaisesRegex(RuntimeError, 'startup failure'):
                    compare_engines.main()
            result = json.loads(output.read_text())
            self.assertFalse(result['completed'])
            self.assertIn('startup failure', result['failure'])
            self.assertEqual(result['current']['stage'], 'start-service')
            self.assertEqual([failure['backend'] for failure in result['cleanup_failures']], ['postgres'])
            self.assertEqual(closed, ['postgres', 'mongo'])

    def test_existing_output_is_not_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'report.json'
            output.write_text('prior result')
            args = ['compare_engines.py', '--directory', directory, '--output', str(output)]
            with (patch.object(sys, 'argv', args), patch.object(compare_engines.os, 'sched_getaffinity', return_value={0, 1, 2, 3, 4}),
                  contextlib.redirect_stderr(io.StringIO())):
                with self.assertRaises(SystemExit):
                    compare_engines.main()
            self.assertEqual(output.read_text(), 'prior result')

    def test_duplicate_service_is_rejected_before_it_can_leak_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'report.json'
            args = ['compare_engines.py', '--directory', directory, '--output', str(output),
                    '--backends', 'postgres', 'postgres']
            with (patch.object(sys, 'argv', args), patch.object(compare_engines.os, 'sched_getaffinity', return_value={0, 1, 2, 3, 4}),
                  patch.object(compare_engines, 'LocalService') as service,
                  contextlib.redirect_stderr(io.StringIO())):
                with self.assertRaises(SystemExit):
                    compare_engines.main()
            service.assert_not_called()
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
