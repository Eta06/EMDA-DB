import copy
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmarks.large_crash import apply_changes, key, value, verify
import large_crash_compare


class MemoryClient:
    def __init__(self, values):
        self.values = values

    def put(self, name, data):
        self.values[name] = data

    def get(self, name):
        return self.values.get(name)

    def delete(self, name):
        self.values.pop(name, None)


class LargeCrashVerificationTests(unittest.TestCase):
    def test_checks_every_seeded_value_update_and_deletion(self):
        count = 7
        client = MemoryClient({key(index): value(index) for index in range(count)})
        apply_changes(client, count)
        self.assertEqual(verify(client, count), {'seeded_values': 7, 'later_values': 90, 'deletions': 10})
        for corrupted_key, replacement in [
            (key(3), b'bad seed'),
            (key(count), b'old update'),
            (key(count + 50), None),
            (key(count + 99), b'reappeared deletion'),
        ]:
            broken = MemoryClient(copy.copy(client.values))
            if replacement is None:
                broken.delete(corrupted_key)
            else:
                broken.put(corrupted_key, replacement)
            with self.subTest(corrupted_key=corrupted_key), self.assertRaises(ValueError):
                verify(broken, count)

    def test_runner_preserves_failure_and_continues_with_other_backend(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            binary = parent / 'binary'
            binary.write_bytes(b'test binary')
            output = parent / 'result.json'
            args = ['large_crash_compare.py', '--directory', directory, '--output', str(output),
                    '--server', str(binary), '--prototype-preparer', str(binary),
                    '--sqlite-preparer', str(binary), '--keys', '1', '--client-cpus', '0',
                    '--server-cpus', '1', '--backends', 'prototype', 'sqlite']

            def check(backend, *_):
                self.assertEqual(json.loads(output.read_text())['current']['backend'], backend)
                if backend == 'prototype':
                    raise RuntimeError('synthetic failure')
                return {'backend': backend, 'result': 'passed'}

            with (patch.object(sys, 'argv', args), patch.object(large_crash_compare, 'check', side_effect=check),
                  patch.object(large_crash_compare, 'quiet_window', return_value={'quiet': True}),
                  patch('os.sched_getaffinity', return_value={0, 1}), patch('os.sched_setaffinity'),
                  contextlib.redirect_stdout(io.StringIO())):
                with self.assertRaisesRegex(SystemExit, 'crash checks failed'):
                    large_crash_compare.main()
            result = json.loads(output.read_text())
            self.assertTrue(result['completed'])
            self.assertIsNone(result['current'])
            self.assertEqual([row['result'] for row in result['runs']], ['failed', 'passed'])
            self.assertIn('synthetic failure', result['runs'][0]['error'])

    def test_existing_report_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            binary = parent / 'binary'
            binary.write_bytes(b'test binary')
            output = parent / 'result.json'
            output.write_text('existing evidence')
            args = ['large_crash_compare.py', '--directory', directory, '--output', str(output),
                    '--server', str(binary), '--prototype-preparer', str(binary),
                    '--sqlite-preparer', str(binary), '--client-cpus', '0', '--server-cpus', '1']
            with (patch.object(sys, 'argv', args), patch.object(large_crash_compare, 'check') as check,
                  patch('os.sched_getaffinity', return_value={0, 1}),
                  contextlib.redirect_stderr(io.StringIO())):
                with self.assertRaises(SystemExit):
                    large_crash_compare.main()
                check.assert_not_called()
            self.assertEqual(output.read_text(), 'existing evidence')


if __name__ == '__main__':
    unittest.main()
