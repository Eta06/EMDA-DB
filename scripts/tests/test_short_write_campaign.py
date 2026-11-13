import errno
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from short_write_campaign import trace_events


class ShortWriteTraceTests(unittest.TestCase):
    def check(self, contents, expected='write', cap=4, mode='success'):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'trace'
            path.write_text(contents)
            return trace_events(path, expected, cap, mode)

    def test_accepts_real_short_retry_and_short_then_eio(self):
        self.assertEqual(len(self.check('write 8 4 0\nwrite 4 4 0\n')), 2)
        events = self.check(f'pwrite 36 4 0\npwrite 32 -1 {errno.EIO}\n',
                            expected='pwrite', mode='error')
        self.assertEqual(events[-1]['errno'], errno.EIO)

    def test_rejects_missing_injection_and_wrong_or_inconsistent_results(self):
        for trace in ('', 'write 4 4 0\n', 'writev 8 4 0\n',
                      'write 8 9 0\n', 'write 8 4 5\n'):
            with self.subTest(trace=trace), self.assertRaises(ValueError):
                self.check(trace)

    def test_error_must_follow_the_short_write(self):
        for trace in (f'write 8 -1 {errno.EIO}\n',
                      'write 8 4 0\nwrite 4 4 0\n',
                      f'write 8 4 0\nwrite 4 -1 {errno.ENOSPC}\n'):
            with self.subTest(trace=trace), self.assertRaises(ValueError):
                self.check(trace, mode='error')


if __name__ == '__main__':
    unittest.main()
