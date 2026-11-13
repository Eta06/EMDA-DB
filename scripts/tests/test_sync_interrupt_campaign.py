import errno
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sync_interrupt_campaign import check_trace


class InterruptedSyncTests(unittest.TestCase):
    def check(self, trace, mode='success'):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'trace'
            path.write_text(trace)
            return check_trace(path, 'fsync', mode)

    def test_checks_real_first_sync_and_both_retry_outcomes(self):
        first = f'fsync 1 0 -1 {errno.EINTR}\n'
        self.assertEqual(len(self.check(first + 'fsync 2 0 0 0\n')), 2)
        self.assertEqual(len(self.check(first + f'fsync 2 -2 -1 {errno.EIO}\n', 'error')), 2)

    def test_rejects_absent_sync_or_failed_retry_reported_as_success(self):
        for trace, mode in ((f'fsync 1 -2 -1 {errno.EINTR}\nfsync 2 0 0 0\n', 'success'),
                            (f'fsync 1 0 -1 {errno.EINTR}\n', 'success'),
                            (f'fsync 1 0 -1 {errno.EINTR}\nfsync 2 0 0 0\n', 'error'),
                            (f'fdatasync 1 0 -1 {errno.EINTR}\nfsync 2 0 0 0\n', 'success')):
            with self.subTest(trace=trace), self.assertRaises(ValueError):
                self.check(trace, mode)


if __name__ == '__main__':
    unittest.main()
