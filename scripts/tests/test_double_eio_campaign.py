import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from double_eio_campaign import calls_after_primary


class DoubleEioCampaignTests(unittest.TestCase):
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

    def test_same_syscall_pair_is_not_selected(self):
        lines = ['1 unlink("/work/case/a") = -1 EIO (INJECTED)',
                 '1 unlink("/work/case/b") = 0', '1 rmdir("/work/case/c") = 0']
        result = calls_after_primary(lines, 'unlink', Path('/work/case'))
        self.assertEqual(result['unlink'], [])
        self.assertEqual(result['rmdir'], [1])


if __name__ == '__main__':
    unittest.main()
