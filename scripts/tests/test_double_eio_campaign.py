import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from double_eio_campaign import sync_ordinals_after_primary


class DoubleEioCampaignTests(unittest.TestCase):
    def test_cleanup_ordinals_count_synchronizations_before_the_first_fault(self):
        lines = [
            '1 fsync(3</work/case/file>) = 0',
            '1 fdatasync(3</work/case/file>) = 0',
            '1 write(3</work/case/file>, "x", 1) = -1 EIO (INJECTED)',
            '1 fsync(4</work/case>) = 0',
            '1 fsync(5</work>) = 0',
            '1 fdatasync(3</work/case/file>) = 0',
        ]
        self.assertEqual(sync_ordinals_after_primary(lines, 'write'),
                         {'fsync': [2, 3], 'fdatasync': [2]})

    def test_missing_or_duplicate_primary_fault_is_rejected(self):
        with self.assertRaises(ValueError):
            sync_ordinals_after_primary(['1 fsync(3</work>) = 0'], 'write')
        with self.assertRaises(ValueError):
            sync_ordinals_after_primary(['1 write(3</work/f>, "x", 1) = -1 EIO (INJECTED)',
                                         '1 write(3</work/f>, "x", 1) = -1 EIO (INJECTED)'], 'write')


if __name__ == '__main__':
    unittest.main()
