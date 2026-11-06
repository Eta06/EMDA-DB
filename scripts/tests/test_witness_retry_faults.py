import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from witness_retry_faults import validate_trace


class WitnessRetryTraceTests(unittest.TestCase):
    def setUp(self):
        self.root = Path('/work/case')
        self.call = '1 fsetxattr(3</work/case/source/records.log>, "user.db.commit.v3", "abc", 3, XATTR_REPLACE)'
        self.interrupted = self.call + ' = -1 EINTR (INJECTED)'
        self.retry = self.call + ' = 0'
        self.sync = '1 fsync(3</work/case/source/records.log>) = 0'
        self.failed_sync = '1 fsync(3</work/case/source/records.log>) = -1 EIO (INJECTED)'

    def test_requires_identical_retry_before_the_selected_sync(self):
        validate_trace([self.interrupted, self.retry, self.sync], self.root, False)
        validate_trace([self.interrupted, self.retry, self.failed_sync], self.root, True)
        for lines, failed in [
            ([self.interrupted, self.sync], False),
            ([self.interrupted, self.retry.replace('"abc"', '"xyz"'), self.sync], False),
            ([self.interrupted, self.sync, self.retry], False),
            ([self.interrupted, self.retry, self.sync], True),
            ([self.interrupted, self.retry, self.failed_sync], False),
        ]:
            with self.subTest(lines=lines), self.assertRaises(ValueError):
                validate_trace(lines, self.root, failed)

    def test_read_retry_compares_descriptor_attribute_and_size(self):
        interrupted = ('1 fgetxattr(3</work/case/source/records.log>, "user.db.commit.v3", '
                       '0x1234, 36) = -1 EINTR (INJECTED)')
        retry = ('1 fgetxattr(3</work/case/source/records.log>, "user.db.commit.v3", '
                 '"new bytes", 36) = 36')
        validate_trace([interrupted, retry, self.sync], self.root, False, 'read')
        validate_trace([interrupted, retry, self.failed_sync], self.root, True, 'read')
        for broken in [retry.replace(', 36)', ', 35)'), retry.replace('3</work', '4</work'),
                       retry.replace(' = 36', ' = -1 EIO')]:
            with self.subTest(broken=broken), self.assertRaises(ValueError):
                validate_trace([interrupted, broken, self.sync], self.root, False, 'read')


if __name__ == '__main__':
    unittest.main()
