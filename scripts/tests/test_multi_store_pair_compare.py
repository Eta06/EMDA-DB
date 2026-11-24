import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from multi_store_pair_compare import fixture_identity


class MultiStorePairIdentityTests(unittest.TestCase):
    @unittest.skipUnless(hasattr(os, 'setxattr'), 'requires the Linux user xattr API')
    def test_identity_includes_log_checkpoint_anchor_and_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            project = Path(root) / 'project-0000'
            store = project / 'store'
            store.mkdir(parents=True)
            for path in (store / 'records.log', store / 'commit.state', project / 'checkpoint', project / 'digest'):
                path.write_bytes(b'original')
            log = store / 'records.log'
            try:
                os.setxattr(log, 'user.db.commit.v3', b'original')
            except OSError as error:
                self.skipTest(f'fixture filesystem cannot store user xattrs: {error}')
            before = fixture_identity(root, 1)
            for path in (log, store / 'commit.state', project / 'checkpoint', project / 'digest'):
                path.write_bytes(b'changed')
                self.assertNotEqual(fixture_identity(root, 1), before)
                path.write_bytes(b'original')
            os.setxattr(log, 'user.db.commit.v3', b'changed')
            self.assertNotEqual(fixture_identity(root, 1), before)


if __name__ == '__main__':
    unittest.main()
