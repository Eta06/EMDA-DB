import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sync_eio_campaign import managed_selection_snapshot


def encoded_selection(path, trusted):
    encoded = os.fsencode(path)
    body = b'EMDASEL1' + len(encoded).to_bytes(4, 'little') + encoded + trusted
    return body + hashlib.sha256(body).digest()


class ManagedSelectionSnapshotTests(unittest.TestCase):
    def test_checks_selection_target_digest_and_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'source').mkdir()
            trusted = b'a' * 32
            (root / 'checkpoint.digest').write_bytes(trusted)
            selection = root / 'source' / 'checkpoint.selection'
            self.assertIsNone(managed_selection_snapshot(root))
            for label in ('previous-selected', 'result'):
                selection.write_bytes(encoded_selection(root / label, trusted))
                self.assertEqual(managed_selection_snapshot(root)['checkpoint'], label)
            next_digest = b'c' * 32
            (root / 'next.digest').write_bytes(next_digest)
            selection.write_bytes(encoded_selection(root / 'previous-selected', trusted))
            self.assertEqual(managed_selection_snapshot(root)['checkpoint'], 'previous-selected')
            selection.write_bytes(encoded_selection(root / 'result', next_digest))
            self.assertEqual(managed_selection_snapshot(root)['checkpoint'], 'result')
            selection.write_bytes(encoded_selection(root / 'unrelated', trusted))
            with self.assertRaisesRegex(ValueError, 'unexpected checkpoint'):
                managed_selection_snapshot(root)
            selection.write_bytes(encoded_selection(root / 'result', b'b' * 32))
            with self.assertRaisesRegex(ValueError, 'differs from trusted fixture'):
                managed_selection_snapshot(root)
            damaged = bytearray(encoded_selection(root / 'result', trusted))
            damaged[-1] ^= 1
            selection.write_bytes(damaged)
            with self.assertRaisesRegex(ValueError, 'checksum'):
                managed_selection_snapshot(root)
            selection.write_bytes(damaged[:-1])
            with self.assertRaisesRegex(ValueError, 'length'):
                managed_selection_snapshot(root)


if __name__ == '__main__':
    unittest.main()
