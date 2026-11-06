import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sync_eio_campaign import check_initialization, check_source


class SyncEioCampaignTests(unittest.TestCase):
    def state(self):
        return {'log': b'acknowledged', 'witness': b'anchor',
                'metadata': b'boundary', 'backup': b'backup',
                'checkpoint': b'checkpoint', 'checkpoint_digest': b'digest'}

    def test_write_may_append_but_cannot_change_acknowledged_prefix_or_anchor(self):
        before = self.state()
        after = copy.deepcopy(before)
        after['log'] += b'unacknowledged'
        after['metadata'] = b'advanced'
        check_source(before, after, 'v3', 'write')
        check_source(before, after, 'v3', 'checkpoint-write')
        for field, value in (('log', b'truncated'), ('witness', b'changed'), ('backup', b'changed'),
                             ('checkpoint', b'changed'), ('checkpoint_digest', b'changed')):
            with self.subTest(field=field):
                bad = copy.deepcopy(after)
                bad[field] = value
                with self.assertRaises(ValueError):
                    check_source(before, bad, 'v3', 'write')

    def test_copy_must_not_change_any_source_or_backup_bytes(self):
        before = self.state()
        check_source(before, copy.deepcopy(before), 'v3', 'restore')
        for field in before:
            with self.subTest(field=field):
                bad = copy.deepcopy(before)
                bad[field] += b'changed'
                with self.assertRaises(ValueError):
                    check_source(before, bad, 'v3', 'restore')

    def test_initialization_verification_preserves_existing_evidence(self):
        before = {'files': {'records.log': b'partial', 'store.lock': b''}, 'metadata': b'boundary'}
        check_initialization(before, copy.deepcopy(before))
        for bad in (None, {'files': {'records.log': b'changed', 'store.lock': b''}, 'metadata': b'boundary'},
                    {'files': before['files'], 'metadata': None}):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    check_initialization(before, bad)
        check_initialization({'files': {}, 'metadata': None},
                             {'files': {'records.log': b'new'}, 'metadata': b'new'})


if __name__ == '__main__':
    unittest.main()
