import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sync_eio_campaign import check_source


class SyncEioCampaignTests(unittest.TestCase):
    def state(self):
        return {'log': b'acknowledged', 'witness': b'anchor',
                'metadata': b'boundary', 'backup': b'backup'}

    def test_write_may_append_but_cannot_change_acknowledged_prefix_or_anchor(self):
        before = self.state()
        after = copy.deepcopy(before)
        after['log'] += b'unacknowledged'
        after['metadata'] = b'advanced'
        check_source(before, after, 'v3', 'write')
        for field, value in (('log', b'truncated'), ('witness', b'changed'), ('backup', b'changed')):
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


if __name__ == '__main__':
    unittest.main()
