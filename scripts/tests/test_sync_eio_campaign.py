import copy
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sync_eio_campaign import (check_initialization, check_source, check_unrecognized_markers,
                               reports_injected_error, stage_inventory, stage_markers, targets_fixture, traced_ordinals)


class SyncEioCampaignTests(unittest.TestCase):
    def test_injected_errno_must_match_the_requested_error(self):
        line = '123 fsync(3</work/case/log>) = -1 EINTR (Interrupted system call) (INJECTED)'
        self.assertTrue(reports_injected_error(line, 'EINTR'))
        self.assertFalse(reports_injected_error(line, 'EIO'))
        self.assertFalse(reports_injected_error(line.replace(' (INJECTED)', ''), 'EINTR'))
        self.assertFalse(reports_injected_error('123 fsync(3</work/case/log>) = 0 (INJECTED)', 'EINTR'))

    def test_filtered_ordinals_include_unrelated_calls_in_the_counter(self):
        lines = [
            '123 write(1<pipe:[1]>, "status", 6) = 6',
            '123 write(3</work/case/records.log>, "data", 4) = 4',
            '123 pwrite64(4</work/case/commit.state>, "x", 1, 0) = 1',
            '123 write(1<pipe:[1]>, "done", 4) = 4',
            '123 write(5</work/case/stage>, "data", 4) = 4',
        ]
        counts, selected = traced_ordinals(lines, ('write', 'pwrite64'), Path('/work/case'))
        self.assertEqual(counts, {'write': 4, 'pwrite64': 1})
        self.assertEqual(selected, {'write': [2, 4], 'pwrite64': [1]})

    def test_filtered_ordinals_reject_multiple_tracees(self):
        lines = ['1 write(3</work/case/file>, "a", 1) = 1',
                 '2 write(3</work/case/file>, "b", 1) = 1']
        with self.assertRaises(ValueError):
            traced_ordinals(lines, ('write',), Path('/work/case'))

    def test_fixture_filter_checks_arguments_and_path_boundaries(self):
        fixture = Path('/work/case')
        self.assertTrue(targets_fixture('1 write(3</work/case/log>, "x", 1) = 1', 'write', fixture))
        self.assertFalse(targets_fixture('1 write(1<pipe:[1]>, "/work/case/log", 14) = 14',
                                         'write', fixture))
        self.assertFalse(targets_fixture('1 read(3</work/case-other/log>, "x", 1) = 1',
                                         'read', fixture))
        self.assertTrue(targets_fixture('1 renameat2(AT_FDCWD, "/work/case/a", AT_FDCWD, '
                                        '"/work/case/b", 0) = 0', 'renameat2', fixture))
        self.assertTrue(targets_fixture('1 openat(AT_FDCWD, "/work/case/log", O_RDONLY) = 3',
                                        'openat', fixture))
        self.assertFalse(targets_fixture('1 openat(AT_FDCWD, "/work/case-other/log", O_RDONLY) = 3',
                                         'openat', fixture))
        self.assertTrue(targets_fixture('1 fgetxattr(3</work/case/log>, "user.db.commit.v3", "", 0) = 0',
                                        'fgetxattr', fixture))
        self.assertFalse(targets_fixture('1 fgetxattr(1<pipe:[1]>, "/work/case/log", "", 0) = 0',
                                         'fgetxattr', fixture))
        self.assertTrue(targets_fixture('1 getxattr("/work/case/log", "user.db.commit.v3", "", 0) = 0',
                                        'getxattr', fixture))

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
        check_source(before, after, 'v3', 'batch')
        check_source(before, after, 'v3', 'checkpoint-batch')
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

    def test_staging_payload_requires_its_valid_owner(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            marker = root / '.emda-stage-v1-f-abcdefghijklmnop.owner'
            payload = root / (marker.name + '.data')
            payload.write_bytes(b'partial')
            with self.assertRaises(FileNotFoundError):
                stage_inventory(root)
            marker.write_bytes(b'corrupt')
            with self.assertRaises(ValueError):
                stage_inventory(root)
            marker.write_bytes(b'EMDA-STAGING-V1-FILE\n')
            self.assertEqual(len(stage_inventory(root)), 2)

    def test_skipped_malformed_marker_is_unchanged_and_has_no_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / '.emda-stage-v1-f-abcdefghijklmnop.owner'
            marker.write_bytes(b'')
            before = stage_markers(root, stage_inventory(root))
            check_unrecognized_markers(root, before, [marker.name])
            marker.write_bytes(b'changed')
            with self.assertRaises(ValueError):
                check_unrecognized_markers(root, before, [marker.name])
            marker.write_bytes(b'')
            Path(str(marker) + '.data').write_bytes(b'orphan')
            with self.assertRaises(ValueError):
                check_unrecognized_markers(root, before, [marker.name])
            Path(str(marker) + '.data').unlink()
            marker.write_bytes(b'EMDA-STAGING-V1-FILE\n')
            with self.assertRaises(ValueError):
                check_unrecognized_markers(root, {marker.name: marker.read_bytes()}, [marker.name])
            marker.unlink()
            marker.symlink_to('missing')
            with self.assertRaises(ValueError):
                check_unrecognized_markers(root, before, [marker.name])


if __name__ == '__main__':
    unittest.main()
