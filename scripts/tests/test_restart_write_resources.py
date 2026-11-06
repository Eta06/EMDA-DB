import copy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from restart_write_resources import evict_fixture_files, validate_sample


class RestartResourceTests(unittest.TestCase):
    def sample(self, phase='write'):
        return {
            'mode': 'full', 'phase': phase, 'epoch': 2, 'base_keys': 32,
            'tail_keys': 4, 'key_bytes': 8, 'workers': 2, 'writes_per_worker': 3,
            'prior_verified': 12, 'current_verified': 6 if phase == 'write' else 0,
            'open_us': 3, 'write_elapsed_ns': 10 if phase == 'write' else 0,
            'write_latencies_ns': [1] * 6 if phase == 'write' else [],
            'rss_after_open_kib': 5, 'peak_rss_kib': 6,
            'process_io': {'rchar': 7, 'wchar': 8, 'read_bytes': 0, 'write_bytes': 2,
                           'read_calls': 9, 'write_calls': 3},
        }

    def check(self, sample, phase='write'):
        return validate_sample(sample, 'full', phase, 2, 32, 4, 8, 2, 3)

    def test_accepts_write_and_final_verification(self):
        self.assertEqual(self.check(self.sample())['prior_verified'], 12)
        self.assertEqual(self.check(self.sample('verify'), 'verify')['current_verified'], 0)

    def test_rejects_incomplete_verification_and_bad_metrics(self):
        good = self.sample()
        changes = {
            'prior_verified': 11, 'current_verified': 5, 'mode': 'checkpoint',
            'write_latencies_ns': [1] * 5, 'write_elapsed_ns': 0,
            'open_us': 0, 'peak_rss_kib': -1,
            'process_io': {'rchar': 1},
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                bad = copy.deepcopy(good)
                bad[field] = value
                with self.assertRaises(ValueError):
                    self.check(bad)
        bad = self.sample('verify')
        bad['write_elapsed_ns'] = 1
        with self.assertRaises(ValueError):
            self.check(bad, 'verify')

    def test_sqlite_requires_checked_durability_and_integrity(self):
        sample = self.sample()
        sample.update(mode='sqlite', journal='wal', synchronous='FULL',
                      sqlite_version='3.49.1', integrity_check_us=10)
        validate_sample(sample, 'sqlite', 'write', 2, 32, 4, 8, 2, 3)
        for field, value in (('journal', 'delete'), ('synchronous', 'NORMAL'),
                             ('sqlite_version', ''), ('integrity_check_us', 0)):
            with self.subTest(field=field):
                bad = copy.deepcopy(sample)
                bad[field] = value
                with self.assertRaises(ValueError):
                    validate_sample(bad, 'sqlite', 'write', 2, 32, 4, 8, 2, 3)

    def test_cache_advice_targets_only_owned_known_files(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            (root / 'store').mkdir()
            (root / 'store' / 'records.log').write_bytes(b'log')
            (root / 'store' / 'commit.state').write_bytes(b'witness')
            (root / 'index').write_bytes(b'index')
            (root / 'unrelated').write_bytes(b'untouched')
            with patch('restart_write_resources.os.posix_fadvise') as advise:
                result = evict_fixture_files(root, 'checkpoint')
            self.assertEqual([entry['file'] for entry in result],
                             ['store/records.log', 'store/commit.state', 'index'])
            self.assertEqual(advise.call_count, 3)
            self.assertEqual((root / 'unrelated').read_bytes(), b'untouched')

    def test_cache_advice_refuses_a_symlink(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            (root / 'store').mkdir()
            (root / 'outside').write_bytes(b'untouched')
            (root / 'store' / 'records.log').symlink_to(root / 'outside')
            (root / 'store' / 'commit.state').write_bytes(b'witness')
            with patch('restart_write_resources.os.posix_fadvise') as advise:
                with self.assertRaises(OSError):
                    evict_fixture_files(root, 'full')
                advise.assert_not_called()


if __name__ == '__main__':
    unittest.main()
