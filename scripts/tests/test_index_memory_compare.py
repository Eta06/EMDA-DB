import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from index_memory_compare import fields, validate_sample


class IndexMemoryComparisonTests(unittest.TestCase):
    def sample(self, mode='checkpoint-store', tail=2):
        overlay = tail if mode in {'recovered', 'checkpoint-store'} else 0
        tail_samples = ','.join(['2'] * 16) if tail and mode != 'view' else 'none'
        return (f'mode={mode} open_us=10 rss_kib=20 after_rss_kib=21 peak_rss_kib=22 '
                f'read_chars=100 disk_read_bytes=0 read_calls=4 blocks=1 overlay_keys={overlay} '
                f'lookups_ns={",".join(["1"] * 16)} tail_lookups_ns={tail_samples}')

    def test_accepts_all_modes_and_checks_tail_presence(self):
        for mode in ('store-full', 'store', 'checkpoint-store', 'view', 'recovered'):
            for tail in (0, 2):
                result = validate_sample(self.sample(mode, tail), mode, tail)
                self.assertEqual(len(result['lookups_ns']), 16)
                self.assertEqual(len(result['tail_lookups_ns']), 16 if tail and mode != 'view' else 0)

    def test_rejects_incomplete_or_invalid_samples(self):
        sample = self.sample()
        for bad in (sample.replace('overlay_keys=2', 'overlay_keys=1'),
                    sample.replace('read_chars=100', 'read_chars=-1'),
                    sample.replace('tail_lookups_ns=', 'tail_lookups_ns=none '),
                    sample.replace('lookups_ns=', 'lookups_ns=1 '),
                    sample + ' mode=view'):
            with self.subTest(bad=bad):
                with self.assertRaises((ValueError, KeyError)):
                    validate_sample(bad, 'checkpoint-store', 2)
        with self.assertRaises(ValueError):
            fields('mode=x mode=y')
        with self.assertRaises(ValueError):
            validate_sample(sample, 'view', 2)


if __name__ == '__main__':
    unittest.main()
