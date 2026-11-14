import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_short_write_results import load_report as load_short_write
from verify_sync_interrupt_results import load_report as load_sync_interrupt


class FaultReportLoadingTests(unittest.TestCase):
    def test_plain_and_compressed_reports_are_read_identically(self):
        expected = {'cases': [{'status': 'verified'}]}
        with tempfile.TemporaryDirectory() as directory:
            plain = Path(directory) / 'report.json'
            compressed = Path(directory) / 'report.json.gz'
            plain.write_text(json.dumps(expected), encoding='utf-8')
            with gzip.open(compressed, 'wt', encoding='utf-8') as file:
                json.dump(expected, file)
            for load in (load_short_write, load_sync_interrupt):
                self.assertEqual(load(plain), expected)
                self.assertEqual(load(compressed), expected)


if __name__ == '__main__':
    unittest.main()
