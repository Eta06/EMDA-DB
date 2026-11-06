import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from campaign_report import ReportWriter


class CampaignReportTests(unittest.TestCase):
    def test_progress_tracks_each_case_and_final_snapshot_is_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'report.json'
            writer = ReportWriter(output, 1.0)
            result = {'revision': 'abc', 'current': {'case': 1}, 'completed': False, 'failure': None}
            with patch('campaign_report.time.monotonic', side_effect=[10.0, 10.1, 10.2]):
                writer.save(result)
                result['current'] = {'case': 2}
                writer.save(result)
                self.assertEqual(json.loads(output.read_text())['current'], {'case': 1})
                self.assertEqual(json.loads(writer.progress.read_text())['current'], {'case': 2})
                result['current'] = None
                result['completed'] = True
                writer.save(result, force=True)
            self.assertEqual(json.loads(output.read_text())['full_report_snapshots'], 2)
            self.assertTrue(json.loads(output.read_text())['completed'])
            self.assertTrue(json.loads(writer.progress.read_text())['completed'])

    def test_failed_final_report_does_not_publish_completed_sidecar(self):
        with tempfile.TemporaryDirectory() as directory:
            writer = ReportWriter(Path(directory) / 'report.json', 1.0)
            result = {'revision': 'abc', 'current': {'case': 1}, 'completed': False, 'failure': None}
            writer.save(result)
            result.update(current=None, completed=True)
            real_write = writer._atomic_json

            def failing_write(path, value):
                if path == writer.output:
                    raise OSError('injected output failure')
                real_write(path, value)

            with patch.object(writer, '_atomic_json', side_effect=failing_write):
                with self.assertRaisesRegex(OSError, 'injected output failure'):
                    writer.save(result, force=True)
            self.assertFalse(json.loads(writer.progress.read_text())['completed'])
            self.assertFalse(json.loads(writer.output.read_text())['completed'])


if __name__ == '__main__':
    unittest.main()
