import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from campaign_fixtures import PreservedFixtureError, fault_fixture


class FaultFixtureTests(unittest.TestCase):
    def test_success_removes_only_the_disposable_fixture(self):
        with tempfile.TemporaryDirectory() as parent:
            other = Path(parent) / 'other'
            other.write_text('untouched')
            with fault_fixture(parent, 'fixture-') as fixture:
                (fixture / 'value').write_text('synthetic')
            self.assertFalse(fixture.exists())
            self.assertEqual(other.read_text(), 'untouched')

    def test_failure_keeps_fixture_and_reports_its_location(self):
        with tempfile.TemporaryDirectory() as parent:
            with self.assertRaises(PreservedFixtureError) as error:
                with fault_fixture(parent, 'fixture-') as fixture:
                    (fixture / 'value').write_text('synthetic')
                    raise ValueError('injected failure')
            self.assertIn(str(fixture), str(error.exception))
            self.assertIn('injected failure', str(error.exception))
            self.assertEqual((fixture / 'value').read_text(), 'synthetic')

    def test_interrupt_keeps_fixture(self):
        with tempfile.TemporaryDirectory() as parent:
            with self.assertRaises(KeyboardInterrupt):
                with fault_fixture(parent, 'fixture-') as fixture:
                    (fixture / 'value').write_text('synthetic')
                    raise KeyboardInterrupt()
            self.assertEqual((fixture / 'value').read_text(), 'synthetic')


if __name__ == '__main__':
    unittest.main()
