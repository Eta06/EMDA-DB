import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run_enospc_container import cargo_binary


class EnospcContainerRunnerTests(unittest.TestCase):
    def write_events(self, path, events):
        path.write_text(''.join(json.dumps(event) + '\n' for event in events))

    def artifact(self, name='enospc_tmpfs', executable='/tmp/test-bin', is_test=True):
        return {'reason': 'compiler-artifact', 'target': {'name': name},
                'profile': {'test': is_test}, 'executable': executable}

    def test_finds_only_the_test_executable(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'build.jsonl'
            self.write_events(output, [self.artifact('other'), self.artifact(), self.artifact(is_test=False)])
            self.assertEqual(cargo_binary(output), Path('/tmp/test-bin'))

    def test_rejects_missing_or_ambiguous_executables(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'build.jsonl'
            for events in ([], [self.artifact(executable=None)], [self.artifact(), self.artifact()]):
                with self.subTest(events=events):
                    self.write_events(output, events)
                    with self.assertRaises(ValueError):
                        cargo_binary(output)


if __name__ == '__main__':
    unittest.main()
