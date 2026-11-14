import fnmatch
import re
import unittest
from pathlib import Path


def missing_dependencies(workflow):
    triggers = workflow.split('\njobs:', 1)[0]
    sections = {
        'push': triggers.split('  push:', 1)[1].split('  pull_request:', 1)[0],
        'pull_request': triggers.split('  pull_request:', 1)[1],
    }
    executed = set(re.findall(r'python3\s+(scripts/[A-Za-z0-9_./-]+\.py)', workflow))
    executed.update('examples/' + name + '.rs' for name in re.findall(r'--example\s+([A-Za-z0-9_-]+)', workflow))
    executed.update({
        'scripts/tests/test_verify_handoff_eio_results.py',
        'scripts/tests/test_verify_configuration_eio_results.py',
        'scripts/tests/test_verify_reclamation_eio_results.py',
        'scripts/tests/test_sync_eio_workflow_paths.py',
    })
    missing = {}
    for event, section in sections.items():
        patterns = re.findall(r'^\s+- "([^\"]+)"', section, re.MULTILINE)
        missing[event] = sorted(path for path in executed
                                if not any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns))
    return executed, missing


class FilesystemWorkflowPathTests(unittest.TestCase):
    def test_push_and_pull_request_watch_executed_probes_and_scripts(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / '.github/workflows/sync-eio.yml').read_text()
        executed, missing = missing_dependencies(workflow)
        self.assertTrue(executed)
        for event, paths in missing.items():
            with self.subTest(event=event):
                self.assertEqual(paths, [], f'{event} does not watch workflow dependencies')

    def test_missing_probe_is_reported_for_affected_event(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / '.github/workflows/sync-eio.yml').read_text()
        entry = '      - "examples/handoff_failure_probe.rs"\n'
        self.assertEqual(workflow.count(entry), 2)
        _, missing = missing_dependencies(workflow.replace(entry, '', 1))
        self.assertIn('examples/handoff_failure_probe.rs', missing['push'])
        self.assertEqual(missing['pull_request'], [])
        _, missing = missing_dependencies(workflow.replace(entry, '', 2))
        self.assertIn('examples/handoff_failure_probe.rs', missing['pull_request'])


if __name__ == '__main__':
    unittest.main()
