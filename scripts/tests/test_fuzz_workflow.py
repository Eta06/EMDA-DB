import collections
import re
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class FuzzWorkflowTests(unittest.TestCase):
    def test_every_declared_target_runs_in_exactly_one_balanced_shard(self):
        workflow = (ROOT / '.github/workflows/fuzz.yml').read_text()
        manifest = tomllib.loads((ROOT / 'fuzz/Cargo.toml').read_text())
        declared = [entry['name'] for entry in manifest['bin']]
        blocks = re.split(r'(?=^      - name: )', workflow, flags=re.MULTILINE)
        assigned = []
        for block in blocks:
            targets = re.findall(r'cargo \+nightly-2026-11-03 fuzz run ([a-z_]+)', block)
            if not targets:
                continue
            self.assertEqual(len(targets), 1, 'one target per workflow step')
            shard = re.findall(r'matrix\.shard == ([1-4])', block)
            self.assertIn("!cancelled() && steps.fuzz_setup.outcome == 'success'", block)
            self.assertEqual(len(shard), 1, f'{targets[0]} must have one shard condition')
            assigned.append((targets[0], int(shard[0])))
        counts = collections.Counter(target for target, _ in assigned)
        self.assertEqual(set(counts), set(declared))
        self.assertTrue(all(count == 1 for count in counts.values()))
        self.assertEqual(len(declared), len(set(declared)))
        shard_counts = collections.Counter(shard for _, shard in assigned)
        self.assertEqual(set(shard_counts), {1, 2, 3, 4})
        self.assertLessEqual(max(shard_counts.values()) - min(shard_counts.values()), 1)
        self.assertIn('shard: [1, 2, 3, 4]', workflow)
        self.assertIn('fail-fast: false', workflow)

    def test_failure_artifacts_have_distinct_shard_names(self):
        workflow = (ROOT / '.github/workflows/fuzz.yml').read_text()
        self.assertIn('if: failure()', workflow)
        self.assertIn('name: recovery-fuzz-reproducer-${{ matrix.shard }}', workflow)
        self.assertIn('path: fuzz/artifacts/', workflow)
        self.assertIn('if-no-files-found: ignore', workflow)


if __name__ == '__main__':
    unittest.main()
