from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmarks.document_contention import expected_row, run_clients, verify_reopened, verify_row
from document_contention_compare import run_embedded, validate


class DocumentContentionTests(unittest.TestCase):
    def test_concurrent_clients_keep_samples_and_match_the_reopened_fixture(self):
        documents, clients, cycles = 7, 4, 3
        rows = dict(expected_row(number, 0) for number in range(documents))

        def replace(identifier, body):
            if identifier not in rows:
                return 0
            rows[identifier] = body
            return 1

        def lookup(email):
            return [(identifier, body) for identifier, body in rows.items() if body["email"] == email]

        result = dict(engine="test", documents=documents, clients=clients, cycles_per_client=cycles)
        run_clients([(replace, lookup)] * clients, cycles, result, None)
        verify_reopened(sorted(rows.items()), lookup, documents, clients, cycles)
        result["reopen_verified"] = True
        validate(result, "test", clients, cycles, documents)
        self.assertIsNone(result["resources_after"]["server_cgroup_cpu"])

    def test_duplicate_missing_or_type_changed_results_fail(self):
        expected = expected_row(0, 1)
        identifier, body = expected
        for rows in ([], [expected, expected], [(identifier, {**body, "active": 1})],
                     [(identifier, {**body, "version": True})]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                verify_row(rows, expected)

    def test_failed_lookup_keeps_update_samples(self):
        result = {}
        with self.assertRaises(ValueError):
            run_clients([(lambda *_: 1, lambda _: [])], 2, result, None)
        self.assertEqual(len(result["clients_samples"][0]["update_ns"]), 1)
        self.assertEqual(result["clients_samples"][0]["query_ns"], [])
        self.assertIn("resources_after", result)

    def test_embedded_timeout_keeps_partial_process_output(self):
        run = {}
        timeout = subprocess.TimeoutExpired("binary", 1, output=b"partial result", stderr=b"error")
        with patch("document_contention_compare.subprocess.run", side_effect=timeout):
            with self.assertRaises(subprocess.TimeoutExpired):
                run_embedded(run, Path("binary"), "store", Path("."), 1, 1, 1)
        self.assertEqual(run["stdout"], "partial result")
        self.assertEqual(run["stderr"], "error")


if __name__ == "__main__":
    unittest.main()
