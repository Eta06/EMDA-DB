import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compare_engines import CASES, configure_cases, validate_result


class EngineComparisonConfigurationTests(unittest.TestCase):
    def test_selected_large_workloads_do_not_mutate_default_cases(self):
        baseline = [case.copy() for case in CASES]
        selected = configure_cases(['write_only', 'concurrent_mixed'], 100000)
        self.assertEqual([case['name'] for case in selected], ['concurrent_mixed', 'write_only'])
        self.assertTrue(all(case['keys'] == 100000 for case in selected))
        self.assertEqual(CASES, baseline)
        selected[0]['clients'] = 17
        self.assertEqual(next(case for case in CASES if case['name'] == 'concurrent_mixed')['clients'], 4)

    def test_noise_calibration_selects_only_its_configured_workload(self):
        cases = configure_cases(key_count=1000, noise_calibration=True)
        self.assertEqual([case['name'] for case in cases], ['concurrent_mixed'])
        self.assertEqual(cases[0]['keys'], 1000)

    def test_rejects_ambiguous_or_invalid_workloads(self):
        for kwargs in ({'names': ['write_only', 'write_only']}, {'names': ['unknown']},
                       {'key_count': 0}, {'key_count': 100001},
                       {'names': ['write_only'], 'noise_calibration': True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                configure_cases(**kwargs)

    def test_rejects_mismatched_or_incomplete_measured_results(self):
        case = configure_cases(['write_only'], 100000)[0]
        latency = {'count': case['clients'] * case['operations'], 'p50_us': 1, 'p95_us': 2,
                   'p99_us': 3, 'p99_9_us': 4, 'max_us': 5}
        result = {'backend': 'prototype', 'workload': 'mixed', 'clients': case['clients'],
                  'operations_per_client': case['operations'], 'keys': case['keys'],
                  'write_percent': 100, 'value_bytes': case['value_bytes'],
                  'completed': case['clients'] * case['operations'], 'ops_per_sec': 12,
                  'write': latency, 'read': {'count': 0}, 'create': {'count': 0},
                  'update': {'count': 0}, 'delete': {'count': 0}}
        self.assertIs(validate_result(result, 'prototype', case), result)
        wrong = {**result, 'keys': 250}
        with self.assertRaisesRegex(ValueError, 'requested workload'):
            validate_result(wrong, 'prototype', case)
        wrong = {**result, 'write': {**latency, 'count': latency['count'] - 1}}
        with self.assertRaisesRegex(ValueError, 'incomplete operations'):
            validate_result(wrong, 'prototype', case)


if __name__ == '__main__':
    unittest.main()
