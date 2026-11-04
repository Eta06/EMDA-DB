import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from update_input_compare import validate


class UpdateInputCompareTests(unittest.TestCase):
    def test_checks_workload_and_every_recorded_sample(self):
        result = {
            "scenario": "large",
            "cycles": 2,
            "verified": True,
            "encoded_bytes": 1048576,
            "samples_ns": [123, 456],
        }
        validate(result, "large", 2)
        for change in (
            {"scenario": "small"},
            {"cycles": 3},
            {"verified": False},
            {"encoded_bytes": 1048575},
            {"samples_ns": [123]},
            {"samples_ns": [123, -1]},
            {"samples_ns": [123, True]},
        ):
            bad = copy.deepcopy(result)
            bad.update(change)
            with self.assertRaises(ValueError):
                validate(bad, "large", 2)

    def test_checks_nested_scenario_encoding(self):
        validate(
            {"scenario": "nested-wide", "cycles": 1, "verified": True,
             "encoded_bytes": 4011, "samples_ns": [1]},
            "nested-wide", 1,
        )


if __name__ == "__main__":
    unittest.main()
