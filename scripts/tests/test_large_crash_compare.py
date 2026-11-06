import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmarks.large_crash import apply_changes, key, value, verify


class MemoryClient:
    def __init__(self, values):
        self.values = values

    def put(self, name, data):
        self.values[name] = data

    def get(self, name):
        return self.values.get(name)

    def delete(self, name):
        self.values.pop(name, None)


class LargeCrashVerificationTests(unittest.TestCase):
    def test_checks_every_seeded_value_update_and_deletion(self):
        count = 7
        client = MemoryClient({key(index): value(index) for index in range(count)})
        apply_changes(client, count)
        self.assertEqual(verify(client, count), {'seeded_values': 7, 'later_values': 90, 'deletions': 10})
        for corrupted_key, replacement in [
            (key(3), b'bad seed'),
            (key(count), b'old update'),
            (key(count + 50), None),
            (key(count + 99), b'reappeared deletion'),
        ]:
            broken = MemoryClient(copy.copy(client.values))
            if replacement is None:
                broken.delete(corrupted_key)
            else:
                broken.put(corrupted_key, replacement)
            with self.subTest(corrupted_key=corrupted_key), self.assertRaises(ValueError):
                verify(broken, count)


if __name__ == '__main__':
    unittest.main()
