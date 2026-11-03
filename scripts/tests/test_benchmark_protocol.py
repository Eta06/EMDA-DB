import socket
import struct
import sys
from pathlib import Path
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmarks.clients import AdapterClient, exact
from benchmarks.embedded import pack, unpack
from benchmarks.telemetry import difference, parse_cpus


class ProtocolTests(unittest.TestCase):
    def request(self, response):
        client = AdapterClient.__new__(AdapterClient)
        client.sock, peer = socket.socketpair()
        client.sock.settimeout(2)
        def respond():
            try:
                header = exact(peer, 9)
                _, key_size, value_size = struct.unpack(">cII", header)
                exact(peer, key_size + value_size)
                for byte in response:
                    peer.sendall(bytes([byte]))
            finally:
                peer.close()
        thread = threading.Thread(target=respond)
        thread.start()
        try:
            return client.get(b"key")
        finally:
            client.close()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_fragmented_response_and_missing_key_are_distinct_from_empty_value(self):
        self.assertEqual(self.request(struct.pack(">BI", 0, 3) + b"abc"), b"abc")
        self.assertEqual(self.request(struct.pack(">BI", 0, 0)), b"")
        self.assertIsNone(self.request(struct.pack(">BI", 2, 0)))

    def test_partial_response_and_database_error_are_not_successes(self):
        with self.assertRaises(RuntimeError):
            self.request(struct.pack(">BI", 0, 4) + b"ab")
        with self.assertRaisesRegex(RuntimeError, "disk error"):
            self.request(struct.pack(">BI", 1, 10) + b"disk error")

    def test_checksum_rejects_corruption_at_every_payload_and_crc_byte(self):
        payload = pack(b"durable data")
        self.assertEqual(unpack(payload), b"durable data")
        self.assertIsNone(unpack(None))
        for index in range(len(payload)):
            corrupt = bytearray(payload)
            corrupt[index] ^= 1
            with self.assertRaises(RuntimeError):
                unpack(bytes(corrupt))


class NoiseTelemetryTests(unittest.TestCase):
    def test_cpu_ranges_and_invalid_reversed_ranges(self):
        self.assertEqual(parse_cpus("0,2-4,3"), {0, 2, 3, 4})
        for invalid in ("", "-1", "4-2", "1-2-3"):
            with self.assertRaises(ValueError): parse_cpus(invalid)

    def test_steal_fraction_does_not_double_count_guest_ticks(self):
        before = {"monotonic_ns": 0, "cpu_stat": {"usage_usec": 1000000, "nr_throttled": 3},
                  "proc_cpu": [100, 0, 50, 1000, 0, 0, 0, 20, 0, 0], "io_totals": {"wbytes": 100}}
        after = {"monotonic_ns": 1000000000, "cpu_stat": {"usage_usec": 1250000, "nr_throttled": 5},
                 "proc_cpu": [120, 0, 60, 1060, 5, 0, 0, 25, 1000, 0], "io_totals": {"wbytes": 200}}
        result = difference(before, after)
        self.assertEqual(result["vm_steal_percent"], 5)
        self.assertEqual(result["cgroup_cpu_cores_used"], 0.25)
        self.assertEqual(result["cgroup_cpu_delta"]["nr_throttled"], 2)
        self.assertEqual(result["io_delta"]["wbytes"], 100)


if __name__ == "__main__":
    unittest.main()
