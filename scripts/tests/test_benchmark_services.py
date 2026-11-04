import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmarks.services import LocalService, docker_logs


class DisposableServiceDiagnosticsTests(unittest.TestCase):
    def service(self):
        service = LocalService.__new__(LocalService)
        service.backend = "mongo"
        service.name = "disposable-test"
        service.env = {"EMDA_BENCH_PG_PASSWORD": "test-secret"}
        return service

    @patch("benchmarks.services.subprocess.run")
    def test_logs_are_bounded_and_use_local_docker(self, run):
        run.return_value = Mock(stdout="x" * 9000)
        self.assertEqual(len(docker_logs("disposable-test")), 8192)
        args, kwargs = run.call_args
        self.assertEqual(args[0][:3], ["docker", "--host=unix:///var/run/docker.sock", "logs"])
        self.assertNotIn("DOCKER_HOST", kwargs["env"])

    @patch("benchmarks.services.docker_logs", return_value="diagnostic test-secret")
    def test_state_and_logs_redact_password(self, _logs):
        diagnostic = self.service().startup_diagnostics({"Error": "test-secret", "ExitCode": 123})
        self.assertNotIn("test-secret", diagnostic)
        self.assertEqual(diagnostic.count("<redacted>"), 2)
        self.assertIn("123", diagnostic)

    @patch("benchmarks.services.time.sleep")
    @patch("benchmarks.services.docker_logs", return_value="diagnostic")
    @patch("benchmarks.services.docker")
    def test_exited_container_is_reported_without_waiting_for_timeout(self, docker, _logs, sleep):
        def command(*args, **_kwargs):
            if args[0] == "port":
                return "127.0.0.1:1234"
            if args[0] == "inspect":
                return json.dumps([{"State": {"Running": False, "Status": "exited", "ExitCode": 132}}])
            return ""

        docker.side_effect = command
        service = self.service()
        service.ready = Mock(side_effect=ConnectionError("not ready test-secret"))
        with self.assertRaisesRegex(RuntimeError, "ExitCode.*132") as error:
            service.start()
        self.assertNotIn("test-secret", str(error.exception))
        self.assertIsNone(error.exception.__cause__)
        sleep.assert_not_called()

    @patch("benchmarks.services.time.monotonic", side_effect=[0, 61])
    @patch("benchmarks.services.docker_logs", return_value="running")
    @patch("benchmarks.services.docker")
    def test_timeout_still_reports_state(self, docker, _logs, _monotonic):
        docker.side_effect = lambda *args, **_kwargs: (
            "127.0.0.1:1234" if args[0] == "port" else
            json.dumps([{"State": {"Running": True}}]) if args[0] == "inspect" else ""
        )
        with self.assertRaisesRegex(RuntimeError, "did not become ready.*Running.*True"):
            self.service().start()


if __name__ == "__main__":
    unittest.main()
