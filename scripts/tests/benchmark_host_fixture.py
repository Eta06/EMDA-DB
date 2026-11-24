"""Stub Linux host metadata in unit tests that do not run a benchmark."""

from contextlib import contextmanager
from pathlib import Path
import subprocess
from unittest.mock import patch


@contextmanager
def benchmark_host_metadata():
    read_text = Path.read_text
    check_output = subprocess.check_output

    def read(path, *args, **kwargs):
        if path == Path("/proc/cpuinfo"):
            return "model name : fixture CPU\n"
        return read_text(path, *args, **kwargs)

    def command(argv, *args, **kwargs):
        if argv[:2] == ["findmnt", "-J"]:
            return '{"filesystems":[{"target":"/fixture","fstype":"ext4"}]}'
        return check_output(argv, *args, **kwargs)

    with (
        patch.object(Path, "read_text", read),
        patch.object(subprocess, "check_output", command),
    ):
        yield
