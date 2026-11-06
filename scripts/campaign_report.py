"""Atomic full reports with a lightweight per-case progress sidecar."""
import json
import os
from pathlib import Path
import tempfile
import time


class ReportWriter:
    def __init__(self, output, interval):
        self.output = Path(output)
        self.progress = Path(str(output) + '.progress.json')
        self.interval = interval
        self.last_snapshot = None
        self.snapshots = 0

    @staticmethod
    def _atomic_json(path, value):
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         prefix='.campaign-report-', delete=False) as file:
            temporary = Path(file.name)
            try:
                json.dump(value, file, indent=2)
                file.write('\n')
                file.flush()
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)

    def save(self, result, force=False):
        progress = {name: result.get(name) for name in ('revision', 'current', 'completed', 'failure')}
        now = time.monotonic()
        full_due = force or self.last_snapshot is None or now - self.last_snapshot >= self.interval
        if not force:
            self._atomic_json(self.progress, progress)
        if full_due:
            self.snapshots += 1
            result['full_report_snapshots'] = self.snapshots
            self._atomic_json(self.output, result)
            self.last_snapshot = now
        # A completed sidecar is published only after its full report.
        if force:
            self._atomic_json(self.progress, progress)
