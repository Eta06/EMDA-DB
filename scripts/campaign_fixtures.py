"""Keep a disposable fault fixture when its case fails or is interrupted."""
from contextlib import contextmanager
from pathlib import Path
import shutil
import tempfile


class PreservedFixtureError(RuntimeError):
    """A failed case whose synthetic fixture was retained for inspection."""


@contextmanager
def fault_fixture(parent, prefix):
    root = Path(tempfile.mkdtemp(prefix=prefix, dir=parent))
    try:
        yield root
    except Exception as error:
        raise PreservedFixtureError(f'{error}; disposable fixture retained at {root}') from error
    except BaseException:
        # Keep the directory if the campaign is interrupted as well.
        raise
    else:
        shutil.rmtree(root)
