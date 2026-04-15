"""Test utilities."""

import sys
from contextlib import contextmanager
from io import StringIO
from typing import Any, Generator


@contextmanager
def capture(command: Any, *args: Any, **kwargs: Any) -> Generator[str, None, None]:
    """Capture stdout from a command execution."""
    out, sys.stdout = sys.stdout, StringIO()
    try:
        command(*args, **kwargs)
        sys.stdout.seek(0)
        yield sys.stdout.read()
    finally:
        sys.stdout = out
