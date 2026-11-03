"""Shared UTF-8 JSON and UTC utilities; task algorithms live in solution.py."""
import json
import os
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

UTC = timezone.utc


def utc_now():
    return datetime.now(UTC).isoformat()


def json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        # Lazy import keeps shared I/O helpers independent of task dependencies.
        from d1.d1_2_dates.solution import normalise_ts
        return normalise_ts(value)
    raise TypeError(f'not JSON serialisable: {type(value).__name__}')


def atomic_json(path, value):
    """Replace a JSON file only after its complete UTF-8 payload is durable."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name + '.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False, default=json_default)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
