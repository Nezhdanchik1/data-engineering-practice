"""D1.4 Advanced: incremental JSONL loading with an atomic watermark.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from common import atomic_json, utc_now

LOG = logging.getLogger(__name__)


class IncrementalLoader:
    """Chronological high-water mark; consumer must commit each file transactionally."""
    pattern = re.compile(r'events_(\d{4}-\d{2}-\d{2})\.jsonl')

    def __init__(self, folder, watermark, process):
        self.folder, self.watermark, self.process = Path(folder), Path(watermark), process

    def run(self):
        state = json.loads(self.watermark.read_text(encoding='utf-8')) if self.watermark.exists() else {}
        last = state.get('date', '')
        files = []
        for path in self.folder.iterdir():
            if path == self.watermark or not path.is_file():
                continue
            match = self.pattern.fullmatch(path.name)
            try:
                if not match:
                    raise ValueError('filename does not match')
                datetime.strptime(match[1], '%Y-%m-%d')
            except ValueError as exc:
                LOG.warning('ignored %s: %s', path, exc)
                continue
            if match[1] > last:
                files.append((match[1], path))
        result = dict(files=0, accepted=0, rejected=0)
        for date, path in sorted(files):
            with path.open('rb') as stream:
                for number, raw in enumerate(stream, 1):
                    try:
                        record = json.loads(raw.decode('utf-8'))
                    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
                        LOG.warning('%s:%s: %s', path, number, exc)
                        result['rejected'] += 1
                        continue
                    self.process(record)
                    result['accepted'] += 1
            atomic_json(self.watermark, {'date': date, 'updated_at': utc_now()})
            result['files'] += 1
        return result
