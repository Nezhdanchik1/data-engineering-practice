"""D1.6 Challenge: UTC clickstream sessionisation.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from d1.d1_1_csv_ingestion.solution import MissingField, ValidationError
from d1.d1_2_dates.solution import normalise_ts


def sessionise(events, *, source_tz='UTC', inactivity=timedelta(minutes=30)):
    if inactivity.total_seconds() < 0:
        raise ValueError('inactivity must be nonnegative')
    users = defaultdict(list)
    rejects = []
    for event in events:
        try:
            if not event.get('user_id') or not event.get('url'):
                raise MissingField('user_id and url are required')
            dt = datetime.fromisoformat(normalise_ts(event['timestamp'], source_tz))
            users[str(event['user_id'])].append(dt)
        except (ValidationError, KeyError, TypeError, AttributeError) as exc:
            rejects.append({'record': event, 'reason': str(exc)})
    rows = []
    for user in sorted(users):
        start = end = None
        count = 0
        for dt in sorted(users[user]):
            if end is not None and dt - end > inactivity:
                rows.append(dict(user_id=user, start=start.isoformat(), end=end.isoformat(),
                                 duration_seconds=(end-start).total_seconds(), page_count=count))
                start, count = None, 0
            if start is None:
                start = dt
            end, count = dt, count + 1
        if end is not None:
            rows.append(dict(user_id=user, start=start.isoformat(), end=end.isoformat(),
                             duration_seconds=(end-start).total_seconds(), page_count=count))
    return {'sessions': rows, 'rejects': rejects, 'rejected': len(rejects)}


def sessionise_file(path, output, **kwargs):
    rejects = []
    def records():
        with Path(path).open('rb') as stream:
            for number, raw in enumerate(stream, 1):
                try:
                    yield json.loads(raw.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
                    rejects.append({'file': str(path), 'line': number, 'reason': str(exc)})
    result = sessionise(records(), **kwargs)
    result['rejects'].extend(rejects)
    result['rejected'] = len(result['rejects'])
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with Path(output).open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, ['user_id', 'start', 'end', 'duration_seconds', 'page_count'])
        writer.writeheader()
        writer.writerows(result['sessions'])
    return result
