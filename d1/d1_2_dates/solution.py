"""D1.2 Core: date normalisation and UTC partitions.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import json
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from d1.d1_1_csv_ingestion.solution import ValidationError
from common import json_default

UTC = timezone.utc


class InvalidDate(ValidationError):
    pass


def get_zone(name):
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        # Windows may lack a system IANA database; bundled data needs no package.
        if any(part in {'', '.', '..'} for part in name.split('/')):
            raise
        with ZipFile(Path(__file__).with_name('timezones.zip')) as archive:
            try:
                data = archive.read(name)
            except KeyError as exc:
                raise ZoneInfoNotFoundError(name) from exc
        return ZoneInfo.from_file(BytesIO(data), key=name)



def normalise_ts(value, source_tz='UTC', dayfirst=True):
    original = value
    try:
        if isinstance(value, datetime):
            dt = value
        else:
            value = str(value).strip()
            try:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                fmt = '%d/%m/%Y' if dayfirst else '%m/%d/%Y'
                dt = None
                for suffix in ('', ' %H:%M', ' %H:%M:%S'):
                    try:
                        dt = datetime.strptime(value, fmt + suffix)
                        break
                    except ValueError:
                        continue
                if dt is None:
                    raise ValueError('unsupported date format')
        if dt.tzinfo is None or dt.utcoffset() is None:
            zone = get_zone(source_tz)
            dt = dt.replace(tzinfo=zone, fold=0)
            # Nonexistent local times must not silently change on a UTC round trip.
            if dt.astimezone(UTC).astimezone(zone).replace(tzinfo=None) != dt.replace(tzinfo=None):
                raise ValueError('nonexistent local time')
        return dt.astimezone(UTC).isoformat()
    except (ValueError, TypeError, OverflowError, OSError, ZoneInfoNotFoundError) as exc:
        raise InvalidDate(f'invalid date {original!r}: {exc}') from exc


def partition_path(ts):
    dt = datetime.fromisoformat(normalise_ts(ts, 'UTC'))
    return f'year={dt:%Y}/month={dt:%m}/day={dt:%d}'


def write_partitions(records, root, *, timestamp='order_date', source_tz='UTC'):
    accepted = 0
    rejects = []
    for record in records:
        try:
            row = dict(record)
            row[timestamp] = normalise_ts(row[timestamp], source_tz)
            folder = Path(root) / partition_path(row[timestamp])
            folder.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(row, ensure_ascii=False, allow_nan=False, default=json_default)
            with (folder / 'records.jsonl').open('a', encoding='utf-8') as stream:
                stream.write(payload + '\n')
            accepted += 1
        except (ValueError, KeyError, TypeError) as exc:
            rejects.append({'record': record, 'reason': str(exc)})
    return {'accepted': accepted, 'rejected': len(rejects), 'rejects': rejects}
