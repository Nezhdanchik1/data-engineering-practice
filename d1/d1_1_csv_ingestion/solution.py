"""D1.1 Core: resilient CSV ingestion and quarantine.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import csv
import io
import json
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path


class ValidationError(ValueError):
    pass


class MissingField(ValidationError):
    pass


class InvalidAmount(ValidationError):
    pass


class DuplicateKey(ValidationError):
    pass


class InvalidCSV(ValidationError):
    pass


def parse_amount(value, *, positive=False):
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0 or (positive and amount == 0):
            raise InvalidAmount(f'invalid amount: {value!r}')
        return amount
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise InvalidAmount(f'invalid amount: {value!r}') from exc


def ingest(path, *, output_dir=None, fallback_encoding='cp1251'):
    """D1.1 deliberately does not apply D1.2/D1.3 validation."""
    path = Path(path)
    target = Path(output_dir) if output_dir is not None else path.parent
    target.mkdir(parents=True, exist_ok=True)
    # Decode fully before opening outputs: fallback never duplicates a partial run.
    try:
        content = path.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        content = path.read_text(encoding=fallback_encoding)
    reader = csv.DictReader(io.StringIO(content.lstrip('\ufeff')), strict=True)
    try:
        fields = reader.fieldnames
    except csv.Error as exc:
        raise InvalidCSV(f'invalid CSV header: {exc}') from exc
    required = {'order_id', 'customer_email', 'amount'}
    if not fields or not required.issubset(fields) or len(set(fields)) != len(fields):
        raise MissingField('CSV header requires order_id, customer_email and amount')
    if path.resolve() in {(target / 'clean.csv').resolve(), (target / 'rejects.csv').resolve()}:
        raise ValueError('input and output must have different paths')
    summary = dict(rows_read=0, accepted=0, rejected=0, reasons={})
    reasons = Counter()
    seen = set()
    with (target / 'clean.csv').open('w', encoding='utf-8', newline='') as good, \
         (target / 'rejects.csv').open('w', encoding='utf-8', newline='') as bad:
        clean = csv.DictWriter(good, fields)
        rejects = csv.DictWriter(bad, fields + ['reason'], extrasaction='ignore')
        clean.writeheader()
        rejects.writeheader()
        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as exc:
                # Parser errors happen during next(), before ordinary validation.
                # Keep the diagnostic short: an oversized rejected field must not
                # make rejects.csv unreadable with the same CSV size limit.
                summary['rows_read'] += 1
                summary['rejected'] += 1
                reasons['InvalidCSV'] += 1
                rejects.writerow({'reason': f'InvalidCSV: line {reader.reader.line_num}: {exc}'})
                continue
            summary['rows_read'] += 1
            try:
                if None in row or any(row.get(k) is None or not row[k].strip() for k in fields):
                    raise MissingField('missing field or incorrect CSV column count')
                amount = parse_amount(row['amount'])
                key = row['order_id'].strip()
                if key in seen:
                    raise DuplicateKey(f'duplicate order_id: {key}')
                row['amount'] = str(amount)
                clean.writerow(row)
                seen.add(key)
                summary['accepted'] += 1
            except ValidationError as exc:
                reason = type(exc).__name__
                rejects.writerow({**row, 'reason': f'{reason}: {exc}'})
                reasons[reason] += 1
                summary['rejected'] += 1
    summary['reasons'] = dict(reasons)
    print(json.dumps(summary, ensure_ascii=False))
    return summary
