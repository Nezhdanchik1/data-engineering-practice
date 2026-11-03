"""D1.3 Core: text cleaning and PII masking with regex.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from d1.d1_1_csv_ingestion.solution import ValidationError
from common import json_default


EMAIL = re.compile(r'[A-Za-z0-9.!#$%&\x27*+/=?^_`{|}~-]+@(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z]{2,63}')


def validate_email(value):
    if not isinstance(value, str) or not EMAIL.fullmatch(value):
        raise ValidationError(f'invalid email: {value!r}')
    return value


def normalise_phone(value):
    digits = re.sub(r'\D', '', str(value))
    if len(digits) == 10:
        digits = '7' + digits
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if not re.fullmatch(r'7\d{10}', digits, flags=re.ASCII):
        raise ValidationError(f'invalid phone: {value!r}')
    return re.sub(r'(7)(\d{3})(\d{3})(\d{4})', r'+\1-\2-\3-\4', digits)


def mask_email(value):
    validate_email(value)
    return re.sub(r'^([^@])[^@]*(@.+)$', lambda m: m[1] + '***' + m[2], value)


def mask_phone(value):
    phone = normalise_phone(value)
    return re.sub(r'^.*(\d{4})$', lambda m: '***-***-' + m[1], phone)


def strip_html(value):
    return re.sub(r'<[^>]*>', '', value)


def analytics_export(records, path):
    count, rejects = 0, []
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open('w', encoding='utf-8') as stream:
        for record in records:
            try:
                row = dict(record)
                row['customer_email'] = mask_email(row['customer_email'])
                row['phone'] = mask_phone(row['phone'])
                row['product_description'] = strip_html(row.get('product_description', ''))
                payload = json.dumps(row, ensure_ascii=False, allow_nan=False, default=json_default)
                stream.write(payload + '\n')
                count += 1
            except (ValueError, KeyError, TypeError) as exc:
                rejects.append({'record': record, 'reason': str(exc)})
    return {'accepted': count, 'rejected': len(rejects), 'rejects': rejects}
