import json
import pytest
from d1.d1_3_text_cleaning.solution import normalise_phone, mask_phone, validate_email, analytics_export, ValidationError

@pytest.mark.parametrize('raw,expected', [('+7 701 123 4567', '+7-701-123-4567'), ('8(701)1234568', '+7-701-123-4568'), ('7011234569', '+7-701-123-4569'), ('+7-701-123-4570', '+7-701-123-4570')])
def test_phone(raw, expected):
    assert normalise_phone(raw) == expected
    assert mask_phone(raw) == '***-***-' + expected[-4:]

@pytest.mark.parametrize('value', ['dana@example', '', 'a@@example.com', 'a@-bad.com', ' x@y.com'])
def test_email_reject(value):
    with pytest.raises(ValidationError):
        validate_email(value)

@pytest.mark.parametrize('value', ['abc', '123', '+1 701 123 4567'])
def test_phone_reject(value):
    with pytest.raises(ValidationError):
        normalise_phone(value)

def test_export(tmp_path):
    p = tmp_path / 'analytics.jsonl'
    result = analytics_export([{'customer_email': 'aru@example.com', 'phone': '7011234567', 'product_description': '<b>Book</b>'}, {'customer_email': 'bad', 'phone': 'bad'}], p)
    assert result['accepted'] == result['rejected'] == 1
    data = json.loads(p.read_text(encoding='utf-8'))
    assert data == {'customer_email': 'a***@example.com', 'phone': '***-***-4567', 'product_description': 'Book'}


def test_export_skips_invalid_shapes_and_nonfinite_payloads(tmp_path):
    from decimal import Decimal
    good = {'customer_email': 'aru@example.com', 'phone': '7011234567', 'amount': Decimal('1.20')}
    path = tmp_path / 'analytics.jsonl'
    result = analytics_export(['bad', good, {**good, 'amount': float('nan')}, good], path)
    assert (result['accepted'], result['rejected']) == (2, 2)
    assert [json.loads(line)['amount'] for line in path.read_text(encoding='utf-8').splitlines()] == ['1.20', '1.20']
