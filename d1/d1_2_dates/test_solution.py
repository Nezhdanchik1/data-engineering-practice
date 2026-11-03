import pytest
from d1.d1_2_dates.solution import normalise_ts, partition_path, write_partitions, InvalidDate

@pytest.mark.parametrize('value,zone,expected', [
    ('2026-10-01', 'UTC', '2026-10-01T00:00:00+00:00'),
    ('01/10/2026', 'UTC', '2026-10-01T00:00:00+00:00'),
    ('2026-10-02T14:30:00+02:00', 'Asia/Tokyo', '2026-10-02T12:30:00+00:00'),
    ('2026-10-31T23:30:00', 'Asia/Tokyo', '2026-10-31T14:30:00+00:00'),
    ('2026-11-01T01:30:00', 'Asia/Tokyo', '2026-10-31T16:30:00+00:00'),
])
def test_dates(value, zone, expected):
    assert normalise_ts(value, zone) == expected
    assert partition_path(expected) == 'year=2026/month=10/day=' + expected[8:10]

def test_dayfirst():
    assert normalise_ts('01/10/2026', 'UTC', False).startswith('2026-01-10')

@pytest.mark.parametrize('value', ['2026-13-01', '31/02/2026', 'bad', '', None])
def test_invalid(value):
    with pytest.raises(InvalidDate, match=repr(value)):
        normalise_ts(value, 'UTC')

def test_partition_writer(tmp_path):
    result = write_partitions([{'order_date': '2026-11-01T01:30:00'}, {'x': 1}], tmp_path, source_tz='Asia/Tokyo')
    assert (result['accepted'], result['rejected']) == (1, 1)
    assert (tmp_path / 'year=2026/month=10/day=31/records.jsonl').exists()

def test_nonexistent_dst():
    with pytest.raises(InvalidDate):
        normalise_ts('2026-03-08T02:30:00', 'America/New_York')


def test_bad_record_shapes_and_payloads_do_not_stop_partitions(tmp_path):
    import json
    from decimal import Decimal
    records = ['bad', 42, {'order_date': '2026-10-01', 'amount': float('nan')},
               {'order_date': '2026-10-01', 'amount': Decimal('1.20')},
               {'order_date': '2026-10-02', 'amount': Decimal('2.30')}]
    result = write_partitions(records, tmp_path)
    assert (result['accepted'], result['rejected']) == (2, 3)
    rows = [json.loads(line) for path in tmp_path.rglob('*.jsonl')
            for line in path.read_text(encoding='utf-8').splitlines()]
    assert [r['amount'] for r in rows] == ['1.20', '2.30']
