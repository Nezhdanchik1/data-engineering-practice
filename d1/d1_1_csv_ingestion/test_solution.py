import csv
import pytest
from d1.d1_1_csv_ingestion.solution import ingest, MissingField, InvalidCSV

HEADER = 'order_id,customer_email,phone,amount,currency,order_date\n'
SAMPLE = HEADER + '1001,aru@example.com,+7 701 123 4567,120.50,USD,2026-10-01\n1002,dana@example,8(701)1234568,99.00,USD,01/10/2026\n1003,timur@example.com,7011234569,abc,EUR,2026-10-02T14:30:00+02:00\n1004,,+7-701-123-4570,45.00,KZT,2026-13-01\n1001,aru@example.com,+7 701 123 4567,120.50,USD,2026-10-01\n'

def test_sample(tmp_path, capsys):
    path = tmp_path / 'orders.csv'
    path.write_text(SAMPLE, encoding='utf-8')
    result = ingest(path)
    assert (result['rows_read'], result['accepted'], result['rejected']) == (5, 2, 3)
    assert result['reasons'] == {'InvalidAmount': 1, 'MissingField': 1, 'DuplicateKey': 1}
    with (tmp_path / 'clean.csv').open(encoding='utf-8') as stream:
        assert [r['order_id'] for r in csv.DictReader(stream)] == ['1001', '1002']
    with (tmp_path / 'rejects.csv').open(encoding='utf-8') as stream:
        assert all(row['reason'] for row in csv.DictReader(stream))
    assert 'rows_read' in capsys.readouterr().out

@pytest.mark.parametrize('amount', ['-1', 'NaN', 'Infinity', 'abc', '-Infinity'])
def test_bad_amount(tmp_path, amount):
    p = tmp_path / 'input.csv'
    p.write_text(HEADER + f'1,a@b.com,7011234567,{amount},USD,2026-10-01\n', encoding='utf-8')
    assert ingest(p)['reasons'] == {'InvalidAmount': 1}

def test_fallback(tmp_path):
    p = tmp_path / 'input.csv'
    p.write_bytes((HEADER + '1,ару@example.com,7011234567,1,USD,2026-10-01\n').encode('cp1251'))
    assert ingest(p, fallback_encoding='cp1251')['accepted'] == 1

def test_missing_header(tmp_path):
    p = tmp_path / 'input.csv'
    p.write_text('wrong\n', encoding='utf-8')
    with pytest.raises(MissingField):
        ingest(p)


@pytest.mark.parametrize('email', ['x' * (csv.field_size_limit() + 1), '"broken"suffix'])
def test_csv_parser_error_quarantines_record_and_keeps_next_row(tmp_path, email):
    path = tmp_path / 'orders.csv'
    path.write_text(HEADER
                    + '1,a@example.com,7011234567,1,USD,2026-10-01\n'
                    + f'2,{email},7011234567,1,USD,2026-10-01\n'
                    + '3,b@example.com,7011234568,2,USD,2026-10-01\n', encoding='utf-8')
    result = ingest(path)
    assert (result['rows_read'], result['accepted'], result['rejected']) == (3, 2, 1)
    assert result['reasons'] == {'InvalidCSV': 1}
    with (tmp_path / 'clean.csv').open(encoding='utf-8', newline='') as stream:
        assert [row['order_id'] for row in csv.DictReader(stream)] == ['1', '3']
    with (tmp_path / 'rejects.csv').open(encoding='utf-8', newline='') as stream:
        rejects = list(csv.DictReader(stream))
    assert len(rejects) == 1 and 'InvalidCSV: line 3:' in rejects[0]['reason']


def test_malformed_header_has_clear_validation_error(tmp_path):
    path = tmp_path / 'orders.csv'
    path.write_text('"order_id"suffix,email\n', encoding='utf-8')
    with pytest.raises(InvalidCSV, match='invalid CSV header'):
        ingest(path)


def test_utf8_bom_header(tmp_path):
    path = tmp_path / 'orders.csv'
    path.write_text('\ufeff' + HEADER + '1,a@example.com,7011234567,1,USD,2026-10-01\n', encoding='utf-8')
    assert ingest(path)['accepted'] == 1
