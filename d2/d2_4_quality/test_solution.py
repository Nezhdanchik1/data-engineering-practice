from datetime import datetime, timezone, timedelta
import json
import pytest
from d2.d2_4_quality.solution import Check, CheckResult, NotNull, Unique, InRange, MatchesPattern, Freshness, QualitySuite

NOW = datetime(2026,11,3,tzinfo=timezone.utc)
FACTORIES = [lambda:NotNull('x'), lambda:Unique('x'), lambda:InRange('x',0,10), lambda:MatchesPattern('x',r'\d+'), lambda:Freshness('x',timedelta(days=1),now=lambda:NOW)]

@pytest.mark.parametrize('factory', FACTORIES)
def test_check_contract(factory):
    check = factory()
    assert check.run(iter([])) == CheckResult(type(check).__name__,True,0,[])
    failed = check.run(({} for _ in range(8)))
    assert failed.failed_count == 8 and not failed.passed
    assert len(failed.sample_failures) == 5

@pytest.mark.parametrize('check,rows,count', [
    (NotNull('x'), [{'x':0},{'x':''},{'x':None}], 2),
    (Unique('x'), [{'x':1},{'x':1},{'x':2}], 1),
    (InRange('x',0,10), [{'x':0},{'x':10},{'x':11},{'x':'bad'}], 2),
    (MatchesPattern('x',r'[A-Z]{3}'), [{'x':'USD'},{'x':'usd'},{'x':'USDx'}], 2),
    (Freshness('x',timedelta(days=1),now=lambda:NOW), [{'x':'2026-11-02T00:00:00Z'},{'x':'2026-11-04'},{'x':'bad'},{'x':'2026-10-01'}], 3),
])
def test_each_check(check, rows, count):
    assert check.run(rows).failed_count == count

def test_severity_report_and_generator(tmp_path):
    p = tmp_path / 'quality.json'
    warning = QualitySuite([NotNull('x',severity='warning')]).run(iter([{}]), p)
    assert warning['passed'] and warning['checks'][0]['failed_count'] == 1
    report = QualitySuite([NotNull('x'), Unique('x')]).run(iter([{},{}]), p)
    assert not report['passed'] and all(c['failed_count']==2 for c in report['checks'])
    assert datetime.fromisoformat(json.loads(p.read_text(encoding='utf-8'))['run_timestamp']).utcoffset() == timedelta(0)

@pytest.mark.parametrize('factory', [lambda:NotNull('x',severity='other'), lambda:InRange('x',10,0), lambda:Freshness('x',timedelta(seconds=-1)), lambda:MatchesPattern('x','[')])
def test_bad_configuration(factory):
    with pytest.raises((ValueError, __import__('re').error)):
        factory()

def test_contract_covers_registered_checks():
    assert set(Check.registry.values()) == {NotNull,Unique,InRange,MatchesPattern,Freshness}


def test_decimal_strings_keep_money_exact():
    from decimal import Decimal
    check=InRange('amount',Decimal('0.10'),Decimal('0.30'))
    assert check.run([{'amount':'0.10'},{'amount':'0.30'},{'amount':'0.30000000000000000001'},{'amount':'NaN'}]).failed_count==2
