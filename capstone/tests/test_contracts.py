"""Every registered component is subject to the same interface contract."""
from datetime import timedelta
from collections.abc import Iterator
import json
import pytest
from d2.d2_2_sources_sinks.solution import Source, Sink, InMemorySink
from d2.d2_5_lsp.solution import DryRunSink
from d2.d2_3_transforms.solution import Transform, Filter, Rename, Cast, Deduplicate, NormaliseDate, TransformChain
from d2.d2_4_quality.solution import Check, CheckResult


@pytest.mark.parametrize('kind',sorted(Source.registry))
def test_all_sources(tmp_path,kind):
    p=tmp_path/'source'
    options={'memory':{'records':[{'id':'1'}]},'csv':{'path':p},'jsonl':{'path':p}}
    p.write_text('id\n1\n' if kind=='csv' else '{"id":"1"}\n',encoding='utf-8')
    source=Source.registry[kind](**options[kind])
    with source:
        iterator=source.read()
        assert isinstance(iterator,Iterator)
        assert list(iterator)==[{'id':'1'}]
    source.close()


@pytest.mark.parametrize('kind',sorted(Sink.registry))
def test_all_sinks(tmp_path,kind):
    options={'jsonl':{'path':tmp_path/'out'},'csv':{'path':tmp_path/'out'},'partitioned':{'path':tmp_path/'out'}}
    sink=Sink.registry[kind](**options.get(kind,{}))
    with sink:
        assert type(sink.write(iter([]))) is int
        assert sink.write(iter([]))==0
        assert sink.write(({'id':i,'ordered_at':'2026-11-01'} for i in range(3)))==3
        sink.flush()
    sink.flush()
    sink.close()


TRANSFORM_ARGS={'filter':{'predicate':lambda r:True},'rename':{'mapping':{'id':'key'}},'cast':{'schema':{'id':int}},'dedupe':{'key':'id'},'date':{'column':'ordered_at'},'chain':{'transforms':[Rename({'id':'key'})]}}

@pytest.mark.parametrize('kind',sorted(Transform.registry))
def test_all_transforms(kind):
    original={'id':'1','ordered_at':'2026-11-01'}
    transform=Transform.registry[kind](**TRANSFORM_ARGS[kind])
    assert isinstance(transform.apply(original),dict)
    assert original=={'id':'1','ordered_at':'2026-11-01'}
    transform.reset()
    assert isinstance(transform.apply(original),dict)


CHECK_ARGS={'not_null':{},'unique':{},'in_range':{'low':0,'high':10},'matches_pattern':{'regex':r'.+'},'freshness':{'max_age':timedelta(days=1)}}

@pytest.mark.parametrize('kind',sorted(Check.registry))
def test_all_checks(kind):
    check=Check.registry[kind]('x',**CHECK_ARGS[kind])
    result=check.run(iter([]))
    assert isinstance(result,CheckResult) and result.passed and result.failed_count==0
    result=check.run(({} for _ in range(7)))
    assert not result.passed and result.failed_count==7 and len(result.sample_failures)==5
