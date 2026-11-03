import copy
import json
import pytest
from d2.d2_6_registry.solution import build_pipeline, ConfigError
from d2.d2_2_sources_sinks.solution import InMemorySink, Source

BASE = {'source':{'type':'memory','records':[{'order_id':'1','amount':'2.50','email':'a@b.com'},{'order_id':'1','amount':'2.50','email':'a@b.com'}]}, 'transforms':[{'type':'cast','schema':'orders'},{'type':'dedupe','key':'order_id'}], 'checks':[{'type':'not_null','column':'email'}], 'sink':{'type':'memory'}}

def test_from_json(tmp_path):
    p = tmp_path / 'config.json'
    p.write_text(json.dumps(BASE), encoding='utf-8')
    pipeline = build_pipeline(p)
    assert pipeline.into().rows_out == 1
    assert pipeline.sink.records[0]['order_id'] == 1
    assert pipeline.checks[0].run(pipeline.sink.records).passed

@pytest.mark.parametrize('spec,path', [
    ({'type':'no_such'}, 'transforms[1].type'),
    ({'type':'dedupe'}, 'transforms[1].key'),
    ({'type':'dedupe','key':'id','extra':1}, 'transforms[1].extra'),
    ({'type':'cast','schema':'missing'}, 'transforms[1].schema'),
    ({'type':'cast','schema':{'id':'unknown'}}, 'transforms[1].schema.id'),
    ({'type':'filter','predicate':'unknown'}, 'transforms[1].predicate'),
    ({}, 'transforms[1].type'),
])
def test_exact_errors(spec, path):
    config = copy.deepcopy(BASE)
    config['transforms'][1] = spec
    with pytest.raises(ConfigError) as exc:
        build_pipeline(config)
    assert path in str(exc.value)

def test_plugin_discovered_without_dispatch_changes():
    class ExtraSource(Source, name='test_extra_source'):
        def open(self): pass
        def close(self): pass
        def read(self): yield {'value':42}
    try:
        config = {'source':{'type':'test_extra_source'},'sink':{'type':'memory'}}
        pipeline = build_pipeline(config)
        assert pipeline.into().rows_out == 1
        assert pipeline.sink.records == [{'value':42}]
    finally:
        Source.registry.pop('test_extra_source')

@pytest.mark.parametrize('text', ['{', '[]'])
def test_bad_config_json(tmp_path,text):
    p = tmp_path / 'bad.json'
    p.write_text(text,encoding='utf-8')
    with pytest.raises(ConfigError,match=r'\$'):
        build_pipeline(p)

def test_freshness_and_nested_chain_config():
    cfg = copy.deepcopy(BASE)
    cfg['checks'] = [{'type':'freshness','column':'ts','max_age':3600}]
    cfg['transforms'] = [{'type':'chain','transforms':[{'type':'rename','mapping':{'order_id':'id'}}]}]
    assert build_pipeline(cfg).into().rows_out == 2

def test_file_sink_serialises_decimal(tmp_path):
    cfg = copy.deepcopy(BASE)
    cfg['sink'] = {'type':'jsonl','path':str(tmp_path/'out.jsonl')}
    assert build_pipeline(cfg).into().rows_out == 1
    assert json.loads((tmp_path/'out.jsonl').read_text(encoding='utf-8'))['amount'] == '2.50'


@pytest.mark.parametrize('schema', [None, 42, {'id': []}, {'id': None}])
def test_invalid_schema_shape_reports_config_path(schema):
    cfg = copy.deepcopy(BASE)
    cfg['transforms'] = [{'type': 'cast', 'schema': schema}]
    with pytest.raises(ConfigError, match=r'transforms\[0\].schema'):
        build_pipeline(cfg)
