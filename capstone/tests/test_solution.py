import copy
import json
from pathlib import Path
import shutil
import pytest
from capstone.solution import run, main, discover
from d2.d2_2_sources_sinks.solution import JsonLinesSource, Source
from d1.d1_5_retry.solution import RetryExhausted
from d2.d2_6_registry.solution import ConfigError


def config(tmp_path, checks=None):
    return {'source': {'type':'jsonl','path':'input/*.jsonl'},
            'transforms':[{'type':'cast','schema':{'id':'int','amount':'positive_amount','email':'email'}},
                          {'type':'date','column':'ordered_at','source_tz':'Asia/Tokyo'}],
            'checks':checks or [{'type':'not_null','column':'email'}],
            'sink':{'type':'jsonl','path':'out'},
            'partition':{'timestamp':'ordered_at','source_tz':'Asia/Tokyo'},
            'incremental':{'watermark':'state/watermark.json','report':'state/report.json','quarantine':'state/rejects.jsonl'},
            'retry':{'max_attempts':3,'base':1,'jitter':0}}


def order(i=1, **kwargs):
    return {'id':str(i),'amount':'2.50','email':'aru@example.com','ordered_at':'2026-11-01T01:30:00',**kwargs}


def put(tmp_path, name, rows):
    path = tmp_path / 'input' / name
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(''.join(json.dumps(r)+'\n' for r in rows),encoding='utf-8')
    return path


def output_rows(tmp_path):
    return [json.loads(line) for path in (tmp_path/'out').rglob('*.jsonl') for line in path.read_text(encoding='utf-8').splitlines()]


def test_first_rerun_new_late_and_reports(tmp_path):
    put(tmp_path,'orders_2026-11-01.jsonl',[order(1)])
    cfg = config(tmp_path)
    result = run(cfg,base_dir=tmp_path)
    assert (result['status'],result['rows_in'],result['rows_out'],result['rejected']) == ('success',1,1,0)
    assert list((tmp_path/'out/year=2026/month=10/day=31').glob('*.jsonl'))
    assert run(cfg,base_dir=tmp_path)['rows_in'] == 0
    put(tmp_path,'orders_2026-11-02.jsonl',[order(2)])
    assert run(cfg,base_dir=tmp_path)['files_processed'] == 1
    put(tmp_path,'orders_2026-10-01.jsonl',[order(3)])
    assert run(cfg,base_dir=tmp_path)['rows_out'] == 1
    assert len(output_rows(tmp_path)) == 3
    report = json.loads((tmp_path/'state/report.json').read_text(encoding='utf-8'))
    assert report['start'].endswith('+00:00') and report['end'].endswith('+00:00')
    assert report['duration_seconds'] >= 0 and report['quality_rows'] == 3
    assert {s['name'] for s in report['steps']} >= {'source','partition','sink'}


def test_bad_records_and_corrupt_file_do_not_abort(tmp_path):
    put(tmp_path,'good.jsonl',[order(),order(2,amount='abc'),order(3,email='bad'),order(4,ordered_at='2026-13-01'),order(5)])
    bad = tmp_path/'input/corrupt.jsonl'
    bad.write_bytes(b'not json\n[]\n\xff\xfe\n')
    result = run(config(tmp_path),base_dir=tmp_path)
    assert (result['rows_in'],result['rows_out'],result['rejected']) == (8,2,6)
    rejected = [json.loads(l) for l in (tmp_path/'state/rejects.jsonl').read_text(encoding='utf-8').splitlines()]
    assert len(rejected) == 6 and all(r['reason'] and r['step'] for r in rejected)
    before = (tmp_path/'state/rejects.jsonl').read_bytes()
    assert run(config(tmp_path),base_dir=tmp_path)['rejected'] == 0
    assert (tmp_path/'state/rejects.jsonl').read_bytes() == before


@pytest.mark.parametrize('severity,status', [('error','failed'),('warning','success')])
def test_quality_failure_and_warning_are_persistent(tmp_path,severity,status):
    put(tmp_path,'data.jsonl',[order()])
    cfg = config(tmp_path,[{'type':'not_null','column':'missing','severity':severity}])
    assert run(cfg,base_dir=tmp_path)['status'] == status
    rerun = run(cfg,base_dir=tmp_path)
    assert rerun['status'] == status and rerun['rows_out'] == 0
    assert rerun['checks'][0]['failed_count'] == 1


def test_retry_transient_midstream_discards_partial_stage(tmp_path):
    class Flaky(JsonLinesSource, name='cap_flaky'):
        attempts = 0
        def read(self):
            type(self).attempts += 1
            for row in super().read():
                yield row
                if type(self).attempts < 3:
                    raise TimeoutError('source interrupted')
    try:
        put(tmp_path,'data.jsonl',[order(1),order(2)])
        cfg = config(tmp_path)
        cfg['source']['type'] = 'cap_flaky'
        waits=[]
        result = run(cfg,base_dir=tmp_path,sleep=waits.append)
        assert result['rows_out'] == 2 and len(output_rows(tmp_path)) == 2
        assert waits == [1,2] and Flaky.attempts == 3
    finally:
        Source.registry.pop('cap_flaky')


def test_exhaustion_writes_failed_report_without_watermark(tmp_path):
    class Broken(JsonLinesSource,name='cap_broken'):
        def open(self):
            raise ConnectionError('down')
    try:
        put(tmp_path,'data.jsonl',[order()])
        cfg=config(tmp_path)
        cfg['source']['type']='cap_broken'
        with pytest.raises(RetryExhausted) as exc:
            run(cfg,base_dir=tmp_path,sleep=lambda _:None)
        assert isinstance(exc.value.__cause__,ConnectionError)
        assert not (tmp_path/'state/watermark.json').exists()
        report=json.loads((tmp_path/'state/report.json').read_text(encoding='utf-8'))
        assert report['status']=='failed' and 'RetryExhausted' in report['error']
    finally:
        Source.registry.pop('cap_broken')


@pytest.mark.parametrize('point',['published','before_watermark'])
def test_crash_recovery_no_duplicate_shards(tmp_path,point):
    put(tmp_path,'data.jsonl',[order(1),order(2,ordered_at='2026-11-02T18:00:00Z')])
    cfg=config(tmp_path)
    def crash(stage,path):
        if stage==point:
            raise RuntimeError('simulated crash')
    with pytest.raises(RuntimeError,match='simulated crash'):
        run(cfg,base_dir=tmp_path,commit_hook=crash)
    assert not (tmp_path/'state/watermark.json').exists()
    result=run(cfg,base_dir=tmp_path)
    assert result['rows_out']==2 and len(output_rows(tmp_path))==2
    assert run(cfg,base_dir=tmp_path)['rows_out']==0


def test_changed_file_replaces_old_partitions(tmp_path):
    put(tmp_path,'data.jsonl',[order(1)])
    cfg=config(tmp_path)
    run(cfg,base_dir=tmp_path)
    put(tmp_path,'data.jsonl',[order(2,ordered_at='2026-12-01T00:00:00Z')])
    result=run(cfg,base_dir=tmp_path)
    assert result['rows_out']==1
    assert [r['id'] for r in output_rows(tmp_path)]==[2]
    assert run(cfg,base_dir=tmp_path)['quality_rows']==1


def test_invalid_config_does_not_create_output(tmp_path):
    cfg=config(tmp_path)
    cfg['transforms'][0]={'type':'cast'}
    with pytest.raises(ConfigError,match=r'transforms\[0\].schema'):
        run(cfg,base_dir=tmp_path)
    assert not (tmp_path/'out').exists()


def test_cli_and_relative_config_paths(tmp_path,capsys):
    put(tmp_path,'data.jsonl',[order()])
    p=tmp_path/'config.json'
    p.write_text(json.dumps(config(tmp_path)),encoding='utf-8')
    assert main([str(p)])==0
    assert json.loads(capsys.readouterr().out)['status']=='success'
    cfg=config(tmp_path,[{'type':'not_null','column':'missing'}])
    p.write_text(json.dumps(cfg),encoding='utf-8')
    assert main([str(p)])==1
    assert json.loads(capsys.readouterr().out)['status']=='failed'


def test_week_fixture_both_tracks_and_late_arrival(tmp_path):
    project=Path(__file__).resolve().parents[1]
    target=tmp_path/'demo'
    shutil.copytree(project/'examples',target/'examples')
    shutil.copytree(project/'configs',target/'configs')
    orders=run(target/'configs/orders.json')
    assert orders['files_processed']==8 and orders['rows_out']==14
    assert orders['rejected']>0 and orders['status']=='success'
    order_rows = [json.loads(line) for path in (target/'work/orders/output').rglob('*.jsonl')
                  for line in path.read_text(encoding='utf-8').splitlines()]
    assert len(order_rows)==14
    assert run(target/'configs/orders.json')['rows_out']==0
    shutil.copyfile(target/'examples/late/orders_2026-10-25.jsonl',target/'examples/data/orders/orders_2026-10-25.jsonl')
    assert run(target/'configs/orders.json')['rows_out']==1
    clicks=run(target/'configs/clicks.json')
    assert clicks['rows_out']==21 and clicks['status']=='success'
    click_rows = [json.loads(line) for path in (target/'work/clicks/output').rglob('*.jsonl')
                  for line in path.read_text(encoding='utf-8').splitlines()]
    assert len(click_rows)==21
    assert run(target/'configs/clicks.json')['rows_out']==0


@pytest.mark.parametrize('sink_type',['csv','partitioned'])
def test_other_registered_file_sinks(tmp_path,sink_type):
    put(tmp_path,'data.jsonl',[order(1),order(2,ordered_at='2026-11-02T18:00:00Z')])
    cfg=config(tmp_path)
    cfg['sink']['type']=sink_type
    result=run(cfg,base_dir=tmp_path)
    assert result['rows_out']==2 and result['quality_rows']==2
    assert run(cfg,base_dir=tmp_path)['rows_out']==0


def test_pipeline_changes_require_new_watermark(tmp_path):
    put(tmp_path,'data.jsonl',[order()])
    cfg=config(tmp_path)
    run(cfg,base_dir=tmp_path)
    cfg['partition']['source_tz']='UTC'
    with pytest.raises(ConfigError,match='pipeline changed'):
        run(cfg,base_dir=tmp_path)


def test_discover_missing_and_glob(tmp_path):
    with pytest.raises(FileNotFoundError):
        discover(tmp_path/'missing')
    assert discover(str(tmp_path/'*.jsonl'))==[]


@pytest.mark.parametrize('section,value,path', [
    ('source', None, 'source'),
    ('sink', {}, 'sink.path'),
    ('incremental', [], 'incremental'),
    ('incremental', {'watermark': None}, 'incremental.watermark'),
    ('partition', [], 'partition'),
    ('partition', {'timestamp': None}, 'partition.timestamp'),
    ('retry', [], 'retry'),
])
def test_malformed_options_report_config_path_before_output(tmp_path, section, value, path):
    cfg = config(tmp_path)
    cfg[section] = value
    with pytest.raises(ConfigError) as exc:
        run(cfg, base_dir=tmp_path)
    assert path in str(exc.value)
    assert not (tmp_path/'out').exists()


@pytest.mark.parametrize('contents', ['{', '[]', '{"version":1,"files":{"x":null}}'])
def test_corrupt_watermark_is_reported_without_overwriting_it(tmp_path, contents):
    put(tmp_path, 'data.jsonl', [order()])
    state = tmp_path/'state/watermark.json'
    state.parent.mkdir()
    state.write_text(contents, encoding='utf-8')
    with pytest.raises(ConfigError, match='incremental.watermark'):
        run(config(tmp_path), base_dir=tmp_path)
    assert state.read_text(encoding='utf-8') == contents
    assert not (tmp_path/'out').exists()


@pytest.mark.parametrize('source_type', ['csv', 'jsonl'])
def test_source_parser_errors_do_not_stop_capstone(tmp_path, source_type):
    import csv
    cfg = config(tmp_path)
    cfg['source'] = {'type': source_type, 'path': f'input/*.{source_type}'}
    path = tmp_path/'input'/f'data.{source_type}'
    path.parent.mkdir()
    if source_type == 'csv':
        path.write_bytes(b'id,amount,email,ordered_at\n1,1,a@example.com,2026-10-01\n'
                         + b'2,1,' + b'x'*(csv.field_size_limit()+1) + b',2026-10-01\n'
                         + b'3,1,\xff,2026-10-01\n4,2,b@example.com,2026-10-01\n')
    else:
        deep = b'['*2000 + b'0' + b']'*2000
        path.write_bytes(json.dumps(order(1)).encode('utf-8') + b'\n' + deep + b'\n\xff\n'
                         + json.dumps(order(4)).encode('utf-8') + b'\n')
    result = run(cfg, base_dir=tmp_path)
    assert (result['rows_in'], result['rows_out'], result['rejected']) == (4, 2, 2)
    assert len(output_rows(tmp_path)) == 2
    assert run(cfg, base_dir=tmp_path)['rows_in'] == 0


@pytest.mark.parametrize('entrypoint', [['-m', 'minietl'], ['minietl.py'], ['-m', 'capstone.solution']])
def test_real_cli_success_repeat_and_missing_config(tmp_path, entrypoint):
    import subprocess
    import sys
    put(tmp_path, 'data.jsonl', [order()])
    path = tmp_path/'config.json'
    path.write_text(json.dumps(config(tmp_path)), encoding='utf-8')
    root = Path(__file__).resolve().parents[2]
    def invoke(config_path):
        return subprocess.run([sys.executable, '-B', *entrypoint, str(config_path)],
                              cwd=root, text=True, encoding='utf-8', capture_output=True)
    first = invoke(path)
    assert first.returncode == 0, first.stderr + first.stdout
    assert json.loads(first.stdout)['rows_out'] == 1
    second = invoke(path)
    assert second.returncode == 0 and json.loads(second.stdout)['rows_out'] == 0
    missing = invoke(tmp_path/'missing.json')
    assert missing.returncode == 1
    assert json.loads(missing.stdout)['status'] == 'failed'
    assert 'Traceback' not in missing.stdout + missing.stderr
