import csv
import json
from collections.abc import Iterator
import pytest
from d2.d2_2_sources_sinks.solution import CsvSource, JsonLinesSource, InMemorySource, CsvSink, JsonLinesSink, InMemorySink

ROWS = [{'id':'1', 'name':'Ару'}, {'id':'2', 'name':'Dana'}]

@pytest.mark.parametrize('source_type', ['csv', 'jsonl', 'memory'])
@pytest.mark.parametrize('sink_type', ['csv', 'jsonl', 'memory'])
def test_matrix(tmp_path, source_type, sink_type):
    src = tmp_path / ('input.' + source_type)
    if source_type == 'csv':
        with src.open('w', encoding='utf-8', newline='') as f:
            w = csv.DictWriter(f, ['id','name'])
            w.writeheader()
            w.writerows(ROWS)
    elif source_type == 'jsonl':
        src.write_text(''.join(json.dumps(r) + '\n' for r in ROWS), encoding='utf-8')
    sources = {'csv': lambda: CsvSource(src), 'jsonl': lambda: JsonLinesSource(src), 'memory': lambda: InMemorySource(ROWS)}
    out = tmp_path / ('output.' + sink_type)
    sinks = {'csv': lambda: CsvSink(out), 'jsonl': lambda: JsonLinesSink(out), 'memory': InMemorySink}
    with sources[source_type]() as source, sinks[sink_type]() as sink:
        iterator = source.read()
        assert isinstance(iterator, Iterator)
        assert sink.write(iterator) == 2
    readers = {'csv': lambda: list(CsvSource(out).read()), 'jsonl': lambda: list(JsonLinesSource(out).read()), 'memory': lambda: sink.records}
    assert readers[sink_type]() == ROWS
    if hasattr(source, 'stream'):
        assert source.stream is None

@pytest.mark.parametrize('kind', ['jsonl', 'csv'])
def test_malformed_record_keeps_streaming(tmp_path, kind):
    p = tmp_path / kind
    content = '{"id":1}\nbad\n[]\n{"id":2}\n' if kind == 'jsonl' else 'id,name\n1,A\n2\n3,C\n'
    p.write_text(content, encoding='utf-8')
    source = {'jsonl': JsonLinesSource, 'csv': CsvSource}[kind](p)
    assert len(list(source.read())) == 2
    assert len(source.rejects) == (2 if kind == 'jsonl' else 1)

def test_stream_is_lazy(tmp_path):
    p = tmp_path / 'data.jsonl'
    p.write_text('{"id":1}\ninvalid\n', encoding='utf-8')
    source = JsonLinesSource(p)
    with source:
        rows = source.read()
        assert next(rows) == {'id':1}
        assert source.rejects == []
        assert list(rows) == [] and len(source.rejects) == 1

def test_context_propagates_exception(tmp_path):
    source = JsonLinesSource(tmp_path / 'missing')
    with pytest.raises(FileNotFoundError):
        with source:
            pass
    assert source.stream is None


@pytest.mark.parametrize('broken', [b'2,' + b'x' * (csv.field_size_limit()+1) + b'\n',
                                   b'2,"bad"suffix\n', b'2,\xff\n'])
def test_csv_source_continues_after_parser_or_encoding_error(tmp_path, broken):
    path = tmp_path / 'bad.csv'
    path.write_bytes(b'id,name\n1,A\n' + broken + b'3,C\n')
    source = CsvSource(path)
    assert list(source.read()) == [{'id': '1', 'name': 'A'}, {'id': '3', 'name': 'C'}]
    assert len(source.rejects) == 1 and source.rejects[0]['reason']


def test_jsonl_source_continues_after_deep_json(tmp_path):
    path = tmp_path / 'bad.jsonl'
    path.write_bytes(b'{"id":1}\n' + b'['*2000 + b'0' + b']'*2000 + b'\n{"id":2}\n')
    source = JsonLinesSource(path)
    assert list(source.read()) == [{'id': 1}, {'id': 2}]
    assert len(source.rejects) == 1 and source.rejects[0]['line'] == 2


def test_csv_source_accepts_utf8_bom_header(tmp_path):
    path = tmp_path / 'bom.csv'
    path.write_text('\ufeffid,name\n1,A\n', encoding='utf-8')
    assert list(CsvSource(path).read()) == [{'id': '1', 'name': 'A'}]
