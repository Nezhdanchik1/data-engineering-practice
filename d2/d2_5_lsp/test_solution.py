import pytest
from d2.d2_2_sources_sinks.solution import Sink, InMemorySink
from d2.d2_5_lsp.solution import BatchSink, DryRunSink, WarehouseSink

KINDS = sorted(Sink.registry)

@pytest.mark.parametrize('kind', KINDS)
@pytest.mark.parametrize('size', [0,1,3,101])
def test_every_sink_contract(tmp_path, kind, size):
    path = tmp_path / kind
    options = {'jsonl': {'path':path}, 'csv': {'path':path}, 'partitioned': {'path':path}}
    sink = Sink.registry[kind](**options.get(kind,{}))
    with sink:
        assert sink.write(iter([])) == 0
        count = sink.write(({'id':i,'ordered_at':'2026-10-01T00:00:00Z'} for i in range(size)))
        assert type(count) is int and count == size
        sink.flush()
    sink.flush()
    if isinstance(sink, InMemorySink) and not isinstance(sink, DryRunSink):
        assert len(sink.records) == size

def test_batch_close_flushes_partial():
    sink = BatchSink(100)
    with sink:
        assert sink.write([{'id':1}]) == 1
        assert sink.records == [] and len(sink.buffer) == 1
    assert sink.records == [{'id':1}] and sink.buffer == []

def test_batch_bad_size():
    with pytest.raises(ValueError):
        BatchSink(0)
