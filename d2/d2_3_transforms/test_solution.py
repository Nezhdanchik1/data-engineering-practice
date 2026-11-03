import pytest
from d2.d2_2_sources_sinks.solution import InMemorySource, InMemorySink
from d2.d2_3_transforms.solution import Transform, Filter, Rename, Cast, Deduplicate, NormaliseDate, TransformChain

def test_pipeline_quarantine_and_drop():
    source = InMemorySource([{'id':'1','paid':True}, {'id':'bad','paid':True}, {'id':'1','paid':True}, {'id':'2','paid':False}, {'id':'3','paid':True}])
    sink = InMemorySink()
    stats = (source | Cast({'id':int}) | Deduplicate('id') | Filter(lambda r:r['paid'])).into(sink)
    assert (stats.rows_in, stats.rows_out, stats.rejected, stats.dropped) == (5,2,1,2)
    assert [r['id'] for r in sink.records] == [1,3]
    assert 'invalid literal' in stats.rejects[0]['reason']
    assert stats.steps[0]['rejected'] == 1

def test_transform_composition_and_copy():
    original = {'old':'4'}
    transform = Rename({'old':'id'}) | Cast({'id':int})
    assert transform.apply(original) == {'id':4}
    assert original == {'old':'4'}

def test_rename_collision():
    with pytest.raises(ValueError, match='collision'):
        Rename({'a':'b'}).apply({'a':1, 'b':2})

def test_missing_cast_and_dedupe_keys():
    for t in [Cast({'missing':int}), Deduplicate('missing')]:
        with pytest.raises(KeyError):
            t.apply({})

def test_pipeline_resets_dedupe_each_execution():
    pipeline = InMemorySource([{'id':1}, {'id':1}]) | Deduplicate('id')
    assert pipeline.into(InMemorySink()).rows_out == 1
    assert pipeline.into(InMemorySink()).rows_out == 1

@pytest.mark.parametrize('factory', [lambda: Filter(lambda r:True), lambda: Rename({'x':'y'}), lambda: Cast({'x':int}), lambda: Deduplicate('x'), lambda: NormaliseDate('timestamp'), lambda: TransformChain([Rename({'x':'y'})])])
def test_transform_contract(factory):
    t = factory()
    original = {'x':'1', 'timestamp':'2026-10-01'}
    result = t.apply(original)
    assert isinstance(result, dict) or result is None
    assert original == {'x':'1', 'timestamp':'2026-10-01'}
    t.reset()
    assert t.apply(original) is not None

def test_contract_covers_all_registered_transforms():
    assert set(Transform.registry.values()) == {Filter,Rename,Cast,Deduplicate,NormaliseDate,TransformChain}


def test_chain_preserves_transforms_from_one_shot_generator():
    chain = TransformChain(t for t in [Rename({'old': 'id'}), Cast({'id': int})])
    assert chain.apply({'old': '4'}) == {'id': 4}
