import json
import pytest
from d1.d1_4_incremental.solution import IncrementalLoader

def test_first_rerun_arrival_and_bad_line(tmp_path, caplog):
    state = tmp_path / 'state.json'
    (tmp_path / 'events_2026-10-17.jsonl').write_text('{"id":1}\nbroken\n{"id":2}\n', encoding='utf-8')
    (tmp_path / 'ignore.txt').write_text('', encoding='utf-8')
    (tmp_path / 'events_2026-99-99.jsonl').write_text('', encoding='utf-8')
    rows = []
    loader = IncrementalLoader(tmp_path, state, rows.append)
    assert loader.run() == {'files': 1, 'accepted': 2, 'rejected': 1}
    assert loader.run()['files'] == 0
    (tmp_path / 'events_2026-10-18.jsonl').write_text('{"id":3}\n', encoding='utf-8')
    assert loader.run()['accepted'] == 1
    assert len(rows) == 3
    assert 'events_2026-10-17.jsonl:2' in caplog.text
    assert 'ignore.txt' in caplog.text
    assert json.loads(state.read_text(encoding='utf-8'))['date'] == '2026-10-18'

def test_crash_preserves_watermark(tmp_path):
    state = tmp_path / 'state.json'
    state.write_text('{"date":"2026-10-16"}', encoding='utf-8')
    (tmp_path / 'events_2026-10-17.jsonl').write_text('{"id":1}\n{"id":2}\n', encoding='utf-8')
    before = state.read_bytes()
    def crash(row):
        if row['id'] == 2:
            raise RuntimeError('crash')
    with pytest.raises(RuntimeError, match='crash'):
        IncrementalLoader(tmp_path, state, crash).run()
    assert state.read_bytes() == before


def test_corrupt_encoding_and_deep_json_are_counted_per_line(tmp_path, caplog):
    path = tmp_path / 'events_2026-10-17.jsonl'
    nested = b'[' * 2000 + b'0' + b']' * 2000
    path.write_bytes(b'{"id":1}\n\xff\n' + nested + b'\n{"id":2}\n')
    rows = []
    loader = IncrementalLoader(tmp_path, tmp_path / 'state.json', rows.append)
    assert loader.run() == {'files': 1, 'accepted': 2, 'rejected': 2}
    assert rows == [{'id': 1}, {'id': 2}]
    assert 'events_2026-10-17.jsonl:2' in caplog.text
    assert loader.run()['files'] == 0
