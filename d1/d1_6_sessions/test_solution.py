from d1.d1_6_sessions.solution import sessionise, sessionise_file

def test_out_of_order_mixed_zones_midnight_and_boundary():
    def e(u, t):
        return {'user_id': u, 'timestamp': t, 'url': '/'}
    result = sessionise([e('a', '2026-11-01T00:20:00Z'), e('a', '2026-11-01T00:50:01Z'), e('a', '2026-11-01T01:50:00+02:00'), e('b', '2026-11-01T00:00:00Z'), e('a', 'bad'), {}])
    assert result['rejected'] == 2
    assert len(result['sessions']) == 3
    first = result['sessions'][0]
    assert first['start'].startswith('2026-10-31T23:50:00')
    assert first['duration_seconds'] == 1800 and first['page_count'] == 2

def test_file_bad_json_and_empty(tmp_path):
    p = tmp_path / 'clicks.jsonl'
    p.write_text('bad\n', encoding='utf-8')
    assert sessionise_file(p, tmp_path / 'sessions.csv')['rejected'] == 1
    assert sessionise([])['sessions'] == []


def test_file_survives_invalid_utf8_and_excessive_json_nesting(tmp_path):
    import json
    path = tmp_path / 'clicks.jsonl'
    first = {'user_id': 'a', 'timestamp': '2026-10-01T00:00:00Z', 'url': '/'}
    last = {**first, 'timestamp': '2026-10-01T00:10:00Z'}
    deep = b'[' * 2000 + b'0' + b']' * 2000
    path.write_bytes(json.dumps(first).encode('utf-8') + b'\n\xff\n' + deep
                     + b'\n' + json.dumps(last).encode('utf-8') + b'\n')
    result = sessionise_file(path, tmp_path / 'sessions.csv')
    assert result['rejected'] == 2
    assert result['sessions'][0]['page_count'] == 2
