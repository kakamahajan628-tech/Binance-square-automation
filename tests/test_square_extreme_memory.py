import json
import random
import pytest

from square_desk import efficient_storage as cache
from square_desk.models import digest
from square_desk.store import Store
from test_square_fresh_start_efficiency import frame


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT', 'true')
    store = Store(':memory:')
    yield store
    store.close()


def test_observation_compression_is_lossless_and_returns_independent_objects(db):
    value = frame()
    original = json.loads(json.dumps(value))
    db.set('cache:BTC:900', value)
    encoded, _ = db._volatile['cache:BTC:900']
    assert isinstance(encoded, bytes)
    assert len(encoded) < len(json.dumps(value).encode()) / 3
    value['candles'][0]['close'] = -1
    result = db.state('cache:BTC:900')
    assert result == original
    result['candles'][0]['close'] = -2
    assert db.state('cache:BTC:900') == original


def test_compressed_frame_pins_exact_evidence_durably_across_restart(monkeypatch, tmp_path):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT', 'true')
    path = str(tmp_path / 'memory.sqlite3')
    db = Store(path)
    value = frame()
    db.archive_snapshot(value)
    ident = digest(['binance', 'BTC', 900, value['candles'][-1]['end']])
    assert isinstance(db._frames[ident][0], bytes)
    db.get(ident)['payload']['candles'][0]['close'] = -1
    event = {'symbol':'BTC', 'as_of':value['candles'][-1]['end'], 'metrics':{'source':'binance'}}
    db.insert('draft', {'event':event}, 'approved')
    db.close()
    restored = Store(path)
    assert restored.get(ident)['payload'] == value
    restored.close()


def test_observation_budget_evicts_ram_only_and_missing_evidence_fails_closed(db, monkeypatch):
    monkeypatch.setattr(cache, 'OBSERVATION_CACHE_BYTES', 1200)
    db.set('emergency_stop', True)
    snapshot = frame()
    db.archive_snapshot(snapshot)
    assert not db._frames
    assert db._ram_bytes <= cache.OBSERVATION_CACHE_BYTES
    assert db.state('emergency_stop') is True
    event = {'symbol':'BTC', 'as_of':snapshot['candles'][-1]['end'], 'metrics':{'source':'binance'}}
    with pytest.raises(ValueError, match='Exact chart evidence unavailable'):
        db.insert('draft', {'event':event}, 'approved')
    assert db.list('draft') == []


def test_entry_limit_preserves_other_collection_and_accounting(db):
    snapshot = frame()
    db.archive_snapshot(snapshot)
    for i in range(2050):
        db.set('provider:' + str(i), {'status':'ok'})
    assert len(db._frames) == 1
    assert len(db._volatile) == 2048
    assert db._ram_bytes == sum(item[1] for part in (db._frames, db._volatile) for item in part.values())


def test_history_compression_filters_sorts_and_cannot_mutate_cached_rows(db):
    ids = [db.insert('draft', {'body':'exact research ' * 300, 'index':i}, 'review') for i in range(6)]
    for i, ident in enumerate(ids):
        db.conn.execute('UPDATE entities SET created=? WHERE id=?', (1000+i, ident))
    db.update(ids[2], status='approved')
    rows = db.list('draft', ['review'], limit=3)
    assert [r['id'] for r in rows] == [ids[5], ids[4], ids[3]]
    assert all(isinstance(entry['data'], bytes) for entry in db._history['draft']['rows'].values())
    rows[0]['payload']['body'] = 'changed outside cache'
    assert db.list('draft', ['review'], limit=1)[0]['payload']['body'] == 'exact research ' * 300
    cutoff = db.get(ids[4])['created']
    assert {r['id'] for r in db.list('draft', since=cutoff)} == {ids[4], ids[5]}
    assert db.list('draft', ['approved'])[0]['id'] == ids[2]


def test_history_update_delete_and_transaction_bypass_preserve_truth(db):
    ident = db.insert('draft', {'body':'first'}, 'review')
    db.list('draft')
    db.update(ident, payload={'body':'new value'}, status='approved')
    assert db.list('draft')[0]['payload'] == {'body':'new value'}
    assert db._history_bytes == sum(e['bytes'] for h in db._history.values() for e in h['rows'].values())
    db.conn.execute('UPDATE entities SET status=? WHERE id=?', ('published', ident))
    with db.transaction():
        assert db.list('draft')[0]['status'] == 'published'
    db.conn.execute('DELETE FROM entities WHERE id=?', (ident,))
    db._after_entity(ident)
    assert not db.list('draft')
    assert db._history_bytes == 0


def test_history_too_large_for_cache_still_returns_durable_rows(db, monkeypatch):
    ids = [db.insert('draft', {'body':str(i)*1000}, 'review') for i in range(4)]
    monkeypatch.setattr(cache, 'HISTORY_CACHE_BYTES', 100)
    assert {row['id'] for row in db.list('draft')} == set(ids)
    assert 'draft' not in db._history
    db.update(ids[0], status='approved')
    assert db.list('draft', ['approved'])[0]['id'] == ids[0]


def test_history_exceeding_budget_after_commit_drops_cache_and_preserves_write(db, monkeypatch):
    ident = db.insert('draft', {'body':'small'}, 'review')
    db.list('draft')
    monkeypatch.setattr(cache, 'HISTORY_CACHE_BYTES', 400)
    text = ''.join(random.Random(42).choices('abcdef0123456789', k=4000))
    db.update(ident, payload={'body':text})
    assert db._history == {}
    assert db.get(ident)['payload']['body'] == text


def test_cache_size_bound_does_not_turn_committed_update_into_failure(db, monkeypatch):
    ident = db.insert('draft', {'body':'small'}, 'review')
    db.list('draft')
    monkeypatch.setattr(cache, 'MAX_VALUE', 100)
    db.update(ident, payload={'body':'large'*100})
    assert not db._history
    assert db.get(ident)['payload']['body'] == 'large'*100


def test_cache_token_change_reloads_rows(db, monkeypatch):
    ident = db.insert('draft', {'body':'first'}, 'review')
    monkeypatch.setattr(db, '_token', lambda: ('one',))
    assert db.list('draft')[0]['status'] == 'review'
    db.conn.execute('UPDATE entities SET status=? WHERE id=?', ('approved', ident))
    monkeypatch.setattr(db, '_token', lambda: ('two',))
    assert db.list('draft')[0]['status'] == 'approved'


def test_corrupt_or_oversized_ram_cache_is_not_decoded(monkeypatch):
    monkeypatch.setattr(cache, 'MAX_VALUE', 20)
    with pytest.raises(ValueError):
        cache.cache_encode({'body':'too long'*30})
    import zlib
    with pytest.raises(ValueError):
        cache.cache_decode(zlib.compress(b'"' + b'a'*30 + b'"'))
    with pytest.raises(ValueError):
        cache.cache_decode(zlib.compress(b'{}') + b'extra')
