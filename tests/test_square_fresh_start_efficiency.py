from contextlib import contextmanager
import json
import sqlite3
import time
import pytest

from square_desk.store import Store
from square_desk.efficient_storage import packed, unpacked
from square_desk.fresh_cluster import ensure, states_for, baseline_for, prepare_node, CONFIRM
from square_desk.models import utc, digest
from square_desk.postgres import Result, Row, DatabaseUnavailable
from test_square_database_failover import HttpJournal, Backend, SqliteTransport


def frame(symbol='BTC',end=None):
    end = end or int(utc()//900)*900
    return {'symbol':symbol,'quote':'USDT','source':'binance','interval':900,'fetched_at':utc(),
            'candles':[{'start':end-(200-i)*900,'end':end-(199-i)*900,
                        'open':100+i*.02,'high':101+i*.02,'low':99+i*.02,
                        'close':100.5+i*.02,'volume':1000+i} for i in range(200)]}


def test_observations_stay_ram_but_draft_evidence_is_exact_durable_and_atomic(monkeypatch,tmp_path):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT','true')
    path = str(tmp_path/'store.sqlite3')
    db = Store(path)
    snapshot = frame()
    db.set('cache:BTC:900',snapshot)
    db.set('latest_market',[{'symbol':'BTC','metrics':{'as_of':snapshot['candles'][-1]['end']}}])
    db.archive_snapshot(snapshot)
    assert db.conn.execute('SELECT COUNT(*) FROM entities').fetchone()[0]==0
    assert db.conn.execute("SELECT COUNT(*) FROM state WHERE key LIKE 'cache:%'").fetchone()[0]==0
    event = {'symbol':'BTC','as_of':snapshot['candles'][-1]['end'],'metrics':{'source':'binance'}}
    ident = db.insert('draft',{'title':'Fresh validated research','event':event},'approved')
    assert db.conn.execute("SELECT COUNT(*) FROM entities WHERE kind='snapshot'").fetchone()[0]==1
    key = digest(['binance','BTC',900,event['as_of']])
    db.close()
    db = Store(path)
    assert db.state('cache:BTC:900') is None
    assert db.get(ident)['status']=='approved'
    assert db.get(key)['payload']==snapshot
    db.close()


def test_no_stale_pin_after_outer_transaction_rolls_back(monkeypatch):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT','true')
    db = Store(':memory:')
    snapshot = frame()
    db.archive_snapshot(snapshot)
    event = {'symbol':'BTC','as_of':snapshot['candles'][-1]['end'],'metrics':{'source':'binance'}}
    with pytest.raises(ValueError):
        with db.transaction():
            db.insert('draft',{'event':event},id='rolled-back')
            raise ValueError('rollback')
    assert db.conn.execute('SELECT COUNT(*) FROM entities').fetchone()[0]==0
    db.insert('draft',{'event':event},id='durable')
    assert db.conn.execute("SELECT COUNT(*) FROM entities WHERE kind='snapshot'").fetchone()[0]==1
    db.close()


def test_signal_pins_evidence_and_missing_frame_is_not_fabricated(monkeypatch):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT','true')
    db = Store(':memory:')
    snapshot=frame()
    db.archive_snapshot(snapshot)
    db.insert('signal',{'symbol':'BTC','source':'binance','as_of':snapshot['candles'][-1]['end']},'watching')
    assert db.conn.execute("SELECT COUNT(*) FROM entities WHERE kind='snapshot'").fetchone()[0]==1
    with pytest.raises(ValueError,match='Exact chart'):
        db.insert('draft',{'event':{'symbol':'MISSING','metrics':{'source':'binance'},'as_of':1}})
    db.close()


def test_history_cache_updates_locally_and_transactional_reads_bypass_it(monkeypatch):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT','true')
    db=Store(':memory:')
    first=db.insert('draft',{'body':'unchanged'},'review')
    traces=[]
    db.conn.set_trace_callback(traces.append)
    assert len(db.list('draft'))==1
    count=lambda:sum(sql.startswith('SELECT * FROM entities WHERE kind=') for sql in traces)
    initial=count()
    for _ in range(20):
        assert db.list('draft',['review'])[0]['id']==first
    assert count()==initial
    db.update(first,status='approved')
    assert not db.list('draft',['review'])
    assert db.list('draft',['approved'])[0]['id']==first
    second=db.insert('draft',{'body':'new exact text'},'approved')
    assert {r['id'] for r in db.list('draft')}=={first,second}
    with db.transaction():
        db.list('draft')
    assert count()==initial+1
    db.close()


def test_critical_controls_and_budgets_never_use_history_cache(monkeypatch):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT','true')
    db=Store(':memory:')
    db.set('emergency_stop',False)
    assert db.state('emergency_stop') is False
    db.conn.execute("UPDATE state SET value='true' WHERE key='emergency_stop'")
    assert db.state('emergency_stop') is True
    assert db.reserve_budget('2026-10-12','posts',1,1)
    assert not db.reserve_budget('2026-10-12','posts',1,1)
    db.close()


def test_compressed_state_round_trip_and_legacy_reads(monkeypatch):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT','true')
    db=Store(':memory:')
    value={'attempts':[{'reason':'observed validation','notes':'exact '*100} for _ in range(100)]}
    db.set('large-audit-state',value)
    raw=db.conn.execute('SELECT value FROM state WHERE key=?',('large-audit-state',)).fetchone()[0]
    assert len(raw)<len(json.dumps(value))*.2
    assert db.state('large-audit-state')==value
    db.conn.execute('INSERT INTO state VALUES (?,?)',('legacy',json.dumps(value)))
    assert db.state('legacy')==value
    assert unpacked(packed(value))==value
    db.close()


class FreshTransport(SqliteTransport):
    def execute(self,sql,args=None):
        if sql.startswith('ALTER TABLE'):
            return Result([],0)  # PostgreSQL RLS DDL is not available in SQLite.
        if 'GENERATED BY DEFAULT AS IDENTITY' in sql:
            sql=sql.replace('BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY','INTEGER PRIMARY KEY')
        return super().execute(sql,args)


def test_authorized_empty_fresh_cluster_without_export_preserves_marker_on_redeploy(monkeypatch,tmp_path):
    monkeypatch.setenv('DESK_DB_FRESH_START',CONFIRM)
    remote=HttpJournal()
    journal=remote.journal()
    backends={name:Backend(str(tmp_path/(name+'.sqlite3'))) for name in ('neon','aiven','supabase')}
    factory=lambda name:FreshTransport(backends[name])
    try:
        ensure({name:name for name in backends},journal,factory=factory)
        row=remote.db.execute('SELECT seq FROM square_cluster').fetchone()
        assert row[0]==0
        for backend in backends.values():
            c=FreshTransport(backend)
            marker=json.loads(c.execute("SELECT value FROM state WHERE key='database_migration'").fetchone()[0])
            assert marker['origin']=='authorized_fresh_start' and marker['history_preserved'] is False
            assert c.execute('SELECT COUNT(*) FROM entities').fetchone()[0]==0
            c.close()
        journal.acquire()
        journal.append([{'sql':'INSERT INTO state VALUES (?,?)','args':['new-live-control','true'],'count':1}])
        # Retaining fresh-start ENV must NEVER clear/reset a started journal.
        ensure({name:name for name in backends},journal,factory=lambda url:pytest.fail('Normal redeploy must not rebuild providers'))
        assert remote.db.execute('SELECT seq FROM square_cluster').fetchone()[0]==1
    finally:
        journal.client.close()
        remote.db.close()


def test_fresh_start_is_explicit_and_refuses_nonempty_old_data(monkeypatch,tmp_path):
    remote=HttpJournal()
    journal=remote.journal()
    monkeypatch.delenv('DESK_DB_FRESH_START',raising=False)
    ensure({},journal,factory=lambda url:pytest.fail('Without explicit flag no setup is allowed'))
    assert remote.db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0]==0
    monkeypatch.setenv('DESK_DB_FRESH_START',CONFIRM)
    backend=Backend(str(tmp_path/'old.sqlite3'))
    db=Store(backend.path)
    db.insert('draft',{'body':'keep old confirmed publication'},'published',id='old-published')
    db.close()
    with pytest.raises(DatabaseUnavailable,match='at least two'):
        ensure({'neon':'neon','aiven':'aiven'},journal,factory=lambda name:FreshTransport(backend))
    c=FreshTransport(backend)
    assert c.execute('SELECT COUNT(*) FROM entities').fetchone()[0]==1
    c.close()
    journal.client.close()
    remote.db.close()


def test_fresh_start_cannot_overwrite_another_replica_cluster(tmp_path):
    seed={'cluster':'new-cluster','cutoff':int(utc())}
    states=states_for(seed)
    backend=Backend(str(tmp_path/'node.sqlite3'))
    c=FreshTransport(backend)
    prepare_node(c,seed['cluster'],baseline_for(states),states)
    with pytest.raises(DatabaseUnavailable,match='another history'):
        prepare_node(c,'another-cluster',baseline_for(states),states)
    c.close()
