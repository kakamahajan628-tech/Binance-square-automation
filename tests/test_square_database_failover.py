"""Real SQL journal protocol with fault injection; no cloud credentials/POSTs."""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
from threading import RLock
import time

import httpx
import pytest

from square_desk.database_failover import (FailoverConnection, META_SCHEMA, environment_nodes,
                                         postgres_url, catch_up, make_connection)
from square_desk.database_cluster import actual_digest, register, verified_checkpoint
from square_desk.database_transfer import TransferError
from square_desk.postgres import DatabaseUnavailable, Result, Row
from square_desk.store import Store
from square_desk.turso_journal import TursoJournal, encode, decode, chain
from square_desk.models import utc


CLUSTER, BASE = 'square-production-test', 'a' * 64


class HttpJournal:
    def __init__(self):
        self.db = sqlite3.connect(':memory:', isolation_level=None, check_same_thread=False)
        self.batons, self.closed, self.down, self.lose_append_ack = set(), False, False, False
        self.requests, self.lock = [], RLock()

    def handler(self, request):
        with self.lock:
            if self.down:
                return httpx.Response(429, text='private-token-and-host')
            body = json.loads(request.content)
            self.requests.append(body)
            baton, results, appended = body.get('baton'), [], False
            for item in body['requests']:
                if item['type'] == 'close':
                    self.batons.discard(baton)
                    baton = None
                    results.append({'type': 'ok', 'response': {'type': 'close'}})
                    continue
                stmt = item['stmt']
                args = [None if a['type'] == 'null' else int(a['value']) if a['type'] == 'integer'
                        else float(a['value']) if a['type'] == 'float' else a['value'] for a in stmt['args']]
                try:
                    cursor = self.db.execute(stmt['sql'], args)
                    columns = [{'name': d[0]} for d in cursor.description] if cursor.description else []
                    data = []
                    for values in cursor.fetchall():
                        data.append([{'type': 'null'} if v is None else {'type':'integer','value':str(v)}
                                     if type(v) is int else {'type':'float','value':v} if type(v) is float
                                     else {'type':'text','value':v} for v in values])
                    results.append({'type':'ok','response':{'type':'execute','result':
                        {'cols':columns,'rows':data,'affected_row_count':max(0,cursor.rowcount)}}})
                    if stmt['sql'].startswith('BEGIN'):
                        baton = 'connection-baton'
                        self.batons.add(baton)
                    if stmt['sql'].startswith('INSERT INTO square_journal'):
                        appended = True
                except sqlite3.Error:
                    results.append({'type':'error','error':{'message':'private SQL error'}})
            if appended and self.lose_append_ack:
                self.lose_append_ack = False
                raise httpx.ReadError('private URL/token', request=request)
            return httpx.Response(200, json={'baton':baton,'base_url':None,'results':results})

    def journal(self):
        return TursoJournal('libsql://test-org.turso.io', 'private-test-token', CLUSTER,
            client=httpx.Client(transport=httpx.MockTransport(self.handler)))


class Backend:
    def __init__(self, path):
        self.path, self.lock, self.down, self.lose_commit_ack = path, RLock(), False, False

    def connection(self, url=None):
        if self.down:
            raise DatabaseUnavailable('Provider unavailable')
        return SqliteTransport(self)


class SqliteTransport:
    def __init__(self, backend):
        self.backend = backend
        self.db = sqlite3.connect(backend.path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.active = False

    def execute(self, sql, args=None):
        if self.backend.down:
            raise DatabaseUnavailable('Provider failed')
        try:
            cursor = self.db.execute(sql, args or ())
            return Result([Row(dict(r)) for r in cursor.fetchall()] if cursor.description else [], cursor.rowcount)
        except sqlite3.IntegrityError:
            raise
        except sqlite3.Error:
            raise DatabaseUnavailable('Provider operation failed') from None

    @contextmanager
    def transaction(self):
        with self.backend.lock:
            self.db.execute('BEGIN IMMEDIATE')
            self.active = True
            changes_before = self.db.total_changes
            try:
                yield
            except BaseException:
                self.db.execute('ROLLBACK')
                raise
            else:
                self.db.execute('COMMIT')
                if self.backend.lose_commit_ack and self.db.total_changes > changes_before:
                    self.backend.lose_commit_ack = False
                    raise DatabaseUnavailable('Commit response lost')
            finally:
                self.active = False

    def close(self):
        self.db.close()


@pytest.fixture
def cluster(tmp_path):
    remote = HttpJournal()
    journal = remote.journal()
    journal.initialize(BASE)
    nodes = {}
    for name in ('aiven','supabase','neon'):
        path = str(tmp_path/(name+'.sqlite3'))
        store = Store(path)
        store.insert('draft', {'receipt':{'id':'confirmed'}, 'text':'exact approved bytes'}, 'published', id='published')
        store.insert('draft', {'text':'pending approved bytes'}, 'approved', id='pending')
        store.insert('media', {'png_base64':'exact-png-bytes'}, 'stored', id='png')
        store.set('paused', False)
        store.set('publisher_blocked', True)
        store.set('worker_lease', {'owner':'worker', 'until':utc()+120})
        store.set('database_migration', {'required':True,'status':'verified','backup_sha256':'c'*64,'verified_at':utc()})
        store.reserve_budget('2026-10-10','posts',7,50)
        store.conn.execute(META_SCHEMA)
        store.conn.execute('INSERT INTO square_replica_meta VALUES (1,?,?,0,?)',(CLUSTER,BASE,BASE))
        store.close()
        nodes[name] = Backend(path)
    engine = FailoverConnection({name:name for name in nodes},journal,
        factory=lambda name:nodes[name].connection(),background=False)
    yield remote,journal,nodes,engine
    engine.shutdown()
    journal.client.close()
    remote.db.close()


def query(engine, key):
    row = engine.execute('SELECT value FROM state WHERE key=?',(key,)).fetchone()
    return json.loads(row[0]) if row else None


def test_atomic_transaction_journal_before_pg_and_fallback_preserves_all_state(cluster):
    remote,journal,nodes,engine = cluster
    with engine.transaction():
        engine.execute('UPDATE entities SET status=? WHERE id=?',('sending','pending'))
        engine.execute('UPDATE budgets SET used=? WHERE day=? AND category=?',(8,'2026-10-10','posts'))
        engine.execute('INSERT INTO state VALUES (?,?)',('telegram_update', '123'))
    assert journal.head['seq'] == 1
    nodes['aiven'].down = True
    # Failure while selecting a node is safe to switch before running caller SQL.
    assert query(engine, 'paused') is False
    assert query(engine, 'telegram_update') == 123
    assert engine.current == 'supabase'
    assert query(engine,'publisher_blocked') is True
    assert engine.execute('SELECT used FROM budgets').fetchone()[0] == 8
    assert engine.execute('SELECT status FROM entities WHERE id=?',('pending',)).fetchone()[0] == 'sending'
    assert engine.execute('SELECT payload FROM entities WHERE id=?',('png',)).fetchone()[0] == '{"png_base64": "exact-png-bytes"}'
    assert query(engine,'worker_lease')['owner'] == 'worker'
    assert engine.diagnostics()['switches'] == 1


def test_lost_journal_ack_is_reconciled_without_repeating_user_mutation(cluster):
    remote,journal,nodes,engine = cluster
    remote.lose_append_ack = True
    with pytest.raises(DatabaseUnavailable):
        engine.execute('UPDATE budgets SET used=used+1 WHERE category=?',('posts',))
    assert engine.execute('SELECT used FROM budgets').fetchone()[0] == 8
    assert journal.head['seq'] == 1
    assert remote.db.execute('SELECT COUNT(*) FROM square_journal').fetchone()[0] == 1


def test_lost_pg_commit_ack_does_not_replay_increment_or_sending_intent(cluster):
    remote,journal,nodes,engine = cluster
    nodes['aiven'].lose_commit_ack = True
    with pytest.raises(DatabaseUnavailable):
        with engine.transaction():
            engine.execute('UPDATE budgets SET used=used+1 WHERE category=?',('posts',))
            engine.execute('UPDATE entities SET status=? WHERE id=?',('sending','pending'))
    assert engine.execute('SELECT used FROM budgets').fetchone()[0] == 8
    assert engine.execute('SELECT status FROM entities WHERE id=?',('pending',)).fetchone()[0] == 'sending'
    nodes['aiven'].down = False
    # Old node already committed. Catch-up checks the watermark and applies nothing.
    c = nodes['aiven'].connection()
    assert catch_up(c,journal,journal.head) == 1
    assert c.execute('SELECT used FROM budgets').fetchone()[0] == 8
    c.close()


def test_constraint_failure_rolls_back_without_committing_journal(cluster):
    remote,journal,nodes,engine = cluster
    with pytest.raises(sqlite3.IntegrityError):
        with engine.transaction():
            engine.execute('INSERT INTO state VALUES (?,?)',('new','1'))
            engine.execute('INSERT INTO state VALUES (?,?)',('paused','false'))
    assert query(engine,'new') is None
    assert journal.head['seq'] == 0


def test_coordinator_outage_stops_mutations_even_with_three_healthy_databases(cluster):
    remote,journal,nodes,engine = cluster
    remote.down = True
    with pytest.raises(DatabaseUnavailable) as exc:
        engine.execute('UPDATE budgets SET used=used+1 WHERE category=?',('posts',))
    assert 'private' not in str(exc.value)
    for backend in nodes.values():
        c = backend.connection()
        assert c.execute('SELECT used FROM budgets').fetchone()[0] == 7
        c.close()


def test_writer_fence_prevents_old_writer_after_authority_changes(cluster):
    remote,journal,nodes,engine = cluster
    other = remote.journal()
    with pytest.raises(DatabaseUnavailable):
        with engine.transaction():
            engine.execute('INSERT INTO state VALUES (?,?)',('stale-writer','true'))
            remote.db.execute('UPDATE square_cluster SET lease_until=0')
            other.acquire()
    c = nodes['aiven'].connection()
    assert c.execute('SELECT * FROM state WHERE key=?',('stale-writer',)).fetchone() is None
    c.close()
    assert remote.db.execute('SELECT COUNT(*) FROM square_journal').fetchone()[0] == 0
    other.release()
    other.client.close()


def test_two_workers_cannot_own_authority_at_same_time(cluster):
    remote,journal,nodes,engine = cluster
    other = remote.journal()
    with pytest.raises(DatabaseUnavailable, match='Another database writer'):
        other.acquire()
    other.client.close()


def test_rolling_deploy_standby_reads_and_health_without_stealing_writer(cluster,monkeypatch,tmp_path):
    import asyncio
    from fastapi.testclient import TestClient
    from square_desk.config import Settings
    from square_desk.service import Desk
    from square_desk.app import create_app
    remote,journal,nodes,engine = cluster
    other = remote.journal()
    standby = FailoverConnection({n:n for n in nodes},other,
        factory=lambda name:nodes[name].connection(),background=False)
    monkeypatch.setattr('square_desk.database_failover.make_connection',lambda p:standby)
    store = Store('postgresql://u:p@aiven.test:5432/db?sslmode=require')
    assert not store.acquire_lease('new-worker')
    assert store.state('paused') is False
    with pytest.raises(DatabaseUnavailable):
        store.set('must-not-write', True)
    settings = Settings(database=':memory:',artifacts=str(tmp_path/'charts'),admin_token='local-password')
    desk = Desk(settings,store=store)
    app = create_app(settings,desk=desk)
    with TestClient(app) as client:
        assert client.get('/health').status_code == 200
        assert client.head('/health').status_code == 200
        assert not desk.lease_owned
        assert remote.db.execute('SELECT owner FROM square_cluster').fetchone()[0] == journal.owner
    other.client.close()
    # Retiring instance released neither the primary's authority nor app lease.
    assert query(engine,'worker_lease')['owner'] == 'worker'


def test_standby_can_take_over_only_after_previous_authority_release(cluster):
    remote,journal,nodes,engine = cluster
    other = remote.journal()
    standby = FailoverConnection({n:n for n in nodes},other,
        factory=lambda name:nodes[name].connection(),background=False)
    assert not standby.writer_available()
    journal.release()
    assert standby.writer_available()
    standby.execute('INSERT INTO state VALUES (?,?)',('new-owner','true'))
    assert query(standby,'new-owner') is True
    with pytest.raises(DatabaseUnavailable):
        engine.execute('INSERT INTO state VALUES (?,?)',('old-owner','true'))
    standby.shutdown()
    other.client.close()


def test_wrong_baseline_and_divergent_replica_are_never_promoted(cluster):
    remote,journal,nodes,engine = cluster
    engine.execute('UPDATE entities SET status=? WHERE id=?',('sending','pending'))
    c = nodes['supabase'].connection()
    c.execute('DELETE FROM entities WHERE id=?',('pending',))
    c.close()
    c = nodes['neon'].connection()
    c.execute('UPDATE square_replica_meta SET baseline=?',('b'*64,))
    c.close()
    nodes['aiven'].down = True
    with pytest.raises(DatabaseUnavailable):
        query(engine,'paused')
    with pytest.raises(DatabaseUnavailable, match='No verified current'):
        query(engine,'paused')
    c = nodes['supabase'].connection()
    assert c.execute('SELECT seq FROM square_replica_meta').fetchone()[0] == 0
    c.close()


def test_missing_and_corrupt_journal_prevent_promotion(cluster):
    remote,journal,nodes,engine = cluster
    engine.execute('INSERT INTO state VALUES (?,?)',('new','true'))
    remote.db.execute('UPDATE square_journal SET payload=?',('corrupt',))
    nodes['aiven'].down = True
    with pytest.raises(DatabaseUnavailable):
        query(engine,'paused')
    with pytest.raises(DatabaseUnavailable):
        query(engine,'paused')


def test_journal_capacity_stops_before_unrecorded_mutation(cluster):
    remote,journal,nodes,engine = cluster
    journal.max_bytes = 1
    with pytest.raises(DatabaseUnavailable, match='capacity'):
        engine.execute('UPDATE budgets SET used=used+1 WHERE category=?',('posts',))
    assert remote.db.execute('SELECT seq FROM square_cluster').fetchone()[0] == 0


def test_audit_ids_are_deterministic_even_if_standby_sequence_differs(cluster):
    remote,journal,nodes,engine = cluster
    engine.execute('INSERT INTO audit(at,category,correlation,message) VALUES (?,?,?,?)',(utc(),'TEST','correlation','unchanged'))
    entry = remote.db.execute('SELECT payload FROM square_journal').fetchone()[0]
    assert decode(entry)[0]['sql'].startswith('INSERT INTO audit(id,')
    nodes['aiven'].down = True
    query(engine,'paused')
    row = engine.execute('SELECT * FROM audit').fetchone()
    assert row['id'] == 1 and row['message'] == 'unchanged'


def test_recovered_primary_does_not_force_failback(cluster):
    remote,journal,nodes,engine = cluster
    nodes['aiven'].down = True
    query(engine,'paused')
    query(engine,'paused')
    nodes['aiven'].down = False
    engine.nodes['aiven'].until = 0
    assert query(engine,'paused') is False
    assert engine.current == 'supabase'


def test_lease_cache_cannot_outlive_server_lease(cluster):
    remote,journal,nodes,engine = cluster
    # A process pause exhausts monotonic cache; acquire forces a write to authority.
    journal.deadline = time.monotonic()-1
    remote.down = True
    with pytest.raises(DatabaseUnavailable):
        query(engine,'paused')


def test_post_accepted_then_receipt_storage_lost_is_never_submitted_again(cluster, tmp_path):
    import asyncio
    from types import SimpleNamespace
    from square_desk.publisher import PublicationService
    from square_desk.config import Settings
    remote,journal,nodes,engine = cluster
    facade = Store(':memory:')
    facade.conn.close()
    facade.conn, facade.postgres = engine, True
    payload = {'title':'Evidence explained', 'body':'Observed context. ' * 40, 'risk':'medium'}
    facade.update('pending',status='queued',payload=payload)
    settings = Settings(artifacts=str(tmp_path/'artifacts'),paper_mode=False,live_enabled=True,mode='automatic')
    checker = SimpleNamespace(check=lambda d:[], duplicate=lambda d,**k:False)
    publication = PublicationService(settings,facade,checker,None)
    publication.policy = SimpleNamespace(gate=lambda row:None,live_gate=lambda:None)
    calls = []
    class Adapter:
        async def send(self, draft):
            calls.append(draft)
            remote.down = True  # Binance accepted, DB receipt cannot be persisted.
            return {'id':'accepted-remote-post'}
    publication.adapter = Adapter()
    with pytest.raises(DatabaseUnavailable):
        asyncio.run(publication.publish('pending'))
    remote.down = False
    assert facade.get('pending')['status'] == 'sending'
    facade.recover()
    assert facade.get('pending')['status'] == 'uncertain'
    asyncio.run(publication.publish('pending'))
    assert len(calls) == 1


def test_day_rollover_budgets_survive_failover(cluster):
    remote,journal,nodes,engine = cluster
    engine.execute('INSERT INTO budgets VALUES (?,?,?)',('2026-10-11','posts',2))
    nodes['aiven'].down = True
    rows = engine.execute('SELECT day,used FROM budgets ORDER BY day').rows
    assert [(r['day'],r['used']) for r in rows] == [('2026-10-10',7),('2026-10-11',2)]


def test_background_replication_copies_only_new_committed_entries(cluster):
    remote,journal,nodes,engine = cluster
    background = FailoverConnection({n:n for n in nodes},journal,
        factory=lambda name:nodes[name].connection(),background=True)
    try:
        background.execute('INSERT INTO state VALUES (?,?)',('new-value','123'))
        end = time.monotonic()+3
        while time.monotonic() < end and any(background.nodes[n].seq != 1 for n in ('supabase','neon')):
            time.sleep(.01)
        for name in ('supabase','neon'):
            assert background.nodes[name].seq == 1
            c = nodes[name].connection()
            assert c.execute('SELECT value FROM state WHERE key=?',('new-value',)).fetchone()[0] == '123'
            c.close()
    finally:
        background.shutdown()


def test_disabling_fallback_cannot_enable_direct_writes_on_registered_replica(monkeypatch):
    class Connection:
        def __init__(self,path):
            self.closed = False
        def execute(self,sql,args=None):
            return Result([Row({'name':'square_replica_meta','cluster':CLUSTER})],1)
        def close(self):
            self.closed = True
    monkeypatch.setenv('DESK_DB_FALLBACK_ENABLED','false')
    monkeypatch.setattr('square_desk.database_failover.PostgresConnection',Connection)
    with pytest.raises(DatabaseUnavailable,match='unfenced'):
        make_connection('postgresql://private:password@host/db?sslmode=require')


def test_round_trip_codec_and_bounded_corrupt_payload():
    value = [{'sql':'INSERT INTO state VALUES (?,?)','args':['key','exact 5% $BTC? 🧠'],'count':1}]
    assert decode(encode(value)) == value
    with pytest.raises(DatabaseUnavailable):
        decode('not-base64')
    with pytest.raises(DatabaseUnavailable):
        decode(encode([{'sql':'DROP TABLE state','args':[],'count':0}]))


@pytest.mark.parametrize('url',[
    'postgresql://user:pass@host:5432/db',
    'postgresql://user:pass@host:5432/db?sslmode=disable',
    'postgresql://user:pass@host:5432/db?sslmode=require&sslmode=verify-full',
    'postgresql://user:pass@host:5432/?sslmode=require',
    'postgresql://user:pass@host:bad/db?sslmode=require',
])
def test_bad_urls_rejected_without_exposing_credentials(url):
    with pytest.raises(ValueError) as exc:
        postgres_url(url)
    assert 'pass@' not in str(exc.value)


def test_configuration_requires_distinct_nodes_and_does_not_enable_itself(monkeypatch):
    for key in list(__import__('os').environ):
        if key.startswith('DESK_DB_') or key.endswith('_DATABASE_URL'):
            monkeypatch.delenv(key)
    monkeypatch.setenv('DESK_AIVEN_DATABASE_URL','postgresql://u:p@aiven.test:5432/db?sslmode=require')
    with pytest.raises(ValueError, match='at least two'):
        environment_nodes('')
    monkeypatch.setenv('DESK_SUPABASE_DATABASE_URL','postgresql://u:p@supabase.test:5432/db?sslmode=require')
    assert list(environment_nodes('')) == ['aiven','supabase']
    monkeypatch.setenv('DESK_DB_FALLBACK_ORDER','aiven,aiven')
    with pytest.raises(ValueError):
        environment_nodes('')


def test_turso_schema_uses_atomic_cas_and_trigger_and_redacts_errors():
    remote = HttpJournal()
    journal = remote.journal()
    try:
        journal.initialize(BASE)
        assert not remote.batons
        journal.acquire()
        journal.append([{'sql':'INSERT INTO state VALUES (?,?)','args':['a','true'],'count':1}])
        row = remote.db.execute('SELECT seq,log_bytes FROM square_cluster').fetchone()
        assert row[0] == 1 and row[1] > 0
        remote.down = True
        journal.deadline = 0
        with pytest.raises(DatabaseUnavailable) as exc:
            journal.acquire()
        assert 'private' not in str(exc.value)
    finally:
        journal.client.close()
        remote.db.close()


def test_journal_trim_requires_every_configured_replica_to_have_applied_history(cluster,monkeypatch):
    remote,journal,nodes,engine=cluster
    journal.head={**journal.head,'seq':5000,'log_bytes':40_000_000}
    engine.nodes['aiven'].seq=5000
    engine.nodes['supabase'].seq=4900
    engine.nodes['neon'].seq=None
    trimmed=[]
    monkeypatch.setattr(journal,'trim',lambda seq:trimmed.append(seq))
    engine._trim_ready()
    assert not trimmed
    engine.nodes['neon'].seq=4000
    engine._trim_ready()
    assert trimmed==[4000-1024]


def test_journal_trim_updates_capacity_but_does_not_reset_chain_head(cluster):
    remote,journal,nodes,engine=cluster
    for i in range(4):
        engine.execute('INSERT INTO state VALUES (?,?)',('key'+str(i),'true'))
    before=journal.head['log_bytes']
    sha=journal.head['head_hash']
    journal.trim(2)
    assert journal.head['seq']==4 and journal.head['head_hash']==sha
    assert journal.head['log_bytes']<before
    assert [r[0] for r in remote.db.execute('SELECT seq FROM square_journal ORDER BY seq')]==[3,4]
    # Empty/stale materialized copies cannot be promoted through missing entries.
    c=nodes['neon'].connection()
    with pytest.raises(DatabaseUnavailable,match='missing|chain mismatch'):
        catch_up(c,journal,journal.head)
    c.close()
