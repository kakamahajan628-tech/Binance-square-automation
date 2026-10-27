import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import httpx
import pytest

from square_desk.ai_router import AIRouter
from square_desk.config import Settings
from square_desk.efficient_storage import packed
from square_desk.models import utc
from square_desk.runtime_status import RuntimeStatus
from square_desk.store import Store
from test_square_generation_audit import evidence, completion
from test_square_database_failover import cluster


@pytest.fixture
def configured(tmp_path):
    s = Settings(database=':memory:', artifacts=str(tmp_path),
        ai_url='https://api.groq.com/openai/v1/chat/completions', ai_key='local-groq',
        ai_model='openai/gpt-oss-120b', google_api_key='local-google', openrouter_api_key='local-openrouter',
        ai_provider_order=('groq','google','openrouter'),
        groq_models=('qwen/qwen3.8-27b','openai/gpt-oss-120b','openai/gpt-oss-20b'),
        google_models=('gemini-3.1-flash-lite','gemini-3-flash-preview','gemma-4-26b-a4b-it','gemma-4-31b-it'),
        openrouter_models=tuple('local-free-model-'+str(n) for n in range(10)))
    db = Store(':memory:')
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={})))
    router = AIRouter(s, db, client)
    yield SimpleNamespace(s=s, db=db, client=client, router=router)
    asyncio.run(client.aclose())
    db.close()


def test_full_ai_status_uses_two_bounded_selects_instead_of_per_model_reads(configured):
    db, router = configured.db, configured.router
    queries = []
    db.conn.set_trace_callback(queries.append)
    result = router.status()
    reads = [q for q in queries if q.lstrip().upper().startswith('SELECT')]
    assert len(reads) == 2, f'AI status issued {len(reads)} SQL reads'
    assert result['configured_order'] == list(router.providers)
    assert not any(q.lstrip().upper().startswith(('INSERT','UPDATE','DELETE')) for q in queries)


def test_report_does_not_fall_back_to_individual_state_or_budget_reads(configured, monkeypatch):
    def forbidden(*args, **kwargs): raise AssertionError('Unbatched diagnostic database read')
    monkeypatch.setattr(configured.db, 'state', forbidden)
    monkeypatch.setattr(configured.db, 'budget_used', forbidden)
    assert configured.router.status()['global_budget']['requests_used'] == 0


def test_snapshot_preserves_jobs_compressed_model_health_budgets_and_provider_order(configured):
    db, router = configured.db, configured.router
    now = utc()
    day = datetime.now(timezone.utc).date().isoformat()
    model = router.model_variants['groq'][0].s.ai_model
    health = {model:{'status':'rate_limited','until':now+90}}
    health.update({f'old-{n}':{'reason':'Old bounded metadata only '*20} for n in range(30)})
    db.set('ai_groq_model_health', packed(health))
    db.set('ai_last_job', {'id':'worker-report','command':'worker','status':'validated'})
    db.set('ai_active_generation', {'id':'active','symbol':'ZRO'})
    db.set('ai_provider_health', {'google':{'status':'unavailable','until':now+90}})
    db.set('ai_last_success', {'provider':'groq','at':now})
    db.reserve_budget(day,'ai_requests',7,1000)
    db.reserve_budget(day,'ai_tokens',4321,1000000)
    result = router.status()
    assert result['last_job']['id'] == 'worker-report'
    assert result['active_generation']['symbol'] == 'ZRO'
    assert result['model_health']['groq'][model]['status'] == 'rate_limited'
    assert 87 <= result['model_health']['groq'][model]['cooldown_remaining_seconds'] <= 90
    assert result['providers']['google']['status'] == 'unavailable'
    assert result['global_budget']['requests_used'] == 7
    assert result['global_budget']['tokens_used_or_reserved'] == 4321
    assert result['last_success']['provider'] == 'groq'


def test_sql_reads_share_the_available_read_transaction(configured):
    db = configured.db
    raw = db.conn
    events = []
    class ObservedConnection:
        @contextmanager
        def read_transaction(self):
            events.append('begin')
            try: yield
            finally: events.append('end')
        def execute(self, *args):
            assert events == ['begin']
            return raw.execute(*args)
    db.conn = ObservedConnection()
    try:
        configured.router.status()
        assert events == ['begin','end']
    finally: db.conn = raw


def test_ram_observations_work_with_database_unavailable_and_exclude_untrusted_fields(configured, monkeypatch):
    router = configured.router
    report = {'id':'live','command':'worker','symbol':'ZRO','started_at':utc(),
              'current_provider':'groq','current_model':'test-model','stage':'ai_generation',
              'prompt':'private text','token':'private key','body':'private draft'}
    router._observe(report, 'generating')
    monkeypatch.setattr(configured.db, 'state', lambda *a: (_ for _ in ()).throw(AssertionError('Database read')))
    result = router.observations()
    assert result['active_generation']['symbol'] == 'ZRO'
    assert result['active_generation']['progress_phase'] == 'generating'
    assert result['active_generation']['elapsed_seconds'] >= 0
    wire = json.dumps(result)
    assert 'private text' not in wire and 'private key' not in wire and 'private draft' not in wire
    assert result['status_source'] == 'memory_observations'


def test_runtime_snapshot_adds_bounded_ai_observations_without_database_reads(configured):
    router = configured.router
    router._observe({'id':'live','command':'worker','symbol':'ZRO','started_at':utc(),
        'current_provider':'groq','current_model':'openai/gpt-oss-120b','stage':'ai_generation',
        'attempts':[{'provider':'google','model':'example','status':'timeout','reason':'timeout'}]*6}, 'generating')
    desk = SimpleNamespace(db=configured.db, s=configured.s, content=SimpleNamespace(ai=router),
        telegram=SimpleNamespace(), publisher=SimpleNamespace(), scheduler=SimpleNamespace(),
        lease_owned=True, scan_task=None, stopping=False)
    result = RuntimeStatus(desk).snapshot()
    assert result['ai']['active_generation']['symbol'] == 'ZRO'
    assert 'attempts' not in result['ai']['active_generation']
    assert len(json.dumps(result,separators=(',',':')).encode('utf-16-le')) <= 6900


def test_compact_status_with_three_database_replicas_and_active_ai_fits_telegram(configured):
    import time
    router = configured.router
    router._observe({'id':'a'*32,'command':'worker','symbol':'ZRO','started_at':utc(),
        'current_provider':'openrouter','current_model':'nvidia/nemotron-3-super-120b-a12b:free',
        'stage':'ai_generation','attempts':[{'provider':'groq','status':'timeout'}]*6},'generating')
    router.record_job({'id':'b'*32,'command':'worker','symbol':'SOL','status':'validated',
        'current_provider':'google','current_model':'gemini-3.1-flash-lite'})
    nodes = {name:SimpleNamespace(status='unavailable_or_not_current',seq=83000,last_error='postgres_operation_failed',
        entity_repair={},snapshot_rebuild={'status':'held','reason':'snapshot_restore_unverified'})
        for name in ('aiven','neon','supabase')}
    nodes['aiven'].status = 'ready'
    conn = SimpleNamespace(nodes=nodes,current='aiven',replicated=True,last_error='',
        journal=SimpleNamespace(head={'seq':90529},epoch=1,deadline=time.monotonic()+120))
    desk = SimpleNamespace(db=SimpleNamespace(conn=conn),s=configured.s,content=SimpleNamespace(ai=router),
        telegram=SimpleNamespace(),publisher=SimpleNamespace(),scheduler=SimpleNamespace(),
        lease_owned=True,scan_task=None,stopping=False,
        scan_observations={'phase':'draft_generation','observed':25,'unavailable':0,'candidates':16,
            'drafts_created':0,'requested':25,'reason':'Evaluating candidates','symbol':'ZRO'})
    wire = json.dumps(RuntimeStatus(desk).snapshot(), separators=(',',':'))
    assert len(wire.encode('utf-16-le')) <= 6900


def test_live_generation_and_cancellation_update_ram_without_changing_validation(tmp_path):
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        async def respond(request):
            started.set()
            await release.wait()
            return httpx.Response(200,json=completion('Conditional market structure deserves careful interpretation. '*18))
        s = Settings(database=':memory:',artifacts=str(tmp_path), ai_key='local',
            ai_url='https://api.groq.com/openai/v1/chat/completions',ai_model='openai/gpt-oss-120b',
            groq_models=('openai/gpt-oss-120b',))
        db = Store(':memory:')
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            router = AIRouter(s, db, client)
            task = asyncio.create_task(router.generate(evidence(True)))
            await asyncio.wait_for(started.wait(),1)
            progress = router.observations()
            assert progress['active_generation']['current_provider'] == 'groq'
            assert progress['active_generation']['progress_phase'] == 'generating'
            release.set()
            result = await task
            assert result['ai_provider'] == 'groq'
            assert router.observations()['active_generation'] == {}
            assert router.observations()['last_job']['status'] == 'validated'
            assert router.observations()['last_job']['saved_to_database']
            provider = router.providers['groq']
            getattr(provider, 'pacing_owner', provider).next_request_at = 0
            started.clear(); release.clear()
            task = asyncio.create_task(router.generate(evidence(True)))
            await asyncio.wait_for(started.wait(),1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError): await task
            assert router.observations()['active_generation'] == {}
            assert router.observations()['last_job']['status'] == 'cancelled'
        db.close()
    asyncio.run(run())


def test_status_snapshot_uses_verified_failover_read_without_journal_mutation(cluster):
    remote, journal, nodes, engine = cluster
    store = Store(':memory:'); store.conn.close()
    store.conn, store.postgres = engine, True
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={})))
    router = AIRouter(Settings(), store, client)
    before = journal.head['seq']
    try:
        result = router.status()
        assert result['configured'] is False and journal.head['seq'] == before
        nodes['aiven'].down = True
        assert router.status()['configured'] is False
        assert engine.current != 'aiven' and journal.head['seq'] == before
    finally: asyncio.run(client.aclose())


def test_ram_progress_remains_readable_during_slow_database_report_write(configured, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from time import perf_counter
    router, db = configured.router, configured.db
    started, release = Event(), Event()
    report = {'id':'saving-worker','command':'worker','symbol':'ZRO','started_at':utc(),'status':'validated'}
    router._observe(report, 'validating_output')
    original = db.set
    def blocked(key, value):
        if key == 'ai_last_job':
            started.set()
            assert release.wait(2)
        return original(key,value)
    monkeypatch.setattr(db,'set',blocked)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(router.record_job,report)
        try:
            assert started.wait(1)
            before = perf_counter()
            observed = router.observations()
            assert perf_counter()-before < .1 and not future.done()
            assert observed['active_generation']['progress_phase'] == 'saving_job_result'
            assert not observed['last_job']['saved_to_database']
        finally:
            release.set()
            future.result(timeout=2)
    assert router.observations()['last_job']['saved_to_database']


def test_failed_report_write_is_never_reported_as_durable_success(configured, monkeypatch):
    from square_desk.postgres import DatabaseUnavailable
    def failed(*args): raise DatabaseUnavailable('private database details')
    monkeypatch.setattr(configured.db,'set',failed)
    with pytest.raises(DatabaseUnavailable):
        configured.router.record_job({'id':'not-saved','command':'worker','status':'validated'})
    observed = configured.router.observations()
    assert observed['last_job']['saved_to_database'] is False
    assert 'private database details' not in json.dumps(observed)


def test_waiting_cancelled_job_cannot_replace_current_owner_in_ram(tmp_path):
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        async def respond(request):
            started.set(); await release.wait()
            return httpx.Response(200,json=completion('Conditional market structure deserves careful interpretation. '*18))
        s = Settings(database=':memory:',artifacts=str(tmp_path),ai_key='local',
            ai_url='https://api.groq.com/openai/v1/chat/completions',ai_model='openai/gpt-oss-120b',
            groq_models=('openai/gpt-oss-120b',))
        db = Store(':memory:')
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            router = AIRouter(s,db,client)
            active = asyncio.create_task(router.generate(evidence(True)))
            await asyncio.wait_for(started.wait(),1)
            ident = router.observations()['active_generation']['id']
            waiter = asyncio.create_task(router.generate(evidence(True)))
            await asyncio.sleep(.01)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError): await waiter
            assert router.observations()['active_generation']['id'] == ident
            release.set(); await active
        db.close()
    asyncio.run(run())
