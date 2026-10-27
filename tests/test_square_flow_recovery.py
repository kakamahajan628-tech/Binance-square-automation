import asyncio
from dataclasses import replace
import json
import httpx
import pytest

from square_desk.config import Settings
from square_desk.models import utc
from square_desk.providers import Transport, ProviderPool, ProviderError
from square_desk.service import Desk
from square_desk.store import Store
from test_square_desk import snapshot, draft_payload


def test_scan_persists_paper_tracking_outcome(tmp_path):
    async def run():
        s = Settings(database=':memory:', artifacts=str(tmp_path), symbols=('BTC',), market_universe='watchlist')
        db = Store(':memory:')
        desk = Desk(s, store=db)
        snap = snapshot()
        db.insert('signal', {'symbol': 'BTC', 'source': 'binance', 'quote': 'USDT',
            'last_bar': snap.candles[-2].end, 'as_of': snap.candles[-2].end,
            'direction': 'long', 'entry': 130, 'stop': 129,
            'expires_at': utc()+3600}, 'watching', id='old-signal')
        async def fetch(symbol, interval=900):
            return snapshot(symbol=symbol, interval=interval)
        desk.pool.fetch = fetch
        await desk.scan()
        assert db.get('old-signal')['status'] == 'invalidated'
        await desk.client.aclose()
        desk.image_executor.shutdown()
        db.close()
    asyncio.run(run())


def test_provider_observations_do_not_write_remote_state_but_controls_do(monkeypatch):
    monkeypatch.setenv('DESK_STORAGE_EFFICIENT', 'true')
    db = Store(':memory:')
    sql = []
    db.conn.set_trace_callback(sql.append)
    for i in range(50):
        db.set('provider:binance', {'status': 'ok', 'at': i})
    assert db.state('provider:binance')['at'] == 49
    assert not any('INSERT' in query for query in sql)
    db.set('paused', True)
    db.set('worker_lease', {'owner': 'other', 'until': utc()+60})
    assert db.conn.execute("SELECT COUNT(*) FROM state WHERE key IN ('paused','worker_lease')").fetchone()[0] == 2
    db.close()


def test_recent_cache_with_stale_candles_refreshes_and_concurrent_fetches_share_result():
    async def run():
        db = Store(':memory:')
        stale = snapshot(end=utc()-5000).dict()
        stale['fetched_at'] = utc()
        db.set('cache:BTC:900', stale)
        calls = []
        class Market:
            name = 'binance'
            async def candles(self, symbol, interval):
                calls.append(symbol)
                await asyncio.sleep(.01)
                return snapshot(symbol=symbol, interval=interval)
        pool = ProviderPool([Market()], db, 1200)
        results = await asyncio.gather(pool.fetch('BTC'), pool.fetch('BTC'))
        assert calls == ['BTC']
        assert results[0].as_of == results[1].as_of
        results[0].validate(utc(), 1200)
        db.close()
    asyncio.run(run())


def test_transport_respects_retry_after_and_cancellation_releases_slots():
    async def run():
        calls = []
        async def respond(request):
            calls.append(str(request.url))
            return httpx.Response(429, headers={'Retry-After': '120'}, json={})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            transport = Transport(client)
            with pytest.raises(ProviderError, match='429'):
                await transport.get('https://market.test', '/limited')
            with pytest.raises(ProviderError, match='cooling'):
                await transport.get('https://market.test', '/next')
            assert len(calls) == 1
    asyncio.run(run())


def test_scan_prepares_one_draft_before_slot_and_keeps_actual_publication_gap(tmp_path):
    async def run():
        s = Settings(database=':memory:', artifacts=str(tmp_path), symbols=('BTC', 'ETH'),
            market_universe='watchlist', mode='automatic', ai_auto_publish=True,
            daily_target=50, hourly_cap=3, gap_seconds=1200, auto_images=False, publishing_hours=tuple(range(24)))
        db = Store(':memory:')
        desk = Desk(s, store=db)
        now = utc()
        published = draft_payload()
        published['submitted_at'] = now-1100
        db.insert('draft', published, 'paper_published')
        db.set('last_auto_draft', now-100)  # Old independent throttle blocked this.
        async def fetch(symbol, interval=900):
            snap = snapshot(symbol=symbol, interval=interval)
            snap.candles[-1] = replace(snap.candles[-1], volume=400)
            return snap
        calls = []
        async def generate(event, article=False):
            calls.append(event)
            return db.insert('draft', {**draft_payload(event), 'generated_by': 'test'}, 'approved')
        async def no_manual_image(ident):
            return None
        desk.pool.fetch = fetch
        desk.content.draft = generate
        desk.make_image_async = no_manual_image
        await desk.scan()
        assert len(calls) == 1
        desk.scheduler.allocate(now)
        assert not desk.scheduler.due(now)
        queued = db.list('draft', ['queued'])
        assert len(queued) == 1
        assert queued[0]['due'] >= published['submitted_at']+1200
        await desk.scan()
        assert len(calls) == 1  # Pending draft prevents overproduction.
        assert desk.scan_observations['observed'] == 2
        await desk.client.aclose()
        desk.image_executor.shutdown()
        db.close()
    asyncio.run(run())


def test_scan_cancellation_stops_all_observation_tasks(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
            symbols=('BTC', 'ETH', 'SOL'), market_universe='watchlist'))
        active, maximum = 0, 0
        started = asyncio.Event()
        async def fetch(symbol, interval=900):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            started.set()
            try: await asyncio.Event().wait()
            finally: active -= 1
        desk.pool.fetch = fetch
        task = asyncio.create_task(desk.scan())
        await started.wait()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert active == 0 and maximum <= 2
        await desk.close()
    asyncio.run(run())


def test_slow_request_does_not_lock_out_other_market_requests():
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        async def respond(request):
            if request.url.path == '/slow':
                started.set()
                await release.wait()
            return httpx.Response(200, json={'ok': True})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            transport = Transport(client)
            transport.spacing_seconds = .005
            task = asyncio.create_task(transport.get('https://market.test', '/slow'))
            await started.wait()
            try:
                data = await asyncio.wait_for(transport.get('https://market.test', '/fresh'), .15)
                assert data['ok']
            finally:
                release.set()
                await task
    asyncio.run(run())


def test_telegram_delivers_oldest_pending_instead_of_starving_backlog(tmp_path):
    async def run():
        delivered = []
        async def respond(request):
            delivered.append(json.loads(request.content)['text'])
            return httpx.Response(200, json={'ok': True, 'result': {'message_id': len(delivered)}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            s = Settings(database=':memory:', artifacts=str(tmp_path), telegram_token='test', telegram_admins=(1,), webhook_secret='local-secret')
            db = Store(':memory:')
            desk = Desk(s, store=db, client=client)
            for i in range(12):
                ident = db.insert('outbox', {'chat_id': 1, 'text': str(i)})
                db.conn.execute('UPDATE entities SET created=? WHERE id=?', (100+i, ident))
            await desk.telegram.flush()
            assert delivered == [str(i) for i in range(10)]
            desk.image_executor.shutdown()
            db.close()
    asyncio.run(run())


def test_finished_job_does_not_keep_emitting_progress_while_delivery_waits(tmp_path):
    async def run():
        s = Settings(database=':memory:', artifacts=str(tmp_path), telegram_token='test',
                     telegram_admins=(1,), webhook_secret='local-secret')
        db = Store(':memory:')
        desk = Desk(s, store=db)
        s.ai_progress_seconds = .005
        command_id = db.insert('command', {}, 'processing')
        async def command(text):
            raise ValueError('Signal market data timed out')
        async def slow_flush():
            await asyncio.sleep(.03)
        desk.telegram.command = command
        desk.telegram.flush = slow_flush
        await desk.telegram._background_command('/post_now BTC', command_id, 42)
        texts = [r['payload']['text'] for r in db.list('outbox')]
        assert not any(' processing: ' in text for text in texts)
        assert db.state('command_last_job')['status'] == 'failed'
        await desk.client.aclose()
        desk.image_executor.shutdown()
        db.close()
    asyncio.run(run())
