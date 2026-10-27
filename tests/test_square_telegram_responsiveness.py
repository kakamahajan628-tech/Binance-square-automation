import asyncio
import json

import httpx

from square_desk.config import Settings
from square_desk.service import Desk


def update(ident, text):
    return {'update_id': ident, 'message': {'from': {'id': 7},
        'chat': {'id': 7, 'type': 'private'}, 'text': text}}


def test_long_job_acknowledges_immediately_and_status_does_not_wait(tmp_path):
    async def run():
        sent, calls = [], []
        def respond(request):
            sent.append(json.loads(request.content).get('text', ''))
            return httpx.Response(200, json={'ok': True, 'result': {'message_id': len(sent)}})
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        s = Settings(database=':memory:', artifacts=str(tmp_path), telegram_token='test-token', telegram_admins=(7,), webhook_secret='test-secret')
        desk = Desk(s, client=client)
        entered, release = asyncio.Event(), asyncio.Event()
        async def generate(symbol, article=False):
            calls.append(symbol)
            entered.set()
            await release.wait()
            return 'draft-test'
        desk.create_for_symbol = generate
        await asyncio.wait_for(desk.telegram.receive(update(1, '/article_now BTC')), timeout=1)
        await asyncio.wait_for(entered.wait(), timeout=1)
        assert any('Command received' in text for text in sent)
        assert desk.db.list('command')[0]['status'] == 'processing'
        await asyncio.wait_for(desk.telegram.receive(update(2, '/status')), timeout=1)
        assert any('paper_mode' in text for text in sent)
        await desk.telegram.receive(update(3, '/post_now ETH'))
        assert any('abhi processing' in text for text in sent)
        await desk.telegram.receive(update(1, '/article_now BTC'))
        assert calls == ['BTC']
        await desk.telegram.receive(update(4, '/emergency_stop'))
        assert desk.db.state('emergency_stop') is True
        release.set()
        await asyncio.wait_for(desk.telegram.job_task, timeout=1)
        assert any('Draft draft-test created' in text for text in sent)
        assert desk.db.list('command', ['applied'])
        await desk.close()
    asyncio.run(run())


def test_telegram_poll_runs_while_scan_cycle_is_waiting(tmp_path):
    async def run():
        polled, scan_waiting = asyncio.Event(), asyncio.Event()
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
                             telegram_token='test-token', telegram_polling=True, telegram_admins=(7,), webhook_secret='test-secret'))
        async def cycle():
            scan_waiting.set()
            await asyncio.Event().wait()
        async def poll():
            polled.set()
        desk.cycle = cycle
        desk.telegram.poll = poll
        await desk.start()
        await asyncio.wait_for(scan_waiting.wait(), timeout=1)
        await asyncio.wait_for(polled.wait(), timeout=1)
        assert desk.telegram_task and not desk.telegram_task.done()
        await desk.close()
        assert desk.telegram_task.done()
    asyncio.run(run())


def test_standby_worker_never_polls_telegram(tmp_path):
    async def run():
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
                             telegram_token='test-token', telegram_polling=True, telegram_admins=(7,), webhook_secret='test-secret'))
        assert desk.db.acquire_lease('other-worker')
        polled = []
        async def poll():
            polled.append(True)
        desk.telegram.poll = poll
        await desk.start()
        await asyncio.sleep(0)
        assert not desk.lease_owned and not polled
        await desk.close()
    asyncio.run(run())


def test_shutdown_cancels_generation_without_replaying(tmp_path):
    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request:
            httpx.Response(200, json={'ok': True, 'result': {}})))
        desk = Desk(Settings(database=':memory:', artifacts=str(tmp_path),
                             telegram_token='test-token', telegram_admins=(7,), webhook_secret='test-secret'), client=client)
        entered, cancelled = asyncio.Event(), asyncio.Event()
        async def generate(symbol, article=False):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        desk.create_for_symbol = generate
        await desk.telegram.receive(update(1, '/post_now BTC'))
        await asyncio.wait_for(entered.wait(), timeout=1)
        await desk.close()
        assert cancelled.is_set() and desk.telegram.job_task.cancelled()
    asyncio.run(run())
