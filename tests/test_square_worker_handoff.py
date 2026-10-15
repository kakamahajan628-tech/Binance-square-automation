import asyncio
from square_desk.config import Settings
from square_desk.service import Desk
from square_desk.store import Store


def test_startup_standby_does_not_recover_or_run_active_worker(tmp_path):
    async def scenario():
        path = str(tmp_path / 'handoff.sqlite3')
        old = Store(path)
        assert old.acquire_lease('old-worker')
        ident = old.insert('draft', {'title': 'In progress'}, status='sending')
        settings = Settings(database=path, artifacts=str(tmp_path / 'artifacts'))
        desk = Desk(settings)
        calls = []
        async def cycle():
            calls.append('cycle')
        desk.cycle = cycle
        await desk.start()
        await asyncio.sleep(0)
        assert desk.status()['scheduler'] == 'waiting_for_worker_lease'
        assert old.get(ident)['status'] == 'sending'
        assert not calls
        await desk.close()
        assert old.state('worker_lease')['owner'] == 'old-worker'
        assert old.state('worker_lease')['until'] > 0
        old.close()
    asyncio.run(scenario())


def test_standby_takes_over_after_release_and_recovers_once(tmp_path, monkeypatch):
    original_sleep = asyncio.sleep
    async def short_sleep(seconds):
        await original_sleep(0)
    monkeypatch.setattr(asyncio, 'sleep', short_sleep)
    async def scenario():
        path = str(tmp_path / 'handoff.sqlite3')
        old = Store(path)
        assert old.acquire_lease('old-worker')
        ident = old.insert('draft', {'title': 'Unconfirmed'}, status='sending')
        desk = Desk(Settings(database=path, artifacts=str(tmp_path / 'artifacts')))
        ran = asyncio.Event()
        async def cycle():
            ran.set()
        desk.cycle = cycle
        await desk.start()
        old.set('worker_lease', {'owner': 'old-worker', 'until': 0})
        await asyncio.wait_for(ran.wait(), timeout=2)
        assert desk.lease_owned
        assert old.state('worker_lease')['owner'] == desk.owner
        assert old.get(ident)['status'] == 'uncertain'
        await desk.close()
        assert old.state('worker_lease')['until'] == 0
        old.close()
    asyncio.run(scenario())


def test_lease_renewal_failure_cancels_worker(tmp_path):
    async def scenario():
        desk = Desk(Settings(database=str(tmp_path / 'test.sqlite3'), artifacts=str(tmp_path / 'artifacts')))
        desk.lease_owned = True
        desk.task = asyncio.create_task(asyncio.sleep(100))
        def fail(*args):
            raise RuntimeError('database offline')
        desk.db.acquire_lease = fail
        await desk.heartbeat()
        assert not desk.lease_owned
        await desk.close()
        assert desk.task.cancelled()
    asyncio.run(scenario())
