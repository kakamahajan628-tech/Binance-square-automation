import asyncio
from threading import Event

from square_desk.config import Settings
from square_desk.service import Desk
from square_desk.models import utc


def test_service_joins_cleanup_before_closing_native_database(tmp_path,monkeypatch):
    async def run():
        desk=Desk(Settings(database=':memory:',artifacts=str(tmp_path)))
        entered,finished=Event(),Event()
        def cleaning(store,directory,*,owner,stop):
            with store.transaction():
                entered.set()
                assert stop.wait(timeout=2)
                assert store.state('schema_version')==1
            finished.set()
            return {'complete':False}
        monkeypatch.setattr('square_desk.retention.run_cleanup',cleaning)
        desk.cleanup.maybe_start()
        assert await asyncio.to_thread(entered.wait,1)
        close=desk.db.close
        def guarded_close():
            assert finished.is_set()
            close()
        desk.db.close=guarded_close
        await asyncio.wait_for(desk.close(),3)
        assert desk.db.closed and finished.is_set()
    asyncio.run(run())


def test_repository_close_is_idempotent_and_serialized_with_transactions(tmp_path):
    from square_desk.store import Store
    from threading import Thread
    db=Store(':memory:')
    entered,release,closed=Event(),Event(),Event()
    def transaction():
        with db.transaction():
            entered.set();assert release.wait(timeout=2)
            db.set('last-safe-write',True)
    worker=Thread(target=transaction);worker.start();assert entered.wait(timeout=1)
    closer=Thread(target=lambda:(db.close(),closed.set()));closer.start()
    assert not closed.wait(timeout=.05)
    release.set();worker.join(2);closer.join(2)
    assert db.closed and closed.is_set()
    db.close()
